"""FastAPI application: subscription management, event ingestion, status queries."""
from __future__ import annotations

import asyncio
import base64
import binascii
import json
import logging
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone

import httpx
from fastapi import FastAPI, HTTPException, Query, Request, Response
from pydantic import ValidationError

from . import config, signing
from .db import Database, MigrationError
from .delivery_window import DeliveryWindow, format_clock, parse_clock
from .schemas import (
    DeliveryStatus,
    DeliveryTaskDetail,
    DeliveryTaskPage,
    DeliveryWindowCreate,
    DeliveryWindowOut,
    EventAccepted,
    EventCancelResult,
    EventCreate,
    EventStatus,
    IngestEvent,
    RatePolicyCreate,
    RatePolicyOut,
    SecretRotationCreate,
    SubscriptionCreate,
    SubscriptionOut,
    TopicEventAccepted,
    TopicEventCreate,
    TopicsOut,
    TopicsUpdate,
)
from .worker import worker_loop

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
log = logging.getLogger("webhook-relay")


def _iso(ts: float | None) -> str | None:
    if ts is None:
        return None
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()


def _encode_cursor(updated_at: float, task_id: int) -> str:
    """Opaque pagination cursor: the (updated_at, id) key of a page's last row."""
    raw = json.dumps([updated_at, task_id], separators=(",", ":")).encode()
    return base64.urlsafe_b64encode(raw).decode().rstrip("=")


def _decode_cursor(cursor: str) -> tuple[float, int]:
    """Decode a cursor produced by _encode_cursor; anything else is a 400."""
    try:
        padded = cursor + "=" * (-len(cursor) % 4)
        data = json.loads(base64.urlsafe_b64decode(padded.encode()).decode())
        updated_at, task_id = data
        if isinstance(updated_at, bool) or not isinstance(updated_at, (int, float)):
            raise ValueError
        if isinstance(task_id, bool) or not isinstance(task_id, int):
            raise ValueError
    except (binascii.Error, UnicodeDecodeError, ValueError, TypeError):
        raise HTTPException(
            400, "invalid cursor: pass the next_cursor value returned by the previous page"
        )
    return float(updated_at), task_id


def _parse_audit_time(value: str, param: str) -> float:
    """Parse an audit time-range bound (ISO-8601, explicit timezone) to epoch seconds."""
    try:
        dt = datetime.fromisoformat(value.strip())
    except ValueError:
        raise HTTPException(
            400, f"{param} must be an ISO-8601 timestamp, e.g. 2026-09-18T00:00:00Z"
        )
    if dt.tzinfo is None or dt.tzinfo.utcoffset(dt) is None:
        raise HTTPException(400, f"{param} must include an explicit timezone, e.g. +00:00 or 'Z'")
    return dt.astimezone(timezone.utc).timestamp()


def _delivery_summary(t: dict) -> dict:
    """Serialize a task row for the audit listing (and as the detail's base)."""
    return {
        "task_id": t["id"],
        "event_id": t["event_id"],
        "subscription_id": t["subscription_id"],
        "target_url": t["target_url"],
        "status": t["status"],
        "attempts": t["attempts"],
        "last_error": t["last_error"],
        "deliver_at": _iso(t["deliver_at"]),
        "updated_at": _iso(t["updated_at"]),
    }


def _sub_out(sub: dict) -> dict:
    """Serialize a subscription row for the API, deriving the paused flag.

    Secret material (current or previous) is never included; only the
    rotation state (flag + grace expiry) is exposed. The circuit-breaker
    state is reported as persisted: 'closed' | 'open' | 'half_open', with
    the consecutive-failure count and, while open, the earliest time the
    half-open probe may run.
    """
    grace_expires = sub.get("previous_secret_expires_at")
    rotating = grace_expires is not None and grace_expires > time.time()
    return {
        "id": sub["id"],
        "target_url": sub["target_url"],
        "created_at": _iso(sub["created_at"]),
        "paused": sub["paused_at"] is not None,
        "paused_at": _iso(sub["paused_at"]),
        "rotating": rotating,
        "previous_secret_expires_at": _iso(grace_expires) if rotating else None,
        "state": sub["breaker_state"],
        "consecutive_failures": sub["breaker_consecutive_failures"],
        "next_probe_at": _iso(sub["breaker_next_probe_at"]),
        "rate_per_second": sub.get("rate_refill_per_second"),
        "burst": sub.get("rate_capacity"),
    }


def _rate_policy_out(policy: dict) -> dict:
    """Serialize a rate-policy row (from db.get_rate_policy) for the API."""
    enabled = policy["rate_per_second"] is not None
    return {
        "enabled": enabled,
        "rate_per_second": policy["rate_per_second"] if enabled else None,
        "burst": policy["burst"] if enabled else None,
        "tokens_available": round(policy["tokens_available"], 6) if enabled else None,
        "next_token_at": _iso(policy["next_token_at"]),
    }


def _delivery_window_out(now: float, policy: dict) -> dict:
    """Serialize a delivery-window row (from db.get_delivery_window) for the API.

    The live is_open_now/next_open_at fields are evaluated here from the
    stored configuration against the supplied 'now' epoch; they never write
    back. next_open_at is reported only while the window is currently
    closed.
    """
    if not policy["configured"]:
        return {
            "enabled": False,
            "timezone": None,
            "weekdays": None,
            "local_start": None,
            "local_end": None,
            "wraps_past_midnight": None,
            "is_open_now": True,  # no window configured: nothing is restricted
            "next_open_at": None,
            "updated_at": None,
        }
    window = DeliveryWindow(
        policy["timezone"], tuple(policy["weekdays"]),
        policy["local_start"], policy["local_end"],
    )
    is_open = window.is_open(now)
    return {
        "enabled": True,
        "timezone": policy["timezone"],
        "weekdays": list(policy["weekdays"]),
        "local_start": format_clock(policy["local_start"]),
        "local_end": format_clock(policy["local_end"]),
        "wraps_past_midnight": window.wraps_past_midnight,
        "is_open_now": is_open,
        "next_open_at": _iso(window.next_open(now)) if not is_open else None,
        "updated_at": _iso(policy["updated_at"]),
    }


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Constructing the Database runs the ordered PRAGMA user_version chain
    # and a final foreign-key integrity check. A failure must abort startup:
    # propagate it before any worker is created, so no delivery runs against
    # a partially migrated or inconsistent database.
    try:
        db = Database(
            config.DB_PATH,
            breaker_failure_threshold=config.BREAKER_FAILURE_THRESHOLD,
            breaker_cooldown_seconds=config.BREAKER_COOLDOWN_SECONDS,
        )
    except MigrationError:
        log.critical(
            "database migration failed for db=%s; refusing to start",
            config.DB_PATH,
            exc_info=True,
        )
        raise
    app.state.db = db
    app.state.http = httpx.AsyncClient(timeout=config.HTTP_TIMEOUT)

    # Resume work interrupted by a previous shutdown.
    recovered = db.requeue_in_progress()
    if recovered:
        log.info("requeued %s task(s) interrupted by previous run", recovered)

    stop = asyncio.Event()
    app.state.stop = stop
    workers = [
        asyncio.create_task(worker_loop(f"worker-{i+1}", db, app.state.http, stop))
        for i in range(config.WORKER_COUNT)
    ]
    log.info("started %s delivery worker(s), db=%s", len(workers), config.DB_PATH)
    try:
        yield
    finally:
        stop.set()
        await asyncio.gather(*workers)
        await app.state.http.aclose()
        db.close()


app = FastAPI(title="Local Webhook Relay", version="1.0.0", lifespan=lifespan)


@app.get("/healthz")
def healthz() -> dict:
    return {"status": "ok"}


# ----------------------------------------------------------------------
# subscriptions
# ----------------------------------------------------------------------
@app.post("/subscriptions", response_model=SubscriptionOut, status_code=201)
def create_subscription(body: SubscriptionCreate, response: Response) -> dict:
    sub = app.state.db.create_subscription(body.target_url, body.secret)
    return _sub_out(sub)


@app.get("/subscriptions", response_model=list[SubscriptionOut])
def list_subscriptions() -> list[dict]:
    return [_sub_out(s) for s in app.state.db.list_subscriptions()]


@app.get("/subscriptions/{sub_id}", response_model=SubscriptionOut)
def get_subscription(sub_id: str) -> dict:
    sub = app.state.db.get_subscription(sub_id)
    if sub is None:
        raise HTTPException(404, f"subscription {sub_id!r} not found")
    return _sub_out(sub)


@app.delete("/subscriptions/{sub_id}", status_code=204)
def delete_subscription(sub_id: str) -> Response:
    if not app.state.db.delete_subscription(sub_id):
        raise HTTPException(404, f"subscription {sub_id!r} not found")
    return Response(status_code=204)


@app.post("/subscriptions/{sub_id}/pause", response_model=SubscriptionOut)
def pause_subscription(sub_id: str) -> dict:
    """Pause claiming of new deliveries for a subscription (idempotent).

    Tasks already in progress run to completion; tasks submitted while
    paused are stored and stay 'pending' until the subscription resumes.
    """
    sub = app.state.db.set_subscription_paused(sub_id, True)
    if sub is None:
        raise HTTPException(404, f"subscription {sub_id!r} not found")
    return _sub_out(sub)


@app.post("/subscriptions/{sub_id}/resume", response_model=SubscriptionOut)
def resume_subscription(sub_id: str) -> dict:
    """Resume a paused subscription (idempotent).

    Pending tasks keep their original next_attempt_at and are claimed in
    the usual per-subscription task_id order; nothing is rescheduled.
    """
    sub = app.state.db.set_subscription_paused(sub_id, False)
    if sub is None:
        raise HTTPException(404, f"subscription {sub_id!r} not found")
    return _sub_out(sub)


@app.post("/subscriptions/{sub_id}/rotate-secret", response_model=SubscriptionOut)
def rotate_subscription_secret(sub_id: str, body: SecretRotationCreate) -> dict:
    """Rotate the delivery-signing secret with a dual-sign grace window.

    The new secret takes effect immediately for the X-Webhook-Signature
    header; during grace_seconds the previous secret is additionally sent
    as X-Webhook-Signature-Previous so receivers can switch over safely.
    A second rotation while the grace window is still open is rejected.
    """
    status, sub = app.state.db.rotate_secret(sub_id, body.new_secret, body.grace_seconds)
    if status == "not_found":
        raise HTTPException(404, f"subscription {sub_id!r} not found")
    if status == "in_grace":
        until = _iso(sub["previous_secret_expires_at"])
        raise HTTPException(409, f"a secret rotation is already in progress until {until}")
    log.info("subscription %s rotated signing secret (grace %.0fs)", sub_id, body.grace_seconds)
    return _sub_out(sub)


@app.put("/subscriptions/{sub_id}/rate-policy", response_model=RatePolicyOut)
def put_rate_policy(sub_id: str, body: RatePolicyCreate) -> dict:
    """Install or replace a subscription's token-bucket rate policy.

    rate_per_second is the sustained rate (fractional allowed, e.g. 0.5
    for one delivery every two seconds); burst is the bucket capacity,
    defaulting to one second worth of tokens. The bucket starts full, so a
    freshly configured subscription may immediately send up to 'burst'
    deliveries back-to-back. Claiming deducts a token atomically inside the
    worker's database critical section; when no token is available the head
    task stays pending until the deterministically scheduled refill. The
    bucket state is persisted, so it stays consistent across restarts and
    across concurrent workers.
    """
    burst = body.burst if body.burst is not None else max(1.0, body.rate_per_second)
    sub = app.state.db.set_rate_policy(sub_id, burst, body.rate_per_second)
    if sub is None:
        raise HTTPException(404, f"subscription {sub_id!r} not found")
    # The bucket was just reset full with its timestamp stamped to now, so
    # its live view is derivable directly from the returned row.
    policy = {
        "rate_per_second": sub["rate_refill_per_second"],
        "burst": sub["rate_capacity"],
        "tokens_available": sub["rate_tokens"],
        "next_token_at": None,
    }
    log.info(
        "subscription %s rate policy set: %s tokens/s, burst %s",
        sub_id, body.rate_per_second, burst,
    )
    return _rate_policy_out(policy)


@app.get("/subscriptions/{sub_id}/rate-policy", response_model=RatePolicyOut)
def get_rate_policy(sub_id: str) -> dict:
    """Return the subscription's rate policy and its live bucket state."""
    policy = app.state.db.get_rate_policy(sub_id)
    if policy is None:
        raise HTTPException(404, f"subscription {sub_id!r} not found")
    return _rate_policy_out(policy)


@app.delete("/subscriptions/{sub_id}/rate-policy", status_code=204)
def delete_rate_policy(sub_id: str) -> Response:
    """Remove a subscription's rate policy (rate limiting disabled)."""
    if not app.state.db.delete_rate_policy(sub_id):
        raise HTTPException(404, f"subscription {sub_id!r} not found")
    return Response(status_code=204)


@app.put("/subscriptions/{sub_id}/delivery-window", response_model=DeliveryWindowOut)
def put_delivery_window(sub_id: str, body: DeliveryWindowCreate) -> dict:
    """Install or replace a subscription's delivery window.

    The window names an IANA timezone, the weekdays (1=Mon..7=Sun) on which
    it opens, and local start/end wall-clock times; an end at or before the
    start denotes a window that wraps past midnight. While the window is
    closed the worker does not claim the subscription's head task: inside
    the same atomic claim critical section it deterministically pushes
    next_attempt_at to the window's next opening instant (DST-aware,
    persisted as an absolute UTC epoch) and keeps processing other
    subscriptions. Nothing is delivered early across a restart because the
    gate and the schedule live entirely in persisted state. Installing a
    window never rewrites task rows; pending tasks meet the new window on
    their next claim attempt.
    """
    start = parse_clock(body.local_start)
    end = parse_clock(body.local_end)
    stored = app.state.db.set_delivery_window(
        sub_id, body.timezone, body.weekdays, start, end
    )
    if stored is None:
        raise HTTPException(404, f"subscription {sub_id!r} not found")
    log.info(
        "subscription %s delivery window set: zone=%s weekdays=%s %s-%s",
        sub_id, body.timezone, body.weekdays, body.local_start, body.local_end,
    )
    # Serialize the row re-read inside the committing write transaction:
    # this is the same committed configuration a follow-up GET returns, so a
    # create -> PUT -> GET chain always observes one persisted window.
    return _delivery_window_out(time.time(), stored)


@app.get("/subscriptions/{sub_id}/delivery-window", response_model=DeliveryWindowOut)
def get_delivery_window(sub_id: str) -> dict:
    """Return the subscription's window and its live open state.

    A subscription without a window answers enabled=false. is_open_now and
    next_open_at are computed from the IANA zone at the query instant; a
    GET never persists scheduling state.
    """
    policy = app.state.db.get_delivery_window(sub_id)
    if policy is None:
        raise HTTPException(404, f"subscription {sub_id!r} not found")
    return _delivery_window_out(time.time(), policy)


@app.delete("/subscriptions/{sub_id}/delivery-window", status_code=204)
def delete_delivery_window(sub_id: str) -> Response:
    """Remove a subscription's delivery window (deliveries unrestricted)."""
    if not app.state.db.delete_delivery_window(sub_id):
        raise HTTPException(404, f"subscription {sub_id!r} not found")
    return Response(status_code=204)


@app.put("/subscriptions/{sub_id}/topics", response_model=TopicsOut)
def put_subscription_topics(sub_id: str, body: TopicsUpdate) -> dict:
    """Replace a subscription's deduplicated topic set.

    The supplied list is normalized (trimmed, deduplicated) and replaces the
    whole set atomically; pass an empty list to leave every topic. The
    response carries the canonical sorted set. Membership only affects
    future publications: events already published keep the recipient
    snapshot taken at publication time. A subscription removed after
    publishing keeps its delivery history; deleting a subscription also
    removes it from every topic set.
    """
    topics = app.state.db.set_subscription_topics(sub_id, body.topics)
    if topics is None:
        raise HTTPException(404, f"subscription {sub_id!r} not found")
    return {"subscription_id": sub_id, "topics": topics}


@app.get("/subscriptions/{sub_id}/topics", response_model=TopicsOut)
def get_subscription_topics(sub_id: str) -> dict:
    """Return a subscription's current sorted, deduplicated topic set."""
    topics = app.state.db.get_subscription_topics(sub_id)
    if topics is None:
        raise HTTPException(404, f"subscription {sub_id!r} not found")
    return {"subscription_id": sub_id, "topics": topics}


# ----------------------------------------------------------------------
# events
# ----------------------------------------------------------------------
@app.post("/events", response_model=EventAccepted)
def submit_event(body: EventCreate, response: Response) -> dict:
    db: Database = app.state.db

    unknown = [sid for sid in body.subscription_ids if not db.subscription_exists(sid)]
    if unknown:
        raise HTTPException(400, f"unknown subscription id(s): {', '.join(unknown)}")

    # Store the event once; an identical re-submission is idempotent, a
    # conflicting payload for the same event_id is rejected. An id already
    # used by a topic publication is a cross-channel conflict as well.
    payload = json.dumps(body.payload, separators=(",", ":"), ensure_ascii=False)
    if db.topic_event_exists(body.event_id):
        raise HTTPException(
            409,
            f"event {body.event_id!r} already exists as a topic publication; "
            "topic events and direct events share the event id namespace",
        )
    existing = db.get_event_payload(body.event_id)
    if existing is None:
        db.create_event(body.event_id, payload)
    elif existing != payload:
        raise HTTPException(409, f"event {body.event_id!r} already exists with a different payload")

    # Optional scheduled delivery. Stored as epoch seconds and seeded into
    # each task's next_attempt_at, so workers simply cannot claim the tasks
    # before this instant; None means the tasks are due right away.
    deliver_at_ts = (
        body.deliver_at.astimezone(timezone.utc).timestamp()
        if body.deliver_at is not None
        else None
    )

    created, duplicates = [], []
    for sid in dict.fromkeys(body.subscription_ids):  # preserve order, drop repeats
        task, was_created = db.create_task(
            body.event_id, sid, config.MAX_ATTEMPTS, deliver_at=deliver_at_ts
        )
        ref = {
            "task_id": task["id"],
            "subscription_id": sid,
            "status": task["status"],
            "deliver_at": _iso(task["deliver_at"]),
        }
        (created if was_created else duplicates).append(ref)

    response.status_code = 201 if created else 200
    return {"event_id": body.event_id, "created": created, "duplicates": duplicates}


# ----------------------------------------------------------------------
# persistent topic publishing
# ----------------------------------------------------------------------
@app.post("/topic-events", response_model=TopicEventAccepted)
def publish_topic_event(body: TopicEventCreate, response: Response) -> dict:
    """Publish an event to every subscription currently on a topic.

    The topic's membership is snapshotted inside a single SQLite
    transaction together with the event and the generated delivery tasks,
    so the recipients are fixed at first-publication time: later topic
    membership changes never add or drop tasks for this event. The tasks
    are ordinary delivery tasks and flow through the existing ordering,
    rate limiting, circuit breaking and retry machinery unchanged.

    The idempotency key is (event_id, topic, payload):

    - an exact replay returns the first publication's recipients and task
      results (same task ids and statuses), HTTP 200, and creates nothing;
    - the same event_id with a different topic or payload is 409;
    - an event_id already used via POST /events or POST /ingest is 409;
    - a topic with no subscribers is 422 and nothing is persisted.
    """
    payload = json.dumps(body.payload, separators=(",", ":"), ensure_ascii=False)
    status, result = app.state.db.publish_topic_event(
        body.event_id, body.topic, payload, config.MAX_ATTEMPTS
    )
    if status == "conflict":
        if result == "topic":
            raise HTTPException(
                409,
                f"event {body.event_id!r} was already published to a different topic",
            )
        if result == "payload":
            raise HTTPException(
                409,
                f"event {body.event_id!r} was already published with a different payload",
            )
        raise HTTPException(
            409,
            f"event {body.event_id!r} already exists as a direct/ingested event; "
            "topic events and direct events share the event id namespace",
        )
    if status == "empty":
        raise HTTPException(422, f"topic {body.topic!r} has no subscribers; nothing was stored")

    response.status_code = 201 if status == "created" else 200
    result["replayed"] = status == "duplicate"
    return result


# ----------------------------------------------------------------------
# HMAC-protected inbound ingest endpoint
# ----------------------------------------------------------------------
@app.post("/ingest", response_model=EventAccepted)
async def ingest_event(request: Request, response: Response) -> dict:
    if not config.INGEST_SECRET:
        # Fail closed: without a configured secret every signature is suspect.
        raise HTTPException(503, "ingest endpoint is not configured (set INGEST_SECRET)")

    timestamp_h = request.headers.get("x-webhook-timestamp")
    webhook_id = request.headers.get("x-webhook-id")
    signature_h = request.headers.get("x-webhook-signature")
    if not timestamp_h or not webhook_id or not signature_h:
        raise HTTPException(
            400,
            "missing required header(s): X-Webhook-Timestamp, X-Webhook-Id, X-Webhook-Signature",
        )
    if len(webhook_id) > 128:
        raise HTTPException(400, "X-Webhook-Id too long (max 128 characters)")

    timestamp_h = timestamp_h.strip()
    try:
        ts = float(timestamp_h)
    except ValueError:
        raise HTTPException(400, "X-Webhook-Timestamp must be a Unix timestamp in seconds")

    skew = abs(time.time() - ts)
    if skew > config.INGEST_TIMESTAMP_TOLERANCE:
        raise HTTPException(
            401,
            f"timestamp outside the allowed +/-{config.INGEST_TIMESTAMP_TOLERANCE:.0f}s window "
            f"(skew {skew:.0f}s)",
        )

    raw_body = await request.body()
    if not raw_body:
        raise HTTPException(400, "request body must not be empty")

    if not signing.verify_ingest(config.INGEST_SECRET, timestamp_h, webhook_id, raw_body, signature_h):
        raise HTTPException(401, "invalid X-Webhook-Signature")

    # Signature verified; now validate the body shape.
    try:
        parsed = json.loads(raw_body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        raise HTTPException(400, "request body must be valid UTF-8 JSON")
    try:
        body = IngestEvent.model_validate(parsed)
    except ValidationError as exc:
        raise HTTPException(422, json.loads(exc.json()))

    status, result = app.state.db.ingest_event(
        webhook_id, raw_body, body.subscription_ids, config.MAX_ATTEMPTS
    )
    if status == "conflict":
        raise HTTPException(
            409,
            f"webhook id {webhook_id!r} was already received with a different body, "
            "or is already used by a topic/direct event",
        )
    if status == "unknown":
        raise HTTPException(400, f"unknown subscription id(s): {', '.join(result)}")

    # Replays with the same id and body return the original result and never
    # create additional delivery tasks (tasks are only created on 'created').
    response.status_code = 201 if status == "created" else 200
    return result


@app.post("/events/{event_id}/cancel", response_model=EventCancelResult)
def cancel_event(event_id: str) -> dict:
    """Cancel the still-pending delivery tasks of an event (idempotent).

    Only tasks no worker has claimed yet are cancelled; tasks already in
    progress run to completion and finished tasks keep their recorded
    result. Cancelled tasks are terminal: they are never retried, never
    counted by the per-subscription circuit breaker, and never block later
    tasks of the same subscription. Repeating the call is a no-op that
    returns the same per-status totals; an unknown event returns 404.
    """
    result = app.state.db.cancel_event(event_id)
    if result is None:
        raise HTTPException(404, f"event {event_id!r} not found")
    log.info(
        "event %s cancelled: %s task(s) newly cancelled, totals=%s",
        event_id,
        result["cancelled"],
        result["counts"],
    )
    return {
        "event_id": event_id,
        "cancelled": result["cancelled"],
        "cancelled_at": _iso(result["cancelled_at"]),
        "counts": result["counts"],
    }


@app.get("/events/{event_id}", response_model=EventStatus)
def get_event_status(event_id: str) -> dict:
    data = app.state.db.event_status(event_id)
    if data is None:
        raise HTTPException(404, f"event {event_id!r} not found")

    counts = {"pending": 0, "in_progress": 0, "delivered": 0, "failed": 0, "cancelled": 0}
    last_cancelled_at: float | None = None
    targets = []
    for t in data["tasks"]:
        counts[t["status"]] = counts.get(t["status"], 0) + 1
        if t["status"] == "cancelled" and t.get("cancelled_at") is not None:
            if last_cancelled_at is None or t["cancelled_at"] > last_cancelled_at:
                last_cancelled_at = t["cancelled_at"]
        history = [
            {
                "attempt_no": a["attempt_no"],
                "status_code": a["status_code"],
                "error": a["error"],
                "started_at": _iso(a["started_at"]),
                "finished_at": _iso(a["finished_at"]),
                "duration_ms": round((a["finished_at"] - a["started_at"]) * 1000),
            }
            for a in data["attempts_by_task"].get(t["id"], [])
        ]
        targets.append(
            {
                "task_id": t["id"],
                "subscription_id": t["subscription_id"],
                "target_url": t["target_url"],
                "status": t["status"],
                "attempts": t["attempts"],
                "max_attempts": t["max_attempts"],
                "deliver_at": _iso(t["deliver_at"]),
                "next_attempt_at": _iso(t["next_attempt_at"]) if t["status"] == "pending" else None,
                "last_status_code": t["last_status_code"],
                "last_error": t["last_error"],
                "result": t["result"],
                "cancelled_at": _iso(t.get("cancelled_at")),
                "history": history,
            }
        )
    return {
        "event_id": event_id,
        "created_at": _iso(data["event"]["created_at"]),
        "cancelled": counts["cancelled"] > 0,
        "cancelled_at": _iso(last_cancelled_at),
        "counts": counts,
        "targets": targets,
    }


# ----------------------------------------------------------------------
# delivery audit queries
# ----------------------------------------------------------------------
@app.get("/audit/deliveries", response_model=DeliveryTaskPage)
def list_deliveries(
    subscription_id: str | None = None,
    event_id: str | None = None,
    status: DeliveryStatus | None = None,
    updated_from: str | None = None,
    updated_to: str | None = None,
    limit: int = Query(default=50, ge=1, le=100),
    cursor: str | None = None,
) -> dict:
    """Audit listing of delivery tasks, newest-updated first.

    Filters combine with AND; updated_from/updated_to are inclusive ISO-8601
    bounds on the task's last update time (UTC after normalization) and must
    carry an explicit timezone. Pagination is keyset-based on
    (updated_at, id): pass the returned next_cursor back as ?cursor= until
    it comes back null.
    """
    from_ts = _parse_audit_time(updated_from, "updated_from") if updated_from else None
    to_ts = _parse_audit_time(updated_to, "updated_to") if updated_to else None
    if from_ts is not None and to_ts is not None and from_ts > to_ts:
        raise HTTPException(400, "updated_from must not be later than updated_to")
    cursor_key = _decode_cursor(cursor) if cursor else None

    items, next_key = app.state.db.list_delivery_tasks(
        subscription_id=subscription_id,
        event_id=event_id,
        status=status,
        updated_from=from_ts,
        updated_to=to_ts,
        cursor=cursor_key,
        limit=limit,
    )
    return {
        "items": [_delivery_summary(t) for t in items],
        "next_cursor": _encode_cursor(*next_key) if next_key else None,
    }


@app.get("/audit/deliveries/{task_id}", response_model=DeliveryTaskDetail)
def get_delivery(task_id: int) -> dict:
    """Full audit view of one delivery task, including every attempt."""
    data = app.state.db.get_delivery_task(task_id)
    if data is None:
        raise HTTPException(404, f"delivery task {task_id} not found")
    t = data["task"]
    history = [
        {
            "attempt_no": a["attempt_no"],
            "status_code": a["status_code"],
            "error": a["error"],
            "started_at": _iso(a["started_at"]),
            "finished_at": _iso(a["finished_at"]),
            "duration_ms": round((a["finished_at"] - a["started_at"]) * 1000),
        }
        for a in data["attempts"]
    ]
    return {
        **_delivery_summary(t),
        "max_attempts": t["max_attempts"],
        "next_attempt_at": _iso(t["next_attempt_at"]) if t["status"] == "pending" else None,
        "last_status_code": t["last_status_code"],
        "result": t["result"],
        "cancelled_at": _iso(t["cancelled_at"]),
        "created_at": _iso(t["created_at"]),
        "history": history,
    }
