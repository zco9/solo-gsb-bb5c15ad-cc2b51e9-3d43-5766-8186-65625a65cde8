"""Background delivery workers.

Each worker repeatedly claims one due task (atomically, via the DB layer),
performs the HTTP delivery, and records the outcome. Failed deliveries are
rescheduled with exponential backoff (1s, 2s, 4s) until the attempt budget
is exhausted.

Claiming is strictly ordered per subscription: a task becomes claimable
only after every earlier task (smaller id) of the same subscription has
reached a terminal state (delivered/failed/cancelled). The rule is
enforced inside the atomic claim query, so it holds across retries,
concurrent workers, and restarts, while different subscriptions are
still delivered in parallel by multiple workers.

Event cancellation (POST /events/{id}/cancel) moves still-pending tasks
to the terminal 'cancelled' state. Such tasks are never claimed, never
retried, and never recorded through finish_attempt, so they neither touch
the circuit-breaker counters nor block subsequent tasks of the same
subscription. A task already in progress when the cancel arrives runs to
completion and keeps its real result.

A per-subscription circuit breaker (persisted in the DB) stops claiming
once too many consecutive tasks have failed permanently: while 'open' no
task of that subscription is claimed; after the cooldown the earliest
pending task is claimed as the single half-open probe, closing the
breaker on success or re-opening it (with a fresh cooldown) on failure.

A per-subscription token bucket (persisted in the DB) caps the delivery
rate: every claimed task spends one token, refilled lazily at the
configured rate. A claim with no whole token available leaves the head
task pending and reschedules it to the exact computed refill instant,
inside the same atomic database critical section; bucket state is shared
by all workers and survives restarts.

A per-subscription delivery window (persisted in the DB) restricts
claims to configured weekdays/local-time ranges in an IANA timezone.
Outside the window a due head task stays pending and is rescheduled in
the same atomic critical section to the window's exact next opening
instant (DST-aware, deterministic); workers then move on to other
subscriptions. The schedule is an absolute persisted UTC epoch, so a
restart can never release a task before the opening, and wrap-past-
midnight windows as well as spring-forward/fall-back transitions follow
the real zone database. The window gate is evaluated before the token
bucket, so no tokens are spent while closed.
"""
from __future__ import annotations

import asyncio
import logging
import time

import httpx

from . import config, signing
from .db import Database

log = logging.getLogger("webhook-relay.worker")


async def worker_loop(
    worker_name: str,
    db: Database,
    client: httpx.AsyncClient,
    stop: asyncio.Event,
) -> None:
    log.info("worker %s started", worker_name)
    while not stop.is_set():
        task = db.claim_due_task(worker_name, time.time())
        if task is None:
            # No work available; sleep briefly but stay responsive to shutdown.
            try:
                await asyncio.wait_for(stop.wait(), timeout=config.POLL_INTERVAL)
            except asyncio.TimeoutError:
                pass
            continue
        await _process_task(task, db, client)
    log.info("worker %s stopped", worker_name)


async def _process_task(task: dict, db: Database, client: httpx.AsyncClient) -> None:
    task_id = task["id"]
    attempt_no = task["attempts"] + 1
    max_attempts = task["max_attempts"]

    # Referenced rows may be gone if the subscription was deleted after
    # the task was created; there is nothing deliverable in that case.
    if not task.get("target_url") or task.get("payload") is None:
        now = time.time()
        db.finish_attempt(
            task_id, attempt_no, None, "subscription or event missing",
            now, now, "failed", None,
        )
        log.warning("task %s failed: subscription or event missing", task_id)
        return

    body = task["payload"].encode("utf-8")
    now = time.time()
    headers = {
        "Content-Type": "application/json",
        "User-Agent": "local-webhook-relay/1.0",
        "X-Webhook-Event-Id": task["event_id"],
        "X-Webhook-Task-Id": str(task_id),
        "X-Webhook-Attempt": str(attempt_no),
        "X-Webhook-Timestamp": str(int(now)),
        "X-Webhook-Signature": signing.signature_header(task["secret"], body),
    }
    # During a secret-rotation grace window, additionally send the signature
    # made with the previous secret so receivers can switch over safely. The
    # window is evaluated at each actual send (secrets are re-read from the
    # DB on every claim), so retries always reflect the rotation state in
    # effect at that moment; once it expires only the new signature is sent.
    prev_secret = task.get("previous_secret")
    prev_expires = task.get("previous_secret_expires_at")
    if prev_secret and prev_expires is not None and now < prev_expires:
        headers["X-Webhook-Signature-Previous"] = signing.signature_header(prev_secret, body)

    started = time.time()
    status_code: int | None = None
    error: str | None = None
    try:
        resp = await client.post(task["target_url"], content=body, headers=headers)
        status_code = resp.status_code
        success = 200 <= status_code < 300
        if not success:
            error = f"non-2xx response: {status_code}"
    except httpx.TimeoutException:
        error = f"request timed out after {config.HTTP_TIMEOUT}s"
        success = False
    except httpx.HTTPError as exc:
        error = f"connection error: {exc}"
        success = False
    finished = time.time()

    if success:
        db.finish_attempt(task_id, attempt_no, status_code, None, started, finished, "delivered", None)
        log.info("task %s delivered on attempt %s (HTTP %s)", task_id, attempt_no, status_code)
    elif attempt_no >= max_attempts:
        db.finish_attempt(task_id, attempt_no, status_code, error, started, finished, "failed", None)
        log.warning("task %s failed permanently after %s attempts: %s", task_id, attempt_no, error)
    else:
        delay = config.retry_delay(attempt_no)
        db.finish_attempt(
            task_id, attempt_no, status_code, error, started, finished,
            "retry", finished + delay,
        )
        log.info("task %s attempt %s failed (%s); retrying in %ss", task_id, attempt_no, error, delay)
