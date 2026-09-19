"""Pydantic request/response models."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Literal

from pydantic import BaseModel, Field, field_validator, model_validator

from .delivery_window import MAX_WEEKDAY, MIN_WEEKDAY, get_zone, parse_clock

# The five states a delivery task can be in; also the legal values of the
# status filter on GET /audit/deliveries.
DeliveryStatus = Literal["pending", "in_progress", "delivered", "failed", "cancelled"]


class SubscriptionCreate(BaseModel):
    target_url: str = Field(min_length=1, max_length=2048)
    secret: str = Field(min_length=1, max_length=256)

    @field_validator("target_url")
    @classmethod
    def must_be_http(cls, v: str) -> str:
        v = v.strip()
        if not (v.startswith("http://") or v.startswith("https://")):
            raise ValueError("target_url must start with http:// or https://")
        return v


class SubscriptionOut(BaseModel):
    id: str
    target_url: str
    created_at: str
    paused: bool
    paused_at: str | None
    rotating: bool
    previous_secret_expires_at: str | None
    state: str  # circuit breaker: 'closed' | 'open' | 'half_open'
    consecutive_failures: int
    next_probe_at: str | None  # set only while the breaker is open
    # Token-bucket rate policy; nulls mean no rate limit is configured.
    rate_per_second: float | None
    burst: float | None


class RatePolicyCreate(BaseModel):
    """Body of PUT /subscriptions/{id}/rate-policy.

    rate_per_second is the sustained delivery rate (tokens added per
    second, may be fractional, e.g. 0.5 = one delivery every 2 seconds);
    burst is the token-bucket capacity and thus the maximum burst of
    deliveries that can fire back-to-back. When burst is omitted it
    defaults to one second worth of tokens (at least 1). Installing a
    policy resets the bucket to full; use DELETE to remove it.
    """

    rate_per_second: float = Field(gt=0, le=1_000_000)
    burst: float | None = Field(default=None, gt=0, le=1_000_000)

    @field_validator("burst")
    @classmethod
    def _burst_is_at_least_one(cls, v: float | None) -> float | None:
        if v is not None and v < 1.0:
            raise ValueError("burst must be at least 1")
        return v


class RatePolicyOut(BaseModel):
    """Response of GET/PUT /subscriptions/{id}/rate-policy.

    enabled is False (and the numeric fields null) only when no policy is
    configured. tokens_available is the bucket balance refilled lazily to
    the query instant; next_token_at is the earliest time a whole token is
    available (null when one already is, or while limited is False).
    """

    enabled: bool
    rate_per_second: float | None
    burst: float | None
    tokens_available: float | None
    next_token_at: str | None


class DeliveryWindowCreate(BaseModel):
    """Body of PUT /subscriptions/{id}/delivery-window.

    timezone is an IANA zone name (e.g. 'Europe/Berlin',
    'America/New_York') in which weekdays and local times are interpreted.
    weekdays uses ISO numbering (1 = Monday ... 7 = Sunday) and must be a
    non-empty list of distinct days. local_start/local_end are local wall
    clock times in HH:MM or HH:MM:SS form; the window is the half-open
    interval [local_start, local_end). A window whose end is not later than
    its start wraps past midnight (e.g. 22:00-06:00); equal start and end
    is rejected. Daylight-saving transitions follow the real IANA database.
    """

    timezone: str = Field(min_length=1, max_length=64)
    weekdays: list[int] = Field(min_length=1, max_length=7)
    local_start: str = Field(min_length=5, max_length=8)
    local_end: str = Field(min_length=5, max_length=8)

    @field_validator("timezone")
    @classmethod
    def _valid_zone(cls, v: str) -> str:
        name = v.strip()
        get_zone(name)  # raises ValueError (-> 422) for unknown IANA names
        return name

    @field_validator("weekdays")
    @classmethod
    def _valid_weekdays(cls, values: list[int]) -> list[int]:
        if any(isinstance(v, bool) or not isinstance(v, int) for v in values):
            raise ValueError("weekdays must be integers")
        if any(v < MIN_WEEKDAY or v > MAX_WEEKDAY for v in values):
            raise ValueError("weekdays must be between 1 (Monday) and 7 (Sunday)")
        if len(set(values)) != len(values):
            raise ValueError("weekdays must not contain duplicates")
        return values

    @field_validator("local_start", "local_end")
    @classmethod
    def _valid_clock(cls, v: str) -> str:
        parse_clock(v)  # raises ValueError (-> 422) on bad format/range
        return v.strip()

    @model_validator(mode="after")
    def _start_end_distinct(self) -> "DeliveryWindowCreate":
        if parse_clock(self.local_start) == parse_clock(self.local_end):
            raise ValueError(
                "local_start and local_end must differ; a window ending at "
                "00:00:00 (or earlier than its start) wraps past midnight"
            )
        return self


class DeliveryWindowOut(BaseModel):
    """Response of GET/PUT /subscriptions/{id}/delivery-window.

    enabled is False (with the configuration fields null) only when no
    window is configured. Times are echoed canonically as HH:MM:SS.
    is_open_now and next_open_at are computed live from the IANA zone at
    the response instant (the stored configuration itself is never
    rewritten by a query); next_open_at is null while the window is open.
    """

    enabled: bool
    timezone: str | None
    weekdays: list[int] | None
    local_start: str | None
    local_end: str | None
    wraps_past_midnight: bool | None
    is_open_now: bool
    next_open_at: str | None
    updated_at: str | None


class SecretRotationCreate(BaseModel):
    """Body of POST /subscriptions/{id}/rotate-secret.

    grace_seconds bounds how long deliveries are dual-signed with both the
    new and the previous secret (0 = switch immediately, max 24h).
    """

    new_secret: str = Field(min_length=1, max_length=256)
    grace_seconds: float = Field(ge=0, le=86400)


class EventCreate(BaseModel):
    event_id: str = Field(min_length=1, max_length=128)
    subscription_ids: list[str] = Field(min_length=1, max_length=100)
    payload: dict[str, Any]
    # Optional scheduled delivery time. When omitted the event's tasks are
    # due immediately; when set it must be an explicit, timezone-aware
    # ISO-8601 timestamp in the future (a 'Z' suffix is accepted).
    deliver_at: datetime | None = Field(default=None)

    @field_validator("deliver_at", mode="before")
    @classmethod
    def _validate_deliver_at(cls, v: Any) -> datetime | None:
        if v is None:
            return None
        if isinstance(v, datetime):
            dt = v
        elif isinstance(v, str):
            text = v.strip()
            try:
                # fromisoformat accepts 'Z' on Python 3.11+.
                dt = datetime.fromisoformat(text)
            except ValueError:
                raise ValueError("deliver_at must be an ISO-8601 timestamp")
        else:
            raise ValueError("deliver_at must be an ISO-8601 timestamp string")
        # An ISO-8601 date without a time (e.g. "2026-09-20") parses to a
        # naive midnight datetime; without an explicit timezone/offset the
        # instant is ambiguous, so it is rejected.
        if dt.tzinfo is None or dt.tzinfo.utcoffset(dt) is None:
            raise ValueError("deliver_at must include an explicit timezone, e.g. +00:00 or 'Z'")
        if dt <= datetime.now(timezone.utc):
            raise ValueError("deliver_at must be a time in the future")
        return dt


class IngestEvent(BaseModel):
    """Body of an HMAC-protected inbound event on POST /ingest.

    The whole body (including any extra fields) is forwarded verbatim to
    targets; only subscription_ids is consumed by the relay itself.
    """

    model_config = {"extra": "allow"}

    subscription_ids: list[str] = Field(min_length=1, max_length=100)


class TaskRef(BaseModel):
    task_id: int
    subscription_id: str
    status: str
    # Scheduled delivery time (ISO time); null = immediate. Has a default so
    # receipts persisted by older versions (which lack the field) still
    # validate when an idempotent replay returns the stored receipt.
    deliver_at: str | None = None


class EventAccepted(BaseModel):
    event_id: str
    created: list[TaskRef]
    duplicates: list[TaskRef]


class AttemptOut(BaseModel):
    attempt_no: int
    status_code: int | None
    error: str | None
    started_at: str
    finished_at: str
    duration_ms: int


class TaskStatus(BaseModel):
    task_id: int
    subscription_id: str
    target_url: str | None
    status: str  # 'pending' | 'in_progress' | 'delivered' | 'failed' | 'cancelled'
    attempts: int
    max_attempts: int
    deliver_at: str | None  # scheduled delivery time requested at submit (null = immediate)
    next_attempt_at: str | None
    last_status_code: int | None
    last_error: str | None
    result: str | None
    cancelled_at: str | None  # set only when the task was cancelled with its event
    history: list[AttemptOut]


class EventStatusCounts(BaseModel):
    """Per-status task totals for an event (all counts always present)."""

    pending: int
    in_progress: int
    delivered: int
    failed: int
    cancelled: int


class EventStatus(BaseModel):
    event_id: str
    created_at: str
    cancelled: bool  # the event was cancelled at least once (one or more tasks are 'cancelled')
    cancelled_at: str | None  # when the event's last cancel ran (ISO time, null if never)
    counts: EventStatusCounts
    targets: list[TaskStatus]


class EventCancelResult(BaseModel):
    """Result of POST /events/{event_id}/cancel.

    'cancelled' is the number of tasks moved to 'cancelled' by this call
    (0 on a repeated, idempotent call); 'counts' reports the resulting
    per-status totals, so callers can see how many tasks were already in
    flight or finished at cancel time.
    """

    event_id: str
    cancelled: int
    cancelled_at: str
    counts: EventStatusCounts


class DeliveryTaskSummary(BaseModel):
    """One delivery task in the audit listing (GET /audit/deliveries)."""

    task_id: int
    event_id: str
    subscription_id: str
    target_url: str | None  # null when the subscription was deleted
    status: str  # 'pending' | 'in_progress' | 'delivered' | 'failed' | 'cancelled'
    attempts: int
    last_error: str | None
    deliver_at: str | None  # scheduled delivery time (ISO time, null = immediate)
    updated_at: str  # last state change; the listing is ordered by it, newest first


class DeliveryTaskPage(BaseModel):
    """One page of the audit listing.

    next_cursor is the opaque cursor for the following page (pass it back as
    ?cursor=...); it is null when this page is the last one.
    """

    items: list[DeliveryTaskSummary]
    next_cursor: str | None


class DeliveryTaskDetail(DeliveryTaskSummary):
    """Full audit view of one delivery task (GET /audit/deliveries/{task_id})."""

    max_attempts: int
    next_attempt_at: str | None  # set only while the task is pending
    last_status_code: int | None
    result: str | None
    cancelled_at: str | None  # set only when the task was cancelled with its event
    created_at: str
    history: list[AttemptOut]


class TopicsUpdate(BaseModel):
    """Body of PUT /subscriptions/{id}/topics.

    The list replaces (not merges) the subscription's deduplicated topic
    set; an empty list removes the subscription from every topic. Names are
    trimmed and must stay within 1..128 characters after trimming; at most
    100 distinct topics per subscription are allowed.
    """

    topics: list[str] = Field(max_length=100)

    @field_validator("topics")
    @classmethod
    def _normalize_topics(cls, values: list[str]) -> list[str]:
        normalized: list[str] = []
        for v in values:
            if not isinstance(v, str):
                raise ValueError("topics must be strings")
            name = v.strip()
            if not name:
                raise ValueError("topic names must not be empty")
            if len(name) > 128:
                raise ValueError("topic names must be at most 128 characters")
            normalized.append(name)
        if len(set(normalized)) > 100:
            raise ValueError("at most 100 distinct topics per subscription")
        return normalized


class TopicsOut(BaseModel):
    """Response of GET/PUT /subscriptions/{id}/topics (sorted, deduplicated)."""

    subscription_id: str
    topics: list[str]


class TopicEventCreate(BaseModel):
    """Body of POST /topic-events.

    The idempotency key is the triple (event_id, topic, payload): replaying
    the same triple returns the first publication's recipients and task
    results even if the topic's membership has since changed. The same
    event_id with a different topic or payload is rejected with 409.
    """

    event_id: str = Field(min_length=1, max_length=128)
    topic: str = Field(min_length=1, max_length=128)
    payload: dict[str, Any]


class TopicEventAccepted(BaseModel):
    """Result of POST /topic-events.

    recipients is the membership snapshotted at first publication with one
    ordinary delivery task each; an exact replay returns the very same list
    (same task ids/statuses) and never creates additional tasks.
    """

    event_id: str
    topic: str
    payload: dict[str, Any]
    replayed: bool  # False on first publication (201), True on an exact replay (200)
    recipients: list[TaskRef]
