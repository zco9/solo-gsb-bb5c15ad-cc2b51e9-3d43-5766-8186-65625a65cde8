"""SQLite persistence layer.

A single connection guarded by a lock serializes all access, which keeps the
claim/update sequences atomic for the in-process workers. WAL mode is enabled
so readers are not blocked while a worker writes attempt results.
"""
from __future__ import annotations

import json
import sqlite3
import threading
import time
import uuid
from contextlib import contextmanager

from .delivery_window import DeliveryWindow, get_zone

SCHEMA = """
CREATE TABLE IF NOT EXISTS subscriptions (
    id          TEXT PRIMARY KEY,
    target_url  TEXT NOT NULL,
    secret      TEXT NOT NULL,
    created_at  REAL NOT NULL,
    paused_at   REAL,                   -- epoch seconds while paused; NULL when active
    previous_secret            TEXT,    -- retired secret, dual-signed during the grace window
    previous_secret_expires_at REAL,    -- epoch seconds when the previous secret stops being sent
    breaker_state                TEXT NOT NULL DEFAULT 'closed'
                                 CHECK (breaker_state IN ('closed', 'open', 'half_open')),
    breaker_consecutive_failures INTEGER NOT NULL DEFAULT 0,
    breaker_next_probe_at        REAL,  -- epoch seconds when an open breaker allows its probe
    breaker_probe_task_id        INTEGER, -- the single half-open probe task; NULL unless half_open
    rate_capacity          REAL,        -- token-bucket size (max burst); NULL = rate limiting off
    rate_refill_per_second REAL,        -- tokens added per second (sustained delivery rate)
    rate_tokens            REAL,        -- persisted balance; refilled lazily by wall clock
    rate_updated_at        REAL,        -- epoch seconds the balance was last persisted
    window_timezone        TEXT,        -- delivery window: IANA zone; NULL when no window configured
    window_weekdays        TEXT,        -- delivery window: JSON array of ISO weekdays 1..7
    window_local_start     INTEGER,     -- delivery window: local seconds after midnight
    window_local_end       INTEGER,     -- delivery window: local seconds; <= start = wraps midnight
    window_updated_at      REAL         -- epoch seconds the window was last installed/replaced
);

CREATE TABLE IF NOT EXISTS events (
    id          TEXT PRIMARY KEY,
    payload     TEXT NOT NULL,          -- raw JSON body delivered to targets
    created_at  REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS tasks (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id         TEXT NOT NULL REFERENCES events(id),
    subscription_id  TEXT NOT NULL,
    status           TEXT NOT NULL DEFAULT 'pending'
                     CHECK (status IN
                       ('pending', 'in_progress', 'delivered', 'failed', 'cancelled')),
    attempts         INTEGER NOT NULL DEFAULT 0,
    max_attempts     INTEGER NOT NULL,
    deliver_at       REAL,              -- requested scheduled time (epoch seconds); NULL = immediate
    next_attempt_at  REAL,              -- epoch seconds; NULL once finished
    claimed_by       TEXT,
    last_status_code INTEGER,
    last_error       TEXT,
    result           TEXT CHECK (result IN ('delivered', 'failed')),
    created_at       REAL NOT NULL,
    updated_at       REAL NOT NULL,
    cancelled_at     REAL,              -- epoch seconds when an event cancel claimed the task
    UNIQUE (event_id, subscription_id)  -- one task per (event, subscription)
);

CREATE TABLE IF NOT EXISTS attempts (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id     INTEGER NOT NULL REFERENCES tasks(id),
    attempt_no  INTEGER NOT NULL,
    status_code INTEGER,                -- NULL when the request never got a response
    error       TEXT,
    started_at  REAL NOT NULL,
    finished_at REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS ingested_events (
    id          TEXT PRIMARY KEY,       -- X-Webhook-Id of a verified inbound event
    raw_body    BLOB NOT NULL,          -- exact original JSON bytes, for conflict checks
    receipt     TEXT NOT NULL,          -- JSON acceptance response returned first time
    created_at  REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS subscription_topics (
    subscription_id TEXT NOT NULL,      -- live routing set; memberships are removed
    topic           TEXT NOT NULL,      -- together with the subscription on delete
    created_at      REAL NOT NULL,      -- (delivery task history is still retained)
    PRIMARY KEY (subscription_id, topic)
);

CREATE TABLE IF NOT EXISTS topic_events (
    id          TEXT PRIMARY KEY,       -- shares the global event id namespace
    topic       TEXT NOT NULL,          -- topic snapshotted at first publication
    payload     TEXT NOT NULL,          -- canonical JSON, compared verbatim on replay
    receipt     TEXT NOT NULL,          -- JSON acceptance (snapshot recipients + tasks)
    created_at  REAL NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_tasks_due ON tasks (status, next_attempt_at);
CREATE INDEX IF NOT EXISTS idx_tasks_sub_order ON tasks (subscription_id, id, status);
CREATE INDEX IF NOT EXISTS idx_attempts_task ON attempts (task_id);
CREATE INDEX IF NOT EXISTS idx_subscription_topics_topic ON subscription_topics (topic);
"""

# Columns returned to the API layer for a subscription (never secret material).
_SUB_COLUMNS = (
    "id, target_url, created_at, paused_at, previous_secret_expires_at, "
    "breaker_state, breaker_consecutive_failures, breaker_next_probe_at, "
    "rate_capacity, rate_refill_per_second, rate_tokens, rate_updated_at, "
    "window_timezone, window_weekdays, window_local_start, window_local_end, "
    "window_updated_at"
)

# Shared projection for reading a subscription's delivery-window columns.
# Both the PUT (re-read after the write) and the GET paths SELECT these exact
# columns through _window_row_to_policy, so write and read serialize one
# identical, persisted configuration.
_WINDOW_COLUMNS = (
    "window_timezone, window_weekdays, window_local_start, "
    "window_local_end, window_updated_at"
)


def _window_row_to_policy(row) -> dict:
    """Map the _WINDOW_COLUMNS projection of one subscription row to the
    canonical delivery-window policy dict shared by reads and writes.

    A row whose window columns are all NULL (never configured) maps to
    configured=False with None fields; callers handle a missing row
    (subscription id does not exist) by returning None themselves.
    """
    if row["window_timezone"] is None:
        return {
            "configured": False,
            "timezone": None,
            "weekdays": None,
            "local_start": None,
            "local_end": None,
            "updated_at": None,
        }
    return {
        "configured": True,
        "timezone": row["window_timezone"],
        "weekdays": json.loads(row["window_weekdays"]),
        "local_start": row["window_local_start"],
        "local_end": row["window_local_end"],
        "updated_at": row["window_updated_at"],
    }

# Token accounting tolerance; avoids rescheduling by microseconds due to
# floating-point round-off when a token is effectively already available.
_TOKEN_EPS = 1e-9

# Length of every subscription id; the same generator is the single source of
# truth for creation, lookup and every delivery-window/rate-policy/topic path,
# so the id POST /subscriptions hands out is exactly the key those later
# writes and reads look up (no trimming, case folding or other reshaping).
_SUBSCRIPTION_ID_LEN = 12


def new_subscription_id() -> str:
    """Mint a fresh subscription id (12 lowercase hex chars, unique primary key).

    Always returns a plain ``str`` of exactly ``_SUBSCRIPTION_ID_LEN``
    characters; it is used verbatim for both the INSERT and the value
    returned to the API, which makes the 'create then immediately configure
    delivery window' sequence use one identical identifier end to end.
    """
    return uuid.uuid4().hex[:_SUBSCRIPTION_ID_LEN]


def _refill_tokens(
    capacity: float,
    refill_per_second: float,
    tokens: float | None,
    updated_at: float | None,
    now: float,
) -> tuple[float, float]:
    """Lazy token-bucket refill against the wall clock.

    Returns (balance, balance_timestamp). Rows created before the rate
    columns existed (or before a policy was installed) have no balance
    state; they start with a full bucket stamped at 'now'. Otherwise the
    balance grows linearly at refill_per_second, capped at capacity.
    """
    if tokens is None or updated_at is None:
        return capacity, now
    if now <= updated_at:
        return tokens, updated_at
    balance = min(capacity, tokens + (now - updated_at) * refill_per_second)
    return balance, now


def _window_from_row(row) -> DeliveryWindow | None:
    """Build the immutable DeliveryWindow described by a subscription row.

    Returns None when no window is configured (any window column NULL).
    A row whose stored configuration cannot be interpreted (e.g. a tzdb that
    lost the zone) is treated as 'no window' rather than wedging claiming;
    configuration is validated when written, so this only guards corruption.
    """
    tz = row["window_timezone"]
    raw_weekdays = row["window_weekdays"]
    start = row["window_local_start"]
    end = row["window_local_end"]
    if tz is None or raw_weekdays is None or start is None or end is None:
        return None
    try:
        weekdays = tuple(sorted(json.loads(raw_weekdays)))
        get_zone(tz)  # fail fast if the zone database cannot resolve it
        return DeliveryWindow(tz, weekdays, int(start), int(end))
    except (ValueError, TypeError, json.JSONDecodeError):
        return None


class Database:
    def __init__(
        self,
        path: str,
        breaker_failure_threshold: int = 5,
        breaker_cooldown_seconds: float = 30.0,
    ):
        self._breaker_threshold = breaker_failure_threshold
        self._breaker_cooldown = breaker_cooldown_seconds
        # isolation_level=None puts the connection in SQLite autocommit mode:
        # the driver never opens an implicit transaction behind our back, so
        # *every* transaction boundary is one we state explicitly
        # (_immediate() / claim_due_task()). Two guarantees follow from this:
        #
        # 1. Transaction visibility is uniform across subscription creation,
        #    lookup and delivery-window read/write. A write committed by
        #    create_subscription() is visible to the very next read on this
        #    connection -- a read-only SELECT cannot stay pinned inside an
        #    implicitly-opened snapshot that predates the INSERT, so a
        #    freshly returned id is never mistaken for 'not found' by a
        #    subsequent set_delivery_window()/get_delivery_window().
        # 2. Explicit 'BEGIN IMMEDIATE' can never collide with an implicit
        #    transaction the driver started for a previous statement, which
        #    makes the BEGIN IMMEDIATE / BEGIN / COMMIT / ROLLBACK statements
        #    in this module behave identically on every sqlite3/Python build
        #    (the legacy transaction mode changed semantics across CPython
        #    3.12.x patch releases).
        self._conn = sqlite3.connect(
            path, check_same_thread=False, isolation_level=None
        )
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA foreign_keys=ON")
        # When several worker *processes* share the database file, an
        # IMMEDIATE write lock can momentarily be held by another connection;
        # wait for it instead of failing instantly with SQLITE_BUSY.
        self._conn.execute("PRAGMA busy_timeout=5000")
        self._lock = threading.Lock()
        with self._lock:
            # In autocommit mode each CREATE TABLE IF NOT EXISTS statement is
            # a one-statement transaction on its own; the migration's ALTERs
            # run the same way, and the explicit table rebuild keeps its own
            # BEGIN/COMMIT. The trailing commit is a harmless no-op kept for
            # symmetry with the old implicit-transaction lifecycle.
            self._conn.executescript(SCHEMA)
            self._migrate()
            self._conn.commit()

    @contextmanager
    def _immediate(self):
        """Open a write transaction that takes the database write lock at once.

        BEGIN IMMEDIATE acquires SQLite's reserved (then pending) lock before
        any row is read, so the SELECT-then-UPDATE claim sequence is
        serialized not only by the in-process threading lock but also across
        separate connections/processes sharing the file: a second claim waits
        on busy_timeout until the first commits and then reads the updated
        state, never double-spending a token or claiming the same task.
        """
        self._conn.execute("BEGIN IMMEDIATE")
        try:
            yield
        except BaseException:
            self._conn.execute("ROLLBACK")
            raise
        else:
            self._conn.commit()

    def _migrate(self) -> None:
        """Bring databases created by older versions up to date.

        CREATE TABLE IF NOT EXISTS leaves pre-existing tables untouched, so
        columns added later are applied here via ALTER TABLE. The tasks
        table additionally needs a widened status CHECK to accept the
        'cancelled' terminal state; SQLite cannot ALTER a CHECK constraint,
        so an old tasks table is rebuilt (foreign_keys off, inside a single
        transaction, per the SQLite recommended table-rebuild procedure).
        """
        cols = {r["name"] for r in self._conn.execute("PRAGMA table_info(subscriptions)")}
        if "paused_at" not in cols:
            self._conn.execute("ALTER TABLE subscriptions ADD COLUMN paused_at REAL")
        if "previous_secret" not in cols:
            self._conn.execute("ALTER TABLE subscriptions ADD COLUMN previous_secret TEXT")
        if "previous_secret_expires_at" not in cols:
            self._conn.execute(
                "ALTER TABLE subscriptions ADD COLUMN previous_secret_expires_at REAL"
            )
        if "breaker_state" not in cols:
            self._conn.execute(
                "ALTER TABLE subscriptions ADD COLUMN breaker_state TEXT NOT NULL DEFAULT 'closed'"
            )
        if "breaker_consecutive_failures" not in cols:
            self._conn.execute(
                "ALTER TABLE subscriptions ADD COLUMN breaker_consecutive_failures "
                "INTEGER NOT NULL DEFAULT 0"
            )
        if "breaker_next_probe_at" not in cols:
            self._conn.execute(
                "ALTER TABLE subscriptions ADD COLUMN breaker_next_probe_at REAL"
            )
        if "breaker_probe_task_id" not in cols:
            self._conn.execute(
                "ALTER TABLE subscriptions ADD COLUMN breaker_probe_task_id INTEGER"
            )
        if "rate_capacity" not in cols:
            self._conn.execute(
                "ALTER TABLE subscriptions ADD COLUMN rate_capacity REAL"
            )
        if "rate_refill_per_second" not in cols:
            self._conn.execute(
                "ALTER TABLE subscriptions ADD COLUMN rate_refill_per_second REAL"
            )
        if "rate_tokens" not in cols:
            self._conn.execute(
                "ALTER TABLE subscriptions ADD COLUMN rate_tokens REAL"
            )
        if "rate_updated_at" not in cols:
            self._conn.execute(
                "ALTER TABLE subscriptions ADD COLUMN rate_updated_at REAL"
            )
        if "window_timezone" not in cols:
            self._conn.execute(
                "ALTER TABLE subscriptions ADD COLUMN window_timezone TEXT"
            )
        if "window_weekdays" not in cols:
            self._conn.execute(
                "ALTER TABLE subscriptions ADD COLUMN window_weekdays TEXT"
            )
        if "window_local_start" not in cols:
            self._conn.execute(
                "ALTER TABLE subscriptions ADD COLUMN window_local_start INTEGER"
            )
        if "window_local_end" not in cols:
            self._conn.execute(
                "ALTER TABLE subscriptions ADD COLUMN window_local_end INTEGER"
            )
        if "window_updated_at" not in cols:
            self._conn.execute(
                "ALTER TABLE subscriptions ADD COLUMN window_updated_at REAL"
            )

        task_cols = {r["name"] for r in self._conn.execute("PRAGMA table_info(tasks)")}
        if "cancelled_at" not in task_cols:
            # Commit the subscription ALTERs first; the rebuild then runs as
            # its own explicit transaction. executescript() is avoided on
            # purpose: it silently COMMITs any open transaction first.
            self._conn.commit()
            self._conn.execute("PRAGMA foreign_keys=OFF")
            try:
                self._conn.execute("BEGIN")
                self._conn.execute(
                    """
                    CREATE TABLE tasks_new (
                        id               INTEGER PRIMARY KEY AUTOINCREMENT,
                        event_id         TEXT NOT NULL REFERENCES events(id),
                        subscription_id  TEXT NOT NULL,
                        status           TEXT NOT NULL DEFAULT 'pending'
                                         CHECK (status IN
                                           ('pending', 'in_progress', 'delivered',
                                            'failed', 'cancelled')),
                        attempts         INTEGER NOT NULL DEFAULT 0,
                        max_attempts     INTEGER NOT NULL,
                        deliver_at       REAL,
                        next_attempt_at  REAL,
                        claimed_by       TEXT,
                        last_status_code INTEGER,
                        last_error       TEXT,
                        result           TEXT CHECK (result IN ('delivered', 'failed')),
                        created_at       REAL NOT NULL,
                        updated_at       REAL NOT NULL,
                        cancelled_at     REAL,
                        UNIQUE (event_id, subscription_id)
                    )
                    """
                )
                self._conn.execute(
                    """
                    INSERT INTO tasks_new
                        (id, event_id, subscription_id, status, attempts, max_attempts,
                         deliver_at, next_attempt_at, claimed_by, last_status_code, last_error,
                         result, created_at, updated_at, cancelled_at)
                    SELECT id, event_id, subscription_id, status, attempts, max_attempts,
                           NULL, next_attempt_at, claimed_by, last_status_code, last_error,
                           result, created_at, updated_at, NULL
                    FROM tasks
                    """
                )
                self._conn.execute("DROP TABLE tasks")
                self._conn.execute("ALTER TABLE tasks_new RENAME TO tasks")
                self._conn.execute(
                    "CREATE INDEX idx_tasks_due ON tasks (status, next_attempt_at)"
                )
                self._conn.execute(
                    "CREATE INDEX idx_tasks_sub_order ON tasks (subscription_id, id, status)"
                )
                self._conn.execute("COMMIT")
            except Exception:
                self._conn.execute("ROLLBACK")
                raise
            finally:
                # Re-enable foreign keys while outside the transaction; the
                # constructor's ON setting is what every new connection gets.
                self._conn.execute("PRAGMA foreign_keys=ON")
            # The rebuild above already has the column; refresh the set so
            # the ALTER below is skipped for this database.
            task_cols = {r["name"] for r in self._conn.execute("PRAGMA table_info(tasks)")}

        if "deliver_at" not in task_cols:
            # A database created by a version that already had 'cancelled'
            # but predates scheduled delivery: just add the column. Existing
            # rows get NULL, i.e. their tasks were immediate submissions.
            self._conn.execute("ALTER TABLE tasks ADD COLUMN deliver_at REAL")

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    # ------------------------------------------------------------------
    # subscriptions
    # ------------------------------------------------------------------
    def create_subscription(self, target_url: str, secret: str) -> dict:
        """Insert a subscription and return its persisted row.

        The row handed back is read straight back out of the committing
        transaction via the same column projection every lookup uses
        (_SUB_COLUMNS), so the returned id and the id later resolved by
        get_subscription()/set_delivery_window()/get_delivery_window() are
        guaranteed to be the same committed identity -- a caller that
        immediately configures a delivery window with the returned id cannot
        hit a 'not found' from a stale or diverged snapshot.
        """
        sub_id = new_subscription_id()
        now = time.time()
        with self._lock, self._immediate():
            self._conn.execute(
                "INSERT INTO subscriptions (id, target_url, secret, created_at) VALUES (?, ?, ?, ?)",
                (sub_id, target_url, secret, now),
            )
            # Re-read within the same transaction so the returned view is the
            # authoritative persisted row (same identity/columns as every
            # later read) rather than a hand-assembled dict that could drift.
            row = self._conn.execute(
                f"SELECT {_SUB_COLUMNS} FROM subscriptions WHERE id = ?",
                (sub_id,),
            ).fetchone()
        return dict(row)

    def list_subscriptions(self) -> list[dict]:
        with self._lock:
            rows = self._conn.execute(
                f"SELECT {_SUB_COLUMNS} FROM subscriptions ORDER BY created_at"
            ).fetchall()
        return [dict(r) for r in rows]

    def get_subscription(self, sub_id: str) -> dict | None:
        with self._lock:
            row = self._conn.execute(
                f"SELECT {_SUB_COLUMNS} FROM subscriptions WHERE id = ?",
                (sub_id,),
            ).fetchone()
        return dict(row) if row else None

    def set_subscription_paused(self, sub_id: str, paused: bool) -> dict | None:
        """Pause or resume a subscription, idempotently.

        Only the paused_at flag on the subscription row is flipped; task
        rows are never touched, so every pending task keeps its original
        next_attempt_at schedule and simply becomes claimable again once
        the subscription is resumed. A repeated pause keeps the original
        paused_at timestamp. Returns the updated row, or None when the
        subscription does not exist.
        """
        with self._lock, self._immediate():
            row = self._conn.execute(
                "SELECT paused_at FROM subscriptions WHERE id = ?", (sub_id,)
            ).fetchone()
            if row is None:
                return None
            if paused and row["paused_at"] is None:
                self._conn.execute(
                    "UPDATE subscriptions SET paused_at = ? WHERE id = ?",
                    (time.time(), sub_id),
                )
            elif not paused and row["paused_at"] is not None:
                self._conn.execute(
                    "UPDATE subscriptions SET paused_at = NULL WHERE id = ?", (sub_id,)
                )
            row = self._conn.execute(
                f"SELECT {_SUB_COLUMNS} FROM subscriptions WHERE id = ?",
                (sub_id,),
            ).fetchone()
        return dict(row)

    def rotate_secret(self, sub_id: str, new_secret: str, grace_seconds: float) -> tuple[str, dict | None]:
        """Rotate a subscription's delivery-signing secret, atomically.

        The current secret becomes the previous secret and keeps being sent
        (as X-Webhook-Signature-Previous) until now + grace_seconds; with
        grace_seconds == 0 the switch is immediate and no previous secret is
        kept. While an earlier rotation's grace window is still open the
        rotation is refused ('in_grace'); once that window has expired the
        retired secret is silently discarded by the next rotation.

        previous_secret and previous_secret_expires_at are always set or
        cleared together, so readers can rely on the timestamp alone.

        Returns ("rotated", row) | ("not_found", None) | ("in_grace", row);
        the row never contains secret material.
        """
        now = time.time()
        with self._lock, self._immediate():
            row = self._conn.execute(
                "SELECT secret, previous_secret_expires_at FROM subscriptions WHERE id = ?",
                (sub_id,),
            ).fetchone()
            if row is None:
                return "not_found", None
            expires = row["previous_secret_expires_at"]
            if expires is not None and expires > now:
                status = "in_grace"
            else:
                if grace_seconds > 0:
                    previous, previous_expires = row["secret"], now + grace_seconds
                else:
                    previous, previous_expires = None, None
                self._conn.execute(
                    """UPDATE subscriptions
                       SET secret = ?, previous_secret = ?, previous_secret_expires_at = ?
                       WHERE id = ?""",
                    (new_secret, previous, previous_expires, sub_id),
                )
                status = "rotated"
            row = self._conn.execute(
                f"SELECT {_SUB_COLUMNS} FROM subscriptions WHERE id = ?",
                (sub_id,),
            ).fetchone()
        return status, dict(row)

    # ------------------------------------------------------------------
    # per-subscription token-bucket rate policy
    # ------------------------------------------------------------------
    def set_rate_policy(
        self, sub_id: str, capacity: float, refill_per_second: float
    ) -> dict | None:
        """Install/replace a subscription's rate policy, atomically.

        The bucket is reset full (rate_tokens = capacity) with the balance
        timestamp set to now, so the new policy starts with its whole burst
        available. Existing task schedules are never rewritten: pending
        tasks simply run into the new bucket state on their next claim
        attempt (a head task whose next_attempt_at is due but whose bucket
        is empty gets rescheduled to the exact refill instant). Returns the
        resulting policy state, or None when the subscription does not exist.
        """
        now = time.time()
        with self._lock, self._immediate():
            row = self._conn.execute(
                "SELECT 1 FROM subscriptions WHERE id = ?", (sub_id,)
            ).fetchone()
            if row is None:
                return None
            self._conn.execute(
                """UPDATE subscriptions
                      SET rate_capacity = ?, rate_refill_per_second = ?,
                          rate_tokens = ?, rate_updated_at = ?
                    WHERE id = ?""",
                (capacity, refill_per_second, capacity, now, sub_id),
            )
            row = self._conn.execute(
                f"SELECT {_SUB_COLUMNS} FROM subscriptions WHERE id = ?",
                (sub_id,),
            ).fetchone()
        return dict(row)

    def get_rate_policy(self, sub_id: str) -> dict | None:
        """Return the subscription's rate policy with its live bucket state.

        The stored balance is refilled lazily against the wall clock for
        display (the persisted row is not modified): tokens = min(capacity,
        stored + refill * elapsed), and next_token_at is the earliest time at
        which one whole token will be available (None when already
        available, or when no policy is configured). Returns None when the
        subscription does not exist; enabled fields are None while limited
        is False.
        """
        now = time.time()
        with self._lock:
            row = self._conn.execute(
                f"SELECT {_SUB_COLUMNS} FROM subscriptions WHERE id = ?",
                (sub_id,),
            ).fetchone()
        if row is None:
            return None
        sub = dict(row)
        capacity = sub["rate_capacity"]
        refill = sub["rate_refill_per_second"]
        if capacity is None or refill is None:
            return {
                "rate_per_second": None,
                "burst": None,
                "tokens_available": None,
                "next_token_at": None,
            }
        tokens, _ = _refill_tokens(
            capacity, refill, sub["rate_tokens"], sub["rate_updated_at"], now
        )
        if tokens >= 1.0:
            next_token_at = None
        else:
            next_token_at = now + (1.0 - tokens) / refill
        return {
            "rate_per_second": refill,
            "burst": capacity,
            "tokens_available": tokens,
            "next_token_at": next_token_at,
        }

    def delete_rate_policy(self, sub_id: str) -> bool:
        """Remove a subscription's rate policy (rate limiting off).

        The bucket columns are cleared; task rows are not touched, so any
        task previously rescheduled for a refill wakes as scheduled (or
        sooner if another path already moved it) and then claims without a
        token check. Returns False when the subscription does not exist.
        """
        with self._lock, self._immediate():
            cur = self._conn.execute(
                """UPDATE subscriptions
                      SET rate_capacity = NULL, rate_refill_per_second = NULL,
                          rate_tokens = NULL, rate_updated_at = NULL
                    WHERE id = ? AND rate_capacity IS NOT NULL""",
                (sub_id,),
            )
        return cur.rowcount > 0

    # ------------------------------------------------------------------
    # per-subscription delivery-window policy
    # ------------------------------------------------------------------
    def set_delivery_window(
        self,
        sub_id: str,
        timezone: str,
        weekdays: list[int],
        local_start: int,
        local_end: int,
    ) -> dict | None:
        """Install/replace a subscription's delivery window, atomically.

        The configuration is validated by the API layer (real IANA zone,
        non-empty deduplicated weekday set, in-range distinct clock times);
        here it is persisted verbatim with an installation timestamp. Task
        rows are never rewritten: like the rate policy, pending tasks simply
        meet the new window on their next claim attempt -- a due head task
        found outside the window is deterministically pushed to the next
        opening inside the claim's critical section, and a task scheduled
        further out is judged by the new window when it wakes. Returns the
        stored window row fields, or None when the subscription does not
        exist.
        """
        now = time.time()
        ordered = sorted(set(weekdays))
        encoded = json.dumps(ordered, separators=(",", ":"))
        with self._lock, self._immediate():
            # Same identity check every subscription write uses; the id here
            # is the exact string minted by create_subscription and returned
            # by POST /subscriptions.
            exists = self._conn.execute(
                "SELECT 1 FROM subscriptions WHERE id = ?", (sub_id,)
            ).fetchone()
            if exists is None:
                return None
            self._conn.execute(
                """UPDATE subscriptions
                      SET window_timezone = ?, window_weekdays = ?,
                          window_local_start = ?, window_local_end = ?,
                          window_updated_at = ?
                    WHERE id = ?""",
                (timezone, encoded, local_start, local_end, now, sub_id),
            )
            # Return the row exactly as persisted (re-read inside the same
            # transaction, identical projection to get_delivery_window), so
            # the PUT response is the committed configuration and needs no
            # second lookup that could observe a different view.
            row = self._conn.execute(
                f"""SELECT {_WINDOW_COLUMNS}
                      FROM subscriptions WHERE id = ?""",
                (sub_id,),
            ).fetchone()
        return _window_row_to_policy(row)

    def get_delivery_window(self, sub_id: str) -> dict | None:
        """Return a subscription's stored delivery-window configuration.

        Returns None when the subscription does not exist. A subscription
        without a window returns a dict whose 'configured' flag is False and
        whose other fields are None.
        """
        with self._lock:
            row = self._conn.execute(
                f"""SELECT {_WINDOW_COLUMNS}
                      FROM subscriptions WHERE id = ?""",
                (sub_id,),
            ).fetchone()
        if row is None:
            return None
        return _window_row_to_policy(row)

    def delete_delivery_window(self, sub_id: str) -> bool:
        """Remove a subscription's delivery window (deliveries unrestricted).

        The window columns are cleared; task rows are not touched, so tasks
        previously pushed to a future opening simply wake as scheduled and
        then claim without a window check. Returns False when the
        subscription does not exist or had no window configured.
        """
        with self._lock, self._immediate():
            cur = self._conn.execute(
                """UPDATE subscriptions
                      SET window_timezone = NULL, window_weekdays = NULL,
                          window_local_start = NULL, window_local_end = NULL,
                          window_updated_at = NULL
                    WHERE id = ? AND window_timezone IS NOT NULL""",
                (sub_id,),
            )
        return cur.rowcount > 0

    def subscription_exists(self, sub_id: str) -> bool:
        with self._lock:
            row = self._conn.execute(
                "SELECT 1 FROM subscriptions WHERE id = ?", (sub_id,)
            ).fetchone()
        return row is not None

    def delete_subscription(self, sub_id: str) -> bool:
        with self._lock, self._immediate():
            cur = self._conn.execute("DELETE FROM subscriptions WHERE id = ?", (sub_id,))
            if cur.rowcount:
                # Remove the subscription from every topic routing set; its
                # delivery task history in tasks/attempts is retained.
                self._conn.execute(
                    "DELETE FROM subscription_topics WHERE subscription_id = ?", (sub_id,)
                )
        return cur.rowcount > 0

    # ------------------------------------------------------------------
    # per-subscription topic sets (persistent pub/sub routing)
    # ------------------------------------------------------------------
    def set_subscription_topics(self, sub_id: str, topics: list[str]) -> list[str] | None:
        """Replace a subscription's deduplicated topic set, atomically.

        The whole set is rewritten in one transaction (delete all, reinsert
        the supplied topics), so PUT semantics are replace-not-merge.
        Passing an empty list removes the subscription from every topic.
        Topics are normalized already by the API layer (trimmed, unique);
        they are stored and returned sorted for a stable canonical view.
        Returns the sorted stored set, or None when the subscription does
        not exist (nothing is written in that case).
        """
        now = time.time()
        ordered = sorted(set(topics))
        with self._lock, self._immediate():
            row = self._conn.execute(
                "SELECT 1 FROM subscriptions WHERE id = ?", (sub_id,)
            ).fetchone()
            if row is None:
                return None
            self._conn.execute(
                "DELETE FROM subscription_topics WHERE subscription_id = ?", (sub_id,)
            )
            self._conn.executemany(
                "INSERT INTO subscription_topics (subscription_id, topic, created_at) VALUES (?, ?, ?)",
                [(sub_id, t, now) for t in ordered],
            )
        return ordered

    def get_subscription_topics(self, sub_id: str) -> list[str] | None:
        """Return a subscription's sorted topic set, or None if it is missing."""
        with self._lock:
            row = self._conn.execute(
                "SELECT 1 FROM subscriptions WHERE id = ?", (sub_id,)
            ).fetchone()
            if row is None:
                return None
            rows = self._conn.execute(
                """SELECT st.topic
                     FROM subscription_topics st
                     JOIN subscriptions s ON s.id = st.subscription_id
                    WHERE st.subscription_id = ?
                    ORDER BY st.topic""",
                (sub_id,),
            ).fetchall()
        return [r["topic"] for r in rows]

    def topic_event_exists(self, event_id: str) -> bool:
        """Whether the id is already claimed by a topic publication."""
        with self._lock:
            row = self._conn.execute(
                "SELECT 1 FROM topic_events WHERE id = ?", (event_id,)
            ).fetchone()
        return row is not None

    # ------------------------------------------------------------------
    # events and tasks
    # ------------------------------------------------------------------
    def get_event_payload(self, event_id: str) -> str | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT payload FROM events WHERE id = ?", (event_id,)
            ).fetchone()
        return row["payload"] if row else None

    def create_event(self, event_id: str, payload: str) -> None:
        with self._lock, self._immediate():
            self._conn.execute(
                "INSERT INTO events (id, payload, created_at) VALUES (?, ?, ?)",
                (event_id, payload, time.time()),
            )

    def create_task(
        self,
        event_id: str,
        subscription_id: str,
        max_attempts: int,
        deliver_at: float | None = None,
    ) -> tuple[dict, bool]:
        """Insert a task unless one already exists for (event_id, subscription_id).

        deliver_at is the requested scheduled delivery time (epoch seconds);
        None means the task is due immediately. It also seeds
        next_attempt_at, so the worker claim query does not pick the task up
        before the scheduled time. Scheduling on an already-existing
        (duplicate) task is never changed.

        Returns (task_row, created) so callers can report duplicates.
        """
        now = time.time()
        due = deliver_at if deliver_at is not None else now
        with self._lock, self._immediate():
            cur = self._conn.execute(
                """INSERT OR IGNORE INTO tasks
                       (event_id, subscription_id, status, attempts, max_attempts,
                        deliver_at, next_attempt_at, created_at, updated_at)
                   VALUES (?, ?, 'pending', 0, ?, ?, ?, ?, ?)""",
                (event_id, subscription_id, max_attempts, deliver_at, due, now, now),
            )
            created = cur.rowcount > 0
            row = self._conn.execute(
                "SELECT * FROM tasks WHERE event_id = ? AND subscription_id = ?",
                (event_id, subscription_id),
            ).fetchone()
        return dict(row), created

    def ingest_event(
        self,
        webhook_id: str,
        raw_body: bytes,
        subscription_ids: list[str],
        max_attempts: int,
    ) -> tuple[str, dict | list[str]]:
        """Atomically record a verified inbound event and enqueue its tasks.

        Runs in a single transaction so a concurrent replay with the same
        X-Webhook-Id can never slip past the idempotency check.

        Returns one of:
            ("duplicate", receipt_dict)  -- id already accepted, body identical;
                                            the original acceptance is returned
            ("conflict", None)           -- id already exists with a different body
            ("unknown", [ids])           -- unknown subscription id(s); nothing stored
            ("created", receipt_dict)    -- first acceptance; tasks were created
        """
        now = time.time()
        with self._lock, self._immediate():
            row = self._conn.execute(
                "SELECT raw_body, receipt FROM ingested_events WHERE id = ?",
                (webhook_id,),
            ).fetchone()
            if row is not None:
                if row["raw_body"] == raw_body:
                    return "duplicate", json.loads(row["receipt"])
                return "conflict", None

            # An event created via POST /events shares the events namespace.
            # Treat an incompatible row as a conflict as well. An id already
            # used by a topic publication is a conflict regardless of body:
            # topic and non-topic channels must not share an event id.
            topic_row = self._conn.execute(
                "SELECT topic, payload FROM topic_events WHERE id = ?", (webhook_id,)
            ).fetchone()
            if topic_row is not None:
                return "conflict", None

            existing = self._conn.execute(
                "SELECT payload FROM events WHERE id = ?", (webhook_id,)
            ).fetchone()
            if existing is not None and existing["payload"].encode("utf-8") != raw_body:
                return "conflict", None

            unknown = [
                sid
                for sid in dict.fromkeys(subscription_ids)
                if self._conn.execute(
                    "SELECT 1 FROM subscriptions WHERE id = ?", (sid,)
                ).fetchone()
                is None
            ]
            if unknown:
                return "unknown", unknown

            if existing is None:
                self._conn.execute(
                    "INSERT INTO events (id, payload, created_at) VALUES (?, ?, ?)",
                    (webhook_id, raw_body.decode("utf-8"), now),
                )

            created, duplicates = [], []
            for sid in dict.fromkeys(subscription_ids):  # preserve order, drop repeats
                cur = self._conn.execute(
                    """INSERT OR IGNORE INTO tasks
                           (event_id, subscription_id, status, attempts, max_attempts,
                            next_attempt_at, created_at, updated_at)
                       VALUES (?, ?, 'pending', 0, ?, ?, ?, ?)""",
                    (webhook_id, sid, max_attempts, now, now, now),
                )
                task = self._conn.execute(
                    "SELECT id, status FROM tasks WHERE event_id = ? AND subscription_id = ?",
                    (webhook_id, sid),
                ).fetchone()
                ref = {
                    "task_id": task["id"],
                    "subscription_id": sid,
                    "status": task["status"],
                    # POST /ingest never schedules; the explicit key keeps
                    # the stored receipt current with the TaskRef schema.
                    "deliver_at": None,
                }
                (created if cur.rowcount > 0 else duplicates).append(ref)

            receipt = {"event_id": webhook_id, "created": created, "duplicates": duplicates}
            self._conn.execute(
                "INSERT INTO ingested_events (id, raw_body, receipt, created_at) VALUES (?, ?, ?, ?)",
                (webhook_id, raw_body, json.dumps(receipt), now),
            )
        return "created", receipt

    def publish_topic_event(
        self,
        event_id: str,
        topic: str,
        payload: str,
        max_attempts: int,
    ) -> tuple[str, dict | str]:
        """Publish a topic event against a snapshot of the topic's members.

        Everything below runs in one SQLite transaction, so a concurrent
        replay can never observe a half-written publication:

        1. Idempotency / conflict check on the idempotency key
           (event_id, topic, payload):
             - a prior topic publication with the same id + topic + payload
               replays ("duplicate") and its stored first-time receipt --
               the snapshotted recipients and their task refs -- is returned
               verbatim; membership changes never add or drop tasks;
             - the same event_id with a different topic or payload is a
               "conflict" (the reason string distinguishes the two);
             - an id already claimed by POST /events or POST /ingest is a
               cross-channel "conflict".
        2. The topic's current membership is snapshotted. With no members
           nothing at all is written ("empty") and the caller answers 422.
        3. The event payload is stored once (events table), one ordinary
           delivery task is created per snapshotted subscription, and the
           publication record (including the receipt) is persisted. These
           tasks are plain tasks: ordering, rate limiting, circuit
           breaking, retries and cancellation all apply unchanged.

        Returns ("created", receipt) | ("duplicate", receipt) |
                ("conflict", reason: 'payload' | 'topic' | 'channel') |
                ("empty", "").
        """
        now = time.time()
        with self._lock, self._immediate():
            row = self._conn.execute(
                "SELECT topic, payload, receipt FROM topic_events WHERE id = ?",
                (event_id,),
            ).fetchone()
            if row is not None:
                if row["topic"] != topic:
                    return "conflict", "topic"
                if row["payload"] != payload:
                    return "conflict", "payload"
                return "duplicate", json.loads(row["receipt"])

            # The global events namespace is shared with POST /events and
            # POST /ingest; claiming an id that already exists there is a
            # cross-channel conflict (its recipients would be unrelated).
            exists = self._conn.execute(
                "SELECT 1 FROM events WHERE id = ?", (event_id,)
            ).fetchone()
            if exists is not None:
                return "conflict", "channel"

            # Snapshot the membership inside the publication transaction:
            # the set the tasks are built against can never change
            # mid-publication, and later membership edits cannot add or
            # remove tasks for this event (replays return the stored set).
            members = [
                r["subscription_id"]
                for r in self._conn.execute(
                    """SELECT subscription_id FROM subscription_topics
                        WHERE topic = ? ORDER BY created_at, subscription_id""",
                    (topic,),
                ).fetchall()
            ]
            if not members:
                # Nothing is persisted: no event, no publication record.
                return "empty", ""

            self._conn.execute(
                "INSERT INTO events (id, payload, created_at) VALUES (?, ?, ?)",
                (event_id, payload, now),
            )

            recipients = []
            for sid in members:
                self._conn.execute(
                    """INSERT INTO tasks
                           (event_id, subscription_id, status, attempts, max_attempts,
                            next_attempt_at, created_at, updated_at)
                       VALUES (?, ?, 'pending', 0, ?, ?, ?, ?)""",
                    (event_id, sid, max_attempts, now, now, now),
                )
                task = self._conn.execute(
                    "SELECT id, status FROM tasks WHERE event_id = ? AND subscription_id = ?",
                    (event_id, sid),
                ).fetchone()
                # Topic publications are never scheduled, like POST /ingest.
                recipients.append(
                    {
                        "task_id": task["id"],
                        "subscription_id": sid,
                        "status": task["status"],
                        "deliver_at": None,
                    }
                )

            receipt = {
                "event_id": event_id,
                "topic": topic,
                "payload": json.loads(payload),
                "recipients": recipients,
            }
            self._conn.execute(
                """INSERT INTO topic_events (id, topic, payload, receipt, created_at)
                   VALUES (?, ?, ?, ?, ?)""",
                (event_id, topic, payload, json.dumps(receipt), now),
            )
        return "created", receipt

    def claim_due_task(self, worker_id: str, now: float) -> dict | None:
        """Atomically claim the oldest due task for this worker.

        The SELECT-then-UPDATE runs under the connection lock, so two workers
        can never claim the same task row.

        Strict per-subscription ordering: a task is eligible only when no
        earlier task (smaller id) of the same subscription is still
        'pending' or 'in_progress' -- the next task is released once its
        predecessor reaches a terminal state
        ('delivered'/'failed'/'cancelled'). The rule is evaluated against
        persisted state at claim time, so it holds across retry backoff,
        concurrent workers, and restarts. Different subscriptions are
        independent and still claimed in parallel; a blocked task simply
        stays 'pending' and remains visible as such in status queries.

        Scheduled delivery: a task submitted with deliver_at in the future
        is stored with next_attempt_at seeded to that instant, so the
        t.next_attempt_at <= ? predicate makes every worker skip it until
        its time comes (no in-memory timers, so the guarantee survives
        restarts). Because it stays 'pending' while waiting, the ordering
        rule above also makes it block every later task of the same
        subscription -- including immediately-due ones -- until the
        scheduled task itself has been delivered, failed, or cancelled.

        Event-cancelled tasks ('cancelled') are terminal and never selected:
        they neither retry nor count towards a subscription's breaker, and
        because they leave the pending/in_progress set they never block a
        later task of the same subscription.

        Paused subscriptions (paused_at set) are skipped entirely: their
        tasks stay 'pending' with their original next_attempt_at and
        become claimable again, in the same order, once resumed. Tasks
        already 'in_progress' when the pause lands are unaffected and
        run to completion.

        Circuit breaker: tasks of a subscription whose breaker is 'open'
        are not claimed until the cooldown expires (breaker_next_probe_at
        <= now). The first claim after that moment belongs to the earliest
        pending task of the subscription (guaranteed by the ordering rule)
        and atomically flips the breaker to 'half_open' with that task
        recorded as the single probe. While 'half_open', only the probe
        task itself can be claimed (e.g. for its retries); every other
        task of the subscription waits for the probe's verdict.

        Per-subscription token bucket: when a subscription has a rate
        policy (rate_capacity / rate_refill_per_second set), the candidate
        task -- which the ordering rule guarantees is the subscription's
        head task -- is admitted only after a token is deducted. The
        balance is refilled lazily against the wall clock and refilled,
        checked, deducted and persisted inside this same locked critical
        section, so concurrent workers can never spend the same token and
        the balance survives restarts. With no whole token available the
        task is NOT claimed: it stays 'pending' and its next_attempt_at is
        pushed to the exact, deterministically computed instant the next
        token arrives (now + (1 - balance) / refill); the refilled balance
        is persisted in the same commit. Because only the head task is
        ever considered and later tasks of the subscription are blocked by
        the ordering NOT EXISTS anyway, rescheduling it preserves strict
        per-subscription order. The claim loop then looks for due work of
        other subscriptions, so one throttled subscription never parks an
        idle worker. A subscription found throttled during this call is
        skipped for the rest of the same call (it is recorded in
        deferred_sids): its head task was pushed to its refill instant
        once, and considering it again would only push that instant out
        further while examining tasks that are blocked behind it anyway.

        Per-subscription delivery window: when a subscription has a window
        (window_timezone / window_weekdays / window_local_start /
        window_local_end set), the due head task is admitted only while the
        window is open in the configured IANA zone. The check is a pure
        function of epoch time and the persisted configuration (see
        delivery_window.DeliveryWindow), so every worker reaches the same
        verdict and the rule survives restarts with no in-memory timers.
        Outside the window the task is NOT claimed: it stays 'pending' and
        its next_attempt_at is pushed to the window's exact next opening
        instant (DST-aware, deterministic); cross-midnight wrap windows and
        spring-forward/fall-back transitions are handled from the real tzdb.
        Only due tasks are pushed forward (a future next_attempt_at is
        never moved earlier), so restarts can never deliver early. Like a
        throttled subscription, a closed-window subscription is deferred
        once per call (deferred_sids) and the loop claims due work of other
        subscriptions instead. The window gate runs before the token gate,
        so waiting outside a window neither spends nor refills tokens.
        """
        with self._lock:
            # Take the database-wide write lock up front (BEGIN IMMEDIATE)
            # so the eligibility SELECT, the token deduction and the claim
            # UPDATE are one indivisible database-level critical section,
            # also across separate worker processes. An entirely idle poll
            # performs no writes and is rolled back, which costs no fsync.
            self._conn.execute("BEGIN IMMEDIATE")
            dirty = False
            try:
                # Defer each gated (closed-window or throttled) subscription
                # at most once per call; the membership check also makes the
                # loop provably terminate (it never revisits a subscription
                # it deferred).
                deferred_sids: set[str] = set()
                while True:
                    skip = ",".join("?" * len(deferred_sids)) if deferred_sids else "SELECT NULL WHERE 0"
                    row = self._conn.execute(
                        f"""SELECT t.*, s.target_url, s.secret, s.previous_secret,
                                  s.previous_secret_expires_at, s.breaker_state,
                                  s.rate_capacity, s.rate_refill_per_second,
                                  s.rate_tokens, s.rate_updated_at,
                                  s.window_timezone, s.window_weekdays,
                                  s.window_local_start, s.window_local_end,
                                  e.payload
                           FROM tasks t
                           LEFT JOIN subscriptions s ON s.id = t.subscription_id
                           LEFT JOIN events e ON e.id = t.event_id
                           WHERE t.status = 'pending' AND t.next_attempt_at <= ?
                             AND s.paused_at IS NULL
                             AND (s.id IS NULL OR t.subscription_id NOT IN ({skip}))
                             AND (
                                   s.id IS NULL              -- subscription deleted: claim and fail
                                   OR s.breaker_state = 'closed'
                                   OR (s.breaker_state = 'open' AND s.breaker_next_probe_at <= ?)
                                   OR (s.breaker_state = 'half_open'
                                       AND t.id = s.breaker_probe_task_id)
                                 )
                             AND NOT EXISTS (
                                   SELECT 1 FROM tasks earlier
                                   WHERE earlier.subscription_id = t.subscription_id
                                     AND earlier.id < t.id
                                     AND earlier.status IN ('pending', 'in_progress')
                                 )
                           ORDER BY t.next_attempt_at, t.id
                           LIMIT 1""",
                        (now, *deferred_sids, now) if deferred_sids else (now, now),
                    ).fetchone()
                    if row is None:
                        if dirty:
                            self._conn.commit()  # persist gated reschedules only
                        else:
                            self._conn.execute("ROLLBACK")  # idle poll: no fsync
                        return None

                    # Delivery-window gate (before tokens): a due head task
                    # outside the window stays pending until its exact next
                    # opening -- an absolute UTC epoch, deterministic for all
                    # workers and across restarts.
                    window = _window_from_row(row)
                    if window is not None and row["target_url"] is not None:
                        if not window.is_open(now):
                            next_opening = window.next_open(now)
                            self._conn.execute(
                                """UPDATE tasks
                                      SET next_attempt_at = ?, updated_at = ?
                                    WHERE id = ? AND status = 'pending'""",
                                (next_opening, now, row["id"]),
                            )
                            # The deferred head task blocks only its own
                            # subscription; remember it and re-query for work
                            # of other subscriptions. The reschedule commits
                            # once the claim transaction leaves the loop.
                            deferred_sids.add(row["subscription_id"])
                            dirty = True
                            continue

                    capacity = row["rate_capacity"]
                    refill = row["rate_refill_per_second"]
                    if capacity is not None and refill is not None and row["target_url"] is not None:
                        # Lazy refill persisted in the same critical section as
                        # the claim, so the bucket is shared consistently across
                        # workers and across restarts.
                        balance, _ = _refill_tokens(
                            capacity,
                            refill,
                            row["rate_tokens"],
                            row["rate_updated_at"],
                            now,
                        )
                        if balance < 1.0 - _TOKEN_EPS:
                            # No token: the head task stays pending until the
                            # missing fraction of a token has accrued. The
                            # instant is a pure function of the persisted
                            # balance, so every worker computes the same time.
                            available_at = now + (1.0 - balance) / refill
                            self._conn.execute(
                                """UPDATE tasks
                                      SET next_attempt_at = ?, updated_at = ?
                                    WHERE id = ? AND status = 'pending'""",
                                (available_at, now, row["id"]),
                            )
                            self._conn.execute(
                                """UPDATE subscriptions
                                      SET rate_tokens = ?, rate_updated_at = ?
                                    WHERE id = ?""",
                                (balance, now, row["subscription_id"]),
                            )
                            # The throttled head task blocks only its own
                            # subscription; remember it and re-query for work of
                            # other subscriptions. The reschedule commits once
                            # the claim transaction leaves the loop.
                            deferred_sids.add(row["subscription_id"])
                            dirty = True
                            continue
                        # Atomically spend one token together with the claim.
                        self._conn.execute(
                            """UPDATE subscriptions
                                  SET rate_tokens = ?, rate_updated_at = ?
                                WHERE id = ?""",
                            (balance - 1.0, now, row["subscription_id"]),
                        )

                    self._conn.execute(
                        "UPDATE tasks SET status = 'in_progress', claimed_by = ?, updated_at = ? WHERE id = ?",
                        (worker_id, now, row["id"]),
                    )
                    if row["breaker_state"] == "open":
                        # Cooldown elapsed: this claim is the subscription's
                        # single half-open probe. The state flip is committed
                        # together with the claim, so no second worker can
                        # also become the probe.
                        self._conn.execute(
                            """UPDATE subscriptions
                               SET breaker_state = 'half_open', breaker_probe_task_id = ?,
                                   breaker_next_probe_at = NULL
                               WHERE id = ? AND breaker_state = 'open'""",
                            (row["id"], row["subscription_id"]),
                        )
                    self._conn.commit()
                    return dict(row)
            except BaseException:
                self._conn.execute("ROLLBACK")
                raise

    def finish_attempt(
        self,
        task_id: int,
        attempt_no: int,
        status_code: int | None,
        error: str | None,
        started_at: float,
        finished_at: float,
        outcome: str,  # 'delivered' | 'failed' | 'retry'
        next_attempt_at: float | None,
    ) -> None:
        """Record an attempt and move the task to its next state, in one commit.

        Terminal outcomes also update the subscription's circuit breaker in
        the same transaction, so the failure count and the state machine can
        never drift apart from the task history.
        """
        with self._lock, self._immediate():
            self._conn.execute(
                """INSERT INTO attempts (task_id, attempt_no, status_code, error, started_at, finished_at)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (task_id, attempt_no, status_code, error, started_at, finished_at),
            )
            if outcome == "retry":
                self._conn.execute(
                    """UPDATE tasks SET status = 'pending', attempts = ?, next_attempt_at = ?,
                           last_status_code = ?, last_error = ?, claimed_by = NULL, updated_at = ?
                       WHERE id = ?""",
                    (attempt_no, next_attempt_at, status_code, error, finished_at, task_id),
                )
            else:
                self._conn.execute(
                    """UPDATE tasks SET status = ?, result = ?, attempts = ?, next_attempt_at = NULL,
                           last_status_code = ?, last_error = ?, claimed_by = NULL, updated_at = ?
                       WHERE id = ?""",
                    (outcome, outcome, attempt_no, status_code, error, finished_at, task_id),
                )
                self._apply_breaker_outcome(task_id, outcome, finished_at)

    def _apply_breaker_outcome(self, task_id: int, outcome: str, now: float) -> None:
        """Advance the circuit breaker of the task's subscription.

        Must be called with the connection lock held, inside the transaction
        that terminates the task. 'outcome' is 'delivered' or 'failed'
        (final failure after the attempt budget is exhausted).

        - delivered: any success breaks the consecutive-failure streak; a
          successful half-open probe additionally closes the breaker.
        - failed: the consecutive-failure count grows; at the configured
          threshold a 'closed' breaker trips 'open' atomically, and a failed
          probe re-opens a 'half_open' breaker with a refreshed cooldown.
        """
        row = self._conn.execute(
            """SELECT s.id, s.breaker_state, s.breaker_consecutive_failures,
                      s.breaker_probe_task_id
               FROM tasks t JOIN subscriptions s ON s.id = t.subscription_id
               WHERE t.id = ?""",
            (task_id,),
        ).fetchone()
        if row is None:
            return  # subscription was deleted; no breaker to update

        sub_id = row["id"]
        state = row["breaker_state"]
        failures = row["breaker_consecutive_failures"]
        is_probe = state == "half_open" and row["breaker_probe_task_id"] == task_id

        if outcome == "delivered":
            if is_probe:
                # Probe succeeded: close the breaker and reset the count.
                self._conn.execute(
                    """UPDATE subscriptions
                       SET breaker_state = 'closed', breaker_consecutive_failures = 0,
                           breaker_next_probe_at = NULL, breaker_probe_task_id = NULL
                       WHERE id = ?""",
                    (sub_id,),
                )
            elif failures:
                self._conn.execute(
                    "UPDATE subscriptions SET breaker_consecutive_failures = 0 WHERE id = ?",
                    (sub_id,),
                )
            return

        # outcome == 'failed': a task reached its final failure.
        failures += 1
        if is_probe or (state == "closed" and failures >= self._breaker_threshold):
            # Probe failed, or the threshold was reached: (re-)open the
            # breaker and start a fresh cooldown from this failure.
            self._conn.execute(
                """UPDATE subscriptions
                   SET breaker_state = 'open', breaker_consecutive_failures = ?,
                       breaker_next_probe_at = ?, breaker_probe_task_id = NULL
                   WHERE id = ?""",
                (failures, now + self._breaker_cooldown, sub_id),
            )
        else:
            self._conn.execute(
                "UPDATE subscriptions SET breaker_consecutive_failures = ? WHERE id = ?",
                (failures, sub_id),
            )

    def requeue_in_progress(self) -> int:
        """Reset tasks left 'in_progress' by a previous run (crash/restart recovery).

        Such a task is again unclaimed ('pending'), so if its event is
        (re-)cancelled after this reset the task is eligible for cancellation
        like any other pending task; otherwise it is redelivered.
        """
        now = time.time()
        with self._lock, self._immediate():
            cur = self._conn.execute(
                """UPDATE tasks SET status = 'pending', next_attempt_at = ?, claimed_by = NULL, updated_at = ?
                   WHERE status = 'in_progress'""",
                (now, now),
            )
        return cur.rowcount

    def event_status(self, event_id: str) -> dict | None:
        """Full status of an event: its tasks plus per-task attempt history."""
        with self._lock:
            event = self._conn.execute(
                "SELECT id, created_at FROM events WHERE id = ?", (event_id,)
            ).fetchone()
            if event is None:
                return None
            tasks = self._conn.execute(
                """SELECT t.*, s.target_url
                   FROM tasks t LEFT JOIN subscriptions s ON s.id = t.subscription_id
                   WHERE t.event_id = ? ORDER BY t.id""",
                (event_id,),
            ).fetchall()
            attempts = self._conn.execute(
                """SELECT a.* FROM attempts a
                   JOIN tasks t ON t.id = a.task_id
                   WHERE t.event_id = ? ORDER BY a.task_id, a.attempt_no""",
                (event_id,),
            ).fetchall()

        history: dict[int, list[dict]] = {}
        for a in attempts:
            history.setdefault(a["task_id"], []).append(dict(a))
        return {
            "event": dict(event),
            "tasks": [dict(t) for t in tasks],
            "attempts_by_task": history,
        }

    # ------------------------------------------------------------------
    # audit queries
    # ------------------------------------------------------------------
    def list_delivery_tasks(
        self,
        *,
        subscription_id: str | None = None,
        event_id: str | None = None,
        status: str | None = None,
        updated_from: float | None = None,
        updated_to: float | None = None,
        cursor: tuple[float, int] | None = None,
        limit: int = 50,
    ) -> tuple[list[dict], tuple[float, int] | None]:
        """Keyset-paginated delivery-task listing for the audit API.

        Rows are ordered by (updated_at DESC, id DESC); the cursor is the
        (updated_at, id) key of the last row of the previous page, so paging
        is stable and offset-free even when many rows share one updated_at
        (e.g. a bulk cancel). One extra row is fetched to detect whether a
        next page exists. Returns (rows, next_cursor_key); next_cursor_key
        is None on the last page. updated_from/updated_to are inclusive
        epoch-second bounds on updated_at.
        """
        clauses, params = [], []
        if subscription_id is not None:
            clauses.append("t.subscription_id = ?")
            params.append(subscription_id)
        if event_id is not None:
            clauses.append("t.event_id = ?")
            params.append(event_id)
        if status is not None:
            clauses.append("t.status = ?")
            params.append(status)
        if updated_from is not None:
            clauses.append("t.updated_at >= ?")
            params.append(updated_from)
        if updated_to is not None:
            clauses.append("t.updated_at <= ?")
            params.append(updated_to)
        if cursor is not None:
            clauses.append("(t.updated_at < ? OR (t.updated_at = ? AND t.id < ?))")
            params.extend([cursor[0], cursor[0], cursor[1]])
        where = "WHERE " + " AND ".join(clauses) if clauses else ""
        with self._lock:
            rows = self._conn.execute(
                f"""SELECT t.id, t.event_id, t.subscription_id, t.status, t.attempts,
                           t.last_error, t.deliver_at, t.updated_at, s.target_url
                      FROM tasks t LEFT JOIN subscriptions s ON s.id = t.subscription_id
                      {where}
                     ORDER BY t.updated_at DESC, t.id DESC
                     LIMIT ?""",
                (*params, limit + 1),
            ).fetchall()
        items = [dict(r) for r in rows[:limit]]
        next_cursor = None
        if len(rows) > limit:
            last = items[-1]
            next_cursor = (last["updated_at"], last["id"])
        return items, next_cursor

    def get_delivery_task(self, task_id: int) -> dict | None:
        """One delivery task with its full attempt history (audit detail view).

        Returns {"task": row, "attempts": [rows]}, or None when no task with
        this id exists. target_url is None when the subscription was deleted.
        """
        with self._lock:
            task = self._conn.execute(
                """SELECT t.*, s.target_url
                     FROM tasks t LEFT JOIN subscriptions s ON s.id = t.subscription_id
                     WHERE t.id = ?""",
                (task_id,),
            ).fetchone()
            if task is None:
                return None
            attempts = self._conn.execute(
                "SELECT * FROM attempts WHERE task_id = ? ORDER BY attempt_no",
                (task_id,),
            ).fetchall()
        return {"task": dict(task), "attempts": [dict(a) for a in attempts]}

    def cancel_event(self, event_id: str) -> dict | None:
        """Atomically cancel every not-yet-claimed delivery task of an event.

        Runs under the connection lock in a single transaction, serialized
        against worker claims, so a task a worker has already claimed
        ('in_progress') -- or which has already finished
        ('delivered'/'failed') -- is never touched and keeps its recorded
        result; only rows still 'pending' are flipped to the terminal
        'cancelled' state. Cancelled tasks:

        - are never claimed again (the claim query only selects 'pending'),
          so they take part in no retry and never reach finish_attempt,
          which also means they never move the circuit-breaker counters;
        - are terminal for the per-subscription ordering rule (the claim
          query's blocking NOT EXISTS watches only 'pending'/'in_progress'),
          so later tasks of the same subscription are released immediately.

        One edge case is repaired in the same transaction: a half-open
        breaker records exactly one probe task. That task sits 'pending'
        while backing off between probe attempts and can therefore be
        cancelled; the breaker is then moved back to 'open' with the probe
        due at once so the next pending task becomes the replacement probe
        instead of the subscription being wedged in 'half_open' forever.

        Repeated cancellation is idempotent: the second call finds no
        'pending' rows, cancels zero, and reports the same totals. Returns
        None when the event does not exist.
        """
        now = time.time()
        with self._lock, self._immediate():
            exists = self._conn.execute(
                "SELECT 1 FROM events WHERE id = ?", (event_id,)
            ).fetchone()
            if exists is None:
                return None

            cur = self._conn.execute(
                """UPDATE tasks
                       SET status = 'cancelled', next_attempt_at = NULL,
                           claimed_by = NULL, cancelled_at = ?, updated_at = ?
                     WHERE event_id = ? AND status = 'pending'""",
                (now, now, event_id),
            )
            newly_cancelled = cur.rowcount

            if newly_cancelled:
                # Release any half-open breaker whose unique probe was just
                # cancelled; the next claim promotes the earliest remaining
                # pending task to a fresh, immediately-due probe.
                self._conn.execute(
                    """UPDATE subscriptions
                          SET breaker_state = 'open', breaker_next_probe_at = ?,
                              breaker_probe_task_id = NULL
                        WHERE breaker_state = 'half_open'
                          AND breaker_probe_task_id IN (
                              SELECT id FROM tasks
                               WHERE event_id = ? AND status = 'cancelled'
                          )""",
                    (now, event_id),
                )

            rows = self._conn.execute(
                "SELECT status, COUNT(*) AS n FROM tasks WHERE event_id = ? GROUP BY status",
                (event_id,),
            ).fetchall()
            counts = {
                "pending": 0,
                "in_progress": 0,
                "delivered": 0,
                "failed": 0,
                "cancelled": 0,
            }
            for r in rows:
                counts[r["status"]] = r["n"]

        return {
            "event_id": event_id,
            "cancelled": newly_cancelled,
            "cancelled_at": now,
            "counts": counts,
        }
