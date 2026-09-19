"""Runtime configuration, overridable via environment variables."""
from __future__ import annotations

import os

# SQLite database file location.
DB_PATH = os.getenv("DB_PATH", "./webhook_relay.db")

# Shared secret for authenticating inbound events on POST /ingest.
# Intentionally has no default: deployments must provide their own secret.
# The development compose file sets one explicitly for quick local testing.
INGEST_SECRET = os.getenv("INGEST_SECRET")

# Allowed clock skew (seconds, in either direction) for inbound timestamps.
INGEST_TIMESTAMP_TOLERANCE = float(os.getenv("INGEST_TIMESTAMP_TOLERANCE_SECONDS", "300"))

# Number of concurrent delivery workers.
WORKER_COUNT = int(os.getenv("WORKER_COUNT", "4"))

# HTTP timeout (seconds) for a single delivery attempt.
HTTP_TIMEOUT = float(os.getenv("HTTP_TIMEOUT_SECONDS", "5"))

# Maximum delivery attempts per task before it is marked failed.
MAX_ATTEMPTS = int(os.getenv("MAX_ATTEMPTS", "4"))

# First retry delay in seconds; subsequent delays double (1s, 2s, 4s, ...).
BACKOFF_BASE = float(os.getenv("BACKOFF_BASE_SECONDS", "1"))

# How often an idle worker polls the database for due tasks.
POLL_INTERVAL = float(os.getenv("POLL_INTERVAL_SECONDS", "0.2"))

# Per-subscription circuit breaker: how many consecutive tasks must reach
# their final failure before the breaker opens and deliveries pause.
BREAKER_FAILURE_THRESHOLD = int(os.getenv("BREAKER_FAILURE_THRESHOLD", "5"))

# How long an open breaker waits before letting a single half-open probe
# task through; a failed probe re-opens the breaker for another cooldown.
BREAKER_COOLDOWN_SECONDS = float(os.getenv("BREAKER_COOLDOWN_SECONDS", "30"))


def retry_delay(attempt_no: int) -> float:
    """Delay before the next attempt, given the attempt that just failed.

    attempt 1 -> 1s, attempt 2 -> 2s, attempt 3 -> 4s (exponential, base 2).
    """
    return BACKOFF_BASE * (2 ** (attempt_no - 1))
