"""HMAC-SHA256 request signing for outgoing webhook deliveries."""
from __future__ import annotations

import hashlib
import hmac


def signature_header(secret: str, body: bytes) -> str:
    """Value for the X-Webhook-Signature header: 'sha256=<hex digest>'."""
    digest = hmac.new(secret.encode("utf-8"), body, hashlib.sha256).hexdigest()
    return f"sha256={digest}"


def verify(secret: str, body: bytes, header: str) -> bool:
    """Constant-time check of a received signature header."""
    return hmac.compare_digest(signature_header(secret, body), header)


def ingest_signed_content(timestamp: str, webhook_id: str, raw_body: bytes) -> bytes:
    """Exact byte string covered by the inbound signature on POST /ingest.

    Layout (delimiters are not allowed inside timestamp/id):
        <timestamp> + "." + <webhook_id> + "." + <raw JSON body>
    """
    return (
        timestamp.encode("utf-8")
        + b"."
        + webhook_id.encode("utf-8")
        + b"."
        + raw_body
    )


def verify_ingest(
    secret: str,
    timestamp: str,
    webhook_id: str,
    raw_body: bytes,
    signature_header: str,
) -> bool:
    """Constant-time verification of an inbound X-Webhook-Signature header.

    The signature must be 'sha256=<hex HMAC over timestamp.id.raw_body>'.
    """
    signed = ingest_signed_content(timestamp, webhook_id, raw_body)
    expected = hmac.new(secret.encode("utf-8"), signed, hashlib.sha256).hexdigest()
    provided = signature_header
    if provided.startswith("sha256="):
        provided = provided[len("sha256="):]
    # Compare the hex digests only; a malformed header simply fails the check.
    return hmac.compare_digest(expected, provided)
