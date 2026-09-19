"""Regression tests: a newly created subscription is immediately configurable.

Covers the create -> configure -> query chain on the delivery-window route:

- the ``id`` returned by ``POST /subscriptions`` can be used at once in
  ``PUT /subscriptions/{id}/delivery-window`` and answers ``200`` with the
  configuration persisted (the write is visible to the very next read on the
  same database connection/transaction stream);
- a follow-up ``GET`` of the same path answers ``enabled=true`` plus the
  normalized window (canonical ``HH:MM:SS`` clocks, weekdays sorted and
  deduplicated);
- a genuinely unknown id keeps answering ``404`` on both PUT and GET (and the
  rejected PUT persists nothing);
- the same holds against the persistence layer directly, under concurrent
  create/configure load, and across a process-restart-style reopen of the
  database file.

The DST scheduling math itself is covered by test_delivery_window_dst.py and
is intentionally left untouched here.

Pure stdlib + the app's own FastAPI test client; run with either of

    python3 -m unittest discover -s tests -v
    python3 -m pytest tests/
"""
from __future__ import annotations

import os
import tempfile
import threading
import unittest

# A private temp database must be selected before app.config (and hence
# app.main) is first imported, since config reads the environment at import.
_TMP_ROOT = tempfile.mkdtemp(prefix="wr-regression-")
os.environ.setdefault("DB_PATH", os.path.join(_TMP_ROOT, "default.db"))
os.environ.setdefault("INGEST_SECRET", "test-ingest-secret")
os.environ.setdefault("WORKER_COUNT", "2")

from fastapi.testclient import TestClient  # noqa: E402

from app import config  # noqa: E402
from app.db import Database  # noqa: E402
import app.main as main  # noqa: E402

WINDOW_BODY = {
    "timezone": "UTC",
    "weekdays": [1, 2, 3, 4, 5],
    "local_start": "09:00",
    "local_end": "18:00",
}


class DeliveryWindowAPIRegressionTests(unittest.TestCase):
    """End-to-end HTTP chain through the real FastAPI lifespan."""

    def setUp(self):
        # Each test gets a fresh database file. The lifespan builds its
        # Database from config.DB_PATH at startup, so point it at this test's
        # file before entering the TestClient context. The background workers
        # also start here, which additionally exercises visibility while
        # worker threads run their own BEGIN IMMEDIATE/ROLLBACK poll cycles.
        self.tmp = tempfile.TemporaryDirectory(dir=_TMP_ROOT)
        config.DB_PATH = os.path.join(self.tmp.name, "api.db")
        self.client = TestClient(main.app)
        self.client.__enter__()
        self.addCleanup(self._shutdown)

    def _shutdown(self):
        self.client.__exit__(None, None, None)
        self.tmp.cleanup()

    def _create(self, url: str = "http://127.0.0.1:1/hook") -> str:
        resp = self.client.post(
            "/subscriptions", json={"target_url": url, "secret": "s3cr3t"}
        )
        self.assertEqual(resp.status_code, 201, resp.text)
        sid = resp.json()["id"]
        self.assertIsInstance(sid, str)
        return sid

    def test_put_immediately_after_create_returns_200_and_persists(self):
        sid = self._create()
        put = self.client.put(
            f"/subscriptions/{sid}/delivery-window", json=WINDOW_BODY
        )
        self.assertEqual(put.status_code, 200, put.text)
        body = put.json()
        self.assertTrue(body["enabled"])
        self.assertEqual(body["timezone"], "UTC")
        self.assertEqual(body["weekdays"], [1, 2, 3, 4, 5])
        # Clocks come back canonicalized to HH:MM:SS even though HH:MM was sent.
        self.assertEqual(body["local_start"], "09:00:00")
        self.assertEqual(body["local_end"], "18:00:00")
        self.assertFalse(body["wraps_past_midnight"])
        self.assertIsNotNone(body["updated_at"])

    def test_get_after_create_put_reports_enabled_and_normalized_window(self):
        sid = self._create()
        # Before configuration the window is the unrestricted default.
        before = self.client.get(f"/subscriptions/{sid}/delivery-window")
        self.assertEqual(before.status_code, 200, before.text)
        self.assertFalse(before.json()["enabled"])
        self.assertIsNone(before.json()["timezone"])

        configured = self.client.put(
            f"/subscriptions/{sid}/delivery-window",
            json={
                "timezone": "Asia/Shanghai",
                # Deliberately unsorted: the stored/echoed set is sorted.
                "weekdays": [7, 1, 5, 3],
                "local_start": "22:00:00",
                "local_end": "06:00:00",  # end <= start -> wraps midnight
            },
        )
        self.assertEqual(configured.status_code, 200, configured.text)

        got = self.client.get(f"/subscriptions/{sid}/delivery-window")
        self.assertEqual(got.status_code, 200, got.text)
        g = got.json()
        self.assertTrue(g["enabled"])
        self.assertEqual(g["timezone"], "Asia/Shanghai")
        self.assertEqual(g["weekdays"], [1, 3, 5, 7])  # sorted
        self.assertEqual(g["local_start"], "22:00:00")
        self.assertEqual(g["local_end"], "06:00:00")
        self.assertTrue(g["wraps_past_midnight"])
        self.assertIsNotNone(g["updated_at"])

    def test_get_and_put_for_unknown_id_still_404(self):
        for method, path, kwargs in (
            ("get", "/subscriptions/doesnotexist00/delivery-window", {}),
            (
                "put",
                "/subscriptions/doesnotexist00/delivery-window",
                {"json": WINDOW_BODY},
            ),
        ):
            resp = getattr(self.client, method)(path, **kwargs)
            self.assertEqual(resp.status_code, 404, resp.text)

    def test_failed_put_on_unknown_id_persists_nothing(self):
        resp = self.client.put(
            "/subscriptions/000000000000/delivery-window", json=WINDOW_BODY
        )
        self.assertEqual(resp.status_code, 404)
        # The id really does not exist on the subscription resource either;
        # a rejected window write must not have created a phantom row.
        self.assertEqual(
            self.client.get("/subscriptions/000000000000").status_code, 404
        )

    def test_id_from_create_is_exactly_the_id_routes_resolve(self):
        # The returned identifier must round-trip verbatim through every
        # per-subscription route (no trimming/normalization mismatch).
        sid = self._create()
        self.assertEqual(self.client.get(f"/subscriptions/{sid}").status_code, 200)
        self.assertEqual(
            self.client.put(
                f"/subscriptions/{sid}/delivery-window", json=WINDOW_BODY
            ).status_code,
            200,
        )
        self.assertEqual(
            self.client.get(f"/subscriptions/{sid}/delivery-window").status_code,
            200,
        )
        # And it shows up in the collection listing under the same id.
        ids = [s["id"] for s in self.client.get("/subscriptions").json()]
        self.assertIn(sid, ids)

    def test_replace_window_then_get_shows_the_new_window(self):
        sid = self._create()
        first = self.client.put(
            f"/subscriptions/{sid}/delivery-window", json=WINDOW_BODY
        )
        self.assertEqual(first.status_code, 200)
        second = self.client.put(
            f"/subscriptions/{sid}/delivery-window",
            json={
                "timezone": "America/New_York",
                "weekdays": [6, 7],
                "local_start": "10:30",
                "local_end": "11:30",
            },
        )
        self.assertEqual(second.status_code, 200, second.text)
        g = self.client.get(f"/subscriptions/{sid}/delivery-window").json()
        self.assertEqual(g["timezone"], "America/New_York")
        self.assertEqual(g["weekdays"], [6, 7])
        self.assertEqual(g["local_start"], "10:30:00")
        self.assertEqual(g["local_end"], "11:30:00")
        self.assertFalse(g["wraps_past_midnight"])


class DeliveryWindowPersistenceLayerTests(unittest.TestCase):
    """Identity/visibility guarantees at the Database boundary."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(dir=_TMP_ROOT)
        self.path = os.path.join(self.tmp.name, "db.db")
        self.db = Database(self.path)

    def tearDown(self):
        self.db.close()
        self.tmp.cleanup()

    def test_create_then_set_and_get_window_in_one_process(self):
        sid = self.db.create_subscription("http://127.0.0.1:1/h", "s")["id"]
        self.assertTrue(self.db.subscription_exists(sid))
        stored = self.db.set_delivery_window(sid, "UTC", [5, 1, 5], 9 * 3600, 18 * 3600)
        self.assertIsNotNone(stored)
        self.assertTrue(stored["configured"])
        self.assertEqual(stored["weekdays"], [1, 5])  # sorted, deduplicated
        policy = self.db.get_delivery_window(sid)
        self.assertIsNotNone(policy)
        self.assertTrue(policy["configured"])
        self.assertEqual(policy["timezone"], "UTC")
        self.assertEqual(policy["local_start"], 9 * 3600)
        self.assertEqual(policy["local_end"], 18 * 3600)

    def test_unknown_id_returns_none_from_window_read_and_write(self):
        self.assertIsNone(self.db.get_delivery_window("deadbeef0000"))
        self.assertIsNone(
            self.db.set_delivery_window("deadbeef0000", "UTC", [1], 0, 3600)
        )

    def test_window_survives_reopen_of_the_database_file(self):
        sid = self.db.create_subscription("http://127.0.0.1:1/h", "s")["id"]
        self.db.set_delivery_window(sid, "Europe/Berlin", [6, 7], 22 * 3600, 6 * 3600)
        self.db.close()

        # Fresh Database instance: a brand-new connection and all new caches,
        # the closest in-process analogue to a container restart.
        reopened = Database(self.path)
        try:
            self.assertTrue(reopened.subscription_exists(sid))
            policy = reopened.get_delivery_window(sid)
            self.assertIsNotNone(policy)
            self.assertTrue(policy["configured"])
            self.assertEqual(policy["timezone"], "Europe/Berlin")
            self.assertEqual(policy["weekdays"], [6, 7])
            self.assertEqual(policy["local_start"], 22 * 3600)
            self.assertEqual(policy["local_end"], 6 * 3600)
            # A real unknown id is still unknown after the restart.
            self.assertIsNone(reopened.get_delivery_window("nonexistent00"))
        finally:
            reopened.close()

    def test_concurrent_create_and_configure_never_404s(self):
        errors: list[tuple] = []
        created_ids: list[str] = []
        lock = threading.Lock()

        def worker(prefix: int) -> None:
            try:
                for i in range(40):
                    sid = self.db.create_subscription(
                        f"http://127.0.0.1:1/{prefix}-{i}", "s"
                    )["id"]
                    with lock:
                        created_ids.append(sid)
                    stored = self.db.set_delivery_window(
                        sid, "UTC", [1, 2, 3, 4, 5], 9 * 3600, 18 * 3600
                    )
                    if stored is None:
                        errors.append(("set", prefix, i, sid))
                        continue
                    policy = self.db.get_delivery_window(sid)
                    if policy is None or not policy["configured"]:
                        errors.append(("get", prefix, i, sid, policy))
            except Exception as exc:  # pragma: no cover - failure reporting only
                errors.append(("exc", prefix, repr(exc)))

        threads = [threading.Thread(target=worker, args=(p,)) for p in range(6)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        self.assertEqual(errors, [])
        self.assertEqual(len(created_ids), 6 * 40)
        # Every minted id is unique and resolvable afterwards.
        self.assertEqual(len(set(created_ids)), len(created_ids))
        sample = created_ids[::37]
        for sid in sample:
            self.assertTrue(self.db.subscription_exists(sid))
            self.assertTrue(self.db.get_delivery_window(sid)["configured"])


if __name__ == "__main__":
    unittest.main()
