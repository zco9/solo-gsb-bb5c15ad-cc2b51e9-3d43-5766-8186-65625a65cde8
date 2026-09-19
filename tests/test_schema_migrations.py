"""Regression tests for the PRAGMA user_version startup migration chain.

Covers the guarantees the versioned migration must provide:

- a fresh database walks the whole ordered chain to LATEST_VERSION in one
  chain application and ends with exactly the current schema (all tables,
  columns and indexes the application uses);
- databases created before user_version tracking (version 0) -- with any
  subset of the historical feature columns -- converge to the latest
  version idempotently, while every existing row, index and task history is
  preserved through ALTER steps and the tasks table rebuild;
- each step's structural change and its user_version bump are one
  transaction: a step that raises leaves both the version and its partial
  DDL rolled back, and a later restart resumes exactly at that step after
  the database is repaired;
- concurrent instances opening one legacy file are serialized by the
  SQLite write lock: every instance reaches the latest version, each step
  is applied once, and no instance errors on a step another instance
  already committed;
- a database stamped above this build's LATEST_VERSION is refused;
- PRAGMA foreign_key_check runs after migration (for both fresh-carried and
  legacy-carried violations) and a violation raises MigrationError, which
  the application lifespan treats as fatal before starting workers.

Pure stdlib; the app's own Database is the system under test. Run with

    python3 -m unittest discover -s tests -v
    python3 -m pytest tests/
"""
from __future__ import annotations

import dataclasses
import os
import sqlite3
import tempfile
import threading
import unittest

from app import db as dbmod
from app.db import Database, LATEST_VERSION, MigrationError, _STEPS


def _user_version(path: str) -> int:
    conn = sqlite3.connect(path)
    try:
        return conn.execute("PRAGMA user_version").fetchone()[0]
    finally:
        conn.close()


def _table_names(conn: sqlite3.Connection) -> set[str]:
    return {
        r[0]
        for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' "
            "AND name NOT LIKE 'sqlite_%'"
        )
    }


def _columns(conn: sqlite3.Connection, table: str) -> set[str]:
    return {r[1] for r in conn.execute(f"PRAGMA table_info({table})")}


def _indexes(conn: sqlite3.Connection) -> set[str]:
    return {
        r[0]
        for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='index' "
            "AND name NOT LIKE 'sqlite_%'"
        )
    }


# The exact original (version 1) DDL, written verbatim so the fixtures model
# real database files produced by the oldest release.
_V1_SUBSCRIPTIONS = """
CREATE TABLE subscriptions (
    id TEXT PRIMARY KEY, target_url TEXT NOT NULL,
    secret TEXT NOT NULL, created_at REAL NOT NULL
)
"""
_V1_TASKS = """
CREATE TABLE tasks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id TEXT NOT NULL REFERENCES events(id),
    subscription_id TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending','in_progress','delivered','failed')),
    attempts INTEGER NOT NULL DEFAULT 0,
    max_attempts INTEGER NOT NULL,
    next_attempt_at REAL, claimed_by TEXT,
    last_status_code INTEGER, last_error TEXT,
    result TEXT CHECK (result IN ('delivered','failed')),
    created_at REAL NOT NULL, updated_at REAL NOT NULL,
    UNIQUE (event_id, subscription_id)
)
"""
_V1_ATTEMPTS = """
CREATE TABLE attempts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id INTEGER NOT NULL REFERENCES tasks(id),
    attempt_no INTEGER NOT NULL, status_code INTEGER, error TEXT,
    started_at REAL NOT NULL, finished_at REAL NOT NULL
)
"""
_V1_INDEXES = (
    "CREATE INDEX idx_tasks_due ON tasks (status, next_attempt_at)",
    "CREATE INDEX idx_tasks_sub_order ON tasks (subscription_id, id, status)",
    "CREATE INDEX idx_attempts_task ON attempts (task_id)",
)


def _create_v1_database(path: str, *, fk_violation: bool = False) -> None:
    """Materialize a pre-versioning (user_version 0) version-1 database."""
    conn = sqlite3.connect(path, isolation_level=None)
    try:
        conn.execute(_V1_SUBSCRIPTIONS)
        conn.execute(
            "CREATE TABLE events (id TEXT PRIMARY KEY, payload TEXT NOT NULL,"
            " created_at REAL NOT NULL)"
        )
        conn.execute(_V1_TASKS)
        conn.execute(_V1_ATTEMPTS)
        for stmt in _V1_INDEXES:
            conn.execute(stmt)
        conn.execute("INSERT INTO subscriptions VALUES ('sub1','http://t','s3',1.5)")
        conn.execute("INSERT INTO events VALUES ('evt1','{\"a\":1}',2.5)")
        conn.execute(
            "INSERT INTO tasks (id,event_id,subscription_id,status,attempts,"
            "max_attempts,next_attempt_at,claimed_by,last_status_code,last_error,"
            "result,created_at,updated_at) VALUES "
            "(1,'evt1','sub1','delivered',2,4,NULL,NULL,200,NULL,'delivered',2.5,3.5)"
        )
        conn.execute(
            "INSERT INTO attempts VALUES (1,1,1,500,'boom',2.5,3.0),"
            "(2,1,2,200,NULL,3.0,3.5)"
        )
        if fk_violation:
            # An orphan attempt with no parent task, inserted with enforcement
            # disabled the way a manually repaired old file could carry one.
            conn.execute("PRAGMA foreign_keys=OFF")
            conn.execute(
                "INSERT INTO attempts (task_id,attempt_no,status_code,error,"
                "started_at,finished_at) VALUES (999,1,NULL,'orphan',1,2)"
            )
    finally:
        conn.close()


class FreshDatabaseTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = os.path.join(self.tmp.name, "fresh.db")

    def tearDown(self):
        self.tmp.cleanup()

    def test_fresh_database_reaches_latest_version_with_full_schema(self):
        db = Database(self.path)
        try:
            self.assertEqual(_user_version(self.path), LATEST_VERSION)
            conn = db._conn
            self.assertEqual(
                _table_names(conn),
                {
                    "subscriptions", "events", "tasks", "attempts",
                    "ingested_events", "subscription_topics", "topic_events",
                },
            )
            self.assertEqual(
                _columns(conn, "subscriptions"),
                {
                    "id", "target_url", "secret", "created_at", "paused_at",
                    "previous_secret", "previous_secret_expires_at",
                    "breaker_state", "breaker_consecutive_failures",
                    "breaker_next_probe_at", "breaker_probe_task_id",
                    "rate_capacity", "rate_refill_per_second",
                    "rate_tokens", "rate_updated_at",
                    "window_timezone", "window_weekdays",
                    "window_local_start", "window_local_end",
                    "window_updated_at",
                },
            )
            self.assertEqual(
                _columns(conn, "tasks"),
                {
                    "id", "event_id", "subscription_id", "status", "attempts",
                    "max_attempts", "deliver_at", "next_attempt_at",
                    "claimed_by", "last_status_code", "last_error", "result",
                    "created_at", "updated_at", "cancelled_at",
                },
            )
            self.assertEqual(
                _indexes(conn),
                {
                    "idx_tasks_due", "idx_tasks_sub_order",
                    "idx_attempts_task", "idx_subscription_topics_topic",
                },
            )
            tasks_sql = conn.execute(
                "SELECT sql FROM sqlite_master WHERE name='tasks'"
            ).fetchone()[0]
            self.assertIn("'cancelled'", tasks_sql)
        finally:
            db.close()

    def test_reopen_is_a_noop(self):
        db = Database(self.path)
        db.create_subscription("http://t", "s")
        db.close()
        db = Database(self.path)  # no exception, version unchanged
        try:
            self.assertEqual(_user_version(self.path), LATEST_VERSION)
        finally:
            db.close()


class LegacyUpgradeTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()

    def tearDown(self):
        self.tmp.cleanup()

    def test_oldest_database_upgrades_and_keeps_every_row(self):
        path = os.path.join(self.tmp.name, "v1.db")
        _create_v1_database(path)
        self.assertEqual(_user_version(path), 0)
        db = Database(path)
        try:
            self.assertEqual(_user_version(path), LATEST_VERSION)
            # Subscription row + later columns default safely.
            sub = db.get_subscription("sub1")
            self.assertEqual(sub["target_url"], "http://t")
            self.assertEqual(sub["breaker_state"], "closed")
            self.assertEqual(sub["breaker_consecutive_failures"], 0)
            # Task row survives the rebuild with data intact; history kept.
            status = db.event_status("evt1")
            (task,) = status["tasks"]
            self.assertEqual(task["status"], "delivered")
            self.assertEqual(task["attempts"], 2)
            self.assertIsNone(task["cancelled_at"])
            self.assertIsNone(task["deliver_at"])
            self.assertEqual(len(status["attempts_by_task"][1]), 2)
            self.assertEqual(
                status["attempts_by_task"][1][0]["error"], "boom"
            )
            # The widened CHECK now accepts the 'cancelled' terminal state.
            result = db.cancel_event("evt1")
            self.assertEqual(result["cancelled"], 0)  # task already terminal
        finally:
            db.close()

    def test_upgrade_is_idempotent_across_reopens(self):
        path = os.path.join(self.tmp.name, "idempotent.db")
        _create_v1_database(path)
        for _ in range(3):
            db = Database(path)
            db.close()
        self.assertEqual(_user_version(path), LATEST_VERSION)

    def test_partially_evolved_unversioned_database_converges(self):
        # Models a database that already carried every later feature
        # (subscriptions columns + the cancelled tasks rebuild) but never
        # had deliver_at, the three topic/ingest tables, or a user_version.
        path = os.path.join(self.tmp.name, "partial.db")
        conn = sqlite3.connect(path, isolation_level=None)
        try:
            conn.execute(_V1_SUBSCRIPTIONS.replace(
                "created_at REAL NOT NULL\n)",
                "created_at REAL NOT NULL, paused_at REAL, "
                "previous_secret TEXT, previous_secret_expires_at REAL, "
                "breaker_state TEXT NOT NULL DEFAULT 'closed', "
                "breaker_consecutive_failures INTEGER NOT NULL DEFAULT 0, "
                "breaker_next_probe_at REAL, breaker_probe_task_id INTEGER, "
                "rate_capacity REAL, rate_refill_per_second REAL, "
                "rate_tokens REAL, rate_updated_at REAL, "
                "window_timezone TEXT, window_weekdays TEXT, "
                "window_local_start INTEGER, window_local_end INTEGER, "
                "window_updated_at REAL)",
            ))
            conn.execute(
                "CREATE TABLE events (id TEXT PRIMARY KEY, payload TEXT NOT NULL,"
                " created_at REAL NOT NULL)"
            )
            conn.execute(
                """CREATE TABLE tasks (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_id TEXT NOT NULL REFERENCES events(id),
                    subscription_id TEXT NOT NULL,
                    status TEXT NOT NULL CHECK (status IN
                      ('pending','in_progress','delivered','failed','cancelled')),
                    attempts INTEGER NOT NULL DEFAULT 0,
                    max_attempts INTEGER NOT NULL,
                    next_attempt_at REAL, claimed_by TEXT,
                    last_status_code INTEGER, last_error TEXT,
                    result TEXT CHECK (result IN ('delivered','failed')),
                    created_at REAL NOT NULL, updated_at REAL NOT NULL,
                    cancelled_at REAL,
                    UNIQUE (event_id, subscription_id))"""
            )
            conn.execute(_V1_ATTEMPTS)
            conn.execute("INSERT INTO subscriptions (id,target_url,secret,created_at)"
                         " VALUES ('s','http://t','s',1)")
        finally:
            conn.close()
        db = Database(path)
        try:
            self.assertEqual(_user_version(path), LATEST_VERSION)
            self.assertIn("deliver_at", _columns(db._conn, "tasks"))
            self.assertTrue(db.subscription_exists("s"))
        finally:
            db.close()

    def test_indexes_survive_the_tasks_rebuild(self):
        path = os.path.join(self.tmp.name, "idx.db")
        _create_v1_database(path)
        db = Database(path)
        try:
            self.assertEqual(
                _indexes(db._conn) & {"idx_tasks_due", "idx_tasks_sub_order"},
                {"idx_tasks_due", "idx_tasks_sub_order"},
            )
            # The indexes are usable against the rebuilt table.
            db._conn.execute(
                "SELECT * FROM tasks INDEXED BY idx_tasks_due WHERE status='pending'"
            ).fetchall()
        finally:
            db.close()


class TransactionalStepTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = os.path.join(self.tmp.name, "atomic.db")
        _create_v1_database(self.path)

    def tearDown(self):
        self.tmp.cleanup()

    def test_failed_step_rolls_back_and_restart_resumes(self):
        # Force the version-7 rebuild step (the table rebuild) to fail once,
        # after it has already created tasks_new inside its transaction.
        original = _STEPS[7].apply
        state = {"calls": 0}

        def flaky(conn):
            state["calls"] += 1
            if state["calls"] == 1:
                conn.execute("CREATE TABLE tasks_new (id INTEGER PRIMARY KEY)")
                raise RuntimeError("simulated failure during rebuild")
            return original(conn)

        patched = list(_STEPS)
        patched[7] = dataclasses.replace(_STEPS[7], apply=flaky)
        saved = dbmod._STEPS
        dbmod._STEPS = tuple(patched)
        try:
            with self.assertRaises(RuntimeError):
                Database(self.path)
            self.assertEqual(_user_version(self.path), 6)
            remaining = {
                r[0]
                for r in sqlite3.connect(self.path).execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                )
            }
            self.assertNotIn("tasks_new", remaining)  # partial DDL rolled back
            self.assertIn("tasks", remaining)

            # "Repair" is simply restarting (the injected failure is gone);
            # migration resumes at step 7 and finishes the whole chain.
            db = Database(self.path)
            try:
                self.assertEqual(_user_version(self.path), LATEST_VERSION)
                self.assertEqual(state["calls"], 2)
                self.assertTrue(db.subscription_exists("sub1"))
            finally:
                db.close()
        finally:
            dbmod._STEPS = saved


class ConcurrentStartupTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = os.path.join(self.tmp.name, "concurrent.db")
        _create_v1_database(self.path)

    def tearDown(self):
        self.tmp.cleanup()

    def test_concurrent_instances_are_serialized_and_skip_done_steps(self):
        databases: list[Database] = []
        errors: list[str] = []
        barrier = threading.Barrier(6)

        def open_db() -> None:
            try:
                barrier.wait()  # maximize contention at startup
                databases.append(Database(self.path))
            except Exception as exc:  # pragma: no cover - failure reporting
                errors.append(repr(exc))

        threads = [threading.Thread(target=open_db) for _ in range(6)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        self.assertEqual(errors, [])
        self.assertEqual(len(databases), 6)
        try:
            self.assertEqual(_user_version(self.path), LATEST_VERSION)
            for db in databases:
                self.assertTrue(db.subscription_exists("sub1"))
        finally:
            for db in databases:
                db.close()


class IntegrityGateTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()

    def tearDown(self):
        self.tmp.cleanup()

    def test_fk_violation_carried_from_legacy_db_refuses_startup(self):
        path = os.path.join(self.tmp.name, "fklegacy.db")
        _create_v1_database(path, fk_violation=True)
        with self.assertRaises(MigrationError) as ctx:
            Database(path)
        self.assertIn("foreign-key integrity check failed", str(ctx.exception))

    def test_fk_violation_introduced_after_upgrade_refuses_restart(self):
        path = os.path.join(self.tmp.name, "fkafter.db")
        db = Database(path)
        db.close()
        conn = sqlite3.connect(path)
        try:
            conn.execute("PRAGMA foreign_keys=OFF")
            conn.execute("INSERT INTO events (id,payload,created_at)"
                         " VALUES ('e','{}',1)")
            conn.execute(
                "INSERT INTO tasks (event_id,subscription_id,status,attempts,"
                "max_attempts,created_at,updated_at)"
                " VALUES ('e','ghost','pending',0,4,1,1)"
            )
            conn.execute("DELETE FROM events WHERE id='e'")
            conn.commit()
        finally:
            conn.close()
        with self.assertRaises(MigrationError):
            Database(path)

    def test_future_user_version_is_refused(self):
        path = os.path.join(self.tmp.name, "future.db")
        db = Database(path)
        db.close()
        conn = sqlite3.connect(path)
        try:
            conn.execute(f"PRAGMA user_version={LATEST_VERSION + 1}")
            conn.commit()
        finally:
            conn.close()
        with self.assertRaises(MigrationError):
            Database(path)


if __name__ == "__main__":
    unittest.main()
