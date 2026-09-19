"""Regression tests for delivery-window DST boundary handling.

Focus: the autumn fall-back repeated hour. For a wrap-past-midnight window
(America/New_York Saturday 23:00 -> Sunday 01:30) one opening is bound to
explicit UTC start/end instants: it opens Saturday 23:00 EDT and closes at
the FIRST real Sunday 01:30 (fold=0); the second pass of 01:00-01:30
(fold=1) must neither reopen nor extend the interval. The worker's atomic
claim and the API-facing ``next_open_at`` share this one determination (both
call :meth:`DeliveryWindow.is_open` / :meth:`DeliveryWindow.next_open`), and
weekday attribution, spring-forward gap handling and the
window-gate-before-token-bucket order are preserved.

Pure stdlib: run with either of

    python3 -m unittest discover -s tests -v
    python3 -m pytest tests/
"""
from __future__ import annotations

import json
import os
import tempfile
import unittest
from datetime import datetime, timezone

from app.db import Database
from app.delivery_window import DeliveryWindow

NY = "America/New_York"


def utc(y, mo, d, h, mi, s=0) -> float:
    """Epoch seconds of a UTC wall time."""
    return datetime(y, mo, d, h, mi, s, tzinfo=timezone.utc).timestamp()


# 2026-11-01: US fall-back. 02:00 EDT -> 01:00 EST, so the wall-clock hour
# 01:00-01:59 happens twice: fold=0 in EDT (UTC-4), fold=1 in EST (UTC-5).
SAT_OPEN = utc(2026, 11, 1, 3, 0)        # Sat 2026-10-31 23:00 EDT
FOLD0_0100 = utc(2026, 11, 1, 5, 0)      # first  01:00 (EDT)
FOLD0_0130 = utc(2026, 11, 1, 5, 30)     # first  01:30 (EDT) -- window closes here
FOLD1_0100 = utc(2026, 11, 1, 6, 0)      # second 01:00 (EST)
FOLD1_0130 = utc(2026, 11, 1, 6, 30)     # second 01:30 (EST)
NEXT_SAT_OPEN = utc(2026, 11, 8, 4, 0)   # Sat 2026-11-07 23:00 EST

# 2026-03-08: US spring-forward. 02:00 EST -> 03:00 EDT; 02:00-02:59 never
# exists on the local clock.
SPRING_FIRST_REAL = utc(2026, 3, 8, 7, 0)   # 03:00 EDT, first instant after the gap


def wrap_window(weekdays=(6,)) -> DeliveryWindow:
    """Saturday 23:00 -> Sunday 01:30 in New York (wraps past midnight)."""
    return DeliveryWindow(NY, weekdays, 23 * 3600, 90 * 60)


class FallBackWrapWindowTests(unittest.TestCase):
    """The open interval is bound to [Sat 23:00 EDT, first Sun 01:30 EDT)."""

    def setUp(self):
        self.window = wrap_window()

    def test_opens_at_saturday_2300(self):
        self.assertFalse(self.window.is_open(SAT_OPEN - 1))
        self.assertTrue(self.window.is_open(SAT_OPEN))

    def test_first_fold_pass_stays_open_until_first_0130(self):
        self.assertTrue(self.window.is_open(utc(2026, 11, 1, 4, 30)))  # 00:30 EDT
        self.assertTrue(self.window.is_open(FOLD0_0100))               # 01:00 EDT
        self.assertTrue(self.window.is_open(FOLD0_0130 - 1))           # 01:29:59 EDT

    def test_closes_at_first_0130(self):
        # Half-open interval: the first real 01:30 is the close instant.
        self.assertFalse(self.window.is_open(FOLD0_0130))
        self.assertFalse(self.window.is_open(utc(2026, 11, 1, 5, 45)))  # 01:45 EDT

    def test_second_fold_does_not_reopen_or_extend(self):
        self.assertFalse(self.window.is_open(FOLD1_0100))        # second 01:00 EST
        self.assertFalse(self.window.is_open(FOLD1_0100 + 900))  # second 01:15 EST
        self.assertFalse(self.window.is_open(FOLD1_0130 - 1))    # second 01:29:59 EST
        self.assertFalse(self.window.is_open(FOLD1_0130))        # second 01:30 EST
        self.assertFalse(self.window.is_open(utc(2026, 11, 1, 7, 0)))  # 02:00 EST

    def test_second_fold_closed_even_when_sunday_also_allowed(self):
        # Sunday's own segment opens Sunday 23:00, so the repeated-hour tail
        # of Saturday's segment still closes at the first 01:30.
        window = wrap_window(weekdays=(6, 7))
        self.assertFalse(window.is_open(FOLD1_0100 + 900))

    def test_next_open_from_second_fold_is_next_saturday(self):
        for ts in (FOLD0_0130, FOLD1_0100, FOLD1_0100 + 900, FOLD1_0130):
            self.assertEqual(self.window.next_open(ts), NEXT_SAT_OPEN, f"at {ts}")
        # The reported opening is an instant the window is actually open:
        # next_open_at and the claim gate share one determination.
        self.assertTrue(self.window.is_open(NEXT_SAT_OPEN))

    def test_next_open_before_opening_is_same_saturday(self):
        self.assertEqual(self.window.next_open(SAT_OPEN - 1), SAT_OPEN)


class WeekdayAttributionTests(unittest.TestCase):
    """The post-midnight tail belongs to the weekday the window opened on."""

    def test_tail_attributed_to_opening_weekday(self):
        sunday_0030 = utc(2026, 11, 1, 4, 30)  # Sun 00:30 EDT
        self.assertTrue(wrap_window((6,)).is_open(sunday_0030))   # Saturday's tail
        self.assertFalse(wrap_window((7,)).is_open(sunday_0030))  # not Sunday's own

    def test_sundays_own_segment_opens_sunday_night(self):
        window = wrap_window((6, 7))
        sunday_open = utc(2026, 11, 2, 4, 0)  # Sun 2026-11-01 23:00 EST
        self.assertFalse(window.is_open(sunday_open - 1))
        self.assertTrue(window.is_open(sunday_open))


class FallBackNonWrapTests(unittest.TestCase):
    """Non-wrap windows are bound to the first occurrence the same way."""

    def test_non_wrap_window_closes_at_first_occurrence(self):
        window = DeliveryWindow(NY, (7,), 30 * 60, 90 * 60)  # Sun 00:30-01:30
        self.assertTrue(window.is_open(utc(2026, 11, 1, 4, 30)))   # 00:30 EDT
        self.assertTrue(window.is_open(FOLD0_0100 + 900))          # 01:15 EDT
        self.assertFalse(window.is_open(FOLD0_0130))               # first 01:30
        self.assertFalse(window.is_open(FOLD1_0100 + 900))         # second 01:15
        self.assertEqual(window.next_open(FOLD0_0130), utc(2026, 11, 8, 5, 30))


class SpringForwardTests(unittest.TestCase):
    """Spring-forward gap handling is unchanged by the interval binding."""

    def test_opening_inside_gap_resolves_to_first_real_instant(self):
        window = DeliveryWindow(NY, (7,), 2 * 3600 + 1800, 4 * 3600)  # Sun 02:30-04:00
        self.assertEqual(window.next_open(utc(2026, 3, 8, 5, 0)), SPRING_FIRST_REAL)
        self.assertFalse(window.is_open(SPRING_FIRST_REAL - 1))
        self.assertTrue(window.is_open(SPRING_FIRST_REAL))
        self.assertTrue(window.is_open(utc(2026, 3, 8, 7, 59, 59)))   # 03:59:59 EDT
        self.assertFalse(window.is_open(utc(2026, 3, 8, 8, 0)))       # 04:00 EDT

    def test_wrap_window_ending_in_gap_closes_at_the_jump(self):
        window = DeliveryWindow(NY, (6,), 23 * 3600, 2 * 3600 + 1800)  # -> Sun 02:30
        self.assertTrue(window.is_open(utc(2026, 3, 8, 4, 0)))         # Sat 23:00 EST
        self.assertTrue(window.is_open(utc(2026, 3, 8, 6, 59, 59)))    # 01:59:59 EST
        # 02:30 never happens; the clock jumps 02:00 -> 03:00 and closes it.
        self.assertFalse(window.is_open(SPRING_FIRST_REAL))

    def test_window_fully_inside_gap_never_opens_that_day(self):
        window = DeliveryWindow(NY, (7,), 2 * 3600, 3 * 3600)  # Sun 02:00-03:00
        self.assertFalse(window.is_open(SPRING_FIRST_REAL))
        # next_open skips the empty interval: next Sunday 02:00 (real, EDT).
        self.assertEqual(window.next_open(utc(2026, 3, 8, 5, 0)), utc(2026, 3, 15, 6, 0))


class OrdinaryWindowTests(unittest.TestCase):
    """Sanity: non-DST behavior of the interval binding (2026-09-21 = Monday)."""

    def test_non_wrap_window(self):
        window = DeliveryWindow("UTC", (1, 2, 3, 4, 5), 9 * 3600, 18 * 3600)
        self.assertFalse(window.is_open(utc(2026, 9, 21, 8, 59, 59)))
        self.assertTrue(window.is_open(utc(2026, 9, 21, 9, 0)))
        self.assertTrue(window.is_open(utc(2026, 9, 21, 17, 59, 59)))
        self.assertFalse(window.is_open(utc(2026, 9, 21, 18, 0)))
        self.assertFalse(window.is_open(utc(2026, 9, 20, 12, 0)))  # Sunday
        self.assertEqual(window.next_open(utc(2026, 9, 20, 12, 0)), utc(2026, 9, 21, 9, 0))
        self.assertEqual(window.next_open(utc(2026, 9, 21, 18, 0)), utc(2026, 9, 22, 9, 0))

    def test_wrap_window_ordinary_days(self):
        window = DeliveryWindow("Asia/Shanghai", (1, 2, 3, 4, 5, 6, 7), 22 * 3600, 6 * 3600)
        self.assertFalse(window.is_open(utc(2026, 9, 21, 13, 59, 59)))  # Mon 21:59:59
        self.assertTrue(window.is_open(utc(2026, 9, 21, 14, 0)))        # Mon 22:00
        self.assertTrue(window.is_open(utc(2026, 9, 22, 21, 59, 59)))   # Tue 05:59:59
        self.assertFalse(window.is_open(utc(2026, 9, 22, 22, 0)))       # Tue 06:00

    def test_full_day_window(self):
        window = DeliveryWindow("UTC", (3,), 0, 0)  # Wednesday 00:00-00:00
        self.assertTrue(window.is_open(utc(2026, 9, 23, 0, 0)))
        self.assertTrue(window.is_open(utc(2026, 9, 23, 23, 59, 59)))
        self.assertFalse(window.is_open(utc(2026, 9, 24, 0, 0)))
        self.assertEqual(window.next_open(utc(2026, 9, 22, 12, 0)), utc(2026, 9, 23, 0, 0))


class WorkerClaimTests(unittest.TestCase):
    """The worker's atomic claim applies the same fall-back determination."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = Database(os.path.join(self.tmp.name, "test.db"))
        self.sub_id = self.db.create_subscription("http://localhost:9/hook", "s")["id"]
        self.db.set_delivery_window(self.sub_id, NY, [6], 23 * 3600, 90 * 60)

    def tearDown(self):
        self.db.close()
        self.tmp.cleanup()

    def _new_task(self, event_id: str) -> dict:
        self.db.create_event(event_id, json.dumps({"n": 1}))
        task, created = self.db.create_task(event_id, self.sub_id, 4)
        self.assertTrue(created)
        return task

    def _next_attempt_at(self, task_id: int) -> float:
        return self.db.get_delivery_task(task_id)["task"]["next_attempt_at"]

    def test_claim_during_first_fold_succeeds(self):
        task = self._new_task("evt-fold0")
        claimed = self.db.claim_due_task("w1", FOLD0_0100 + 900)  # 01:15 EDT
        self.assertIsNotNone(claimed)
        self.assertEqual(claimed["id"], task["id"])

    def test_claim_during_second_fold_defers_to_next_saturday(self):
        task = self._new_task("evt-fold1")
        # Second pass of 01:00-01:30: the window must not reopen, so the head
        # task stays pending and is pushed to the next real opening.
        self.assertIsNone(self.db.claim_due_task("w1", FOLD1_0100 + 900))
        self.assertEqual(self._next_attempt_at(task["id"]), NEXT_SAT_OPEN)
        # The persisted instant is exactly what next_open_at reports (one
        # shared determination), and the task becomes claimable there.
        window = wrap_window()
        self.assertEqual(self._next_attempt_at(task["id"]), window.next_open(FOLD1_0100 + 900))
        claimed = self.db.claim_due_task("w1", NEXT_SAT_OPEN)
        self.assertIsNotNone(claimed)
        self.assertEqual(claimed["id"], task["id"])

    def test_window_gate_runs_before_token_bucket(self):
        # Full bucket, closed window (second fold): the claim is refused by
        # the window gate alone -- no token is spent and the reschedule
        # instant is the window's next opening, not a token-refill time.
        self.db.set_rate_policy(self.sub_id, 1.0, 1.0)
        task = self._new_task("evt-gate-order")
        self.assertIsNone(self.db.claim_due_task("w1", FOLD1_0100 + 900))
        self.assertEqual(self._next_attempt_at(task["id"]), NEXT_SAT_OPEN)
        policy = self.db.get_rate_policy(self.sub_id)
        self.assertEqual(policy["tokens_available"], 1.0)
        self.assertIsNone(policy["next_token_at"])


if __name__ == "__main__":
    unittest.main()
