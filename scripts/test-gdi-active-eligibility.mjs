/**
 * Unit tests: GDI active-date eligibility (V5A).
 */
import assert from "node:assert/strict";
import {
  ACTIVE_DATE_CLASS,
  classifyActiveDate,
  isActiveCustomerDateEligible,
  businessDateYmd,
} from "../lib/group-demand-intelligence/active-eligibility-v1.js";

const TODAY = "2026-09-28";

// yesterday → PAST_CLOSED
{
  const r = classifyActiveDate(
    { eventStartDate: "2026-09-27", eventEndDate: "2026-09-27" },
    { nowDate: TODAY }
  );
  assert.equal(r.activeDateClass, ACTIVE_DATE_CLASS.PAST_CLOSED);
  assert.equal(r.activeEligible, false);
}

// today (single day) → ACTIVE_CURRENT
{
  const r = classifyActiveDate(
    { eventStartDate: "2026-09-28", eventEndDate: "2026-09-28" },
    { nowDate: TODAY }
  );
  assert.equal(r.activeDateClass, ACTIVE_DATE_CLASS.ACTIVE_CURRENT);
  assert.equal(r.activeEligible, true);
}

// multi-day ending today → ACTIVE_CURRENT
{
  const r = classifyActiveDate(
    { eventStartDate: "2026-09-26", eventEndDate: "2026-09-28" },
    { nowDate: TODAY }
  );
  assert.equal(r.activeDateClass, ACTIVE_DATE_CLASS.ACTIVE_CURRENT);
  assert.equal(r.activeEligible, true);
}

// event ending yesterday → PAST_CLOSED
{
  const r = classifyActiveDate(
    { eventStartDate: "2026-09-25", eventEndDate: "2026-09-27" },
    { nowDate: TODAY }
  );
  assert.equal(r.activeDateClass, ACTIVE_DATE_CLASS.PAST_CLOSED);
  assert.equal(r.activeEligible, false);
}

// year-only future → ACTIVE_FUTURE
{
  const r = classifyActiveDate({ eventYear: "2027" }, { nowDate: TODAY });
  assert.equal(r.activeDateClass, ACTIVE_DATE_CLASS.ACTIVE_FUTURE);
  assert.equal(r.activeEligible, true);
}

// year-only past → PAST_CLOSED
{
  const r = classifyActiveDate({ eventYear: "2025" }, { nowDate: TODAY });
  assert.equal(r.activeDateClass, ACTIVE_DATE_CLASS.PAST_CLOSED);
  assert.equal(r.activeEligible, false);
}

// unknown date → not automatically active
{
  const r = classifyActiveDate({ title: "Some Meeting" }, { nowDate: TODAY });
  assert.equal(r.activeDateClass, ACTIVE_DATE_CLASS.UNKNOWN_DATE);
  assert.equal(r.activeEligible, false);
}

// past cycle + explicit future motion evidence → PAST_WITH_VALID_FUTURE_MOTION
{
  const r = classifyActiveDate(
    {
      eventStartDate: "2026-05-02",
      eventEndDate: "2026-05-04",
      futureCommercialMotionOpen: true,
      futureCommercialMotionEvidence: "https://example.org/2027-dates",
      futureCycleId: "cycle_2027",
    },
    { nowDate: TODAY }
  );
  assert.equal(r.activeDateClass, ACTIVE_DATE_CLASS.PAST_WITH_VALID_FUTURE_MOTION);
  assert.equal(r.activeEligible, true);
}

// past cycle without evidence stays closed (never invent 2027)
{
  const opp = { eventStartDate: "2026-05-02", eventEndDate: "2026-05-04", title: "CDA 2026" };
  const r = classifyActiveDate(opp, { nowDate: TODAY });
  assert.equal(r.activeDateClass, ACTIVE_DATE_CLASS.PAST_CLOSED);
  assert.equal(isActiveCustomerDateEligible(opp, { nowDate: TODAY }), false);
}

// current ongoing cycle (started earlier, ends later)
{
  const r = classifyActiveDate(
    { eventStartDate: "2026-09-01", eventEndDate: "2026-10-15" },
    { nowDate: TODAY }
  );
  assert.equal(r.activeDateClass, ACTIVE_DATE_CLASS.ACTIVE_CURRENT);
  assert.equal(r.activeEligible, true);
}

// off-by-one: end == today must remain eligible (civil date, not UTC shift inventing yesterday)
{
  const ymd = businessDateYmd(new Date("2026-09-28T04:00:00Z"), "America/New_York");
  assert.equal(ymd, "2026-09-28");
  const r = classifyActiveDate(
    { eventStartDate: "2026-09-28", eventEndDate: "2026-09-28" },
    { nowDate: ymd }
  );
  assert.equal(r.activeEligible, true);
}

console.log("test:gdi-active-eligibility OK");
