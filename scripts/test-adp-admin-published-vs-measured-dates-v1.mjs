#!/usr/bin/env node
/**
 * Unit + local durable-prefer gates for Admin Published vs Measured dates.
 * Does not deploy.
 */
import assert from "node:assert/strict";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  monthYearFromPeriodId,
  monthYearFromPublicationTimestamp,
  resolveAuthoritativePublishedColumn,
} from "../lib/ai-demand-positioning/monthly-review/admin/resolve-admin-publication-columns-v1.js";
import { ADP_FUTURE_PUBLICATION_WORKFLOW_V1 } from "../lib/ai-demand-positioning/monthly-review/admin/adp-admin-publication-parity-authority-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");

process.env.ADP_DURABLE_PUBLISHED_MODE = "prefer";
process.env.ADP_DURABLE_PUBLISHED_ROOT = path.join(
  ROOT,
  "reports/dealality-adp-single-version-publication-contract-v1/production-activation/durable-staging-prefer"
);

// Measurement period October ≠ publication November
assert.equal(
  monthYearFromPeriodId(
    "adp_period_adp_casas_del_xvi_20261020091515_9971ab"
  ),
  "October 2026"
);
assert.equal(
  monthYearFromPublicationTimestamp("2026-11-06T07:55:28.324Z"),
  "November 2026"
);
assert.equal(monthYearFromPublicationTimestamp(null), null);
assert.equal(monthYearFromPublicationTimestamp(""), null);

const casas = resolveAuthoritativePublishedColumn("adp_casas_del_xvi");
assert.equal(casas.measuredMonthYear, "October 2026");
assert.equal(casas.publishedMonthYear, "November 2026");
assert.ok(casas.publishedAt);
assert.equal(casas.publicationTimestampMissing, false);
// Critical: Published must not equal Measured when they differ
assert.notEqual(casas.publishedMonthYear, casas.measuredMonthYear);

const ac = resolveAuthoritativePublishedColumn("adp_ac_hotel_a_coruna");
assert.equal(ac.measuredMonthYear, "November 2026");
assert.equal(ac.publishedMonthYear, "November 2026");
assert.ok(ac.publishedAt);

assert.ok(
  ADP_FUTURE_PUBLICATION_WORKFLOW_V1.forbidden.includes(
    "infer_published_month_from_measurement_period_id"
  )
);
assert.ok(
  ADP_FUTURE_PUBLICATION_WORKFLOW_V1.steps.includes(
    "PERSIST_PUBLICATION_TIMESTAMP"
  )
);

console.log(
  JSON.stringify(
    {
      ok: true,
      gate: "ADP_ADMIN_PUBLISHED_VS_MEASURED_DATES_V1",
      casas: {
        measured: casas.measuredMonthYear,
        published: casas.publishedMonthYear,
        publishedAt: casas.publishedAt,
      },
      ac: {
        measured: ac.measuredMonthYear,
        published: ac.publishedMonthYear,
      },
    },
    null,
    2
  )
);
