#!/usr/bin/env node
/**
 * Full E2E Airtable persistence contract + Bethesda reconciliation — deterministic.
 * No production Airtable writes.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  GDI_AIRTABLE_BASE,
  GDI_AIRTABLE_TABLES,
  WRITE_OUTCOME,
  routeResearchObjectToAirtable,
  listCanonicalAirtableRoutingInventory,
  AIRTABLE_PRODUCTION_WRITE_GUARD,
} from "../lib/group-demand-intelligence/workflow/gdi-full-e2e-airtable-persistence-contract-v1.js";
import { dryRunFullE2eAirtablePersistence } from "../lib/group-demand-intelligence/workflow/gdi-full-e2e-airtable-persistence-dry-run-v1.js";
import {
  BETHESDA_LIVE_EXA_RUN,
  reconcileBethesdaLiveExaAirtable,
} from "../lib/group-demand-intelligence/workflow/gdi-bethesda-exa-airtable-reconciliation-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const results = [];
function check(name, fn) {
  try {
    fn();
    results.push({ name, ok: true });
  } catch (e) {
    results.push({ name, ok: false, error: String(e.message || e) });
  }
}

check("canonical_base_and_existing_table_ids", () => {
  assert.equal(GDI_AIRTABLE_BASE.baseId, "appa2cE7FTRmIbB32");
  assert.equal(
    GDI_AIRTABLE_TABLES.GROUP_DEMAND_OPPORTUNITIES.tableId,
    "tblRuReslJMwsfRQj",
  );
  assert.equal(GDI_AIRTABLE_TABLES.DEMAND_GENERATORS.tableId, "tblykUVOjGVkawvdD");
  assert.equal(GDI_AIRTABLE_TABLES.PRIVATE_EVENT_VENUES.tableId, "tblE5p4HjVHfcMDna");
  assert.equal(GDI_AIRTABLE_TABLES.GDI_RESEARCH_RUNS.tableId, "tblzNMUIo2T6onaKH");
  const inv = listCanonicalAirtableRoutingInventory();
  assert.ok(inv.tables.length >= 14);
  assert.ok(inv.doNotCreate.some((x) => /Global Demand Intelligence/i.test(x)));
});

check("org_routes_to_demand_generators", () => {
  const r = routeResearchObjectToAirtable({
    kind: "ORGANIZATION",
    hotelQualified: false,
    outOfMarket: true,
  });
  assert.ok(r.airtable.some((t) => t.table.name === "Demand Generators"));
  assert.ok(!r.airtable.some((t) => t.table.name === "Group Demand Opportunities"));
});

check("qualified_opportunity_routes_to_gdo", () => {
  const r = routeResearchObjectToAirtable({
    kind: "HOTEL_OPPORTUNITY",
    hotelQualified: true,
  });
  assert.ok(
    r.airtable.some((t) => t.table.name === "Group Demand Opportunities"),
  );
  assert.ok(r.airtable.some((t) => t.table.name === "Demand Generators"));
});

check("unsupported_stays_audit_only", () => {
  const r = routeResearchObjectToAirtable({
    kind: "ORGANIZATION",
    unsupported: true,
    credibleGlobal: false,
  });
  assert.equal(r.airtable.length, 0);
  assert.ok(r.nonAirtable.length >= 1);
});

check("venue_and_planner_mapping", () => {
  const v = routeResearchObjectToAirtable({
    kind: "VENUE",
    hotelQualified: true,
  });
  assert.ok(v.airtable.some((t) => t.table.name === "Private Event Venues"));
  assert.ok(v.airtable.some((t) => t.table.name === "Hotel Venue Fit"));
  const p = routeResearchObjectToAirtable({
    kind: "PLANNER",
    segmentId: "professional_event_planners",
    hotelQualified: true,
  });
  assert.ok(p.airtable.some((t) => t.table.name === "Demand Generators"));
  assert.ok(p.airtable.some((t) => t.table.name === "GDI Research Targets"));
});

check("dry_run_refuses_live_apply", () => {
  const blocked = dryRunFullE2eAirtablePersistence({
    apply: true,
    candidates: [{ organizationName: "X", disposition: "EXCLUDE" }],
  });
  assert.equal(blocked.ok, false);
  assert.equal(blocked.code, AIRTABLE_PRODUCTION_WRITE_GUARD.code);
});

check("dry_run_plans_idempotent_identities", () => {
  const plan = dryRunFullE2eAirtablePersistence({
    hotelId: "recLuxvwwxID7U2B8",
    candidates: [
      {
        organizationName: "Amentum Holdings, Inc. / Amentum",
        segmentId: "corporate_accounts_scaling",
        disposition: "WATCHLIST_RESEARCH",
        evaluationLabel: "NEW_UNIQUE",
        opportunityId: "disc_amentum",
        hotelQualified: true,
      },
      {
        organizationName: "Rock Creek Mansion",
        segmentId: "preferred_lodging_partnerships",
        disposition: "EXCLUDE",
        evaluationLabel: "DUPLICATE",
      },
      {
        organizationName: "NASPA",
        segmentId: "associations_conferences_events",
        disposition: "EXCLUDE",
        evaluationLabel: "REJECTED",
      },
    ],
  });
  assert.equal(plan.ok, true);
  assert.equal(plan.dryRun, true);
  assert.ok(plan.plannedWriteCount >= 1);
  assert.ok(plan.deferredAuditOnly >= 1);
  assert.equal(plan.readBackContract.required, true);
  assert.ok(Object.values(WRITE_OUTCOME).includes("CREATED"));
});

check("bethesda_reconciliation_fixture", () => {
  const recon = reconcileBethesdaLiveExaAirtable({ root: ROOT });
  assert.equal(recon.run.runId, BETHESDA_LIVE_EXA_RUN.runId);
  assert.equal(recon.campaignReported.airtableWrites, 0);
  assert.equal(recon.campaignReported.extracted, 165);
  assert.equal(recon.campaignReported.processed, 20);
  assert.ok(
    recon.findings.some((f) => f.code === "DO_NOT_BACKFILL_165_RAW"),
  );
  assert.ok(
    recon.priorAirtableCoverageAudit.groupDemandOpportunitiesBethesda >= 1,
  );
  assert.ok(recon.referenceHotels.length >= 4);
});

check("chain_wires_airtable_dry_run", () => {
  const src = fs.readFileSync(
    path.join(
      ROOT,
      "lib/group-demand-intelligence/workflow/original-full-e2e-research-chain-v1.js",
    ),
    "utf8",
  );
  assert.ok(src.includes("dryRunFullE2eAirtablePersistence"));
  assert.ok(src.includes("airtablePersistenceDryRun"));
});

check("publication_packages_untouched_pointers", () => {
  for (const id of [
    "recLuxvwwxID7U2B8",
    "recrPQcZg7SFARRb2",
    "recH9h3McgMNFQEla",
  ]) {
    assert.ok(
      fs.existsSync(
        path.join(
          ROOT,
          "lib/group-demand-intelligence/bundled-publication-packages",
          id,
          "CURRENT.json",
        ),
      ),
      id,
    );
  }
});

const failed = results.filter((r) => !r.ok);
const recon = reconcileBethesdaLiveExaAirtable({ root: ROOT });
fs.mkdirSync(
  path.join(
    ROOT,
    "reports/gdi-full-e2e-airtable-persistence-reconciliation-v1",
  ),
  { recursive: true },
);
fs.writeFileSync(
  path.join(
    ROOT,
    "reports/gdi-full-e2e-airtable-persistence-reconciliation-v1/BETHESDA_RECONCILIATION.json",
  ),
  `${JSON.stringify(recon, null, 2)}\n`,
);

console.log(
  JSON.stringify(
    {
      contract: "gdi-full-e2e-airtable-persistence-reconciliation-v1",
      passed: results.filter((r) => r.ok).length,
      failed: failed.length,
      results,
      bethesda: {
        runId: recon.run.runId,
        extracted: recon.campaignReported.extracted,
        processed: recon.campaignReported.processed,
        airtableWritesReported: recon.campaignReported.airtableWrites,
        dryRunPlannedWrites:
          recon.dryRunPlanForProcessed20.plannedWriteCount,
      },
      safety: {
        paidResearch: false,
        airtableProductionWrites: false,
        publicationChanges: false,
      },
    },
    null,
    2,
  ),
);
if (failed.length) {
  console.error("FAILED:", failed.map((f) => f.name).join(", "));
  process.exit(1);
}
console.log("OK: Full E2E Airtable persistence reconciliation V1");
