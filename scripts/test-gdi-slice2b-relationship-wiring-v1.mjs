#!/usr/bin/env node
/**
 * Slice 2B — offline DG / Partner / Program / Signal relationship wiring ($0).
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  wireOpportunityRelationshipsOffline,
  assertPartnerReuseAcrossOpportunities,
  assertCrossHotelSharedIdentityIsolation,
  buildRelationshipManifest,
  createMemoryDemandGeneratorGraphStore,
  WRITE_OUTCOME,
} from "../lib/group-demand-intelligence/workflow/gdi-slice2b-relationship-wiring-v1.js";
import { buildSlimResearchPayload } from "../lib/group-demand-intelligence/workflow/gdi-research-inventory-slim-payload-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const FIX = path.join(ROOT, "fixtures/gdi-slice2b-relationship-wiring-v1");
const REPORT = path.join(ROOT, "reports/gdi-slice2b-relationship-wiring-v1");

function loadJson(rel) {
  return JSON.parse(fs.readFileSync(path.join(ROOT, rel), "utf8"));
}

const store = createMemoryDemandGeneratorGraphStore();
const runs = [];

// 1) NADO primary from Slice 2A fixture
const nado = loadJson(
  "fixtures/gdi-slice2a-persistence-honesty-v1/recLuxvwwxID7U2B8-gdi_opp_nado_ddaa_washcon_2028.json",
);
const nadoRun = wireOpportunityRelationshipsOffline(nado, { store });
runs.push(nadoRun);
assert.ok(nadoRun.plan.identity.dgGeneratorId);
assert.ok(nadoRun.plan.identity.hotelGeneratorFitId);
assert.ok(nadoRun.plan.identity.dgSignalId);
assert.equal(
  nadoRun.applied.results.find((r) => r.kind === "DEMAND_GENERATOR")?.outcome,
  WRITE_OUTCOME.CREATED,
);
assert.ok(
  nadoRun.applied.results.some(
    (r) =>
      r.kind === "DEMAND_PROGRAM" && r.outcome === WRITE_OUTCOME.DEFERRED,
  ),
  "Program without recurrence evidence must defer",
);
assert.ok(nadoRun.readBack.demandGenerator);
assert.equal(
  nadoRun.readBack.demandGenerator.demandGeneratorId,
  nadoRun.plan.identity.dgGeneratorId,
);

// 2) ICPIC + partner planner → distinct partner DG
const icpic = JSON.parse(
  fs.readFileSync(path.join(FIX, "icpic-with-partner-planner.json"), "utf8"),
);
const icpicRun = wireOpportunityRelationshipsOffline(icpic, { store });
runs.push(icpicRun);
assert.ok(icpicRun.plan.identity.dgGeneratorId);
assert.ok(icpicRun.plan.identity.partnerDgGeneratorId);
assert.notEqual(
  icpicRun.plan.identity.dgGeneratorId,
  icpicRun.plan.identity.partnerDgGeneratorId,
);
assert.equal(
  icpicRun.plan.candidates.filter((c) => c.role === "PARTNER_OR_PLANNER").length,
  1,
);

// 3) GuidePoint cross-hotel shared DG
const gp = JSON.parse(
  fs.readFileSync(path.join(FIX, "guidepoint-cross-hotel.json"), "utf8"),
);
const gpStore = createMemoryDemandGeneratorGraphStore();
const gpRuns = gp.hotels.map((h) =>
  wireOpportunityRelationshipsOffline(
    {
      organizationName: gp.organizationName,
      officialSource: gp.officialSource,
      sources: gp.sources,
      segment: gp.segment,
      eventStartDate: gp.eventStartDate,
      title: gp.title,
      demandThesis: gp.demandThesis,
      hotelId: h.hotelId,
      hotelName: h.hotelName,
      opportunityId: h.opportunityId,
    },
    { store: gpStore },
  ),
);
runs.push(...gpRuns);
assert.equal(
  gpRuns[0].plan.identity.dgGeneratorId,
  gpRuns[1].plan.identity.dgGeneratorId,
);
assert.notEqual(
  gpRuns[0].plan.identity.hotelGeneratorFitId,
  gpRuns[1].plan.identity.hotelGeneratorFitId,
);
assert.equal(
  gpRuns[1].applied.results.find((r) => r.kind === "DEMAND_GENERATOR")?.outcome,
  WRITE_OUTCOME.UNCHANGED,
);
const isolation = assertCrossHotelSharedIdentityIsolation({
  hotelAOpportunity: gpRuns[0].stampedOpportunity,
  hotelBOpportunity: gpRuns[1].stampedOpportunity,
});
assert.equal(isolation.ok, true);
const reuse = assertPartnerReuseAcrossOpportunities(
  gpRuns.map((r) => r.stampedOpportunity),
);
assert.equal(reuse.ok, true);

// 4) Confidential strip
const leak = JSON.parse(
  fs.readFileSync(path.join(FIX, "confidential-feedback-plant.json"), "utf8"),
);
const leakRun = wireOpportunityRelationshipsOffline(leak, {
  store: createMemoryDemandGeneratorGraphStore(),
});
runs.push(leakRun);
for (const p of leakRun.plan.plans) {
  assert.equal(p.privacyOk, true, `${p.kind} privacy`);
  const blob = JSON.stringify(p.entity);
  assert.equal(blob.includes("CONFIDENTIAL_MUST_NOT_ENTER_DG_PAYLOAD"), false);
  assert.equal(blob.includes("ShouldNotAppearOnDemandGenerator"), false);
  assert.equal(p.entity.hotelSalesFeedback, undefined);
}
const slim = buildSlimResearchPayload(leakRun.stampedOpportunity);
assert.ok(slim.ok);
assert.equal(slim.payload?.hotelSalesFeedback, undefined);
assert.ok(slim.payload?.dgGeneratorId);

// 5) Recurring program created
const prog = JSON.parse(
  fs.readFileSync(path.join(FIX, "recurring-program-evidence.json"), "utf8"),
);
const progRun = wireOpportunityRelationshipsOffline(prog, {
  store: createMemoryDemandGeneratorGraphStore(),
});
runs.push(progRun);
assert.ok(progRun.plan.identity.dgProgramId);
assert.equal(
  progRun.applied.results.find((r) => r.kind === "DEMAND_PROGRAM")?.outcome,
  WRITE_OUTCOME.CREATED,
);
assert.ok(progRun.readBack.program);

// 6) Idempotent second pass on NADO store
const nadoSecond = wireOpportunityRelationshipsOffline(nado, { store });
assert.equal(
  nadoSecond.applied.results.find((r) => r.kind === "DEMAND_GENERATOR")?.outcome,
  WRITE_OUTCOME.UNCHANGED,
);

const manifest = buildRelationshipManifest(runs);
fs.mkdirSync(REPORT, { recursive: true });
const summary = {
  version: "gdi-slice2b-relationship-wiring-v1",
  at: new Date().toISOString(),
  paidCalls: 0,
  airtableWrites: 0,
  productionWrites: 0,
  newTables: 0,
  priorPrs: { slice2a: 444, pdi1: 446 },
  outcomes: {
    nadoDg: nadoRun.plan.identity.dgGeneratorId,
    icpicPartnerDistinct: true,
    guidepointSharedDg: gpRuns[0].plan.identity.dgGeneratorId,
    guidepointFitA: gpRuns[0].plan.identity.hotelGeneratorFitId,
    guidepointFitB: gpRuns[1].plan.identity.hotelGeneratorFitId,
    confidentialStripped: true,
    programWithEvidence: progRun.plan.identity.dgProgramId,
    idempotentUnchanged: true,
  },
  manifest,
  remainingGaps: [
    "Live Airtable dry-run adapter still previews DG only — production Program/Signal/Fit apply remains founder-gated",
    "Research Target continuation persistence deferred to Slice 2C",
    "PDI-2 portfolio API authorization still required before customer portfolio UI",
  ],
  recommendedNext:
    "After this PR merges: Track B PDI-2 read-only portfolio API (then Brief v2 + PDI-3 coordinated)",
};
fs.writeFileSync(
  path.join(REPORT, "RESULTS.json"),
  `${JSON.stringify(summary, null, 2)}\n`,
);
fs.writeFileSync(
  path.join(REPORT, "FOUNDER_RESULTS.md"),
  `# GDI Slice 2B — Relationship wiring results

Generated: ${summary.at}

## Safety
- Paid calls: 0
- Airtable writes: 0
- New tables: 0
- Production writes: 0

## Proven
- Stable primary \`dgGeneratorId\` stamped and read back
- Partner/planner org gets independent DG id when distinct
- Cross-hotel shared org → one DG, distinct hotel fits
- Confidential hotel feedback stripped from DG/Program/Signal entities
- Program upsert deferred without recurrence evidence; created when evidenced
- Second pass → UNCHANGED (idempotent)

## Remaining gaps
${summary.remainingGaps.map((g) => `- ${g}`).join("\n")}

## Next
${summary.recommendedNext}
`,
);

console.log(
  JSON.stringify(
    {
      ok: true,
      dg: summary.outcomes.nadoDg,
      sharedDg: summary.outcomes.guidepointSharedDg,
      reportDir: REPORT,
    },
    null,
    2,
  ),
);
