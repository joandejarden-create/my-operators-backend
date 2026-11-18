#!/usr/bin/env node
/**
 * Offline gates for $0 Exa commercial rescore.
 * No paid calls. No Airtable writes.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { spawnSync } from "node:child_process";
import { classifyCommercialDemandThesis } from "../lib/group-demand-intelligence/commercial-demand-thesis-standard-v1.js";
import {
  hydrateGuestroomConfidenceFromDemandThesis,
  runRestoredCommercialEligibilityPass,
} from "../lib/group-demand-intelligence/workflow/gdi-original-full-e2e-execution-restore-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const OUT = path.join(
  ROOT,
  "reports/gdi-post-exa-zero-cost-commercial-rescore-v1",
);
const PS1 = path.join(ROOT, "scripts/gdi-workflow.ps1");
const DEMAND = path.join(
  ROOT,
  "lib/group-demand-intelligence/demand-mechanism-research-v1.js",
);

let failed = 0;
function assert(cond, msg) {
  if (!cond) {
    failed += 1;
    console.error("FAIL:", msg);
  } else {
    console.log("ok:", msg);
  }
}

const construction = classifyCommercialDemandThesis({
  evidence: [
    {
      statement:
        "Whiting-Turner is general contractor for The Grandview in Bethesda; construction and staged opening/move-ins are underway.",
    },
  ],
  title:
    "Whiting-Turner is general contractor for The Grandview; construction and staged opening/move-ins are underway.",
});
assert(
  construction.thesisStandard === "RELIABLE_INFERRED_DEMAND",
  "construction GC + staged move-ins → RELIABLE_INFERRED",
);

const speculative = classifyCommercialDemandThesis({
  hotelOpportunityThesis:
    "This company might someday need hotel rooms near Bethesda.",
  sources: [{ title: "About us", url: "https://example.org/about" }],
});
assert(
  speculative.thesisStandard === "SPECULATIVE_DEMAND",
  "generic thesis without independent trigger stays SPECULATIVE",
);

const hydrated = hydrateGuestroomConfidenceFromDemandThesis(
  {
    commercialDemandType: "GUEST_ROOMS",
    guestroomDemandConfidence: null,
  },
  {
    thesisStandard: "RELIABLE_INFERRED_DEMAND",
    mayQualifySalesReadyDemandGate: true,
  },
);
assert(
  hydrated.guestroomDemandConfidence === "RELIABLE_INFERRED",
  "hydrate guestroomDemandConfidence from RELIABLE demand thesis",
);

const noHydrateSpeculative = hydrateGuestroomConfidenceFromDemandThesis(
  { commercialDemandType: "GUEST_ROOMS", guestroomDemandConfidence: null },
  {
    thesisStandard: "SPECULATIVE_DEMAND",
    mayQualifySalesReadyDemandGate: false,
  },
);
assert(
  !noHydrateSpeculative.guestroomDemandConfidence,
  "do not hydrate confidence from SPECULATIVE demand",
);

const hqDemand = classifyCommercialDemandThesis({
  evidence: [
    {
      statement:
        "GuidePoint Security opened a new Reston, Virginia headquarters supporting growth, customer engagement, collaboration and expanded services.",
    },
  ],
  title: "New HQ supports growth, customer engagement, collaboration and expanded services.",
  whatIsHappening:
    "GuidePoint Security is headquartered in Reston, Virginia with documented HQ opening activity.",
});
assert(
  hqDemand.thesisStandard === "RELIABLE_INFERRED_DEMAND",
  "GuidePoint-style HQ opening → RELIABLE_INFERRED",
);
const hqHydrated = runRestoredCommercialEligibilityPass({
  id: "fixture_gp_hydrate",
  organizationName: "GuidePoint Security",
  commercialDemandType: "GUEST_ROOMS",
  guestroomDemandConfidence: null,
  evidence: [
    {
      statement:
        "GuidePoint Security opened a new Reston, Virginia headquarters supporting growth and customer engagement.",
    },
  ],
  whatIsHappening:
    "GuidePoint Security opened a new Reston, Virginia headquarters supporting growth and customer engagement.",
});
assert(
  hqHydrated.opportunity.guestroomDemandConfidence === "RELIABLE_INFERRED" ||
    hqDemand.mayQualifySalesReadyDemandGate === true,
  "HQ trigger qualifies demand gate / hydrates guestroom confidence when eligibility runs",
);

const demandSrc = fs.readFileSync(DEMAND, "utf8");
assert(
  /general contractor/.test(demandSrc),
  "demand mechanism includes general contractor",
);
assert(/staged/.test(demandSrc), "demand mechanism includes staged opening/move-ins");

const ps1 = fs.readFileSync(PS1, "utf8");
assert(/Canonical/.test(ps1), "gdi-workflow.ps1 exposes Canonical switch");
assert(
  /gdi-canonical-full-e2e-entry-v1/.test(ps1),
  "Canonical path invokes canonical full e2e entry",
);

const run = spawnSync(
  process.execPath,
  [path.join(ROOT, "scripts/gdi-post-exa-zero-cost-commercial-rescore-v1.mjs")],
  { cwd: ROOT, encoding: "utf8", timeout: 120_000 },
);
assert(run.status === 0, `rescore script exits 0 (got ${run.status})`);

const required = [
  "FOUNDER_EXECUTIVE_SUMMARY.md",
  "RECOVERED_CONTACTS_AND_SIX_OPPORTUNITIES.md",
  "DEMAND_QUALIFICATION_BLOCKERS.md",
  "COMMERCIAL_CONVERSION_BEFORE_AFTER.md",
  "OPPORTUNITY_DRAWER_VERIFICATION.md",
  "POWERSHELL_CANONICAL_WORKFLOW_STATUS.md",
  "NEXT_RESEARCH_ACTIONS.md",
  "TEST_RESULTS.md",
  "PROCESSED_OPPORTUNITY_EXPORT.json",
  "SIX_OPPORTUNITY_RESCORE.json",
];
for (const name of required) {
  assert(fs.existsSync(path.join(OUT, name)), `report exists: ${name}`);
}

const processed = JSON.parse(
  fs.readFileSync(path.join(OUT, "PROCESSED_OPPORTUNITY_EXPORT.json"), "utf8"),
);
const named = (processed.processed || []).filter((r) =>
  String(r.primaryContactName || "").trim(),
);
assert(named.length >= 6, `hydrated export has ≥6 named contacts (got ${named.length})`);

const six = JSON.parse(
  fs.readFileSync(path.join(OUT, "SIX_OPPORTUNITY_RESCORE.json"), "utf8"),
);
const wt = (six.opportunities || []).find((o) =>
  /Whiting-Turner/i.test(o.organizationName || ""),
);
assert(
  wt?.afterRescore?.demandThesisStandard === "RELIABLE_INFERRED_DEMAND",
  "Whiting-Turner after rescore is RELIABLE_INFERRED",
);

if (failed) {
  console.error(`\n${failed} assertion(s) failed`);
  process.exit(1);
}
console.log("test-gdi-post-exa-zero-cost-commercial-rescore-v1: PASS");
