#!/usr/bin/env node
/**
 * Verify private-event path integration on Bethesda (evidence-supported hotel).
 * Local / fixture-capable. No production writes. No paid research.
 *
 *   node scripts/gdi-workflow-v2-pe-path-verify.mjs
 */
import "dotenv/config";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { spawnSync } from "node:child_process";
import {
  buildReusePriorIntelligenceLedger,
  reuseLedgerToCsv,
  researchPathSelectionToCsv,
} from "../lib/group-demand-intelligence/workflow/reuse-prior-intelligence-v1.js";
import {
  buildPrivateEventIntelligence,
  peIntelligenceToCsv,
  PE_DISPOSITION,
} from "../lib/group-demand-intelligence/workflow/private-event-intelligence-v1.js";
import { selectResearchPaths } from "../lib/group-demand-intelligence/workflow/research-path-selection-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const OUT = path.join(ROOT, "reports/gdi-workflow-v2-integration");
const BETHESDA = "recLuxvwwxID7U2B8";
const MAYFLOWER = "rec0eTEnCqlsGJJt9";

fs.mkdirSync(OUT, { recursive: true });

function write(name, data) {
  const p = path.join(OUT, name);
  if (typeof data === "string") fs.writeFileSync(p, data);
  else fs.writeFileSync(p, JSON.stringify(data, null, 2) + "\n");
  return p;
}

const report = { asOf: new Date().toISOString(), assertions: [], hotels: {} };
function assert(name, ok, detail = {}) {
  report.assertions.push({ name, ok: Boolean(ok), ...detail });
  console.log(ok ? "PASS" : "FAIL", name, detail.note || "");
}

const pe = await buildPrivateEventIntelligence(BETHESDA, {
  skipAirtable: process.env.GDI_V2_SKIP_AIRTABLE === "1",
});
write("bethesda-private-event-intelligence.json", pe);
write("bethesda-private-event-intelligence.csv", peIntelligenceToCsv(pe));

assert("bethesda_pe_corpus_loaded", (pe.counts?.venues || 0) > 0 && (pe.counts?.fits || 0) > 0, {
  venues: pe.counts?.venues,
  fits: pe.counts?.fits,
  signals: pe.counts?.signals,
  source: pe.source,
});

const partnership = (pe.rows || []).filter(
  (r) => r.disposition === PE_DISPOSITION.VENUE_REFERRAL_PARTNERSHIP
);
const alreadyLinked = (pe.rows || []).filter(
  (r) => r.disposition === PE_DISPOSITION.ALREADY_LINKED_OPPORTUNITY
);
const rejectedOrBackground = (pe.rows || []).filter((r) =>
  [
    PE_DISPOSITION.REJECTED_INSUFFICIENT,
    PE_DISPOSITION.BACKGROUND_ONLY,
    PE_DISPOSITION.MEETING_ANCILLARY,
  ].includes(r.disposition)
);

assert(
  "bethesda_has_supported_pe_disposition",
  partnership.length > 0 || alreadyLinked.length > 0,
  {
    partnership: partnership.length,
    alreadyLinked: alreadyLinked.length,
    sample: (partnership[0] || alreadyLinked[0])?.organizationName,
    disposition: (partnership[0] || alreadyLinked[0])?.disposition,
  }
);

assert(
  "no_fabricated_customer_facing_pe_promote",
  (pe.rows || []).every((r) => r.promote !== true && r.customerFacingCandidate !== true),
  { rows: pe.rows?.length }
);

assert(
  "pe_preserves_source_ids",
  (pe.rows || []).some((r) => r.preserveIds?.peVenueId || r.peVenueId),
  {}
);

assert(
  "internal_or_rejected_dispositions_present_when_warranted",
  rejectedOrBackground.length > 0 || (pe.counts?.signals || 0) === 0 || alreadyLinked.length > 0,
  {
    rejectedOrBackground: rejectedOrBackground.length,
    note: "Signals without lodging evidence should land background/rejected — not forced prospects",
  }
);

const reuse = await buildReusePriorIntelligenceLedger(BETHESDA, {
  skipAirtable: process.env.GDI_V2_SKIP_AIRTABLE === "1",
});
write("bethesda-reuse-ledger-with-pe.json", reuse);
write("bethesda-reuse-ledger-with-pe.csv", reuseLedgerToCsv(reuse));
write(
  "bethesda-research-path-selection.json",
  reuse.researchPathSelection
);
write(
  "bethesda-research-path-selection.csv",
  researchPathSelectionToCsv(reuse.researchPathSelection)
);

assert(
  "path_selection_records_pe_paths",
  (reuse.researchPathSelection?.paths || []).some((p) =>
    /private_event/.test(p.pathId)
  ),
  { summary: reuse.researchPathSelection?.summary }
);

assert(
  "no_silent_path_skip",
  (reuse.researchPathSelection?.paths || []).every((p) => p.status && p.selectionRule),
  { pathCount: reuse.researchPathSelection?.paths?.length }
);

assert(
  "reuse_ledger_includes_pe_rows",
  (reuse.counts?.private_event_intelligence || 0) > 0,
  { counts: reuse.counts }
);

// Mayflower: PE path should still be assessed (not silently N/A solely due to empty local PE DB)
const mayPaths = selectResearchPaths(MAYFLOWER, {
  evidenceHints: {
    opportunityCount: 37,
    peVenueCount: 0,
    peFitCount: 0,
    peSignalCount: 0,
    airtableBlocked: true,
    airtableBlockReason: "airtable_not_configured",
  },
});
write("mayflower-research-path-selection.json", mayPaths);
const mayPe = (mayPaths.paths || []).filter((p) => /private_event|hotel_venue_fit/.test(p.pathId));
assert(
  "mayflower_pe_not_silently_skipped",
  mayPe.length === 3 && mayPe.every((p) => p.status === "blocked" || p.status === "incomplete"),
  {
    statuses: mayPe.map((p) => `${p.pathId}:${p.status}`),
    note: "Empty PE DB → blocked/incomplete, not not_applicable",
  }
);

const wf = spawnSync(
  process.execPath,
  [
    path.join(ROOT, "scripts/gdi-research-to-page-workflow-v1.mjs"),
    "--hotel",
    BETHESDA,
    "--dry-run",
    "--stage",
    "reuse_evidence",
    "--skip-airtable-reuse",
    "--out-dir",
    path.join(OUT, "workflow-runs"),
  ],
  { cwd: ROOT, encoding: "utf8" }
);
write("bethesda-pe-workflow-stdout.txt", wf.stdout || "");
assert("workflow_bethesda_reuse_with_pe_exit_0", wf.status === 0, {
  status: wf.status,
  stderr: (wf.stderr || "").slice(0, 400),
});

report.hotels.bethesda = {
  peSource: pe.source,
  dispositions: pe.counts?.byDisposition,
  partnershipSample: (partnership[0] || alreadyLinked[0]) || null,
  pathSummary: reuse.researchPathSelection?.summary,
};
report.assertionSummary = {
  pass: report.assertions.filter((a) => a.ok).length,
  fail: report.assertions.filter((a) => !a.ok).length,
};
write("_pe-path-verify-summary.json", report);

console.log(
  JSON.stringify(
    { ok: report.assertionSummary.fail === 0, out: OUT, assertionSummary: report.assertionSummary },
    null,
    2
  )
);
process.exit(report.assertionSummary.fail === 0 ? 0 : 1);
