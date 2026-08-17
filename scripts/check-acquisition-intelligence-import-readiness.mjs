/**
 * Import readiness check using synthetic fixtures only (no real PII).
 *
 *   node scripts/check-acquisition-intelligence-import-readiness.mjs
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { parseLinkedInConnectionsCsv } from "../lib/acquisition-intelligence/linkedin-connections-parse.js";
import { buildLinkedInConnectionsPreview } from "../lib/acquisition-intelligence/linkedin-connections-preview.js";
import { buildAcquisitionImportPlan } from "../lib/acquisition-intelligence/import-plan.js";
import { classifyAcquisitionRelationship } from "../lib/acquisition-intelligence/classify-relationship.js";
import { buildClassificationReviewSample } from "../lib/acquisition-intelligence/classify-batch.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const FIXTURE = path.join(ROOT, "fixtures", "acquisition-intelligence", "linkedin-connections-synthetic.csv");
const REIMPORT = path.join(
  ROOT,
  "fixtures",
  "acquisition-intelligence",
  "linkedin-connections-reimport-changed.csv"
);

const checks = [];
function pass(name, detail) {
  checks.push({ name, ok: true, detail });
  console.log("PASS", name, detail || "");
}
function fail(name, detail) {
  checks.push({ name, ok: false, detail });
  console.error("FAIL", name, detail || "");
}

const text = fs.readFileSync(FIXTURE, "utf8");
const parsed = parseLinkedInConnectionsCsv(text, { fileName: "Connections.csv" });
if (!parsed.ok) fail("parse_synthetic", parsed.error);
else {
  pass("metadata_header_detection", `headerRowIndex=${parsed.headerRowIndex}`);
  pass("preferred_columns", (parsed.matchedPreferred || []).join(","));
}

const preview = buildLinkedInConnectionsPreview(parsed);
if (!preview.ok || !preview.validation.pass) fail("preview", JSON.stringify(preview.validation));
else {
  pass(
    "preview_stats",
    `detected=${preview.stats.connectionsDetected} dups=${preview.stats.potentialDuplicates} invalid=${preview.stats.invalidRows}`
  );
}

const unique = (parsed.rows || []).filter((r) => !r.duplicateOfRow);
const plan = buildAcquisitionImportPlan(unique, "mem_readiness_user", {
  sourceFileName: "Connections.csv",
});
pass("import_plan", JSON.stringify(plan.summary));

const reParsed = parseLinkedInConnectionsCsv(fs.readFileSync(REIMPORT, "utf8"));
pass("reimport_parse", `rows=${(reParsed.rows || []).length}`);

const classified = unique.map((row) => {
  const out = classifyAcquisitionRelationship({
    position: row.position,
    company: row.company,
    firstName: row.firstName,
    lastName: row.lastName,
  });
  return {
    id: String(row.rowNumber),
    name: row.displayName,
    position: row.position,
    company: row.company,
    ...(out.result || {}),
  };
});
const relevant = classified.filter(
  (r) =>
    r.researchQueueEligibility === "Research Priority" ||
    r.researchQueueEligibility === "Research Candidate"
);
pass(
  "synthetic_classification_relevant",
  `${relevant.length}/${classified.length} relevant on synthetic fixture`
);

const review = buildClassificationReviewSample(classified);
pass(
  "review_sample",
  `direct=${review.topDirectProspects.length} connector=${review.topConnectors.length}`
);

const report = {
  generatedAt: new Date().toISOString(),
  realCsvFoundInWorkspace: false,
  note:
    "No real LinkedIn Connections.csv found on disk. Use /internal/acquisition-intelligence.html while signed in as admin to preview/import Joan's export. Do not commit the file.",
  checks,
  syntheticPreview: preview.stats,
  importPlanSummary: plan.summary,
  classificationRelevantCount: relevant.length,
  apiReady: {
    preview: "POST /api/acquisition-intelligence/connections/preview",
    import: "POST /api/acquisition-intelligence/connections/import",
    classify: "POST /api/acquisition-intelligence/classify",
    ui: "/internal/acquisition-intelligence.html",
  },
};

const out = path.join(ROOT, "reports", "acquisition-intelligence-import-readiness.json");
fs.mkdirSync(path.dirname(out), { recursive: true });
fs.writeFileSync(out, JSON.stringify(report, null, 2));
console.log("\nReport:", out);

if (checks.some((c) => !c.ok)) process.exit(1);
