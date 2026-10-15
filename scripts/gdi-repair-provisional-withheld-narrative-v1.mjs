#!/usr/bin/env node
/**
 * Scoped repair: provisional numerics must not keep stale "overall withheld"
 * copy in fitExplanation / summaryWhyHotel / hotelOpportunityThesis.
 *
 * Does not rescore. Clears assessment-status prose from those sales fields only;
 * preserves hotelFitRationale / dimension assessments / contacts.
 *
 *   node scripts/gdi-repair-provisional-withheld-narrative-v1.mjs --hotel rec0eTEnCqlsGJJt9 --dry-run
 *   node scripts/gdi-repair-provisional-withheld-narrative-v1.mjs --hotel rec0eTEnCqlsGJJt9 --apply
 */
import "dotenv/config";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  loadOpportunitiesCanonical,
  saveOpportunitiesCanonical,
} from "../lib/group-demand-intelligence/opportunity-persistence.js";
import { isAssessmentStatusProse } from "../lib/group-demand-intelligence/customer-presentation-sanitize-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");

function arg(name, fallback = null) {
  const i = process.argv.indexOf(name);
  if (i < 0) return fallback;
  return process.argv[i + 1] || fallback;
}
function has(flag) {
  return process.argv.includes(flag);
}

const apply = has("--apply");
const hotelId = arg("--hotel") || "rec0eTEnCqlsGJJt9";
const idsFilter = new Set(
  String(arg("--ids", "") || "")
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean)
);
const outDir = arg("--out-dir") || path.join(ROOT, "reports/gdi-quality-review/narrative-repair");
fs.mkdirSync(outDir, { recursive: true });
fs.mkdirSync(path.join(outDir, "backups"), { recursive: true });

process.env.GDI_OPPORTUNITIES_PERSISTENCE = process.env.GDI_OPPORTUNITIES_PERSISTENCE || "airtable";

const CLEAR_KEYS = ["fitExplanation", "fitRationale", "summaryWhyHotel", "hotelOpportunityThesis"];

function isProvisionalNumeric(o) {
  const status = String(o.hotelFitOverallStatus || "").toLowerCase();
  const provisional =
    o.hotelFitProvisional === true ||
    status === "provisional" ||
    o.hotelFitCalculationBasis?.provisional === true;
  const n = Number(o.hotelFitScore);
  return provisional && Number.isFinite(n);
}

function needsRepair(o) {
  if (!isProvisionalNumeric(o)) return false;
  return CLEAR_KEYS.some((k) => isAssessmentStatusProse(o[k]));
}

function repairOne(o) {
  const before = {};
  const next = { ...o };
  for (const k of CLEAR_KEYS) {
    if (isAssessmentStatusProse(next[k])) {
      before[k] = next[k];
      next[k] = null;
    }
  }
  // If whyThisHotel is only assessment prose or equals occasion, clear it.
  if (isAssessmentStatusProse(next.whyThisHotel)) {
    before.whyThisHotel = next.whyThisHotel;
    next.whyThisHotel = null;
  }
  next.narrativeRepairVersion = "gdi-repair-provisional-withheld-narrative-v1";
  next.narrativeRepairedAt = new Date().toISOString();
  return { next, before };
}

const loaded = await loadOpportunitiesCanonical(hotelId);
const opps = Array.isArray(loaded?.opportunities)
  ? loaded.opportunities
  : Array.isArray(loaded)
    ? loaded
    : [];

const targets = opps.filter((o) => {
  const id = o.opportunityId || o.id;
  if (idsFilter.size && !idsFilter.has(id)) return false;
  return needsRepair(o);
});

const stamp = new Date().toISOString().replace(/[:.]/g, "-");
const backupPath = path.join(outDir, "backups", `${hotelId}-opportunities-${stamp}.json`);
fs.writeFileSync(backupPath, JSON.stringify(loaded, null, 2) + "\n");

const diffs = [];
const repaired = opps.map((o) => {
  const id = o.opportunityId || o.id;
  if (!targets.some((t) => (t.opportunityId || t.id) === id)) return o;
  const { next, before } = repairOne(o);
  diffs.push({
    id,
    title: o.title || null,
    hotelFitScore: o.hotelFitScore,
    hotelFitOverallStatus: o.hotelFitOverallStatus,
    cleared: before,
  });
  return next;
});

const planPath = path.join(outDir, `${hotelId}-plan-${stamp}.json`);
fs.writeFileSync(
  planPath,
  JSON.stringify(
    {
      hotelId,
      apply,
      backupPath,
      count: diffs.length,
      diffs,
    },
    null,
    2
  ) + "\n"
);

console.log(
  JSON.stringify(
    {
      hotelId,
      mode: apply ? "apply" : "dry-run",
      candidates: diffs.length,
      ids: diffs.map((d) => d.id),
      backupPath,
      planPath,
    },
    null,
    2
  )
);

if (!apply) {
  console.log("Dry-run only. Re-run with --apply to persist.");
  process.exit(0);
}

const payload = Array.isArray(loaded?.opportunities)
  ? { ...loaded, opportunities: repaired, updatedAt: new Date().toISOString() }
  : repaired;

await saveOpportunitiesCanonical(hotelId, payload);
const readBack = await loadOpportunitiesCanonical(hotelId);
const rb = Array.isArray(readBack?.opportunities)
  ? readBack.opportunities
  : Array.isArray(readBack)
    ? readBack
    : [];
const still = rb.filter(needsRepair);
console.log(
  JSON.stringify(
    {
      applied: true,
      remainingConflicts: still.map((o) => o.opportunityId || o.id),
    },
    null,
    2
  )
);
if (still.length) process.exit(2);
