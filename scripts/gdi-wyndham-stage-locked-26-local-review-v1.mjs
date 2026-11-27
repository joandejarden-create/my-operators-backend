#!/usr/bin/env node
/**
 * Wyndham San Diego Bayside — stage locked accepted qualified 26 for local
 * founder review on :8080 (FULL E2E → STAGED lifecycle).
 *
 * Founder authorization (this instruction):
 *   FOUNDER_GO AUTHORIZE_WYNDHAM_STAGED_LOCKED_26_LOCAL_REVIEW
 *
 * - Builds STAGED from CLEAN11 + locked accepted qualified additions (26)
 * - Does NOT include HOLD / continuations / discovery 161 as qualified
 * - Does NOT write Airtable
 * - Does NOT publish CURRENT
 * - Preserves Bethesda pointers
 *
 * Usage:
 *   node scripts/gdi-wyndham-stage-locked-26-local-review-v1.mjs --dry-run
 *   node scripts/gdi-wyndham-stage-locked-26-local-review-v1.mjs --apply \
 *     --auth "FOUNDER_GO AUTHORIZE_WYNDHAM_STAGED_LOCKED_26_LOCAL_REVIEW"
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { createGdiPublicationPackageStore } from "../lib/group-demand-intelligence/publication-package/index.js";
import { resolveWyndhamFounderInventoryPreview } from "../lib/group-demand-intelligence/publication-package/gdi-wyndham-proposed-26-founder-preview-v1.js";
import { stageQualifiedInventoryFromE2e } from "../lib/group-demand-intelligence/publication-package/gdi-canonical-e2e-to-staged-lifecycle-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const OUT = path.join(
  ROOT,
  "reports/gdi-wyndham-stage-locked-26-local-review-v1",
);
const HOTEL_ID = "recnPB3Q02NO8Ma1o";
const BETHESDA = "recLuxvwwxID7U2B8";
const AUTH =
  "FOUNDER_GO AUTHORIZE_WYNDHAM_STAGED_LOCKED_26_LOCAL_REVIEW";

fs.mkdirSync(OUT, { recursive: true });

function writeJson(name, data) {
  fs.writeFileSync(path.join(OUT, name), `${JSON.stringify(data, null, 2)}\n`);
}

const args = process.argv.slice(2);
const dryRun = args.includes("--dry-run") || !args.includes("--apply");
const authIdx = args.indexOf("--auth");
const auth =
  authIdx >= 0 ? String(args[authIdx + 1] || "").trim() : dryRun ? AUTH : "";

if (!dryRun && auth !== AUTH) {
  writeJson("00-BLOCKED.json", {
    reason: "AUTH_REQUIRED",
    expected: AUTH,
    got: auth || null,
  });
  console.error("BLOCKED: exact founder auth phrase required for --apply");
  process.exit(2);
}

const store = createGdiPublicationPackageStore({});
const bethStagedBefore = store.readPointer(BETHESDA, "STAGED");
const bethCurrentBefore = store.readPointer(BETHESDA, "CURRENT");
const wyStagedBefore = store.readPointer(HOTEL_ID, "STAGED");
const wyCurrentBefore = store.readPointer(HOTEL_ID, "CURRENT");

const preview = resolveWyndhamFounderInventoryPreview({ bucket: "proposed" });
if (!preview.ok || preview.count !== 26) {
  writeJson("00-BLOCKED.json", {
    reason: "LOCKED_26_HYDRATION_FAILED",
    preview,
  });
  console.error("BLOCKED: expected 26 hydrated qualified opportunities");
  process.exit(3);
}

const stagedPkgBefore = wyStagedBefore?.publicationVersion
  ? store.loadPackage(HOTEL_ID, wyStagedBefore.publicationVersion)
  : null;
const stagedRows =
  stagedPkgBefore?.ok && Array.isArray(stagedPkgBefore.package?.opportunities)
    ? stagedPkgBefore.package.opportunities
    : [];
const stagedById = new Map(
  stagedRows.map((o) => [String(o.id || o.opportunityId), o]),
);

// Prefer rich STAGED CLEAN11 rows; fill additions from hydrated preview.
const opportunities = preview.opportunities.map((row) => {
  const id = String(row.id || row.opportunityId);
  const rich = stagedById.get(id);
  if (rich) {
    return { ...rich, id };
  }
  return { ...row, id };
});

const result = stageQualifiedInventoryFromE2e({
  hotelId: HOTEL_ID,
  hotelName: "Wyndham San Diego Bayside",
  opportunities,
  dryRun,
  activate: !dryRun,
  runId: `wyndham_locked26_local_${Date.now()}`,
  notes: {
    auth: dryRun ? "DRY_RUN" : AUTH,
    at: new Date().toISOString(),
    source: "08-PROPOSED_WRITE_SET_QA.json locked accepted qualified 26",
    priorStaged: wyStagedBefore?.publicationVersion || null,
    note: "Founder-authorized local STAGED for review — CURRENT unchanged — no Airtable",
  },
});

const bethStagedAfter = store.readPointer(BETHESDA, "STAGED");
const bethCurrentAfter = store.readPointer(BETHESDA, "CURRENT");
const wyCurrentAfter = store.readPointer(HOTEL_ID, "CURRENT");

const integrity = {
  bethesdaStagedUnchanged:
    (bethStagedBefore?.publicationVersion || null) ===
    (bethStagedAfter?.publicationVersion || null),
  bethesdaCurrentUnchanged:
    (bethCurrentBefore?.publicationVersion || null) ===
    (bethCurrentAfter?.publicationVersion || null),
  wyndhamCurrentUnchanged:
    (wyCurrentBefore?.publicationVersion || null) ===
    (wyCurrentAfter?.publicationVersion || null),
  wyndhamCurrentStillNull: (wyCurrentAfter?.publicationVersion || null) === null,
  countIs26: result.opportunityCount === 26,
};

writeJson("01-STAGE_RESULT.json", {
  dryRun,
  auth: dryRun ? null : AUTH,
  result: {
    ok: result.ok,
    activated: result.activated === true,
    publicationVersion: result.publicationVersion || null,
    opportunityCount: result.opportunityCount || null,
    opportunityIds: result.opportunityIds || null,
    priorStagedVersion: result.priorStagedVersion || null,
    currentUnchanged: result.currentUnchanged,
    error: result.error || null,
    fails: result.fails || null,
  },
  integrity,
  pointers: {
    bethStaged: bethStagedAfter?.publicationVersion || null,
    bethCurrent: bethCurrentAfter?.publicationVersion || null,
    wyStaged: store.readPointer(HOTEL_ID, "STAGED")?.publicationVersion || null,
    wyCurrent: wyCurrentAfter?.publicationVersion || null,
  },
});

if (!result.ok) {
  console.error("FAIL", result.error || result);
  process.exit(10);
}

if (
  !integrity.bethesdaStagedUnchanged ||
  !integrity.bethesdaCurrentUnchanged ||
  !integrity.wyndhamCurrentStillNull ||
  !integrity.countIs26
) {
  writeJson("00-INTEGRITY_FAIL.json", { integrity, result });
  console.error("FAIL integrity", integrity);
  process.exit(11);
}

console.log(
  JSON.stringify(
    {
      ok: true,
      dryRun,
      activated: result.activated === true,
      publicationVersion: result.publicationVersion,
      opportunityCount: result.opportunityCount,
      currentUnchanged: true,
      outDir: OUT,
    },
    null,
    2,
  ),
);
