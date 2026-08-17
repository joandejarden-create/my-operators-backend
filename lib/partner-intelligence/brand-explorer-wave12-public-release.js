/**
 * Wave 12 Stage 10 — Public release / restore.
 *
 * Allowed writes (Wave 12 Basics only):
 * - Active Profile Approved
 * - Ready for Active Profile
 * - Active Profile Approved Date
 * - Founder Visual Review Pass
 * - intentional public restore registry inclusion
 *
 * Forbidden: Brand Status (already promoted), CV / Source / Registry,
 * content, images, protected 27, Radisson Collection.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { getBrandLibraryBrandById } from "../../api/brand-library.js";
import { isBrandStatusActive } from "../brand-status-active.js";
import { loadActiveUniverse } from "./brand-explorer-active-universe.js";
import {
  readIntentionalPublicRestoreSlugs,
  writeIntentionalPublicRestoreSlugs,
} from "./brand-explorer-public-restore-registry.js";
import {
  WAVE12_VERSION,
  WAVE12_SLUGS,
  WAVE12_PROTECTED_BASELINE_COUNT,
  WAVE12_EXPECTED_FINAL_ACTIVE_COUNT,
  WAVE12_PUBLIC_RELEASE_APPLY_FLAGS,
  WAVE12_RELEASE_FIELDS,
  WAVE12_NEVER_WRITE_FIELDS,
  WAVE12_FOUNDER_APPROVE_RECOMMENDATION,
} from "./brand-explorer-wave12-factory-plan.js";
import { FACTORY_PREVIEW_CANDIDATE_IDENTITIES } from "./brand-explorer-factory-preview-candidates.js";

export const WAVE12_PUBLIC_RELEASE_VERSION = "wave12-public-release-v1";

const BASICS_TABLE = "Brand Setup - Brand Basics";
const WRITE_THROTTLE_MS = 260;
const RADISSON_COLLECTION_SLUG = "radisson-collection";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../..");
const REPORTS_DIR = path.join(ROOT, "reports");

function nz(v) {
  return v == null ? "" : String(v).trim();
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function todayIsoDate() {
  return new Date().toISOString().slice(0, 10);
}

function mockRes() {
  return {
    statusCode: 200,
    payload: null,
    setHeader() {},
    status(c) {
      this.statusCode = c;
      return this;
    },
    json(p) {
      this.payload = p;
    },
  };
}

function checkFlags(required, argv, apply) {
  const missing = required.filter((f) => !argv.includes(f));
  return {
    apply: apply === true,
    ok: apply === true && missing.length === 0,
    missing,
    required: [...required],
  };
}

function readJsonSafe(relOrAbs) {
  const p = path.isAbsolute(relOrAbs) ? relOrAbs : path.join(ROOT, relOrAbs);
  if (!fs.existsSync(p)) return null;
  try {
    return JSON.parse(fs.readFileSync(p, "utf8"));
  } catch (err) {
    if (process.env.NODE_ENV !== "production") {
      console.warn("[wave12-public-release] json read failed", p, err?.message || err);
    }
    return null;
  }
}

function loadProtected27RecordIds() {
  const freeze =
    readJsonSafe("reports/brand-explorer-27-active-public-full-baseline.json") ||
    readJsonSafe("docs/data-intelligence/brand-explorer-27-active-public-full-baseline.json");
  const ids = new Set();
  for (const b of freeze?.brands || []) {
    const id = nz(b.recordId || b.id);
    if (id) ids.add(id);
  }
  return ids;
}

function wave12Identities() {
  return WAVE12_SLUGS.map((slug) => {
    const id = FACTORY_PREVIEW_CANDIDATE_IDENTITIES[slug];
    if (!id?.recordId) throw new Error(`Missing factory-preview identity for ${slug}`);
    return { slug, name: id.name, recordId: id.recordId };
  });
}

async function fetchBrand(recordId) {
  const res = mockRes();
  await getBrandLibraryBrandById({ query: { brandId: recordId }, headers: {} }, res);
  if (res.statusCode >= 400 || !res.payload?.brand) {
    throw new Error(`fetch failed ${recordId}: ${res.statusCode}`);
  }
  return res.payload.brand;
}

function buildReleaseFields(today = todayIsoDate()) {
  return {
    "Active Profile Approved": true,
    "Ready for Active Profile": true,
    "Active Profile Approved Date": today,
    "Founder Visual Review Pass": true,
  };
}

function assertReleasePayload(fields) {
  const keys = Object.keys(fields || {});
  for (const k of keys) {
    if (WAVE12_NEVER_WRITE_FIELDS.includes(k)) {
      throw new Error(`Refuse: never-write field in release payload: ${k}`);
    }
    if (k === "Brand Status") {
      throw new Error("Refuse: public-release must not write Brand Status");
    }
    if (!WAVE12_RELEASE_FIELDS.includes(k)) {
      throw new Error(`Refuse: unexpected release field: ${k}`);
    }
  }
  for (const required of WAVE12_RELEASE_FIELDS) {
    if (!(required in fields)) {
      throw new Error(`Refuse: missing required release field: ${required}`);
    }
  }
}

async function patchBasicsRelease({ recordId, fields }) {
  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey) throw new Error("AIRTABLE_BASE_ID and AIRTABLE_API_KEY required");
  assertReleasePayload(fields);

  const url = `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(BASICS_TABLE)}/${recordId}`;
  const res = await fetch(url, {
    method: "PATCH",
    headers: {
      Authorization: `Bearer ${apiKey}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ fields }),
  });
  const json = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(json.error?.message || `PATCH Basics failed: ${res.status}`);
  return { id: json.id, fieldsPatched: Object.keys(fields), sanitizedPayloadPreview: fields };
}

function assertFounderApprovals() {
  const summary = readJsonSafe("reports/brand-explorer-wave12-founder-review-summary.json");
  const issues = [];
  if (!summary) {
    issues.push("missing_founder_review_summary");
    return { ok: false, issues };
  }
  const counts = summary.counts || {};
  if ((counts[WAVE12_FOUNDER_APPROVE_RECOMMENDATION] || 0) !== WAVE12_SLUGS.length) {
    issues.push("founder_approve_count_incomplete");
  }
  return { ok: issues.length === 0, issues };
}

function writeReports(report, md) {
  fs.mkdirSync(REPORTS_DIR, { recursive: true });
  const jsonPath = path.join(REPORTS_DIR, "brand-explorer-wave12-public-release.json");
  const mdPath = path.join(REPORTS_DIR, "brand-explorer-wave12-public-release.md");
  fs.writeFileSync(jsonPath, `${JSON.stringify(report, null, 2)}\n`, "utf8");
  fs.writeFileSync(mdPath, md.endsWith("\n") ? md : `${md}\n`, "utf8");
  return { jsonPath, mdPath };
}

function renderMarkdown(r) {
  const lines = [];
  lines.push(`# Brand Explorer Wave 12 — Public Release`);
  lines.push("");
  lines.push(`Version: \`${r.version}\` · Generated: ${r.generatedAt}`);
  lines.push(`Mode: **${r.applyPerformed ? "APPLY" : "dry-run"}** · writePerformed: **${r.writePerformed}**`);
  lines.push("");
  lines.push(`Active universe: **${r.universe?.totalCount ?? "n/a"}** (expected ${WAVE12_EXPECTED_FINAL_ACTIVE_COUNT})`);
  lines.push("");
  lines.push("## Planned release fields");
  lines.push("");
  for (const f of r.plannedReleaseFields || []) lines.push(`- \`${f}\``);
  lines.push("");
  lines.push("## Brand readiness");
  lines.push("");
  lines.push("| Slug | Status | Active/Live | Needs release write |");
  lines.push("| --- | --- | --- | --- |");
  for (const b of r.brands || []) {
    lines.push(
      `| ${b.slug} | ${b.brandStatus || "(empty)"} | ${b.isActiveOrLive} | ${b.needsWrite} |`
    );
  }
  lines.push("");
  lines.push("## Apply outcome");
  lines.push("");
  lines.push("```json");
  lines.push(JSON.stringify(r.applyOutcome, null, 2));
  lines.push("```");
  lines.push("");
  lines.push("## Intentional restore registry");
  lines.push("");
  lines.push(`- Before count: ${r.intentionalRegistry?.beforeCount ?? "n/a"}`);
  lines.push(`- After count: ${r.intentionalRegistry?.afterCount ?? "n/a"}`);
  lines.push(`- Wave12 added: ${(r.intentionalRegistry?.wave12Added || []).join(", ") || "(none)"}`);
  lines.push("");
  lines.push("## Guardrails");
  lines.push("");
  for (const [k, v] of Object.entries(r.guardrails || {})) {
    lines.push(`- ${k}: ${Array.isArray(v) ? v.join(", ") : v}`);
  }
  lines.push("");
  return lines.join("\n");
}

/**
 * @param {{ apply?: boolean, argv?: string[] }} opts
 */
export async function runWave12PublicRelease({ apply = false, argv = [] } = {}) {
  const stage = "public-release";
  const flagCheck = checkFlags(WAVE12_PUBLIC_RELEASE_APPLY_FLAGS, argv, apply);
  const identities = wave12Identities();
  const protectedIds = loadProtected27RecordIds();
  const preflightIssues = [];
  const today = todayIsoDate();
  const releaseFields = buildReleaseFields(today);

  for (const id of identities) {
    if (protectedIds.has(id.recordId)) {
      preflightIssues.push(`target_collides_with_protected_27:${id.slug}`);
    }
    if (id.slug === RADISSON_COLLECTION_SLUG) {
      preflightIssues.push("radisson_collection_in_targets");
    }
  }

  const founderApprovals = assertFounderApprovals();
  if (!founderApprovals.ok) preflightIssues.push(...founderApprovals.issues);

  const universe = await loadActiveUniverse({ includeDetails: false });
  if (universe.totalCount < WAVE12_PROTECTED_BASELINE_COUNT) {
    preflightIssues.push(`active_universe_too_small:${universe.totalCount}`);
  }

  const brands = [];
  for (const id of identities) {
    const live = await fetchBrand(id.recordId);
    await sleep(80);
    const status = nz(live.brandStatus || live.status);
    const isActiveOrLive = isBrandStatusActive(status);
    if (!isActiveOrLive) {
      preflightIssues.push(`brand_status_not_active_or_live:${id.slug}:${status || "(empty)"}`);
    }

    const basics = live.basics || live.brandBasics || live;
    const alreadyApproved =
      basics["Active Profile Approved"] === true ||
      live.activeProfileApproved === true ||
      live.releaseRestoreStatus?.activeProfileApproved === true;
    const alreadyReady =
      basics["Ready for Active Profile"] === true ||
      live.readyForActiveProfile === true ||
      live.releaseRestoreStatus?.readyForActiveProfile === true;
    const alreadyFounder =
      basics["Founder Visual Review Pass"] === true ||
      live.founderVisualReviewPass === true ||
      live.releaseRestoreStatus?.founderVisualReviewPass === true;

    const needsWrite = !(alreadyApproved && alreadyReady && alreadyFounder);
    brands.push({
      slug: id.slug,
      name: id.name,
      recordId: id.recordId,
      brandStatus: status,
      isActiveOrLive,
      needsWrite,
      already: { alreadyApproved, alreadyReady, alreadyFounder },
      sanitizedPayloadPreview: releaseFields,
      plannedFields: [...WAVE12_RELEASE_FIELDS],
    });
  }

  const allActive = brands.every((b) => b.isActiveOrLive);
  const preflightOk = preflightIssues.length === 0 && allActive && founderApprovals.ok;

  const applyPerformed = apply === true && flagCheck.ok === true && preflightOk;
  const applyResults = [];
  let writePerformed = false;
  let intentionalRegistryOutcome = null;

  if (applyPerformed) {
    for (const b of brands) {
      if (!b.needsWrite) {
        applyResults.push({
          slug: b.slug,
          recordId: b.recordId,
          applied: false,
          reason: "release_fields_already_set",
          writePerformed: false,
        });
        continue;
      }
      try {
        const response = await patchBasicsRelease({
          recordId: b.recordId,
          fields: releaseFields,
        });
        writePerformed = true;
        applyResults.push({
          slug: b.slug,
          recordId: b.recordId,
          applied: true,
          writePerformed: true,
          table: BASICS_TABLE,
          fieldMapping: {
            activeProfileApproved: "Active Profile Approved",
            readyForActiveProfile: "Ready for Active Profile",
            activeProfileApprovedDate: "Active Profile Approved Date",
            founderVisualReviewPass: "Founder Visual Review Pass",
          },
          sanitizedPayloadPreview: response.sanitizedPayloadPreview,
          response: { id: response.id, fieldsPatched: response.fieldsPatched },
        });
      } catch (err) {
        applyResults.push({
          slug: b.slug,
          recordId: b.recordId,
          applied: false,
          writePerformed: false,
          error: err.message,
        });
      }
      await sleep(WRITE_THROTTLE_MS);
    }

    const before = readIntentionalPublicRestoreSlugs();
    const beforeSet = new Set(before);
    const wave12Added = WAVE12_SLUGS.filter((s) => !beforeSet.has(s));
    // Append Wave 12 only; do not remove existing entries (incl. historical Radisson).
    const merged = [...new Set([...before, ...WAVE12_SLUGS])];
    const registry = writeIntentionalPublicRestoreSlugs(merged);
    intentionalRegistryOutcome = {
      updated: true,
      beforeCount: before.length,
      afterCount: registry.slugs.length,
      wave12Added,
      path: "data/brand-explorer-public-restore-intentional.json",
      note: "Wave 12 slugs added for intentional public restore; no removals; no content/CV/Source/Registry writes.",
    };
    writePerformed = true;
  }

  let applyOutcome;
  if (applyPerformed) {
    applyOutcome = {
      applied: applyResults.some((r) => r.applied) || intentionalRegistryOutcome?.updated === true,
      results: applyResults,
      intentionalRegistry: intentionalRegistryOutcome,
    };
  } else if (apply && !flagCheck.ok) {
    applyOutcome = { applied: false, reason: "missing_apply_flags", missing: flagCheck.missing };
  } else if (apply && !preflightOk) {
    applyOutcome = { applied: false, reason: "preflight_failed", issues: preflightIssues };
  } else {
    applyOutcome = { applied: false, reason: "dry_run_only", plannedBrandWrites: brands.filter((b) => b.needsWrite).length };
  }

  const report = {
    version: WAVE12_PUBLIC_RELEASE_VERSION,
    waveVersion: WAVE12_VERSION,
    stage,
    generatedAt: new Date().toISOString(),
    apply,
    applyPerformed,
    writePerformed,
    dryRun: !applyPerformed,
    flagCheck,
    requiredApplyFlags: [...WAVE12_PUBLIC_RELEASE_APPLY_FLAGS],
    plannedReleaseFields: [...WAVE12_RELEASE_FIELDS],
    releaseDate: today,
    preflight: {
      ok: preflightOk,
      issues: preflightIssues,
      founderApprovals,
      allBrandStatusActiveOrLive: allActive,
    },
    universe: {
      totalCount: universe.totalCount,
      expectedAfterPromotion: WAVE12_EXPECTED_FINAL_ACTIVE_COUNT,
      wave12InUniverse: (universe.brands || [])
        .filter((b) => WAVE12_SLUGS.includes(nz(b.slug).toLowerCase()))
        .map((b) => b.slug),
    },
    brands,
    applyOutcome,
    intentionalRegistry: intentionalRegistryOutcome || {
      beforeCount: readIntentionalPublicRestoreSlugs().length,
      plannedAdd: WAVE12_SLUGS.filter((s) => !readIntentionalPublicRestoreSlugs().includes(s)),
    },
    fieldMapping: {
      activeProfileApproved: "Active Profile Approved",
      readyForActiveProfile: "Ready for Active Profile",
      activeProfileApprovedDate: "Active Profile Approved Date",
      founderVisualReviewPass: "Founder Visual Review Pass",
    },
    guardrails: {
      targetBrandsOnly: true,
      companyValidatedWrites: false,
      sourceLibraryWrites: false,
      registryApprovalWrites: false,
      brandStatusWrites: false,
      contentRewrites: false,
      imageWrites: false,
      protected27Untouched: true,
      radissonCollectionUntouched: true,
      neverWriteFields: [...WAVE12_NEVER_WRITE_FIELDS],
      intentionalPublicRestoreRegistryUpdate: applyPerformed === true,
    },
    expectedAcceptance: {
      shouldRenderFullProfile: true,
      displayState: "active_profile_ready",
      activeUniverse: WAVE12_EXPECTED_FINAL_ACTIVE_COUNT,
      publicFullPvql: `${WAVE12_EXPECTED_FINAL_ACTIVE_COUNT}/${WAVE12_EXPECTED_FINAL_ACTIVE_COUNT}`,
    },
  };

  const paths = writeReports(report, renderMarkdown(report));
  return { report, paths };
}
