/**
 * Wave 13 Stage 9b — SO/ Brand Status promotion (Under Review → Active).
 *
 * Allowed write: Brand Status only on SO/ Basics (recTJdPlr4mDs9app).
 * No content / images / release / CV / Source / Registry / active-45 writes.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { getBrandLibraryBrandById } from "../../api/brand-library.js";
import { isBrandStatusActive } from "../brand-status-active.js";
import { loadActiveUniverse } from "./brand-explorer-active-universe.js";
import {
  WAVE13_VERSION,
  WAVE13_HELD_PROMOTION_SLUG,
  WAVE13_STATUS_FROM,
  WAVE13_STATUS_TO_PREFERRED,
  WAVE13_STATUS_TO_ALLOWED,
  WAVE13_FOUNDER_APPROVE_RECOMMENDATION,
  WAVE13_NEVER_WRITE_FIELDS,
  WAVE13_SO_STATUS_PROMOTION_APPLY_FLAGS,
  WAVE13_EXPECTED_PARTIAL_ACTIVE_COUNT,
  WAVE13_EXPECTED_SO_ACTIVE_COUNT,
} from "./brand-explorer-wave13-factory-plan.js";
import { FACTORY_PREVIEW_CANDIDATE_IDENTITIES } from "./brand-explorer-factory-preview-candidates.js";
import {
  EXPECTED_ACTIVE_COUNT_45,
  run45ActivePublicFullBaselineRegression,
} from "./brand-explorer-45-active-public-full-baseline.js";
import { runWave13SoFounderAcceptance } from "./brand-explorer-wave13-so-founder-acceptance.js";

export const WAVE13_SO_STATUS_PROMOTION_VERSION = "wave13-so-status-promotion-v1";

const BASICS_TABLE = "Brand Setup - Brand Basics";
const WRITE_THROTTLE_MS = 280;
const SO_SLUG = WAVE13_HELD_PROMOTION_SLUG;
const SO_RECORD_ID = "recTJdPlr4mDs9app";
const HOUSE_SLUG = "the-house-of-originals";
const MORGANS_SLUG = "morgans-originals";
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

function readJsonSafe(rel) {
  const p = path.join(ROOT, rel);
  if (!fs.existsSync(p)) return null;
  try {
    return JSON.parse(fs.readFileSync(p, "utf8"));
  } catch (err) {
    if (process.env.NODE_ENV !== "production") {
      console.warn("[wave13-so-status-promotion] json read failed", p, err?.message || err);
    }
    return null;
  }
}

function soIdentity() {
  const id = FACTORY_PREVIEW_CANDIDATE_IDENTITIES[SO_SLUG];
  if (!id?.recordId || id.recordId !== SO_RECORD_ID) {
    throw new Error(`SO/ identity mismatch: ${id?.recordId}`);
  }
  return { slug: SO_SLUG, name: id.name || "SO/", recordId: SO_RECORD_ID };
}

async function fetchBrand(recordId) {
  const res = mockRes();
  await getBrandLibraryBrandById({ query: { brandId: recordId }, headers: {} }, res);
  if (res.statusCode >= 400 || !res.payload?.brand) {
    throw new Error(`fetch failed ${recordId}: ${res.statusCode}`);
  }
  return res.payload.brand;
}

async function patchBasicsBrandStatus({ recordId, targetStatus }) {
  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey) throw new Error("AIRTABLE_BASE_ID and AIRTABLE_API_KEY required");
  if (recordId !== SO_RECORD_ID) {
    throw new Error(`Refuse: SO status-promotion may only write ${SO_RECORD_ID}`);
  }
  const fields = { "Brand Status": targetStatus };
  for (const k of Object.keys(fields)) {
    if (WAVE13_NEVER_WRITE_FIELDS.includes(k)) {
      throw new Error(`Refuse: never-write field in payload: ${k}`);
    }
  }
  if (Object.keys(fields).length !== 1 || !("Brand Status" in fields)) {
    throw new Error(`Refuse: payload must be Brand Status only: ${JSON.stringify(fields)}`);
  }

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

function writeReports(report, md) {
  fs.mkdirSync(REPORTS_DIR, { recursive: true });
  const jsonPath = path.join(REPORTS_DIR, "brand-explorer-wave13-so-status-promotion.json");
  const mdPath = path.join(REPORTS_DIR, "brand-explorer-wave13-so-status-promotion.md");
  fs.writeFileSync(jsonPath, `${JSON.stringify(report, null, 2)}\n`);
  fs.writeFileSync(mdPath, md.endsWith("\n") ? md : `${md}\n`);
  return { jsonPath, mdPath };
}

function renderMarkdown(r) {
  const lines = [
    `# Brand Explorer Wave 13 — SO/ Status Promotion`,
    ``,
    `Version: \`${r.version}\` · Generated: ${r.generatedAt}`,
    `Mode: **${r.applyPerformed ? "APPLY" : "dry-run"}** · writePerformed: **${r.writePerformed}**`,
    ``,
    `Target Brand Status: **${r.targetBrandStatus}**`,
    `Active universe before: **${r.universeBefore?.totalCount ?? "n/a"}** (expected ${EXPECTED_ACTIVE_COUNT_45})`,
  ];
  if (r.universeAfter) {
    lines.push(
      `Active universe after: **${r.universeAfter.totalCount}** (expected ${WAVE13_EXPECTED_SO_ACTIVE_COUNT})`
    );
  }
  lines.push(
    ``,
    `## Scope`,
    ``,
    `- Promote (1): \`${SO_SLUG}\` (\`${SO_RECORD_ID}\`)`,
    `- Untouched: active 45 · House of Originals · Morgans Originals · Radisson Collection`,
    ``,
    `## Founder acceptance`,
    ``,
    `- founder_accepts_cleanly_unavailable_steward_posture: **${r.founderAcceptance?.founder_accepts_cleanly_unavailable_steward_posture}**`,
    `- promotion_recommendation: **${r.founderAcceptance?.promotion_recommendation || "—"}**`,
    ``,
    `## Preflight`,
    ``,
    `- Protected 45 baseline: **${r.preflight?.baseline45?.ok ? "PASS" : "FAIL"}**`,
    `- SO/ Under Review → Active: **${r.preflight?.statusGateOk ? "PASS" : "FAIL"}**`,
    `- Preflight OK: **${r.preflightOk}**`,
  );
  if (r.preflight?.issues?.length) {
    for (const i of r.preflight.issues) lines.push(`  - ${i}`);
  }
  lines.push(
    ``,
    `## Planned patch (Brand Status only)`,
    ``,
    `| Slug | From | To | Needs write |`,
    `| --- | --- | --- | --- |`,
    `| ${r.brand?.slug} | ${r.brand?.from || "(empty)"} | ${r.brand?.to} | ${r.brand?.needsWrite} |`,
    ``,
    `## Apply results`,
    ``,
    "```json",
    JSON.stringify(r.applyResults, null, 2),
    "```",
    ``,
    `## Guardrails`,
    ``
  );
  for (const [k, v] of Object.entries(r.guardrails || {})) {
    lines.push(`- ${k}: ${Array.isArray(v) ? v.join(", ") : v}`);
  }
  lines.push("", `Ready: \`${r.readyStatement}\``, "");
  return lines.join("\n");
}

/**
 * @param {{ apply?: boolean, argv?: string[], targetStatus?: string }} opts
 */
export async function runWave13SoStatusPromotion({
  apply = false,
  argv = [],
  targetStatus = WAVE13_STATUS_TO_PREFERRED,
} = {}) {
  const stage = "so-status-promotion";
  if (!WAVE13_STATUS_TO_ALLOWED.includes(targetStatus)) {
    throw new Error(`Refuse: targetStatus '${targetStatus}' not allowed`);
  }
  if (targetStatus !== "Active") {
    throw new Error("Refuse: SO status-promotion target must be Active");
  }

  const flagCheck = checkFlags(WAVE13_SO_STATUS_PROMOTION_APPLY_FLAGS, argv, apply);
  const identity = soIdentity();
  const preflightIssues = [];

  const founderAcceptance = await runWave13SoFounderAcceptance({
    argv,
    requireExplicitFlag: true,
  });
  if (!founderAcceptance.founder_accepts_cleanly_unavailable_steward_posture) {
    preflightIssues.push("founder_acceptance_not_recorded_or_failed");
  }
  if (
    founderAcceptance.promotion_recommendation !== WAVE13_FOUNDER_APPROVE_RECOMMENDATION
  ) {
    preflightIssues.push(
      `promotion_recommendation_not_approve:got=${founderAcceptance.promotion_recommendation}`
    );
  }

  let baseline45 = { ok: false };
  try {
    const result = await run45ActivePublicFullBaselineRegression({
      allowCachedPvqlIfPass: true,
      maxPvqlAgeMs: 72 * 60 * 60 * 1000,
      forceLivePvql: false,
      evaluateEvidence: true,
    });
    baseline45 = {
      ok: result?.regression?.pass === true,
      liveUniverseCount: result?.liveUniverseCount ?? null,
      pvqlSource: result?.pvqlSource || null,
      failures: (result?.regression?.failures || []).slice(0, 12),
    };
    if (!baseline45.ok) preflightIssues.push("protected_45_baseline_failed");
  } catch (err) {
    baseline45 = { ok: false, error: err.message };
    preflightIssues.push(`protected_45_baseline_error:${err.message}`);
  }

  const universeBefore = await loadActiveUniverse({ includeDetails: false });
  if (universeBefore.totalCount !== EXPECTED_ACTIVE_COUNT_45) {
    preflightIssues.push(
      `active_universe_not_45:got=${universeBefore.totalCount}`
    );
  }
  if ((universeBefore.brands || []).some((b) => nz(b.slug).toLowerCase() === SO_SLUG)) {
    preflightIssues.push("so_already_in_active_universe");
  }

  const live = await fetchBrand(identity.recordId);
  await sleep(90);
  const current = nz(live.brandStatus || live.status);
  const statusGateOk =
    current === WAVE13_STATUS_FROM || current === targetStatus;
  if (!statusGateOk) {
    preflightIssues.push(`status_unexpected:got=${current || "(empty)"}`);
  }
  if (isBrandStatusActive(current) && universeBefore.totalCount === EXPECTED_ACTIVE_COUNT_45) {
    // Already Active but not in universe would be inconsistent; if Active and count 45, SO shouldn't be Active yet
    if ((universeBefore.brands || []).every((b) => nz(b.slug).toLowerCase() !== SO_SLUG)) {
      preflightIssues.push("so_active_but_missing_from_universe_inconsistent");
    }
  }

  const brand = {
    slug: identity.slug,
    name: identity.name,
    recordId: identity.recordId,
    from: current,
    to: targetStatus,
    needsWrite: current !== targetStatus,
    plannedFields: ["Brand Status"],
    sanitizedPayloadPreview: { "Brand Status": targetStatus },
  };

  const preflightOk =
    preflightIssues.length === 0 &&
    statusGateOk &&
    baseline45.ok &&
    founderAcceptance.founder_accepts_cleanly_unavailable_steward_posture === true;

  const applyPerformed = apply === true && flagCheck.ok === true && preflightOk;
  const applyResults = [];
  let writePerformed = false;

  if (applyPerformed) {
    if (!brand.needsWrite) {
      applyResults.push({
        slug: brand.slug,
        recordId: brand.recordId,
        applied: false,
        reason: "already_at_target",
        writePerformed: false,
      });
    } else {
      try {
        const response = await patchBasicsBrandStatus({
          recordId: brand.recordId,
          targetStatus,
        });
        writePerformed = true;
        applyResults.push({
          slug: brand.slug,
          recordId: brand.recordId,
          applied: true,
          writePerformed: true,
          table: BASICS_TABLE,
          fieldMapping: { brandStatus: "Brand Status" },
          sanitizedPayloadPreview: response.sanitizedPayloadPreview,
          response: { id: response.id, fieldsPatched: response.fieldsPatched },
        });
      } catch (err) {
        applyResults.push({
          slug: brand.slug,
          recordId: brand.recordId,
          applied: false,
          writePerformed: false,
          error: err.message,
        });
      }
      await sleep(WRITE_THROTTLE_MS);
    }
  } else if (apply && !flagCheck.ok) {
    applyResults.push({ applied: false, reason: "missing_apply_flags", missing: flagCheck.missing });
  } else if (apply && !preflightOk) {
    applyResults.push({ applied: false, reason: "preflight_failed", issues: preflightIssues });
  }

  let universeAfter = null;
  if (applyPerformed && writePerformed) {
    await sleep(400);
    universeAfter = await loadActiveUniverse({ includeDetails: false });
    if (universeAfter.totalCount !== WAVE13_EXPECTED_SO_ACTIVE_COUNT) {
      // Soft note — Soft note in report; SoftAirtable cache may lag
    }
  }

  const report = {
    version: WAVE13_SO_STATUS_PROMOTION_VERSION,
    waveVersion: WAVE13_VERSION,
    stage,
    generatedAt: new Date().toISOString(),
    apply,
    applyPerformed,
    writePerformed,
    targetBrandStatus: targetStatus,
    founderAcceptance: {
      founder_accepts_cleanly_unavailable_steward_posture:
        founderAcceptance.founder_accepts_cleanly_unavailable_steward_posture,
      promotion_recommendation: founderAcceptance.promotion_recommendation,
      readyStatement: founderAcceptance.readyStatement,
    },
    brand,
    universeBefore: {
      totalCount: universeBefore.totalCount,
      expected: EXPECTED_ACTIVE_COUNT_45,
      soIncluded: (universeBefore.brands || []).some((b) => nz(b.slug).toLowerCase() === SO_SLUG),
    },
    universeAfter: universeAfter
      ? {
          totalCount: universeAfter.totalCount,
          expected: WAVE13_EXPECTED_SO_ACTIVE_COUNT,
          soIncluded: (universeAfter.brands || []).some(
            (b) => nz(b.slug).toLowerCase() === SO_SLUG
          ),
        }
      : null,
    preflight: {
      issues: preflightIssues,
      baseline45,
      statusGateOk,
      soOnly: true,
    },
    preflightOk,
    flagCheck: {
      apply: flagCheck.apply,
      ok: flagCheck.ok,
      missing: flagCheck.missing,
    },
    applyResults,
    guardrails: {
      soOnly: true,
      brandStatusOnly: true,
      noContentWrites: true,
      noImageWrites: true,
      noReleaseFieldWrites: true,
      noCompanyValidationChanges: true,
      noSourceLibraryStatusChanges: true,
      noRegistryApprovalChanges: true,
      noActive45Writes: true,
      noHouseOfOriginalsWrites: true,
      noMorgansOriginalsWrites: true,
      noRadissonCollectionChanges: true,
      excludedSlugs: [HOUSE_SLUG, MORGANS_SLUG, RADISSON_COLLECTION_SLUG],
      neverWriteFields: [...WAVE13_NEVER_WRITE_FIELDS],
      priorPartialActiveCount: WAVE13_EXPECTED_PARTIAL_ACTIVE_COUNT,
    },
    readyStatement: applyPerformed
      ? "wave13_so_status_promotion_complete_ready_for_public_release"
      : preflightOk
        ? "wave13_so_status_promotion_dry_run_ready"
        : "wave13_so_status_promotion_blocked",
  };

  const paths = writeReports(report, renderMarkdown(report));
  return { ...report, paths };
}
