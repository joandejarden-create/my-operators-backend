/**
 * Wave 13 Stage 10b — SO/ public release (release fields + intentional restore).
 *
 * Allowed writes (SO/ Basics only): release/restore fields listed in
 * WAVE13_RELEASE_FIELDS + intentional public restore registry inclusion.
 * No Brand Status / content / images / CV / Source / Registry approval /
 * active-45 / House / Morgans / Radisson writes.
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
  WAVE13_VERSION,
  WAVE13_HELD_PROMOTION_SLUG,
  WAVE13_RELEASE_FIELDS,
  WAVE13_NEVER_WRITE_FIELDS,
  WAVE13_SO_PUBLIC_RELEASE_APPLY_FLAGS,
  WAVE13_EXPECTED_SO_ACTIVE_COUNT,
  WAVE13_FOUNDER_APPROVE_RECOMMENDATION,
} from "./brand-explorer-wave13-factory-plan.js";
import { FACTORY_PREVIEW_CANDIDATE_IDENTITIES } from "./brand-explorer-factory-preview-candidates.js";
import { runWave13SoFounderAcceptance } from "./brand-explorer-wave13-so-founder-acceptance.js";

export const WAVE13_SO_PUBLIC_RELEASE_VERSION = "wave13-so-public-release-v1";

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
const DOCS_DIR = path.join(ROOT, "docs", "data-intelligence");

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

function readJsonSafe(rel) {
  const p = path.join(ROOT, rel);
  if (!fs.existsSync(p)) return null;
  try {
    return JSON.parse(fs.readFileSync(p, "utf8"));
  } catch (err) {
    if (process.env.NODE_ENV !== "production") {
      console.warn("[wave13-so-public-release] json read failed", p, err?.message || err);
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
    if (WAVE13_NEVER_WRITE_FIELDS.includes(k)) {
      throw new Error(`Refuse: never-write field in release payload: ${k}`);
    }
    if (k === "Brand Status") {
      throw new Error("Refuse: public-release must not write Brand Status");
    }
    if (!WAVE13_RELEASE_FIELDS.includes(k)) {
      throw new Error(`Refuse: unexpected release field: ${k}`);
    }
  }
  for (const required of WAVE13_RELEASE_FIELDS) {
    if (!(required in fields)) {
      throw new Error(`Refuse: missing required release field: ${required}`);
    }
  }
}

async function patchBasicsRelease({ recordId, fields }) {
  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey) throw new Error("AIRTABLE_BASE_ID and AIRTABLE_API_KEY required");
  if (recordId !== SO_RECORD_ID) {
    throw new Error(`Refuse: SO public-release may only write ${SO_RECORD_ID}`);
  }
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

function writeReports(report, md) {
  fs.mkdirSync(REPORTS_DIR, { recursive: true });
  fs.mkdirSync(DOCS_DIR, { recursive: true });
  const jsonPath = path.join(REPORTS_DIR, "brand-explorer-wave13-so-public-release.json");
  const mdPath = path.join(REPORTS_DIR, "brand-explorer-wave13-so-public-release.md");
  fs.writeFileSync(jsonPath, `${JSON.stringify(report, null, 2)}\n`);
  fs.writeFileSync(mdPath, md.endsWith("\n") ? md : `${md}\n`);

  const docPath = path.join(DOCS_DIR, "brand-explorer-wave13-so-release.md");
  fs.writeFileSync(
    docPath,
    [
      `# Wave 13 — SO/ Status Promotion + Public Release`,
      ``,
      `SO/ (\`so-hotels-and-resorts\`, \`${SO_RECORD_ID}\`) promoted Active and publicly released.`,
      `Active universe: **45 → 46**. Founder accepted cleanly-unavailable steward posture for`,
      `\`snapshot.*\` scale fields and \`footprint.primary_regions\` (not invented).`,
      ``,
      `## Commands`,
      ``,
      "```bash",
      `npm run brand-explorer-wave13-factory -- --stage so-status-promotion --dry-run`,
      `npm run brand-explorer-wave13-factory -- --stage so-public-release --dry-run`,
      "```",
      ``,
      `## Ready statement`,
      ``,
      `\`${report.readyStatement || "wave13_so_public_release_complete_ready_for_46_baseline_freeze"}\``,
      ``,
      `Last generated: ${report.generatedAt}`,
      ``,
      `## Guardrails`,
      ``,
      `- No content / image rewrites`,
      `- No Company Validated / Source Library / Registry approval changes`,
      `- Active 45 brands untouched`,
      `- House of Originals excluded · Morgans Originals untouched · Radisson Collection excluded`,
      ``,
    ].join("\n"),
    "utf8"
  );

  return { jsonPath, mdPath, docPath };
}

function renderMarkdown(r) {
  const lines = [
    `# Brand Explorer Wave 13 — SO/ Public Release`,
    ``,
    `Version: \`${r.version}\` · Generated: ${r.generatedAt}`,
    `Mode: **${r.applyPerformed ? "APPLY" : "dry-run"}** · writePerformed: **${r.writePerformed}**`,
    ``,
    `Active universe: **${r.universe?.totalCount ?? "n/a"}** (expected ${WAVE13_EXPECTED_SO_ACTIVE_COUNT})`,
    `Ready: \`${r.readyStatement}\``,
    ``,
    `## Scope`,
    ``,
    `- Release (1): \`${SO_SLUG}\``,
    `- Untouched: active 45 · House · Morgans · Radisson Collection`,
    ``,
    `## Founder acceptance`,
    ``,
    `- founder_accepts_cleanly_unavailable_steward_posture: **${r.founderAcceptance?.founder_accepts_cleanly_unavailable_steward_posture}**`,
    ``,
    `## Planned release fields`,
    ``,
  ];
  for (const f of r.plannedReleaseFields || []) lines.push(`- \`${f}\``);
  lines.push(
    ``,
    `## Brand readiness`,
    ``,
    `| Slug | Status | Active/Live | Needs release write |`,
    `| --- | --- | --- | --- |`,
    `| ${r.brand?.slug} | ${r.brand?.brandStatus || "(empty)"} | ${r.brand?.isActiveOrLive} | ${r.brand?.needsWrite} |`,
    ``,
    `## Apply outcome`,
    ``,
    "```json",
    JSON.stringify(r.applyOutcome, null, 2),
    "```",
    ``,
    `## Intentional restore registry`,
    ``,
    `- Before count: ${r.intentionalRegistry?.beforeCount ?? "n/a"}`,
    `- After count: ${r.intentionalRegistry?.afterCount ?? "n/a"}`,
    `- SO/ added: **${r.intentionalRegistry?.soAdded}**`,
    ``,
    `## Baseline freeze posture`,
    ``,
    `- ${r.baselineFreezeNote || "—"}`,
    ``,
    `## Guardrails`,
    ``
  );
  for (const [k, v] of Object.entries(r.guardrails || {})) {
    lines.push(`- ${k}: ${Array.isArray(v) ? v.join(", ") : v}`);
  }
  lines.push("");
  return lines.join("\n");
}

/**
 * @param {{ apply?: boolean, argv?: string[] }} opts
 */
export async function runWave13SoPublicRelease({ apply = false, argv = [] } = {}) {
  const stage = "so-public-release";
  const flagCheck = checkFlags(WAVE13_SO_PUBLIC_RELEASE_APPLY_FLAGS, argv, apply);
  const identity = soIdentity();
  const preflightIssues = [];
  const today = todayIsoDate();
  const releaseFields = buildReleaseFields(today);

  const founderAcceptance = await runWave13SoFounderAcceptance({
    argv,
    requireExplicitFlag: true,
  });
  // After status promotion SO is Active — acceptance gate re-check will flag so_already_active.
  // Prefer the recorded acceptance report from status-promotion / prior acceptance file.
  const acceptanceFile = readJsonSafe("reports/brand-explorer-wave13-so-founder-acceptance.json");
  const statusPromo = readJsonSafe("reports/brand-explorer-wave13-so-status-promotion.json");
  const acceptedFromFile =
    acceptanceFile?.founder_accepts_cleanly_unavailable_steward_posture === true &&
    (acceptanceFile?.promotion_recommendation === WAVE13_FOUNDER_APPROVE_RECOMMENDATION ||
      statusPromo?.founderAcceptance?.founder_accepts_cleanly_unavailable_steward_posture === true);

  const founderOk =
    argv.includes("--confirm-founder-accepts-cleanly-unavailable-steward-posture") &&
    (acceptedFromFile ||
      founderAcceptance.founder_accepts_cleanly_unavailable_steward_posture === true ||
      statusPromo?.applyPerformed === true);

  if (!founderOk) {
    preflightIssues.push("founder_acceptance_not_confirmed_for_public_release");
  }
  if (!argv.includes("--confirm-founder-visual-review-passed")) {
    preflightIssues.push("missing_confirm_founder_visual_review_passed");
  }

  const universe = await loadActiveUniverse({ includeDetails: false });
  if (universe.totalCount !== WAVE13_EXPECTED_SO_ACTIVE_COUNT) {
    preflightIssues.push(
      `active_universe_not_46_after_status_promotion:got=${universe.totalCount}`
    );
  }
  const soInUniverse = (universe.brands || []).some(
    (b) => nz(b.slug).toLowerCase() === SO_SLUG
  );
  if (!soInUniverse) {
    preflightIssues.push("so_not_in_active_universe");
  }

  const live = await fetchBrand(identity.recordId);
  await sleep(90);
  const status = nz(live.brandStatus || live.status);
  const isActiveOrLive = isBrandStatusActive(status);
  if (!isActiveOrLive) {
    preflightIssues.push(`brand_status_not_active_or_live:got=${status || "(empty)"}`);
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

  const brand = {
    slug: identity.slug,
    name: identity.name,
    recordId: identity.recordId,
    brandStatus: status,
    isActiveOrLive,
    needsWrite: !(alreadyApproved && alreadyReady && alreadyFounder),
    already: { alreadyApproved, alreadyReady, alreadyFounder },
    sanitizedPayloadPreview: releaseFields,
    plannedFields: [...WAVE13_RELEASE_FIELDS],
  };

  const registryBefore = readIntentionalPublicRestoreSlugs();
  const soAlreadyInRegistry = registryBefore.includes(SO_SLUG);

  const preflightOk = preflightIssues.length === 0 && isActiveOrLive && founderOk;
  const applyPerformed = apply === true && flagCheck.ok === true && preflightOk;
  const applyResults = [];
  let writePerformed = false;
  let intentionalRegistryOutcome = null;

  if (applyPerformed) {
    if (!brand.needsWrite) {
      applyResults.push({
        slug: brand.slug,
        recordId: brand.recordId,
        applied: false,
        reason: "release_fields_already_set",
        writePerformed: false,
      });
    } else {
      try {
        const response = await patchBasicsRelease({
          recordId: brand.recordId,
          fields: releaseFields,
        });
        writePerformed = true;
        applyResults.push({
          slug: brand.slug,
          recordId: brand.recordId,
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
          slug: brand.slug,
          recordId: brand.recordId,
          applied: false,
          writePerformed: false,
          error: err.message,
        });
      }
      await sleep(WRITE_THROTTLE_MS);
    }

    const before = readIntentionalPublicRestoreSlugs();
    const beforeSet = new Set(before);
    const soAdded = !beforeSet.has(SO_SLUG);
    const merged = [...new Set([...before, SO_SLUG])];
    // Never add House; never remove existing; Morgans not touched
    const filtered = merged.filter((s) => s !== HOUSE_SLUG);
    const registry = writeIntentionalPublicRestoreSlugs(filtered);
    intentionalRegistryOutcome = {
      updated: true,
      beforeCount: before.length,
      afterCount: registry.slugs.length,
      soAdded,
      soAlreadyInRegistry: !soAdded,
      path: "data/brand-explorer-public-restore-intentional.json",
      note: "SO/ added for intentional public restore; no content/CV/Source/Registry-approval writes.",
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
    applyOutcome = {
      applied: false,
      reason: "dry_run_only",
      plannedBrandWrites: brand.needsWrite ? 1 : 0,
      plannedRegistryAdd: soAlreadyInRegistry ? false : true,
    };
  }

  const readyStatement = applyPerformed
    ? "wave13_so_public_release_complete_ready_for_46_baseline_freeze"
    : preflightOk
      ? "wave13_so_public_release_dry_run_ready"
      : "wave13_so_public_release_blocked";

  const report = {
    version: WAVE13_SO_PUBLIC_RELEASE_VERSION,
    waveVersion: WAVE13_VERSION,
    stage,
    generatedAt: new Date().toISOString(),
    apply,
    applyPerformed,
    writePerformed,
    founderAcceptance: {
      founder_accepts_cleanly_unavailable_steward_posture: founderOk,
      source: acceptedFromFile
        ? "reports/brand-explorer-wave13-so-founder-acceptance.json"
        : "live_acceptance_gate",
      promotion_recommendation: WAVE13_FOUNDER_APPROVE_RECOMMENDATION,
    },
    brand,
    plannedReleaseFields: [...WAVE13_RELEASE_FIELDS],
    universe: {
      totalCount: universe.totalCount,
      expected: WAVE13_EXPECTED_SO_ACTIVE_COUNT,
      soIncluded: soInUniverse,
      beforePromotion: 45,
      afterRelease: WAVE13_EXPECTED_SO_ACTIVE_COUNT,
    },
    intentionalRegistry: intentionalRegistryOutcome || {
      beforeCount: registryBefore.length,
      afterCount: registryBefore.length + (soAlreadyInRegistry ? 0 : 1),
      soAdded: !soAlreadyInRegistry,
      planned: true,
    },
    preflight: { issues: preflightIssues, ok: preflightOk },
    flagCheck: {
      apply: flagCheck.apply,
      ok: flagCheck.ok,
      missing: flagCheck.missing,
    },
    applyOutcome,
    baselineFreezeNote:
      "Active universe is 46 after SO/ release. Protected 45 freeze remains historical. Next task: freeze protected 46 Active/Live public-full baseline.",
    guardrails: {
      soOnly: true,
      releaseFieldsOnly: true,
      noBrandStatusWrites: true,
      noContentRewrites: true,
      noImageWrites: true,
      noCompanyValidationChanges: true,
      noSourceLibraryStatusChanges: true,
      noRegistryApprovalChanges: true,
      noActive45Writes: true,
      noHouseOfOriginalsWrites: true,
      noMorgansOriginalsWrites: true,
      noRadissonCollectionChanges: true,
      excludedSlugs: [HOUSE_SLUG, MORGANS_SLUG, RADISSON_COLLECTION_SLUG],
      neverWriteFields: [...WAVE13_NEVER_WRITE_FIELDS],
    },
    readyStatement,
  };

  const paths = writeReports(report, renderMarkdown(report));
  return { ...report, paths };
}
