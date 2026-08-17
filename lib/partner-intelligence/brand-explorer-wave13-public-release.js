/**
 * Wave 13 Stage 10 — Partial public release for six founder-approved brands.
 *
 * Allowed writes (six Basics only): release/restore fields + intentional public
 * restore registry inclusion. SO/ held. No Brand Status / CV / Source / Registry
 * approval / content / images.
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
  WAVE13_PARTIAL_PROMOTION_SLUGS,
  WAVE13_HELD_PROMOTION_SLUG,
  WAVE13_PROTECTED_BASELINE_COUNT,
  WAVE13_EXPECTED_PARTIAL_ACTIVE_COUNT,
  WAVE13_PUBLIC_RELEASE_APPLY_FLAGS,
  WAVE13_RELEASE_FIELDS,
  WAVE13_NEVER_WRITE_FIELDS,
  WAVE13_FOUNDER_APPROVE_RECOMMENDATION,
  WAVE13_FOUNDER_HOLD_RECOMMENDATION,
  WAVE13_STATUS_FROM,
} from "./brand-explorer-wave13-factory-plan.js";
import { FACTORY_PREVIEW_CANDIDATE_IDENTITIES } from "./brand-explorer-factory-preview-candidates.js";

export const WAVE13_PUBLIC_RELEASE_VERSION = "wave13-partial-public-release-v1";

const BASICS_TABLE = "Brand Setup - Brand Basics";
const WRITE_THROTTLE_MS = 280;

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

function readJsonSafe(relOrAbs) {
  const p = path.isAbsolute(relOrAbs) ? relOrAbs : path.join(ROOT, relOrAbs);
  if (!fs.existsSync(p)) return null;
  try {
    return JSON.parse(fs.readFileSync(p, "utf8"));
  } catch (err) {
    if (process.env.NODE_ENV !== "production") {
      console.warn("[wave13-public-release] json read failed", p, err?.message || err);
    }
    return null;
  }
}

function loadProtected39RecordIds() {
  const freeze =
    readJsonSafe("reports/brand-explorer-39-active-public-full-baseline.json") ||
    readJsonSafe("docs/data-intelligence/brand-explorer-39-active-public-full-baseline.json");
  const ids = new Set();
  for (const b of freeze?.brands || []) {
    const id = nz(b.recordId || b.id);
    if (id) ids.add(id);
  }
  return ids;
}

function partialIdentities() {
  return WAVE13_PARTIAL_PROMOTION_SLUGS.map((slug) => {
    const id = FACTORY_PREVIEW_CANDIDATE_IDENTITIES[slug];
    if (!id?.recordId) throw new Error(`Missing factory-preview identity for ${slug}`);
    return { slug, name: id.name, recordId: id.recordId };
  });
}

function heldIdentity() {
  const id = FACTORY_PREVIEW_CANDIDATE_IDENTITIES[WAVE13_HELD_PROMOTION_SLUG];
  if (!id?.recordId) throw new Error(`Missing held identity for ${WAVE13_HELD_PROMOTION_SLUG}`);
  return { slug: WAVE13_HELD_PROMOTION_SLUG, name: id.name, recordId: id.recordId };
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
  assertReleasePayload(fields);
  if (recordId === heldIdentity().recordId) {
    throw new Error("Refuse: SO/ is held — no release field writes");
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

function assertFounderPartialApprovals() {
  const summary = readJsonSafe("reports/brand-explorer-wave13-founder-review-summary.json");
  const issues = [];
  if (!summary) return { ok: false, issues: ["missing_founder_review_summary"] };
  const brands = summary.brands || [];
  for (const slug of WAVE13_PARTIAL_PROMOTION_SLUGS) {
    const row = brands.find((b) => nz(b.brandSlug || b.slug).toLowerCase() === slug);
    if (nz(row?.recommendation) !== WAVE13_FOUNDER_APPROVE_RECOMMENDATION) {
      issues.push(`founder_not_approve:${slug}`);
    }
  }
  const so = brands.find(
    (b) => nz(b.brandSlug || b.slug).toLowerCase() === WAVE13_HELD_PROMOTION_SLUG
  );
  if (nz(so?.recommendation) !== WAVE13_FOUNDER_HOLD_RECOMMENDATION) {
    issues.push("so_not_held_in_founder_summary");
  }
  return { ok: issues.length === 0, issues };
}

function writeReports(report, md) {
  fs.mkdirSync(REPORTS_DIR, { recursive: true });
  fs.mkdirSync(DOCS_DIR, { recursive: true });
  const jsonPath = path.join(REPORTS_DIR, "brand-explorer-wave13-partial-public-release.json");
  const mdPath = path.join(REPORTS_DIR, "brand-explorer-wave13-partial-public-release.md");
  fs.writeFileSync(jsonPath, `${JSON.stringify(report, null, 2)}\n`, "utf8");
  fs.writeFileSync(mdPath, md.endsWith("\n") ? md : `${md}\n`, "utf8");

  const docPath = path.join(DOCS_DIR, "brand-explorer-wave13-partial-release.md");
  fs.writeFileSync(
    docPath,
    [
      `# Wave 13 — Partial Status Promotion + Public Release`,
      ``,
      `Six founder-approved brands only. SO/ held.`,
      ``,
      `## Commands`,
      ``,
      "```bash",
      `npm run brand-explorer-wave13-factory -- --stage status-promotion --dry-run --approved-only`,
      `npm run brand-explorer-wave13-factory -- --stage public-release --dry-run --approved-only`,
      "```",
      ``,
      `## Ready statement`,
      ``,
      `\`${report.readyStatement || "wave13_six_brand_partial_release_complete_so_held"}\``,
      ``,
      `Last generated: ${report.generatedAt}`,
      ``,
    ].join("\n"),
    "utf8"
  );

  return { jsonPath, mdPath, docPath };
}

function renderMarkdown(r) {
  const lines = [
    `# Brand Explorer Wave 13 — Partial Public Release`,
    ``,
    `Version: \`${r.version}\` · Generated: ${r.generatedAt}`,
    `Mode: **${r.applyPerformed ? "APPLY" : "dry-run"}** · writePerformed: **${r.writePerformed}**`,
    ``,
    `Active universe: **${r.universe?.totalCount ?? "n/a"}** (expected ${WAVE13_EXPECTED_PARTIAL_ACTIVE_COUNT})`,
    `Ready: \`${r.readyStatement}\``,
    ``,
    `## Scope`,
    ``,
    `- Release (6): ${WAVE13_PARTIAL_PROMOTION_SLUGS.map((s) => `\`${s}\``).join(", ")}`,
    `- Held: \`${WAVE13_HELD_PROMOTION_SLUG}\` — Under Review, no release fields`,
    `- Excluded: House of Originals · Morgans Originals · Radisson Collection`,
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
    `| --- | --- | --- | --- |`
  );
  for (const b of r.brands || []) {
    lines.push(
      `| ${b.slug} | ${b.brandStatus || "(empty)"} | ${b.isActiveOrLive} | ${b.needsWrite} |`
    );
  }
  lines.push(
    ``,
    `### SO/ held`,
    ``,
    `- Status: **${r.held?.brandStatus || "—"}** · release write: **false**`,
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
    `- Wave13 partial added: ${(r.intentionalRegistry?.wave13Added || r.intentionalRegistry?.plannedAdd || []).join(", ") || "(none)"}`,
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
export async function runWave13PublicRelease({ apply = false, argv = [] } = {}) {
  const stage = "public-release";
  const flagCheck = checkFlags(WAVE13_PUBLIC_RELEASE_APPLY_FLAGS, argv, apply);
  const identities = partialIdentities();
  const held = heldIdentity();
  const protectedIds = loadProtected39RecordIds();
  const preflightIssues = [];
  const today = todayIsoDate();
  const releaseFields = buildReleaseFields(today);

  if (!argv.includes("--approved-only")) {
    preflightIssues.push("missing_--approved-only_partial_scope_flag");
  }

  for (const id of identities) {
    if (protectedIds.has(id.recordId)) {
      preflightIssues.push(`target_collides_with_protected_39:${id.slug}`);
    }
    if (id.slug === WAVE13_HELD_PROMOTION_SLUG) {
      preflightIssues.push("so_in_partial_release_targets");
    }
  }

  const founderApprovals = assertFounderPartialApprovals();
  if (!founderApprovals.ok) preflightIssues.push(...founderApprovals.issues);

  const statusPromotion = readJsonSafe("reports/brand-explorer-wave13-partial-status-promotion.json");
  if (!statusPromotion?.applyPerformed && !statusPromotion?.summary?.acceptanceUniverse45) {
    // Allow if universe already 45 from a prior apply
  }

  const universe = await loadActiveUniverse({ includeDetails: false });
  if (universe.totalCount < WAVE13_PROTECTED_BASELINE_COUNT) {
    preflightIssues.push(`active_universe_too_small:${universe.totalCount}`);
  }
  if (
    universe.totalCount !== WAVE13_EXPECTED_PARTIAL_ACTIVE_COUNT &&
    universe.totalCount !== WAVE13_PROTECTED_BASELINE_COUNT
  ) {
    preflightIssues.push(
      `active_universe_unexpected:${universe.totalCount};expected_${WAVE13_EXPECTED_PARTIAL_ACTIVE_COUNT}_after_stage9`
    );
  }

  const brands = [];
  for (const id of identities) {
    const live = await fetchBrand(id.recordId);
    await sleep(90);
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

    brands.push({
      slug: id.slug,
      name: id.name,
      recordId: id.recordId,
      brandStatus: status,
      isActiveOrLive,
      needsWrite: !(alreadyApproved && alreadyReady && alreadyFounder),
      already: { alreadyApproved, alreadyReady, alreadyFounder },
      sanitizedPayloadPreview: releaseFields,
      plannedFields: [...WAVE13_RELEASE_FIELDS],
    });
  }

  const soLive = await fetchBrand(held.recordId);
  await sleep(90);
  const soStatus = nz(soLive.brandStatus || soLive.status);
  const soHeldOk = soStatus === WAVE13_STATUS_FROM && !isBrandStatusActive(soStatus);
  if (!soHeldOk) {
    preflightIssues.push(`so_not_under_review_at_release:${soStatus || "(empty)"}`);
  }
  const soInUniverse = (universe.brands || []).some(
    (b) => nz(b.slug).toLowerCase() === WAVE13_HELD_PROMOTION_SLUG
  );
  if (soInUniverse) {
    preflightIssues.push("so_unexpectedly_in_active_universe");
  }

  const allActive = brands.every((b) => b.isActiveOrLive);
  const preflightOk = preflightIssues.length === 0 && allActive && founderApprovals.ok && soHeldOk;

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
    const wave13Added = WAVE13_PARTIAL_PROMOTION_SLUGS.filter((s) => !beforeSet.has(s));
    if (beforeSet.has(WAVE13_HELD_PROMOTION_SLUG)) {
      // Do not add SO/; also do not remove if somehow present — but never add
    }
    const merged = [
      ...new Set([...before.filter((s) => s !== WAVE13_HELD_PROMOTION_SLUG), ...WAVE13_PARTIAL_PROMOTION_SLUGS]),
    ];
    const registry = writeIntentionalPublicRestoreSlugs(merged);
    intentionalRegistryOutcome = {
      updated: true,
      beforeCount: before.length,
      afterCount: registry.slugs.length,
      wave13Added,
      soAdded: false,
      path: "data/brand-explorer-public-restore-intentional.json",
      note: "Six Wave 13 partial brands added for intentional public restore; SO/ not added; no content/CV/Source/Registry-approval writes.",
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
      plannedBrandWrites: brands.filter((b) => b.needsWrite).length,
    };
  }

  const readyStatement = applyPerformed
    ? "wave13_six_brand_partial_release_complete_so_held"
    : "wave13_partial_public_release_dry_run";

  const report = {
    version: WAVE13_PUBLIC_RELEASE_VERSION,
    waveVersion: WAVE13_VERSION,
    stage,
    generatedAt: new Date().toISOString(),
    apply,
    applyPerformed,
    writePerformed,
    dryRun: !applyPerformed,
    approvedOnly: argv.includes("--approved-only"),
    flagCheck,
    requiredApplyFlags: [...WAVE13_PUBLIC_RELEASE_APPLY_FLAGS],
    plannedReleaseFields: [...WAVE13_RELEASE_FIELDS],
    releaseDate: today,
    preflight: {
      ok: preflightOk,
      issues: preflightIssues,
      founderApprovals,
      allBrandStatusActiveOrLive: allActive,
      soHeldOk,
    },
    held: {
      slug: held.slug,
      recordId: held.recordId,
      brandStatus: soStatus,
      releaseWrite: false,
      recommendation: WAVE13_FOUNDER_HOLD_RECOMMENDATION,
    },
    universe: {
      totalCount: universe.totalCount,
      expectedAfterPartialRelease: WAVE13_EXPECTED_PARTIAL_ACTIVE_COUNT,
      protectedBaselineWas: WAVE13_PROTECTED_BASELINE_COUNT,
      wave13PartialInUniverse: (universe.brands || [])
        .filter((b) => WAVE13_PARTIAL_PROMOTION_SLUGS.includes(nz(b.slug).toLowerCase()))
        .map((b) => b.slug),
      soInUniverse,
    },
    brands,
    applyOutcome,
    intentionalRegistry: intentionalRegistryOutcome || {
      beforeCount: readIntentionalPublicRestoreSlugs().length,
      plannedAdd: WAVE13_PARTIAL_PROMOTION_SLUGS.filter(
        (s) => !readIntentionalPublicRestoreSlugs().includes(s)
      ),
      soAdded: false,
    },
    fieldMapping: {
      activeProfileApproved: "Active Profile Approved",
      readyForActiveProfile: "Ready for Active Profile",
      activeProfileApprovedDate: "Active Profile Approved Date",
      founderVisualReviewPass: "Founder Visual Review Pass",
    },
    baselineFreezeNote:
      "Do **not** freeze a 45-brand baseline as the permanent protected baseline until SO/ is cleaned and promoted, **or** an explicit founder decision accepts an interim 45 freeze. Prefer waiting for SO/ before revising EXPECTED_ACTIVE_COUNT_39 → 45/46.",
    mayFreeze45BaselineNow: false,
    waitForSoBeforePermanentFreeze: true,
    guardrails: {
      targetBrandsOnly: true,
      sixApprovedOnly: true,
      soHeld: true,
      companyValidatedWrites: false,
      sourceLibraryWrites: false,
      registryApprovalWrites: false,
      brandStatusWrites: false,
      contentRewrites: false,
      imageWrites: false,
      protected39Untouched: true,
      houseOfOriginalsUntouched: true,
      morgansOriginalsUntouched: true,
      radissonCollectionUntouched: true,
      neverWriteFields: [...WAVE13_NEVER_WRITE_FIELDS],
      intentionalPublicRestoreRegistryUpdate: applyPerformed === true,
    },
    expectedAcceptance: {
      shouldRenderFullProfile: true,
      displayState: "active_profile_ready",
      activeUniverse: WAVE13_EXPECTED_PARTIAL_ACTIVE_COUNT,
      publicFullPvql: `${WAVE13_EXPECTED_PARTIAL_ACTIVE_COUNT}/${WAVE13_EXPECTED_PARTIAL_ACTIVE_COUNT}`,
      soExcludedFromActive: true,
    },
    readyStatement,
  };

  const paths = writeReports(report, renderMarkdown(report));
  return { ...report, report, paths, pass: preflightOk || applyPerformed, ok: preflightOk || !apply };
}
