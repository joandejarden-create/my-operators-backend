/**
 * Wave 15 Stage 10 — Public release for all eight Hilton Worldwide brands.
 *
 * Allowed writes (eight Basics only): release/restore fields + intentional
 * public restore registry inclusion. Four Points Flex by Sheraton is held
 * OUTSIDE the Wave 15 cohort (not a Hilton brand) and is never written or
 * added to the registry. No Brand Status / CV / Source / Registry approval /
 * content / images.
 *
 * `--confirm-public-visibility-quality-lock-passed` is a confirm-only flag
 * here — this module does NOT block preflight on a live PVQL run. Post-
 * release validation (e.g. `npm run test:brand-explorer-public-visibility-
 * quality-lock`) is the separate gate that verifies PVQL after the writes
 * land.
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
  WAVE15_VERSION,
  WAVE15_SLUGS,
  WAVE15_PROMOTION_SLUGS,
  WAVE15_PROTECTED_BASELINE_COUNT,
  WAVE15_EXPECTED_FINAL_ACTIVE_COUNT,
  WAVE15_PUBLIC_RELEASE_APPLY_FLAGS,
  WAVE15_RELEASE_FIELDS,
  WAVE15_NEVER_WRITE_FIELDS,
  WAVE15_FOUNDER_APPROVE_RECOMMENDATION,
  WAVE15_STATUS_FROM,
  WAVE15_FLEX_HELD_SLUG,
  WAVE15_FLEX_HELD_RECORD_ID,
} from "./brand-explorer-wave15-factory-plan.js";
import { FACTORY_PREVIEW_CANDIDATE_IDENTITIES } from "./brand-explorer-factory-preview-candidates.js";

export const WAVE15_PUBLIC_RELEASE_VERSION = "wave15-public-release-v1";

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
      console.warn("[wave15-public-release] json read failed", p, err?.message || err);
    }
    return null;
  }
}

function loadProtected54RecordIds() {
  const freeze =
    readJsonSafe("reports/brand-explorer-54-active-public-full-baseline.json") ||
    readJsonSafe("docs/data-intelligence/brand-explorer-54-active-public-full-baseline.json");
  const ids = new Set();
  for (const b of freeze?.brands || []) {
    const id = nz(b.recordId || b.id);
    if (id) ids.add(id);
  }
  return ids;
}

function promotionIdentities() {
  return WAVE15_PROMOTION_SLUGS.map((slug) => {
    const id = FACTORY_PREVIEW_CANDIDATE_IDENTITIES[slug];
    if (!id?.recordId) throw new Error(`Missing factory-preview identity for ${slug}`);
    return { slug, name: id.name, recordId: id.recordId };
  });
}

/** Four Points Flex — verified held OUTSIDE the Wave 15 cohort. Never written; never registered. */
function flexHeldIdentity() {
  return {
    slug: WAVE15_FLEX_HELD_SLUG,
    name: "Four Points Flex by Sheraton",
    recordId: WAVE15_FLEX_HELD_RECORD_ID,
  };
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
    if (WAVE15_NEVER_WRITE_FIELDS.includes(k)) {
      throw new Error(`Refuse: never-write field in release payload: ${k}`);
    }
    if (k === "Brand Status") {
      throw new Error("Refuse: public-release must not write Brand Status");
    }
    if (!WAVE15_RELEASE_FIELDS.includes(k)) {
      throw new Error(`Refuse: unexpected release field: ${k}`);
    }
  }
  for (const required of WAVE15_RELEASE_FIELDS) {
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
  if (recordId === WAVE15_FLEX_HELD_RECORD_ID) {
    throw new Error("Refuse: Four Points Flex is held outside the Wave 15 cohort — no release field writes");
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

function assertFounderApprovals() {
  const summary = readJsonSafe("reports/brand-explorer-wave15-founder-review-summary.json");
  const issues = [];
  if (!summary) return { ok: false, issues: ["missing_founder_review_summary"] };
  const brands = summary.brands || [];
  for (const slug of WAVE15_PROMOTION_SLUGS) {
    const row = brands.find((b) => nz(b.brandSlug || b.slug).toLowerCase() === slug);
    if (nz(row?.recommendation) !== WAVE15_FOUNDER_APPROVE_RECOMMENDATION) {
      issues.push(`founder_not_approve:${slug}`);
    }
  }
  const flexNote = nz(summary.heldExcluded?.fourPointsFlex?.status);
  if (flexNote && !/held|under review/i.test(flexNote)) {
    issues.push(`founder_summary_flex_note_unexpected:${flexNote}`);
  }
  return { ok: issues.length === 0, issues };
}

function writeReports(report, md) {
  fs.mkdirSync(REPORTS_DIR, { recursive: true });
  fs.mkdirSync(DOCS_DIR, { recursive: true });
  const jsonPath = path.join(REPORTS_DIR, "brand-explorer-wave15-public-release.json");
  const mdPath = path.join(REPORTS_DIR, "brand-explorer-wave15-public-release.md");
  fs.writeFileSync(jsonPath, `${JSON.stringify(report, null, 2)}\n`, "utf8");
  fs.writeFileSync(mdPath, md.endsWith("\n") ? md : `${md}\n`, "utf8");

  const docPath = path.join(DOCS_DIR, "brand-explorer-wave15-public-release.md");
  fs.writeFileSync(
    docPath,
    [
      `# Wave 15 — Status Promotion + Public Release`,
      ``,
      `All eight Hilton Worldwide brands. Four Points Flex by Sheraton is held **outside** this cohort (not a Hilton brand) — verified Under Review, never written.`,
      ``,
      `## Commands`,
      ``,
      "```bash",
      `npm run brand-explorer-wave15-factory -- --stage status-promotion --dry-run`,
      `npm run brand-explorer-wave15-factory -- --stage public-release --dry-run`,
      "```",
      ``,
      `## PVQL note`,
      ``,
      `\`--confirm-public-visibility-quality-lock-passed\` is a confirm-only flag on this module. It does **not** block preflight on a live PVQL run here — post-release validation (\`npm run test:brand-explorer-public-visibility-quality-lock\`) is the gate that verifies PVQL after the writes land.`,
      ``,
      `## Ready statement`,
      ``,
      `\`${report.readyStatement || "wave15_public_release_dry_run"}\``,
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
    `# Brand Explorer Wave 15 — Public Release`,
    ``,
    `Version: \`${r.version}\` · Generated: ${r.generatedAt}`,
    `Mode: **${r.applyPerformed ? "APPLY" : "dry-run"}** · writePerformed: **${r.writePerformed}**`,
    ``,
    `Active universe: **${r.universe?.totalCount ?? "n/a"}** (expected ${WAVE15_EXPECTED_FINAL_ACTIVE_COUNT})`,
    `Ready: \`${r.readyStatement}\``,
    ``,
    `## Scope`,
    ``,
    `- Release (8, all — no held slug in cohort): ${WAVE15_PROMOTION_SLUGS.map((s) => `\`${s}\``).join(", ")}`,
    `- Four Points Flex by Sheraton: held **outside** the Wave 15 cohort — Under Review, no release fields, not registered`,
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
    `### Four Points Flex — held outside cohort`,
    ``,
    `- Status: **${r.held?.brandStatus || "—"}** · in active universe: **${r.held?.inActiveUniverse}** · release write: **false**`,
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
    `- Wave 15 added: ${(r.intentionalRegistry?.wave15Added || r.intentionalRegistry?.plannedAdd || []).join(", ") || "(none)"}`,
    `- Flex added: **false** (never added — held outside cohort)`,
    ``,
    `## PVQL note`,
    ``,
    `- ${r.pvqlNote}`,
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
export async function runWave15PublicRelease({ apply = false, argv = [] } = {}) {
  const stage = "public-release";
  const flagCheck = checkFlags(WAVE15_PUBLIC_RELEASE_APPLY_FLAGS, argv, apply);
  const identities = promotionIdentities();
  const held = flexHeldIdentity();
  const protectedIds = loadProtected54RecordIds();
  const preflightIssues = [];
  const today = todayIsoDate();
  const releaseFields = buildReleaseFields(today);

  for (const id of identities) {
    if (protectedIds.has(id.recordId)) {
      preflightIssues.push(`target_collides_with_protected_54:${id.slug}`);
    }
    if (id.slug === WAVE15_FLEX_HELD_SLUG) {
      preflightIssues.push("flex_in_release_targets");
    }
  }
  if (identities.length !== WAVE15_SLUGS.length) {
    preflightIssues.push(
      `release_scope_mismatch:got=${identities.length};expected=${WAVE15_SLUGS.length}`
    );
  }

  const founderApprovals = assertFounderApprovals();
  if (!founderApprovals.ok) preflightIssues.push(...founderApprovals.issues);

  const statusPromotion = readJsonSafe("reports/brand-explorer-wave15-status-promotion.json");
  const universe = await loadActiveUniverse({ includeDetails: false });
  const wave15InUniverse = (universe.brands || []).filter((b) =>
    WAVE15_PROMOTION_SLUGS.includes(nz(b.slug).toLowerCase())
  );
  const statusPromotionConfirmed =
    statusPromotion?.applyPerformed === true ||
    (universe.totalCount === WAVE15_EXPECTED_FINAL_ACTIVE_COUNT && wave15InUniverse.length === WAVE15_PROMOTION_SLUGS.length);
  if (!statusPromotionConfirmed) {
    preflightIssues.push("status_promotion_not_confirmed_applied_or_universe_62_with_eight_present");
  }

  if (universe.totalCount < WAVE15_PROTECTED_BASELINE_COUNT) {
    preflightIssues.push(`active_universe_too_small:${universe.totalCount}`);
  }
  if (
    universe.totalCount !== WAVE15_EXPECTED_FINAL_ACTIVE_COUNT &&
    universe.totalCount !== WAVE15_PROTECTED_BASELINE_COUNT
  ) {
    preflightIssues.push(
      `active_universe_unexpected:${universe.totalCount};expected_${WAVE15_EXPECTED_FINAL_ACTIVE_COUNT}_after_stage9`
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
      plannedFields: [...WAVE15_RELEASE_FIELDS],
    });
  }

  const flexLive = await fetchBrand(held.recordId);
  await sleep(90);
  const flexStatus = nz(flexLive.brandStatus || flexLive.status);
  const flexInUniverse = (universe.brands || []).some(
    (b) => nz(b.slug).toLowerCase() === WAVE15_FLEX_HELD_SLUG
  );
  const flexHeldOk = flexStatus === WAVE15_STATUS_FROM && !isBrandStatusActive(flexStatus) && !flexInUniverse;
  if (flexStatus !== WAVE15_STATUS_FROM || isBrandStatusActive(flexStatus)) {
    preflightIssues.push(`flex_not_under_review_at_release:${flexStatus || "(empty)"}`);
  }
  if (flexInUniverse) {
    preflightIssues.push("flex_unexpectedly_in_active_universe");
  }

  const allActive = brands.every((b) => b.isActiveOrLive);
  const preflightOk = preflightIssues.length === 0 && allActive && founderApprovals.ok && flexHeldOk;

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
    const wave15Added = WAVE15_PROMOTION_SLUGS.filter((s) => !beforeSet.has(s));
    const merged = [
      ...new Set([...before.filter((s) => s !== WAVE15_FLEX_HELD_SLUG), ...WAVE15_PROMOTION_SLUGS]),
    ];
    const registry = writeIntentionalPublicRestoreSlugs(merged);
    intentionalRegistryOutcome = {
      updated: true,
      beforeCount: before.length,
      afterCount: registry.slugs.length,
      wave15Added,
      flexAdded: false,
      path: "data/brand-explorer-public-restore-intentional.json",
      note: "Eight Wave 15 Hilton brands added for intentional public restore; Four Points Flex not added (held outside cohort); no content/CV/Source/Registry-approval writes.",
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
    ? "wave15_eight_brand_release_complete_ready_for_62_freeze_or_post_release_cleanup"
    : "wave15_public_release_dry_run";

  const pvqlNote =
    "--confirm-public-visibility-quality-lock-passed is a confirm-only flag in this module — preflight does NOT run or block on a live PVQL check here. Post-release validation (npm run test:brand-explorer-public-visibility-quality-lock) is the separate gate that verifies PVQL after these writes land.";

  const report = {
    version: WAVE15_PUBLIC_RELEASE_VERSION,
    waveVersion: WAVE15_VERSION,
    stage,
    generatedAt: new Date().toISOString(),
    apply,
    applyPerformed,
    writePerformed,
    dryRun: !applyPerformed,
    flagCheck,
    requiredApplyFlags: [...WAVE15_PUBLIC_RELEASE_APPLY_FLAGS],
    plannedReleaseFields: [...WAVE15_RELEASE_FIELDS],
    releaseDate: today,
    pvqlNote,
    preflight: {
      ok: preflightOk,
      issues: preflightIssues,
      founderApprovals,
      allBrandStatusActiveOrLive: allActive,
      flexHeldOk,
      statusPromotionConfirmed,
    },
    held: {
      slug: held.slug,
      recordId: held.recordId,
      brandStatus: flexStatus,
      inActiveUniverse: flexInUniverse,
      releaseWrite: false,
      insideCohort: false,
      note: "Four Points Flex is not a Hilton brand and was never part of Wave 15 — held outside this cohort entirely.",
    },
    universe: {
      totalCount: universe.totalCount,
      expectedAfterRelease: WAVE15_EXPECTED_FINAL_ACTIVE_COUNT,
      protectedBaselineWas: WAVE15_PROTECTED_BASELINE_COUNT,
      wave15InUniverse: wave15InUniverse.map((b) => b.slug),
      flexInUniverse,
    },
    brands,
    applyOutcome,
    intentionalRegistry: intentionalRegistryOutcome || {
      beforeCount: readIntentionalPublicRestoreSlugs().length,
      plannedAdd: WAVE15_PROMOTION_SLUGS.filter(
        (s) => !readIntentionalPublicRestoreSlugs().includes(s)
      ),
      flexAdded: false,
    },
    fieldMapping: {
      activeProfileApproved: "Active Profile Approved",
      readyForActiveProfile: "Ready for Active Profile",
      activeProfileApprovedDate: "Active Profile Approved Date",
      founderVisualReviewPass: "Founder Visual Review Pass",
    },
    guardrails: {
      targetBrandsOnly: true,
      allEightReleasedNoHeldSlugInCohort: true,
      flexHeldOutsideCohort: true,
      flexUntouched: true,
      flexNeverRegistered: true,
      brandStatusWrites: false,
      companyValidatedWrites: false,
      sourceLibraryWrites: false,
      registryApprovalWrites: false,
      contentRewrites: false,
      imageWrites: false,
      protected54Untouched: true,
      protected54ReadOnlyValidation: true,
      houseOfOriginalsUntouched: true,
      morgansOriginalsUntouched: true,
      radissonCollectionUntouched: true,
      neverWriteFields: [...WAVE15_NEVER_WRITE_FIELDS],
      intentionalPublicRestoreRegistryUpdate: applyPerformed === true,
      writeThrottleMs: WRITE_THROTTLE_MS,
      pvqlConfirmFlagOnlyNotBlocking: true,
    },
    expectedAcceptance: {
      shouldRenderFullProfile: true,
      displayState: "active_profile_ready",
      activeUniverse: WAVE15_EXPECTED_FINAL_ACTIVE_COUNT,
      publicFullPvql: `${WAVE15_EXPECTED_FINAL_ACTIVE_COUNT}/${WAVE15_EXPECTED_FINAL_ACTIVE_COUNT}`,
      flexExcludedFromActive: true,
    },
    readyStatement,
  };

  const paths = writeReports(report, renderMarkdown(report));
  return { ...report, report, paths, pass: preflightOk || applyPerformed, ok: preflightOk || !apply };
}
