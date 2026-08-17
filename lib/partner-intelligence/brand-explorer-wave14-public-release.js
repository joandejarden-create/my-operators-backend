/**
 * Wave 14 Stage 10 — Partial public release for eight founder-approved brands.
 *
 * Allowed writes (eight Basics only): release/restore fields + intentional public
 * restore registry inclusion. Four Points Flex held. No Brand Status / CV / Source / Registry
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
  WAVE14_VERSION,
  WAVE14_PARTIAL_PROMOTION_SLUGS,
  WAVE14_HELD_PROMOTION_SLUG,
  WAVE14_PROTECTED_BASELINE_COUNT,
  WAVE14_EXPECTED_PARTIAL_ACTIVE_COUNT,
  WAVE14_PUBLIC_RELEASE_APPLY_FLAGS,
  WAVE14_RELEASE_FIELDS,
  WAVE14_NEVER_WRITE_FIELDS,
  WAVE14_FOUNDER_APPROVE_RECOMMENDATION,
  WAVE14_FOUNDER_HOLD_RECOMMENDATION,
  WAVE14_STATUS_FROM,
} from "./brand-explorer-wave14-factory-plan.js";
import { FACTORY_PREVIEW_CANDIDATE_IDENTITIES } from "./brand-explorer-factory-preview-candidates.js";

export const WAVE14_PUBLIC_RELEASE_VERSION = "wave14-partial-public-release-v1";

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
      console.warn("[wave14-public-release] json read failed", p, err?.message || err);
    }
    return null;
  }
}

function loadProtected46RecordIds() {
  const freeze =
    readJsonSafe("reports/brand-explorer-46-active-public-full-baseline.json") ||
    readJsonSafe("docs/data-intelligence/brand-explorer-46-active-public-full-baseline.json");
  const ids = new Set();
  for (const b of freeze?.brands || []) {
    const id = nz(b.recordId || b.id);
    if (id) ids.add(id);
  }
  return ids;
}

function partialIdentities() {
  return WAVE14_PARTIAL_PROMOTION_SLUGS.map((slug) => {
    const id = FACTORY_PREVIEW_CANDIDATE_IDENTITIES[slug];
    if (!id?.recordId) throw new Error(`Missing factory-preview identity for ${slug}`);
    return { slug, name: id.name, recordId: id.recordId };
  });
}

function heldIdentity() {
  const id = FACTORY_PREVIEW_CANDIDATE_IDENTITIES[WAVE14_HELD_PROMOTION_SLUG];
  if (!id?.recordId) throw new Error(`Missing held identity for ${WAVE14_HELD_PROMOTION_SLUG}`);
  return { slug: WAVE14_HELD_PROMOTION_SLUG, name: id.name, recordId: id.recordId };
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
    if (WAVE14_NEVER_WRITE_FIELDS.includes(k)) {
      throw new Error(`Refuse: never-write field in release payload: ${k}`);
    }
    if (k === "Brand Status") {
      throw new Error("Refuse: public-release must not write Brand Status");
    }
    if (!WAVE14_RELEASE_FIELDS.includes(k)) {
      throw new Error(`Refuse: unexpected release field: ${k}`);
    }
  }
  for (const required of WAVE14_RELEASE_FIELDS) {
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
    throw new Error("Refuse: Four Points Flex is held — no release field writes");
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
  const summary = readJsonSafe("reports/brand-explorer-wave14-founder-review-summary.json");
  const issues = [];
  if (!summary) return { ok: false, issues: ["missing_founder_review_summary"] };
  const brands = summary.brands || [];
  for (const slug of WAVE14_PARTIAL_PROMOTION_SLUGS) {
    const row = brands.find((b) => nz(b.brandSlug || b.slug).toLowerCase() === slug);
    if (nz(row?.recommendation) !== WAVE14_FOUNDER_APPROVE_RECOMMENDATION) {
      issues.push(`founder_not_approve:${slug}`);
    }
  }
  const flex = brands.find(
    (b) => nz(b.brandSlug || b.slug).toLowerCase() === WAVE14_HELD_PROMOTION_SLUG
  );
  if (nz(flex?.recommendation) !== WAVE14_FOUNDER_HOLD_RECOMMENDATION) {
    issues.push("flex_not_held_in_founder_summary");
  }
  return { ok: issues.length === 0, issues };
}

function writeReports(report, md) {
  fs.mkdirSync(REPORTS_DIR, { recursive: true });
  fs.mkdirSync(DOCS_DIR, { recursive: true });
  const jsonPath = path.join(REPORTS_DIR, "brand-explorer-wave14-partial-public-release.json");
  const mdPath = path.join(REPORTS_DIR, "brand-explorer-wave14-partial-public-release.md");
  fs.writeFileSync(jsonPath, `${JSON.stringify(report, null, 2)}\n`, "utf8");
  fs.writeFileSync(mdPath, md.endsWith("\n") ? md : `${md}\n`, "utf8");

  const docPath = path.join(DOCS_DIR, "brand-explorer-wave14-partial-release.md");
  fs.writeFileSync(
    docPath,
    [
      `# Wave 14 — Partial Status Promotion + Public Release`,
      ``,
      `Eight founder-approved brands only. Four Points Flex held.`,
      ``,
      `## Commands`,
      ``,
      "```bash",
      `npm run brand-explorer-wave14-factory -- --stage status-promotion --dry-run --approved-only`,
      `npm run brand-explorer-wave14-factory -- --stage public-release --dry-run --approved-only`,
      "```",
      ``,
      `## Ready statement`,
      ``,
      `\`${report.readyStatement || "wave14_eight_brand_partial_release_complete_flex_held"}\``,
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
    `# Brand Explorer Wave 14 — Partial Public Release`,
    ``,
    `Version: \`${r.version}\` · Generated: ${r.generatedAt}`,
    `Mode: **${r.applyPerformed ? "APPLY" : "dry-run"}** · writePerformed: **${r.writePerformed}**`,
    ``,
    `Active universe: **${r.universe?.totalCount ?? "n/a"}** (expected ${WAVE14_EXPECTED_PARTIAL_ACTIVE_COUNT})`,
    `Ready: \`${r.readyStatement}\``,
    ``,
    `## Scope`,
    ``,
    `- Release (8): ${WAVE14_PARTIAL_PROMOTION_SLUGS.map((s) => `\`${s}\``).join(", ")}`,
    `- Held: \`${WAVE14_HELD_PROMOTION_SLUG}\` — Under Review, no release fields`,
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
    `### Four Points Flex held`,
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
    `- Wave 14 partial added: ${(r.intentionalRegistry?.wave14Added || r.intentionalRegistry?.plannedAdd || []).join(", ") || "(none)"}`,
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
export async function runWave14PublicRelease({ apply = false, argv = [] } = {}) {
  const stage = "public-release";
  const flagCheck = checkFlags(WAVE14_PUBLIC_RELEASE_APPLY_FLAGS, argv, apply);
  const identities = partialIdentities();
  const held = heldIdentity();
  const protectedIds = loadProtected46RecordIds();
  const preflightIssues = [];
  const today = todayIsoDate();
  const releaseFields = buildReleaseFields(today);

  if (!argv.includes("--approved-only")) {
    preflightIssues.push("missing_--approved-only_partial_scope_flag");
  }

  for (const id of identities) {
    if (protectedIds.has(id.recordId)) {
      preflightIssues.push(`target_collides_with_protected_46:${id.slug}`);
    }
    if (id.slug === WAVE14_HELD_PROMOTION_SLUG) {
      preflightIssues.push("flex_in_partial_release_targets");
    }
  }

  const founderApprovals = assertFounderPartialApprovals();
  if (!founderApprovals.ok) preflightIssues.push(...founderApprovals.issues);

  const statusPromotion = readJsonSafe("reports/brand-explorer-wave14-partial-status-promotion.json");
  if (!statusPromotion?.applyPerformed && !statusPromotion?.summary?.acceptanceUniverse54) {
    // Allow if universe already 54 from a prior Stage 9 apply
  }

  const universe = await loadActiveUniverse({ includeDetails: false });
  if (universe.totalCount < WAVE14_PROTECTED_BASELINE_COUNT) {
    preflightIssues.push(`active_universe_too_small:${universe.totalCount}`);
  }
  if (
    universe.totalCount !== WAVE14_EXPECTED_PARTIAL_ACTIVE_COUNT &&
    universe.totalCount !== WAVE14_PROTECTED_BASELINE_COUNT
  ) {
    preflightIssues.push(
      `active_universe_unexpected:${universe.totalCount};expected_${WAVE14_EXPECTED_PARTIAL_ACTIVE_COUNT}_after_stage9`
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
      plannedFields: [...WAVE14_RELEASE_FIELDS],
    });
  }

  const flexLive = await fetchBrand(held.recordId);
  await sleep(90);
  const flexStatus = nz(flexLive.brandStatus || flexLive.status);
  const flexHeldOk = flexStatus === WAVE14_STATUS_FROM && !isBrandStatusActive(flexStatus);
  if (!flexHeldOk) {
    preflightIssues.push(`flex_not_under_review_at_release:${flexStatus || "(empty)"}`);
  }
  const flexInUniverse = (universe.brands || []).some(
    (b) => nz(b.slug).toLowerCase() === WAVE14_HELD_PROMOTION_SLUG
  );
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
    const wave14Added = WAVE14_PARTIAL_PROMOTION_SLUGS.filter((s) => !beforeSet.has(s));
    if (beforeSet.has(WAVE14_HELD_PROMOTION_SLUG)) {
      // Do not add Four Points Flex; also do not remove if somehow present — but never add
    }
    const merged = [
      ...new Set([...before.filter((s) => s !== WAVE14_HELD_PROMOTION_SLUG), ...WAVE14_PARTIAL_PROMOTION_SLUGS]),
    ];
    const registry = writeIntentionalPublicRestoreSlugs(merged);
    intentionalRegistryOutcome = {
      updated: true,
      beforeCount: before.length,
      afterCount: registry.slugs.length,
      wave14Added,
      flexAdded: false,
      path: "data/brand-explorer-public-restore-intentional.json",
      note: "Eight Wave 14 partial brands added for intentional public restore; Four Points Flex not added; no content/CV/Source/Registry-approval writes.",
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
    ? "wave14_eight_brand_partial_release_complete_flex_held"
    : "wave14_partial_public_release_dry_run";

  const report = {
    version: WAVE14_PUBLIC_RELEASE_VERSION,
    waveVersion: WAVE14_VERSION,
    stage,
    generatedAt: new Date().toISOString(),
    apply,
    applyPerformed,
    writePerformed,
    dryRun: !applyPerformed,
    approvedOnly: argv.includes("--approved-only"),
    flagCheck,
    requiredApplyFlags: [...WAVE14_PUBLIC_RELEASE_APPLY_FLAGS],
    plannedReleaseFields: [...WAVE14_RELEASE_FIELDS],
    releaseDate: today,
    preflight: {
      ok: preflightOk,
      issues: preflightIssues,
      founderApprovals,
      allBrandStatusActiveOrLive: allActive,
      flexHeldOk,
    },
    held: {
      slug: held.slug,
      recordId: held.recordId,
      brandStatus: flexStatus,
      releaseWrite: false,
      recommendation: WAVE14_FOUNDER_HOLD_RECOMMENDATION,
    },
    universe: {
      totalCount: universe.totalCount,
      expectedAfterPartialRelease: WAVE14_EXPECTED_PARTIAL_ACTIVE_COUNT,
      protectedBaselineWas: WAVE14_PROTECTED_BASELINE_COUNT,
      wave14PartialInUniverse: (universe.brands || [])
        .filter((b) => WAVE14_PARTIAL_PROMOTION_SLUGS.includes(nz(b.slug).toLowerCase()))
        .map((b) => b.slug),
      flexInUniverse,
    },
    brands,
    applyOutcome,
    intentionalRegistry: intentionalRegistryOutcome || {
      beforeCount: readIntentionalPublicRestoreSlugs().length,
      plannedAdd: WAVE14_PARTIAL_PROMOTION_SLUGS.filter(
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
    baselineFreezeNote:
      "Do **not** freeze a 54-brand baseline as the permanent protected baseline until Four Points Flex is cleaned and promoted (→ 55), **or** an explicit founder decision accepts an interim 54 freeze. Prefer waiting for Four Points Flex before revising EXPECTED_ACTIVE_COUNT_46 → 54/55.",
    mayFreeze54BaselineNow: false,
    waitForFlexBeforePermanentFreeze: true,
    guardrails: {
      targetBrandsOnly: true,
      eightApprovedOnly: true,
      flexHeld: true,
      companyValidatedWrites: false,
      sourceLibraryWrites: false,
      registryApprovalWrites: false,
      brandStatusWrites: false,
      contentRewrites: false,
      imageWrites: false,
      protected46Untouched: true,
      houseOfOriginalsUntouched: true,
      morgansOriginalsUntouched: true,
      radissonCollectionUntouched: true,
      neverWriteFields: [...WAVE14_NEVER_WRITE_FIELDS],
      intentionalPublicRestoreRegistryUpdate: applyPerformed === true,
    },
    expectedAcceptance: {
      shouldRenderFullProfile: true,
      displayState: "active_profile_ready",
      activeUniverse: WAVE14_EXPECTED_PARTIAL_ACTIVE_COUNT,
      publicFullPvql: `${WAVE14_EXPECTED_PARTIAL_ACTIVE_COUNT}/${WAVE14_EXPECTED_PARTIAL_ACTIVE_COUNT}`,
      flexExcludedFromActive: true,
    },
    readyStatement,
  };

  const paths = writeReports(report, renderMarkdown(report));
  return { ...report, report, paths, pass: preflightOk || applyPerformed, ok: preflightOk || !apply };
}
