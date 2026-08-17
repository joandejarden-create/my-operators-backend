/**
 * Wave 16A LOW-risk — Public release (visibility fields + intentional restore).
 *
 * Allowed Basics writes (three brands only):
 *   Active Profile Approved, Ready for Active Profile, Active Profile Approved Date
 *
 * Founder Visual Review Pass is intentionally NOT written.
 * Display-state `active_profile_ready` enables shouldRenderFullProfile without FVR Pass;
 * founder will visually review in the live app after publication.
 *
 * No Brand Status / CV / Census / Momentum / content / image / Flex writes.
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
  WAVE16A_VERSION,
  WAVE16A_LOW_RISK_RELEASE_SLUGS,
  WAVE16A_IDENTITIES,
  WAVE16A_FLEX_HOLD,
  WAVE16A_EXPECTED_FINAL_ACTIVE_COUNT,
  WAVE16A_PUBLIC_RELEASE_APPLY_FLAGS,
  WAVE16A_RELEASE_FIELDS,
  WAVE16A_NEVER_WRITE_FIELDS,
} from "./brand-explorer-wave16a-factory-plan.js";

export const WAVE16A_PUBLIC_RELEASE_VERSION = "wave16a-low-risk-public-release-v1";

const BASICS_TABLE = "Brand Setup - Brand Basics";
const WRITE_THROTTLE_MS = 280;

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../..");
const REPORTS = path.join(ROOT, "reports");
const DOCS = path.join(ROOT, "docs", "data-intelligence");

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
  return { apply: apply === true, ok: apply === true && missing.length === 0, missing, required: [...required] };
}

function identities() {
  return WAVE16A_LOW_RISK_RELEASE_SLUGS.map((slug) => {
    const id = WAVE16A_IDENTITIES[slug];
    if (!id?.recordId) throw new Error(`Missing Wave 16A identity for ${slug}`);
    return { slug, name: id.exactBrandBasicsName, recordId: id.recordId };
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
  };
}

function assertReleasePayload(fields) {
  for (const k of Object.keys(fields || {})) {
    if (WAVE16A_NEVER_WRITE_FIELDS.includes(k)) {
      throw new Error(`Refuse: never-write field in release payload: ${k}`);
    }
    if (k === "Brand Status") throw new Error("Refuse: public-release must not write Brand Status");
    if (k === "Founder Visual Review Pass") {
      throw new Error("Refuse: Wave 16A LOW-risk does not write Founder Visual Review Pass");
    }
    if (!WAVE16A_RELEASE_FIELDS.includes(k)) {
      throw new Error(`Refuse: unexpected release field: ${k}`);
    }
  }
  for (const required of WAVE16A_RELEASE_FIELDS) {
    if (!(required in fields)) throw new Error(`Refuse: missing required release field: ${required}`);
  }
}

async function patchBasicsRelease({ recordId, fields }) {
  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey) throw new Error("AIRTABLE_BASE_ID and AIRTABLE_API_KEY required");
  assertReleasePayload(fields);
  if (recordId === WAVE16A_FLEX_HOLD.recordId) {
    throw new Error("Refuse: Four Points Flex release write");
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

export async function runWave16aLowRiskPublicRelease({ apply = false, argv = [] } = {}) {
  const flagCheck = checkFlags(WAVE16A_PUBLIC_RELEASE_APPLY_FLAGS, argv, apply);
  const ids = identities();
  const issues = [];
  const today = todayIsoDate();
  const releaseFields = buildReleaseFields(today);

  const statusPromotion = (() => {
    try {
      return JSON.parse(
        fs.readFileSync(path.join(REPORTS, "brand-explorer-wave16a-low-risk-status-promotion.json"), "utf8")
      );
    } catch {
      return null;
    }
  })();

  const universe = await loadActiveUniverse({ includeDetails: false });
  const threeInUniverse = (universe.brands || []).filter((b) =>
    WAVE16A_LOW_RISK_RELEASE_SLUGS.includes(nz(b.slug).toLowerCase())
  );
  if (universe.totalCount !== WAVE16A_EXPECTED_FINAL_ACTIVE_COUNT || threeInUniverse.length !== 3) {
    issues.push(
      `active_universe_not_65_with_three:got=${universe.totalCount};three=${threeInUniverse.length}`
    );
  }
  if (!(statusPromotion?.applyPerformed === true || threeInUniverse.length === 3)) {
    issues.push("status_promotion_not_confirmed");
  }

  const flexLive = await fetchBrand(WAVE16A_FLEX_HOLD.recordId);
  const flexStatus = nz(flexLive.brandStatus || flexLive.status);
  const flexInUniverse = (universe.brands || []).some(
    (b) => nz(b.recordId) === WAVE16A_FLEX_HOLD.recordId
  );
  if (flexStatus !== "Under Review" || flexInUniverse) {
    issues.push(`flex_hold_broken:status=${flexStatus};inUniverse=${flexInUniverse}`);
  }

  const brands = [];
  for (const id of ids) {
    const live = await fetchBrand(id.recordId);
    await sleep(90);
    const status = nz(live.brandStatus || live.status);
    if (!isBrandStatusActive(status)) {
      issues.push(`brand_status_not_active:${id.slug}:${status}`);
    }
    const alreadyApproved =
      live.activeProfileApproved === true || live.readyForActiveProfile === true;
    brands.push({
      slug: id.slug,
      name: id.name,
      recordId: id.recordId,
      brandStatus: status,
      isActiveOrLive: isBrandStatusActive(status),
      founderVisualReviewPass: live.founderVisualReviewPass === true,
      needsWrite: !alreadyApproved,
      companyValidated: live.governance?.companyValidated === true,
    });
  }

  const preflightPass = issues.length === 0;
  const applyResults = [];
  let writePerformed = false;
  let intentionalRegistryOutcome = null;

  if (apply) {
    if (!flagCheck.ok) {
      return {
        version: WAVE16A_PUBLIC_RELEASE_VERSION,
        pass: false,
        readyStatement: "wave16a_low_risk_public_release_blocked_missing_flags",
        flagCheck,
        preflightIssues: issues,
      };
    }
    if (!preflightPass) {
      return {
        version: WAVE16A_PUBLIC_RELEASE_VERSION,
        pass: false,
        readyStatement: "wave16a_low_risk_public_release_blocked_preflight",
        flagCheck,
        preflightIssues: issues,
        brands,
      };
    }
    for (const b of brands) {
      if (!b.needsWrite) {
        applyResults.push({ slug: b.slug, recordId: b.recordId, applied: false, skipped: true });
        continue;
      }
      const result = await patchBasicsRelease({ recordId: b.recordId, fields: releaseFields });
      writePerformed = true;
      applyResults.push({
        slug: b.slug,
        recordId: b.recordId,
        applied: true,
        fieldsPatched: result.fieldsPatched,
        sanitizedPayloadPreview: result.sanitizedPayloadPreview,
      });
      await sleep(WRITE_THROTTLE_MS);
    }

    const beforeSlugs = readIntentionalPublicRestoreSlugs();
    const nextSlugs = [...new Set([...beforeSlugs, ...WAVE16A_LOW_RISK_RELEASE_SLUGS])];
    const added = WAVE16A_LOW_RISK_RELEASE_SLUGS.filter((s) => !beforeSlugs.includes(s));
    if (added.length) {
      writeIntentionalPublicRestoreSlugs(nextSlugs);
      writePerformed = true;
    }
    intentionalRegistryOutcome = {
      beforeCount: beforeSlugs.length,
      afterCount: nextSlugs.length,
      wave16aLowRiskAdded: added,
      flexAdded: false,
      updated: added.length > 0,
      path: "data/brand-explorer-public-restore-intentional.json",
    };
  }

  const pass = preflightPass && (!apply || applyResults.every((r) => r.applied || r.skipped));

  const report = {
    version: WAVE16A_PUBLIC_RELEASE_VERSION,
    wave16aVersion: WAVE16A_VERSION,
    stage: "public-release",
    generatedAt: new Date().toISOString(),
    applyPerformed: apply === true,
    writePerformed,
    flagCheck,
    preflightIssues: issues,
    preflightPass,
    universe: { totalCount: universe.totalCount },
    plannedReleaseFields: [...WAVE16A_RELEASE_FIELDS],
    founderVisualReviewPassPolicy:
      "left_unchanged — not required for active_profile_ready full-profile render; founder reviews in-app after publication",
    brands,
    applyResults,
    intentionalRegistry: intentionalRegistryOutcome,
    held: {
      slug: WAVE16A_FLEX_HOLD.slug,
      recordId: WAVE16A_FLEX_HOLD.recordId,
      brandStatus: flexStatus,
      inActiveUniverse: flexInUniverse,
      writes: 0,
    },
    writeAudit: {
      releaseFieldWrites: applyResults.filter((r) => r.applied).length,
      founderVisualReviewPassWrites: 0,
      brandStatusWrites: 0,
      fourPointsFlexWrites: 0,
      remainingWave16aWrites: 0,
      wave16bWrites: 0,
      active62ContentWrites: 0,
      companyValidatedWrites: 0,
      recentMomentumWrites: 0,
      intentionalRestoreRegistryUpdates: intentionalRegistryOutcome?.updated ? 1 : 0,
    },
    pass,
    readyStatement: !apply
      ? "wave16a_low_risk_public_release_dry_run_ready"
      : pass
        ? "wave16a_low_risk_public_release_applied_ready_for_post_validation"
        : "wave16a_low_risk_public_release_blocked",
  };

  const md = [
    `# Wave 16A LOW-risk — Public Release`,
    ``,
    `- Ready: \`${report.readyStatement}\``,
    `- Active universe: **${universe.totalCount}** (expected ${WAVE16A_EXPECTED_FINAL_ACTIVE_COUNT})`,
    `- Release fields: ${WAVE16A_RELEASE_FIELDS.map((f) => `\`${f}\``).join(", ")}`,
    `- Founder Visual Review Pass writes: **0** (left unchanged)`,
    `- Flex writes: **0**`,
    ``,
    ...brands.map(
      (b) =>
        `- ${b.name}: status=${b.brandStatus} · needsReleaseWrite=${b.needsWrite} · FVRPass=${b.founderVisualReviewPass}`
    ),
    ``,
  ].join("\n");

  fs.mkdirSync(REPORTS, { recursive: true });
  fs.mkdirSync(DOCS, { recursive: true });
  fs.writeFileSync(path.join(REPORTS, "brand-explorer-wave16a-low-risk-public-release.json"), JSON.stringify(report, null, 2));
  fs.writeFileSync(path.join(REPORTS, "brand-explorer-wave16a-low-risk-public-release.md"), md);
  fs.writeFileSync(path.join(DOCS, "brand-explorer-wave16a-low-risk-public-release.md"), md);
  return report;
}
