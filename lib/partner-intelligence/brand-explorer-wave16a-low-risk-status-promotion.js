/**
 * Wave 16A LOW-risk Stage — Brand Status promotion (Under Review → Active).
 *
 * Scope: Fairfield by Marriott, Four Points by Sheraton, Delta Hotels by Marriott.
 * Allowed write: Brand Status only on those three Basics records.
 * Four Points Flex / remaining Wave 16A / Wave 16B / Active 62: never written.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { getBrandLibraryBrandById } from "../../api/brand-library.js";
import { isBrandStatusActive } from "../brand-status-active.js";
import { loadActiveUniverse } from "./brand-explorer-active-universe.js";
import {
  WAVE16A_VERSION,
  WAVE16A_LOW_RISK_RELEASE_SLUGS,
  WAVE16A_IDENTITIES,
  WAVE16A_FLEX_HOLD,
  WAVE16A_PROTECTED_BASELINE_COUNT,
  WAVE16A_EXPECTED_FINAL_ACTIVE_COUNT,
  WAVE16A_STATUS_FROM,
  WAVE16A_STATUS_TO_PREFERRED,
  WAVE16A_STATUS_TO_ALLOWED,
  WAVE16A_STATUS_PROMOTION_APPLY_FLAGS,
} from "./brand-explorer-wave16a-factory-plan.js";

export const WAVE16A_STATUS_PROMOTION_VERSION = "wave16a-low-risk-status-promotion-v1";

const BASICS_TABLE = "Brand Setup - Brand Basics";
const WRITE_THROTTLE_MS = 280;
const FORBIDDEN_REMAINING = Object.freeze([
  "jw-marriott",
  "w-hotels",
  "st-regis",
  "luxury-collection",
  "ritz-carlton",
  "edition",
  "le-meridien",
  "renaissance",
]);

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

async function fetchBrand(recordId) {
  const res = mockRes();
  await getBrandLibraryBrandById({ query: { brandId: recordId }, headers: {} }, res);
  if (res.statusCode >= 400 || !res.payload?.brand) {
    throw new Error(`fetch failed ${recordId}: ${res.statusCode}`);
  }
  return res.payload.brand;
}

function identities() {
  return WAVE16A_LOW_RISK_RELEASE_SLUGS.map((slug) => {
    const id = WAVE16A_IDENTITIES[slug];
    if (!id?.recordId) throw new Error(`Missing Wave 16A identity for ${slug}`);
    return {
      slug,
      name: id.exactBrandBasicsName,
      recordId: id.recordId,
    };
  });
}

async function patchBrandStatus({ recordId, targetStatus }) {
  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey) throw new Error("AIRTABLE_BASE_ID and AIRTABLE_API_KEY required");
  if (recordId === WAVE16A_FLEX_HOLD.recordId) {
    throw new Error("Refuse: Four Points Flex Brand Status write");
  }
  const fields = { "Brand Status": targetStatus };
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

export async function runWave16aLowRiskStatusPromotion({
  apply = false,
  argv = [],
  targetStatus = WAVE16A_STATUS_TO_PREFERRED,
} = {}) {
  if (!WAVE16A_STATUS_TO_ALLOWED.includes(targetStatus)) {
    throw new Error(`Refuse: targetStatus ${targetStatus} not allowed`);
  }
  const flagCheck = checkFlags(WAVE16A_STATUS_PROMOTION_APPLY_FLAGS, argv, apply);
  const ids = identities();
  const issues = [];

  if (ids.length !== 3) issues.push(`scope_mismatch:${ids.length}`);
  for (const id of ids) {
    if (FORBIDDEN_REMAINING.includes(id.slug)) issues.push(`forbidden_remaining:${id.slug}`);
    if (id.recordId === WAVE16A_FLEX_HOLD.recordId) issues.push("flex_in_targets");
  }

  const universeBefore = await loadActiveUniverse({ includeDetails: false });
  if (universeBefore.totalCount !== WAVE16A_PROTECTED_BASELINE_COUNT) {
    // Allow idempotent re-run at 65 if already promoted
    const already = (universeBefore.brands || []).filter((b) =>
      WAVE16A_LOW_RISK_RELEASE_SLUGS.includes(nz(b.slug).toLowerCase())
    );
    if (
      !(
        universeBefore.totalCount === WAVE16A_EXPECTED_FINAL_ACTIVE_COUNT &&
        already.length === 3
      )
    ) {
      issues.push(
        `active_universe_unexpected:got=${universeBefore.totalCount};expected=${WAVE16A_PROTECTED_BASELINE_COUNT}`
      );
    }
  }

  const flexLive = await fetchBrand(WAVE16A_FLEX_HOLD.recordId);
  const flexStatus = nz(flexLive.brandStatus || flexLive.status);
  const flexInUniverse = (universeBefore.brands || []).some(
    (b) => nz(b.recordId) === WAVE16A_FLEX_HOLD.recordId || /four-points-flex/i.test(nz(b.slug))
  );
  if (flexStatus !== "Under Review" || flexInUniverse) {
    issues.push(`flex_hold_broken:status=${flexStatus};inUniverse=${flexInUniverse}`);
  }

  const brands = [];
  for (const id of ids) {
    const live = await fetchBrand(id.recordId);
    await sleep(90);
    const from = nz(live.brandStatus || live.status);
    if (from !== WAVE16A_STATUS_FROM && from !== targetStatus) {
      issues.push(`status_unexpected:${id.slug}:${from || "(empty)"}`);
    }
    brands.push({
      slug: id.slug,
      name: id.name,
      recordId: id.recordId,
      from,
      to: targetStatus,
      needsWrite: from !== targetStatus,
    });
  }

  const preflightPass = issues.length === 0;
  const applyResults = [];
  let writePerformed = false;

  if (apply) {
    if (!flagCheck.ok) {
      return {
        version: WAVE16A_STATUS_PROMOTION_VERSION,
        pass: false,
        readyStatement: "wave16a_low_risk_status_promotion_blocked_missing_flags",
        flagCheck,
        preflightIssues: issues,
      };
    }
    if (!preflightPass) {
      return {
        version: WAVE16A_STATUS_PROMOTION_VERSION,
        pass: false,
        readyStatement: "wave16a_low_risk_status_promotion_blocked_preflight",
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
      const result = await patchBrandStatus({
        recordId: b.recordId,
        targetStatus: b.to,
      });
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
  }

  const universeAfter = await loadActiveUniverse({ includeDetails: false });
  const threeInAfter = (universeAfter.brands || []).filter((b) =>
    WAVE16A_LOW_RISK_RELEASE_SLUGS.includes(nz(b.slug).toLowerCase())
  );
  const pass =
    preflightPass &&
    (!apply ||
      (universeAfter.totalCount === WAVE16A_EXPECTED_FINAL_ACTIVE_COUNT &&
        threeInAfter.length === 3 &&
        !isBrandStatusActive(flexStatus)));

  const report = {
    version: WAVE16A_STATUS_PROMOTION_VERSION,
    wave16aVersion: WAVE16A_VERSION,
    stage: "status-promotion",
    generatedAt: new Date().toISOString(),
    applyPerformed: apply === true,
    writePerformed,
    flagCheck,
    targetBrandStatus: targetStatus,
    preflightIssues: issues,
    preflightPass,
    universeBefore: { totalCount: universeBefore.totalCount },
    universeAfter: { totalCount: universeAfter.totalCount },
    brands,
    applyResults,
    held: {
      slug: WAVE16A_FLEX_HOLD.slug,
      recordId: WAVE16A_FLEX_HOLD.recordId,
      brandStatus: flexStatus,
      inActiveUniverse: flexInUniverse,
      writes: 0,
    },
    writeAudit: {
      brandStatusWrites: applyResults.filter((r) => r.applied).length,
      fourPointsFlexWrites: 0,
      remainingWave16aWrites: 0,
      wave16bWrites: 0,
      active62ContentWrites: 0,
      companyValidatedWrites: 0,
      recentMomentumWrites: 0,
      releaseFieldWrites: 0,
      founderVisualReviewPassWrites: 0,
    },
    pass,
    readyStatement: !apply
      ? "wave16a_low_risk_status_promotion_dry_run_ready"
      : pass
        ? "wave16a_low_risk_status_promotion_applied_ready_for_public_release"
        : "wave16a_low_risk_status_promotion_blocked",
  };

  const md = [
    `# Wave 16A LOW-risk — Status Promotion`,
    ``,
    `- Ready: \`${report.readyStatement}\``,
    `- Active universe: **${report.universeBefore.totalCount} → ${report.universeAfter.totalCount}**`,
    `- Target Brand Status: **${targetStatus}**`,
    `- Flex writes: **0** · Flex status: **${flexStatus}**`,
    ``,
    ...brands.map((b) => `- ${b.name}: ${b.from} → ${b.to} (needsWrite=${b.needsWrite})`),
    ``,
  ].join("\n");

  fs.mkdirSync(REPORTS, { recursive: true });
  fs.mkdirSync(DOCS, { recursive: true });
  fs.writeFileSync(path.join(REPORTS, "brand-explorer-wave16a-low-risk-status-promotion.json"), JSON.stringify(report, null, 2));
  fs.writeFileSync(path.join(REPORTS, "brand-explorer-wave16a-low-risk-status-promotion.md"), md);
  fs.writeFileSync(path.join(DOCS, "brand-explorer-wave16a-low-risk-status-promotion.md"), md);
  return report;
}
