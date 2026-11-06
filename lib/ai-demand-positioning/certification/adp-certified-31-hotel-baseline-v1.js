/**
 * ADP_CERTIFIED_31_HOTEL_BASELINE_20261021_V1
 * Immutable certified baseline registry + golden comparison helpers.
 * Replay must not mutate baseline, live Airtable, or published/staged customer artifacts.
 */

import fs from "node:fs";
import path from "node:path";
import crypto from "node:crypto";
import { fileURLToPath } from "node:url";
import { listCanonicalAdp31PropertyIds } from "../governance/adp-operational-registry-hygiene-v1.js";
import { getCensusRecordIdForAdpProperty } from "../census-link-registry.js";
import {
  loadPropertyProfile,
  loadLatestCoreCustomerPeriod,
  loadAllPeriods,
  isLoyaltyMeasurementPeriod,
} from "../data-model.js";
import { loadStagedOwnerPayload } from "../review/adp-staged-report-store-v1.js";
import { loadPublishedManifest } from "../published-snapshot.js";
import { independentlyRecalculateRealityGapV1V2 } from "./adp-rg-v2-cross-surface-parity-certification-v1.js";
import { CONDITIONAL_APPROVED_METHODOLOGY_ID } from "../intelligence/reality-gap-methodology-v2.js";
import { loadLoyaltyBppStagedPack } from "../brand-portfolio/loyalty-bpp-staged-pack-store-v1.js";
import { getBrandPortfolioPeerSet } from "../brand-portfolio/brand-portfolio-peer-set-v1.js";

export const ADP_CERTIFIED_31_HOTEL_BASELINE_ID =
  "ADP_CERTIFIED_31_HOTEL_BASELINE_20261021_V1";
export const ADP_FULL_E2E_CERTIFIED_BASELINE_REPLAY_V1 =
  "ADP_FULL_E2E_CERTIFIED_BASELINE_REPLAY_V1";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
export const REPO_ROOT = path.resolve(__dirname, "../../..");
export const BASELINE_OUT = path.join(
  REPO_ROOT,
  "reports/dealality-adp-certified-baseline-and-full-e2e-replay-v1"
);
const PDF_DIR = path.join(
  REPO_ROOT,
  "reports/dealality-adp-rg-v2-universal-integration-parity-v1/STAGED_PDFS"
);

function sha256(buf) {
  return crypto.createHash("sha256").update(buf).digest("hex");
}
function shaFile(fp) {
  return sha256(fs.readFileSync(fp));
}
function bppRank(bpp) {
  const kpi = (bpp?.kpis || []).find((k) => k.id === "portfolioRank");
  if (!kpi) return null;
  if (kpi.available === false || kpi.value === "—") {
    return "SUBJECT_MEASURED_RANK_BLOCKED";
  }
  return kpi.value || null;
}

/**
 * Build in-memory certified baseline rows for all 31 hotels (no Airtable writes).
 */
export function buildCertified31HotelBaselineRows() {
  const rows = [];
  for (const propertyId of listCanonicalAdp31PropertyIds()) {
    const profile = loadPropertyProfile(propertyId);
    const staged = loadStagedOwnerPayload(propertyId);
    const published = loadPublishedManifest(propertyId);
    const core = loadLatestCoreCustomerPeriod(propertyId);
    const loyalty = (loadAllPeriods(propertyId) || []).find((p) =>
      isLoyaltyMeasurementPeriod(p)
    );
    const liveRg = independentlyRecalculateRealityGapV1V2(propertyId);
    const bppPack = loadLoyaltyBppStagedPack(propertyId);
    const peerSet = getBrandPortfolioPeerSet(propertyId);
    const pdfPath = path.join(PDF_DIR, `${propertyId}.pdf`);
    const stagedPath = staged?.path || null;
    let stagedHash = null;
    if (staged?.versionId) {
      const candidate = path.join(
        REPO_ROOT,
        "data/ai-demand-positioning/staged",
        propertyId,
        `report-${staged.versionId}.json`
      );
      if (fs.existsSync(candidate)) stagedHash = shaFile(candidate);
    }
    rows.push({
      baselineId: ADP_CERTIFIED_31_HOTEL_BASELINE_ID,
      propertyId,
      hotelName: profile?.name || propertyId,
      hpcRecordId: getCensusRecordIdForAdpProperty(propertyId) || null,
      corePeriodId: core?.periodId || null,
      coreObservationCount: core?.observations?.length || 0,
      loyaltyPeriodId: loyalty?.periodId || null,
      loyaltyObservationCount: loyalty?.observations?.length || 0,
      stagedVersionId: staged?.versionId || null,
      stagedPayloadHash: stagedHash,
      publishedVersionId: published?.latestVersionId || null,
      realityGapMethodologyId:
        staged?.payload?.realityGap?.methodologyId ||
        CONDITIONAL_APPROVED_METHODOLOGY_ID,
      realityGapV2: liveRg.ok ? liveRg.gapScoreV2 : staged?.payload?.realityGap?.gapScore,
      realityGapV1: liveRg.ok ? liveRg.gapScoreV1 : staged?.payload?.realityGap?.gapScoreBinaryV1,
      testedEligible: liveRg.ok ? liveRg.testedEligible : null,
      recognized: liveRg.ok ? liveRg.recognized : null,
      partial: liveRg.ok ? liveRg.partial : null,
      missed: liveRg.ok ? liveRg.missed : null,
      bppRank: bppRank(staged?.payload?.brandPortfolioPosition || bppPack?.payload),
      peerSetId: peerSet?.peerSetId || null,
      peerCount: peerSet?.peerCountExcludingSubject ?? null,
      pdfExists: fs.existsSync(pdfPath),
      pdfSha256: fs.existsSync(pdfPath) ? shaFile(pdfPath) : null,
      attributeCount: Array.isArray(profile?.attributes)
        ? profile.attributes.length
        : Object.keys(profile?.attributes || {}).length,
    });
  }
  return rows;
}

/**
 * Exact-equivalence compare of golden metrics vs live recalc (replay-safe).
 */
export function compareHotelToGolden(propertyId, goldenRow) {
  const live = independentlyRecalculateRealityGapV1V2(propertyId);
  const staged = loadStagedOwnerPayload(propertyId);
  const issues = [];
  if (!live.ok) issues.push(`recalc_${live.error}`);
  if (live.ok && live.gapScoreV2 !== goldenRow.realityGapV2) {
    issues.push(`rg_v2 ${live.gapScoreV2}!=${goldenRow.realityGapV2}`);
  }
  if (live.ok && live.gapScoreV1 !== goldenRow.realityGapV1) {
    issues.push(`rg_v1 ${live.gapScoreV1}!=${goldenRow.realityGapV1}`);
  }
  const goldenMeth =
    goldenRow.realityGapMethodologyId || goldenRow.methodologyId || null;
  if (
    staged?.payload?.realityGap?.methodologyId &&
    goldenMeth &&
    staged.payload.realityGap.methodologyId !== goldenMeth
  ) {
    issues.push("methodology_drift");
  }
  if (staged?.versionId !== goldenRow.stagedVersionId) {
    issues.push(
      `staged_version_drift ${staged?.versionId}!=${goldenRow.stagedVersionId}`
    );
  }
  return {
    propertyId,
    ok: issues.length === 0,
    issues,
    equivalence: {
      exact: issues.length === 0,
      semantic: "NARRATIVE_NOT_COMPARED_IN_THIS_HELPER",
      presentation: "ARTIFACT_HASH_COMPARE_SEPARATE",
    },
  };
}

export function verifyBaselineIntegrity(manifestPath = null) {
  const p =
    manifestPath ||
    path.join(BASELINE_OUT, "CERTIFIED_31_HOTEL_BASELINE_MANIFEST.json");
  if (!fs.existsSync(p)) {
    return { ok: false, error: "manifest_missing", path: p };
  }
  const man = JSON.parse(fs.readFileSync(p, "utf8"));
  const rows = man.hotels || man.rows || [];
  const issues = [];
  if (rows.length !== 31) issues.push(`hotel_count_${rows.length}`);
  for (const r of rows) {
    if (!r.corePeriodId) issues.push(`${r.propertyId}:missing_core`);
    if (!r.stagedVersionId) issues.push(`${r.propertyId}:missing_staged`);
    if (r.pdfExists && r.pdfSha256) {
      const pdfPath = path.join(PDF_DIR, `${r.propertyId}.pdf`);
      if (!fs.existsSync(pdfPath)) issues.push(`${r.propertyId}:pdf_missing`);
      else if (shaFile(pdfPath) !== r.pdfSha256) {
        issues.push(`${r.propertyId}:pdf_hash_drift`);
      }
    }
  }
  return { ok: issues.length === 0, issues, baselineId: man.baselineId };
}
