/**
 * Immutable greenfield Full ADP E2E hotel baselines (Chapultepec / Condesa pattern).
 * Content-addressed manifests + archive file hashes. No credentials. No paid calls.
 */

import fs from "node:fs";
import path from "node:path";
import crypto from "node:crypto";
import { fileURLToPath } from "node:url";
import {
  loadPropertyProfile,
  loadLatestCoreCustomerPeriod,
  loadAllPeriods,
  isLoyaltyMeasurementPeriod,
} from "../data-model.js";
import { loadStagedOwnerPayload } from "../review/adp-staged-report-store-v1.js";
import { getBrandPortfolioPeerSet } from "../brand-portfolio/brand-portfolio-peer-set-v1.js";
import { loadLoyaltyBppStagedPack } from "../brand-portfolio/loyalty-bpp-staged-pack-store-v1.js";
import { getCensusRecordIdForAdpProperty } from "../census-link-registry.js";
import { independentlyRecalculateRealityGapV1V2 } from "./adp-rg-v2-cross-surface-parity-certification-v1.js";
import {
  ensureAutonomousCompetitorRegistry,
  reconcileDisplacementFourState,
} from "./adp-autonomous-competitor-entity-resolution-v1.js";

export const ADP_GREENFIELD_CHAPULTEPEC_20261022_V1 =
  "ADP_GREENFIELD_GRAND_FIESTA_AMERICANA_CHAPULTEPEC_20261022_V1";
export const ADP_GREENFIELD_CONDESA_20261022_V1 =
  "ADP_GREENFIELD_FIESTA_AMERICANA_CONDESA_CANCUN_20261022_V1";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
export const REPO_ROOT = path.resolve(__dirname, "../../..");

function sha256(buf) {
  return crypto.createHash("sha256").update(buf).digest("hex");
}
function shaFile(fp) {
  if (!fp || !fs.existsSync(fp)) return null;
  return sha256(fs.readFileSync(fp));
}

function periodArchivePaths(propertyId, periodId) {
  const base = path.join(
    REPO_ROOT,
    "data/ai-demand-positioning/runtime",
    propertyId
  );
  const candidates = [
    path.join(base, `${periodId}.json`),
    path.join(base, "periods", `${periodId}.json`),
    path.join(base, "loyalty", `${periodId}.json`),
  ];
  return candidates.filter((p) => fs.existsSync(p));
}

/**
 * Build a freezeable greenfield baseline manifest for one hotel.
 */
export function buildGreenfieldHotelBaselineManifest({
  baselineId,
  propertyId,
  hotelName,
  hpcRecordId = null,
  runId = null,
  researchCostsUsd = null,
  pdfPath = null,
  shareManifest = null,
  authorizationRefs = [],
  codeVersion = null,
  dirtyWorkingTree = null,
} = {}) {
  const profile = loadPropertyProfile(propertyId);
  const staged = loadStagedOwnerPayload(propertyId);
  const core = loadLatestCoreCustomerPeriod(propertyId);
  const loyalty = (loadAllPeriods(propertyId) || []).find((p) =>
    isLoyaltyMeasurementPeriod(p)
  );
  const peerSet = getBrandPortfolioPeerSet(propertyId);
  const bppPack = loadLoyaltyBppStagedPack(propertyId);
  const rgLive = independentlyRecalculateRealityGapV1V2(propertyId);
  const er = ensureAutonomousCompetitorRegistry(propertyId, { persist: false });
  const disp = reconcileDisplacementFourState(propertyId);
  const bpp = staged?.payload?.brandPortfolioPosition || null;
  const kpis = Object.fromEntries(
    (bpp?.kpis || []).map((k) => [k.id, k.value])
  );

  const corePaths = periodArchivePaths(propertyId, core?.periodId);
  const loyPaths = periodArchivePaths(propertyId, loyalty?.periodId);
  const peerArchives = (peerSet?.included || []).map((p) => {
    const peerId = p.canonicalEntityId || p.propertyId;
    const peerLoy = (loadAllPeriods(peerId) || []).find((x) =>
      isLoyaltyMeasurementPeriod(x)
    );
    const paths = periodArchivePaths(peerId, peerLoy?.periodId);
    return {
      peerHotel: p.peerHotel || p.name,
      canonicalEntityId: peerId,
      periodId: peerLoy?.periodId || null,
      observations: peerLoy?.observations?.length || 0,
      archivePaths: paths.map((fp) => path.relative(REPO_ROOT, fp).replace(/\\/g, "/")),
      archiveSha256: paths.map(shaFile),
    };
  });

  const stagedReportPath = staged?.versionId
    ? path.join(
        REPO_ROOT,
        "data/ai-demand-positioning/staged",
        propertyId,
        `report-${staged.versionId}.json`
      )
    : null;

  const pdfSha = shaFile(pdfPath);

  return {
    contract: "ADP_GREENFIELD_HOTEL_BASELINE_V1",
    baselineId,
    frozenAt: new Date().toISOString(),
    propertyId,
    hotelName: hotelName || profile?.name || propertyId,
    hpcRecordId:
      hpcRecordId || getCensusRecordIdForAdpProperty(propertyId) || profile?.hpcHotelId || null,
    runId,
    researchCostsUsd,
    identity: {
      brand: profile?.brand || null,
      parentCompany: profile?.parentCompany || null,
      city: profile?.city || null,
      country: profile?.country || null,
      attributes: profile?.attributes || [],
    },
    research: {
      corePeriodId: core?.periodId || null,
      coreObservations: core?.observations?.length || 0,
      coreArchivePaths: corePaths.map((fp) =>
        path.relative(REPO_ROOT, fp).replace(/\\/g, "/")
      ),
      coreArchiveSha256: corePaths.map(shaFile),
      subjectLoyaltyPeriodId: loyalty?.periodId || null,
      subjectLoyaltyObservations: loyalty?.observations?.length || 0,
      subjectLoyaltyArchivePaths: loyPaths.map((fp) =>
        path.relative(REPO_ROOT, fp).replace(/\\/g, "/")
      ),
      subjectLoyaltyArchiveSha256: loyPaths.map(shaFile),
      peers: peerArchives,
    },
    peerSet: {
      peerSetId: peerSet?.peerSetId || null,
      lensId: peerSet?.lensId || null,
      included: (peerSet?.included || []).map((p) => ({
        peerHotel: p.peerHotel,
        canonicalEntityId: p.canonicalEntityId || p.propertyId,
      })),
    },
    analytics: {
      realityGapMethodologyId: staged?.payload?.realityGap?.methodologyId || null,
      realityGapV2:
        staged?.payload?.realityGap?.gapScore ??
        (rgLive.ok ? rgLive.gapScoreV2 : null),
      realityGapLiveV2: rgLive.ok ? rgLive.gapScoreV2 : null,
      certifiedCompetitors: er?.certifiedCompetitors?.length ?? null,
      topAlternative:
        er?.certifiedCompetitors?.[0]?.canonical ||
        er?.certifiedCompetitors?.[0]?.canonicalName ||
        null,
      displacementScenarioCount: disp?.displacementScenarioCount ?? null,
      displacementExclusiveSumEquals63:
        disp?.reconciliation?.exclusiveSumEquals63 ?? null,
      bppCustomerState: bpp?.bppCustomerState || null,
      bppKpis: kpis,
      bppRankingRows: (bpp?.ranking?.rows || []).map((r) => ({
        rank: r.rank,
        name: r.name,
        presence: r.presenceDisplay || r.presencePct,
        isSubject: r.isSubject,
        canonicalEntityId: r.canonicalEntityId,
      })),
    },
    artifacts: {
      stagedVersionId: staged?.versionId || null,
      stagedArtifactHash: staged?.artifactHash || null,
      stagedReportPath: stagedReportPath
        ? path.relative(REPO_ROOT, stagedReportPath).replace(/\\/g, "/")
        : null,
      stagedReportSha256: shaFile(stagedReportPath),
      loyaltyBppPackVersionId: bppPack?.versionId || null,
      pdfPath: pdfPath
        ? path.relative(REPO_ROOT, pdfPath).replace(/\\/g, "/")
        : null,
      pdfSha256: pdfSha,
      shareManifest: shareManifest
        ? {
            candidateId: shareManifest.candidateId || null,
            tokenId: shareManifest.tokenId || null,
            localTestOnly: true,
            // Never persist bearer / capability secrets
            secretRedacted: true,
          }
        : null,
    },
    competitorRegistry: {
      source: er?.registrySource || null,
      certifiedCount: er?.certifiedCompetitors?.length ?? null,
      sampleObservationIds:
        er?.certifiedCompetitors?.[0]?.sampleObservationIds?.slice?.(0, 5) || [],
    },
    authorizationRefs,
    provenance: {
      codeVersion,
      dirtyWorkingTree,
      disasterRecoveryNote:
        "31-hotel backup and greenfield archives currently share the C: volume — isolate copies for disaster recovery when an approved destination is available.",
    },
    goldenMetrics: {
      realityGapV2:
        staged?.payload?.realityGap?.gapScore ??
        (rgLive.ok ? rgLive.gapScoreV2 : null),
      certifiedCompetitors: er?.certifiedCompetitors?.length ?? null,
      displacementScenarioCount: disp?.displacementScenarioCount ?? null,
      bppPresence: kpis.portfolioAiPresence || null,
      bppRank: kpis.portfolioRank || null,
      bppNumberOne: kpis.numberOneAppearance || null,
      bppTop3: kpis.top3Appearance || null,
      coreObservations: core?.observations?.length || 0,
      subjectLoyaltyObservations: loyalty?.observations?.length || 0,
      peerLoyaltyObservationsTotal: peerArchives.reduce(
        (s, p) => s + (p.observations || 0),
        0
      ),
    },
  };
}

/**
 * Zero-paid numerical replay against a frozen greenfield baseline goldenMetrics.
 */
export function replayGreenfieldBaselineAgainstLive(manifest) {
  const propertyId = manifest.propertyId;
  const golden = manifest.goldenMetrics || {};
  const staged = loadStagedOwnerPayload(propertyId);
  const core = loadLatestCoreCustomerPeriod(propertyId);
  const loyalty = (loadAllPeriods(propertyId) || []).find((p) =>
    isLoyaltyMeasurementPeriod(p)
  );
  const rgLive = independentlyRecalculateRealityGapV1V2(propertyId);
  const er = ensureAutonomousCompetitorRegistry(propertyId, { persist: false });
  const disp = reconcileDisplacementFourState(propertyId);
  const bpp = staged?.payload?.brandPortfolioPosition || null;
  const kpis = Object.fromEntries(
    (bpp?.kpis || []).map((k) => [k.id, k.value])
  );
  const live = {
    realityGapV2: rgLive.ok ? rgLive.gapScoreV2 : null,
    certifiedCompetitors: er?.certifiedCompetitors?.length ?? null,
    displacementScenarioCount: disp?.displacementScenarioCount ?? null,
    bppPresence: kpis.portfolioAiPresence || null,
    bppRank: kpis.portfolioRank || null,
    bppNumberOne: kpis.numberOneAppearance || null,
    bppTop3: kpis.top3Appearance || null,
    coreObservations: core?.observations?.length || 0,
    subjectLoyaltyObservations: loyalty?.observations?.length || 0,
  };
  const drifts = [];
  for (const [k, expected] of Object.entries(golden)) {
    if (k === "peerLoyaltyObservationsTotal") continue;
    const actual = live[k];
    if (expected == null || actual == null) continue;
    if (String(expected) !== String(actual)) {
      drifts.push({ metric: k, expected, actual });
    }
  }
  return {
    ok: drifts.length === 0,
    baselineId: manifest.baselineId,
    propertyId,
    live,
    golden,
    drifts,
    paidCalls: 0,
    note: "Deterministic numerical parity only — PDF metadata timestamps may differ on rebuild.",
  };
}
