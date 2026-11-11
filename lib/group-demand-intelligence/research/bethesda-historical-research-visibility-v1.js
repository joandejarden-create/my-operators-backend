/**
 * Bethesda Historical Research Visibility V1
 *
 * Read-only, admin-facing projection of recovered historical attestations.
 * Never mutates opportunities.json. Never customer-facing.
 */

import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  BETHESDA_CANONICAL_HOTEL_ID,
  BETHESDA_CANONICAL_HOTEL_NAME,
} from "./bethesda-full-demand-database-reconciliation-v1.js";
import {
  RECOVERY_CATEGORY,
  buildBethesdaHistoricalRecoveryManifest,
} from "./bethesda-historical-opportunity-recovery-v1.js";
import {
  BETHESDA_29_ATTESTED_IDS,
  buildBethesdaHistoricalAttestationBag,
} from "./bethesda-historical-attestation-bag-v1.js";
import {
  BODY_STATUS,
  RECOVERY_DISPOSITION,
} from "./gdi-research-stage-persistence-v1.js";
import { loadStagedReconstructionBag } from "./bethesda-29-historical-reconstruction-v1.js";

export const BETHESDA_HISTORICAL_RESEARCH_VISIBILITY_V1 =
  "gdi-bethesda-historical-research-visibility-v1";

export const VISIBILITY_GROUP = Object.freeze({
  LOCAL_CATERING_EVENTS: "LOCAL_CATERING_EVENTS",
  POTENTIAL_PARTNERSHIPS: "POTENTIAL_PARTNERSHIPS",
  OTHER_GROUP_CORPORATE_ASSOCIATION: "OTHER_GROUP_CORPORATE_ASSOCIATION",
});

export const VISIBILITY_GROUP_LABEL = Object.freeze({
  [VISIBILITY_GROUP.LOCAL_CATERING_EVENTS]: "Local Catering / Events",
  [VISIBILITY_GROUP.POTENTIAL_PARTNERSHIPS]: "Potential Partnerships",
  [VISIBILITY_GROUP.OTHER_GROUP_CORPORATE_ASSOCIATION]:
    "Other Group / Corporate / Association",
});

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../../..");

const ABS_PATH_RE = /(?:[A-Za-z]:\\|\\\\|\/(?:Users|home|var|tmp|private)\b)/i;
const SECRETISH_RE =
  /(?:api[_-]?key|token|secret|password|authorization|bearer|memberstack|sk-|crsr_)/i;

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

function sha256File(absPath) {
  const buf = fs.readFileSync(absPath);
  return {
    bytes: buf.length,
    sha256: crypto.createHash("sha256").update(buf).digest("hex"),
  };
}

/** Safe provenance for browser — no absolute paths, secrets, or credentials. */
export function sanitizeProvenance({
  discoveryRun = null,
  discoveryDate = null,
  sourceArtifactPath = null,
  evidenceThatRecordExisted = [],
} = {}) {
  const artifactRel = text(sourceArtifactPath);
  let artifactLabel = null;
  if (artifactRel && !ABS_PATH_RE.test(artifactRel) && !SECRETISH_RE.test(artifactRel)) {
    const parts = artifactRel.split(/[\\/]/).filter(Boolean);
    artifactLabel = parts.slice(-2).join("/") || parts[0] || null;
  }
  const evidence = (Array.isArray(evidenceThatRecordExisted)
    ? evidenceThatRecordExisted
    : []
  )
    .map((e) => text(e))
    .filter(Boolean)
    .filter((e) => !ABS_PATH_RE.test(e) && !SECRETISH_RE.test(e))
    .map((e) => {
      if (e.startsWith("source:")) {
        const rest = e.slice("source:".length);
        const parts = rest.split(/[\\/]/).filter(Boolean);
        return `source:${parts.slice(-2).join("/") || parts[0] || "artifact"}`;
      }
      return e;
    })
    .slice(0, 8);

  return {
    discoveryRun: text(discoveryRun) || null,
    discoveryDate: text(discoveryDate) || null,
    artifactLabel,
    evidenceLabels: evidence,
  };
}

function visibilityGroupForCategory(researchCategory) {
  if (researchCategory === RECOVERY_CATEGORY.LOCAL_CATERING) {
    return VISIBILITY_GROUP.LOCAL_CATERING_EVENTS;
  }
  if (researchCategory === RECOVERY_CATEGORY.POTENTIAL_PARTNERSHIPS) {
    return VISIBILITY_GROUP.POTENTIAL_PARTNERSHIPS;
  }
  return VISIBILITY_GROUP.OTHER_GROUP_CORPORATE_ASSOCIATION;
}

function mapContinuationDisposition(row) {
  const raw = text(
    row.disposition ||
      row.classification ||
      row.originalCommercialQualification ||
      row.researchStatus ||
      ""
  ).toLowerCase();
  if (/venue|intelligence|venue_only|venue-only/.test(raw)) {
    return RECOVERY_DISPOSITION.VENUE_INTELLIGENCE;
  }
  return RECOVERY_DISPOSITION.RESEARCH_CONTINUATION;
}

function mapAttestationRow(rec, stagedById = new Map()) {
  const group = visibilityGroupForCategory(rec.researchCategory);
  const provenance = sanitizeProvenance({
    discoveryRun: rec.discoveryRun,
    discoveryDate: rec.discoveryDate,
    sourceArtifactPath: rec.sourceArtifactPath,
    evidenceThatRecordExisted: rec.evidenceThatRecordExisted,
  });
  const staged = stagedById.get(rec.id) || null;
  const bodyStatus =
    staged?.bodyStatus || BODY_STATUS.HISTORICAL_ATTESTATION_ONLY;
  const missingBody =
    staged?.missingOriginalResearchBody != null
      ? staged.missingOriginalResearchBody === true
      : true;
  return {
    id: rec.id,
    organizationName: rec.organizationName || null,
    title: rec.title || null,
    researchCategory: rec.researchCategory || null,
    visibilityGroup: group,
    visibilityGroupLabel: VISIBILITY_GROUP_LABEL[group],
    originalDisposition: rec.originalDisposition || null,
    historicalDisposition: rec.recoveryDisposition,
    bodyStatus,
    reconstructionStatus: staged?.reconstructionStatus || null,
    commercialMaturity: staged?.commercialMaturity || null,
    missingOriginalResearchBody: missingBody,
    furtherResearchRequired: true,
    sourceEvidenceAvailable: Boolean(
      rec.sourceEvidenceAvailable || staged?.sources?.length
    ),
    customerFacing: false,
    salesReady: false,
    publicationApproved: false,
    whatIsHappeningPreview: staged?.whatIsHappening
      ? String(staged.whatIsHappening).slice(0, 280)
      : null,
    provenance,
  };
}

function mapContinuationRow(row) {
  return {
    id: row.id,
    organizationName: row.organizationName || null,
    title: row.title || null,
    researchCategory: row.researchCategory || null,
    historicalDisposition: mapContinuationDisposition(row),
    bodyStatus: row.bodyAvailable
      ? BODY_STATUS.ORIGINAL_BODY_AVAILABLE
      : BODY_STATUS.HISTORICAL_ATTESTATION_ONLY,
    missingOriginalResearchBody: row.bodyAvailable !== true,
    furtherResearchRequired: true,
    customerFacing: false,
    salesReady: false,
    publicationApproved: false,
    provenance: sanitizeProvenance({
      discoveryRun: row.discoveryRun,
      discoveryDate: row.discoveryDate,
      sourceArtifactPath: row.sourceArtifactPath,
      evidenceThatRecordExisted: row.evidenceThatRecordExisted || [],
    }),
  };
}

/**
 * Build admin-safe visibility payload for Bethesda historical research.
 * Read-only. Does not write hotel store or share registries.
 */
export function buildBethesdaHistoricalResearchVisibility({ root = ROOT } = {}) {
  const bag = buildBethesdaHistoricalAttestationBag({ root });
  const manifest = buildBethesdaHistoricalRecoveryManifest({ root });
  const stagedDoc = loadStagedReconstructionBag({ root });
  const stagedById = new Map(
    (stagedDoc?.opportunities || []).map((o) => [o.id || o.opportunityId, o])
  );

  const attestationRows = (bag.records || []).map((rec) =>
    mapAttestationRow(rec, stagedById)
  );
  const byGroup = {
    [VISIBILITY_GROUP.LOCAL_CATERING_EVENTS]: attestationRows.filter(
      (r) => r.visibilityGroup === VISIBILITY_GROUP.LOCAL_CATERING_EVENTS
    ),
    [VISIBILITY_GROUP.POTENTIAL_PARTNERSHIPS]: attestationRows.filter(
      (r) => r.visibilityGroup === VISIBILITY_GROUP.POTENTIAL_PARTNERSHIPS
    ),
    [VISIBILITY_GROUP.OTHER_GROUP_CORPORATE_ASSOCIATION]: attestationRows.filter(
      (r) =>
        r.visibilityGroup === VISIBILITY_GROUP.OTHER_GROUP_CORPORATE_ASSOCIATION
    ),
  };

  const continuationSource = manifest.groups?.D_researchStageCandidates?.rows || [];
  const continuationRows = continuationSource
    .filter((r) => r && r.id)
    .map(mapContinuationRow);

  const oppPath = path.join(
    root,
    "data/group-demand-intelligence/hotels",
    BETHESDA_CANONICAL_HOTEL_ID,
    "opportunities.json"
  );
  const oppHash = fs.existsSync(oppPath) ? sha256File(oppPath) : null;
  let canonicalCount = manifest.baseline?.opportunityCount || 0;
  if (oppHash) {
    try {
      const raw = JSON.parse(fs.readFileSync(oppPath, "utf8"));
      const ops = Array.isArray(raw) ? raw : raw.opportunities || [];
      canonicalCount = ops.length;
    } catch {
      /* keep manifest baseline */
    }
  }

  return {
    version: BETHESDA_HISTORICAL_RESEARCH_VISIBILITY_V1,
    generatedAt: new Date().toISOString(),
    hotelId: BETHESDA_CANONICAL_HOTEL_ID,
    hotelName: BETHESDA_CANONICAL_HOTEL_NAME,
    counts: {
      canonicalOpportunities: canonicalCount,
      historicalAttestations: attestationRows.length,
      researchContinuationAvailable: continuationRows.length,
      localCateringEvents: byGroup[VISIBILITY_GROUP.LOCAL_CATERING_EVENTS].length,
      potentialPartnerships: byGroup[VISIBILITY_GROUP.POTENTIAL_PARTNERSHIPS].length,
      otherGroupCorporateAssociation:
        byGroup[VISIBILITY_GROUP.OTHER_GROUP_CORPORATE_ASSOCIATION].length,
      reconstructedBodiesAvailable: stagedById.size,
      reconstructedWithSourceEvidence: attestationRows.filter(
        (r) =>
          r.bodyStatus &&
          r.bodyStatus !== BODY_STATUS.HISTORICAL_ATTESTATION_ONLY
      ).length,
    },
    expected: {
      canonicalOpportunities: 59,
      historicalAttestations: 29,
      localCateringEvents: 10,
      potentialPartnerships: 8,
      otherGroupCorporateAssociation: 11,
      researchContinuationDeclared: 17,
    },
    labels: {
      historicalBodyStatus: BODY_STATUS.HISTORICAL_ATTESTATION_ONLY,
      dispositions: RECOVERY_DISPOSITION,
      groups: VISIBILITY_GROUP_LABEL,
    },
    safety: {
      customerFacing: false,
      salesReady: false,
      publicationApproved: false,
      opportunitiesJsonWrites: false,
      shareRegistryWrites: false,
      paidResearch: false,
      inventedBodies: false,
      internalAdminOnly: true,
    },
    opportunityStore: {
      pathRel: `data/group-demand-intelligence/hotels/${BETHESDA_CANONICAL_HOTEL_ID}/opportunities.json`,
      sha256: oppHash?.sha256 || null,
      bytes: oppHash?.bytes || null,
      untouched: true,
    },
    groups: {
      localCateringEvents: {
        key: VISIBILITY_GROUP.LOCAL_CATERING_EVENTS,
        label: VISIBILITY_GROUP_LABEL[VISIBILITY_GROUP.LOCAL_CATERING_EVENTS],
        count: byGroup[VISIBILITY_GROUP.LOCAL_CATERING_EVENTS].length,
        rows: byGroup[VISIBILITY_GROUP.LOCAL_CATERING_EVENTS],
      },
      potentialPartnerships: {
        key: VISIBILITY_GROUP.POTENTIAL_PARTNERSHIPS,
        label: VISIBILITY_GROUP_LABEL[VISIBILITY_GROUP.POTENTIAL_PARTNERSHIPS],
        count: byGroup[VISIBILITY_GROUP.POTENTIAL_PARTNERSHIPS].length,
        rows: byGroup[VISIBILITY_GROUP.POTENTIAL_PARTNERSHIPS],
      },
      otherGroupCorporateAssociation: {
        key: VISIBILITY_GROUP.OTHER_GROUP_CORPORATE_ASSOCIATION,
        label:
          VISIBILITY_GROUP_LABEL[
            VISIBILITY_GROUP.OTHER_GROUP_CORPORATE_ASSOCIATION
          ],
        count: byGroup[VISIBILITY_GROUP.OTHER_GROUP_CORPORATE_ASSOCIATION].length,
        rows: byGroup[VISIBILITY_GROUP.OTHER_GROUP_CORPORATE_ASSOCIATION],
      },
    },
    attestations: attestationRows,
    researchContinuation: {
      declaredCount: 17,
      availableCount: continuationRows.length,
      note:
        continuationRows.length < 17
          ? "Displaying only recoverable research-continuation rows; count not inflated."
          : "All declared research-continuation rows are available.",
      rows: continuationRows,
    },
    attestedIds: [...BETHESDA_29_ATTESTED_IDS],
  };
}

export function isBethesdaHistoricalVisibilityHotel(hotelId) {
  return text(hotelId) === BETHESDA_CANONICAL_HOTEL_ID;
}

export { BETHESDA_CANONICAL_HOTEL_ID, BETHESDA_CANONICAL_HOTEL_NAME };
