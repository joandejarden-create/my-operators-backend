/**
 * GDI V37 — versioned discovery-candidate ledger schema.
 * Hotel-generic. Never stores raw provider prompts/credentials.
 */
import { createHash } from "node:crypto";
import { RESEARCH_STATUS } from "./research-status-transitions-v1.js";
import { buildDisposition } from "./disposition-reason-codes-v1.js";

export const DISCOVERY_CANDIDATE_LEDGER_V1 =
  "gdi-discovery-candidate-ledger-v1";

export const LEDGER_DOCUMENT_VERSION =
  "gdi-discovery-candidate-ledger-document-v1";

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

export function normalizeOrgKey(name) {
  return text(name)
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
}

export function normalizeProgrammeKey(name) {
  return normalizeOrgKey(name);
}

export function mintDiscoveryId({
  hotelId = "",
  workflowId = "",
  organizationName = "",
  programmeName = "",
  sourceUrl = "",
  pass = "",
  exaRunId = "",
} = {}) {
  const raw = [
    hotelId,
    workflowId,
    normalizeOrgKey(organizationName),
    normalizeProgrammeKey(programmeName),
    text(sourceUrl).toLowerCase(),
    String(pass || ""),
    text(exaRunId),
  ].join("|");
  const hash = createHash("sha256").update(raw).digest("hex").slice(0, 16);
  return `gdi_disc_${hash}`;
}

export function mintDedupeFamilyKey({
  hotelId = "",
  workflowId = "",
  organizationName = "",
  programmeName = "",
} = {}) {
  return [
    hotelId,
    workflowId,
    normalizeOrgKey(organizationName),
    normalizeProgrammeKey(programmeName),
  ].join("|");
}

/** Strip fields that must never reach customer-facing surfaces. */
export function sanitizeStructuredCandidate(candidate = {}) {
  const out = { ...candidate };
  delete out.raw;
  delete out.providerPrompt;
  delete out.systemPrompt;
  delete out.instructions;
  delete out.apiKey;
  delete out.authorization;
  delete out.grounding;
  delete out.providerText;
  delete out.text;
  return out;
}

export function emptyLedgerDocument({
  hotelId = null,
  hotelKey = null,
  hotelName = null,
  researchCampaignId = null,
} = {}) {
  return {
    version: LEDGER_DOCUMENT_VERSION,
    schema: DISCOVERY_CANDIDATE_LEDGER_V1,
    hotelId,
    hotelKey,
    hotelName,
    researchCampaignId,
    generatedAt: new Date().toISOString(),
    candidates: [],
  };
}

/**
 * Create a new ledger row from a discovery pass candidate.
 */
export function createDiscoveryLedgerRecord({
  hotelId,
  hotelKey = null,
  hotelName = null,
  workflowId,
  powershell = null,
  segmentId = null,
  researchRunId = null,
  exaRunId = null,
  queryText = null,
  queryLanguage = null,
  queryTimestamp = null,
  pass = null,
  candidate = {},
  discoveryCostUsd = null,
  artifactPath = null,
  provenanceAvailable = true,
  provenanceGaps = [],
} = {}) {
  const organizationName = text(
    candidate.organizationName || candidate.organisationName || candidate.org,
  );
  const programmeName = text(
    candidate.programmeName || candidate.programName || candidate.eventName,
  );
  const evidenceUrls = []
    .concat(candidate.evidenceUrls || [])
    .concat(candidate.sourceUrls || [])
    .map(text)
    .filter(Boolean);
  const primaryUrl = evidenceUrls[0] || text(candidate.sourceUrl) || "";
  const discoveryId = mintDiscoveryId({
    hotelId,
    workflowId,
    organizationName,
    programmeName,
    sourceUrl: primaryUrl,
    pass,
    exaRunId,
  });
  const at = queryTimestamp || new Date().toISOString();
  const structured = sanitizeStructuredCandidate(candidate);

  return {
    version: DISCOVERY_CANDIDATE_LEDGER_V1,
    discoveryId,
    hotelId,
    hotelKey,
    hotelName,
    researchRunId: researchRunId || null,
    workflowId,
    powershell: powershell || candidate.powershell || null,
    segmentId: segmentId || workflowId || null,
    queryText: queryText || null,
    queryLanguage: queryLanguage || null,
    queryTimestamp: at,
    exaRunId: exaRunId || null,
    discoveryPass: pass == null ? null : Number(pass),
    originalSourceUrls: evidenceUrls,
    originalResultTitle: text(candidate.title || programmeName) || null,
    originalResultExcerpt:
      text(
        candidate.demandMechanismSummary ||
          candidate.summary ||
          candidate.excerpt ||
          "",
      ) || null,
    structuredExaCandidateFields: structured,
    normalizedOrganization: normalizeOrgKey(organizationName) || null,
    normalizedProgramme: normalizeProgrammeKey(programmeName) || null,
    organizationName: organizationName || null,
    programmeName: programmeName || null,
    opportunityId: null,
    dedupeFamilyKey: mintDedupeFamilyKey({
      hotelId,
      workflowId,
      organizationName,
      programmeName,
    }),
    researchStatus: RESEARCH_STATUS.DISCOVERED_UNSCREENED,
    statusHistory: [
      {
        from: null,
        to: RESEARCH_STATUS.DISCOVERED_UNSCREENED,
        stage: "discovery_persist",
        reasonCode: null,
        note: "Ingested from Exa discovery artifact",
        at,
      },
    ],
    disposition: null,
    evidenceSupportingDecision: [],
    researchCostUsd: discoveryCostUsd == null ? null : Number(discoveryCostUsd),
    deepResearchCostUsd: null,
    lastEvaluatedAt: at,
    deepArtifactPath: null,
    staged: false,
    customerFacing: false,
    awaitingFounderPromotion: false,
    provenance: {
      artifactPath: artifactPath || null,
      available: provenanceAvailable !== false,
      gaps: Array.isArray(provenanceGaps) ? provenanceGaps : [],
      rawProviderEnvelopePersisted: false,
      note:
        provenanceAvailable === false
          ? "Historical Exa provider envelope not retained in artifact"
          : "Structured candidate fields persisted; raw Agent text/grounding not stored in four-hotel discovery writes",
    },
  };
}

export function applyDispositionToRecord(record, dispositionInput, status) {
  const disposition = buildDisposition(dispositionInput);
  let next = { ...record, disposition, lastEvaluatedAt: disposition.decidedAt };
  if (status) {
    const history = Array.isArray(next.statusHistory) ? [...next.statusHistory] : [];
    history.push({
      from: next.researchStatus,
      to: status,
      stage: disposition.stage,
      reasonCode: disposition.code,
      note: disposition.label,
      at: disposition.decidedAt,
    });
    next = {
      ...next,
      researchStatus: status,
      statusHistory: history,
    };
  }
  return next;
}

export function validateLedgerRecord(record = {}) {
  const errors = [];
  if (!record.discoveryId) errors.push("discoveryId_required");
  if (!record.hotelId) errors.push("hotelId_required");
  if (!record.workflowId) errors.push("workflowId_required");
  if (!record.researchStatus) errors.push("researchStatus_required");
  if (
    record.researchStatus === RESEARCH_STATUS.REJECTED &&
    !record.disposition?.code
  ) {
    errors.push("rejected_requires_disposition_code");
  }
  return { ok: errors.length === 0, errors };
}
