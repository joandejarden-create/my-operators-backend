/**
 * ADP_BPP_CANONICAL_EVIDENCE_SET_V1
 *
 * Universal Brand & Portfolio evidence contract for current + future hotels.
 * Doctrine:
 *   BPP_METRICS_REQUIRE_INSPECTABLE_EVIDENCE
 *   BPP_EVIDENCE_IS_A_CORE_PRODUCT_CAPABILITY_NOT_AN_OPTIONAL_PROPERTY_FEATURE
 *   EVERY_BPP_ANALYTICAL_STATE_MUST_TRACE_TO_CANONICAL_OBSERVATIONS
 *   BPP_EVIDENCE_USES_THE_SAME_TRUST_STANDARD_AS_CORE_ADP_EVIDENCE
 *   CURRENT_AND_FUTURE_PROPERTIES_USE_ONE_BPP_EVIDENCE CONTRACT
 *   ADP_BPP_EVIDENCE_PUBLISHED_SNAPSHOT_SELF_CONTAINED
 *   ADP_BPP_EVIDENCE_RAW_RESPONSE_REQUIRED
 *   ADP_BPP_EVIDENCE_COUNT_SUPPORT_EXACT_PARITY
 *   ADP_BPP_CORE_EVIDENCE_LENS_ISOLATION
 *   ADP_BPP_METRICS_AND_EVIDENCE_ATOMIC_PUBLICATION
 *   BPP_MISSING_EVIDENCE_TRUE_SUBJECT_ABSENCE_ONLY
 *   BPP_DISPLACEMENT_CLAIM_CANONICAL_SUPPORT
 *
 * METHODOLOGY_IS_GOVERNED — packaging / publishability only; no new provider calls.
 */

import { detectPropertyMention, normalizeSubjectHaystack, findTokenBoundaryIndex } from "../execution/response-parser.js";
import { hashResponse } from "../measurement-assurance/prompt-persistence-v1.js";

export const ADP_BPP_CANONICAL_EVIDENCE_SET_V1 = "ADP_BPP_CANONICAL_EVIDENCE_SET_V1";
export const ADP_BPP_EVIDENCE_PUBLICATION_GATE = "ADP_BPP_EVIDENCE_PUBLICATION_GATE";
export const ADP_BPP_EVIDENCE_RAW_RESPONSE_REQUIRED = "ADP_BPP_EVIDENCE_RAW_RESPONSE_REQUIRED";
export const ADP_BPP_EVIDENCE_COUNT_SUPPORT_EXACT_PARITY =
  "ADP_BPP_EVIDENCE_COUNT_SUPPORT_EXACT_PARITY";
export const ADP_BPP_EVIDENCE_PUBLISHED_SNAPSHOT_SELF_CONTAINED =
  "ADP_BPP_EVIDENCE_PUBLISHED_SNAPSHOT_SELF_CONTAINED";
export const ADP_BPP_METRICS_AND_EVIDENCE_ATOMIC_PUBLICATION =
  "ADP_BPP_METRICS_AND_EVIDENCE_ATOMIC_PUBLICATION";
export const ADP_BPP_CORE_EVIDENCE_LENS_ISOLATION = "ADP_BPP_CORE_EVIDENCE_LENS_ISOLATION";
export const BPP_MISSING_EVIDENCE_TRUE_SUBJECT_ABSENCE_ONLY =
  "BPP_MISSING_EVIDENCE_TRUE_SUBJECT_ABSENCE_ONLY";
export const BPP_DISPLACEMENT_CLAIM_CANONICAL_SUPPORT = "BPP_DISPLACEMENT_CLAIM_CANONICAL_SUPPORT";
export const BPP_METRICS_REQUIRE_INSPECTABLE_EVIDENCE = "BPP_METRICS_REQUIRE_INSPECTABLE_EVIDENCE";

export const BPP_EVIDENCE_LENS = "BRAND_PORTFOLIO";
export const BPP_EVIDENCE_TYPE = Object.freeze({
  POSITIVE: "BPP_POSITIVE",
  MISSING: "BPP_MISSING",
  DISPLACEMENT: "BPP_DISPLACEMENT",
});

const DEFAULT_LIMIT = 40;

function peerMentioned(response, peerHotel) {
  if (!response || !peerHotel) return false;
  const text = normalizeSubjectHaystack(response);
  const variants = [
    peerHotel,
    peerHotel.replace(/,.*$/, "").trim(),
    peerHotel.replace(/\s+(Curio Collection|A Tapestry Collection).*$/i, "").trim(),
    peerHotel.replace(/\s+by Hilton.*$/i, "").trim(),
    peerHotel.replace(/\s+Hotel$/i, "").trim(),
  ].filter((v) => v && v.length >= 5);
  for (const v of variants) {
    const needle = normalizeSubjectHaystack(v);
    if (needle.length < 5) continue;
    if (findTokenBoundaryIndex(text, needle) !== -1) return true;
  }
  return false;
}

function comparableObs(obs) {
  return obs && !obs.error && obs.rawResponse && String(obs.rawResponse).trim() && obs.metricInclusion !== false;
}

function responseText(obsOrEv) {
  const raw =
    obsOrEv?.rawResponse ||
    obsOrEv?.aiResponse ||
    obsOrEv?.exactResponse ||
    obsOrEv?.excerpt ||
    obsOrEv?.responseExcerpt ||
    "";
  return String(raw || "");
}

function withResponseAliases(text, extra = {}) {
  const body = String(text || "");
  return {
    ...extra,
    aiResponse: body,
    exactResponse: body,
    rawResponse: body,
    responseHash: extra.responseHash || hashResponse(body),
  };
}

/**
 * Normalize any historical evidence item to the canonical customer-safe shape.
 */
export function normalizeBppEvidenceItem(item, evidenceType) {
  if (!item || typeof item !== "object") return null;
  const body = responseText(item).trim();
  if (!body) return null;
  const type = evidenceType || item.evidenceType || BPP_EVIDENCE_TYPE.POSITIVE;
  return withResponseAliases(body, {
    evidenceType: type,
    lens: BPP_EVIDENCE_LENS,
    propertyId: item.propertyId || null,
    periodId: item.periodId || item.measurementPeriodId || null,
    observationId: item.observationId || null,
    scenarioId: item.scenarioId || null,
    territoryId: item.territoryId || item.territory || null,
    territory: item.territory || item.territoryId || null,
    provider: item.provider || item.sampleProvider || null,
    sampleProvider: item.sampleProvider || item.provider || null,
    subjectCanonicalHotelId: item.subjectCanonicalHotelId || null,
    subjectMentioned:
      type === BPP_EVIDENCE_TYPE.POSITIVE
        ? true
        : type === BPP_EVIDENCE_TYPE.MISSING || type === BPP_EVIDENCE_TYPE.DISPLACEMENT
          ? false
          : item.subjectMentioned ?? null,
    subjectRank: item.subjectRank ?? item.position ?? null,
    matchedVariant: item.matchedVariant || null,
    context: item.context || null,
    competitorCanonicalHotelId:
      item.competitorCanonicalHotelId || item.competitorEntityId || null,
    competitorName: item.competitorName || null,
    competitorRank: item.competitorRank ?? null,
    peersPresent: Array.isArray(item.peersPresent) ? item.peersPresent : undefined,
    scenarioLabel: item.scenarioLabel || null,
    measurementPeriodId: item.measurementPeriodId || item.periodId || null,
  });
}

export function buildPositiveEvidencePackV1({
  observations,
  profile,
  periodId = null,
  limit = DEFAULT_LIMIT,
} = {}) {
  const hits = [];
  for (const obs of observations || []) {
    if (!comparableObs(obs)) continue;
    const m = detectPropertyMention(obs.rawResponse, profile);
    if (!m.mentioned) continue;
    hits.push(
      withResponseAliases(obs.rawResponse, {
        evidenceType: BPP_EVIDENCE_TYPE.POSITIVE,
        lens: BPP_EVIDENCE_LENS,
        propertyId: profile?.propertyId || null,
        periodId,
        measurementPeriodId: periodId,
        observationId: obs.observationId,
        scenarioId: obs.scenarioId,
        territoryId: obs.territory || null,
        territory: obs.territory || null,
        provider: obs.provider,
        sampleProvider: obs.provider,
        subjectCanonicalHotelId: profile?.propertyId || null,
        subjectMentioned: true,
        subjectRank: m.position ?? null,
        matchedVariant: m.matchedVariant || null,
        context: m.context || null,
        promptHash: obs.promptHash || null,
        responseHash: obs.responseHash || hashResponse(obs.rawResponse),
      })
    );
    if (hits.length >= limit) break;
  }
  return hits;
}

/**
 * True subject absence only — provider failures already excluded by comparableObs.
 */
export function buildMissingEvidencePackV1({
  observations,
  profile,
  periodId = null,
  limit = DEFAULT_LIMIT,
} = {}) {
  const misses = [];
  for (const obs of observations || []) {
    if (!comparableObs(obs)) continue;
    const m = detectPropertyMention(obs.rawResponse, profile);
    if (m.mentioned) continue;
    misses.push(
      withResponseAliases(obs.rawResponse, {
        evidenceType: BPP_EVIDENCE_TYPE.MISSING,
        lens: BPP_EVIDENCE_LENS,
        propertyId: profile?.propertyId || null,
        periodId,
        measurementPeriodId: periodId,
        observationId: obs.observationId,
        scenarioId: obs.scenarioId,
        territoryId: obs.territory || null,
        territory: obs.territory || null,
        provider: obs.provider,
        sampleProvider: obs.provider,
        subjectCanonicalHotelId: profile?.propertyId || null,
        subjectMentioned: false,
        subjectRank: null,
        promptHash: obs.promptHash || null,
        responseHash: obs.responseHash || hashResponse(obs.rawResponse),
        gate: BPP_MISSING_EVIDENCE_TRUE_SUBJECT_ABSENCE_ONLY,
      })
    );
    if (misses.length >= limit) break;
  }
  return misses;
}

/**
 * Subject absent + at least one portfolio peer present on the same observation.
 */
export function buildDisplacementEvidencePackV1({
  observations,
  profile,
  peerSet,
  scenarioRows = null,
  periodId = null,
  limit = DEFAULT_LIMIT,
} = {}) {
  const peerList = peerSet?.included || [];
  const displacedScenarioIds = new Set(
    (scenarioRows || []).filter((r) => r.displacement).map((r) => r.scenarioId)
  );
  const items = [];
  const seen = new Set();

  for (const obs of observations || []) {
    if (!comparableObs(obs)) continue;
    if (displacedScenarioIds.size && !displacedScenarioIds.has(obs.scenarioId)) continue;
    if (detectPropertyMention(obs.rawResponse, profile).mentioned) continue;

    const displacing = [];
    for (const peer of peerList) {
      if (peerMentioned(obs.rawResponse, peer.peerHotel)) {
        displacing.push(peer);
      }
    }
    if (!displacing.length) continue;

    if (scenarioRows && scenarioRows.length && !displacedScenarioIds.has(obs.scenarioId)) continue;

    const key = `${obs.observationId}::${displacing[0].canonicalEntityId}`;
    if (seen.has(key)) continue;
    seen.add(key);

    const top = displacing[0];
    items.push(
      withResponseAliases(obs.rawResponse, {
        evidenceType: BPP_EVIDENCE_TYPE.DISPLACEMENT,
        lens: BPP_EVIDENCE_LENS,
        propertyId: profile?.propertyId || null,
        periodId,
        measurementPeriodId: periodId,
        observationId: obs.observationId,
        scenarioId: obs.scenarioId,
        territoryId: obs.territory || null,
        territory: obs.territory || null,
        provider: obs.provider,
        sampleProvider: obs.provider,
        subjectCanonicalHotelId: profile?.propertyId || null,
        subjectMentioned: false,
        competitorCanonicalHotelId: top.canonicalEntityId,
        competitorEntityId: top.canonicalEntityId,
        competitorName: top.peerHotel || top.name || null,
        peersPresent: displacing.map((p) => p.canonicalEntityId),
        promptHash: obs.promptHash || null,
        responseHash: obs.responseHash || hashResponse(obs.rawResponse),
        gate: BPP_DISPLACEMENT_CLAIM_CANONICAL_SUPPORT,
      })
    );
    if (items.length >= limit) break;
  }
  return items;
}

/**
 * Build the full published evidence object for a BPP payload.
 */
export function buildAdpBppCanonicalEvidenceSetV1({
  propertyId,
  periodId,
  bppEditionId = null,
  observations,
  profile,
  peerSet,
  scenarioRows = null,
  limit = DEFAULT_LIMIT,
} = {}) {
  const positive = buildPositiveEvidencePackV1({
    observations,
    profile,
    periodId,
    limit,
  });
  const missing = buildMissingEvidencePackV1({
    observations,
    profile,
    periodId,
    limit,
  });
  const displacement = buildDisplacementEvidencePackV1({
    observations,
    profile,
    peerSet,
    scenarioRows,
    periodId,
    limit,
  });

  return {
    schema: ADP_BPP_CANONICAL_EVIDENCE_SET_V1,
    propertyId: propertyId || profile?.propertyId || null,
    periodId,
    bppEditionId,
    lens: BPP_EVIDENCE_LENS,
    positive,
    missing,
    displacement,
    measurementPeriodId: periodId,
    counts: {
      positive: positive.length,
      missing: missing.length,
      displacement: displacement.length,
    },
    gates: {
      [ADP_BPP_EVIDENCE_RAW_RESPONSE_REQUIRED]: [...positive, ...missing, ...displacement].every(
        (e) => String(e.aiResponse || "").trim().length > 0
      ),
      [ADP_BPP_EVIDENCE_COUNT_SUPPORT_EXACT_PARITY]: true,
      [ADP_BPP_CORE_EVIDENCE_LENS_ISOLATION]: [...positive, ...missing, ...displacement].every(
        (e) => e.lens === BPP_EVIDENCE_LENS
      ),
      [BPP_MISSING_EVIDENCE_TRUE_SUBJECT_ABSENCE_ONLY]: true,
      [BPP_DISPLACEMENT_CLAIM_CANONICAL_SUPPORT]: displacement.every(
        (e) => e.competitorCanonicalHotelId && e.observationId
      ),
    },
  };
}

/**
 * Attach / replace evidence on an existing READY BPP payload without changing metrics.
 */
export function attachCanonicalEvidenceToBppPayload(payload, evidenceSet) {
  if (!payload || typeof payload !== "object") return payload;
  return {
    ...payload,
    evidence: {
      positive: evidenceSet.positive,
      missing: evidenceSet.missing,
      displacement: evidenceSet.displacement,
      measurementPeriodId: evidenceSet.periodId || evidenceSet.measurementPeriodId || null,
      schema: ADP_BPP_CANONICAL_EVIDENCE_SET_V1,
      counts: evidenceSet.counts,
      lens: BPP_EVIDENCE_LENS,
    },
  };
}

/**
 * Normalize an existing evidence object (aliases only) when period rebuild is unavailable.
 */
export function normalizeExistingBppEvidenceObject(evidence) {
  if (!evidence || typeof evidence !== "object") {
    return { positive: [], missing: [], displacement: [], measurementPeriodId: null };
  }
  const positive = (evidence.positive || [])
    .map((e) => normalizeBppEvidenceItem(e, BPP_EVIDENCE_TYPE.POSITIVE))
    .filter(Boolean);
  const missing = (evidence.missing || [])
    .map((e) => normalizeBppEvidenceItem(e, BPP_EVIDENCE_TYPE.MISSING))
    .filter(Boolean);
  const displacement = (evidence.displacement || [])
    .map((e) => normalizeBppEvidenceItem(e, BPP_EVIDENCE_TYPE.DISPLACEMENT))
    .filter(Boolean);
  return {
    ...evidence,
    positive,
    missing,
    displacement,
    schema: ADP_BPP_CANONICAL_EVIDENCE_SET_V1,
    lens: BPP_EVIDENCE_LENS,
    counts: {
      positive: positive.length,
      missing: missing.length,
      displacement: displacement.length,
    },
  };
}

/**
 * Publication gate for populated BPP.
 */
export function auditBppEvidencePublicationGate(payload) {
  const state = payload?.bppCustomerState || null;
  const ready =
    payload?.status === "READY" ||
    state === "BPP_READY_POPULATED_FULL" ||
    state === "BPP_READY_POPULATED_RANK_ONLY";
  const suppressed =
    payload?.status === "EXCEPTION_SUPPRESSED" ||
    state === "BPP_EXCEPTION_SUPPRESSED" ||
    state === "BPP_NOT_APPLICABLE";

  if (!ready || suppressed) {
    return {
      gate: ADP_BPP_EVIDENCE_PUBLICATION_GATE,
      applicable: false,
      pass: true,
      reason: suppressed ? "bpp_suppressed" : "bpp_not_ready",
    };
  }

  const ev = payload.evidence || {};
  const pos = ev.positive || [];
  const miss = ev.missing || [];
  const disp = ev.displacement || [];
  const defects = [];

  if (!payload.evidence) defects.push("missing_evidence_object");

  const checkItems = (arr, label) => {
    for (let i = 0; i < arr.length; i++) {
      const item = arr[i];
      const body = responseText(item).trim();
      if (!body) defects.push(`${label}[${i}]_blank_response`);
      if (item.lens && item.lens !== BPP_EVIDENCE_LENS && item.lens !== "bpp") {
        defects.push(`${label}[${i}]_lens_leak`);
      }
    }
  };
  checkItems(pos, "positive");
  checkItems(miss, "missing");
  checkItems(disp, "displacement");

  for (let i = 0; i < disp.length; i++) {
    if (!disp[i].observationId && !responseText(disp[i]).trim()) {
      defects.push(`displacement[${i}]_metadata_only`);
    }
  }

  const counts = ev.counts || {
    positive: pos.length,
    missing: miss.length,
    displacement: disp.length,
  };
  if (counts.positive !== pos.length) defects.push("positive_count_mismatch");
  if (counts.missing !== miss.length) defects.push("missing_count_mismatch");
  if (counts.displacement !== disp.length) defects.push("displacement_count_mismatch");

  return {
    gate: ADP_BPP_EVIDENCE_PUBLICATION_GATE,
    applicable: true,
    pass: defects.length === 0,
    defects,
    counts: {
      positive: pos.length,
      missing: miss.length,
      displacement: disp.length,
    },
    atomicPublication: ADP_BPP_METRICS_AND_EVIDENCE_ATOMIC_PUBLICATION,
  };
}

export function resolveBppEvidenceResponseText(item) {
  return responseText(item);
}
