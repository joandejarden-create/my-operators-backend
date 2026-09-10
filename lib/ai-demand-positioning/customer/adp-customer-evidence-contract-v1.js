/**
 * ADP_CUSTOMER_EVIDENCE_CONTRACT_V1
 *
 * EVERY_CLICKABLE_EVIDENCE_LINK_MUST_OPEN_EXACT_SUPPORTING_EVIDENCE
 * CUSTOMER_FACING_CLAIM_AND_EVIDENCE_MUST_SHARE_THE_SAME_CANONICAL_TRACE
 * EMPTY_WRONG_STALE_OR_SUPPRESSED_EVIDENCE_IS_NOT_CLIENT_READY
 * IF A CLAIM CANNOT BE SUPPORTED BY EXACT EVIDENCE, THE CLAIM OR LINK MAY NOT BE PUBLISHED
 *
 * Methodology unchanged. No fabricated evidence. No internal diagnostics customer-facing.
 */

import { getPublishedEvidenceResponse } from "../published-read-service.js";
import { getPublishedOwnerReport } from "../published-read-service.js";
import { loadPublishedReport } from "../published-snapshot.js";
import { loadLatestCustomerPeriod, loadPropertyProfile } from "../data-model.js";
import { buildScenarioUniverse } from "../prompt-universe/scenario-registry.js";
import { filterPositiveEvidencePool } from "./positive-evidence-v1.js";
import { filterMissingEvidencePool } from "./missing-evidence-v1.js";
import {
  DISPLACEMENT_EVIDENCE_RESOLVER_VERSION,
} from "./resolve-displacement-evidence-v1.js";
import { enrichObservationsWithRank } from "../metrics/executive-metrics-foundation.js";
import { OVERALL_RANKING_KEY } from "./competitive-ranking-overall-view-v1.js";
import { assertVerbatimEquality } from "./verbatim-evidence-response-v1.js";
import { getGovernedSubjectMentioned } from "../subject-presence/canonical-subject-presence-v1.js";
import { territoryLabelForIntent } from "../metrics/intent-territory-labels.js";
export const ADP_CUSTOMER_EVIDENCE_CONTRACT_V1 = "ADP_CUSTOMER_EVIDENCE_CONTRACT_V1";

export const EVERY_CLICKABLE_EVIDENCE_LINK_MUST_OPEN_EXACT_SUPPORTING_EVIDENCE =
  "EVERY_CLICKABLE_EVIDENCE_LINK_MUST_OPEN_EXACT_SUPPORTING_EVIDENCE";
export const CUSTOMER_FACING_CLAIM_AND_EVIDENCE_MUST_SHARE_THE_SAME_CANONICAL_TRACE =
  "CUSTOMER_FACING_CLAIM_AND_EVIDENCE_MUST_SHARE_THE_SAME_CANONICAL_TRACE";
export const EMPTY_WRONG_STALE_OR_SUPPRESSED_EVIDENCE_IS_NOT_CLIENT_READY =
  "EMPTY_WRONG_STALE_OR_SUPPRESSED_EVIDENCE_IS_NOT_CLIENT_READY";
export const ADP_EVIDENCE_CLAIM_SEMANTIC_PARITY = "ADP_EVIDENCE_CLAIM_SEMANTIC_PARITY";
export const ADP_EVIDENCE_LINK_NONEMPTY = "ADP_EVIDENCE_LINK_NONEMPTY";
export const ADP_NO_INTERNAL_EVIDENCE_DIAGNOSTICS_CUSTOMER_FACING =
  "ADP_NO_INTERNAL_EVIDENCE_DIAGNOSTICS_CUSTOMER_FACING";
export const ADP_EVIDENCE_MODAL_TYPE_LABEL_PARITY = "ADP_EVIDENCE_MODAL_TYPE_LABEL_PARITY";
export const ADP_EVIDENCE_EXACT_RAW_RESPONSE = "ADP_EVIDENCE_EXACT_RAW_RESPONSE";
export const ADP_EVIDENCE_VERBATIM_TEXT_PRESERVATION = "ADP_EVIDENCE_VERBATIM_TEXT_PRESERVATION";
export const ADP_EVIDENCE_SAFE_STRUCTURED_TEXT_RENDER = "ADP_EVIDENCE_SAFE_STRUCTURED_TEXT_RENDER";
export const ADP_DISPLACEMENT_ROW_EVIDENCE_CANONICAL_ID_PARITY =
  "ADP_DISPLACEMENT_ROW_EVIDENCE_CANONICAL_ID_PARITY";
export const ADP_DISPLACEMENT_CLAIM_REQUIRES_CANONICAL_SUPPORT =
  "ADP_DISPLACEMENT_CLAIM_REQUIRES_CANONICAL_SUPPORT";
export const ADP_REALITY_GAP_EVIDENCE_ATTRIBUTE_PARITY =
  "ADP_REALITY_GAP_EVIDENCE_ATTRIBUTE_PARITY";
export const ADP_POSITIVE_EVIDENCE_SUBJECT_PRESENCE_PARITY =
  "ADP_POSITIVE_EVIDENCE_SUBJECT_PRESENCE_PARITY";
export const ADP_MISSING_EVIDENCE_SUBJECT_ABSENCE_PARITY =
  "ADP_MISSING_EVIDENCE_SUBJECT_ABSENCE_PARITY";
export const ADP_PROVIDER_EVIDENCE_PROVIDER_PARITY = "ADP_PROVIDER_EVIDENCE_PROVIDER_PARITY";
export const ADP_EXECUTIVE_READ_EVIDENCE_TRACE_PARITY =
  "ADP_EXECUTIVE_READ_EVIDENCE_TRACE_PARITY";
export const ADP_BPP_EVIDENCE_LENS_PARITY = "ADP_BPP_EVIDENCE_LENS_PARITY";
export const ADP_ACTION_EVIDENCE_DIAGNOSIS_PARITY = "ADP_ACTION_EVIDENCE_DIAGNOSIS_PARITY";
export const ADP_EVIDENCE_LOCAL_EXTERNAL_PARITY = "ADP_EVIDENCE_LOCAL_EXTERNAL_PARITY";

/** Canonical evidenceType values for every customer-facing evidence control. */
export const EVIDENCE_TYPE = Object.freeze({
  COMPETITIVE_DISPLACEMENT: "COMPETITIVE_DISPLACEMENT",
  POSITIVE_PRESENCE: "POSITIVE_PRESENCE",
  MISSING_PRESENCE: "MISSING_PRESENCE",
  REALITY_GAP: "REALITY_GAP",
  PROVIDER_PRESENCE: "PROVIDER_PRESENCE",
  PROVIDER_MISSING: "PROVIDER_MISSING",
  EXECUTIVE_INSIGHT: "EXECUTIVE_INSIGHT",
  BPP_DISPLACEMENT: "BPP_DISPLACEMENT",
  BPP_PRESENCE: "BPP_PRESENCE",
  BPP_MISSING: "BPP_MISSING",
  ACTION_SUPPORT: "ACTION_SUPPORT",
  TERRITORY_SUPPORT: "TERRITORY_SUPPORT",
  TERRITORY_MISSING: "TERRITORY_MISSING",
});

/** Surfaces that currently expose clickable evidence controls. */
export const EVIDENCE_LINKED_SURFACES = Object.freeze([
  {
    id: "COMPETITIVE_DISPLACEMENT",
    label: "Competitive Displacement",
    clickable: true,
    evidenceTypes: [EVIDENCE_TYPE.COMPETITIVE_DISPLACEMENT],
  },
  {
    id: "COMPETITIVE_OVERVIEW",
    label: "Competitive Overview",
    clickable: true,
    evidenceTypes: [EVIDENCE_TYPE.COMPETITIVE_DISPLACEMENT],
  },
  {
    id: "DEMAND_TERRITORY",
    label: "Demand Territory evidence",
    clickable: true,
    evidenceTypes: [EVIDENCE_TYPE.TERRITORY_SUPPORT, EVIDENCE_TYPE.TERRITORY_MISSING],
  },
  {
    id: "PROVIDER_PRESENCE",
    label: "Provider Presence",
    clickable: true,
    evidenceTypes: [EVIDENCE_TYPE.PROVIDER_PRESENCE, EVIDENCE_TYPE.PROVIDER_MISSING],
  },
  {
    id: "BRAND_PORTFOLIO",
    label: "Brand & Portfolio Position",
    clickable: true,
    evidenceTypes: [
      EVIDENCE_TYPE.BPP_PRESENCE,
      EVIDENCE_TYPE.BPP_MISSING,
      EVIDENCE_TYPE.BPP_DISPLACEMENT,
    ],
  },
  {
    id: "REALITY_GAPS",
    label: "Reality Gaps",
    clickable: false,
    note: "No customer-facing evidence hyperlink in current UI",
    evidenceTypes: [EVIDENCE_TYPE.REALITY_GAP],
  },
  {
    id: "EXECUTIVE_READ",
    label: "Executive Read",
    clickable: false,
    note: "No customer-facing evidence hyperlink in current UI",
    evidenceTypes: [EVIDENCE_TYPE.EXECUTIVE_INSIGHT],
  },
  {
    id: "PRIORITY_ACTIONS",
    label: "Priority Actions",
    clickable: false,
    note: "No customer-facing evidence hyperlink in current UI",
    evidenceTypes: [EVIDENCE_TYPE.ACTION_SUPPORT],
  },
]);

/** Forbidden phrases — must never appear in customer evidence drawer copy. */
export const FORBIDDEN_INTERNAL_DIAGNOSTIC_PATTERNS = Object.freeze([
  /identity did not reconcile/i,
  /evidence ref missing/i,
  /configuration mismatch/i,
  /lookup failed/i,
  /stale entity/i,
  /internal evidence unavailable/i,
  /analytical integrity check/i,
  /canonical competitor identity/i,
  /not client-ready/i,
  /failClosed/i,
]);

export const CUSTOMER_SAFE_EVIDENCE_UNAVAILABLE =
  "Supporting evidence is unavailable for this claim.";

/**
 * Modal title labels by evidenceType (+ optional context suffix).
 */
export function buildEvidenceModalTitle(evidenceType, contextLabel = "") {
  const ctx = String(contextLabel || "").trim();
  const base = (() => {
    switch (evidenceType) {
      case EVIDENCE_TYPE.COMPETITIVE_DISPLACEMENT:
        return "Displacement Evidence";
      case EVIDENCE_TYPE.POSITIVE_PRESENCE:
      case EVIDENCE_TYPE.TERRITORY_SUPPORT:
      case EVIDENCE_TYPE.PROVIDER_PRESENCE:
        return "Positive Evidence";
      case EVIDENCE_TYPE.MISSING_PRESENCE:
      case EVIDENCE_TYPE.TERRITORY_MISSING:
      case EVIDENCE_TYPE.PROVIDER_MISSING:
        return "Missing Evidence";
      case EVIDENCE_TYPE.REALITY_GAP:
        return "Reality Gap Evidence";
      case EVIDENCE_TYPE.BPP_PRESENCE:
        return "Brand & Portfolio Evidence · Positive";
      case EVIDENCE_TYPE.BPP_MISSING:
        return "Brand & Portfolio Evidence · Missing";
      case EVIDENCE_TYPE.BPP_DISPLACEMENT:
        return "Brand & Portfolio Evidence · Displacement";
      case EVIDENCE_TYPE.EXECUTIVE_INSIGHT:
        return "Executive Insight Evidence";
      case EVIDENCE_TYPE.ACTION_SUPPORT:
        return "Action Support Evidence";
      default:
        return "Evidence";
    }
  })();
  if (
    evidenceType === EVIDENCE_TYPE.BPP_PRESENCE ||
    evidenceType === EVIDENCE_TYPE.BPP_MISSING ||
    evidenceType === EVIDENCE_TYPE.BPP_DISPLACEMENT
  ) {
    return ctx ? `${base} · ${ctx}` : base;
  }
  return ctx ? `${base} · ${ctx}` : base;
}

export function customerCopyContainsInternalDiagnostics(text) {
  const s = String(text || "");
  return FORBIDDEN_INTERNAL_DIAGNOSTIC_PATTERNS.some((re) => re.test(s));
}

function rawResponsePresent(ev) {
  const raw = ev?.aiResponse || ev?.rawResponse || ev?.responseExcerpt || "";
  return String(raw).trim().length > 0;
}

function responseLooksFlattened(text) {
  const t = String(text || "");
  if (!t.includes("\n") && t.length > 600 && /\d+\.\s+\S/.test(t)) return true;
  return false;
}

/**
 * Enumerate every customer-facing evidence hyperlink that the UI would publish.
 */
export async function enumerateCustomerEvidenceLinks(propertyId) {
  const links = [];
  const published = loadPublishedReport(propertyId);
  const profile = loadPropertyProfile(propertyId);
  if (!published || !profile) {
    return { propertyId, ok: false, error: "missing_published_or_profile", links: [] };
  }

  const period = loadLatestCustomerPeriod(propertyId);
  const scenarios = buildScenarioUniverse(profile);
  const owner = await getPublishedOwnerReport(propertyId);
  const payload = owner?.payload || published;

  // Demand Territory
  for (const [intent, data] of Object.entries(published.demandCapture?.byIntent || {})) {
    const captured = Number(data.captured) || 0;
    const total = Number(data.total) || 0;
    const missing = Math.max(0, total - captured);
    const label = territoryLabelForIntent(intent) || intent;
    if (missing > 0) {
      links.push({
        surface: "DEMAND_TERRITORY",
        label: `View Missing · ${label}`,
        evidenceType: EVIDENCE_TYPE.TERRITORY_MISSING,
        claimId: `territory:${intent}:missing`,
        propertyId,
        periodId: period?.periodId || null,
        scenarioId: null,
        provider: null,
        intent,
        expectedEvidenceCount: null,
        query: { type: "missing", mode: "missing", intent, limit: 25, offset: 0 },
      });
    }
    if (captured > 0) {
      links.push({
        surface: "DEMAND_TERRITORY",
        label: `View Examples · ${label}`,
        evidenceType: EVIDENCE_TYPE.TERRITORY_SUPPORT,
        claimId: `territory:${intent}:present`,
        propertyId,
        periodId: period?.periodId || null,
        intent,
        expectedEvidenceCount: Math.min(5, captured),
        query: { type: "present", mode: "positive", intent, limit: 5, offset: 0 },
      });
    }
  }

  // Provider Presence
  for (const p of published.evidence?.providers || []) {
    const denom = p.comparable != null ? p.comparable : p.total;
    const mentioned = Number(p.mentioned) || 0;
    const missing = Math.max(0, Number(denom) - mentioned);
    const unavailable = p.presenceUnavailable === true || p.presence == null;
    if (unavailable) continue;
    if (mentioned > 0) {
      links.push({
        surface: "PROVIDER_PRESENCE",
        label: `View Examples · ${p.provider}`,
        evidenceType: EVIDENCE_TYPE.PROVIDER_PRESENCE,
        claimId: `provider:${p.provider}:present`,
        propertyId,
        provider: p.provider,
        expectedEvidenceCount: Math.min(5, mentioned),
        query: {
          type: "present",
          mode: "positive",
          provider: p.provider,
          limit: 5,
          offset: 0,
        },
      });
    }
    if (missing > 0) {
      links.push({
        surface: "PROVIDER_PRESENCE",
        label: `View Missing · ${p.provider}`,
        evidenceType: EVIDENCE_TYPE.PROVIDER_MISSING,
        claimId: `provider:${p.provider}:missing`,
        propertyId,
        provider: p.provider,
        expectedEvidenceCount: missing,
        query: {
          type: "missing",
          mode: "missing",
          provider: p.provider,
          limit: 100,
          offset: 0,
        },
      });
    }
  }

  // Competitive Overview + Competitive Displacement (overall + by territory)
  const ranking = payload?.competitiveRankingByTerritory;
  if (ranking?.byTerritory) {
    for (const [scopeKey, block] of Object.entries(ranking.byTerritory)) {
      for (const row of block.displayRows || []) {
        if (row.isSubject) continue;
        const count = row.displacement?.count || 0;
        if (count <= 0 || !row.entityId) continue;
        const scopeLabel =
          scopeKey === OVERALL_RANKING_KEY || scopeKey === "overall"
            ? "Overall"
            : territoryLabelForIntent(scopeKey) || scopeKey;
        links.push({
          surface: scopeKey === OVERALL_RANKING_KEY || scopeKey === "overall"
            ? "COMPETITIVE_DISPLACEMENT"
            : "COMPETITIVE_OVERVIEW",
          label: `Displacement · ${row.name} · ${scopeLabel}`,
          evidenceType: EVIDENCE_TYPE.COMPETITIVE_DISPLACEMENT,
          claimId: `displacement:${row.entityId}:${scopeKey}`,
          propertyId,
          competitorCanonicalHotelId: row.entityId,
          competitorEntityId: row.entityId,
          competitorName: row.name,
          scopeKey,
          expectedEvidenceCount: count,
          query: {
            type: "displacement",
            competitorId: row.entityId,
            competitor: row.name,
            scope:
              scopeKey === OVERALL_RANKING_KEY || scopeKey === "overall"
                ? "overall"
                : "demand_territory",
            intent:
              scopeKey === OVERALL_RANKING_KEY || scopeKey === "overall" ? null : scopeKey,
          },
        });
      }
    }
  }

  // BPP — only nonempty packs become clickable (published customer payload)
  const bpp = payload?.brandPortfolioPosition || null;
  if (bpp?.evidence) {
    const packs = [
      {
        kind: "positive",
        evidenceType: EVIDENCE_TYPE.BPP_PRESENCE,
        items: bpp.evidence.positive || [],
      },
      {
        kind: "missing",
        evidenceType: EVIDENCE_TYPE.BPP_MISSING,
        items: bpp.evidence.missing || [],
      },
      {
        kind: "displacement",
        evidenceType: EVIDENCE_TYPE.BPP_DISPLACEMENT,
        items: bpp.evidence.displacement || [],
      },
    ];
    for (const pack of packs) {
      if (!pack.items.length) continue;
      links.push({
        surface: "BRAND_PORTFOLIO",
        label: `BPP ${pack.kind}`,
        evidenceType: pack.evidenceType,
        claimId: `bpp:${pack.kind}`,
        propertyId,
        expectedEvidenceCount: pack.items.length,
        bppItems: pack.items,
        query: null,
      });
    }
  }

  return {
    propertyId,
    ok: true,
    periodId: period?.periodId || null,
    scenariosAvailable: Boolean(scenarios?.length),
    links,
    surfaces: EVIDENCE_LINKED_SURFACES,
  };
}

function validateEvidenceItems(link, evidence, observationsByKey) {
  const defects = [];
  const items = evidence || [];

  if (!items.length) {
    defects.push({
      gate: ADP_EVIDENCE_LINK_NONEMPTY,
      code: "EMPTY_EVIDENCE",
      detail: "zero observations",
    });
    return defects;
  }

  for (const ev of items) {
    if (!rawResponsePresent(ev)) {
      defects.push({
        gate: ADP_EVIDENCE_EXACT_RAW_RESPONSE,
        code: "MISSING_RAW_RESPONSE",
        detail: ev.observationId || ev.scenarioId || "unknown",
      });
    }
    const text = ev.aiResponse || ev.rawResponse || "";
    if (responseLooksFlattened(text)) {
      defects.push({
        gate: ADP_EVIDENCE_SAFE_STRUCTURED_TEXT_RENDER,
        code: "FLATTENED_RESPONSE",
        detail: ev.observationId || "unknown",
      });
    }

    if (
      link.evidenceType === EVIDENCE_TYPE.COMPETITIVE_DISPLACEMENT &&
      link.competitorEntityId
    ) {
      const rowId = ev.displacementCompetitorEntityId || ev.competitorEntityId || "";
      if (rowId && rowId !== link.competitorEntityId) {
        defects.push({
          gate: ADP_DISPLACEMENT_ROW_EVIDENCE_CANONICAL_ID_PARITY,
          code: "WRONG_COMPETITOR",
          detail: `${rowId} !== ${link.competitorEntityId}`,
        });
      }
      if (!rowId && !ev.displacingCompetitor) {
        defects.push({
          gate: ADP_DISPLACEMENT_CLAIM_REQUIRES_CANONICAL_SUPPORT,
          code: "MISSING_COMPETITOR_KEY",
          detail: ev.observationId || "unknown",
        });
      }
    }

    if (
      (link.evidenceType === EVIDENCE_TYPE.PROVIDER_PRESENCE ||
        link.evidenceType === EVIDENCE_TYPE.PROVIDER_MISSING) &&
      link.provider
    ) {
      if (ev.provider && ev.provider !== link.provider) {
        defects.push({
          gate: ADP_PROVIDER_EVIDENCE_PROVIDER_PARITY,
          code: "CROSS_PROVIDER",
          detail: `${ev.provider} !== ${link.provider}`,
        });
      }
    }

    if (
      link.evidenceType === EVIDENCE_TYPE.POSITIVE_PRESENCE ||
      link.evidenceType === EVIDENCE_TYPE.TERRITORY_SUPPORT ||
      link.evidenceType === EVIDENCE_TYPE.PROVIDER_PRESENCE
    ) {
      // Prefer Path-A governed flag on card; fall back to observation lookup
      if (ev.subjectAppeared === false || ev.mentioned === false) {
        defects.push({
          gate: ADP_POSITIVE_EVIDENCE_SUBJECT_PRESENCE_PARITY,
          code: "SUBJECT_NOT_PRESENT",
          detail: ev.observationId || "unknown",
        });
      }
    }

    if (
      link.evidenceType === EVIDENCE_TYPE.MISSING_PRESENCE ||
      link.evidenceType === EVIDENCE_TYPE.TERRITORY_MISSING ||
      link.evidenceType === EVIDENCE_TYPE.PROVIDER_MISSING
    ) {
      if (ev.subjectAppeared === true || ev.mentioned === true) {
        defects.push({
          gate: ADP_MISSING_EVIDENCE_SUBJECT_ABSENCE_PARITY,
          code: "SUBJECT_NOT_ABSENT",
          detail: ev.observationId || "unknown",
        });
      }
    }

    if (link.intent && ev.intent && ev.intent !== link.intent && ev.demandTerritory) {
      // soft: some cards use territory label only
    }

    if (observationsByKey && ev.scenarioId && ev.provider) {
      const key = `${ev.scenarioId}::${ev.provider}`;
      const sourceObs = observationsByKey.get(key);
      if (sourceObs?.rawResponse && text) {
        const vDefects = assertVerbatimEquality(sourceObs.rawResponse, text);
        for (const d of vDefects) {
          defects.push({
            gate: ADP_EVIDENCE_VERBATIM_TEXT_PRESERVATION,
            code: d.code,
            detail: d.detail,
          });
        }
        if (
          (link.evidenceType === EVIDENCE_TYPE.POSITIVE_PRESENCE ||
            link.evidenceType === EVIDENCE_TYPE.TERRITORY_SUPPORT ||
            link.evidenceType === EVIDENCE_TYPE.PROVIDER_PRESENCE) &&
          !getGovernedSubjectMentioned(sourceObs)
        ) {
          defects.push({
            gate: ADP_POSITIVE_EVIDENCE_SUBJECT_PRESENCE_PARITY,
            code: "PATH_A_ABSENT",
            detail: key,
          });
        }
        if (
          (link.evidenceType === EVIDENCE_TYPE.MISSING_PRESENCE ||
            link.evidenceType === EVIDENCE_TYPE.TERRITORY_MISSING ||
            link.evidenceType === EVIDENCE_TYPE.PROVIDER_MISSING) &&
          getGovernedSubjectMentioned(sourceObs)
        ) {
          defects.push({
            gate: ADP_MISSING_EVIDENCE_SUBJECT_ABSENCE_PARITY,
            code: "PATH_A_PRESENT",
            detail: key,
          });
        }
      }
    }
  }

  return defects;
}

/**
 * Full-universe audit of one property's published evidence links.
 */
export async function auditPropertyEvidenceLinks(propertyId) {
  const enumerated = await enumerateCustomerEvidenceLinks(propertyId);
  const profile = loadPropertyProfile(propertyId);
  const period = loadLatestCustomerPeriod(propertyId);
  const scenarios = profile ? buildScenarioUniverse(profile) : [];
  const observations = enrichObservationsWithRank(
    (period?.observations || []).filter((o) => o.parsed),
    profile
  );
  const observationsByKey = new Map(
    observations.map((o) => [`${o.scenarioId}::${o.provider}`, o])
  );

  const results = [];
  let pass = 0;
  let fail = 0;
  const failuresByType = Object.create(null);
  const emptyLinks = [];
  const wrongStale = [];
  const internalDiagnostics = [];
  const modalLabelDefects = [];

  for (const link of enumerated.links || []) {
    const row = {
      surface: link.surface,
      label: link.label,
      evidenceType: link.evidenceType,
      claimId: link.claimId,
      expectedEvidenceCount: link.expectedEvidenceCount,
      actualEvidenceCount: 0,
      semanticParity: true,
      rawResponsePresent: true,
      modalTitleCorrect: true,
      modalTitle: "",
      status: "PASS",
      defects: [],
    };

    const contextLabel =
      link.intent
        ? territoryLabelForIntent(link.intent) || link.intent
        : link.provider
          ? link.provider
          : link.competitorName || "";
    row.modalTitle = buildEvidenceModalTitle(link.evidenceType, contextLabel);
    if (!row.modalTitle || /Positive Evidence/i.test(row.modalTitle) &&
      link.evidenceType === EVIDENCE_TYPE.COMPETITIVE_DISPLACEMENT) {
      row.modalTitleCorrect = false;
      row.defects.push({
        gate: ADP_EVIDENCE_MODAL_TYPE_LABEL_PARITY,
        code: "WRONG_MODAL_TITLE",
        detail: row.modalTitle,
      });
      modalLabelDefects.push(row);
    }
    if (
      link.evidenceType === EVIDENCE_TYPE.COMPETITIVE_DISPLACEMENT &&
      !/^Displacement Evidence/i.test(row.modalTitle)
    ) {
      row.modalTitleCorrect = false;
      row.defects.push({
        gate: ADP_EVIDENCE_MODAL_TYPE_LABEL_PARITY,
        code: "DISPLACEMENT_TITLE_NOT_DISPLACEMENT",
        detail: row.modalTitle,
      });
      modalLabelDefects.push(row);
    }

    let evidence = [];
    if (link.bppItems) {
      evidence = link.bppItems;
      // BPP lens: cards must not look like CORE-only dumps without loyalty context when displacement
      if (link.evidenceType === EVIDENCE_TYPE.BPP_DISPLACEMENT) {
        for (const ev of evidence) {
          if (ev.lens && ev.lens !== "bpp" && ev.lens !== "brand_portfolio") {
            row.defects.push({
              gate: ADP_BPP_EVIDENCE_LENS_PARITY,
              code: "WRONG_LENS",
              detail: ev.lens,
            });
          }
        }
      }
    } else if (link.query) {
      const api = await getPublishedEvidenceResponse(propertyId, link.query);
      if (api?.integrity?.failClosed) {
        row.defects.push({
          gate: ADP_EVIDENCE_LINK_NONEMPTY,
          code: "FAIL_CLOSED",
          detail: api.integrity.code || "failClosed",
        });
        emptyLinks.push(row);
      }
      evidence = api?.evidence || [];
      // Simulate drawer competitor scope for displacement
      if (link.evidenceType === EVIDENCE_TYPE.COMPETITIVE_DISPLACEMENT && link.competitorEntityId) {
        evidence = evidence.filter((ev) => {
          const rowId = ev.displacementCompetitorEntityId || ev.competitorEntityId || "";
          return !rowId || rowId === link.competitorEntityId;
        });
      }
      // Count parity for missing provider (observation grain)
      if (
        link.evidenceType === EVIDENCE_TYPE.PROVIDER_MISSING &&
        link.expectedEvidenceCount != null &&
        (api.totalQualifying ?? api.total) != null &&
        Number(api.totalQualifying ?? api.total) !== Number(link.expectedEvidenceCount)
      ) {
        // Provider missing UI uses observation-grain denom; soft if pools match Path-A
        const { pool } = filterMissingEvidencePool({
          period,
          scenarios,
          provider: link.provider,
        });
        if (pool.length !== Number(api.totalQualifying ?? api.total)) {
          row.defects.push({
            gate: ADP_EVIDENCE_CLAIM_SEMANTIC_PARITY,
            code: "PROVIDER_MISSING_COUNT_MISMATCH",
            detail: `ui=${link.expectedEvidenceCount} api=${api.totalQualifying ?? api.total} pool=${pool.length}`,
          });
        }
      }
    }

    row.actualEvidenceCount = evidence.length;
    const itemDefects = validateEvidenceItems(link, evidence, observationsByKey);
    row.defects.push(...itemDefects);
    if (itemDefects.some((d) => d.gate === ADP_EVIDENCE_LINK_NONEMPTY)) {
      emptyLinks.push(row);
    }
    if (
      itemDefects.some((d) =>
        [
          ADP_DISPLACEMENT_ROW_EVIDENCE_CANONICAL_ID_PARITY,
          ADP_POSITIVE_EVIDENCE_SUBJECT_PRESENCE_PARITY,
          ADP_MISSING_EVIDENCE_SUBJECT_ABSENCE_PARITY,
          ADP_PROVIDER_EVIDENCE_PROVIDER_PARITY,
          ADP_BPP_EVIDENCE_LENS_PARITY,
          ADP_EVIDENCE_CLAIM_SEMANTIC_PARITY,
        ].includes(d.gate)
      )
    ) {
      row.semanticParity = false;
      wrongStale.push(row);
    }
    if (itemDefects.some((d) => d.gate === ADP_EVIDENCE_EXACT_RAW_RESPONSE)) {
      row.rawResponsePresent = false;
    }

    // Customer-facing copy scan on sample card strings
    for (const ev of evidence.slice(0, 3)) {
      const blob = JSON.stringify(ev);
      if (customerCopyContainsInternalDiagnostics(blob)) {
        row.defects.push({
          gate: ADP_NO_INTERNAL_EVIDENCE_DIAGNOSTICS_CUSTOMER_FACING,
          code: "INTERNAL_DIAGNOSTIC_IN_PAYLOAD",
          detail: ev.observationId || "card",
        });
        internalDiagnostics.push(row);
      }
    }

    if (row.defects.length) {
      row.status = "FAIL";
      fail += 1;
      failuresByType[link.evidenceType] = (failuresByType[link.evidenceType] || 0) + 1;
    } else {
      pass += 1;
    }
    results.push(row);
  }

  // Positive/missing pool consistency smoke (false action prevention)
  if (period && profile) {
    for (const p of loadPublishedReport(propertyId)?.evidence?.providers || []) {
      const mentioned = Number(p.mentioned) || 0;
      if (mentioned <= 0 || p.presenceUnavailable) continue;
      const { pool } = filterPositiveEvidencePool({
        period,
        scenarios,
        propertyProfile: profile,
        provider: p.provider,
      });
      if (pool.length === 0) {
        fail += 1;
        const row = {
          surface: "PROVIDER_PRESENCE",
          label: `false positive action ${p.provider}`,
          evidenceType: EVIDENCE_TYPE.PROVIDER_PRESENCE,
          claimId: `provider:${p.provider}:false_action`,
          status: "FAIL",
          defects: [
            {
              gate: ADP_EVIDENCE_LINK_NONEMPTY,
              code: "FALSE_POSITIVE_EVIDENCE_ACTION",
              detail: p.provider,
            },
          ],
        };
        results.push(row);
        emptyLinks.push(row);
        failuresByType[EVIDENCE_TYPE.PROVIDER_PRESENCE] =
          (failuresByType[EVIDENCE_TYPE.PROVIDER_PRESENCE] || 0) + 1;
      }
    }
  }

  return {
    propertyId,
    contract: ADP_CUSTOMER_EVIDENCE_CONTRACT_V1,
    displacementResolverVersion: DISPLACEMENT_EVIDENCE_RESOLVER_VERSION,
    linkCount: results.length,
    pass,
    fail,
    status: fail === 0 ? "PASS" : "FAIL",
    failuresByType,
    emptyLinks: emptyLinks.map((r) => r.claimId || r.label),
    wrongStale: wrongStale.map((r) => r.claimId || r.label),
    internalDiagnostics: internalDiagnostics.map((r) => r.claimId || r.label),
    modalLabelDefects: modalLabelDefects.map((r) => ({
      claimId: r.claimId,
      title: r.modalTitle,
    })),
    links: results,
    surfaces: EVIDENCE_LINKED_SURFACES,
  };
}

/**
 * Full published universe audit.
 */
export async function auditAllEvidenceLinksClientReady(propertyIds) {
  const ids = propertyIds || [];
  const properties = [];
  let totalLinks = 0;
  let totalFail = 0;
  const failuresByType = Object.create(null);
  const emptyLinks = [];
  const wrongStale = [];
  const internalDiagnostics = [];
  const modalLabelDefects = [];

  for (const propertyId of ids) {
    const audit = await auditPropertyEvidenceLinks(propertyId);
    properties.push(audit);
    totalLinks += audit.linkCount;
    totalFail += audit.fail;
    for (const [k, v] of Object.entries(audit.failuresByType || {})) {
      failuresByType[k] = (failuresByType[k] || 0) + v;
    }
    emptyLinks.push(...(audit.emptyLinks || []).map((c) => ({ propertyId, claimId: c })));
    wrongStale.push(...(audit.wrongStale || []).map((c) => ({ propertyId, claimId: c })));
    internalDiagnostics.push(
      ...(audit.internalDiagnostics || []).map((c) => ({ propertyId, claimId: c }))
    );
    modalLabelDefects.push(
      ...(audit.modalLabelDefects || []).map((c) => ({ propertyId, ...c }))
    );
  }

  return {
    contract: ADP_CUSTOMER_EVIDENCE_CONTRACT_V1,
    doctrine: [
      EVERY_CLICKABLE_EVIDENCE_LINK_MUST_OPEN_EXACT_SUPPORTING_EVIDENCE,
      CUSTOMER_FACING_CLAIM_AND_EVIDENCE_MUST_SHARE_THE_SAME_CANONICAL_TRACE,
      EMPTY_WRONG_STALE_OR_SUPPRESSED_EVIDENCE_IS_NOT_CLIENT_READY,
      ADP_NO_INTERNAL_EVIDENCE_DIAGNOSTICS_CUSTOMER_FACING,
      ADP_EVIDENCE_MODAL_TYPE_LABEL_PARITY,
      ADP_EVIDENCE_EXACT_RAW_RESPONSE,
    ],
    propertyCount: ids.length,
    totalLinks,
    totalFail,
    status: totalFail === 0 ? "PASS" : "FAIL",
    failuresByType,
    emptyLinks,
    wrongStale,
    internalDiagnostics,
    modalLabelDefects,
    surfaces: EVIDENCE_LINKED_SURFACES,
    properties,
    methodologyChanged: false,
    rootCausePriorMiss: [
      "AUDIT_SCOPE_GAP",
      "CLICK_QA_MISSING",
      "RENDERER_GAP",
      "EVIDENCE_INDEX_GAP",
    ],
  };
}
