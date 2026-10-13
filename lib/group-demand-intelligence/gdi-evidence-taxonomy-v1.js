/**
 * GDI structured evidence taxonomy V1.
 * Extends claim typing for maturity / QUALIFIED honesty without breaking
 * existing CLAIM_KIND consumers (FACT / INFERENCE / ESTIMATED).
 */

import { CLAIM_KIND } from "./claim-types.js";

/** Structured evidence types for account ↔ demand-signal linkage and hypotheses. */
export const GDI_EVIDENCE_TYPE = Object.freeze({
  CONFIRMED_EVENT: "CONFIRMED_EVENT",
  CONFIRMED_SPONSOR: "CONFIRMED_SPONSOR",
  CONFIRMED_EXHIBITOR: "CONFIRMED_EXHIBITOR",
  CONFIRMED_PARTICIPATION: "CONFIRMED_PARTICIPATION",
  CONFIRMED_DELEGATION: "CONFIRMED_DELEGATION",
  CONFIRMED_VENDOR: "CONFIRMED_VENDOR",
  CONFIRMED_SPEAKER: "CONFIRMED_SPEAKER",
  CONFIRMED_TEAM: "CONFIRMED_TEAM",
  CONFIRMED_PCO: "CONFIRMED_PCO",
  CONFIRMED_CONTRACT_AWARD: "CONFIRMED_CONTRACT_AWARD",
  CONFIRMED_RECURRING_PATTERN: "CONFIRMED_RECURRING_PATTERN",
  CONFIRMED_TRAVELING_COHORT: "CONFIRMED_TRAVELING_COHORT",
  CONFIRMED_LODGING_CONTROL: "CONFIRMED_LODGING_CONTROL",
  CONFIRMED_HOTEL_RFP: "CONFIRMED_HOTEL_RFP",
  INFERRED_TRAVEL: "INFERRED_TRAVEL",
  INFERRED_ROOM_BAND: "INFERRED_ROOM_BAND",
  INFERRED_LODGING_CONTROL: "INFERRED_LODGING_CONTROL",
});

const CONFIRMED_TYPES = new Set(
  Object.values(GDI_EVIDENCE_TYPE).filter((t) => String(t).startsWith("CONFIRMED_"))
);

/** Maturity-panel claim kinds (may include MODELED; maps to legacy CLAIM_KIND). */
export const GDI_MATURITY_CLAIM_KIND = Object.freeze({
  FACT: "FACT",
  INFERRED: "INFERRED",
  MODELED: "MODELED",
});

export const GDI_CONFIDENCE = Object.freeze({
  HIGH: "HIGH",
  MEDIUM: "MEDIUM",
  LOW: "LOW",
  UNKNOWN: "UNKNOWN",
});

/**
 * Map maturity claimKind → legacy CLAIM_KIND for existing consumers.
 */
export function toLegacyClaimKind(claimKind = "") {
  const k = String(claimKind || "").toUpperCase();
  if (k === "FACT" || k === "VERIFIED") return CLAIM_KIND.FACT;
  if (k === "INFERRED" || k === "INFERENCE") return CLAIM_KIND.INFERENCE;
  if (k === "MODELED" || k === "ESTIMATED") return CLAIM_KIND.ESTIMATED;
  return CLAIM_KIND.UNKNOWN;
}

export function isConfirmedEvidenceType(type = "") {
  return CONFIRMED_TYPES.has(String(type || "").toUpperCase());
}

/**
 * Normalize a single evidence item without inventing facts.
 */
export function normalizeEvidenceItem(raw = {}) {
  if (!raw || typeof raw !== "object") return null;
  const evidenceType = String(raw.evidenceType || raw.type || "").toUpperCase() || null;
  const claimKindRaw = String(raw.claimKind || "").toUpperCase();
  let claimKind = claimKindRaw;
  if (claimKind === "INFERENCE") claimKind = GDI_MATURITY_CLAIM_KIND.INFERRED;
  if (claimKind === "ESTIMATED") claimKind = GDI_MATURITY_CLAIM_KIND.MODELED;
  if (claimKind === "VERIFIED") claimKind = GDI_MATURITY_CLAIM_KIND.FACT;
  if (
    claimKind &&
    !Object.values(GDI_MATURITY_CLAIM_KIND).includes(claimKind)
  ) {
    claimKind = GDI_MATURITY_CLAIM_KIND.INFERRED;
  }
  if (!claimKind && isConfirmedEvidenceType(evidenceType)) {
    claimKind = GDI_MATURITY_CLAIM_KIND.FACT;
  }
  return {
    claimKind: claimKind || null,
    legacyClaimKind: toLegacyClaimKind(claimKind),
    sourceUrl: raw.sourceUrl || raw.url || null,
    evidenceType,
    excerpt: raw.excerpt || raw.note || null,
    confidence: String(raw.confidence || GDI_CONFIDENCE.UNKNOWN).toUpperCase(),
  };
}

export function listEvidenceItems(opp = {}) {
  const rows = [];
  if (Array.isArray(opp.evidenceItems)) {
    for (const e of opp.evidenceItems) {
      const n = normalizeEvidenceItem(e);
      if (n) rows.push(n);
    }
  }
  if (Array.isArray(opp.evidence)) {
    for (const e of opp.evidence) {
      const n = normalizeEvidenceItem(e);
      if (n) rows.push(n);
    }
  }
  return rows;
}

export function hasConfirmedAccountSignalEvidence(opp = {}) {
  const items = listEvidenceItems(opp);
  if (
    items.some(
      (e) =>
        e.claimKind === GDI_MATURITY_CLAIM_KIND.FACT &&
        (isConfirmedEvidenceType(e.evidenceType) || Boolean(e.sourceUrl))
    )
  ) {
    return true;
  }
  // Legacy production rows: officialSource / discoverySource + named org
  const url = opp.officialSource || opp.discoverySource;
  if (url && /^https?:\/\//i.test(String(url))) {
    const org = String(opp.organizationName || opp.company || "").trim();
    if (org.length >= 3) return true;
  }
  return false;
}
