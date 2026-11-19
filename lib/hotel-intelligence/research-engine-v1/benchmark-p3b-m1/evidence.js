/**
 * Evidence URL validation + score only genuinely new accepted intelligence.
 */

import { CLAIM_ACCEPTANCE, EVIDENCE_LAYER } from "../benchmark-p3a/contract.js";
import { buildClaimEnvelope, CLAIM_TYPES } from "../benchmark-p3a/claim-schema.js";
import { scoreClaims } from "../benchmark-p3a/quality-rubric.js";

const SEED_CLAIM_TYPES = new Set([CLAIM_TYPES.PROPERTY_IDENTITY]);

export function isSafeHttpUrl(url) {
  try {
    const u = new URL(String(url || ""));
    return u.protocol === "http:" || u.protocol === "https:";
  } catch {
    return false;
  }
}

export function validateSourceUrl(url) {
  if (!url) return { ok: false, reason: "MISSING_URL" };
  if (!isSafeHttpUrl(url)) return { ok: false, reason: "INVALID_URL" };
  const host = new URL(url).hostname.toLowerCase();
  if (host === "localhost" || host.endsWith(".invalid")) {
    return { ok: true, reason: "FIXTURE_OR_MOCK_URL", mock: true };
  }
  return { ok: true, reason: "OK" };
}

/**
 * Normalize provider facts → claim envelopes; exclude seed identity from scored new intel.
 */
export function normalizeAndFilterNewIntelligence(facts = [], opts = {}) {
  const seedNames = new Set(
    (opts.seedPropertyNames || []).map((s) => String(s).toLowerCase().trim())
  );
  const acceptedNew = [];
  const unknowns = [];
  const incorrectOrRejected = [];
  const unresolvedOwnership = [];
  const all = [];

  for (const raw of facts) {
    const urlCheck = validateSourceUrl(raw.url || raw.supportingSource?.url);
    const inferred = raw.inferredContact === true;
    const envelope = buildClaimEnvelope({
      claimType: raw.claimType,
      value: raw.value,
      text: raw.text,
      supportingSource: {
        url: raw.url || raw.supportingSource?.url || null,
        publisherClass: raw.publisherClass || null,
        publishedAt: raw.publishedAt || null,
        excerpt: raw.excerpt || null,
      },
      independentVerification: raw.independentVerification || {
        ok: Boolean(urlCheck.ok && !urlCheck.mock),
        method: "SOURCE_URL",
        note: urlCheck.reason,
      },
      inferredContact: inferred,
      acceptanceDecision: inferred ? CLAIM_ACCEPTANCE.REJECTED : undefined,
      graphMapping: { stagedOnly: true, productionWriteForbidden: true },
    });
    // Belt-and-suspenders for inferred contacts
    if (inferred) {
      envelope[EVIDENCE_LAYER.ACCEPTANCE_DECISION] = CLAIM_ACCEPTANCE.REJECTED;
      envelope.inferredContact = true;
    }

    const claimType = envelope[EVIDENCE_LAYER.DISCOVERED_CLAIM]?.claimType;
    const value = String(envelope[EVIDENCE_LAYER.DISCOVERED_CLAIM]?.value || "")
      .toLowerCase()
      .trim();
    const isSeed =
      raw.isSeedFact === true ||
      SEED_CLAIM_TYPES.has(claimType) ||
      seedNames.has(value);

    envelope.isSeedFact = isSeed;
    envelope.sourceValidation = urlCheck;
    all.push(envelope);

    const decision = envelope[EVIDENCE_LAYER.ACCEPTANCE_DECISION];
    if (decision === CLAIM_ACCEPTANCE.UNKNOWN) unknowns.push(envelope);
    if (decision === CLAIM_ACCEPTANCE.REJECTED) incorrectOrRejected.push(envelope);
    if (
      (claimType === CLAIM_TYPES.OWNER_ENTITY ||
        claimType === CLAIM_TYPES.ECONOMIC_SPONSOR ||
        claimType === CLAIM_TYPES.OWNER_VS_OPERATOR) &&
      decision !== CLAIM_ACCEPTANCE.ACCEPTED
    ) {
      unresolvedOwnership.push(envelope);
    }
    if (decision === CLAIM_ACCEPTANCE.ACCEPTED && !isSeed) {
      acceptedNew.push(envelope);
    }
  }

  const score = scoreClaims(acceptedNew);
  return {
    all,
    acceptedNew,
    unknowns,
    incorrectOrRejected,
    unresolvedOwnership,
    score,
    scoredOnlyNewAccepted: true,
  };
}

export const PRIORITY_OBJECTIVES = Object.freeze([
  "LEGAL_OR_ECONOMIC_OWNER",
  "OPERATING_OR_MANAGEMENT_COMPANY",
  "PARENT_AND_PORTFOLIO_RELATIONSHIPS",
  "VERIFIED_NAMED_OWNERSHIP_MANAGEMENT_EXECUTIVES",
  "COMMERCIALLY_RELEVANT_DECISION_MAKERS",
  "PUBLIC_EMAIL_AND_TELEPHONE",
]);
