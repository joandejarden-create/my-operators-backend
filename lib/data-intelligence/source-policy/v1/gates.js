/**
 * Source-policy gates — single choke point for persist / score / display.
 */

import {
  SourceRole,
  SourceContentDomain,
  VerificationStatus,
  SOURCE_POLICY_VERSION,
  CVENT_VENUE_BLOCKED_FIELDS,
} from "./source-roles.js";
import {
  classifySourceContentDomain,
  defaultRoleForDomain,
  normalizeObservation,
} from "./domain-classifier.js";

function asObs(input) {
  if (!input || typeof input !== "object") {
    return normalizeObservation({ url: String(input || "") });
  }
  return normalizeObservation(input);
}

function isHotelPropertyField(field) {
  if (!field) return true; // fail closed for unspecified hotel facts from discovery-only
  const f = String(field);
  if (CVENT_VENUE_BLOCKED_FIELDS.includes(f)) return true;
  return /rooms|keys|suite|meeting|ballroom|capacity|description|overview|listing|renovat|built|airport|address|phone|brand/i.test(
    f
  );
}

/**
 * May this observation establish a canonical hotel/property field value?
 */
export function canPersistAsCanonical(observation) {
  const obs = asObs(observation);
  const field = obs.field;

  if (obs.sourceRole === SourceRole.BLOCKED) {
    return {
      ok: false,
      reason: "source_role_blocked",
      sourceRole: obs.sourceRole,
      contentDomain: obs.contentDomain,
      sourcePolicyVersion: SOURCE_POLICY_VERSION,
    };
  }

  if (obs.contentDomain === SourceContentDomain.CVENT_VENUE_HOTEL) {
    return {
      ok: false,
      reason: "cvent_venue_hotel_discovery_only",
      sourceRole: SourceRole.DISCOVERY_ONLY,
      contentDomain: obs.contentDomain,
      verificationStatus: VerificationStatus.DISCOVERY_ONLY,
      sourcePolicyVersion: SOURCE_POLICY_VERSION,
      field: field || null,
    };
  }

  if (obs.sourceRole === SourceRole.DISCOVERY_ONLY) {
    return {
      ok: false,
      reason: "discovery_only_cannot_persist_canonical",
      sourceRole: obs.sourceRole,
      contentDomain: obs.contentDomain,
      sourcePolicyVersion: SOURCE_POLICY_VERSION,
    };
  }

  if (obs.sourceRole === SourceRole.UNVERIFIED && isHotelPropertyField(field)) {
    return {
      ok: false,
      reason: "unverified_hotel_fact",
      sourceRole: obs.sourceRole,
      contentDomain: obs.contentDomain,
      sourcePolicyVersion: SOURCE_POLICY_VERSION,
    };
  }

  if (
    obs.sourceRole === SourceRole.VERIFIED_PRIMARY ||
    obs.sourceRole === SourceRole.VERIFIED_SECONDARY
  ) {
    return {
      ok: true,
      reason: null,
      sourceRole: obs.sourceRole,
      contentDomain: obs.contentDomain,
      sourcePolicyVersion: SOURCE_POLICY_VERSION,
    };
  }

  return {
    ok: false,
    reason: "unknown_or_insufficient_role",
    sourceRole: obs.sourceRole,
    contentDomain: obs.contentDomain,
    sourcePolicyVersion: SOURCE_POLICY_VERSION,
  };
}

/**
 * May this observation feed verified hotel-capability / fit scoring (GDI/ADP)?
 */
export function canUseForScoring(observation) {
  const obs = asObs(observation);

  if (obs.contentDomain === SourceContentDomain.CVENT_VENUE_HOTEL) {
    return {
      ok: false,
      reason: "cvent_venue_hotel_excluded_from_verified_scoring",
      sourceRole: SourceRole.DISCOVERY_ONLY,
      contentDomain: obs.contentDomain,
      sourcePolicyVersion: SOURCE_POLICY_VERSION,
    };
  }

  if (
    obs.sourceRole === SourceRole.DISCOVERY_ONLY ||
    obs.sourceRole === SourceRole.UNVERIFIED ||
    obs.sourceRole === SourceRole.BLOCKED
  ) {
    return {
      ok: false,
      reason: "role_excluded_from_verified_scoring",
      sourceRole: obs.sourceRole,
      contentDomain: obs.contentDomain,
      sourcePolicyVersion: SOURCE_POLICY_VERSION,
    };
  }

  // Event-platform Cvent: allowed for event evidence scoring, not hotel capability
  if (obs.contentDomain === SourceContentDomain.CVENT_EVENT_PLATFORM) {
    const field = String(obs.field || "");
    if (isHotelPropertyField(field) && /rooms|meeting|ballroom|capacity|keys/i.test(field)) {
      return {
        ok: false,
        reason: "cvent_event_platform_not_hotel_capability",
        sourceRole: obs.sourceRole,
        contentDomain: obs.contentDomain,
        sourcePolicyVersion: SOURCE_POLICY_VERSION,
      };
    }
    return {
      ok: true,
      reason: null,
      sourceRole: obs.sourceRole,
      contentDomain: obs.contentDomain,
      sourcePolicyVersion: SOURCE_POLICY_VERSION,
      note: "event_evidence_only",
    };
  }

  return {
    ok: true,
    reason: null,
    sourceRole: obs.sourceRole,
    contentDomain: obs.contentDomain,
    sourcePolicyVersion: SOURCE_POLICY_VERSION,
  };
}

/**
 * May this observation be shown to customers as verified hotel truth?
 */
export function canDisplayToCustomer(observation) {
  const obs = asObs(observation);

  if (obs.contentDomain === SourceContentDomain.CVENT_VENUE_HOTEL) {
    return {
      ok: false,
      reason: "cvent_venue_hotel_not_customer_verified_truth",
      sourceRole: SourceRole.DISCOVERY_ONLY,
      contentDomain: obs.contentDomain,
      sourcePolicyVersion: SOURCE_POLICY_VERSION,
    };
  }

  if (
    obs.sourceRole === SourceRole.DISCOVERY_ONLY ||
    obs.sourceRole === SourceRole.UNVERIFIED ||
    obs.sourceRole === SourceRole.BLOCKED
  ) {
    return {
      ok: false,
      reason: "role_not_customer_displayable_as_verified",
      sourceRole: obs.sourceRole,
      contentDomain: obs.contentDomain,
      sourcePolicyVersion: SOURCE_POLICY_VERSION,
    };
  }

  return {
    ok: true,
    reason: null,
    sourceRole: obs.sourceRole,
    contentDomain: obs.contentDomain,
    sourcePolicyVersion: SOURCE_POLICY_VERSION,
  };
}

/**
 * Does this observation require independent verification before canonical use?
 */
export function requiresIndependentVerification(observation) {
  const obs = asObs(observation);

  if (obs.contentDomain === SourceContentDomain.CVENT_VENUE_HOTEL) {
    return {
      required: true,
      reason: "cvent_venue_hotel_discovery_only",
      sourceRole: SourceRole.DISCOVERY_ONLY,
      contentDomain: obs.contentDomain,
      sourcePolicyVersion: SOURCE_POLICY_VERSION,
    };
  }

  if (
    obs.sourceRole === SourceRole.DISCOVERY_ONLY ||
    obs.sourceRole === SourceRole.UNVERIFIED
  ) {
    return {
      required: true,
      reason: "role_requires_independent_verification",
      sourceRole: obs.sourceRole,
      contentDomain: obs.contentDomain,
      sourcePolicyVersion: SOURCE_POLICY_VERSION,
    };
  }

  if (obs.contentDomain === SourceContentDomain.CVENT_EVENT_PLATFORM) {
    return {
      required: false,
      reason: "cvent_event_platform_evaluated_under_gdi_event_policy",
      sourceRole: obs.sourceRole,
      contentDomain: obs.contentDomain,
      sourcePolicyVersion: SOURCE_POLICY_VERSION,
    };
  }

  return {
    required: false,
    reason: null,
    sourceRole: obs.sourceRole,
    contentDomain: obs.contentDomain,
    sourcePolicyVersion: SOURCE_POLICY_VERSION,
  };
}

/**
 * Resolve role when Cvent discovery is corroborated by an independent source.
 * Canonical provenance follows the independent source.
 *
 * @param {object} discoveryObs - typically Cvent venue
 * @param {object} independentObs - first-party / brand / etc.
 */
export function resolveMixedProvenance(discoveryObs, independentObs) {
  const d = asObs(discoveryObs);
  const i = asObs(independentObs);
  const persist = canPersistAsCanonical(i);
  if (!persist.ok) {
    return {
      ok: false,
      canonicalSource: null,
      discoverySource: d,
      verificationStatus: VerificationStatus.NEEDS_SOURCE_REVIEW,
      reason: persist.reason,
    };
  }
  return {
    ok: true,
    canonicalSource: i,
    discoverySource: d,
    primarySourceRole: i.sourceRole,
    discoverySourceRole: SourceRole.DISCOVERY_ONLY,
    verificationStatus:
      i.sourceRole === SourceRole.VERIFIED_PRIMARY
        ? VerificationStatus.VERIFIED_PRIMARY
        : VerificationStatus.VERIFIED_MULTI_SOURCE,
    note: "canonical_follows_independent_source; cvent_retained_as_discovery_only",
  };
}

/**
 * Create a research candidate from a discovery-only observation (no canonical write).
 */
export function createDiscoveryResearchCandidate(observation, opts = {}) {
  const obs = asObs(observation);
  const domain =
    obs.contentDomain ||
    classifySourceContentDomain(obs.url, { sourceId: obs.sourceId });
  return {
    type: "PROPERTY_FACT_RESEARCH_CANDIDATE",
    sourcePolicyVersion: SOURCE_POLICY_VERSION,
    contentDomain: domain,
    sourceRole: SourceRole.DISCOVERY_ONLY,
    verificationStatus: VerificationStatus.DISCOVERY_ONLY,
    field: obs.field || opts.field || null,
    candidateValue: obs.candidateValue ?? opts.candidateValue ?? null,
    sourceUrl: obs.url,
    requiresIndependentVerification: true,
    canPersistAsCanonical: false,
    canUseForScoring: false,
    canDisplayToCustomer: false,
    createdAt: new Date().toISOString(),
    notes: opts.notes || "Discovery-only candidate — independent verification required",
  };
}

/**
 * Convenience: evaluate all four gates for one observation.
 */
export function evaluateSourceGates(observation) {
  return {
    sourcePolicyVersion: SOURCE_POLICY_VERSION,
    observation: asObs(observation),
    canPersistAsCanonical: canPersistAsCanonical(observation),
    canUseForScoring: canUseForScoring(observation),
    canDisplayToCustomer: canDisplayToCustomer(observation),
    requiresIndependentVerification: requiresIndependentVerification(observation),
  };
}

export {
  classifySourceContentDomain,
  defaultRoleForDomain,
  normalizeObservation,
};
