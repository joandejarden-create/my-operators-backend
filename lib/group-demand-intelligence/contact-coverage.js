/**
 * Reusable GDI hotel-level contact coverage + funnel metrics.
 * Hotel-agnostic — no Bethesda/hotelId constants.
 *
 * GDI Operating Law: reusable product learning lives here;
 * hotel-specific facts stay in opportunity/hotel config data.
 */

import { PRIMARY_KIND } from "./contact-candidate/person-discovery-states.js";
import { hasNamedPerson, isActionableEntityContact } from "./contact-resolution.js";

export const REACHABILITY_NEED = Object.freeze({
  EMAIL_ONLY: "EMAIL_ONLY",
  PHONE_ONLY: "PHONE_ONLY",
  EMAIL_AND_PHONE: "EMAIL_AND_PHONE",
  NO_ENRICHMENT_NEEDED: "NO_ENRICHMENT_NEEDED",
  NOT_ELIGIBLE: "NOT_ELIGIBLE",
});

export const FIELD_MERGE_DECISION = Object.freeze({
  ACCEPT_NEW_FIELD: "ACCEPT_NEW_FIELD",
  CORROBORATE_EXISTING: "CORROBORATE_EXISTING",
  HOLD_FOR_REVIEW: "HOLD_FOR_REVIEW",
  REJECT_FIELD: "REJECT_FIELD",
  NO_INCREMENTAL_VALUE: "NO_INCREMENTAL_VALUE",
});

/**
 * Classify reachability need for a named-person candidate.
 * Functional entities and unresolved identities are NOT_ELIGIBLE.
 */
export function classifyReachabilityNeed(candidate = {}, opts = {}) {
  const kind = candidate.primaryKind || opts.primaryKind || null;
  if (kind === PRIMARY_KIND.FUNCTIONAL_ENTITY || candidate.functionalEntity) {
    return REACHABILITY_NEED.NOT_ELIGIBLE;
  }
  if (kind === PRIMARY_KIND.UNRESOLVED || !candidate.name) {
    return REACHABILITY_NEED.NOT_ELIGIBLE;
  }
  if (!hasNamedPerson(candidate) && !opts.forceNamed) {
    return REACHABILITY_NEED.NOT_ELIGIBLE;
  }

  const conf = String(candidate.candidateConfidence || candidate.whoConfidence || "").toUpperCase();
  if (!["HIGH", "MEDIUM"].includes(conf) && !opts.skipConfidenceGate) {
    return REACHABILITY_NEED.NOT_ELIGIBLE;
  }

  const employment = String(candidate.employmentStatus || "").toUpperCase();
  if (
    employment &&
    !["CURRENT_CONFIRMED", "CURRENT_PROBABLE", ""].includes(employment) &&
    !opts.skipEmploymentGate
  ) {
    return REACHABILITY_NEED.NOT_ELIGIBLE;
  }

  const eventRel = String(candidate.eventRelationship || "");
  if (eventRel === "WEAK_CONTEXT" || eventRel === "ORGANIZATION_ROLE_ONLY") {
    if (!opts.allowOrgRoleOnly) return REACHABILITY_NEED.NOT_ELIGIBLE;
  }

  const hasEmail = Boolean(candidate.email);
  const hasPhone = Boolean(candidate.phone);
  if (hasEmail && hasPhone) return REACHABILITY_NEED.NO_ENRICHMENT_NEEDED;
  if (!hasEmail && !hasPhone) return REACHABILITY_NEED.EMAIL_AND_PHONE;
  if (!hasEmail) return REACHABILITY_NEED.EMAIL_ONLY;
  return REACHABILITY_NEED.PHONE_ONLY;
}

/**
 * Hotel-agnostic contact coverage calculator.
 *
 * @param {object} input
 * @param {object[]} input.opportunities - qualified opportunities
 * @param {object[]} [input.discoveryRows] - discoverContactCandidates results
 * @param {object[]} [input.enrichmentOutcomes] - per-person reachability eval rows
 */
export function calculateGdiContactCoverage({
  opportunities = [],
  discoveryRows = [],
  enrichmentOutcomes = [],
} = {}) {
  const qualified = (opportunities || []).filter(
    (o) => o && o.priority !== "DISQUALIFIED" && o.priority !== "CLOSED_DISQUALIFIED"
  );
  const total = qualified.length;

  const byOppId = new Map((discoveryRows || []).map((r) => [r.opportunityId || r.id, r]));

  let namedPersonPrimaries = 0;
  let functionalEntityPrimaries = 0;
  let unresolved = 0;
  let highConfidenceWho = 0;
  let emailCoverage = 0;
  let phoneCoverage = 0;
  let bothCoverage = 0;
  let enrichmentEligible = 0;

  for (const o of qualified) {
    const row = byOppId.get(o.id);
    const primary = row?.primaryCandidate || o.primaryContact || null;
    const kind =
      row?.primaryKind ||
      (primary?.functionalEntity
        ? PRIMARY_KIND.FUNCTIONAL_ENTITY
        : hasNamedPerson(primary)
          ? PRIMARY_KIND.NAMED_PERSON
          : isActionableEntityContact(primary || {}, o)
            ? PRIMARY_KIND.FUNCTIONAL_ENTITY
            : PRIMARY_KIND.UNRESOLVED);

    if (kind === PRIMARY_KIND.NAMED_PERSON) namedPersonPrimaries += 1;
    else if (kind === PRIMARY_KIND.FUNCTIONAL_ENTITY) functionalEntityPrimaries += 1;
    else unresolved += 1;

    const conf = String(
      primary?.candidateConfidence || row?.primaryCandidate?.candidateConfidence || ""
    ).toUpperCase();
    if (kind !== PRIMARY_KIND.UNRESOLVED && conf === "HIGH") highConfidenceWho += 1;

    const email = primary?.email || null;
    const phone = primary?.phone || null;
    if (email) emailCoverage += 1;
    if (phone) phoneCoverage += 1;
    if (email && phone) bothCoverage += 1;

    const need = classifyReachabilityNeed(
      { ...primary, primaryKind: kind, candidateConfidence: conf || primary?.candidateConfidence },
      {}
    );
    if (
      need === REACHABILITY_NEED.EMAIL_ONLY ||
      need === REACHABILITY_NEED.PHONE_ONLY ||
      need === REACHABILITY_NEED.EMAIL_AND_PHONE
    ) {
      enrichmentEligible += 1;
    }
  }

  const attempted = (enrichmentOutcomes || []).filter((r) => r.attempted).length;
  const improved = (enrichmentOutcomes || []).filter((r) => r.meaningfulImprovement).length;
  const creditsSpent = (enrichmentOutcomes || []).reduce(
    (s, r) => s + (Number(r.creditsSpent) || 0),
    0
  );

  const whoEstablished = namedPersonPrimaries + functionalEntityPrimaries;
  const pct = (n) => (total ? Math.round((1000 * n) / total) / 10 : 0);

  return {
    totalOpportunities: total,
    namedPersonPrimaries,
    functionalEntityPrimaries,
    unresolved,
    whoEstablished,
    highConfidenceWho,
    emailCoverage,
    phoneCoverage,
    bothCoverage,
    enrichmentEligible,
    enrichmentAttempted: attempted,
    enrichmentImproved: improved,
    creditsSpent,
    creditsPerMeaningfulImprovement:
      improved > 0 ? Math.round((100 * creditsSpent) / improved) / 100 : null,
    rates: {
      whoResolutionRate: pct(whoEstablished),
      highConfidenceWhoRate: pct(highConfidenceWho),
      emailReachabilityRate: pct(emailCoverage),
      phoneReachabilityRate: pct(phoneCoverage),
      fullReachabilityRate: pct(bothCoverage),
      paidEnrichmentEligibilityRate: pct(enrichmentEligible),
      paidEnrichmentSuccessRate: attempted ? pct(improved) : null,
      unresolvedIdentityRate: pct(unresolved),
    },
    funnel: buildGdiContactFunnel({
      totalOpportunities: total,
      whoEstablished,
      unresolved,
      enrichmentEligible,
      emailCoverage,
      phoneCoverage,
      bothCoverage,
      actionable: bothCoverage, // conservative: both channels = actionable
    }),
  };
}

export function buildGdiContactFunnel({
  totalOpportunities,
  whoEstablished,
  unresolved,
  enrichmentEligible,
  emailCoverage,
  phoneCoverage,
  bothCoverage,
  actionable,
} = {}) {
  return {
    qualifiedOpportunities: totalOpportunities,
    credibleWhoEstablished: whoEstablished,
    unresolvedWho: unresolved,
    reachabilityEnrichmentEligible: enrichmentEligible,
    usableEmail: emailCoverage,
    usablePhone: phoneCoverage,
    bothEmailAndPhone: bothCoverage,
    actionableContact: actionable,
  };
}

/**
 * Build a frozen reachability cohort from discovery enrichment queue / rows.
 * Dedupes same person+org for credit efficiency; keeps opportunityIds[].
 */
export function buildReachabilityCohortFromQueue(queue = [], opts = {}) {
  const subjects = [];
  const seen = new Map();

  for (const q of queue || []) {
    if (!q?.name) continue;
    const parts = String(q.name).trim().split(/\s+/);
    if (parts.length < 2) continue;
    const first_name = parts[0];
    const last_name = parts.slice(1).join(" ");
    const org = q.organization || "";
    const domain =
      q.domain ||
      (q.email && String(q.email).includes("@")
        ? String(q.email).split("@")[1].toLowerCase()
        : null) ||
      inferDomainFromOfficialSourceUrl(q.sourceUrl || q.officialSourceUrl);
    const dedupeKey = `${normalizeKey(q.name)}::${normalizeKey(org)}`;
    if (seen.has(dedupeKey)) {
      const existing = seen.get(dedupeKey);
      if (q.opportunityId && !existing.opportunityIds.includes(q.opportunityId)) {
        existing.opportunityIds.push(q.opportunityId);
        existing.opportunityTitles.push(q.title || q.opportunityId);
      }
      continue;
    }

    const gap = String(q.gap || q.nextAction || "").toUpperCase();
    const requestEmail =
      gap.includes("EMAIL") || gap.includes("BOTH") || q.nextAction === "SURFE_BOTH";
    const requestMobile =
      gap.includes("PHONE") || gap.includes("BOTH") || q.nextAction === "SURFE_PHONE" || q.nextAction === "SURFE_BOTH";

    const subject = {
      id: `gdi_reach_${slug(q.name)}_${slug(org).slice(0, 24)}`,
      full_name: q.name,
      first_name,
      last_name,
      opportunityId: q.opportunityId,
      opportunityIds: q.opportunityId ? [q.opportunityId] : [],
      opportunityTitles: q.title ? [q.title] : [],
      opportunityTitle: q.title || null,
      enrichmentOrganization: org,
      enrichmentDomain: domain,
      title: q.role || null,
      whoConfidence: q.confidence,
      candidateScore: q.score,
      baselineEmail: q.email || null,
      baselinePhone: q.phone || null,
      baselineEmailType: q.email ? inferEmailType(q.email, q.name) : null,
      baselineEmailVerification: q.email ? "OFFICIAL_SOURCE_VERIFIED" : null,
      baselinePhoneType: q.phone ? "UNKNOWN" : null,
      reachabilityNeed: classifyReachabilityNeed(
        {
          name: q.name,
          email: q.email,
          phone: q.phone,
          candidateConfidence: q.confidence,
          primaryKind: PRIMARY_KIND.NAMED_PERSON,
          employmentStatus: "CURRENT_PROBABLE",
          eventRelationship: "CURRENT_ROLE_LIKELY_OWNER",
        },
        {}
      ),
      requestEmail: Boolean(requestEmail && !q.email),
      requestMobile: Boolean(requestMobile && !q.phone),
      // If BOTH and no email, request both; if phone-only gap, email false
      whyInCohort: `WHO established · gap=${q.gap || "unknown"} · conf=${q.confidence}`,
    };

    // Fix request flags from need
    if (subject.reachabilityNeed === REACHABILITY_NEED.EMAIL_ONLY) {
      subject.requestEmail = true;
      subject.requestMobile = false;
    } else if (subject.reachabilityNeed === REACHABILITY_NEED.PHONE_ONLY) {
      subject.requestEmail = false;
      subject.requestMobile = true;
    } else if (subject.reachabilityNeed === REACHABILITY_NEED.EMAIL_AND_PHONE) {
      subject.requestEmail = true;
      subject.requestMobile = true;
    } else {
      continue; // not eligible
    }

    seen.set(dedupeKey, subject);
    subjects.push(subject);
  }

  return {
    version: opts.version || "gdi_contact_reachability_cohort_v1",
    frozenAt: new Date().toISOString(),
    purpose:
      opts.purpose ||
      "Bounded reachability enrichment cohort — Dealality owns WHO; Surfe may help HOW.",
    identity_rule:
      "Dealality establishes WHO. Surfe may only enrich HOW to reach. Never create/replace identity.",
    hotelId: opts.hotelId || null,
    hotelName: opts.hotelName || null,
    credit_caps: opts.credit_caps || {
      email_people_max: subjects.filter((s) => s.requestEmail).length,
      mobile_people_max: subjects.filter((s) => s.requestMobile).length,
      one_attempt_per_enrichment_type: true,
      retry_only_on_transient_api_failure: true,
    },
    subjects,
  };
}

function normalizeKey(s) {
  return String(s || "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
}

function slug(s) {
  return normalizeKey(s).replace(/\s+/g, "_");
}

function inferEmailType(email, name) {
  const local = String(email).split("@")[0].toLowerCase();
  if (/^(info|contact|support|hello|admin|office|meetings|registration)$/i.test(local)) {
    return "GENERIC_ORGANIZATION";
  }
  if (/^(tournaments?|registrar|events?|housing)$/i.test(local)) return "ROLE_BASED";
  const tokens = String(name || "")
    .toLowerCase()
    .split(/\s+/)
    .filter(Boolean);
  if (tokens.length >= 2 && local.includes(tokens[0][0]) && local.includes(tokens[tokens.length - 1])) {
    return "DIRECT_WORK";
  }
  return "UNKNOWN";
}

/**
 * Infer enrichment domain from an official-source URL host (reusable).
 * Strips www. — does not invent brand domains without a source URL.
 */
export function inferDomainFromOfficialSourceUrl(url) {
  if (!url) return null;
  try {
    const host = new URL(String(url)).hostname.toLowerCase().replace(/^www\./, "");
    if (!host || host.includes("linkedin.com") || host.includes("facebook.com")) return null;
    return host;
  } catch {
    return null;
  }
}

/**
 * Map Surfe email/phone outcomes → field merge decision (report simulation).
 */
export function mapOutcomeToFieldMerge({ outcome, field } = {}) {
  const o = String(outcome || "");
  if (!o || /NOT_FOUND|IDENTITY_REJECTED/i.test(o)) return FIELD_MERGE_DECISION.REJECT_FIELD;
  if (/CONFLICTS|AMBIGUOUS|SAME_MAIN|OTHER_PERSON_PHONE_COLLISION|COLLISION/i.test(o)) {
    return /SAME_MAIN/i.test(o)
      ? FIELD_MERGE_DECISION.NO_INCREMENTAL_VALUE
      : FIELD_MERGE_DECISION.HOLD_FOR_REVIEW;
  }
  if (/CORROBORAT/i.test(o)) return FIELD_MERGE_DECISION.CORROBORATE_EXISTING;
  if (/NEW_DIRECT|NEW_MOBILE|NEW_OFFICE|NEW_ROLE/i.test(o)) {
    return FIELD_MERGE_DECISION.ACCEPT_NEW_FIELD;
  }
  if (/DIFFERENT_BUT_PLAUSIBLE|UNVERIFIED/i.test(o)) return FIELD_MERGE_DECISION.HOLD_FOR_REVIEW;
  return FIELD_MERGE_DECISION.NO_INCREMENTAL_VALUE;
}

/**
 * Hypothetical grade shift after accepted fields only (A–E heuristic, reusable).
 * Rejected / held / no-result fields must not change grade.
 * Pass hadOfficialEmail=true only when recomputing absolute grade from scratch
 * (not when comparing before→after on an enrichment eval).
 */
export function simulateGradeAfterAcceptedFields({
  beforeGrade,
  hasNamedPerson: named,
  acceptedDirectEmail,
  acceptedUsefulPhone,
  hadOfficialEmail,
} = {}) {
  let g = String(beforeGrade || "E").toUpperCase();
  if (!named) return g;
  const emailStrength = Boolean(acceptedDirectEmail || hadOfficialEmail);
  if (!acceptedDirectEmail && !acceptedUsefulPhone && !hadOfficialEmail) return g;
  if (emailStrength) {
    if (acceptedUsefulPhone) return "A";
    if (acceptedDirectEmail && (g === "E" || g === "D" || g === "C")) return "B";
    if (hadOfficialEmail && acceptedUsefulPhone) return "A";
    if (hadOfficialEmail && !acceptedDirectEmail && !acceptedUsefulPhone) return g;
    return g;
  }
  if (acceptedUsefulPhone && (g === "B" || g === "C")) return g === "B" ? "A" : "B";
  return g;
}
