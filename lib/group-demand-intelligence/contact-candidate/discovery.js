/**
 * GDI Contact Candidate Discovery engine.
 * Finds / ranks WHO (probable decision influencers). Does NOT call Surfe.
 * Reachability stays in Contact Intelligence / contact-resolution.
 */

import { personDedupeKey } from "../../hotel-intelligence/contact-intelligence/contact-reachability.js";
import {
  hasNamedPerson,
  isActionableEntityContact,
  classifyTargetRoleMatch,
} from "../contact-resolution.js";
import { OFFICIAL_CONTACT_ENRICHMENTS_V1 } from "../contact-official-enrichments-v1.js";
import {
  CANDIDATE_CONFIDENCE,
  CANDIDATE_STATUS,
  GDI_CONTACT_ROLE,
  IDENTITY_STATUS,
  buildCandidateSearchQueries,
  classifyEventFamily,
} from "./ontology.js";
import { annotateCandidate } from "./scoring.js";
import {
  CONTACT_CANDIDATE_DISCOVERY_PASS_ID,
  CONTACT_CANDIDATE_SEEDS_V1,
} from "./seeds-v1.js";
import { OFFICIAL_PERSON_DISCOVERIES_V1 } from "./official-person-discoveries-v1.js";
import {
  PRIMARY_KIND,
  applyPersonDiscoveryGates,
  classifyEnrichmentGap,
  isSurfeEligiblePerson,
} from "./person-discovery-states.js";
import { bridgeResearchContactsToWhoCandidates, normalizeOpportunityContactEvidence } from "./opportunity-contact-evidence.js";

const NO_PROBABLE = "NO PROBABLE NAMED CONTACT RESOLVED";

function isPlaceholderName(name) {
  const n = String(name || "").trim();
  if (!n) return true;
  if (/^(unknown|n\/?a|tbd|staff|none)$/i.test(n)) return true;
  if (
    /\b(conference staff|meetings staff|program office|organizers|tournaments?\s+desk|official channel|channel TBD)\b/i.test(
      n
    )
  ) {
    return true;
  }
  // Org/desk labels that look multi-token but are not people (must fail person-name shape)
  const looksLikePerson = /^[A-Z][a-z]+(?:\s+[A-Z][a-z'.-]+){1,3}$/.test(n);
  if (looksLikePerson) return false;
  if (/[\/|]/.test(n)) return true; // "Andrea / Justin" combined labels
  if (
    /\b(association|soccer|society|academy|congress|bureau|desk|inbox|office|services|staff|organizers|meetings|housing|tournaments)\b/i.test(
      n
    )
  ) {
    return true;
  }
  return false;
}

/** True person (First Last…) or approved housing entity (e.g. HBC Event Services). */
function isCredibleCandidateIdentity(contact, opportunity) {
  if (contact.functionalEntity && contact.gdiContactRole === GDI_CONTACT_ROLE.HOUSING_OWNER) {
    return true;
  }
  if (contact.functionalEntity && contact.targetRoleMatch === "HOUSING_SOURCING_CONTACT") {
    return true;
  }
  if (isActionableEntityContact(contact, opportunity)) return true;
  if (isPlaceholderName(contact.name || contact.fullName)) return false;
  return hasNamedPerson(contact);
}

function collectRawCandidates(opportunity = {}, opts = {}) {
  const out = [];
  const push = (c, provenance) => {
    if (!c) return;
    out.push({ ...c, __discoveryProvenance: provenance });
  };

  const pushResearchNormalized = (raw, provenance) => {
    if (!raw) return;
    if (raw.__officialPatchPrimary || raw.__officialPatchBackup || raw.skipResearchNormalize) {
      push(raw, provenance);
      return;
    }
    const n = normalizeOpportunityContactEvidence(raw, opportunity, opts);
    if (!n || n.forceReject || n.rejected) return;
    push(n, provenance);
  };

  const patch = OFFICIAL_CONTACT_ENRICHMENTS_V1[opportunity.id];
  if (patch?.primary) push(patch.primary, "official_enrichment_primary");
  for (const b of patch?.backups || []) push(b, "official_enrichment_backup");

  // A1: normalize research pack contacts before scoring (raw labels must not bypass bridge)
  if (opportunity.primaryContact) {
    pushResearchNormalized(opportunity.primaryContact, "pack_primary_bridged");
  }
  for (const c of opportunity.contacts || []) {
    pushResearchNormalized(c, "pack_contacts_bridged");
  }
  for (const b of opportunity.backupContacts || []) {
    pushResearchNormalized(b, "pack_backup_bridged");
  }

  const seeds = [
    ...(opts.seeds?.[opportunity.id] || CONTACT_CANDIDATE_SEEDS_V1[opportunity.id] || []),
    ...(opts.personDiscoveries?.[opportunity.id] ||
      OFFICIAL_PERSON_DISCOVERIES_V1[opportunity.id] ||
      []),
  ];
  for (const s of seeds) push(s, s.__discoveryProvenance || "person_discovery_seed");

  for (const extra of opts.extraCandidates || []) push(extra, "extra");

  // A1 bridge: researchContactEvidence + remaining research labels (deduped later)
  if (opts.skipResearchBridge !== true) {
    const bridged = bridgeResearchContactsToWhoCandidates(opportunity, opts);
    for (const c of bridged.extraCandidates || []) {
      push(c, c.__discoveryProvenance || "research_contact_evidence_bridge");
    }
  }

  return out;
}

function dedupeRaw(candidates = []) {
  const byKey = new Map();
  const provenanceRank = (x) => {
    const p = String(x?.__discoveryProvenance || "");
    if (p === "person_discovery_seed") return 20;
    if (p === "official_enrichment_primary") return 18;
    if (p.startsWith("official")) return 12;
    if (p === "pack_backup_bridged") return 4;
    if (p === "pack_primary_bridged") return 3;
    if (p.startsWith("pack")) return 2;
    return 6;
  };
  const evidenceScore = (x) =>
    (x.claimKind === "FACT" ? 4 : 0) +
    (x.sourceUrl || x.source ? 2 : 0) +
    (x.email ? 2 : 0) +
    (x.phone ? 1 : 0) +
    (x.whyThisContact || x.whyThisPerson ? 1 : 0) +
    (x.employmentStatus ? 3 : 0) +
    (x.sourceAuthorityTier || x.sourceTier ? 2 : 0) +
    provenanceRank(x);

  for (const c of candidates) {
    if (!c) continue;
    const key =
      personDedupeKey(c.name || c.fullName, c.organization) ||
      String(c.email || "")
        .toLowerCase()
        .trim() ||
      `${c.role || ""}|${c.organization || ""}|${c.phone || ""}`;
    if (!key) continue;
    const existing = byKey.get(key);
    if (!existing) {
      byKey.set(key, c);
      continue;
    }
    const preferNew = evidenceScore(c) > evidenceScore(existing);
    const base = preferNew ? { ...existing, ...c } : { ...c, ...existing };
    // Always keep strongest WHO evidence fields
    byKey.set(key, {
      ...base,
      employmentStatus: c.employmentStatus || existing.employmentStatus || null,
      eventRelationship: c.eventRelationship || existing.eventRelationship || null,
      sourceAuthorityTier:
        c.sourceAuthorityTier ||
        existing.sourceAuthorityTier ||
        c.sourceTier ||
        existing.sourceTier ||
        null,
      sourceTier:
        c.sourceAuthorityTier ||
        existing.sourceAuthorityTier ||
        c.sourceTier ||
        existing.sourceTier ||
        null,
      evidenceQuote: c.evidenceQuote || existing.evidenceQuote || null,
      qualityAudit: c.qualityAudit || existing.qualityAudit || null,
      gdiContactRole: c.gdiContactRole || existing.gdiContactRole || null,
      targetRoleMatch: c.targetRoleMatch || existing.targetRoleMatch || null,
      whyThisPerson: c.whyThisPerson || existing.whyThisPerson || null,
      email: c.email || existing.email || null,
      phone: c.phone || existing.phone || null,
      sourceUrl: c.sourceUrl || existing.sourceUrl || c.source || existing.source || null,
      __discoveryProvenance:
        provenanceRank(c) >= provenanceRank(existing)
          ? c.__discoveryProvenance || existing.__discoveryProvenance
          : existing.__discoveryProvenance || c.__discoveryProvenance,
      functionalEntity: Boolean(c.functionalEntity || existing.functionalEntity),
    });
  }
  return [...byKey.values()];
}

function isProbableNamedOrEntity(annotated, opportunity) {
  if (annotated.candidateStatus === CANDIDATE_STATUS.REJECTED) return false;
  if (annotated.candidateStatus === CANDIDATE_STATUS.STALE) return false;
  if (annotated.rejected) return false;
  if (annotated.identityStatus === IDENTITY_STATUS.STALE) return false;
  if (!isCredibleCandidateIdentity(annotated, opportunity)) return false;
  // Generic org desk without housing entity role → not a named probable person
  if (
    !hasNamedPerson(annotated) &&
    !isActionableEntityContact(annotated, opportunity) &&
    annotated.gdiContactRole === GDI_CONTACT_ROLE.GENERAL_ORGANIZATION_CONTACT
  ) {
    return false;
  }
  return (annotated.candidateScore || 0) >= 45;
}

/**
 * Select primary + up to 2 backups by candidate probability (not seniority / email ease).
 */
function selectionRank(candidate, opportunity, opts = {}) {
  let rank = candidate.candidateScore || 0;
  const isOverflow = opportunity.opportunityType === "OVERFLOW_HOUSING";
  const isPerson =
    hasNamedPerson(candidate) &&
    !candidate.functionalEntity &&
    !isActionableEntityContact(candidate, opportunity);

  // Named humans beat org desks / housing entities when role is event/meetings-relevant
  if (isPerson) {
    rank += 22;
    if (
      candidate.gdiContactRole === GDI_CONTACT_ROLE.MEETINGS_OWNER ||
      candidate.gdiContactRole === GDI_CONTACT_ROLE.EVENT_OWNER ||
      candidate.gdiContactRole === GDI_CONTACT_ROLE.CONFERENCE_DIRECTOR ||
      candidate.targetRoleMatch === "EVENT_MEETINGS_OWNER" ||
      candidate.targetRoleMatch === "DIRECT_DECISION_MAKER"
    ) {
      rank += 18;
    }
  } else {
    // Desk / entity labels must not outrank official named seeds
    rank -= 20;
  }

  // Housing / overflow: housing owner must beat association executives — but not
  // confirmed named meetings/event owners (those stay primary; housing is backup).
  if (isOverflow && candidate.gdiContactRole === GDI_CONTACT_ROLE.HOUSING_OWNER) {
    rank += opts.hasStrongNamedMeetingsOwner ? 5 : 25;
  }
  if (isOverflow && candidate.gdiContactRole === GDI_CONTACT_ROLE.EXECUTIVE_SPONSOR) {
    rank -= 15;
  }
  // Prefer official person discovery seeds and enrichment primaries
  if (candidate.discoveryProvenance === "person_discovery_seed") rank += 14;
  if (candidate.discoveryProvenance === "official_enrichment_primary") rank += 8;
  if (candidate.discoveryProvenance === "official_enrichment_backup") rank -= 2;
  if (candidate.discoveryProvenance === "pack_primary_bridged" && !isPerson) rank -= 8;
  // Prefer current event evidence over historical
  if (candidate.eventRelationship === "CURRENT_EVENT_CONTACT") rank += 10;
  if (candidate.eventRelationship === "CURRENT_ROLE_LIKELY_OWNER") rank += 12;
  if (candidate.eventRelationship === "HISTORICAL_EVENT_CONTACT") rank -= 4;
  // Stale already filtered; weak identity shouldn't win
  if (candidate.identityStatus === IDENTITY_STATUS.IDENTITY_UNCONFIRMED) rank -= 2;
  return rank;
}

export function selectRankedCandidates(annotatedList = [], opportunity = {}) {
  const probable = annotatedList.filter((c) => isProbableNamedOrEntity(c, opportunity));
  const hasStrongNamedMeetingsOwner = probable.some((c) => {
    if (!hasNamedPerson(c) || c.functionalEntity) return false;
    const role = String(c.gdiContactRole || "");
    const match = String(c.targetRoleMatch || "");
    return (
      (c.candidateScore || 0) >= 55 &&
      (/MEETINGS_OWNER|EVENT_OWNER|CONFERENCE_DIRECTOR|HOUSING_OWNER/.test(role) ||
        /EVENT_MEETINGS_OWNER|DIRECT_DECISION_MAKER|HOUSING_SOURCING_CONTACT/.test(match))
    );
  });
  const usable = probable
    .map((c) => ({
      c,
      rank: selectionRank(c, opportunity, { hasStrongNamedMeetingsOwner }),
    }))
    .sort((a, b) => {
      if (b.rank !== a.rank) return b.rank - a.rank;
      const ae = a.c.candidateScoreBreakdown?.eventSpecificEvidence || 0;
      const be = b.c.candidateScoreBreakdown?.eventSpecificEvidence || 0;
      return be - ae;
    })
    .map((x) => x.c);

  const primary = usable[0] || null;
  const backups = usable.slice(1, 3);
  return { primary, backups, ranked: usable };
}

/**
 * Discover + rank candidates for one opportunity. No Surfe. No paid enrichment.
 */
export function discoverContactCandidates(opportunity = {}, opts = {}) {
  const eventFamily = classifyEventFamily(opportunity);
  const queryPlan = buildCandidateSearchQueries(opportunity);
  const raw = dedupeRaw(collectRawCandidates(opportunity, opts));

  const rejected = [];
  const annotated = [];

  for (const c of raw) {
    // Normalize target role if missing; apply employment / LinkedIn gates
    let withRole = {
      ...c,
      targetRoleMatch: c.targetRoleMatch || classifyTargetRoleMatch(c, opportunity),
    };
    withRole = applyPersonDiscoveryGates(withRole, opportunity);

    if (withRole.stillEmployed === false || withRole.staleUnverified) {
      withRole.rejected = Boolean(withRole.forceReject || withRole.rejected);
      withRole.identityStatus = withRole.identityStatus || IDENTITY_STATUS.STALE;
    }

    const a = annotateCandidate(withRole, opportunity, { eventFamily });
    a.searchQueriesUsed = queryPlan.queries;
    a.discoveryProvenance = c.__discoveryProvenance || null;
    a.employmentStatus = withRole.employmentStatus || null;
    a.eventRelationship = withRole.eventRelationship || null;
    a.functionalEntity = Boolean(withRole.functionalEntity);
    a.enrichmentGap = classifyEnrichmentGap(a);

    if (a.candidateStatus === CANDIDATE_STATUS.REJECTED || a.rejected || withRole.forceReject) {
      rejected.push({
        name: a.name,
        reason: withRole.rejectReason || a.rejectReason || "rejected",
        candidateScore: a.candidateScore,
      });
      continue;
    }
    if (a.candidateStatus === CANDIDATE_STATUS.STALE) {
      rejected.push({
        name: a.name,
        reason: "stale_former_employee_or_unverified",
        candidateScore: a.candidateScore,
        candidateStatus: a.candidateStatus,
      });
      // Keep stale in audit but do not select as primary
      annotated.push(a);
      continue;
    }
    annotated.push(a);
  }

  const { primary, backups, ranked } = selectRankedCandidates(annotated, opportunity);
  const isEntity = (c) =>
    Boolean(c?.functionalEntity) || isActionableEntityContact(c, opportunity);
  const probablePeople = ranked.filter((c) => hasNamedPerson(c) && !isEntity(c));
  const probableEntities = ranked.filter((c) => isEntity(c));

  let primaryKind = PRIMARY_KIND.UNRESOLVED;
  if (primary) {
    primaryKind = isEntity(primary) ? PRIMARY_KIND.FUNCTIONAL_ENTITY : PRIMARY_KIND.NAMED_PERSON;
  }

  const unresolved = !primary;
  const packageOut = {
    opportunityId: opportunity.id,
    title: opportunity.title,
    priority: opportunity.priority,
    opportunityType: opportunity.opportunityType,
    segment: opportunity.segment,
    organizationName: opportunity.organizationName,
    eventFamily,
    passId: CONTACT_CANDIDATE_DISCOVERY_PASS_ID,
    paidEnrichmentUsed: false,
    surfeUsed: false,
    searchQueries: queryPlan.queries,
    primaryKind,
    primaryCandidate: primary
      ? {
          name: primary.name,
          role: primary.role || primary.title,
          organization: primary.organization,
          gdiContactRole: primary.gdiContactRole,
          roleRelevance: primary.roleRelevance,
          candidateScore: primary.candidateScore,
          candidateConfidence: primary.candidateConfidence,
          candidateStatus: primary.candidateStatus,
          identityStatus: primary.identityStatus,
          employmentStatus: primary.employmentStatus || null,
          eventRelationship: primary.eventRelationship || null,
          primaryKind,
          functionalEntity: Boolean(primary.functionalEntity),
          whyThisPerson: primary.whyThisPerson,
          sourceUrl: primary.sourceUrl || primary.source || null,
          sourceAuthorityTier: primary.sourceAuthorityTier || primary.sourceTier || null,
          sourceTier: primary.sourceAuthorityTier || primary.sourceTier || null,
          evidenceQuote: primary.evidenceQuote || null,
          email: primary.email || null,
          phone: primary.phone || null,
          reachabilityGap: primary.reachability?.gap || null,
          enrichmentGap: primary.enrichmentGap || classifyEnrichmentGap(primary),
          scoreBreakdown: primary.candidateScoreBreakdown,
          provenance: primary.discoveryProvenance,
          qualityAudit: primary.qualityAudit || null,
          surfeEligible: isSurfeEligiblePerson(
            { ...primary, candidateConfidence: primary.candidateConfidence },
            primaryKind
          ),
        }
      : null,
    backupCandidates: backups.map((b) => {
      const kind = isEntity(b) ? PRIMARY_KIND.FUNCTIONAL_ENTITY : PRIMARY_KIND.NAMED_PERSON;
      return {
        name: b.name,
        role: b.role || b.title,
        organization: b.organization,
        gdiContactRole: b.gdiContactRole,
        candidateScore: b.candidateScore,
        candidateConfidence: b.candidateConfidence,
        candidateStatus: b.candidateStatus,
        employmentStatus: b.employmentStatus || null,
        eventRelationship: b.eventRelationship || null,
        primaryKind: kind,
        functionalEntity: Boolean(b.functionalEntity),
        whyThisPerson: b.whyThisPerson,
        sourceUrl: b.sourceUrl || b.source || null,
        email: b.email || null,
        phone: b.phone || null,
        reachabilityGap: b.reachability?.gap || null,
        enrichmentGap: b.enrichmentGap || classifyEnrichmentGap(b),
        surfeEligible: isSurfeEligiblePerson(
          { ...b, candidateConfidence: b.candidateConfidence },
          kind
        ),
      };
    }),
    resolution: unresolved
      ? NO_PROBABLE
      : "PROBABLE_CONTACT_RESOLVED",
    metrics: {
      candidateCount: annotated.length,
      probableNamedCount: probablePeople.length + probableEntities.length,
      probablePersonCount: probablePeople.length,
      probableEntityCount: probableEntities.length,
      hasPrimary: Boolean(primary),
      hasBackup1: backups.length >= 1,
      hasBackup2: backups.length >= 2,
      primaryHasEmail: Boolean(primary?.email),
      primaryHasPhone: Boolean(primary?.phone),
      primaryHasBoth: Boolean(primary?.email && primary?.phone),
      primaryHighConfidence: primary?.candidateConfidence === CANDIDATE_CONFIDENCE.HIGH,
      primaryIsPerson: primaryKind === PRIMARY_KIND.NAMED_PERSON,
      primaryIsEntity: primaryKind === PRIMARY_KIND.FUNCTIONAL_ENTITY,
    },
    audit: {
      rejectedCandidates: rejected,
      allCandidates: annotated.map((a) => ({
        name: a.name,
        role: a.role,
        gdiContactRole: a.gdiContactRole,
        candidateScore: a.candidateScore,
        candidateStatus: a.candidateStatus,
        candidateConfidence: a.candidateConfidence,
        identityStatus: a.identityStatus,
        employmentStatus: a.employmentStatus || null,
        eventRelationship: a.eventRelationship || null,
        functionalEntity: Boolean(a.functionalEntity),
        provenance: a.discoveryProvenance,
        sourceUrl: a.sourceUrl || a.source || null,
        whyThisPerson: a.whyThisPerson,
        scoreBreakdown: a.candidateScoreBreakdown,
        email: a.email || null,
        phone: a.phone || null,
      })),
      existingPackPrimary: opportunity.primaryContact
        ? {
            name: opportunity.primaryContact.name || null,
            email: opportunity.primaryContact.email || null,
            phone: opportunity.primaryContact.phone || null,
            grade: opportunity.contactGrade || null,
          }
        : null,
    },
  };

  return packageOut;
}

/**
 * Run discovery across a list of opportunities (e.g. 29 qualified Bethesda).
 */
export function discoverContactCandidatesForOpportunities(opportunities = [], opts = {}) {
  const rows = [];
  for (const o of opportunities) {
    if (!o || o.priority === "DISQUALIFIED") continue;
    rows.push(discoverContactCandidates(o, opts));
  }
  return {
    passId: CONTACT_CANDIDATE_DISCOVERY_PASS_ID,
    generatedAt: new Date().toISOString(),
    paidEnrichmentEnabled: false,
    surfeEnabled: false,
    count: rows.length,
    opportunities: rows,
    /** Alias for callers that expect `.rows` */
    rows,
    summary: summarizeDiscoveryResults(rows),
  };
}

export function summarizeDiscoveryResults(rows = []) {
  const n = rows.length;
  const withPrimary = rows.filter((r) => r.metrics.hasPrimary).length;
  const personPrimary = rows.filter((r) => r.primaryKind === PRIMARY_KIND.NAMED_PERSON).length;
  const entityPrimary = rows.filter((r) => r.primaryKind === PRIMARY_KIND.FUNCTIONAL_ENTITY).length;
  const with2 = rows.filter((r) => r.metrics.probableNamedCount >= 2).length;
  const highConf = rows.filter((r) => r.metrics.primaryHighConfidence).length;
  const medConf = rows.filter(
    (r) => r.primaryCandidate?.candidateConfidence === CANDIDATE_CONFIDENCE.MEDIUM
  ).length;
  const unresolved = rows.filter((r) => r.resolution === NO_PROBABLE).length;
  const email = rows.filter((r) => r.metrics.primaryHasEmail).length;
  const phone = rows.filter((r) => r.metrics.primaryHasPhone).length;
  const both = rows.filter((r) => r.metrics.primaryHasBoth).length;
  const backup = rows.filter((r) => r.metrics.hasBackup1).length;

  return {
    totalOpportunities: n,
    withAtLeastOneProbableNamedContact: withPrimary,
    namedPersonPrimaries: personPrimary,
    functionalEntityPrimaries: entityPrimary,
    withAtLeastTwoCandidates: with2,
    highConfidencePrimary: highConf,
    mediumConfidencePrimary: medConf,
    unresolved,
    primaryWithEmail: email,
    primaryWithPhone: phone,
    primaryWithBoth: both,
    backupCoverage: backup,
  };
}

export function buildBeforeSnapshot(opportunities = []) {
  let named = 0;
  let email = 0;
  let phone = 0;
  let both = 0;
  let backup = 0;
  for (const o of opportunities) {
    if (!o || o.priority === "DISQUALIFIED") continue;
    const p = o.primaryContact || {};
    const isNamed =
      hasNamedPerson(p) || isActionableEntityContact(p, o);
    if (isNamed) named += 1;
    if (p.email) email += 1;
    if (p.phone) phone += 1;
    if (p.email && p.phone) both += 1;
    if ((o.backupContacts || []).length > 0) backup += 1;
  }
  return {
    totalOpportunities: opportunities.filter((o) => o && o.priority !== "DISQUALIFIED").length,
    withAtLeastOneProbableNamedContact: named,
    primaryWithEmail: email,
    primaryWithPhone: phone,
    primaryWithBoth: both,
    backupCoverage: backup,
  };
}

export { NO_PROBABLE, CONTACT_CANDIDATE_DISCOVERY_PASS_ID };
