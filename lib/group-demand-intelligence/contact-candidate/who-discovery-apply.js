/**
 * Apply WHO discovery results onto GDI opportunities.
 * Named primary wins; prior functional/generic contact retained as backup.
 * Does not call Surfe/PDL.
 */

import { enrichContactRecord, gradeContact, hasNamedPerson, isLikelyPersonName } from "../contact-resolution.js";
import { CONTACT_QUALITY } from "../claim-types.js";
import {
  WHO_RESEARCH_STATE,
  acceptNamedWhoGate,
  mapDiscoveryToCurrentness,
  mapDiscoveryToEventRelation,
} from "./who-gap.js";
import { PRIMARY_KIND } from "./person-discovery-states.js";

function contactKey(c = {}) {
  return `${String(c.name || "").toLowerCase()}|${String(c.email || "").toLowerCase()}`;
}

function isFunctionalLike(contact = {}) {
  if (!contact) return false;
  if (contact.functionalEntity) return true;
  if (!hasNamedPerson(contact)) return true;
  const cq = String(contact.contactQuality || "");
  return (
    cq === CONTACT_QUALITY.GENERIC_INBOX ||
    cq === CONTACT_QUALITY.GENERAL_ORGANIZATION_CONTACT ||
    cq === CONTACT_QUALITY.ASSOCIATION_MANAGEMENT_CONTACT ||
    cq === CONTACT_QUALITY.HOUSING_SOURCING_CONTACT
  );
}

function isNamedPersonCandidate(candidate = {}) {
  if (!candidate || candidate.functionalEntity) return false;
  return isLikelyPersonName(candidate.name) && hasNamedPerson(candidate);
}

/**
 * If discovery mis-labels a desk as NAMED_PERSON, promote a named backup
 * or coerce to functional entity handling.
 */
function normalizeDiscoveryRowForApply(discoveryRow = {}) {
  const primaryKind = discoveryRow.primaryKind || PRIMARY_KIND.UNRESOLVED;
  const primaryCand = discoveryRow.primaryCandidate;
  const backups = [...(discoveryRow.backupCandidates || [])];

  if (!primaryCand || primaryKind === PRIMARY_KIND.UNRESOLVED) {
    return discoveryRow;
  }

  if (primaryKind === PRIMARY_KIND.FUNCTIONAL_ENTITY || primaryCand.functionalEntity) {
    return discoveryRow;
  }

  if (isNamedPersonCandidate(primaryCand)) {
    return discoveryRow;
  }

  const namedBackupIdx = backups.findIndex((b) => isNamedPersonCandidate(b));
  if (namedBackupIdx >= 0) {
    const named = backups[namedBackupIdx];
    const rest = backups.filter((_, i) => i !== namedBackupIdx);
    return {
      ...discoveryRow,
      primaryKind: PRIMARY_KIND.NAMED_PERSON,
      primaryCandidate: named,
      backupCandidates: [
        { ...primaryCand, functionalEntity: true, backupPreferred: true },
        ...rest,
      ],
    };
  }

  return {
    ...discoveryRow,
    primaryKind: PRIMARY_KIND.FUNCTIONAL_ENTITY,
    primaryCandidate: { ...primaryCand, functionalEntity: true },
  };
}

function toContactFromCandidate(candidate = {}, opportunity = {}) {
  const enriched = enrichContactRecord(
    {
      name: candidate.name,
      role: candidate.role || candidate.title,
      title: candidate.role || candidate.title,
      organization: candidate.organization,
      email: candidate.email || null,
      phone: candidate.phone || null,
      sourceUrl: candidate.sourceUrl || candidate.source || null,
      source: candidate.sourceUrl || candidate.source || null,
      relationshipToEvent: candidate.whyThisPerson || candidate.eventRelationship,
      gdiContactRole: candidate.gdiContactRole,
      targetRoleMatch: candidate.targetRoleMatch,
      functionalEntity: Boolean(candidate.functionalEntity),
      contactQuality: candidate.functionalEntity
        ? CONTACT_QUALITY.HOUSING_SOURCING_CONTACT
        : hasNamedPerson(candidate)
          ? CONTACT_QUALITY.NAMED_EVENT_MEETINGS_CONTACT
          : CONTACT_QUALITY.GENERAL_ORGANIZATION_CONTACT,
      whyThisContact: candidate.whyThisPerson || null,
      employmentStatus: candidate.employmentStatus,
      eventRelationship: candidate.eventRelationship,
      whoCurrentness: mapDiscoveryToCurrentness(candidate),
      whoEventRelation: mapDiscoveryToEventRelation(candidate),
      whoEvidenceQuote: candidate.evidenceQuote || null,
      whoDiscoveredAt: candidate.lastVerifiedAt || new Date().toISOString().slice(0, 10),
    },
    opportunity
  );
  return enriched;
}

/**
 * Apply one discovery row to an opportunity.
 * @returns {{ opportunity, changed, researchState, primaryKind, note }}
 */
export function applyWhoDiscoveryToOpportunity(opportunity, discoveryRow = {}) {
  const normalized = normalizeDiscoveryRowForApply(discoveryRow);
  const before = opportunity.primaryContact ? { ...opportunity.primaryContact } : null;
  const beforeBackups = [...(opportunity.backupContacts || [])];
  const primaryKind = normalized.primaryKind || PRIMARY_KIND.UNRESOLVED;
  const primaryCand = normalized.primaryCandidate;
  const backups = normalized.backupCandidates || [];

  if (!primaryCand || primaryKind === PRIMARY_KIND.UNRESOLVED) {
    const researched =
      normalized.researched === true || normalized.passId
        ? WHO_RESEARCH_STATE.NO_WHO_RESEARCHED
        : WHO_RESEARCH_STATE.NO_WHO_UNRESEARCHED;
    const functionalResearched =
      before && isFunctionalLike(before)
        ? WHO_RESEARCH_STATE.FUNCTIONAL_CONTACT_ONLY_RESEARCHED
        : researched;
    return {
      opportunity: {
        ...opportunity,
        whoResearchState: before ? functionalResearched : researched,
        whoResearchPassId: normalized.passId || null,
        whoResearchNote:
          normalized.unresolvedReason ||
          "No supportable named WHO after Dealality research",
      },
      changed: false,
      researchState: before ? functionalResearched : researched,
      primaryKind,
      note: "no_named_upgrade",
    };
  }

  // Functional entity primary (housing vendor) — upgrade functional quality, not named person
  if (primaryKind === PRIMARY_KIND.FUNCTIONAL_ENTITY || primaryCand.functionalEntity) {
    const nextPrimary = toContactFromCandidate(primaryCand, opportunity);
    const nextBackups = [...beforeBackups];
    for (const b of backups) {
      if (b.backupPreferred === false) continue;
      const bc = toContactFromCandidate(b, opportunity);
      if (!nextBackups.some((x) => contactKey(x) === contactKey(bc))) {
        nextBackups.push({
          ...bc,
          contactKind: isNamedPersonCandidate(b) ? "BACKUP" : "FUNCTIONAL_BACKUP",
        });
      }
    }
    // Prefer promoting a named backup into primary if present (precision WHO)
    const namedInBackups = nextBackups.find((b) => hasNamedPerson(b));
    if (namedInBackups && isLikelyPersonName(namedInBackups.name)) {
      return applyWhoDiscoveryToOpportunity(opportunity, {
        ...normalized,
        primaryKind: PRIMARY_KIND.NAMED_PERSON,
        primaryCandidate: {
          ...namedInBackups,
          functionalEntity: false,
          whyThisPerson: namedInBackups.whyThisContact || namedInBackups.whyThisPerson,
        },
        backupCandidates: [
          { ...primaryCand, functionalEntity: true },
          ...backups.filter((b) => contactKey(b) !== contactKey(namedInBackups)),
        ],
      });
    }
    const graded = gradeContact(nextPrimary, opportunity);
    return {
      opportunity: {
        ...opportunity,
        primaryContact: { ...nextPrimary, contactKind: "FUNCTIONAL" },
        backupContacts: nextBackups.slice(0, 3),
        contactQuality: nextPrimary.contactQuality,
        contactGrade: graded.contactGrade,
        contactGradeLabel: graded.contactGradeLabel,
        whyThisContact: nextPrimary.whyThisContact,
        whoResearchState: WHO_RESEARCH_STATE.FUNCTIONAL_CONTACT_ONLY_RESEARCHED,
        whoResearchPassId: normalized.passId || null,
        whoPrimaryReason: primaryCand.whyThisPerson || primaryCand.evidenceQuote,
      },
      changed: true,
      researchState: WHO_RESEARCH_STATE.FUNCTIONAL_CONTACT_ONLY_RESEARCHED,
      primaryKind: PRIMARY_KIND.FUNCTIONAL_ENTITY,
      note: "functional_housing_or_entity_primary",
    };
  }

  // Named person
  const gate = acceptNamedWhoGate(primaryCand);
  const nextPrimary = toContactFromCandidate(primaryCand, opportunity);
  if (!hasNamedPerson(nextPrimary) || !isLikelyPersonName(nextPrimary.name)) {
    return {
      opportunity: {
        ...opportunity,
        whoResearchState: WHO_RESEARCH_STATE.FUNCTIONAL_CONTACT_ONLY_RESEARCHED,
        whoResearchNote: "candidate_failed_named_person_check",
      },
      changed: false,
      researchState: WHO_RESEARCH_STATE.FUNCTIONAL_CONTACT_ONLY_RESEARCHED,
      primaryKind,
      note: "rejected_not_named",
    };
  }

  // Never replace existing A-grade named with weaker
  const beforeGrade = before ? gradeContact(before, opportunity).contactGrade : null;
  if (
    before &&
    hasNamedPerson(before) &&
    beforeGrade === "A" &&
    gate.researchState !== WHO_RESEARCH_STATE.NAMED_PERSON_CONFIRMED
  ) {
    return {
      opportunity: {
        ...opportunity,
        whoResearchState: WHO_RESEARCH_STATE.NAMED_PERSON_CONFIRMED,
        whoResearchNote: "preserved_existing_A_grade",
      },
      changed: false,
      researchState: WHO_RESEARCH_STATE.NAMED_PERSON_CONFIRMED,
      primaryKind: PRIMARY_KIND.NAMED_PERSON,
      note: "regression_guard_a_grade",
    };
  }

  // Never demote an already-confirmed named WHO to needs-validation on re-run
  if (
    before &&
    hasNamedPerson(before) &&
    opportunity.whoResearchState === WHO_RESEARCH_STATE.NAMED_PERSON_CONFIRMED &&
    gate.researchState === WHO_RESEARCH_STATE.NAMED_PERSON_NEEDS_VALIDATION &&
    contactKey(before) === contactKey(nextPrimary)
  ) {
    return {
      opportunity: {
        ...opportunity,
        whoResearchState: WHO_RESEARCH_STATE.NAMED_PERSON_CONFIRMED,
        whoResearchNote: "preserved_confirmed_named_who",
      },
      changed: false,
      researchState: WHO_RESEARCH_STATE.NAMED_PERSON_CONFIRMED,
      primaryKind: PRIMARY_KIND.NAMED_PERSON,
      note: "regression_guard_confirmed",
    };
  }

  const nextBackups = [];
  // Demote prior functional to backup
  if (before && isFunctionalLike(before) && contactKey(before) !== contactKey(nextPrimary)) {
    nextBackups.push({
      ...before,
      contactKind: "FUNCTIONAL_BACKUP",
      whyThisContact: before.whyThisContact || "Prior functional contact retained as backup",
    });
  } else if (before && hasNamedPerson(before) && contactKey(before) !== contactKey(nextPrimary)) {
    nextBackups.push({ ...before, contactKind: "BACKUP" });
  }
  for (const x of beforeBackups) {
    if (!nextBackups.some((b) => contactKey(b) === contactKey(x))) {
      nextBackups.push(x);
    }
  }
  for (const b of backups) {
    const bc = toContactFromCandidate(b, opportunity);
    if (!nextBackups.some((x) => contactKey(x) === contactKey(bc))) {
      nextBackups.push({
        ...bc,
        contactKind: b.functionalEntity || !hasNamedPerson(bc) ? "FUNCTIONAL_BACKUP" : "BACKUP",
      });
    }
  }

  const cq = gate.ok
    ? CONTACT_QUALITY.NAMED_EVENT_MEETINGS_CONTACT
    : CONTACT_QUALITY.GENERAL_ORGANIZATION_CONTACT;
  nextPrimary.contactQuality = cq;
  nextPrimary.contactKind = "PRIMARY";
  const graded = gradeContact(nextPrimary, opportunity);

  return {
    opportunity: {
      ...opportunity,
      primaryContact: nextPrimary,
      backupContacts: nextBackups.slice(0, 3),
      contactQuality: cq,
      contactGrade: graded.contactGrade,
      contactGradeLabel: graded.contactGradeLabel,
      whyThisContact: nextPrimary.whyThisContact,
      whoResearchState: gate.researchState,
      whoResearchPassId: normalized.passId || null,
      whoPrimaryReason: primaryCand.whyThisPerson || primaryCand.evidenceQuote,
      whoAcceptanceGate: gate.reason,
    },
    changed: true,
    researchState: gate.researchState,
    primaryKind: PRIMARY_KIND.NAMED_PERSON,
    note: gate.ok ? "named_confirmed" : "named_needs_validation",
  };
}

/**
 * Apply discovery rows map (opportunityId → discovery result) to opportunity list.
 */
export function applyWhoDiscoveryBatch(opportunities = [], discoveryByOppId = {}) {
  const out = [];
  const stats = {
    changed: 0,
    namedConfirmed: 0,
    namedNeedsValidation: 0,
    functionalResearched: 0,
    noWhoResearched: 0,
  };

  for (const o of opportunities) {
    const row = discoveryByOppId[o.id];
    if (!row) {
      out.push(o);
      continue;
    }
    const applied = applyWhoDiscoveryToOpportunity(o, row);
    out.push(applied.opportunity);
    if (applied.changed) stats.changed += 1;
    if (applied.researchState === WHO_RESEARCH_STATE.NAMED_PERSON_CONFIRMED) {
      stats.namedConfirmed += 1;
    } else if (applied.researchState === WHO_RESEARCH_STATE.NAMED_PERSON_NEEDS_VALIDATION) {
      stats.namedNeedsValidation += 1;
    } else if (
      applied.researchState === WHO_RESEARCH_STATE.FUNCTIONAL_CONTACT_ONLY_RESEARCHED
    ) {
      stats.functionalResearched += 1;
    } else if (applied.researchState === WHO_RESEARCH_STATE.NO_WHO_RESEARCHED) {
      stats.noWhoResearched += 1;
    }
  }

  return { opportunities: out, stats };
}
