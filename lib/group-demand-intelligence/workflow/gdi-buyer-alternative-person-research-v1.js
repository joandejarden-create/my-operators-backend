/**
 * Reconnect original alternative-person contact research into Full E2E.
 *
 * DOES NOT invent a new contact engine. Orchestrates existing modules:
 *   - BUYER_RESEARCH_STRATEGY_SEQUENCE / selectNextBuyerResearchStrategy
 *   - preferredRolesForOpportunity (ROLE_PRIORITY_BY_FAMILY)
 *   - selectBestCommercialContacts / projectSelectionOntoOpportunity
 *   - selectSecondaryNamedContact / attachSecondaryNamedContact
 *   - evaluateNamedContactCoverageStandard
 *   - applyContactLineageReuse (optional prior index)
 *
 * Stage: BUYER_ALTERNATIVE_PERSON_RESEARCH
 * Must not stop after a single unsuccessful buyer-WHO pass.
 */

import {
  BUYER_RESEARCH_STRATEGY_SEQUENCE,
  selectNextBuyerResearchStrategy,
} from "../scaling-buyer-research-strategies-v1.js";

export { BUYER_RESEARCH_STRATEGY_SEQUENCE, selectNextBuyerResearchStrategy };
import { preferredRolesForOpportunity } from "../contact-completeness-v1.js";
import {
  selectBestCommercialContacts,
  projectSelectionOntoOpportunity,
} from "../presentation/gdi-commercial-contact-selection-v1.js";
import {
  COMMERCIAL_CONTACT_CLASS,
  inferEvidenceHintsFromText,
} from "../presentation/gdi-commercial-contact-classification-v1.js";
import {
  selectSecondaryNamedContact,
  attachSecondaryNamedContact,
} from "../secondary-named-contact-v1.js";
import {
  evaluateNamedContactCoverageStandard,
  attachNamedContactCoverageMetadata,
  SECONDARY_CONTACT_RESEARCH_STATUS,
} from "./gdi-named-contact-coverage-standard-v1.js";
import { isPlausibleBuyerPersonName } from "./gdi-original-full-e2e-execution-restore-v1.js";
import { applyContactLineageReuse } from "./gdi-opportunity-contact-lineage-reuse-v1.js";

export const GDI_BUYER_ALTERNATIVE_PERSON_RESEARCH_V1 =
  "gdi-buyer-alternative-person-research-v1";

export const BUYER_ALTERNATIVE_PERSON_STAGE =
  "BUYER_ALTERNATIVE_PERSON_RESEARCH";

export const PERSON_KIND = Object.freeze({
  DIRECT_BUYER: "DIRECT_BUYER",
  VERIFIED_VIABLE_ALTERNATIVE: "VERIFIED_VIABLE_ALTERNATIVE",
  SECONDARY_CONTACT: "SECONDARY_CONTACT",
  UNVERIFIED_PERSON: "UNVERIFIED_PERSON",
  ORGANIZATIONAL_ROUTE: "ORGANIZATIONAL_ROUTE",
});

export const ALTERNATIVE_PERSON_REQUIRED_WORKFLOWS = Object.freeze([
  "new_hotel_full_e2e",
  "existing_hotel_full_e2e",
  "portfolio_full_e2e",
  "leisure_demand_full_e2e",
  "resume_full_e2e",
  "replay_full_e2e",
  "cohort_execute",
]);

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

function now() {
  return new Date().toISOString();
}

function nameKey(n) {
  return text(n)
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
}

function looksLikePerson(name, opts = {}) {
  if (typeof opts.looksLikePersonName === "function") {
    return opts.looksLikePersonName(name);
  }
  // Prefer strict buyer-person gate (rejects place/chrome junk). Do not OR with
  // looser looksLikePersonName — that re-admits "Family Travel" / "Historic Center".
  return isPlausibleBuyerPersonName(name);
}

/**
 * Harvest candidate people from opportunity evidence; drop junk chrome names.
 * Retain provenance; never invent people.
 */
export function harvestAlternativePersonCandidates(opportunity = {}, opts = {}) {
  const rejected = [];
  const accepted = [];
  const raw = [];

  const push = (p, src) => {
    if (!p) return;
    const name = text(p.name || p.fullName || p.person);
    if (!name) return;
    raw.push({ ...p, name, _src: src });
  };

  push(opportunity.primaryContact, "primaryContact");
  push(opportunity.secondaryContact, "secondaryContact");
  for (const p of opportunity.namedStakeholders || []) push(p, "namedStakeholders");
  for (const p of opportunity.namedStakeholdersFound || []) {
    push(p, "namedStakeholdersFound");
  }
  for (const p of opportunity.contacts || []) push(p, "contacts");
  for (const p of opportunity.buyerCandidates || []) push(p, "buyerCandidates");
  for (const p of opportunity.whoContacts || []) push(p, "whoContacts");

  const seen = new Set();
  for (const p of raw) {
    const key = nameKey(p.name);
    if (!key || seen.has(key)) continue;
    seen.add(key);
    if (!looksLikePerson(p.name, opts)) {
      rejected.push({
        name: p.name,
        title: text(p.title || p.role),
        reason: "JUNK_OR_NON_PERSON_NAME",
        source: p._src,
        sourceUrl: p.sourceUrl || null,
      });
      continue;
    }
    accepted.push({
      name: p.name,
      title: text(p.title || p.role) || null,
      role: text(p.role || p.title) || null,
      organization:
        text(p.organization || p.organizationName || p.currentOrganization) ||
        text(opportunity.organizationName) ||
        null,
      email: p.email || p.workEmail || null,
      phone: p.phone || null,
      sourceUrl: p.sourceUrl || p.channelSourceUrl || null,
      contactClass: p.contactClass || p.contactClassification || null,
      whyRelevant: p.whyRelevant || p.whyThisContact || null,
      contactDataOrigin: p.contactDataOrigin || "SAVED_EVIDENCE",
      _src: p._src,
      commercialEvidence: buildEvidenceForCandidate(p, opportunity),
    });
  }

  return { accepted, rejected, rawCount: raw.length };
}

function buildEvidenceForCandidate(person = {}, opportunity = {}) {
  const { hints } = inferEvidenceHintsFromText(person, opportunity);
  const title = text(person.title || person.role);
  const orgOk = Boolean(
    text(person.organization || person.organizationName) ||
      text(opportunity.organizationName),
  );
  const roleRelevant =
    hints.titleSuggestsMeetings ||
    hints.titleSuggestsHousing ||
    hints.titleSuggestsTravel ||
    hints.titleSuggestsProcurement ||
    hints.titleSuggestsSales ||
    hints.eventOrganizerSales ||
    /director|manager|coordinator|planner|organizer|sourcing|procurement|events|meetings|housing|travel|operations|partner/i.test(
      title,
    );

  return {
    connectedToDemandActivity: roleRelevant,
    currentOrganizationAndRole: orgOk && Boolean(title),
    relevantOperationalOrCommercialInfluence: roleRelevant,
    credibleReferralRoute: roleRelevant,
    confirmedHotelSourcingResponsibility: false,
    confirmedMeetingOrVenueBuyingResponsibility: hints.titleSuggestsMeetings,
    confirmedTravelProcurementResponsibility: hints.titleSuggestsTravel,
    confirmedHousingAccommodationResponsibility: hints.titleSuggestsHousing,
    supportingSourceUrls: [person.sourceUrl].filter(Boolean),
    unverifiedResponsibilityGaps: roleRelevant
      ? ["hotel_sourcing_authority_not_fully_confirmed"]
      : ["commercial_relevance_not_established"],
    evidenceNotes: roleRelevant
      ? "Title/role suggests commercially relevant pathway; not auto-certified as direct buyer"
      : "Named person without clear commercial role signal",
  };
}

function personKindFromClass(klass) {
  if (
    klass === COMMERCIAL_CONTACT_CLASS.DIRECT_HOTEL_BUYER ||
    klass === COMMERCIAL_CONTACT_CLASS.VERIFIED_MEETINGS_OR_TRAVEL_BUYER
  ) {
    return PERSON_KIND.DIRECT_BUYER;
  }
  if (klass === COMMERCIAL_CONTACT_CLASS.RELEVANT_COMMERCIAL_ENTRY_POINT) {
    return PERSON_KIND.VERIFIED_VIABLE_ALTERNATIVE;
  }
  if (
    klass === COMMERCIAL_CONTACT_CLASS.ORGANIZATIONAL_HOUSING_OR_MEETINGS_ROUTE ||
    klass === COMMERCIAL_CONTACT_CLASS.GENERAL_BUSINESS_ROUTE
  ) {
    return PERSON_KIND.ORGANIZATIONAL_ROUTE;
  }
  return PERSON_KIND.UNVERIFIED_PERSON;
}

function qualifyingNamedSelection(selection) {
  const primary = selection?.primary;
  if (!primary?.name) return false;
  if (
    primary.classification ===
      COMMERCIAL_CONTACT_CLASS.ORGANIZATIONAL_HOUSING_OR_MEETINGS_ROUTE ||
    primary.classification === COMMERCIAL_CONTACT_CLASS.GENERAL_BUSINESS_ROUTE
  ) {
    return false;
  }
  // Viable alternative or buyer or authority-unverified with role evidence
  return (
    primary.classification === COMMERCIAL_CONTACT_CLASS.DIRECT_HOTEL_BUYER ||
    primary.classification ===
      COMMERCIAL_CONTACT_CLASS.VERIFIED_MEETINGS_OR_TRAVEL_BUYER ||
    primary.classification ===
      COMMERCIAL_CONTACT_CLASS.RELEVANT_COMMERCIAL_ENTRY_POINT ||
    (primary.classification === COMMERCIAL_CONTACT_CLASS.AUTHORITY_UNVERIFIED &&
      Number(primary.commercialRelevanceScore || 0) >= 20)
  );
}

function strategyMatchesCandidate(strategy, person) {
  const blob = `${person.title || ""} ${person.role || ""} ${person.whyRelevant || ""}`.toLowerCase();
  const hints = strategy?.queryHints || [];
  if (!hints.length) return true;
  return hints.some((h) => {
    const token = String(h || "")
      .toLowerCase()
      .replace(/[^a-z0-9\s]/g, " ")
      .trim();
    if (!token) return false;
    return token.split(/\s+/).some((w) => w.length >= 4 && blob.includes(w));
  });
}

/**
 * Run one Full E2E alternative-person research pass ($0 by default).
 *
 * @param {object} opportunity
 * @param {object} [opts]
 * @param {Function} [opts.researcher] — optional; when absent, ranks saved evidence only
 * @param {Map} [opts.priorContactIndex] — lineage reuse
 * @param {number} [opts.maxStrategies]
 * @param {boolean} [opts.authorizePaidResearch] — must be true to invoke researcher
 */
export async function runBuyerAlternativePersonResearch(
  opportunity = {},
  opts = {},
) {
  const started = Date.now();
  let working = { ...opportunity };
  const attempts = Array.isArray(working.buyerResearchAttempts)
    ? [...working.buyerResearchAttempts]
    : [];
  const rolePrefs = preferredRolesForOpportunity(working);
  const trace = {
    stage: BUYER_ALTERNATIVE_PERSON_STAGE,
    version: GDI_BUYER_ALTERNATIVE_PERSON_RESEARCH_V1,
    preferredRoles: rolePrefs.preferredRoles,
    commercialMotion: rolePrefs.commercialMotion,
    strategiesAttempted: [],
    ladderExhausted: false,
    rankingRan: false,
    secondaryResearchRan: false,
    lineageReuse: null,
    rejectedCandidates: [],
    researcherInvoked: false,
    findymail: false,
    surfe: false,
  };

  // Optional identity reuse before ladder
  if (opts.priorContactIndex) {
    const reuse = applyContactLineageReuse(working, opts.priorContactIndex);
    working = reuse.opportunity;
    trace.lineageReuse = {
      reused: reuse.reused === true,
      reason: reuse.reason || null,
    };
  }

  const coverageBefore = evaluateNamedContactCoverageStandard(working, opts);
  const idealPresent = coverageBefore.namedCount >= 1;

  // Always harvest + rank saved evidence (junk-tolerant: keep genuine people)
  let harvest = harvestAlternativePersonCandidates(working, opts);
  trace.rejectedCandidates.push(...harvest.rejected);

  let selection = selectBestCommercialContacts(harvest.accepted, {
    organizationName: working.organizationName,
  });
  trace.rankingRan = true;

  // Rank saved evidence first — retain genuine people among junk.
  if (qualifyingNamedSelection(selection)) {
    const projected = projectSelectionOntoOpportunity(working, selection);
    working = projected.opportunity;
  }

  // When no qualifying named person yet: walk strategy ladder (do not stop after one WHO miss).
  if (!qualifyingNamedSelection(selection)) {
    const maxStrategies = Math.max(
      1,
      Number(opts.maxStrategies || BUYER_RESEARCH_STRATEGY_SEQUENCE.length),
    );
    let guard = 0;
    while (guard < maxStrategies) {
      guard += 1;
      const pick = selectNextBuyerResearchStrategy({
        gaps: ["missing_buyer"],
        attempts,
      });
      if (pick.exhausted || !pick.nextStrategy) {
        trace.ladderExhausted = true;
        break;
      }
      const strategy = pick.nextStrategy;
      const attempt = {
        strategyId: strategy.id,
        label: strategy.label,
        at: now(),
        queryHints: strategy.queryHints || [],
      };

      let newPeople = [];
      if (
        opts.authorizePaidResearch === true &&
        typeof opts.researcher?.execute === "function"
      ) {
        trace.researcherInvoked = true;
        const researchResult = await opts.researcher.execute({
          opportunity: working,
          strategy,
          action: {
            strategy: strategy.id,
            queries: (strategy.queryHints || []).map(
              (h) =>
                `"${working.organizationName || "organization"}" ${h}`,
            ),
          },
        });
        newPeople = [
          ...(researchResult?.namedStakeholders || []),
          ...(researchResult?.people || []),
          ...(researchResult?.deepPatch?.namedStakeholdersFound || []),
        ];
        attempt.researcherPeople = newPeople.length;
      } else {
        // $0 path: filter existing harvest against strategy hints; document attempt
        newPeople = harvest.accepted.filter((p) =>
          strategyMatchesCandidate(strategy, p),
        );
        attempt.savedEvidenceMatches = newPeople.length;
        attempt.mode = "SAVED_EVIDENCE_FILTER";
      }

      attempts.push(attempt);
      trace.strategiesAttempted.push(attempt);

      if (newPeople.length) {
        working = {
          ...working,
          namedStakeholders: [
            ...(working.namedStakeholders || []),
            ...newPeople,
          ],
        };
        harvest = harvestAlternativePersonCandidates(working, opts);
        trace.rejectedCandidates = harvest.rejected;
        selection = selectBestCommercialContacts(harvest.accepted, {
          organizationName: working.organizationName,
        });
        if (qualifyingNamedSelection(selection)) {
          const projected = projectSelectionOntoOpportunity(working, selection);
          working = projected.opportunity;
          attempt.selected = selection.primary?.name || null;
          attempt.personKind = personKindFromClass(
            selection.primary?.classification,
          );
          break;
        }
      }

      attempt.selected = null;
      attempt.reason = "NO_QUALIFYING_NAMED_PERSON_THIS_STRATEGY";
    }

    if (!qualifyingNamedSelection(selection)) {
      const exhausted = selectNextBuyerResearchStrategy({
        gaps: ["missing_buyer"],
        attempts,
      });
      trace.ladderExhausted = exhausted.exhausted === true;
    }
  } else if (!idealPresent) {
    // Qualifying person came from ranking saved evidence (viable alternative path).
    // Record that ranking satisfied WHO without further paid ladder steps.
    attempts.push({
      strategyId: "SAVED_EVIDENCE_COMMERCIAL_RANKING",
      label: "Commercial ranking of saved evidence candidates",
      at: now(),
      selected: selection.primary?.name || null,
      personKind: personKindFromClass(selection.primary?.classification),
      mode: "SAVED_EVIDENCE_RANKING",
    });
  }

  // Secondary person research (existing helper)
  const beforeSecondary = text(working.secondaryContact?.name);
  working = attachSecondaryNamedContact(working, {
    allowSavedEvidence: opts.allowSavedEvidence !== false,
  });
  // Prefer ranking backup when secondary still missing
  if (
    !text(working.secondaryContact?.name) &&
    selection?.relevantBackup?.name &&
    nameKey(selection.relevantBackup.name) !==
      nameKey(working.primaryContact?.name)
  ) {
    const secondary = selectSecondaryNamedContact(working, [
      {
        name: selection.relevantBackup.name,
        title: selection.relevantBackup.title,
        organization: selection.relevantBackup.organization,
        email: selection.relevantBackup.email,
        sourceUrl: selection.relevantBackup.sourceUrl,
        contactClassification: selection.relevantBackup.classification,
        whyRelevant: selection.relevantBackup.salesFriendlyWhy,
      },
    ]);
    if (secondary) working = { ...working, secondaryContact: secondary };
  }
  trace.secondaryResearchRan = true;
  trace.secondaryFound =
    Boolean(text(working.secondaryContact?.name)) &&
    nameKey(working.secondaryContact?.name) !==
      nameKey(working.primaryContact?.name);

  working.buyerResearchAttempts = attempts;
  working = attachNamedContactCoverageMetadata(working, {
    secondContactAttempted: true,
    secondContactFound: trace.secondaryFound === true,
  });

  const coverage = evaluateNamedContactCoverageStandard(working, opts);
  const primaryClass =
    working.primaryContact?.contactClass ||
    working.primaryContact?.contactClassification ||
    selection?.primary?.classification ||
    null;
  const primaryKind = primaryClass
    ? personKindFromClass(primaryClass)
    : coverage.namedCount >= 1
      ? PERSON_KIND.VERIFIED_VIABLE_ALTERNATIVE
      : null;

  if (working.primaryContact && primaryKind) {
    working.primaryContact = {
      ...working.primaryContact,
      personKind: primaryKind,
    };
  }
  if (working.secondaryContact?.name) {
    working.secondaryContact = {
      ...working.secondaryContact,
      personKind: PERSON_KIND.SECONDARY_CONTACT,
    };
  }

  working.buyerAlternativePersonResearch = {
    version: GDI_BUYER_ALTERNATIVE_PERSON_RESEARCH_V1,
    at: now(),
    strategiesAttempted: attempts.map((a) => a.strategyId),
    ladderExhausted: trace.ladderExhausted,
    rankingRan: true,
    primaryKind,
    secondaryFound: trace.secondaryFound === true,
    coverage: coverage.coverage,
    namedCount: coverage.namedCount,
    rejectedCount: harvest.rejected.length,
    runtimeMs: Date.now() - started,
  };

  const executed =
    attempts.length > 0 ||
    harvest.accepted.length > 0 ||
    idealPresent ||
    trace.rankingRan;

  return {
    opportunity: working,
    executed,
    skipped: false,
    stage: BUYER_ALTERNATIVE_PERSON_STAGE,
    trace: {
      ...trace,
      strategiesAttempted: attempts,
      primaryName: working.primaryContact?.name || null,
      secondaryName: working.secondaryContact?.name || null,
      primaryKind,
      namedCount: coverage.namedCount,
      secondaryResearchStatus: coverage.secondaryResearchStatus,
      beforeSecondary: beforeSecondary || null,
      coverageDisposition: coverage.disposition,
      at: now(),
      runtimeMs: Date.now() - started,
    },
    selection,
    coverage,
  };
}

/**
 * Assert Full E2E stages include alternative-person research when required.
 */
export function assertBuyerAlternativePersonStage(stages = [], opts = {}) {
  const list = Array.isArray(stages) ? stages : [];
  const alt = list.find((s) => s?.stage === BUYER_ALTERNATIVE_PERSON_STAGE);
  if (!alt) {
    return {
      ok: false,
      code: "ALTERNATIVE_PERSON_STAGE_MISSING",
      reason: `${BUYER_ALTERNATIVE_PERSON_STAGE} not recorded in Full E2E stages`,
    };
  }
  if (opts.requireExecuted === true && alt.executed !== true) {
    return {
      ok: false,
      code: "ALTERNATIVE_PERSON_NOT_EXECUTED",
      reason: alt.reason || "Alternative-person stage present but not executed",
      stage: alt,
    };
  }
  if (
    opts.requireLadderWhenWhoIncomplete === true &&
    opts.whoIncomplete === true &&
    !(alt.strategiesAttempted > 0 || (alt.strategies || []).length > 0)
  ) {
    return {
      ok: false,
      code: "LADDER_NOT_ATTEMPTED",
      reason:
        "WHO incomplete but strategy ladder recorded zero attempts — must not stop after first WHO miss",
      stage: alt,
    };
  }

  const whoIdx = Math.max(
    list.findIndex((s) => s?.stage === "BUYER_WHO_RESEARCH"),
    list.findIndex((s) => s?.stage === "CONTACT_RESEARCH"),
  );
  const altIdx = list.findIndex(
    (s) => s?.stage === BUYER_ALTERNATIVE_PERSON_STAGE,
  );
  const howIdx = list.findIndex(
    (s) => s?.stage === "HOW_CONTACT_CHANNEL_DISCOVERY",
  );
  if (whoIdx >= 0 && altIdx >= 0 && altIdx < whoIdx) {
    return {
      ok: false,
      code: "ALTERNATIVE_BEFORE_WHO",
      reason: "Alternative-person research must follow WHO / contact research",
    };
  }
  if (howIdx >= 0 && altIdx >= 0 && howIdx < altIdx) {
    return {
      ok: false,
      code: "HOW_BEFORE_ALTERNATIVE",
      reason: "HOW must run after alternative-person ranking when both present",
    };
  }
  return { ok: true, code: "ALTERNATIVE_PERSON_STAGE_OK", stage: alt };
}

export default {
  GDI_BUYER_ALTERNATIVE_PERSON_RESEARCH_V1,
  BUYER_ALTERNATIVE_PERSON_STAGE,
  PERSON_KIND,
  ALTERNATIVE_PERSON_REQUIRED_WORKFLOWS,
  harvestAlternativePersonCandidates,
  runBuyerAlternativePersonResearch,
  assertBuyerAlternativePersonStage,
  BUYER_RESEARCH_STRATEGY_SEQUENCE,
};
