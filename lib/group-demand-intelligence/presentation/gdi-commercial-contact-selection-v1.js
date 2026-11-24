/**
 * Select best commercial contact — relevance first, channels second.
 * Never silently replace a verified decision-maker with an easier-to-email person.
 */
import {
  COMMERCIAL_CONTACT_CLASS,
  classifyCommercialContact,
} from "./gdi-commercial-contact-classification-v1.js";
import { scoreCommercialContactCandidate } from "./gdi-commercial-contact-scoring-v1.js";

export const GDI_COMMERCIAL_CONTACT_SELECTION_V1 =
  "gdi-commercial-contact-selection-v1";

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

function nameKey(n) {
  return text(n)
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
}

function hasEmail(p) {
  return Boolean(text(p.email || p.workEmail).includes("@"));
}

function rankClass(klass) {
  switch (klass) {
    case COMMERCIAL_CONTACT_CLASS.DIRECT_HOTEL_BUYER:
      return 100;
    case COMMERCIAL_CONTACT_CLASS.VERIFIED_MEETINGS_OR_TRAVEL_BUYER:
      return 90;
    case COMMERCIAL_CONTACT_CLASS.RELEVANT_COMMERCIAL_ENTRY_POINT:
      return 70;
    case COMMERCIAL_CONTACT_CLASS.ORGANIZATIONAL_HOUSING_OR_MEETINGS_ROUTE:
      return 50;
    case COMMERCIAL_CONTACT_CLASS.GENERAL_BUSINESS_ROUTE:
      return 30;
    default:
      return 10;
  }
}

/**
 * @param {object[]} candidates — person objects (+ optional commercialEvidence)
 * @param {object} [opts]
 */
export function selectBestCommercialContacts(candidates = [], opts = {}) {
  const scored = (Array.isArray(candidates) ? candidates : [])
    .filter((c) => text(c.name || c.fullName) || opts.allowOrgRoutes)
    .map((c) => {
      const s = scoreCommercialContactCandidate(c, {
        evidence: c.commercialEvidence,
        organizationName: opts.organizationName,
        channels: {
          email: c.email || c.workEmail,
          phone: c.phone || c.directPhone,
          sourceUrl: c.channelSourceUrl || c.sourceUrl,
          verifiedAt: c.channelLastVerifiedAt || c.verifiedAt,
          contactDataOrigin: c.contactDataOrigin,
          profileUrl: c.officialContactUrl || c.profileUrl,
        },
      });
      return { person: c, ...s };
    });

  // Sort: buyer authority > commercial relevance > class rank > channel recovery
  scored.sort((a, b) => {
    const aBuy = a.classification.buyerAuthorityClaimed ? 1 : 0;
    const bBuy = b.classification.buyerAuthorityClaimed ? 1 : 0;
    if (bBuy !== aBuy) return bBuy - aBuy;
    if (b.commercialRelevance.score !== a.commercialRelevance.score) {
      return b.commercialRelevance.score - a.commercialRelevance.score;
    }
    const classDiff =
      rankClass(b.classification.class) - rankClass(a.classification.class);
    if (classDiff) return classDiff;
    return b.channelRecovery.score - a.channelRecovery.score;
  });

  const bestBuyer =
    scored.find((s) => s.classification.buyerAuthorityClaimed) || null;

  // Primary = best buyer if any; else best relevant entry / scored head
  let primary = bestBuyer || scored[0] || null;

  // If verified buyer has no email, keep as best buyer candidate and pick
  // reachable entry point separately — do NOT replace silently.
  let bestAccessibleEntry = null;
  if (bestBuyer && !hasEmail(bestBuyer.person)) {
    bestAccessibleEntry =
      scored.find(
        (s) =>
          nameKey(s.person.name) !== nameKey(bestBuyer.person.name) &&
          hasEmail(s.person) &&
          (s.classification.class ===
            COMMERCIAL_CONTACT_CLASS.RELEVANT_COMMERCIAL_ENTRY_POINT ||
            s.classification.class ===
              COMMERCIAL_CONTACT_CLASS.ORGANIZATIONAL_HOUSING_OR_MEETINGS_ROUTE ||
            s.classification.buyerAuthorityClaimed),
      ) ||
      scored.find(
        (s) =>
          nameKey(s.person.name) !== nameKey(bestBuyer.person.name) &&
          hasEmail(s.person),
      ) ||
      null;
    primary = bestBuyer;
  } else if (!bestBuyer) {
    bestAccessibleEntry = scored.find((s) => hasEmail(s.person)) || primary;
  }

  const backup =
    scored.find((s) => {
      if (!primary) return true;
      return nameKey(s.person.name) !== nameKey(primary.person.name);
    }) || null;

  const orgRoute =
    scored.find(
      (s) =>
        s.classification.class ===
          COMMERCIAL_CONTACT_CLASS.ORGANIZATIONAL_HOUSING_OR_MEETINGS_ROUTE ||
        s.classification.class ===
          COMMERCIAL_CONTACT_CLASS.GENERAL_BUSINESS_ROUTE,
    ) || null;

  return {
    version: GDI_COMMERCIAL_CONTACT_SELECTION_V1,
    primary: primary
      ? {
          role: "BEST_DIRECT_OR_PRIMARY",
          ...summarize(primary),
        }
      : null,
    bestDirectBuyer: bestBuyer ? summarize(bestBuyer) : null,
    bestAccessibleEntryPoint: bestAccessibleEntry
      ? summarize(bestAccessibleEntry)
      : primary && hasEmail(primary.person)
        ? summarize(primary)
        : null,
    relevantBackup: backup ? summarize(backup) : null,
    departmentOrHousingRoute: orgRoute ? summarize(orgRoute) : null,
    candidatesScored: scored.map(summarize),
    selectionRules: [
      "Verified buyer relevance outranks channel completeness",
      "Never silently replace a verified decision-maker with an easier-to-email contact",
      "When buyer has no public email, keep buyer + show reachable referral separately",
      "Scores cannot override unsupported authority claims",
    ],
  };
}

function summarize(scored) {
  return {
    name: text(scored.person.name || scored.person.fullName) || null,
    title: text(scored.person.title || scored.person.role) || null,
    organization: text(scored.person.organization) || null,
    email: text(scored.person.email || scored.person.workEmail) || null,
    phone: text(scored.person.phone || scored.person.directPhone) || null,
    classification: scored.classification.class,
    classificationLabel: scored.classification.label,
    salesFriendlyWhy: scored.classification.salesFriendlyWhy,
    channelRecoveryScore: scored.channelRecovery.score,
    commercialRelevanceScore: scored.commercialRelevance.score,
    channelComponents: scored.channelRecovery.components,
    commercialComponents: scored.commercialRelevance.components,
    sourceUrl:
      text(
        scored.person.channelSourceUrl ||
          scored.person.sourceUrl ||
          scored.person.officialContactUrl,
      ) || null,
    evidenceGaps:
      scored.classification.evidence?.unverifiedResponsibilityGaps || [],
  };
}

/**
 * Compare baseline (original GDI primary) vs enhanced selection.
 */
export function compareBaselineToEnhanced(baselinePerson = {}, selection) {
  const baselineName = nameKey(baselinePerson.name || baselinePerson.fullName);
  const enhancedName = nameKey(selection?.primary?.name);
  const samePerson =
    Boolean(baselineName) &&
    Boolean(enhancedName) &&
    baselineName === enhancedName;

  let upgradeType = "RETAINED_CORRECTLY";
  if (!selection?.primary) upgradeType = "NO_RECOMMENDATION";
  else if (!baselineName) upgradeType = "NEW_PRIMARY_IDENTIFIED";
  else if (!samePerson) {
    if (selection.primary.classification?.includes?.("BUYER")) {
      upgradeType = "BETTER_BUYER_OR_ENTRY";
    } else if (
      selection.primary.classification ===
      COMMERCIAL_CONTACT_CLASS.RELEVANT_COMMERCIAL_ENTRY_POINT
    ) {
      upgradeType = "BETTER_RELEVANT_ENTRY_POINT";
    } else {
      upgradeType = "CONTACT_CHANGED";
    }
  } else if (
    text(selection.primary.email) &&
    !text(baselinePerson.email || baselinePerson.workEmail)
  ) {
    upgradeType = "SAME_PERSON_CHANNEL_IMPROVED";
  }

  return {
    baselineName: text(baselinePerson.name) || null,
    enhancedName: selection?.primary?.name || null,
    samePerson,
    upgradeType,
    meaningfulCommercialUpgrade: [
      "BETTER_BUYER_OR_ENTRY",
      "BETTER_RELEVANT_ENTRY_POINT",
      "NEW_PRIMARY_IDENTIFIED",
    ].includes(upgradeType),
    // Explicit: new person ≠ automatic improvement without class upgrade
    newPersonIsNotAutomaticImprovement: true,
  };
}

export function projectSelectionOntoOpportunity(opportunity = {}, selection) {
  if (!selection?.primary) {
    return {
      opportunity,
      changed: false,
      commercialContactIntelligence: {
        version: GDI_COMMERCIAL_CONTACT_SELECTION_V1,
        selection,
      },
    };
  }
  const p = selection.primary;
  const baseline = opportunity.primaryContact || {};
  const primaryContact = {
    ...baseline,
    name: p.name || baseline.name,
    title: p.title || baseline.title,
    role: p.title || baseline.role,
    organization: p.organization || baseline.organization,
    email: p.email || baseline.email || null,
    workEmail: p.email || baseline.workEmail || null,
    phone: p.phone || baseline.phone || null,
    contactClass: p.classification,
    contactClassification: p.classification,
    whyRelevant: p.salesFriendlyWhy,
    whyThisContact: p.salesFriendlyWhy,
    channelSourceUrl: p.sourceUrl || baseline.channelSourceUrl || null,
    commercialContactIntelligence: {
      version: GDI_COMMERCIAL_CONTACT_SELECTION_V1,
      channelRecoveryScore: p.channelRecoveryScore,
      commercialRelevanceScore: p.commercialRelevanceScore,
      // Internal scores stored for research — drawer uses salesFriendlyWhy
      salesFriendlyWhy: p.salesFriendlyWhy,
      evidenceGaps: p.evidenceGaps,
    },
  };

  const alt = selection.bestAccessibleEntryPoint;
  let secondaryContact = opportunity.secondaryContact || null;
  // Drop inherited secondary when it duplicates the newly selected primary.
  if (
    secondaryContact?.name &&
    nameKey(secondaryContact.name) === nameKey(primaryContact.name)
  ) {
    secondaryContact = null;
  }
  if (
    alt &&
    nameKey(alt.name) !== nameKey(primaryContact.name) &&
    (alt.email || alt.name)
  ) {
    secondaryContact = {
      ...(secondaryContact || {}),
      name: alt.name,
      title: alt.title,
      organization: alt.organization,
      email: alt.email,
      workEmail: alt.email,
      phone: alt.phone,
      contactClass: alt.classification,
      whyRelevant: alt.salesFriendlyWhy,
      whyThisContact: alt.salesFriendlyWhy,
      channelSourceUrl: alt.sourceUrl,
    };
  }
  if (
    secondaryContact?.name &&
    nameKey(secondaryContact.name) === nameKey(primaryContact.name)
  ) {
    secondaryContact = null;
  }

  return {
    opportunity: {
      ...opportunity,
      primaryContact,
      secondaryContact,
      commercialContactIntelligence: {
        version: GDI_COMMERCIAL_CONTACT_SELECTION_V1,
        selection,
        baselinePrimaryName: text(baseline.name) || null,
      },
    },
    changed: true,
  };
}
