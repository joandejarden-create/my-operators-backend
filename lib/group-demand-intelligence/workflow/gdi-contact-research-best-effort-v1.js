/**
 * GDI Contact Research Best-Effort V1
 *
 * Permanent Stage 4 policy: maximize verified professional email, telephone,
 * and individual LinkedIn for every applicable commercial contact (primary +
 * alternatives) without inventing identity or terminating early after one channel.
 *
 * Contract: GDI_CONTACT_RESEARCH_BEST_EFFORT_V1
 * Extends: gdi-contact-research-stage-v1 (does not replace it)
 */
import {
  collectVerifiedChannelsForPerson,
  resolvePersonLinkedInUrl,
  CONTACT_CHANNEL_TYPE,
  GDI_CONTACT_RESEARCH_STAGE_V1,
} from "./gdi-contact-research-stage-v1.js";
import {
  isFullyVerifiedPersonName,
  classifyPersonIdentityCompleteness,
  PERSON_IDENTITY_COMPLETENESS,
} from "../presentation/gdi-abbreviated-contact-identity-v1.js";
import { assessFindymailEmailEligibility } from "./gdi-findymail-stage4-enrichment-v1.js";
import { readFindymailUsageLedger } from "../providers/gdi-findymail-client-v1.js";

export const GDI_CONTACT_RESEARCH_BEST_EFFORT_V1 =
  "gdi-contact-research-best-effort-v1";

export const CONTACT_CHANNEL_RESEARCH_STATUS = Object.freeze({
  NOT_YET_RESEARCHED: "NOT_YET_RESEARCHED",
  EXISTING_VERIFIED_INTELLIGENCE_REUSED: "EXISTING_VERIFIED_INTELLIGENCE_REUSED",
  RESEARCH_ATTEMPTED: "RESEARCH_ATTEMPTED",
  VERIFIED_CONTACT_INFORMATION_FOUND: "VERIFIED_CONTACT_INFORMATION_FOUND",
  FOUND_BUT_NOT_VERIFIED: "FOUND_BUT_NOT_VERIFIED",
  RESEARCH_COMPLETED_NOT_FOUND: "RESEARCH_COMPLETED_NOT_FOUND",
  IDENTITY_RESOLUTION_REQUIRED: "IDENTITY_RESOLUTION_REQUIRED",
  AUTHORIZATION_OR_BUDGET_BLOCKED: "AUTHORIZATION_OR_BUDGET_BLOCKED",
  RESTRICTED_TRANSIENT: "RESTRICTED_TRANSIENT",
});

export const CONTACT_ROLE_SLOT = Object.freeze({
  PRIMARY: "PRIMARY",
  SECONDARY: "SECONDARY",
  ALTERNATIVE_ENTRY_1: "ALTERNATIVE_ENTRY_1",
  ALTERNATIVE_ENTRY_2: "ALTERNATIVE_ENTRY_2",
  BUYER_INFLUENCER: "BUYER_INFLUENCER",
});

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

function personKey(name, org) {
  return `${text(name).toLowerCase()}|${text(org).toLowerCase()}`;
}

/**
 * Map Stage 4 contactChannelResearch enums → best-effort researched flags.
 * Accepts both boolean flags (emailResearched) and status enums
 * (professionalEmail: FOUND | NOT_FOUND_OR_UNAVAILABLE | NOT_YET_RESEARCHED).
 */
function resolveChannelResearchFlags(researched = {}, channel) {
  const enumKey =
    channel === "email"
      ? "professionalEmail"
      : channel === "telephone"
        ? "professionalTelephone"
        : "personalLinkedIn";
  const boolKey =
    channel === "email"
      ? "emailResearched"
      : channel === "telephone"
        ? "phoneResearched"
        : "linkedinResearched";
  const attemptedKey =
    channel === "email"
      ? "emailAttempted"
      : channel === "telephone"
        ? "phoneAttempted"
        : "linkedinAttempted";
  const reusedKey =
    channel === "email"
      ? "emailReused"
      : channel === "telephone"
        ? "phoneReused"
        : "linkedinReused";
  const budgetKey =
    channel === "email"
      ? "emailBudgetBlocked"
      : channel === "telephone"
        ? "phoneBudgetBlocked"
        : "linkedinBudgetBlocked";

  const enumStatus = text(researched[enumKey]).toUpperCase();
  const researchedDone =
    researched[boolKey] === true ||
    enumStatus === "FOUND" ||
    enumStatus === "NOT_FOUND_OR_UNAVAILABLE" ||
    enumStatus === "RESEARCH_COMPLETED_NOT_FOUND" ||
    enumStatus === "UNAVAILABLE";
  return {
    researched: researchedDone,
    attempted:
      researched[attemptedKey] === true ||
      enumStatus === "RESEARCH_ATTEMPTED" ||
      researchedDone,
    reused: researched[reusedKey] === true,
    budgetBlocked:
      researched[budgetKey] === true ||
      enumStatus === "AUTHORIZATION_OR_BUDGET_BLOCKED",
  };
}

function channelStatusFromValue(value, opts = {}) {
  if (opts.identityIncomplete) {
    return CONTACT_CHANNEL_RESEARCH_STATUS.IDENTITY_RESOLUTION_REQUIRED;
  }
  if (opts.restrictedTransient) {
    return CONTACT_CHANNEL_RESEARCH_STATUS.RESTRICTED_TRANSIENT;
  }
  if (opts.budgetBlocked) {
    return CONTACT_CHANNEL_RESEARCH_STATUS.AUTHORIZATION_OR_BUDGET_BLOCKED;
  }
  if (text(value)) {
    if (opts.verified === false) {
      return CONTACT_CHANNEL_RESEARCH_STATUS.FOUND_BUT_NOT_VERIFIED;
    }
    if (opts.reused) {
      return CONTACT_CHANNEL_RESEARCH_STATUS.EXISTING_VERIFIED_INTELLIGENCE_REUSED;
    }
    return CONTACT_CHANNEL_RESEARCH_STATUS.VERIFIED_CONTACT_INFORMATION_FOUND;
  }
  if (opts.researched === true) {
    return CONTACT_CHANNEL_RESEARCH_STATUS.RESEARCH_COMPLETED_NOT_FOUND;
  }
  if (opts.attempted === true) {
    return CONTACT_CHANNEL_RESEARCH_STATUS.RESEARCH_ATTEMPTED;
  }
  return CONTACT_CHANNEL_RESEARCH_STATUS.NOT_YET_RESEARCHED;
}

/**
 * Build per-channel ledger row for one person.
 */
export function buildContactChannelLedgerForPerson(person = {}, opts = {}) {
  const name = text(person.fullName || person.name);
  const org = text(person.organization || opts.organizationName);
  const identityCompleteness = classifyPersonIdentityCompleteness(name);
  const identityOk = isFullyVerifiedPersonName(name);
  const email = text(person.email);
  const phone = text(person.phone);
  const linkedin = resolvePersonLinkedInUrl(person) || "";
  const researched =
    person.contactChannelResearch ||
    opts.contactChannelResearch ||
    {};
  const identityIncomplete = !identityOk;
  const emailFlags = resolveChannelResearchFlags(researched, "email");
  const phoneFlags = resolveChannelResearchFlags(researched, "telephone");
  const linkedinFlags = resolveChannelResearchFlags(researched, "linkedin");

  const emailStatus = channelStatusFromValue(email, {
    identityIncomplete: identityIncomplete && !email,
    verified: person.emailCustomerVerified !== false && Boolean(email),
    reused: emailFlags.reused || person.emailSource === "AIRTABLE",
    researched:
      emailFlags.researched ||
      researched.findymailUsed === true ||
      Boolean(email),
    attempted: emailFlags.attempted,
    budgetBlocked: emailFlags.budgetBlocked,
  });
  const phoneStatus = channelStatusFromValue(phone, {
    identityIncomplete: identityIncomplete && !phone,
    verified: person.phoneCustomerVerified !== false && Boolean(phone),
    reused: phoneFlags.reused,
    researched: phoneFlags.researched || Boolean(phone),
    attempted: phoneFlags.attempted,
    budgetBlocked: phoneFlags.budgetBlocked,
  });
  const linkedinStatus = channelStatusFromValue(linkedin, {
    identityIncomplete: false,
    verified: Boolean(linkedin),
    reused: linkedinFlags.reused,
    researched: linkedinFlags.researched || Boolean(linkedin),
    attempted: linkedinFlags.attempted,
  });

  const collected = collectVerifiedChannelsForPerson(person, opts.opportunity || {});
  const findymailEligibility = assessFindymailEmailEligibility(
    person,
    {
      organizationName: org,
      ...(opts.opportunity || {}),
    },
    { ledger: opts.findymailLedger || null },
  );

  return {
    version: GDI_CONTACT_RESEARCH_BEST_EFFORT_V1,
    stageContract: GDI_CONTACT_RESEARCH_STAGE_V1,
    personKey: personKey(name, org),
    roleSlot: opts.roleSlot || CONTACT_ROLE_SLOT.PRIMARY,
    fullName: name || null,
    title: text(person.title || person.role) || null,
    organization: org || null,
    identityCompleteness,
    identityFullyVerified: identityOk,
    identityStatus: person.identityStatus || null,
    channels: {
      email: {
        status: emailStatus,
        value: email || null,
        channelType: CONTACT_CHANNEL_TYPE.PROFESSIONAL_EMAIL,
      },
      telephone: {
        status: phoneStatus,
        value: phone || null,
        channelType: CONTACT_CHANNEL_TYPE.PROFESSIONAL_DIRECT_PHONE,
      },
      linkedin: {
        status: linkedinStatus,
        value: linkedin || null,
        channelType: CONTACT_CHANNEL_TYPE.PERSON_LINKEDIN,
      },
    },
    usableOutreachRoute: Boolean(collected.usable?.ok),
    hasEmailOrTelephone: Boolean(email || phone),
    hasAllThreeChannels: Boolean(email && phone && linkedin),
    findymail: {
      eligible: findymailEligibility.eligible === true,
      reason: findymailEligibility.reason || null,
      requiredWhenEligible: true,
    },
    buyingAuthorityStatus:
      person.buyingAuthorityStatus ||
      person.contractingAuthorityStatus ||
      null,
    whyWeAreShowingThisContact:
      text(person.whyThisContact || person.whyWeAreShowingThisContact) || null,
  };
}

/**
 * Collect primary + secondary + alternative entry points for an opportunity.
 */
export function collectApplicableContactsForOpportunity(opportunity = {}) {
  const rows = [];
  const org = text(opportunity.organizationName || opportunity.displayCompanyName);
  if (opportunity.primaryContact?.name) {
    rows.push({
      person: opportunity.primaryContact,
      roleSlot: CONTACT_ROLE_SLOT.PRIMARY,
      organizationName: org,
    });
  }
  if (opportunity.secondaryContact?.name) {
    rows.push({
      person: opportunity.secondaryContact,
      roleSlot: CONTACT_ROLE_SLOT.SECONDARY,
      organizationName: org,
    });
  }
  const alts = Array.isArray(opportunity.alternativeEntryPoints)
    ? opportunity.alternativeEntryPoints
    : [];
  alts.forEach((p, idx) => {
    if (!text(p?.name || p?.fullName)) return;
    rows.push({
      person: p,
      roleSlot:
        idx === 0
          ? CONTACT_ROLE_SLOT.ALTERNATIVE_ENTRY_1
          : idx === 1
            ? CONTACT_ROLE_SLOT.ALTERNATIVE_ENTRY_2
            : CONTACT_ROLE_SLOT.BUYER_INFLUENCER,
      organizationName: org,
    });
  });
  return rows;
}

/**
 * Hotel-level best-effort ledger + coverage scorecard.
 */
export function buildHotelContactBestEffortLedger(opportunities = [], opts = {}) {
  const personLedgers = [];
  const byPerson = new Map();
  let opportunitiesNeedingAlt = 0;
  let altResearchPresent = 0;
  let opportunitiesMissingAuthorityAnnotation = 0;
  let opportunitiesWithVerifiedDirectBuyer = 0;
  const findymailLedger =
    opts.findymailLedger ||
    (opts.skipFindymailLedger === true
      ? null
      : readFindymailUsageLedger({ repoRoot: opts.repoRoot }));

  for (const o of opportunities) {
    const contacts = collectApplicableContactsForOpportunity(o);
    const primary = o.primaryContact || {};
    const authorityRaw = text(
      primary.buyingAuthorityStatus ||
        primary.contractingAuthorityStatus ||
        primary.contactRole ||
        o.buyingAuthorityStatus,
    );
    const primaryBuyerVerified =
      primary.buyerAuthorityVerified === true ||
      primary.hotelBuyingAuthorityVerified === true ||
      /DIRECT_BUYER|CONTRACTING_AUTHORITY_VERIFIED|LODGING_DECISION_MAKER/i.test(
        authorityRaw,
      );
    const primaryExplicitNonBuyer =
      /INFLUENCER|ALTERNATIVE_ENTRY|NO_AUTHORITY|AUTHORITY_NOT_VERIFIED|REQUIRES_ALTERNATIVE/i.test(
        authorityRaw,
      ) || o.requiresAlternativeEntryPoints === true;
    const authorityAnnotated = Boolean(authorityRaw) || primary.buyerAuthorityVerified === true;
    const hasAlt =
      contacts.some((c) =>
        [
          CONTACT_ROLE_SLOT.SECONDARY,
          CONTACT_ROLE_SLOT.ALTERNATIVE_ENTRY_1,
          CONTACT_ROLE_SLOT.ALTERNATIVE_ENTRY_2,
        ].includes(c.roleSlot),
      ) ||
      o.alternativeEntryResearchStatus === "COMPLETED" ||
      o.whoHow?.alternativeEntryResearchCompleted === true;

    if (primaryBuyerVerified) opportunitiesWithVerifiedDirectBuyer += 1;
    if (text(primary.name) && !authorityAnnotated) {
      opportunitiesMissingAuthorityAnnotation += 1;
    }
    // Alt research is mandatory only when authority is annotated as non-buyer
    // or explicitly required. Missing annotations are a separate outstanding gap.
    const altRequired = primaryExplicitNonBuyer && !primaryBuyerVerified;
    if (altRequired) {
      opportunitiesNeedingAlt += 1;
      if (hasAlt) altResearchPresent += 1;
    } else if (hasAlt) {
      altResearchPresent += 1;
    }
    for (const c of contacts) {
      const ledger = buildContactChannelLedgerForPerson(c.person, {
        roleSlot: c.roleSlot,
        organizationName: c.organizationName,
        opportunity: o,
        findymailLedger,
      });
      ledger.opportunityId = o.id || o.opportunityId;
      personLedgers.push(ledger);
      const prev = byPerson.get(ledger.personKey) || {
        ...ledger,
        opportunityIds: [],
        roleSlots: [],
      };
      prev.opportunityIds = [
        ...new Set([
          ...(prev.opportunityIds || []),
          ledger.opportunityId,
        ].filter(Boolean)),
      ];
      prev.roleSlots = [
        ...new Set([...(prev.roleSlots || []), ledger.roleSlot]),
      ];
      // Prefer stronger channel status when merging duplicates
      for (const ch of ["email", "telephone", "linkedin"]) {
        if (
          ledger.channels[ch].value &&
          !prev.channels[ch].value
        ) {
          prev.channels[ch] = ledger.channels[ch];
        }
      }
      byPerson.set(ledger.personKey, prev);
    }
  }

  const unique = [...byPerson.values()];
  const countStatus = (channel, status) =>
    unique.filter((p) => p.channels[channel].status === status).length;
  const withValue = (channel) =>
    unique.filter((p) => text(p.channels[channel].value)).length;

  const scorecard = {
    opportunityCount: opportunities.length,
    uniqueCommercialContacts: unique.length,
    personLedgerRows: personLedgers.length,
    fullProfessionalIdentityCoverage: unique.filter((p) => p.identityFullyVerified)
      .length,
    abbreviatedOrIncompleteIdentity: unique.filter((p) => !p.identityFullyVerified)
      .length,
    verifiedTitleCoverage: unique.filter((p) => text(p.title)).length,
    emailVerifiedCoverage: withValue("email"),
    telephoneVerifiedCoverage: withValue("telephone"),
    linkedinVerifiedCoverage: withValue("linkedin"),
    contactsWithAllThreeChannels: unique.filter((p) => p.hasAllThreeChannels).length,
    contactsWithEmailOrTelephone: unique.filter((p) => p.hasEmailOrTelephone)
      .length,
    contactsWithUsableOutreach: unique.filter((p) => p.usableOutreachRoute).length,
    emailNotYetResearched: countStatus(
      "email",
      CONTACT_CHANNEL_RESEARCH_STATUS.NOT_YET_RESEARCHED,
    ),
    telephoneNotYetResearched: countStatus(
      "telephone",
      CONTACT_CHANNEL_RESEARCH_STATUS.NOT_YET_RESEARCHED,
    ),
    linkedinNotYetResearched: countStatus(
      "linkedin",
      CONTACT_CHANNEL_RESEARCH_STATUS.NOT_YET_RESEARCHED,
    ),
    findymailEligibleRemaining: unique.filter((p) => p.findymail.eligible).length,
    opportunitiesRequiringAlternativeResearch: opportunitiesNeedingAlt,
    alternativeResearchPresent: altResearchPresent,
    opportunitiesWithVerifiedDirectBuyer,
    opportunitiesMissingAuthorityAnnotation,
    opportunitiesWithAlternativeContacts: opportunities.filter((o) => {
      const contacts = collectApplicableContactsForOpportunity(o);
      return contacts.some((c) =>
        [
          CONTACT_ROLE_SLOT.SECONDARY,
          CONTACT_ROLE_SLOT.ALTERNATIVE_ENTRY_1,
          CONTACT_ROLE_SLOT.ALTERNATIVE_ENTRY_2,
        ].includes(c.roleSlot),
      );
    }).length,
  };

  return {
    version: GDI_CONTACT_RESEARCH_BEST_EFFORT_V1,
    hotelId: opts.hotelId || null,
    at: new Date().toISOString(),
    scorecard,
    uniquePersons: unique,
    personOpportunityRows: personLedgers,
  };
}

/**
 * Permanent certification gate for future hotel Full E2E runs.
 * Does not invent channels; fails closed on unexecuted applicable research.
 */
export function evaluateContactBestEffortCertificationGate(ledger = {}) {
  const s = ledger.scorecard || {};
  const failures = [];
  if ((s.opportunityCount || 0) < 1) {
    failures.push("NO_QUALIFIED_OPPORTUNITIES");
  }
  if ((s.emailNotYetResearched || 0) > 0) {
    failures.push("EMAIL_RESEARCH_INCOMPLETE");
  }
  if ((s.telephoneNotYetResearched || 0) > 0) {
    failures.push("TELEPHONE_RESEARCH_INCOMPLETE");
  }
  if ((s.linkedinNotYetResearched || 0) > 0) {
    failures.push("LINKEDIN_RESEARCH_INCOMPLETE");
  }
  if ((s.findymailEligibleRemaining || 0) > 0) {
    failures.push("FINDYMAIL_ELIGIBLE_GAPS_REMAIN");
  }
  if (
    (s.opportunitiesRequiringAlternativeResearch || 0) >
    (s.alternativeResearchPresent || 0)
  ) {
    failures.push("ALTERNATIVE_ENTRY_RESEARCH_INCOMPLETE");
  }
  if ((s.opportunitiesMissingAuthorityAnnotation || 0) > 0) {
    failures.push("BUYING_AUTHORITY_ANNOTATION_INCOMPLETE");
  }
  // Abbreviated identities must not be treated as fully verified
  const badIdentity = (ledger.uniquePersons || []).filter(
    (p) =>
      p.identityCompleteness === PERSON_IDENTITY_COMPLETENESS.ABBREVIATED_SURNAME &&
      p.identityStatus !== "PARTIAL_IDENTITY_ABBREVIATED_SURNAME",
  );
  if (badIdentity.length) {
    failures.push("ABBREVIATED_IDENTITY_MISCLASSIFIED_AS_VERIFIED");
  }

  return {
    version: GDI_CONTACT_RESEARCH_BEST_EFFORT_V1,
    ok: failures.length === 0,
    failures,
    scorecard: s,
    note:
      "Gate requires research completion per channel, not that every contact has all three channels.",
  };
}
