/**
 * Full E2E V3 Stage 4 — Professional contact research & verification.
 *
 * Runs AFTER identity/role research (Stage 3). Public-first. Findymail/Surfe
 * OFF by default and require explicit authorization. Never invents emails.
 */

import { runRestoredHowContactChannelDiscovery } from "./gdi-who-how-contact-intelligence-restore-v1.js";
import { assessPublicContactVerification } from "../presentation/gdi-public-contact-verification-v1.js";
import { enrichPersonEmailWithFindymail } from "./gdi-findymail-stage4-enrichment-v1.js";

export const GDI_CONTACT_RESEARCH_STAGE_V1 = "gdi-contact-research-stage-v1";

export const CONTACT_CHANNEL_TYPE = Object.freeze({
  PROFESSIONAL_EMAIL: "PROFESSIONAL_EMAIL",
  PROFESSIONAL_DIRECT_PHONE: "PROFESSIONAL_DIRECT_PHONE",
  ORGANIZATIONAL_PHONE: "ORGANIZATIONAL_PHONE",
  PERSON_LINKEDIN: "PERSON_LINKEDIN",
  OFFICIAL_PROFESSIONAL_PROFILE: "OFFICIAL_PROFESSIONAL_PROFILE",
  OTHER_VERIFIED_BUSINESS_ROUTE: "OTHER_VERIFIED_BUSINESS_ROUTE",
});

export const CONTACT_VERIFICATION_STATUS = Object.freeze({
  VERIFIED_PERSON_DIRECT: "VERIFIED_PERSON_DIRECT",
  VERIFIED_ORGANIZATIONAL: "VERIFIED_ORGANIZATIONAL",
  UNVERIFIED: "UNVERIFIED",
  REJECTED_INVENTED: "REJECTED_INVENTED",
  REJECTED_MISATTRIBUTED: "REJECTED_MISATTRIBUTED",
  NOT_FOUND: "NOT_FOUND",
});

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

function isHttpUrl(url) {
  return /^https?:\/\//i.test(text(url));
}

function isPersonLinkedIn(url) {
  return /^https?:\/\/([a-z0-9-]+\.)?linkedin\.com\/in\/[A-Za-z0-9_%-]+\/?/i.test(
    text(url),
  );
}

/**
 * Resolve a verified personal LinkedIn URL from any approved person fields.
 * WHO/HOW often stores the profile in sourceUrl / personSourceUrl while
 * linkedinUrl remains null — Stage 4 must still treat that as researched.
 */
export function resolvePersonLinkedInUrl(person = {}, fallback = {}) {
  const c = fallback && typeof fallback === "object" ? fallback : {};
  const candidates = [
    person.linkedinUrl,
    person.linkedInUrl,
    person.verifiedLinkedIn,
    c.linkedinUrl,
    c.linkedInUrl,
    c.verifiedLinkedIn,
    person.personSourceUrl,
    person.sourceUrl,
    person.publicProfileUrl,
    person.profileUrl,
    c.personSourceUrl,
    c.sourceUrl,
    c.publicProfileUrl,
    c.profileUrl,
  ];
  for (const raw of candidates) {
    const u = text(raw);
    if (isPersonLinkedIn(u)) return u.replace(/\/$/, "");
  }
  return null;
}

export { isPersonLinkedIn };

/**
 * Reject invented / misattributed channels (Stage 4 policy N).
 */
export function validateContactChannelCandidate(channel = {}, person = {}) {
  const type = text(channel.channelType || channel.type).toUpperCase();
  const value = text(channel.value || channel.url || channel.email || channel.phone);
  const reasons = [];

  if (!value) reasons.push("EMPTY_VALUE");

  if (channel.invented === true || channel.guessedPattern === true) {
    reasons.push("INVENTED_OR_GUESSED");
  }

  // Pattern-guessed emails like first.last@domain without source
  if (
    type === CONTACT_CHANNEL_TYPE.PROFESSIONAL_EMAIL &&
    channel.sourceMissing === true
  ) {
    reasons.push("EMAIL_WITHOUT_SOURCE");
  }

  if (
    type === CONTACT_CHANNEL_TYPE.PERSON_LINKEDIN &&
    (!isPersonLinkedIn(value) || /linkedin\.com\/company\//i.test(value))
  ) {
    reasons.push("ORG_HOMEPAGE_OR_COMPANY_PAGE_NOT_PERSON_LINKEDIN");
  }

  if (
    type === CONTACT_CHANNEL_TYPE.PROFESSIONAL_DIRECT_PHONE &&
    channel.isSwitchboard === true
  ) {
    reasons.push("SWITCHBOARD_MISLABELED_AS_DIRECT");
  }

  if (
    (type === CONTACT_CHANNEL_TYPE.PROFESSIONAL_EMAIL ||
      type === CONTACT_CHANNEL_TYPE.PROFESSIONAL_DIRECT_PHONE) &&
    channel.personDirect !== true &&
    channel.organizational === true
  ) {
    // Allowed only if typed as organizational
    if (type === CONTACT_CHANNEL_TYPE.PROFESSIONAL_EMAIL) {
      reasons.push("ORG_EMAIL_MUST_NOT_CLAIM_PERSON_DIRECT");
    }
  }

  if (channel.provider === "SURFE" && channel.persist === true) {
    reasons.push("SURFE_MUST_NOT_PERSIST");
  }

  const ok = reasons.length === 0;
  return {
    ok,
    reasons,
    channel: ok
      ? {
          channelType: type || CONTACT_CHANNEL_TYPE.OTHER_VERIFIED_BUSINESS_ROUTE,
          value,
          source: text(channel.source) || null,
          sourceUrl: isHttpUrl(channel.sourceUrl) ? text(channel.sourceUrl) : null,
          verificationStatus:
            text(channel.verificationStatus).toUpperCase() ||
            CONTACT_VERIFICATION_STATUS.UNVERIFIED,
          verificationDate: channel.verificationDate || new Date().toISOString(),
          personDirect: channel.personDirect === true,
          organizational: channel.organizational === true,
          researchProvenance: channel.researchProvenance || null,
          personName: text(person.fullName || person.name) || null,
        }
      : null,
  };
}

/**
 * One verified usable outreach route is enough (email OR phone OR LinkedIn).
 */
export function hasUsableOutreachRoute(channels = []) {
  const ok = (channels || []).filter(
    (c) =>
      c &&
      (c.verificationStatus === CONTACT_VERIFICATION_STATUS.VERIFIED_PERSON_DIRECT ||
        c.verificationStatus ===
          CONTACT_VERIFICATION_STATUS.VERIFIED_ORGANIZATIONAL ||
        (c.channelType === CONTACT_CHANNEL_TYPE.PERSON_LINKEDIN &&
          isPersonLinkedIn(c.value))),
  );
  return {
    ok: ok.length >= 1,
    count: ok.length,
    channels: ok,
  };
}

/**
 * Build durable channel records for a verified person (buyer or entry point).
 */
export function collectVerifiedChannelsForPerson(person = {}, opportunity = {}) {
  const candidates = [];
  const c = opportunity.primaryContact || {};
  const email = text(person.email || c.email);
  const phone = text(person.phone || c.phone);
  const linkedin = resolvePersonLinkedInUrl(person, c);
  const orgPhone = text(person.organizationPhone || c.companyPhone);

  if (email) {
    const existingFm = (Array.isArray(person.channels) ? person.channels : []).find(
      (ch) =>
        text(ch?.source).toUpperCase() === "FINDYMAIL" &&
        text(ch?.value).toLowerCase() === email.toLowerCase(),
    );
    const emailSource =
      person.emailSource || c.emailSource || existingFm?.source || null;
    candidates.push({
      channelType: CONTACT_CHANNEL_TYPE.PROFESSIONAL_EMAIL,
      value: email,
      source: emailSource,
      sourceUrl: person.emailSourceUrl || c.emailSourceUrl || null,
      verificationStatus: c.emailCustomerVerified
        ? CONTACT_VERIFICATION_STATUS.VERIFIED_PERSON_DIRECT
        : existingFm?.verificationStatus ||
          CONTACT_VERIFICATION_STATUS.UNVERIFIED,
      personDirect: true,
      organizational: false,
      invented: false,
      sourceMissing: !emailSource,
      researchProvenance: existingFm?.researchProvenance || null,
    });
  }
  if (phone) {
    candidates.push({
      channelType: CONTACT_CHANNEL_TYPE.PROFESSIONAL_DIRECT_PHONE,
      value: phone,
      source: person.phoneSource || null,
      sourceUrl: person.phoneSourceUrl || null,
      verificationStatus: c.phoneCustomerVerified
        ? CONTACT_VERIFICATION_STATUS.VERIFIED_PERSON_DIRECT
        : CONTACT_VERIFICATION_STATUS.UNVERIFIED,
      personDirect: true,
      isSwitchboard: person.isSwitchboard === true,
      invented: false,
    });
  }
  if (orgPhone) {
    candidates.push({
      channelType: CONTACT_CHANNEL_TYPE.ORGANIZATIONAL_PHONE,
      value: orgPhone,
      verificationStatus: CONTACT_VERIFICATION_STATUS.VERIFIED_ORGANIZATIONAL,
      personDirect: false,
      organizational: true,
      invented: false,
    });
  }
  if (linkedin) {
    candidates.push({
      channelType: CONTACT_CHANNEL_TYPE.PERSON_LINKEDIN,
      value: linkedin,
      sourceUrl: linkedin,
      verificationStatus: isPersonLinkedIn(linkedin)
        ? CONTACT_VERIFICATION_STATUS.VERIFIED_PERSON_DIRECT
        : CONTACT_VERIFICATION_STATUS.REJECTED_MISATTRIBUTED,
      personDirect: true,
      invented: false,
    });
  }

  const accepted = [];
  const rejected = [];
  for (const ch of candidates) {
    const v = validateContactChannelCandidate(ch, person);
    if (v.ok) accepted.push(v.channel);
    else rejected.push({ candidate: ch, reasons: v.reasons });
  }
  return { accepted, rejected, usable: hasUsableOutreachRoute(accepted) };
}

/**
 * Stage 4 runner — public HOW first, then permanent Findymail email enrichment
 * when allowFindymail=true (Live or dry-run planning). Surfe remains refused.
 */
export async function runProfessionalContactResearchStage(
  opportunity = {},
  opts = {},
) {
  if (opts.authorizeSurfe === true) {
    throw new Error(
      "Stage 4 refuses automatic Surfe — explicit user-click only outside Full E2E",
    );
  }
  if (opts.authorizeFindymail === true && opts.allowFindymail !== true) {
    throw new Error(
      "Findymail requires allowFindymail=true (permanent Stage 4 component gate)",
    );
  }
  if (opts.mode !== "Live" && opts.authorizePaidPublicSearch === true) {
    throw new Error("Paid public search requires Mode=Live + authorization");
  }

  const whoComplete = Boolean(
    opportunity.primaryContact?.name || opportunity.whoHowStatus,
  );
  if (!whoComplete && opts.requireWhoFirst !== false) {
    return {
      version: GDI_CONTACT_RESEARCH_STAGE_V1,
      outcome: "SKIPPED",
      reason: "WHO_HOW_INCOMPLETE",
      opportunity,
      paid: false,
      findymail: false,
      surfe: false,
    };
  }

  const runFindymail =
    opts.allowFindymail === true &&
    (opts.mode === "Live" || opts.findymailDryRun === true);

  // DryRun / fixture: channel collection; optional Findymail dry-run planning
  if (
    (opts.mode === "DryRun" || opts.fixture === true || opts.skipProvider === true) &&
    !runFindymail
  ) {
    const person = opportunity.primaryContact || {};
    const collected = collectVerifiedChannelsForPerson(person, opportunity);
    const next = {
      ...opportunity,
      contactResearchStage: {
        version: GDI_CONTACT_RESEARCH_STAGE_V1,
        at: new Date().toISOString(),
        channels: collected.accepted,
        rejected: collected.rejected,
        usableOutreach: collected.usable,
        findymailUsed: false,
        findymailPermanentComponent: true,
        surfeUsed: false,
        surfePersisted: false,
      },
      verifiedContactChannels: collected.accepted,
    };
    if (person && (collected.accepted.length || linkedinFromPerson(person))) {
      const li = resolvePersonLinkedInUrl(person);
      next.primaryContact = {
        ...person,
        ...(li ? { linkedinUrl: li, sourceUrl: person.sourceUrl || li } : {}),
        channels: collected.accepted,
        contactChannelResearch: summarizeChannelResearchStatus(collected, person),
      };
    }
    return {
      version: GDI_CONTACT_RESEARCH_STAGE_V1,
      outcome: collected.usable.ok ? "PASS" : "HOLD",
      reason: collected.usable.ok ? "USABLE_ROUTE" : "NO_USABLE_ROUTE",
      opportunity: next,
      paid: false,
      findymail: false,
      surfe: false,
      verification: assessPublicContactVerification(next),
    };
  }

  const how = await runRestoredHowContactChannelDiscovery(opportunity, {
    ...opts,
    authorizeFindymail: false,
    authorizeSurfe: false,
  });
  let person = how.opportunity?.primaryContact || opportunity.primaryContact || {};
  let findymailUsed = false;
  let findymailOutcome = null;
  let secondary = how.opportunity?.secondaryContact || opportunity.secondaryContact || null;

  if (runFindymail) {
    const primaryEnrich = await enrichPersonEmailWithFindymail(person, how.opportunity || opportunity, {
      ...opts,
      dryRun: opts.mode !== "Live" || opts.findymailDryRun === true,
      persistFindymailResults: opts.persistFindymailResults === true,
    });
    person = primaryEnrich.person || person;
    findymailUsed = primaryEnrich.findymailUsed === true;
    findymailOutcome = primaryEnrich.outcome;
    if (secondary?.name) {
      const secEnrich = await enrichPersonEmailWithFindymail(
        secondary,
        how.opportunity || opportunity,
        {
          ...opts,
          dryRun: opts.mode !== "Live" || opts.findymailDryRun === true,
          persistFindymailResults: opts.persistFindymailResults === true,
        },
      );
      secondary = secEnrich.person || secondary;
      findymailUsed = findymailUsed || secEnrich.findymailUsed === true;
    }
  }

  const collected = collectVerifiedChannelsForPerson(person, {
    ...(how.opportunity || opportunity),
    primaryContact: person,
  });
  // Preserve Findymail-sourced email channels for Airtable persist even when
  // customer verification is still pending (FOUND_VERIFICATION_REQUIRED).
  const acceptedKeys = new Set(
    (collected.accepted || []).map(
      (ch) => `${text(ch.channelType).toUpperCase()}|${text(ch.value).toLowerCase()}`,
    ),
  );
  const findymailChannels = (Array.isArray(person.channels) ? person.channels : []).filter(
    (ch) =>
      text(ch?.source).toUpperCase() === "FINDYMAIL" &&
      text(ch?.value) &&
      !acceptedKeys.has(
        `${CONTACT_CHANNEL_TYPE.PROFESSIONAL_EMAIL}|${text(ch.value).toLowerCase()}`,
      ),
  );
  const mergedChannels = [...(collected.accepted || []), ...findymailChannels];
  const li = resolvePersonLinkedInUrl(person);
  return {
    version: GDI_CONTACT_RESEARCH_STAGE_V1,
    outcome: collected.usable.ok ? "PASS" : "HOLD",
    opportunity: {
      ...how.opportunity,
      primaryContact: {
        ...person,
        ...(li ? { linkedinUrl: li, sourceUrl: person.sourceUrl || li } : {}),
        channels: mergedChannels,
        contactChannelResearch: summarizeChannelResearchStatus(collected, person),
      },
      ...(secondary ? { secondaryContact: secondary } : {}),
      contactResearchStage: {
        version: GDI_CONTACT_RESEARCH_STAGE_V1,
        at: new Date().toISOString(),
        channels: mergedChannels,
        rejected: collected.rejected,
        usableOutreach: collected.usable,
        findymailUsed,
        findymailOutcome,
        findymailPermanentComponent: true,
        surfeUsed: false,
        surfePersisted: false,
        howWorkflow: how.researchStatus || null,
      },
      verifiedContactChannels: collected.accepted,
    },
    paid: findymailUsed,
    findymail: findymailUsed,
    surfe: false,
  };
}

function linkedinFromPerson(person = {}) {
  return Boolean(resolvePersonLinkedInUrl(person));
}

/**
 * Per-channel research status for customer/admin scorecards.
 * Finding LinkedIn must NOT mark email/phone as researched-complete.
 */
export function summarizeChannelResearchStatus(collected = {}, person = {}) {
  const accepted = collected.accepted || [];
  const has = (type) =>
    accepted.some((c) => text(c.channelType).toUpperCase() === type);
  const email = text(person.email);
  const phone = text(person.phone);
  const linkedin = resolvePersonLinkedInUrl(person);
  return {
    version: "gdi-contact-channel-research-status-v1",
    professionalEmail: email
      ? "FOUND"
      : has(CONTACT_CHANNEL_TYPE.PROFESSIONAL_EMAIL)
        ? "FOUND"
        : "NOT_FOUND_OR_UNAVAILABLE",
    professionalTelephone: phone
      ? "FOUND"
      : has(CONTACT_CHANNEL_TYPE.PROFESSIONAL_DIRECT_PHONE)
        ? "FOUND"
        : "NOT_FOUND_OR_UNAVAILABLE",
    personalLinkedIn:
      linkedin || has(CONTACT_CHANNEL_TYPE.PERSON_LINKEDIN)
        ? "FOUND"
        : "NOT_YET_RESEARCHED",
    usableOutreachRoute: collected.usable?.ok === true,
  };
}
