/**
 * Buyer-focused contact enrichment orchestrator V1.
 *
 * Extends targeted contact resolution — does not replace publication eligibility.
 * Surfe only after named relevant person is established and public channels exhausted.
 * Provider metadata stays internal (usage ledger + field provenance).
 */

import fs from "node:fs";
import path from "node:path";
import {
  assessContactResearchAxes,
  targetRolesForOpportunity,
  resolveContactDepthOutcome,
  RESPONSIBILITY_CLASS,
  NAMED_RESEARCH_STATUS,
  PUBLICATION_CONTACT_STATUS,
  CONTACT_DEPTH_OUTCOME,
  PHONE_TYPE,
  CHANNEL_OWNERSHIP,
  BUYER_CONTACT_ENRICHMENT_CONTRACT_VERSION,
} from "./buyer-contact-enrichment-contract-v1.js";
import {
  buildFieldProvenanceRecord,
  buildContactProvenanceBag,
  mergeFieldProvenance,
  sanitizeContactForCustomerDisplay,
  FIELD_KINDS,
  CHANNEL_OUTCOME,
  personIdentityKey,
} from "./contact-field-provenance-v1.js";
import {
  reserveSurfeSpend,
  recordSurfeUsageEntry,
  estimateSurfeUsd,
  findReusableSurfeResult,
  remainingSharedBudgetUsd,
} from "./surfe-usage-ledger-v1.js";
import {
  scorecardFromBuyerEnrichment,
  writeScorecardArtifacts,
  RESEARCH_SCORECARD_VERSION,
} from "./research-scorecard-v1.js";
import {
  startSurfePeopleEnrichment,
  pollSurfePeopleEnrichment,
  normalizeSurfePerson,
  getSurfeApiKeyFromEnv,
  getSurfeCredits,
} from "../../surfe/client.js";
import {
  acceptSurfeIdentity,
  IDENTITY_DECISION,
} from "../../hotel-intelligence/contact-intelligence/surfe-identity-acceptance.js";
import { isGdiParallelSurfeFallbackEnabled } from "../parallel/feature-flag.js";
import {
  buildCanonicalPersonEntry,
  loadCanonicalContacts,
  saveCanonicalContacts,
} from "../canonical-contacts-store.js";
import {
  evaluateProspectPublicationEligibility,
  preservePublicationMetadata,
} from "../prospect-publication-eligibility-v1.js";

export const BUYER_CONTACT_ENRICHMENT_VERSION = "gdi-buyer-contact-enrichment-v1";

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

function splitName(full) {
  const parts = text(full).split(/\s+/);
  return { firstName: parts[0] || "", lastName: parts.slice(1).join(" ") };
}

function isSeniorExecutiveTitle(title) {
  return /\b(president|ceo|founder|chair(man|woman|person)?|chief\s+executive)\b/i.test(
    text(title)
  );
}

/**
 * Score whether a named person is a relevant buyer for the opportunity use case.
 * Presidents/founders are leads to verify — not automatic decision-makers.
 */
export function classifyPersonResponsibility(person = {}, opp = {}, opts = {}) {
  const title = text(person.title || person.role);
  const evidence = text(person.relevanceEvidence || opts.relevanceEvidence);
  const roles = targetRolesForOpportunity(opp);
  const roleBlob = roles.roles.join(" ");
  const roleSignalBlob = `${title} ${evidence}`;

  if (opts.confirmedBookingAuthority === true && evidence) {
    return {
      class: RESPONSIBILITY_CLASS.BOOKING_DECISION_MAKER,
      rationale: "Evidence supports booking/decision authority for this opportunity",
    };
  }

  const titleMatchesTarget =
    /meetings|events|event\s*planning|housing|travel|workplace|planner|operations|program|conference|owner|principal|founder|general\s*manager|director of sales|destination/i.test(
      roleSignalBlob
    ) ||
    roles.roles.some((r) => title.toLowerCase().includes(r.replace(/_/g, " ")));

  // For professional planner / DMC accounts, owner-principal is an intended target role
  const plannerOwnerOk =
    roles.bucket === "PROFESSIONAL_PLANNER" &&
    /owner|principal|founder|ceo|president|general\s*manager/i.test(title);

  if ((titleMatchesTarget || plannerOwnerOk) && evidence) {
    return {
      class: RESPONSIBILITY_CLASS.CONFIRMED_RELEVANT,
      rationale: plannerOwnerOk
        ? `Owner/principal is a target role for professional planner accounts; affiliation evidence exists`
        : `Title/role aligns with ${roles.bucket} targets and affiliation evidence exists`,
    };
  }
  if (titleMatchesTarget || plannerOwnerOk) {
    return {
      class: RESPONSIBILITY_CLASS.POTENTIALLY_RELEVANT,
      rationale: plannerOwnerOk
        ? "Owner/principal suggested for planner account; corroborate current venue-sourcing responsibility"
        : "Title suggests relevant role; affiliation/responsibility evidence incomplete",
    };
  }
  if (isSeniorExecutiveTitle(title) && roles.bucket !== "PROFESSIONAL_PLANNER") {
    return {
      class: RESPONSIBILITY_CLASS.REFERRAL_ENTRY,
      rationale:
        "Senior executive / president is a lead to verify — not automatically the hotel buyer",
      leadOnly: true,
    };
  }
  if (evidence) {
    return {
      class: RESPONSIBILITY_CLASS.POTENTIALLY_RELEVANT,
      rationale: "Some relevance evidence; responsibility unconfirmed",
    };
  }
  return {
    class: RESPONSIBILITY_CLASS.UNCONFIRMED,
    rationale: "Responsibility to this opportunity not established",
  };
}

/**
 * Build enrichment queue items for a hotel opportunity set.
 */
export function buildBuyerContactEnrichmentQueue({
  hotelId,
  opportunities = [],
  maxItems = 12,
  prioritizeIds = [],
} = {}) {
  const scored = [];
  for (const opp of opportunities || []) {
    const axes = assessContactResearchAxes(opp, {
      hotelReportedWrong: /wrong.?contact|bad.?contact/i.test(
        String(opp.hotelFeedbackNote || opp.feedbackSummary || "")
      ),
    });
    if (
      axes.namedResearchStatus === NAMED_RESEARCH_STATUS.EXHAUSTED_ORG_FALLBACK &&
      !prioritizeIds.includes(opp.id) &&
      !/wrong.?contact/i.test(String(opp.hotelFeedbackNote || ""))
    ) {
      // Avoid repeating unchanged exhausted searches every week
      continue;
    }
    if (!axes.enrichmentQueue && !prioritizeIds.includes(opp.id)) continue;
    let priority = 50;
    if (prioritizeIds.includes(opp.id)) priority -= 40;
    if (/wrong.?contact/i.test(String(opp.hotelFeedbackNote || ""))) priority -= 30;
    if (axes.namedResearchStatus === NAMED_RESEARCH_STATUS.NOT_ATTEMPTED) {
      priority -= 20;
    }
    if (axes.publicationContactStatus === PUBLICATION_CONTACT_STATUS.USABLE_ROUTE) {
      priority -= 10; // strong prospect with only functional route — still queue named research
    }
    if (
      axes.namedResearchStatus === NAMED_RESEARCH_STATUS.PENDING ||
      axes.namedResearchStatus === NAMED_RESEARCH_STATUS.NOT_ATTEMPTED
    ) {
      priority -= 5;
    }
    if (
      text(opp.primaryContact?.name) &&
      !text(opp.primaryContact?.email) &&
      !text(opp.primaryContact?.phone)
    ) {
      priority -= 15;
    }
    scored.push({
      hotelId,
      opportunityId: opp.id,
      organizationName: opp.organizationName || opp.company || null,
      title: opp.title || null,
      priority,
      axes,
      targetRoles: targetRolesForOpportunity(opp),
      existingRoute: {
        email:
          opp.functionalContactEmail ||
          opp.primaryContact?.email ||
          opp.contactEmail ||
          null,
        phone: opp.primaryContact?.phone || opp.contactPhone || null,
        url:
          opp.publicContactPath ||
          opp.organizationContactUrl ||
          opp.contactUrl ||
          null,
        name: opp.primaryContact?.name || null,
      },
      selectedReason: axes.queueReasons[0] || "named_enrichment_pending",
    });
  }
  scored.sort((a, b) => a.priority - b.priority);
  return {
    version: BUYER_CONTACT_ENRICHMENT_VERSION,
    hotelId,
    generatedAt: new Date().toISOString(),
    items: scored.slice(0, maxItems),
    deferred: scored.slice(maxItems),
  };
}

async function trySurfeForNamedPerson({
  personName,
  organization,
  domain,
  title,
  hotelId,
  opportunityIds,
  workflowRunId,
  authorizeSurfe,
  env = process.env,
}) {
  if (!authorizeSurfe) {
    return { status: "skipped", reason: "surfe_not_authorized" };
  }
  if (!isGdiParallelSurfeFallbackEnabled(env)) {
    return { status: "skipped", reason: "surfe_flag_off" };
  }
  if (!getSurfeApiKeyFromEnv()) {
    return { status: "blocked", reason: "surfe_key_missing" };
  }

  const reusable = findReusableSurfeResult(
    { name: personName, organization, domain },
    { env }
  );
  if (reusable) {
    const cachedEmail =
      reusable.entry?.acceptedFieldValues?.email ||
      reusable.entry?.email ||
      null;
    recordSurfeUsageEntry(
      {
        workflowRunId,
        hotelId,
        personName,
        organization,
        domain,
        opportunityIds,
        outcome: "reused",
        newRequest: false,
        cacheReuse: true,
        creditsConsumed: 0,
        estimatedCostUsd: 0,
        actualBilledCostUsd: 0,
        fieldsAccepted: reusable.entry.fieldsAccepted || [],
        acceptedFieldValues: reusable.entry.acceptedFieldValues || null,
        providerRequestId: reusable.entry.providerRequestId,
      },
      { env }
    );
    return {
      status: "reused",
      // Treat reuse as accepted for downstream email application when value is cached
      email: cachedEmail,
      enrichmentId: reusable.entry?.providerRequestId || null,
      validationStatus: "REUSED_CACHE",
      entry: reusable.entry,
      channelOutcome: CHANNEL_OUTCOME.REUSED_CACHE,
      creditsConsumed: 0,
      estimatedCostUsd: 0,
      emailAccepted: Boolean(cachedEmail),
    };
  }

  const costEst = estimateSurfeUsd({ email: true, mobile: false });
  if (!costEst.basisKnown || costEst.estimatedUsd == null) {
    return {
      status: "blocked",
      reason: "surfe_cost_basis_unknown",
      remainingUsd: remainingSharedBudgetUsd({ env }),
    };
  }

  // Check credits balance when available (do not invent 0)
  let creditsBefore = null;
  try {
    const cr = await getSurfeCredits();
    if (cr?.ok) creditsBefore = cr.payload || null;
  } catch {
    creditsBefore = null;
  }

  const reservation = reserveSurfeSpend({
    estimatedUsd: costEst.estimatedUsd,
    person: { name: personName, organization, domain },
    hotelId,
    opportunityIds,
    requestedChannels: ["email"],
    workflowRunId,
    env,
  });
  if (!reservation.ok) {
    return {
      status: "blocked",
      reason: reservation.reason,
      remainingUsd: reservation.remainingUsd,
    };
  }
  if (reservation.reuse) {
    return {
      status: "reused",
      entry: reservation.reusable.entry,
      channelOutcome: CHANNEL_OUTCOME.REUSED_CACHE,
    };
  }

  const { firstName, lastName } = splitName(personName);
  if (!firstName || !lastName) {
    recordSurfeUsageEntry(
      {
        workflowRunId,
        hotelId,
        personName,
        organization,
        domain,
        opportunityIds,
        outcome: "error",
        error: "invalid_person_name",
        newRequest: true,
        reservationId: reservation.reservationId,
        estimatedCostUsd: costEst.estimatedUsd,
        creditsConsumed: null,
      },
      { env }
    );
    return { status: "error", reason: "invalid_person_name" };
  }

  const start = await startSurfePeopleEnrichment({
    people: [
      {
        firstName,
        lastName,
        companyName: organization || undefined,
        companyDomain: domain || undefined,
        externalID: `gdi_buyer_${personIdentityKey({ name: personName, organization, domain })}`.slice(
          0,
          64
        ),
      },
    ],
    include: { email: true, mobile: false, linkedInUrl: true },
    enrichmentOptions: { acceptedEmailType: "professional" },
  });

  if (!start.ok) {
    recordSurfeUsageEntry(
      {
        workflowRunId,
        hotelId,
        personName,
        organization,
        domain,
        opportunityIds,
        outcome: "error",
        error: start.error || `http_${start.http_status}`,
        newRequest: true,
        reservationId: reservation.reservationId,
        estimatedCostUsd: costEst.estimatedUsd,
        creditsConsumed: null,
        requestedChannels: ["email"],
      },
      { env }
    );
    return {
      status: "error",
      reason: start.error || "surfe_start_failed",
      channelOutcome: CHANNEL_OUTCOME.PROVIDER_ERROR,
    };
  }

  const enrichmentId =
    start.payload?.enrichmentID ||
    start.payload?.enrichmentId ||
    start.payload?.id ||
    null;
  const done = await pollSurfePeopleEnrichment(enrichmentId, { maxWaitMs: 180000 });
  const person = normalizeSurfePerson((done.payload?.people || [])[0] || {});
  const surfeEmail =
    person.emails?.find((e) => e.validation_status === "VALID")?.email ||
    person.emails?.[0]?.email ||
    null;
  const identity = acceptSurfeIdentity({
    expectedFullName: personName,
    expectedOrganization: organization,
    expectedDomain: domain || null,
    expectedTitle: title || null,
    returnedFullName: person.full_name,
    returnedOrganization: person.company_name,
    returnedDomain: person.company_domain,
    returnedTitle: person.job_title,
    returnedEmail: surfeEmail,
    returnedLinkedInUrl: person.linkedin_url,
  });
  const identityOk =
    identity?.decision === IDENTITY_DECISION.ACCEPTED ||
    identity?.decision === IDENTITY_DECISION.ACCEPTED_WITH_LIMITED_EVIDENCE ||
    identity?.decision === IDENTITY_DECISION.CORROBORATION_ONLY;

  let creditsAfter = null;
  try {
    const cr = await getSurfeCredits();
    if (cr?.ok) creditsAfter = cr.payload || null;
  } catch {
    /* leave unknown */
  }

  let creditsConsumed = null;
  if (
    creditsBefore?.emailCredits != null &&
    creditsAfter?.emailCredits != null
  ) {
    creditsConsumed = Math.max(
      0,
      Number(creditsBefore.emailCredits) - Number(creditsAfter.emailCredits)
    );
  } else if (
    creditsBefore?.emails != null &&
    creditsAfter?.emails != null
  ) {
    creditsConsumed = Math.max(
      0,
      Number(creditsBefore.emails) - Number(creditsAfter.emails)
    );
  }

  if (!identityOk) {
    recordSurfeUsageEntry(
      {
        workflowRunId,
        hotelId,
        personName,
        organization,
        domain,
        opportunityIds,
        providerRequestId: enrichmentId,
        outcome: "identity_conflict",
        newRequest: true,
        reservationId: reservation.reservationId,
        estimatedCostUsd: costEst.estimatedUsd,
        creditsConsumed,
        fieldsReturned: surfeEmail ? ["email"] : [],
        fieldsRejected: surfeEmail ? ["email"] : [],
        requestedChannels: ["email"],
      },
      { env }
    );
    return {
      status: "identity_conflict",
      identity,
      channelOutcome: CHANNEL_OUTCOME.IDENTITY_CONFLICT,
      enrichmentId,
    };
  }

  if (!surfeEmail) {
    recordSurfeUsageEntry(
      {
        workflowRunId,
        hotelId,
        personName,
        organization,
        domain,
        opportunityIds,
        providerRequestId: enrichmentId,
        outcome: "no_result",
        newRequest: true,
        reservationId: reservation.reservationId,
        estimatedCostUsd: costEst.estimatedUsd,
        creditsConsumed, // may still have consumed credits
        requestedChannels: ["email"],
      },
      { env }
    );
    return {
      status: "no_result",
      channelOutcome: CHANNEL_OUTCOME.NO_CHANNEL_FOUND,
      enrichmentId,
      creditsConsumed,
    };
  }

  const validation =
    person.emails?.find((e) => e.email === surfeEmail)?.validation_status || null;
  recordSurfeUsageEntry(
    {
      workflowRunId,
      hotelId,
      personName,
      organization,
      domain,
      opportunityIds,
      providerRequestId: enrichmentId,
      outcome: "accepted",
      newRequest: true,
      reservationId: reservation.reservationId,
      estimatedCostUsd: costEst.estimatedUsd,
      actualBilledCostUsd: null, // unknown unless invoice available
      creditsConsumed,
      fieldsReturned: ["email"],
      fieldsAccepted: ["email"],
      acceptedFieldValues: { email: surfeEmail },
      requestedChannels: ["email"],
    },
    { env }
  );

  return {
    status: "accepted",
    email: surfeEmail,
    linkedinUrl: person.linkedin_url || null,
    jobTitle: person.job_title || null,
    enrichmentId,
    validationStatus: validation,
    channelOutcome:
      validation === "VALID"
        ? CHANNEL_OUTCOME.ACCEPTED_SOURCE_SUPPORTED
        : CHANNEL_OUTCOME.UNVERIFIED_PROVIDER_RESULT,
    creditsConsumed,
    estimatedCostUsd: costEst.estimatedUsd,
    identity,
  };
}

/**
 * Enrich one opportunity's buyer contact.
 * Preserves existing functional route. Does not demote publication eligibility.
 */
/**
 * Effective Surfe authorization: dry-run always wins over authorizeSurfe.
 * Live Surfe still requires authorizeSurfe === true (and budget/gap gates below).
 */
export function resolveEffectiveSurfeAuthorization(opts = {}) {
  const dryRun = opts.dryRun === true;
  const authorizeSurfe = opts.authorizeSurfe === true;
  return {
    dryRun,
    authorizeSurfeRequested: authorizeSurfe,
    authorizeSurfeEffective: !dryRun && authorizeSurfe,
    reason: dryRun
      ? "dry_run_blocks_paid_surfe"
      : authorizeSurfe
        ? "authorized_live"
        : "surfe_not_authorized",
  };
}

export async function enrichBuyerContactForOpportunity(opp = {}, opts = {}) {
  const hotelId = opts.hotelId || opp.hotelId;
  const workflowRunId = opts.workflowRunId || null;
  const dryRun = opts.dryRun === true;
  const surfeAuth = resolveEffectiveSurfeAuthorization(opts);
  const authorizeSurfe = surfeAuth.authorizeSurfeEffective;
  const env = opts.env || process.env;
  const leadPerson = opts.leadPerson || null; // e.g. Paulette Cazares for AMWA
  const persistCanonical =
    opts.persistCanonical !== false && !dryRun;
  // Injected spies for isolation tests (must throw if forbidden call occurs)
  const surfeCaller = opts.surfeCaller || trySurfeForNamedPerson;
  const canonicalWriter = opts.canonicalWriter || null;

  const beforeAxes = assessContactResearchAxes(opp);
  const eligBefore = evaluateProspectPublicationEligibility(opp);

  // Separate org/department route from any person-attributed channel already on record
  const existingPersonEmailOwnership =
    opp.primaryContact?.emailOwnership || null;
  const existingPersonEmail = text(opp.primaryContact?.email) || null;
  const orgInbox =
    text(opp.functionalContactEmail) ||
    text(opp.organizationContactEmail) ||
    null;
  // If primaryContact.email is only the org inbox (or unmarked), treat as org route
  const primaryLooksLikeOrg =
    existingPersonEmail &&
    (!existingPersonEmailOwnership ||
      existingPersonEmailOwnership === CHANNEL_OWNERSHIP.ORGANIZATION_DEPARTMENT ||
      (orgInbox &&
        existingPersonEmail.toLowerCase() === orgInbox.toLowerCase()) ||
      /^(info|newbiz|sales|contact|hello|office|admin|events|meetings)@/i.test(
        existingPersonEmail
      ));
  const personSpecificEmailAlready =
    existingPersonEmail &&
    existingPersonEmailOwnership === CHANNEL_OWNERSHIP.PERSON_SPECIFIC &&
    !primaryLooksLikeOrg
      ? existingPersonEmail
      : null;

  const fallbackRoute = {
    email: orgInbox || (primaryLooksLikeOrg ? existingPersonEmail : null) || null,
    phone:
      opp.primaryContact?.phoneOwnership === CHANNEL_OWNERSHIP.ORGANIZATION_DEPARTMENT
        ? opp.primaryContact?.phone
        : opp.contactPhone || null,
    url:
      opp.publicContactPath ||
      opp.organizationContactUrl ||
      opp.contactUrl ||
      null,
    label: "Organization contact route",
    ownership: CHANNEL_OWNERSHIP.ORGANIZATION_DEPARTMENT,
  };

  const fields = {};
  // Org inbox provenance — never selected as the person's direct email
  if (fallbackRoute.email) {
    fields.organization_email = buildFieldProvenanceRecord({
      fieldKind: FIELD_KINDS.EMAIL,
      value: fallbackRoute.email,
      sourceProvider: opts.existingEmailSource || "existing_canonical_or_official",
      discoveryMethod: "reuse_verified_existing",
      sourceUrl: opts.existingEmailSourceUrl || opp.officialSource || null,
      verificationStatus: "EXISTING",
      selectedForCustomerDisplay: false,
      channelOutcome: CHANNEL_OUTCOME.ACCEPTED_SOURCE_SUPPORTED,
    });
  }
  if (personSpecificEmailAlready) {
    fields[FIELD_KINDS.EMAIL] = buildFieldProvenanceRecord({
      fieldKind: FIELD_KINDS.EMAIL,
      value: personSpecificEmailAlready,
      sourceProvider: "existing_person_attributed",
      discoveryMethod: "reuse_verified_existing",
      verificationStatus: "EXISTING",
      selectedForCustomerDisplay: true,
      channelOutcome: CHANNEL_OUTCOME.ACCEPTED_SOURCE_SUPPORTED,
    });
  }

  let personName = text(opp.primaryContact?.name) || null;
  let personTitle = text(opp.primaryContact?.role || opp.primaryContact?.title) || null;
  let profileUrl = null;
  let responsibility = classifyPersonResponsibility(
    { title: personTitle, relevanceEvidence: opp.primaryContact?.relevanceEvidence },
    opp
  );

  // Lead from opts (e.g. LinkedIn screenshot of president) — verify, don't auto-promote
  if (leadPerson?.name) {
    const leadResp = classifyPersonResponsibility(
      {
        title: leadPerson.title || leadPerson.role,
        relevanceEvidence: leadPerson.relevanceEvidence,
      },
      opp,
      { confirmedBookingAuthority: leadPerson.confirmedBookingAuthority === true }
    );
    personName = leadPerson.name;
    personTitle = leadPerson.title || leadPerson.role || personTitle;
    profileUrl = leadPerson.profileUrl || leadPerson.linkedinUrl || null;
    responsibility = leadResp;

    fields[FIELD_KINDS.NAME] = buildFieldProvenanceRecord({
      fieldKind: FIELD_KINDS.NAME,
      value: personName,
      sourceProvider: leadPerson.sourceProvider || "public_professional_profile_lead",
      discoveryMethod: leadPerson.discoveryMethod || "founder_supplied_lead_to_verify",
      sourceUrl: profileUrl,
      verificationStatus: "LEAD_UNVERIFIED",
      selectedForCustomerDisplay:
        leadResp.class !== RESPONSIBILITY_CLASS.REFERRAL_ENTRY &&
        leadResp.class !== RESPONSIBILITY_CLASS.UNCONFIRMED,
    });
    if (personTitle) {
      fields[FIELD_KINDS.EMPLOYER_TITLE] = buildFieldProvenanceRecord({
        fieldKind: FIELD_KINDS.EMPLOYER_TITLE,
        value: personTitle,
        sourceProvider: leadPerson.titleSource || leadPerson.sourceProvider || "public_professional_profile",
        sourceUrl: profileUrl,
        verificationStatus: "LEAD_UNVERIFIED",
      });
    }
    if (profileUrl) {
      fields[FIELD_KINDS.PROFILE_URL] = buildFieldProvenanceRecord({
        fieldKind: FIELD_KINDS.PROFILE_URL,
        value: profileUrl,
        sourceProvider: "public_professional_profile",
        sourceUrl: profileUrl,
      });
    }
  }

  let surfeResult = null;
  // Org inbox alone does NOT satisfy person-channel completeness — Surfe may still help
  const hasPersonSpecificChannel = Boolean(fields[FIELD_KINDS.EMAIL]?.value);
  const surfeJustified =
    Boolean(personName) &&
    personName.split(/\s+/).length >= 2 &&
    responsibility.class !== RESPONSIBILITY_CLASS.UNCONFIRMED &&
    responsibility.class !== RESPONSIBILITY_CLASS.REFERRAL_ENTRY &&
    (!hasPersonSpecificChannel || opts.forceSurfeDespiteExistingEmail === true);

  if (
    surfeJustified &&
    authorizeSurfe &&
    responsibility.class !== RESPONSIBILITY_CLASS.REFERRAL_ENTRY
  ) {
    surfeResult = await surfeCaller({
      personName,
      organization: opp.organizationName || opp.company,
      domain: opp.officialDomain || leadPerson?.domain,
      title: personTitle,
      hotelId,
      opportunityIds: [opp.id].filter(Boolean),
      workflowRunId,
      authorizeSurfe,
      env,
    });
  } else if (surfeJustified && dryRun && opts.authorizeSurfe === true) {
    surfeResult = {
      status: "skipped",
      reason: "dry_run_blocks_paid_surfe",
    };

    if (
      (surfeResult.status === "accepted" || surfeResult.status === "reused") &&
      surfeResult.email
    ) {
      const existingEmail = fields[FIELD_KINDS.EMAIL];
      const surfeField = buildFieldProvenanceRecord({
        fieldKind: FIELD_KINDS.EMAIL,
        value: surfeResult.email,
        sourceProvider: "surfe",
        discoveryMethod:
          surfeResult.status === "reused"
            ? "surfe_usage_ledger_reuse"
            : "surfe_people_enrichment",
        providerRequestId: surfeResult.enrichmentId,
        verificationStatus: surfeResult.validationStatus || "PROVIDER_RETURNED",
        selectedForCustomerDisplay: true,
        channelOutcome: surfeResult.channelOutcome,
      });
      if (
        existingEmail &&
        String(existingEmail.value).toLowerCase() ===
          String(surfeResult.email).toLowerCase()
      ) {
        fields[FIELD_KINDS.EMAIL] = mergeFieldProvenance(existingEmail, surfeField);
      } else if (existingEmail && opts.preferNamedEmail !== false) {
        // Keep functional as fallback; named Surfe email becomes primary display
        fields[FIELD_KINDS.EMAIL] = surfeField;
        fields.fallback_email = existingEmail;
      } else if (!existingEmail) {
        fields[FIELD_KINDS.EMAIL] = surfeField;
      }
    }
  } else if (
    surfeJustified &&
    responsibility.class === RESPONSIBILITY_CLASS.REFERRAL_ENTRY
  ) {
    surfeResult = {
      status: "skipped",
      reason: "senior_executive_lead_not_automatic_buyer_no_surfe",
    };
  } else if (!personName) {
    surfeResult = {
      status: "skipped",
      reason: "no_named_person_yet_surfe_not_substitute_for_buyer_identification",
    };
  } else if (hasPersonSpecificChannel && !opts.forceSurfeDespiteExistingEmail) {
    surfeResult = {
      status: "skipped",
      reason: "person_specific_channel_exists_paid_enrichment_not_required",
    };
  } else if (
    fallbackRoute.email &&
    !hasPersonSpecificChannel &&
    !authorizeSurfe
  ) {
    surfeResult = {
      status: "skipped",
      reason:
        "org_inbox_only_person_channel_pending_surfe_not_authorized_this_run",
    };
  }

  // Person display email: ONLY person-attributed values — never the org inbox
  const personEmail = fields[FIELD_KINDS.EMAIL]?.value || null;
  const personPhone =
    fields[FIELD_KINDS.PHONE]?.phoneOwnership ===
      CHANNEL_OWNERSHIP.PERSON_SPECIFIC ||
    fields[FIELD_KINDS.PHONE]?.value
      ? fields[FIELD_KINDS.PHONE]?.value
      : null;

  const isReferral =
    responsibility.class === RESPONSIBILITY_CLASS.REFERRAL_ENTRY;
  const isUnconfirmed =
    responsibility.class === RESPONSIBILITY_CLASS.UNCONFIRMED;
  // Referral leads remain visible with clear labeling; unconfirmed names stay off-tile
  const displayName = isUnconfirmed ? null : personName;
  const customerContact = sanitizeContactForCustomerDisplay({
    name: displayName,
    title: personTitle,
    organization: opp.organizationName,
    email: isReferral ? null : personEmail,
    emailOwnership:
      !isReferral && personEmail ? CHANNEL_OWNERSHIP.PERSON_SPECIFIC : null,
    phone: isReferral ? null : personPhone,
    phoneOwnership:
      !isReferral && personPhone ? CHANNEL_OWNERSHIP.PERSON_SPECIFIC : null,
    phoneType: fields[FIELD_KINDS.PHONE]?.phoneType || PHONE_TYPE.UNKNOWN,
    relevanceEvidence: responsibility.rationale,
    whyThisContact: responsibility.rationale,
    publicProfileUrl: profileUrl,
    verificationDate: new Date().toISOString().slice(0, 10),
    relationshipToOpportunity: responsibility.class,
    responsibilityUncertainty:
      responsibility.class === RESPONSIBILITY_CLASS.CONFIRMED_RELEVANT ||
      responsibility.class === RESPONSIBILITY_CLASS.BOOKING_DECISION_MAKER
        ? null
        : responsibility.rationale,
    suggestedContactLabel: displayName
      ? isReferral
        ? `Referral lead: ${displayName}${personTitle ? ` (${personTitle})` : ""} — ask to be directed to the meetings/events contact`
        : `Suggested contact: ${displayName}${personTitle ? ` (${personTitle})` : ""}`
      : null,
    organizationContactRoute: fallbackRoute.email || fallbackRoute.url || null,
    organizationContactRouteLabel: fallbackRoute.email
      ? `Organization contact route: ${fallbackRoute.email}`
      : fallbackRoute.url
        ? `Organization contact route: ${fallbackRoute.url}`
        : null,
    askToBeDirectedTo: displayName
      ? isReferral
        ? `Ask ${displayName} (or the organization route) to direct you to the meetings / events / venue-sourcing contact`
        : `Ask to be directed to ${personTitle || "the relevant meetings / workplace / operations contact"} (${displayName})`
      : null,
  });

  const provenanceBag = buildContactProvenanceBag({
    personName,
    organization: opp.organizationName,
    domain: opp.officialDomain || leadPerson?.domain,
    fields,
    responsibilityClass: responsibility.class,
    relevanceEvidence: responsibility.rationale,
    fallbackRoute,
    enrichmentStatus: beforeAxes.namedResearchStatus,
    unresolvedGaps: [
      ...beforeAxes.queueReasons,
      ...(surfeResult?.reason ? [surfeResult.reason] : []),
      ...(responsibility.class === RESPONSIBILITY_CLASS.REFERRAL_ENTRY
        ? ["president_lead_requires_meetings_staff_identification"]
        : []),
    ],
    nextRefreshDate: opts.nextRefreshDate || null,
    usageLedgerRefs: surfeResult?.enrichmentId
      ? [`surfe:${surfeResult.enrichmentId}`]
      : [],
  });

  // Persist internal canonical person (backend) — never during dry-run
  if (persistCanonical && hotelId && personName) {
    if (typeof canonicalWriter === "function") {
      canonicalWriter({ hotelId, personName, opportunityId: opp.id });
    }
    const doc = loadCanonicalContacts(hotelId);
    const personId =
      doc.people?.find(
        (p) =>
          text(p.displayName).toLowerCase() === text(personName).toLowerCase()
      )?.personId || undefined;
    const entry = buildCanonicalPersonEntry({
      personId,
      displayName: personName,
      organization: opp.organizationName,
      title: personTitle,
      fields: {
        EMAIL: fields[FIELD_KINDS.EMAIL]
          ? {
              value: fields[FIELD_KINDS.EMAIL].value,
              provenance: {
                provider: fields[FIELD_KINDS.EMAIL].originalSourceProvider,
                verificationState: fields[FIELD_KINDS.EMAIL].verificationStatus,
                discoveredAt: fields[FIELD_KINDS.EMAIL].retrievalDate,
                opportunityId: opp.id,
                surfeOutcome:
                  fields[FIELD_KINDS.EMAIL].originalSourceProvider === "surfe"
                    ? surfeResult?.channelOutcome
                    : null,
              },
              acceptedAt: new Date().toISOString(),
            }
          : null,
        PHONE: fields[FIELD_KINDS.PHONE]
          ? {
              value: fields[FIELD_KINDS.PHONE].value,
              phoneType: fields[FIELD_KINDS.PHONE].phoneType,
              provenance: {
                provider: fields[FIELD_KINDS.PHONE].originalSourceProvider,
                verificationState: fields[FIELD_KINDS.PHONE].verificationStatus,
                discoveredAt: fields[FIELD_KINDS.PHONE].retrievalDate,
                opportunityId: opp.id,
              },
            }
          : null,
      },
      identityDecision: surfeResult?.identity?.decision || null,
    });
    const people = [...(doc.people || []).filter((p) => p.personId !== entry.personId), entry];
    const bindings = [
      ...(doc.opportunityBindings || []).filter((b) => b.opportunityId !== opp.id),
      {
        opportunityId: opp.id,
        personId: entry.personId,
        active: customerContact?.name != null,
        responsibilityClass: responsibility.class,
        fallbackRoute,
      },
    ];
    saveCanonicalContacts(hotelId, { ...doc, people, opportunityBindings: bindings });
  }

  // Do not claim named research was attempted merely by invoking this function.
  // Exhaustion requires documented source checks or an explicit markExhausted.
  const sourcesChecked = []
    .concat(opts.sourcesChecked || [])
    .concat(leadPerson?.sourcesChecked || [])
    .filter(Boolean);
  const researchAttempted = Boolean(
    personName ||
      leadPerson?.name ||
      sourcesChecked.length > 0 ||
      opts.markExhausted === true ||
      surfeResult?.status
  );
  const exhaustedNoPerson =
    !personName &&
    (opts.markExhausted === true ||
      sourcesChecked.length > 0 ||
      leadPerson?.exhausted === true);

  let namedStatus = researchAttempted
    ? NAMED_RESEARCH_STATUS.PENDING
    : NAMED_RESEARCH_STATUS.NOT_ATTEMPTED;
  const relevantClass =
    responsibility.class === RESPONSIBILITY_CLASS.CONFIRMED_RELEVANT ||
    responsibility.class === RESPONSIBILITY_CLASS.POTENTIALLY_RELEVANT ||
    responsibility.class === RESPONSIBILITY_CLASS.BOOKING_DECISION_MAKER;
  if (customerContact?.name && relevantClass && personEmail) {
    // Person-specific channel present — research complete for depth purposes
    // (responsibility uncertainty may still be noted separately on the contact)
    namedStatus = NAMED_RESEARCH_STATUS.COMPLETE;
  } else if (customerContact?.name && relevantClass) {
    namedStatus = NAMED_RESEARCH_STATUS.PERSON_ESTABLISHED_CHANNEL_PENDING;
  } else if (
    personName &&
    responsibility.class === RESPONSIBILITY_CLASS.REFERRAL_ENTRY
  ) {
    namedStatus = NAMED_RESEARCH_STATUS.WEAK_RELEVANCE;
  } else if (exhaustedNoPerson) {
    namedStatus = NAMED_RESEARCH_STATUS.EXHAUSTED_ORG_FALLBACK;
  } else if (surfeResult?.status === "identity_conflict") {
    namedStatus = NAMED_RESEARCH_STATUS.STALE;
  } else if (surfeResult?.status === "blocked") {
    namedStatus = NAMED_RESEARCH_STATUS.BLOCKED;
  }

  const depthOutcome = resolveContactDepthOutcome(
    {
      namedResearchStatus: namedStatus,
      namedResearchAttempted: researchAttempted,
      responsibilityClass: responsibility.class,
    },
    {
      exhausted: exhaustedNoPerson,
      identityConflict: surfeResult?.status === "identity_conflict",
      blocked: surfeResult?.status === "blocked",
    }
  );

  const contactDepthResearch = {
    version: BUYER_CONTACT_ENRICHMENT_VERSION,
    attemptedAt: researchAttempted ? new Date().toISOString() : null,
    targetRoles: targetRolesForOpportunity(opp),
    sourcesChecked,
    namedResearchStatus: namedStatus,
    outcome: depthOutcome,
    unresolvedReason:
      depthOutcome === CONTACT_DEPTH_OUTCOME.NAMED_WITH_PERSON_CHANNEL
        ? null
        : depthOutcome === CONTACT_DEPTH_OUTCOME.NAMED_CHANNEL_PENDING
          ? "person_specific_business_channel_still_missing"
          : depthOutcome === CONTACT_DEPTH_OUTCOME.REFERRAL_LEAD_ONLY
            ? "named_person_is_referral_entry_not_confirmed_buyer"
            : depthOutcome === CONTACT_DEPTH_OUTCOME.ORG_FALLBACK_AFTER_RESEARCH
              ? "bounded_named_person_search_exhausted_org_route_retained"
              : surfeResult?.reason || "named_research_incomplete",
    nextStep:
      depthOutcome === CONTACT_DEPTH_OUTCOME.NAMED_CHANNEL_PENDING
        ? "public_or_authorized_provider_channel_enrichment"
        : depthOutcome === CONTACT_DEPTH_OUTCOME.ORG_FALLBACK_AFTER_RESEARCH
          ? "revisit_on_weekly_refresh_or_hotel_feedback"
          : depthOutcome === CONTACT_DEPTH_OUTCOME.REFERRAL_LEAD_ONLY
            ? "identify_meetings_events_or_operations_staff"
            : null,
    nextReviewDate: opts.nextRefreshDate || null,
  };

  const patchedOpp = preservePublicationMetadata(opp, {
    ...opp,
    // Customer-safe primary contact (no Surfe keys; no org inbox as person email)
    primaryContact: customerContact,
    primaryContactName: customerContact?.name || opp.primaryContactName || null,
    primaryContactRole: customerContact?.title || opp.primaryContactRole || null,
    primaryContactEmail: personEmail || null,
    primaryContactPhone: personPhone || null,
    primaryContactPhoneType: customerContact?.phoneType || null,
    primaryContactPhoneTypeLabel: customerContact?.phoneTypeLabel || null,
    functionalContactEmail:
      fallbackRoute.email || opp.functionalContactEmail || null,
    organizationContactEmail:
      fallbackRoute.email || opp.organizationContactEmail || null,
    contactResponsibilityClass: responsibility.class,
    namedContactResearchStatus: namedStatus,
    namedContactResearchAttempted: researchAttempted,
    namedContactResearchCompletedAt: new Date().toISOString(),
    contactDepthResearch,
    contactChannelSeparation: {
      suggestedPerson: customerContact?.name || null,
      personSpecificEmail: personEmail,
      personSpecificPhone: personPhone,
      organizationRoute: fallbackRoute.email || fallbackRoute.url || null,
      askToBeDirectedTo: customerContact?.askToBeDirectedTo || null,
    },
    // Internal bag — never copy to customer DTO
    _internalContactProvenance: provenanceBag,
  });

  const afterAxes = assessContactResearchAxes(patchedOpp, {
    responsibilityClass: responsibility.class,
  });
  const eligAfter = evaluateProspectPublicationEligibility(patchedOpp);

  return {
    version: BUYER_CONTACT_ENRICHMENT_VERSION,
    contractVersion: BUYER_CONTACT_ENRICHMENT_CONTRACT_VERSION,
    opportunityId: opp.id,
    hotelId,
    before: {
      axes: beforeAxes,
      eligibility: eligBefore.eligibility,
      contact: {
        name: opp.primaryContact?.name || null,
        email: fallbackRoute.email,
        phone: fallbackRoute.phone,
      },
    },
    after: {
      axes: afterAxes,
      eligibility: eligAfter.eligibility,
      customerContact,
      responsibility,
    },
    surfe: surfeResult
      ? {
          // Internal summary — not for customer surfaces
          status: surfeResult.status,
          reason: surfeResult.reason || null,
          channelOutcome: surfeResult.channelOutcome || null,
          enrichmentId: surfeResult.enrichmentId || null,
          creditsConsumed:
            surfeResult.creditsConsumed === undefined
              ? null
              : surfeResult.creditsConsumed,
          estimatedCostUsd: surfeResult.estimatedCostUsd ?? null,
          emailAccepted: surfeResult.status === "accepted",
        }
      : null,
    provenanceBag,
    patchedOpportunity: patchedOpp,
    materialImprovement: Boolean(
      (customerContact?.name && !opp.primaryContact?.name) ||
        (surfeResult?.status === "accepted" && surfeResult.email) ||
        (personEmail &&
          personEmail !== fallbackRoute.email &&
          responsibility.class === RESPONSIBILITY_CLASS.CONFIRMED_RELEVANT) ||
        // Channel-ownership correction: org inbox previously shown as person email
        (customerContact?.name &&
          primaryLooksLikeOrg &&
          !personEmail &&
          Boolean(fallbackRoute.email))
    ),
    publicationPreserved:
      eligBefore.hotelFacing === true
        ? eligAfter.hotelFacing === true || eligAfter.eligibility !== "NOT_A_PROSPECT"
        : true,
  };
}

/**
 * Run bounded enrichment for a queue of opportunities.
 */
export async function runBuyerContactEnrichmentPass({
  hotelId,
  opportunities = [],
  queueItems = null,
  leadPeopleByOpportunityId = {},
  authorizeSurfe = false,
  dryRun = false,
  workflowRunId = null,
  outDir = null,
  env = process.env,
  maxItems = 8,
  surfeCaller = null,
  canonicalWriter = null,
  persistCanonical = true,
  // Resume/retry may restore flags from saved stage state — re-resolve here.
  restoredAuthorization = null,
} = {}) {
  const restored =
    restoredAuthorization && typeof restoredAuthorization === "object"
      ? restoredAuthorization
      : {};
  const effectiveDryRun =
    dryRun === true || restored.dryRun === true;
  const effectiveAuthorizeSurfe =
    authorizeSurfe === true || restored.authorizeSurfe === true;
  const surfeAuth = resolveEffectiveSurfeAuthorization({
    dryRun: effectiveDryRun,
    authorizeSurfe: effectiveAuthorizeSurfe,
  });

  const queue =
    queueItems ||
    buildBuyerContactEnrichmentQueue({
      hotelId,
      opportunities,
      maxItems,
      prioritizeIds: Object.keys(leadPeopleByOpportunityId),
    });
  const byId = new Map((opportunities || []).map((o) => [o.id, o]));
  const results = [];

  for (const item of queue.items || []) {
    const opp = byId.get(item.opportunityId);
    if (!opp) continue;
    const result = await enrichBuyerContactForOpportunity(opp, {
      hotelId,
      // Pass requested Surfe flag; enrichBuyerContactForOpportunity re-resolves
      // so dryRun always wins over authorizeSurfe (incl. resume-restored auth).
      authorizeSurfe: effectiveAuthorizeSurfe,
      dryRun: effectiveDryRun,
      workflowRunId,
      env,
      leadPerson: leadPeopleByOpportunityId[item.opportunityId] || null,
      existingEmailSourceUrl: item.existingRoute?.url,
      persistCanonical: persistCanonical !== false && !effectiveDryRun,
      surfeCaller: surfeCaller || undefined,
      canonicalWriter: canonicalWriter || undefined,
    });
    // Re-assert auth on nested call: dry-run must win even if caller passed authorizeSurfe
    if (effectiveDryRun && result?.surfe?.status === "accepted") {
      const err = new Error("dry_run_paid_surfe_violation");
      err.code = "DRY_RUN_PAID_VIOLATION";
      throw err;
    }
    results.push(result);
    if (outDir) {
      fs.mkdirSync(outDir, { recursive: true });
      fs.writeFileSync(
        path.join(outDir, `${item.opportunityId}.json`),
        JSON.stringify(result, null, 2) + "\n"
      );
    }
  }

  let remainingUsd = null;
  try {
    remainingUsd = remainingSharedBudgetUsd({ env });
  } catch (err) {
    // Reporting must not bootstrap an empty auth ledger. Missing ledger → null.
    if (err?.code === "PAID_MS_LEDGER_MISSING") {
      remainingUsd = null;
    } else {
      throw err;
    }
  }

  return {
    version: BUYER_CONTACT_ENRICHMENT_VERSION,
    hotelId,
    workflowRunId,
    generatedAt: new Date().toISOString(),
    dryRun: effectiveDryRun,
    authorizeSurfeRequested: effectiveAuthorizeSurfe,
    authorizeSurfeEffective: surfeAuth.authorizeSurfeEffective,
    queue,
    results,
    remainingSharedBudgetUsd: remainingUsd,
  };
}

export function writeBuyerEnrichmentArtifacts(pass, outDir) {
  fs.mkdirSync(outDir, { recursive: true });
  fs.writeFileSync(
    path.join(outDir, "buyer-contact-enrichment-pass.json"),
    JSON.stringify(pass, null, 2) + "\n"
  );
  const register = (pass.results || []).map((r) => ({
    opportunityId: r.opportunityId,
    before: r.before?.contact,
    after: r.after?.customerContact,
    responsibility: r.after?.responsibility?.class,
    materialImprovement: r.materialImprovement,
    publicationPreserved: r.publicationPreserved,
    namedStatus: r.after?.axes?.namedResearchStatus,
    surfeStatus: r.surfe?.status || null,
  }));
  fs.writeFileSync(
    path.join(outDir, "before-after-contact-register.json"),
    JSON.stringify({ generatedAt: pass.generatedAt, rows: register }, null, 2) +
      "\n"
  );

  const scorecards = (pass.results || []).map((r) =>
    scorecardFromBuyerEnrichment(r, {
      runId: pass.workflowRunId,
      hotelId: pass.hotelId,
      applied: Boolean(r.materialImprovement),
    })
  );
  writeScorecardArtifacts(
    {
      version: RESEARCH_SCORECARD_VERSION,
      generatedAt: pass.generatedAt || new Date().toISOString(),
      source: "buyer_contact_enrichment_v1",
      remainingSharedBudgetUsd: pass.remainingSharedBudgetUsd ?? null,
      tasks: scorecards,
    },
    path.join(outDir, "scorecard")
  );

  return outDir;
}
