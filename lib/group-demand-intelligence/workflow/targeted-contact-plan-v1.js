/**
 * Targeted contact resolution plan — only for qualified / enrichment-queue
 * candidates with missing or inadequate routes. Does not enrich every raw hit.
 * Surfe remains opt-in behind existing spend authorization.
 */

import {
  evaluateProspectPublicationEligibility,
  extractContactSurface,
  hasUsableContactRoute,
  classifyContactCompletenessGroup,
  CONTACT_COMPLETENESS_GROUP,
} from "../prospect-publication-eligibility-v1.js";
import { hasNamedPerson, isLikelyPersonName } from "../contact-resolution.js";
import { REUSE_DISPOSITION } from "./reuse-prior-intelligence-v1.js";
import {
  evaluateContactChannels,
  buildSurfeFallbackQueue,
} from "../parallel/contact-channel-policy-v1.js";
import { isGdiParallelSurfeFallbackEnabled } from "../parallel/feature-flag.js";

export const TARGETED_CONTACT_PLAN_VERSION = "gdi-targeted-contact-plan-v1";

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

function isHomepageOnly(url) {
  return /^https?:\/\/[^/]+\/?(?:en\/|es\/|de\/)?(?:home\/?)?(?:index\.(?:html?|php))?$/i.test(
    text(url)
  );
}

/**
 * Decide whether a candidate needs targeted contact work.
 */
export function needsTargetedContactResolution(opp = {}, reuseRow = null) {
  const elig = evaluateProspectPublicationEligibility(opp);
  const surface = extractContactSurface(opp);
  const route = hasUsableContactRoute(opp);
  const group =
    opp.contactCompletenessGroup ?? classifyContactCompletenessGroup(opp);

  if (elig.eligibility === "NOT_A_PROSPECT") {
    return {
      needed: false,
      reason: "not_a_prospect",
      group,
    };
  }

  const disposition = reuseRow?.disposition || null;
  const enrichment =
    elig.enrichmentQueue === true ||
    disposition === REUSE_DISPOSITION.ENRICHMENT_NEEDED;

  const inadequate =
    group === CONTACT_COMPLETENESS_GROUP.NO_USABLE_ROUTE ||
    !route.ok ||
    (surface.contactUrl &&
      isHomepageOnly(surface.contactUrl) &&
      !surface.email &&
      !surface.phone);

  if (!enrichment && !inadequate && elig.hotelFacing) {
    return { needed: false, reason: "already_usable_or_facing", group };
  }

  if (inadequate || enrichment) {
    return {
      needed: true,
      reason: inadequate ? "missing_or_inadequate_route" : "enrichment_queue",
      group,
      eligibility: elig.eligibility,
    };
  }

  return { needed: false, reason: "not_selected", group };
}

/**
 * Build a contact work plan from reuse ledger + opportunities.
 * Plan only — does not call Surfe or native-who unless opts.executeLocal is true
 * (still no Surfe without authorizeSurfe).
 */
export function buildTargetedContactPlan({
  hotelId,
  opportunities = [],
  reuseRows = [],
  authorizeSurfe = false,
  maxTargets = 25,
  env = process.env,
} = {}) {
  const byId = new Map(reuseRows.map((r) => [r.sourceRecordId, r]));
  const targets = [];
  const surfeEvalRows = [];
  const surfeFlagOn = isGdiParallelSurfeFallbackEnabled(env);
  const surfeAuthorized = authorizeSurfe === true && surfeFlagOn;

  for (const opp of opportunities) {
    const id = opp.id || opp.opportunityId;
    const decision = needsTargetedContactResolution(opp, byId.get(id));
    if (!decision.needed) continue;

    const surface = extractContactSurface(opp);
    const named = hasNamedPerson(opp.primaryContact || { name: surface.person });
    const channelEval = evaluateContactChannels(
      {
        name: surface.person,
        email: surface.email,
        phone: surface.phone,
        identityKind: named ? "named_person" : "unknown",
        role: surface.title,
        source: "existing_opportunity",
        emailQuality: opp.primaryContact?.emailVerification?.status,
        phoneQuality: opp.primaryContact?.phoneVerification?.status,
      },
      { organizationName: opp.organizationName || opp.company }
    );
    surfeEvalRows.push({
      ...channelEval,
      personName: surface.person,
      organizationName: opp.organizationName || opp.company,
      opportunityId: id,
      contact: { name: surface.person },
    });
    targets.push({
      opportunityId: id,
      organizationName: opp.organizationName || opp.company || null,
      title: opp.title || null,
      reason: decision.reason,
      contactCompletenessGroup: decision.group,
      eligibility: decision.eligibility || null,
      channelEvaluation: channelEval,
      existing: {
        person: surface.person || null,
        email: surface.email || null,
        phone: surface.phone || null,
        formUrl: surface.contactUrl || null,
        namedPerson: named,
        likelyPersonName: isLikelyPersonName(surface.person),
        hasUsableEmailOrPhone: channelEval.hasUsableChannel,
        routeClass: channelEval.routeClass,
      },
      resolutionOrder: [
        "reuse_existing_verified_records",
        "official_organization_or_event_sources",
        "native_who_public_professional",
        surfeAuthorized && channelEval.surfeEligible
          ? "surfe_fallback_within_authorized_spend"
          : channelEval.surfeEligible
            ? "surfe_eligible_but_not_authorized"
            : "surfe_skipped_not_eligible_or_not_authorized",
      ],
      preserveFields: [
        "personIdentity",
        "organization",
        "role",
        "usableMethod",
        "phoneType",
        "source",
        "verificationDate",
        "responsibilityClass",
        "perChannelVerification",
      ],
      routingOnlyPolicy:
        "Routing-only contacts must be relevant usable routes, not generic homepages. Do not describe surviving routes as verified named contacts. Generic switchboard ≠ usable phone.",
      secondChannelPolicy:
        "Email OR phone is enough — do not purchase a second channel automatically.",
    });
    if (targets.length >= maxTargets) break;
  }

  const surfeQueue = buildSurfeFallbackQueue(
    surfeEvalRows.filter((r) => r.surfeEligible)
  );

  return {
    version: TARGETED_CONTACT_PLAN_VERSION,
    hotelId,
    asOf: new Date().toISOString(),
    authorizeSurfe: authorizeSurfe === true,
    surfeFlagEnabled: surfeFlagOn,
    surfeInvoked: false,
    targetCount: targets.length,
    targets,
    surfeFallbackQueue: surfeAuthorized
      ? surfeQueue
      : {
          ...surfeQueue,
          queue: [],
          deferredQueue: surfeQueue.queue,
          note: surfeAuthorized
            ? surfeQueue.note
            : "Surfe queue computed but not authorized (need --authorize-surfe + GDI_PARALLEL_SURFE_FALLBACK_ENABLED)",
        },
    notes: [
      "Contact resolution runs only on qualified/enrichment candidates with inadequate routes.",
      "Sequence: saved evidence → official/public → Surfe only when named person+org and no usable email/phone.",
      "Surfe is not invoked unless authorizeSurfe=true AND GDI_PARALLEL_SURFE_FALLBACK_ENABLED=1.",
      "Routing-only ≠ verified named contact. Provider validation ≠ officially published.",
    ],
  };
}

/**
 * Summarize contact identity fields for drawer / reports.
 */
export function summarizeContactIdentity(contact = {}, opts = {}) {
  const name = text(contact.name || contact.person);
  const named = isLikelyPersonName(name);
  return {
    personIdentity: named ? name : null,
    organization: text(contact.organization || opts.organizationName) || null,
    role: text(contact.role || contact.title) || null,
    usableMethod: text(contact.email || contact.phone || contact.formUrl) || null,
    phoneType: contact.phoneType || contact.phoneKind || null,
    source: contact.source || contact.identitySource || null,
    verificationDate: contact.verificationDate || contact.verifiedAt || null,
    responsibilityClass:
      contact.responsibilityClass ||
      contact.identityKind ||
      (named ? "named_person" : "routing_or_functional"),
    isVerifiedNamedContact: named && Boolean(contact.email || contact.phone),
  };
}
