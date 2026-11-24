/**
 * GDI Finalized Qualified Opportunity (FQO) contract V1
 *
 * Founder-approved product policy: local STAGED presentation eligibility.
 * Sales Ready is NOT required for FQO.
 *
 * Remediation P0 (Bethesda): evidence inheritance, labeled org routes,
 * structured fit/action (not length), market-local competitive incumbents,
 * historical presentation provenance compatibility.
 *
 * Does not write Airtable, activate STAGED/CURRENT pointers, or spend providers.
 */
import { looksLikePersonName } from "../secondary-named-contact-v1.js";
import { isPlausibleBuyerPersonName } from "./gdi-original-full-e2e-execution-restore-v1.js";
import { evaluateContactCompletenessForFinalization } from "../presentation/gdi-verified-commercial-route-v1.js";
import {
  evaluateNamedContactCoverageStandard,
  NAMED_CONTACT_COVERAGE,
} from "./gdi-named-contact-coverage-standard-v1.js";
import { isHistoricalOnlyCustomerExclusion } from "../commercial-inventory/gdi-unified-commercial-inventory-contract-v1.js";

const FQO_JUNK_NAME_RE =
  /\b(headquarters|enquire|beyond|colonial|dames|climate|monitoring|android|once|member\s*select|learning\s*futures|free\s*univ|booth|onpeak)\b/i;

/** Stricter than generic looksLikePersonName — rejects desks, orgs, and junk scrapes. */
function fqoLooksLikePersonName(name) {
  const n = text(name);
  if (!n || FQO_JUNK_NAME_RE.test(n)) return false;
  return isPlausibleBuyerPersonName(n) && looksLikePersonName(n);
}

export const GDI_FINALIZED_QUALIFIED_OPPORTUNITY_CONTRACT_V1 =
  "gdi-finalized-qualified-opportunity-contract-v1";

export const FQO_STATUS = Object.freeze({
  FINALIZED_QUALIFIED: "FINALIZED_QUALIFIED",
  RESEARCH_CONTINUATION: "RESEARCH_CONTINUATION",
  FOUNDER_HELD: "FOUNDER_HELD",
  FOUNDER_REJECTED: "FOUNDER_REJECTED",
  INELIGIBLE: "INELIGIBLE",
});

export const FQO_GATE = Object.freeze({
  EVIDENCE_BACKED_DEMAND: "EVIDENCE_BACKED_DEMAND",
  HOTEL_GEO_COMMERCIAL_FIT: "HOTEL_GEO_COMMERCIAL_FIT",
  FUTURE_SOURCING_PATHWAY: "FUTURE_SOURCING_PATHWAY",
  NAMED_COMMERCIAL_PERSON: "NAMED_COMMERCIAL_PERSON",
  PERSON_SPECIFIC_CHANNEL: "PERSON_SPECIFIC_CHANNEL",
  HOTEL_SPECIFIC_ACTION: "HOTEL_SPECIFIC_ACTION",
  COMPETITIVE_INCUMBENT_PATHWAY: "COMPETITIVE_INCUMBENT_PATHWAY",
});

/** Internal processing dispositions (not customer-facing categories). */
export const FQO_PROCESSING_DISPOSITION = Object.freeze({
  RETAINED_HISTORICAL_APPROVAL: "RETAINED_HISTORICAL_APPROVAL",
  QUALIFIED_NEW: "QUALIFIED_NEW",
  GENUINE_COMMERCIAL_REVIEW: "GENUINE_COMMERCIAL_REVIEW",
  TECHNICAL_REMEDIATION_REQUIRED: "TECHNICAL_REMEDIATION_REQUIRED",
});

const FUNCTIONAL_ORG_MAILBOX_RE =
  /^(info|contact|hello|press|pr|media|sales|support|admin|office|enquiries|inquiries|events?|meetings?|groups?|housing|lodging|reservations?|booking|noreply|no-reply|team|general|help|group[-_.]|comcoor|pcchair|paleo)@/i;

const PLACEHOLDER_RE =
  /CONTACT_RESEARCH_REQUIRED|TODO_RESEARCH|PLACEHOLDER|TBD_CONTACT|BUYER NOT YET|ROLE NOT YET/i;

const UNSUPPORTED_SPECULATION_RE =
  /\b(maybe|might be|could possibly|unverified scrape|random exhibitor|no evidence|speculation only)\b/i;

const ACTIONABLE_RE =
  /\b(outreach|contact|ask|monitor|introduce|propose|call|email|build|pursue|qualify|confirm|reach|engage|offer|bid|rfp|housing|block)\b/i;

const HOTEL_FIT_RE =
  /\b(hotel|lodging|housing|rooms?|guestrooms?|overflow|sourcing|marriott|bethesda|midtown|fit|capture|accommodate|stay)\b/i;

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

function upper(v) {
  return text(v).toUpperCase();
}

function pushUrl(set, u) {
  const t = text(u);
  if (/^https?:\/\//i.test(t)) set.add(t.replace(/[),.;]+$/, ""));
}

/**
 * Collect evidence URLs from canonical + presentation provenance fields.
 * Does not scrape arbitrary narrative for *new* qualification unless field is a known provenance slot.
 */
export function collectEvidenceUrls(opp = {}) {
  const urls = new Set();
  for (const u of [
    ...(Array.isArray(opp.sourceUrls) ? opp.sourceUrls : []),
    ...(Array.isArray(opp.seedUrls) ? opp.seedUrls : []),
    ...(Array.isArray(opp.investigatedUrls) ? opp.investigatedUrls : []),
    ...(Array.isArray(opp.additionalEvidenceUrls)
      ? opp.additionalEvidenceUrls
      : []),
  ]) {
    pushUrl(urls, typeof u === "string" ? u : u?.url);
  }
  for (const e of Array.isArray(opp.evidence) ? opp.evidence : []) {
    pushUrl(urls, e?.url || e?.sourceUrl);
  }
  for (const s of Array.isArray(opp.sources) ? opp.sources : []) {
    pushUrl(urls, typeof s === "string" ? s : s?.url || s?.sourceUrl);
  }
  for (const c of Array.isArray(opp.citations) ? opp.citations : []) {
    pushUrl(urls, typeof c === "string" ? c : c?.url || c?.sourceUrl);
  }
  pushUrl(urls, opp.sourceUrl);
  pushUrl(urls, opp.locationEvidenceUrl);
  pushUrl(urls, opp.primaryContact?.sourceUrl);
  pushUrl(urls, opp.primaryContact?.emailSource);
  pushUrl(urls, opp.provenance?.sourceUrl);
  pushUrl(urls, opp.provenance?.url);

  const timingEvidence =
    opp.tileBadgePlan?.canonicalTiming?.dateEvidence ||
    opp.canonicalTiming?.dateEvidence ||
    [];
  for (const d of Array.isArray(timingEvidence) ? timingEvidence : []) {
    pushUrl(urls, d?.source || d?.url || d?.sourceUrl);
  }
  for (const ref of opp.tileBadgePlan?.canonicalTiming?.dateSourceRefs || []) {
    pushUrl(urls, typeof ref === "string" ? ref : ref?.url);
  }

  // Known prose provenance slots (not full-object scrape)
  for (const field of [
    opp.cardWhyNowLine,
    opp.primaryContact?.relevanceEvidence,
    opp.eventsOpportunityOverview?.venueConfirmationNote,
  ]) {
    const blob = text(field);
    for (const m of blob.match(/https?:\/\/[^\s)"']+/g) || []) {
      pushUrl(urls, m);
    }
  }

  return [...urls];
}

/**
 * Evidence-supported demand (plausible / inferred / confirmed).
 * Historical approved presentation packages may lack top-level sourceUrls —
 * that is schema incompatibility, not automatic rejection.
 */
export function evaluateEvidenceSupportedDemand(opp = {}, opts = {}) {
  const demand = text(
    opp.organizationName ||
      opp.organization ||
      opp.programmeName ||
      opp.demandGenerator ||
      opp.title ||
      opp.displayTitle,
  );
  const urls = collectEvidenceUrls(opp);
  const historical =
    opts.historicalApprovedInventory === true ||
    opp._historicalApprovedInventory === true ||
    opp.historicalApprovedInventory === true;

  const claim = text(
    opp.cardWhyNowLine ||
      opp.summaryWhat ||
      opp.whatIsHappening ||
      opp.hotelOpportunityThesis ||
      opp.whyThisHotel ||
      "",
  );

  if (!demand) {
    return {
      ok: false,
      mode: null,
      urls,
      reason: "MISSING_DEMAND_GENERATOR",
      schemaIncompatibility: false,
    };
  }
  if (UNSUPPORTED_SPECULATION_RE.test(claim) && urls.length === 0) {
    return {
      ok: false,
      mode: null,
      urls,
      reason: "UNSUPPORTED_SPECULATION",
      schemaIncompatibility: false,
    };
  }
  if (urls.length > 0) {
    return {
      ok: true,
      mode: "URL_BACKED",
      urls,
      reason: null,
      schemaIncompatibility: false,
    };
  }
  if (
    opp.canonicalCustomerPresentation?.evidencePresent === true ||
    (Array.isArray(opp.evidence) && opp.evidence.length > 0)
  ) {
    return {
      ok: true,
      mode: "STRUCTURED_EVIDENCE_FLAG",
      urls,
      reason: null,
      schemaIncompatibility: false,
    };
  }
  if (historical && claim.length > 0 && !PLACEHOLDER_RE.test(claim)) {
    return {
      ok: true,
      mode: "HISTORICAL_PRESENTATION_PROVENANCE",
      urls,
      reason: null,
      schemaIncompatibility: true,
      note: "Approved presentation package lacks top-level sourceUrls; commercial claim retained as historical provenance",
    };
  }
  return {
    ok: false,
    mode: null,
    urls,
    reason: "MISSING_EVIDENCE_URLS",
    schemaIncompatibility: historical,
  };
}

/**
 * Outreach channel: person-specific email/phone/LinkedIn OR labeled org route
 * tied to a verified named person. Direct personal email is not mandatory.
 * Org routes must be labeled — never presented as personal.
 */
export function evaluatePersonSpecificOutreachChannel(opportunity = {}, opts = {}) {
  const primary =
    opportunity.primaryContact && typeof opportunity.primaryContact === "object"
      ? opportunity.primaryContact
      : {
          name: opportunity.primaryContactName || opportunity.contactName,
          email: opportunity.primaryContactEmail || opportunity.contactEmail,
          phone: opportunity.primaryContactPhone || opportunity.contactPhone,
          linkedinUrl:
            opportunity.primaryContactLinkedin ||
            opportunity.linkedinUrl ||
            opportunity.contactLinkedin,
          emailType: opportunity.primaryContactEmailType,
        };

  const name = text(primary.name);
  const namedOk =
    typeof opts.looksLikePersonName === "function"
      ? opts.looksLikePersonName(name)
      : fqoLooksLikePersonName(name);
  const email = text(primary.email);
  const phone = text(primary.phone || primary.mobile || primary.directPhone);
  const linkedin = text(
    primary.linkedinUrl || primary.linkedin || primary.profileUrl,
  );
  const emailType = upper(primary.emailType || primary.emailClassification);
  const emailOwnership = upper(primary.emailOwnership || "");

  const channels = [];
  const rejected = [];

  if (email && /@/.test(email)) {
    if (/%[0-9a-f]{2}|\s/i.test(email)) {
      rejected.push({
        kind: "EMAIL",
        value: email,
        reason: "MALFORMED_OR_ENCODED_EMAIL",
      });
    } else if (
      FUNCTIONAL_ORG_MAILBOX_RE.test(email) ||
      emailType === "GENERIC_ORGANIZATION" ||
      emailType === "ORG_FUNCTIONAL" ||
      emailType === "SWITCHBOARD"
    ) {
      rejected.push({
        kind: "EMAIL",
        value: email,
        reason: "GENERIC_OR_FUNCTIONAL_ORGANIZATION_INBOX",
      });
    } else if (
      emailOwnership === "ORGANIZATION" ||
      emailOwnership === "ORG" ||
      emailType === "ORGANIZATION_ROUTE"
    ) {
      // Labeled org email — not a personal channel claim.
      rejected.push({
        kind: "EMAIL",
        value: email,
        reason: "ORGANIZATION_EMAIL_NOT_PERSONAL",
      });
    } else {
      channels.push({
        kind: "DIRECT_WORK_EMAIL",
        value: email,
        provenance: text(
          primary.emailSource ||
            primary.emailProvenance ||
            primary.sourceUrl ||
            opportunity.contactChannelProvenance,
        ),
      });
    }
  }

  const contactKind = upper(
    primary.contactKind || primary.kind || opportunity.primaryContactKind || "",
  );
  const phoneType = upper(primary.phoneType || "");
  const phoneTypeLabel = text(primary.phoneTypeLabel || "");
  if (phone && /\d{7,}/.test(phone.replace(/\D/g, ""))) {
    if (
      contactKind === "FUNCTIONAL" ||
      phoneType === "SWITCHBOARD" ||
      phoneType === "ORGANIZATION" ||
      /headquarters|housing desk|main office/i.test(
        text(primary.role || primary.title || ""),
      )
    ) {
      rejected.push({
        kind: "PHONE",
        value: phone,
        reason: "ORGANIZATION_OR_FUNCTIONAL_PHONE",
      });
    } else if (/agency|organization|org\s*route|main\s*line/i.test(phoneTypeLabel)) {
      rejected.push({
        kind: "PHONE",
        value: phone,
        reason: "LABELED_AGENCY_OR_ORG_PHONE_NOT_PERSONAL",
      });
    } else {
      channels.push({
        kind: "DIRECT_PROFESSIONAL_PHONE",
        value: phone,
        provenance: text(primary.phoneSource || primary.sourceUrl || ""),
      });
    }
  }

  if (/linkedin\.com\/in\//i.test(linkedin)) {
    channels.push({
      kind: "PERSON_LINKEDIN",
      value: linkedin,
      provenance: text(primary.linkedinSource || primary.sourceUrl || ""),
    });
  }

  // Labeled organizational outreach route connected to the named person.
  // Requires an explicit label — a bare generic inbox/phone is not enough.
  const orgRouteLabel = text(
    primary.organizationContactRouteLabel ||
      opportunity.organizationContactRouteLabel ||
      opportunity.approvedCommercialRoute,
  );
  const askDirect = text(
    primary.askToBeDirectedTo || opportunity.askToBeDirectedTo,
  );
  const agencyPhoneLabeled =
    phone &&
    /\d{7,}/.test(phone.replace(/\D/g, "")) &&
    /agency|organization route|org\s*route/i.test(phoneTypeLabel);
  const explicitLabeledRoute = Boolean(
    orgRouteLabel || askDirect || agencyPhoneLabeled,
  );

  if (channels.length === 0 && namedOk && explicitLabeledRoute) {
    const routeValue =
      orgRouteLabel ||
      askDirect ||
      (agencyPhoneLabeled ? `agency-phone:${phone}` : null) ||
      "labeled-organizational-route";
    channels.push({
      kind: "LABELED_ORGANIZATIONAL_ROUTE",
      value: routeValue,
      labeledAsPersonal: false,
      provenance: text(
        primary.sourceUrl || opportunity.contactChannelProvenance || "",
      ),
      connectedNamedPerson: name,
    });
  }

  const ok = channels.length > 0;
  return {
    ok,
    name: name || null,
    channels,
    rejected,
    reason: ok
      ? channels.some((c) => c.kind === "LABELED_ORGANIZATIONAL_ROUTE")
        ? "Verified named person with labeled organizational outreach pathway"
        : "At least one verified person-specific professional channel"
      : rejected.length
        ? "Organizational details present but not labeled as a connected outreach pathway"
        : "No person-specific email, direct phone, person LinkedIn, or labeled org route",
  };
}

function isCompetitive(opp = {}) {
  const scope = upper(
    typeof opp.marketScope === "string"
      ? opp.marketScope
      : opp.marketScope?.inventoryScope || opp.inventoryScope || "",
  );
  return (
    scope === "COMPETITIVE" ||
    opp.competitiveMarketScopeEligible === true ||
    Boolean(opp.competitiveContext?.incumbentHotel) ||
    Boolean(opp.competitiveContext?.label) ||
    Boolean(opp.incumbentHotel || opp.competitorHotel || opp.likelyCompetitor) ||
    /CONFIRMED_COMPETITOR|EVIDENCED_OVERFLOW|OVERFLOW_OPPORTUNITY/i.test(
      text(opp.competitiveStatusLabel || opp.competitiveContext?.label || ""),
    )
  );
}

function hasFuturePathway(opp = {}, opts = {}) {
  if (opp.futureDemandPathway === true) return true;
  if (opp.accountDevelopmentReactivated === true) return true;
  const historical =
    opts.historicalApprovedInventory === true ||
    opp._historicalApprovedInventory === true ||
    opp.historicalApprovedInventory === true;
  const state = upper(
    opp.futureCycleEvidenceState ||
      opp.commercialActionabilityStateV1 ||
      opp.commercialTemporalDisposition ||
      "",
  );
  if (
    /CURRENT_FUTURE|FUTURE_CYCLE|OPEN_SOURCING|ACTIVE|ENTERING_WINDOW|WATCH_FUTURE/.test(
      state,
    )
  ) {
    return true;
  }
  if (isHistoricalOnlyCustomerExclusion(opp)) return false;
  const start = Date.parse(
    opp.eventStartDate ||
      opp.tileEventStartDate ||
      opp.needDates ||
      opp.needDateStart ||
      opp.eventDate ||
      "",
  );
  const asOfRaw = opp._asOf;
  const asOf =
    asOfRaw != null && asOfRaw !== "" ? Date.parse(asOfRaw) : Date.now();
  const asOfMs = Number.isFinite(asOf) ? asOf : Date.now();
  if (Number.isFinite(start)) {
    if (start >= asOfMs - 7 * 86400000) return true;
  }
  const pathway = upper(
    opp.competitiveContext?.pathway ||
      opp.opportunityPathway ||
      opp.sourcingPathway ||
      "",
  );
  if (/OVERFLOW|FUTURE_CYCLE|NEXT_CYCLE|OPEN_HOUSING|STAY.?TO.?PLAY|RECURRING/.test(pathway)) {
    return true;
  }
  // Recurring / next-edition language in timing notes (not bare speculation)
  const timingNotes = text(
    opp.tileBadgePlan?.canonicalTiming?.timingNotes ||
      opp.eventsOpportunityOverview?.demandCycle ||
      opp.futureCycleOrSourcingWindow ||
      "",
  );
  if (/recurring|next (edition|cycle|year)|annual|future cycle|202[6-9]|2030/i.test(timingNotes)) {
    return true;
  }
  const thesis = text(
    opp.hotelOpportunityThesis || opp.whyThisHotel || opp.whatIsHappening || opp.summaryWhat,
  );
  if (
    !isHistoricalOnlyCustomerExclusion(opp) &&
    /\b(202[6-9]|2030|next cycle|future|upcoming|overflow|sourcing|recurring|annual)\b/i.test(
      thesis,
    )
  ) {
    return true;
  }
  // Historical approved open accounts / watches: no dated event is not a
  // commercial expiration. Past-dated one-time events still fail above unless
  // an explicit future-cycle signal already returned true.
  if (historical && !isHistoricalOnlyCustomerExclusion(opp)) {
    if (!Number.isFinite(start)) return true;
    if (start >= asOfMs - 7 * 86400000) return true;
  }
  return false;
}

function hotelFitOk(opp = {}) {
  const structured = text(
    opp.hotelOpportunityThesis ||
      opp.whyThisHotel ||
      opp.summaryWhyMatters ||
      opp.potentialBusinessForThisHotel,
  );
  const narrative = text(opp.summaryWhat || opp.whatIsHappening || "");
  const thesis = structured || narrative;
  if (!thesis || PLACEHOLDER_RE.test(thesis)) {
    return { ok: false, thesis, basis: null };
  }
  if (opp.hotelMarketScopeEligible === false && !HOTEL_FIT_RE.test(thesis)) {
    return { ok: false, thesis, basis: null };
  }
  if (structured && (HOTEL_FIT_RE.test(structured) || opp.hotelMarketScopeEligible !== false)) {
    if (HOTEL_FIT_RE.test(structured) || opp.hotelMarketScopeEligible === true) {
      return { ok: true, thesis: structured, basis: "STRUCTURED_THESIS" };
    }
  }
  if (opp.hotelMarketScopeEligible === true && thesis) {
    return { ok: true, thesis, basis: "MARKET_SCOPE_PLUS_NARRATIVE" };
  }
  if (HOTEL_FIT_RE.test(thesis)) {
    return { ok: true, thesis, basis: "HOTEL_RELEVANCE_LANGUAGE" };
  }
  return { ok: false, thesis, basis: null };
}

function commercialActionOk(opp = {}) {
  const action = text(
    opp.recommendedNextStep ||
      opp.recommendedNextAction ||
      opp.recommendedAction ||
      opp.recommendedSalesOrMarketingAction ||
      opp.whyActNow ||
      opp.cardWhyNowLine ||
      opp.customerNarrative,
  );
  if (!action || PLACEHOLDER_RE.test(action)) {
    return { ok: false, action };
  }
  // Structured field presence beats arbitrary length; require actionable cue OR explicit recommended* field.
  const hasStructuredField = Boolean(
    text(opp.recommendedNextStep) ||
      text(opp.recommendedNextAction) ||
      text(opp.recommendedAction) ||
      text(opp.recommendedSalesOrMarketingAction),
  );
  if (hasStructuredField || ACTIONABLE_RE.test(action)) {
    return { ok: true, action };
  }
  return { ok: false, action };
}

/**
 * Deterministic FQO evaluation. Never requires salesReady.
 * @param {object} opportunity
 * @param {object} [opts]
 * @param {boolean} [opts.historicalApprovedInventory] — schema-compat evidence path
 */
export function evaluateFinalizedQualifiedOpportunity(opportunity = {}, opts = {}) {
  const opp = opportunity && typeof opportunity === "object" ? opportunity : {};
  const nameFn =
    typeof opts.looksLikePersonName === "function"
      ? opts.looksLikePersonName
      : fqoLooksLikePersonName;

  const founderDisposition = upper(
    opp.founderDisposition ||
      opp.founderReviewDisposition ||
      opp.publicationHoldReason ||
      "",
  );
  if (/REJECT|DECLIN/.test(founderDisposition) || opp.founderRejected === true) {
    return {
      contractVersion: GDI_FINALIZED_QUALIFIED_OPPORTUNITY_CONTRACT_V1,
      status: FQO_STATUS.FOUNDER_REJECTED,
      finalizedQualified: false,
      salesReadyRequired: false,
      salesReady: opp.salesReady === true,
      gates: {},
      reasons: ["FOUNDER_REJECTED"],
      bindingFounderHold: true,
      processingDisposition: FQO_PROCESSING_DISPOSITION.GENUINE_COMMERCIAL_REVIEW,
    };
  }
  if (
    /HOLD|PAUSE|DEFER/.test(founderDisposition) ||
    opp.founderHold === true ||
    opp.founderHeld === true
  ) {
    return {
      contractVersion: GDI_FINALIZED_QUALIFIED_OPPORTUNITY_CONTRACT_V1,
      status: FQO_STATUS.FOUNDER_HELD,
      finalizedQualified: false,
      salesReadyRequired: false,
      salesReady: opp.salesReady === true,
      gates: {},
      reasons: ["FOUNDER_HELD"],
      bindingFounderHold: true,
      processingDisposition: FQO_PROCESSING_DISPOSITION.GENUINE_COMMERCIAL_REVIEW,
    };
  }

  const gates = {};
  const reasons = [];
  const historical =
    opts.historicalApprovedInventory === true ||
    opp._historicalApprovedInventory === true;

  // 1. Evidence-supported demand (plausible OK; URLs preferred; historical provenance compat)
  const evidence = evaluateEvidenceSupportedDemand(opp, {
    historicalApprovedInventory: historical,
  });
  gates[FQO_GATE.EVIDENCE_BACKED_DEMAND] = {
    ok: evidence.ok,
    demand: text(
      opp.organizationName ||
        opp.organization ||
        opp.programmeName ||
        opp.demandGenerator ||
        opp.title,
    ) || null,
    evidenceUrlCount: evidence.urls.length,
    mode: evidence.mode,
    schemaIncompatibility: evidence.schemaIncompatibility === true,
  };
  if (!evidence.ok) {
    reasons.push(evidence.reason || "MISSING_EVIDENCE_URLS");
  }

  // 2. Hotel-specific fit (structured / relevance — not narrative length)
  const fit = hotelFitOk(opp);
  gates[FQO_GATE.HOTEL_GEO_COMMERCIAL_FIT] = {
    ok: fit.ok,
    basis: fit.basis,
    thesisPresent: Boolean(fit.thesis),
  };
  if (!fit.ok) reasons.push("MISSING_HOTEL_FIT_THESIS");

  // 3. Future / recurring sourcing pathway
  const futureOk =
    hasFuturePathway(opp, { historicalApprovedInventory: historical }) &&
    !isHistoricalOnlyCustomerExclusion(opp);
  gates[FQO_GATE.FUTURE_SOURCING_PATHWAY] = { ok: futureOk };
  if (!futureOk) reasons.push("NO_VALID_FUTURE_SOURCING_PATHWAY");

  // 4. Named commercially relevant person
  const contactEval = evaluateContactCompletenessForFinalization(opp, {
    looksLikePersonName: nameFn,
    allowOrgRouteOnly: false,
  });
  const coverage = evaluateNamedContactCoverageStandard(opp);
  const primaryName = text(
    opp.primaryContact?.name || opp.primaryContactName || opp.contactName,
  );
  const namedOk =
    contactEval.complete === true &&
    contactEval.mode === "NAMED_PERSON" &&
    nameFn(primaryName) &&
    coverage.namedCount >= 1;
  gates[FQO_GATE.NAMED_COMMERCIAL_PERSON] = {
    ok: namedOk,
    name: primaryName || null,
    role: text(opp.primaryContact?.role || opp.primaryContact?.title),
    coverage: coverage.coverage,
  };
  if (!namedOk) reasons.push("NAMED_COMMERCIAL_PERSON_REQUIRED");

  // 5. Outreach pathway (person-specific OR labeled org route)
  const channelEval = evaluatePersonSpecificOutreachChannel(opp, {
    looksLikePersonName: nameFn,
  });
  gates[FQO_GATE.PERSON_SPECIFIC_CHANNEL] = {
    ok: channelEval.ok,
    channels: channelEval.channels,
    rejected: channelEval.rejected,
    reason: channelEval.reason,
  };
  if (!channelEval.ok) reasons.push("PERSON_SPECIFIC_CHANNEL_REQUIRED");

  // 6. Hotel-specific recommended commercial action (not length)
  const action = commercialActionOk(opp);
  gates[FQO_GATE.HOTEL_SPECIFIC_ACTION] = {
    ok: action.ok,
    actionPresent: Boolean(text(action.action)),
  };
  if (!action.ok) reasons.push("MISSING_HOTEL_SPECIFIC_COMMERCIAL_ACTION");

  // Competitive extras — market-local incumbent (no NYC hardcode)
  const competitive = isCompetitive(opp);
  if (competitive) {
    const incumbent = text(
      opp.competitiveContext?.incumbentHotel ||
        opp.incumbentHotel ||
        opp.competitorHotel ||
        opp.likelyCompetitor ||
        "",
    );
    const pathway = text(
      opp.competitiveContext?.pathway ||
        opp.opportunityPathway ||
        opp.sourcingPathway ||
        (/overflow|stay-to-play|next cycle|future.cycle/i.test(
          text(opp.competitiveStatusLabel || opp.primaryContact?.relevanceEvidence || ""),
        )
          ? "OVERFLOW_OR_FUTURE_CYCLE"
          : ""),
    );
    const competitiveOk =
      Boolean(incumbent) && (Boolean(pathway) || hasFuturePathway(opp));
    gates[FQO_GATE.COMPETITIVE_INCUMBENT_PATHWAY] = {
      ok: competitiveOk,
      incumbentHotel: incumbent || null,
      pathway: pathway || null,
    };
    if (!competitiveOk) {
      reasons.push("COMPETITIVE_INCUMBENT_OR_PATHWAY_UNVERIFIED");
    }
  }

  if (
    PLACEHOLDER_RE.test(
      JSON.stringify({
        title: opp.title,
        action: action.action,
        thesis: fit.thesis,
        name: primaryName,
      }),
    )
  ) {
    reasons.push("INTERNAL_PLACEHOLDER_PRESENT");
    gates.PLACEHOLDER_FREE = { ok: false };
  } else {
    gates.PLACEHOLDER_FREE = { ok: true };
  }

  const allGateOk = Object.values(gates).every((g) => g && g.ok === true);
  const finalizedQualified = allGateOk && reasons.length === 0;

  let processingDisposition = FQO_PROCESSING_DISPOSITION.QUALIFIED_NEW;
  if (finalizedQualified && historical) {
    processingDisposition = FQO_PROCESSING_DISPOSITION.RETAINED_HISTORICAL_APPROVAL;
  } else if (!finalizedQualified && historical) {
    const technicalOnly = reasons.every((r) =>
      /MISSING_EVIDENCE_URLS|PERSON_SPECIFIC_CHANNEL_REQUIRED/.test(r),
    );
    processingDisposition = technicalOnly
      ? FQO_PROCESSING_DISPOSITION.TECHNICAL_REMEDIATION_REQUIRED
      : FQO_PROCESSING_DISPOSITION.GENUINE_COMMERCIAL_REVIEW;
  } else if (!finalizedQualified) {
    processingDisposition = FQO_PROCESSING_DISPOSITION.GENUINE_COMMERCIAL_REVIEW;
  }

  return {
    contractVersion: GDI_FINALIZED_QUALIFIED_OPPORTUNITY_CONTRACT_V1,
    status: finalizedQualified
      ? FQO_STATUS.FINALIZED_QUALIFIED
      : reasons.some((r) => /HISTORICAL|INELIGIBLE|WRONG|UNSUPPORTED/.test(r))
        ? FQO_STATUS.INELIGIBLE
        : FQO_STATUS.RESEARCH_CONTINUATION,
    finalizedQualified,
    salesReadyRequired: false,
    salesReady: opp.salesReady === true,
    salesReadyWasNotBlocking: true,
    competitive,
    marketScope: competitive ? "COMPETITIVE" : "HOTEL",
    priorityBand: upper(opp.priority || opp.priorityBand || "") || null,
    gates,
    reasons,
    evidenceUrls: evidence.urls.slice(0, 8),
    evidenceMode: evidence.mode,
    schemaIncompatibility: evidence.schemaIncompatibility === true,
    namedCoverage: coverage.coverage || NAMED_CONTACT_COVERAGE.ZERO,
    secondaryPresent: coverage.namedCount >= 2,
    bindingFounderHold: false,
    processingDisposition,
    historicalApprovedInventory: historical,
  };
}

/**
 * Filter opportunities to FQO-pass only (for NEW local STAGED candidacy).
 * Does not mutate inputs. Does not rewrite historical packages.
 */
export function selectFinalizedQualifiedOpportunities(opportunities = [], opts = {}) {
  const rows = Array.isArray(opportunities) ? opportunities : [];
  const evaluated = rows.map((o) => ({
    opportunity: o,
    evaluation: evaluateFinalizedQualifiedOpportunity(o, opts),
  }));
  const qualified = evaluated
    .filter((r) => r.evaluation.finalizedQualified === true)
    .map((r) => ({
      ...r.opportunity,
      fqoEvaluation: r.evaluation,
    }));
  return {
    contractVersion: GDI_FINALIZED_QUALIFIED_OPPORTUNITY_CONTRACT_V1,
    beforeCount: rows.length,
    afterCount: qualified.length,
    count: qualified.length,
    opportunities: qualified,
    evaluated,
  };
}
