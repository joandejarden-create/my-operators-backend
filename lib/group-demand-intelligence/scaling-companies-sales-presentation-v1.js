/**
 * Scaling Companies — hotel-sales presentation V1
 *
 * Shared scrub + narrative mapping + contact-path honesty + secondary
 * contactability gate. Used by card builder, customer sanitize, and tests.
 * Hotel-agnostic: does not hardcode Waterstone company copy into other hotels.
 *
 * When a hotel profile is supplied, commercial-intelligence brief V1 upgrades
 * generic template copy into chronology-aware, property-specific briefs.
 */

import { applyScalingCommercialIntelligenceBrief } from "./scaling-commercial-intelligence-brief-v1.js";

export const SCALING_COMPANIES_SALES_PRESENTATION_V1 =
  "gdi-scaling-companies-sales-presentation-v1";

/** Internal ops / research-process language that must never reach hotel-facing copy. */
// Non-global: safe for repeated .test() calls (global RegExp lastIndex is stateful).
export const INTERNAL_RESEARCH_LANGUAGE_RE =
  /Requires qualification\s*\+\s*ORS(?:\s*\+\s*contact route)?(?:\s*before publication)?|REVIEWED_PACKAGE_V1|DISCOVERY_HIT_UNQUALIFIED|HOLD_CONTACT|\bNAMED_DIRECT\b|0 verified field\(s\),\s*0 estimated,\s*0 inferred|contact route before publication|Sales posture|dimension-level hotel-fit assessment|weights renormalized|overall numeric (?:score )?(?:provisional\/)?withheld/i;

const GENERIC_ORG_EMAIL_LOCAL_RE =
  /^(info|contact|hello|office|admin|support|sales|inquiries|enquiry|reception|mail|team|hr|press|media|customerservice|hq|bocaraton|atlanta|miami|dallas|jacksonville|denver)$/i;

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

export function isGenericOrganizationEmail(email) {
  const e = text(email).toLowerCase();
  if (!e.includes("@")) return false;
  const local = e.split("@")[0] || "";
  return GENERIC_ORG_EMAIL_LOCAL_RE.test(local);
}

/**
 * Strip internal research directives, collapse duplicated titles, and
 * remove bare truncated URLs from narrative prose.
 */
/** True when prose still looks like a scraped article body / CMS dump. */
export function isScrapedNarrativeContamination(input) {
  const s = String(input || "");
  if (!s.trim()) return false;
  if (/^#{1,6}\s|\s#{1,6}\s|##\s*About/i.test(s)) return true;
  if (/\b\d+\s*minute\s*read\b/i.test(s)) return true;
  if (/Trade and Industry Development/i.test(s) && /Relocates/i.test(s)) {
    const hits = s.match(/Relocates Corporate HQ/gi);
    if (hits && hits.length >= 2) return true;
  }
  // Same title fragment appears twice with material gap (dedupe failure)
  const titleish = s.slice(0, Math.min(70, s.length)).replace(/\s+/g, " ").trim();
  if (
    titleish.length >= 28 &&
    s.toLowerCase().indexOf(titleish.toLowerCase(), 12) > 12
  ) {
    return true;
  }
  // "Org: Title. Org — Title" / duplicated marketing headline dump
  if (/^[^:]{2,80}:\s+.+\1\s*[—-]/i.test(s) || /:\s*([^.]{12,80})\.\s+\1/i.test(s)) {
    return true;
  }
  if (
    /\bFlorida Workforce Housing for Temporary Workforces\b/i.test(s) &&
    (s.match(/Florida Workforce Housing/gi) || []).length >= 2
  ) {
    return true;
  }
  return false;
}

export function scrubInternalResearchLanguage(input) {
  if (input == null) return input == null ? null : "";
  let s = String(input);
  if (!s.trim()) return "";
  // Truncated or full ops instructions (including "Requires qualification + O")
  s = s.replace(/Requires qualification[^.\n]{0,160}/gi, " ");
  s = s.replace(INTERNAL_RESEARCH_LANGUAGE_RE, " ");
  // Drop markdown / CMS scrape scaffolding
  s = s.replace(/#{1,6}\s*/g, " ");
  s = s.replace(/\b\d+\s*minute\s*read\b/gi, " ");
  s = s.replace(/\bTrade and Industry Development\b/gi, " ");
  // Drop raw http(s) URLs from narrative (sources belong in Evidence)
  s = s.replace(/https?:\/\/\S+/gi, " ");
  // Leftover truncated URL tails after scrub (e.g. ".com/news/anno", "cpgcre.com/news/…")
  s = s.replace(
    /\s*(?:[a-z0-9-]+\.)?(?:com|org|net|io)\/[a-z0-9_./?&=%-]*/gi,
    " ",
  );
  // Collapse repeated adjacent title fragments (same sentence twice)
  s = s.replace(/\s+/g, " ").trim();
  // Remove leftover punctuation clusters
  s = s.replace(/\s*[|;·]+\s*/g, ". ").replace(/\.\s*\./g, ".");
  s = s.replace(/^[\s.·|;:-]+|[\s.·|;:-]+$/g, "").trim();
  // Deduplicate if the same headline appears twice back-to-back
  const half = Math.floor(s.length / 2);
  if (half >= 24) {
    const a = s.slice(0, half).trim();
    const b = s.slice(half).trim();
    if (a && b && a === b) s = a;
    else if (a && b.toLowerCase().startsWith(a.toLowerCase().slice(0, Math.min(40, a.length)))) {
      s = a.length >= b.length ? a : b;
    }
  }
  // Repeated title with interstitial publisher noise
  const m = s.match(/^(.{28,120}?)(?:[. ]+)\1(?:\b|$)/i);
  if (m) s = m[1].trim();
  if (isScrapedNarrativeContamination(s) && s.length > 160) {
    // Prefer first sentence / clause before scrape body
    const cut = s.split(/(?<=\.)\s+|;\s+/)[0] || s;
    s = cut.length >= 24 && cut.length <= 180 ? cut : s.slice(0, 160).replace(/\s+\S*$/, "").trim();
  }
  return s || null;
}

/**
 * Prefer a clean activity headline: title without ops notes / URLs.
 */
export function cleanActivityHeadline(opportunity = {}) {
  const org = text(opportunity.organizationName || opportunity.company);
  const candidates = [
    opportunity.activityHeadline,
    opportunity.growthHeadline,
    opportunity.sourceTitle,
    // Prefer contaminated-but-scrubbable local presence (often holds the real headline)
    opportunity.localPresence,
    opportunity.localConnection,
    Array.isArray(opportunity.sources) &&
      opportunity.sources.find(
        (s) =>
          s?.title &&
          !/^growth evidence$/i.test(s.title) &&
          !/^official contact/i.test(s.title) &&
          !/^contact page$/i.test(s.title),
      )?.title,
    opportunity.summaryWhat,
    opportunity.datedGrowthEvidence,
  ];
  for (const c of candidates) {
    let scrubbed = scrubInternalResearchLanguage(c);
    if (!scrubbed) continue;
    // Drop useless placeholders
    if (/^growth evidence$/i.test(scrubbed)) continue;
    if (/^see evidence$/i.test(scrubbed)) continue;
    if (/^official contact/i.test(scrubbed)) continue;
    // Contractor / CMS project-page titles are not hotel-sales headlines
    if (/Hensel Phelps/i.test(scrubbed)) continue;
    if (isScrapedNarrativeContamination(scrubbed) && scrubbed.length > 140) {
      // Keep only the leading title-like clause when scrape body remains
      const lead = scrubbed.split(/(?<=\.)\s+/)[0] || scrubbed;
      scrubbed =
        lead.length >= 12 && lead.length <= 160
          ? lead
          : scrubbed.slice(0, 120).replace(/\s+\S*$/, "").trim();
    }
    // Prefer short headline-like strings over concatenated blobs
    if (scrubbed.length > 220) {
      scrubbed = scrubbed.slice(0, 220).replace(/\s+\S*$/, "").trim();
    }
    // Strip leading "Org: " / duplicated org name from headlines
    if (org) {
      const orgEsc = org.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
      scrubbed = scrubbed
        .replace(new RegExp(`^${orgEsc}\\s*[:—-]\\s*`, "i"), "")
        .replace(new RegExp(`\\s*[—-]\\s*${orgEsc}\\b`, "ig"), "")
        .replace(new RegExp(`\\b${orgEsc}\\b`, "ig"), " ")
        .replace(/\s+/g, " ")
        .replace(/^[\s.:—-]+|[\s.:—-]+$/g, "")
        .trim();
    }
    // Keep first clause before em-dash duplication
    if (scrubbed.includes(" — ")) {
      const lead = scrubbed.split(" — ")[0].trim();
      if (lead.length >= 12 && lead.length <= 120) scrubbed = lead;
    }
    // Collapse immediately repeated phrase ("EXPANDS TO TAMPA FL. … EXPANDS TO TAMPA FL.")
    const rep = scrubbed.match(/^(.{12,90}?)(?:[. ]+)\1/i);
    if (rep) scrubbed = rep[1].trim();
    // Prefer first sentence
    const sentence = scrubbed.split(/(?<=\.)\s+/)[0] || scrubbed;
    if (sentence.length >= 12 && sentence.length <= 110) scrubbed = sentence;
    if (scrubbed.length > 110) {
      scrubbed = scrubbed.slice(0, 110).replace(/\s+\S*$/, "").trim();
    }
    if (scrubbed.length >= 12) return scrubbed;
  }
  return org ? `${org} — documented local business activity` : null;
}

export function resolveCommercialContactPath(opportunity = {}) {
  const named =
    text(opportunity.primaryContact?.name || opportunity.contactName) &&
    text(opportunity.primaryContact?.name || opportunity.contactName) !== "UNKNOWN";
  const email = text(
    opportunity.primaryContact?.email ||
      opportunity.contactEmail ||
      opportunity.functionalContactEmail ||
      opportunity.organizationContactEmail,
  );
  const phone = text(
    opportunity.primaryContact?.phone ||
      opportunity.contactPhone ||
      opportunity.companyPhone,
  );
  const url = text(
    opportunity.contactUrl ||
      opportunity.contactOfficialUrl ||
      opportunity.organizationPath,
  );

  if (named && email && !isGenericOrganizationEmail(email)) {
    return {
      commercialContactPath: "NAMED_DIRECT",
      commercialContactPathLabel: "Named person — direct business email",
    };
  }
  // Only treat phone as named-person route when a real person name exists AND
  // the phone is not merely the company switchboard parked on primaryContact.
  const phoneOwnership = text(
    opportunity.primaryContact?.phoneOwnership ||
      opportunity.primaryContactPhoneOwnership ||
      opportunity.primaryContact?.phoneType ||
      ""
  );
  const phoneIsPersonDirect =
    named &&
    phone &&
    !/organization|company|switchboard|office/i.test(phoneOwnership) &&
    opportunity.primaryContact?.phoneType !== "company" &&
    opportunity.primaryContact?.phoneType !== "organization";
  if (phoneIsPersonDirect) {
    return {
      commercialContactPath: "NAMED_PHONE",
      commercialContactPathLabel: "Named person — phone route",
    };
  }
  if (email && isGenericOrganizationEmail(email)) {
    return {
      commercialContactPath: "ORGANIZATION_PATH",
      commercialContactPathLabel: "Company contact email",
    };
  }
  if (email) {
    return {
      commercialContactPath: "FUNCTIONAL_CONTACT",
      commercialContactPathLabel: "Functional / department email",
    };
  }
  if (phone || url) {
    return {
      commercialContactPath: "ORGANIZATION_PATH",
      commercialContactPathLabel: phone
        ? "Company office telephone"
        : "Organization contact path",
    };
  }
  return {
    commercialContactPath: "NO_CONTACT",
    commercialContactPathLabel: "Contact route not yet established",
  };
}

/**
 * Secondary is customer-displayable only when a usable reach path exists.
 * LinkedIn / profile alone is identity evidence, not a contact route.
 */
export function secondaryContactHasUsableRoute(secondary = {}) {
  if (!secondary || typeof secondary !== "object") return false;
  if (!text(secondary.name)) return false;
  if (text(secondary.email) && secondary.email.includes("@")) return true;
  if (text(secondary.phone)) return true;
  if (text(secondary.companyPhone)) return true;
  const route = text(secondary.organizationRoutingAlternative);
  if (route) {
    // Email or phone-like org routing
    if (route.includes("@")) return true;
    if (/\d{3}/.test(route.replace(/\D/g, "")) && route.replace(/\D/g, "").length >= 7) {
      return true;
    }
  }
  // Stage D/E: named commercially relevant secondary may surface with profile /
  // source evidence without inventing email/phone (Surfe remains on-demand).
  if (
    text(secondary.profileUrl) ||
    text(secondary.sourceUrl) ||
    text(secondary.linkedinUrl) ||
    text(secondary.whyThisContact) ||
    text(secondary.whyRelevant) ||
    text(secondary.relationshipToOpportunity) ||
    text(secondary.commercialRelevance) ||
    secondary.contactRelevanceLabel === "CONTACT_RELEVANT" ||
    /contact_relevant|relevant_influencer|referral/i.test(
      String(secondary.relevanceClass || ""),
    )
  ) {
    return true;
  }
  return false;
}

export function customerSafeSecondaryContact(secondary) {
  if (!secondaryContactHasUsableRoute(secondary)) return null;
  const next = { ...secondary };
  // Honest org-routing labels
  if (!next.email && !next.phone && next.organizationRoutingAlternative) {
    const route = text(next.organizationRoutingAlternative);
    if (route.includes("@")) {
      next.organizationRoutingLabel =
        next.organizationRoutingLabel ||
        `Company email — ask for ${next.name}`;
    } else {
      next.organizationRoutingLabel =
        next.organizationRoutingLabel ||
        `Company office — ask for ${next.name}`;
    }
  }
  return next;
}

/**
 * Build hotel-sales-facing narrative fields from existing evidence.
 * Does not invent stronger demand than the record supports.
 */
export function buildScalingSalesFacingFields(opportunity = {}, opts = {}) {
  const org = text(opportunity.organizationName || opportunity.company) || "This company";
  const hotelName = text(opts.hotelDisplayName || opportunity.hotelDisplayName || "your hotel");
  const headline = cleanActivityHeadline(opportunity);
  // Never treat research/verification timestamps as the corporate activity date.
  const researchVerificationDate = text(
    opportunity.researchVerificationDate ||
      opportunity.lastResearchedAt ||
      opportunity.evidenceFreshnessAt ||
      opportunity.secondaryContact?.verificationDate,
  ).slice(0, 10);
  const rawGrowthDate = text(
    opportunity.originalActivityDate ||
      opportunity.growthEvidenceDate ||
      opportunity.sourcePublicationDate,
  ).slice(0, 10);
  const growthDate =
    rawGrowthDate && rawGrowthDate !== researchVerificationDate
      ? rawGrowthDate
      : text(opportunity.originalActivityDate || opportunity.sourcePublicationDate).slice(
          0,
          10,
        ) || null;

  const rawInput = opportunity.whatIsHappening || opportunity.summaryWhat || "";
  const rawWhat =
    scrubInternalResearchLanguage(opportunity.whatIsHappening) ||
    scrubInternalResearchLanguage(opportunity.summaryWhat);
  const expansionMexicoCity = /mexico city|ciudad de m[eé]xico|cdmx/i.test(
    `${headline} ${rawWhat} ${opportunity.datedGrowthEvidence || ""}`,
  );
  const expansionToronto = /toronto/i.test(
    `${headline} ${rawWhat} ${opportunity.datedGrowthEvidence || ""}`,
  );
  const isOfficeExpansion =
    /expand|new office|global footprint|apertura|nueva sede/i.test(
      `${headline} ${rawWhat} ${opportunity.datedGrowthEvidence || ""}`,
    );
  const isWorkforceHousingProvider =
    /workforce housing|temporary workforce|flexible workforce housing|staff housing provider/i.test(
      `${headline} ${rawWhat} ${rawInput} ${opportunity.datedGrowthEvidence || ""} ${opportunity.demandMechanism || ""}`,
    );
  const whatIsHappening = (() => {
    // Prefer a concise factual expansion summary when the record is an office opening.
    if (isOfficeExpansion && expansionMexicoCity) {
      const dateBit = growthDate ? ` in ${growthDate}` : "";
      if (expansionToronto) {
        return `${org} announced new offices in Mexico City and Toronto${dateBit}. The Mexico City location is intended to support the company's Latin American operations.`;
      }
      return `${org} announced a new Mexico City office${dateBit}, intended to support regional / Latin American operations.`;
    }
    if (isWorkforceHousingProvider) {
      return `${org} publicly markets Florida workforce / temporary-staff housing for hospitality, healthcare, construction, and related industries${growthDate ? ` (evidence reviewed ${growthDate})` : ""}.`;
    }
    const rebuilt = headline
      ? growthDate
        ? `${org} has documented local activity: ${headline} (original public date ${growthDate}).`
        : `${org} has documented local activity: ${headline}.`
      : `${org} is a published scaling-company account based on local business-activity evidence.`;
    if (!rawWhat) return rebuilt;
    // Decide from original input — scrub may leave a short contractor/CMS title
    // that is still not hotel-sales language.
    if (isScrapedNarrativeContamination(rawInput)) return rebuilt;
    if (isScrapedNarrativeContamination(rawWhat)) return rebuilt;
    if (rawWhat.length > 280 && headline && headline.length < 160) return rebuilt;
    // Collapse duplicated title fragments ("Title — Title body…")
    if (/^.+ — \1/i.test(rawWhat) || /Expands Global Footprint.+\1/i.test(rawWhat)) {
      return rebuilt;
    }
    if (/^[^:]{2,60}:\s+.+\1/i.test(rawWhat)) return rebuilt;
    // "Org: Headline — Org — Headline" marketing dump
    if (
      org &&
      rawWhat.toLowerCase().startsWith(org.toLowerCase() + ":") &&
      (rawWhat.match(new RegExp(org.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"), "gi")) || [])
        .length >= 2
    ) {
      return rebuilt;
    }
    return rawWhat;
  })();
  const workforceHousingBusiness = `${org} markets workforce / temporary-staff housing. That does not automatically mean it purchases Marriott guestrooms — hotel demand is only plausible if ${org} overflows placements into hotels or hosts visiting operators. Treat as a development account until a lodging buyer path is verified.`;
  const workforceHousingWhyHotel = `${hotelName} is in the same Florida market as ${org}'s workforce-housing footprint. Relevance depends on whether ${org} (or its clients) ever places temporary staff in hotels near this property rather than in ${org}-operated housing. No guestroom contract is established from the public page alone.`;
  const potentialBusiness = isWorkforceHousingProvider
    ? workforceHousingBusiness
    : scrubInternalResearchLanguage(
        opportunity.potentialHotelBusiness ||
          opportunity.proposedHotelUse ||
          opportunity.potentialHotelUseCases ||
          opportunity.summaryWhyMatters,
      ) ||
      (isOfficeExpansion
        ? `Visiting executives, project teams, training sessions, and client or internal meetings are plausible hotel-demand hypotheses linked to the new office — not confirmed room blocks or RFPs.`
        : `Potential visiting-staff lodging and/or meeting needs linked to ${org}'s local activity. Hotel demand volume is not yet confirmed — treat as a development account until lodging or meetings are verified.`);

  const whyThisHotel = isWorkforceHousingProvider
    ? workforceHousingWhyHotel
    : scrubInternalResearchLanguage(
        opportunity.whyThisHotelClean ||
          opportunity.whyThisHotel ||
          opportunity.hotelOpportunityThesis ||
          opportunity.summaryWhyHotel,
      ) ||
      `${hotelName} is in the same commercial market as the documented ${org} activity and can compete for overnight or meeting needs if the account develops. Specific room-block or meeting contracts are not established from public research alone.`;

  // Avoid repeating the raw announcement inside whyThisHotel
  const whyHotelDeduped = (() => {
    const w = text(whyThisHotel);
    const h = text(headline);
    if (h && w.toLowerCase().includes(h.toLowerCase().slice(0, 60))) {
      if (expansionMexicoCity && /chapultepec|mexico city|cdmx/i.test(hotelName + " mexico city")) {
        return `${hotelName} sits in Mexico City's Chapultepec / Paseo de la Reforma business corridor, making it a relevant option if ${org}'s Mexico City teams need guest rooms or meeting space for visiting staff. Specific contracts are not yet confirmed.`;
      }
      return `${hotelName} is positioned to compete for overnight or meeting needs tied to this local ${org} activity if lodging or meetings materialize. Specific contracts are not yet confirmed.`;
    }
    return w;
  })();

  const recommendedActionRaw = scrubInternalResearchLanguage(
    opportunity.recommendedAction || opportunity.recommendedNextStep,
  );
  const growthReference = expansionMexicoCity
    ? "the Mexico City office expansion"
    : headline
      ? `the documented local activity (${headline.slice(0, 80)})`
      : "the documented local expansion / growth activity";
  let recommendedAction =
    recommendedActionRaw &&
    !/do not sell on organization existence|Validate the public buyer\/housing path/i.test(
      recommendedActionRaw,
    )
      ? recommendedActionRaw
      : `Use ${org}'s official contact path or published media referral email to introduce ${hotelName}, reference ${growthReference}, and ask who owns visiting-staff lodging, training, and meeting decisions for the local team.`;
  // Strip cross-market Mexico City template bleed when the opportunity is not CDMX expansion.
  if (
    !expansionMexicoCity &&
    /mexico city office expansion/i.test(recommendedAction)
  ) {
    recommendedAction = recommendedAction.replace(
      /,\s*reference the Mexico City office expansion,/i,
      `, reference ${growthReference},`,
    );
  }

  const localPresenceClean =
    scrubInternalResearchLanguage(opportunity.localPresence) ||
    scrubInternalResearchLanguage(opportunity.localConnection) ||
    headline;

  const datedGrowthClean =
    scrubInternalResearchLanguage(opportunity.datedGrowthEvidence) ||
    (growthDate && headline ? `${headline} (${growthDate})` : headline);

  const path = resolveCommercialContactPath(opportunity);
  const secondary = customerSafeSecondaryContact(opportunity.secondaryContact);

  const establishedFacts = Array.isArray(opportunity.establishedFacts)
    ? opportunity.establishedFacts
        .map(scrubInternalResearchLanguage)
        .filter(Boolean)
        .filter((f) => !/local connection:/i.test(f) || f.length < 200)
        .filter((f) => text(f).toLowerCase() !== text(whatIsHappening).toLowerCase())
    : [];

  // Do not repeat the same potential-business sentence under inferredHotelDemand.
  const inferredClean = Array.isArray(opportunity.inferredHotelDemand)
    ? opportunity.inferredHotelDemand
        .map(scrubInternalResearchLanguage)
        .filter(Boolean)
        .filter((f) => text(f).toLowerCase() !== text(potentialBusiness).toLowerCase())
    : [];

  const constraintsClean = Array.isArray(opportunity.constraints)
    ? opportunity.constraints.map(scrubInternalResearchLanguage).filter(Boolean)
    : [];

  return {
    scalingSalesPresentationVersion: SCALING_COMPANIES_SALES_PRESENTATION_V1,
    activityHeadline: headline && !isScrapedNarrativeContamination(headline)
      ? scrubInternalResearchLanguage(headline)
      : isOfficeExpansion && expansionMexicoCity
        ? `${org} — Mexico City office expansion`
        : headline,
    whatIsHappening,
    summaryWhat: whatIsHappening,
    potentialHotelBusiness: potentialBusiness,
    proposedHotelUse: potentialBusiness,
    potentialHotelUseCases: potentialBusiness,
    summaryWhyMatters: potentialBusiness,
    whyThisHotel: whyHotelDeduped,
    hotelOpportunityThesis: whyHotelDeduped,
    summaryWhyHotel: whyHotelDeduped,
    hotelFitRationale: whyHotelDeduped,
    fitExplanation: whyHotelDeduped,
    localPresence: localPresenceClean,
    localConnection: localPresenceClean,
    datedGrowthEvidence: datedGrowthClean,
    recommendedAction,
    recommendedNextStep: recommendedAction,
    commercialContactPath: path.commercialContactPath,
    commercialContactPathLabel: path.commercialContactPathLabel,
    secondaryContact: secondary,
    establishedFacts: establishedFacts.length ? establishedFacts : undefined,
    inferredHotelDemand: inferredClean,
    constraints: constraintsClean,
    uncertainties: constraintsClean,
    // Never emit research jargon onto customer cards
    evidenceStatusLabel: null,
    venueSourcingStatusLabel: null,
    // Customer-safe demand stage language — preserve chronology-aware labels from
    // commercial-intelligence brief when already computed.
    demandStageLabel:
      text(opportunity.demandStageLabel) &&
      !/^Potential business opportunity; hotel needs not yet confirmed$/i.test(
        text(opportunity.demandStageLabel),
      )
        ? text(opportunity.demandStageLabel)
        : /CONTINUING_ESTABLISHED|HISTORICAL_ACCOUNT/i.test(
            String(opportunity.activityStatus || ""),
          )
          ? "Corporate account-development opportunity (historical expansion; ongoing local presence)"
          : "Potential business opportunity; hotel needs not yet confirmed",
    prospectCustomerLabel:
      text(opportunity.prospectCustomerLabel) &&
      !/^Potential opportunity$/i.test(text(opportunity.prospectCustomerLabel))
        ? text(opportunity.prospectCustomerLabel)
        : /WATCHLIST|MONITOR/i.test(String(opportunity.scalingResearchCompleteness || ""))
          ? "Monitor / research continuation"
          : "Potential corporate account",
    growthEvidenceDate: growthDate || opportunity.growthEvidenceDate || null,
  };
}

/**
 * Apply sales-presentation mapping onto an opportunity for customer surfaces.
 */
export function applyScalingSalesPresentation(opportunity = {}, opts = {}) {
  let o = opportunity && typeof opportunity === "object" ? opportunity : {};
  const isScaling =
    o.researchSegment === "scaling_companies" ||
    o.detailLayout === "CORPORATE_ACCOUNT_RELATIONSHIP" ||
    o.opportunityType === "CORPORATE_ACCOUNT_RELATIONSHIP" ||
    /scaling compan/i.test(String(o.segment || ""));
  // Commercial-intelligence depth: when hotel profile (or continuity opts) provided,
  // rebuild chronology-aware brief before template mapping.
  if (
    isScaling &&
    (opts.hotel ||
      opts.applyCommercialIntelligenceBrief === true ||
      opts.continuityEvidence === true)
  ) {
    o = applyScalingCommercialIntelligenceBrief(o, opts.hotel || {}, {
      continuityEvidence: opts.continuityEvidence === true,
      continuityNote: opts.continuityNote || null,
      asOfIso: opts.asOfIso,
    });
  }
  if (!isScaling) {
    // Still scrub internal language if it leaked onto any opp
    const scrubbed = { ...o };
    for (const key of [
      "summaryWhat",
      "localPresence",
      "localConnection",
      "datedGrowthEvidence",
      "whyThisHotel",
      "hotelFitRationale",
      "hotelOpportunityThesis",
      "summaryWhyHotel",
      "fitExplanation",
      "recommendedAction",
      "recommendedNextStep",
    ]) {
      if (scrubbed[key]) scrubbed[key] = scrubInternalResearchLanguage(scrubbed[key]);
    }
    if (scrubbed.secondaryContact) {
      scrubbed.secondaryContact = customerSafeSecondaryContact(scrubbed.secondaryContact);
    }
    return scrubbed;
  }

  const sales = buildScalingSalesFacingFields(o, opts);
  return {
    ...o,
    ...sales,
    // Preserve evidence arrays / sources / ids / priority / publication
    id: o.id,
    opportunityId: o.opportunityId || o.id,
    sources: o.sources,
    primaryContact: o.primaryContact,
    contactName: o.contactName,
    contactEmail: o.contactEmail,
    contactPhone: o.contactPhone,
    companyPhone: o.companyPhone,
    organizationContactEmail: o.organizationContactEmail,
    functionalContactEmail: o.functionalContactEmail,
    secondaryContact: sales.secondaryContact ?? o.secondaryContact,
    priority: o.priority,
    customerVisible: o.customerVisible,
    hotelFacing: o.hotelFacing,
    prospectHotelFacing: o.prospectHotelFacing,
    opportunityQualification: o.opportunityQualification,
    segment: o.segment,
    researchSegment: o.researchSegment,
    detailLayout: o.detailLayout,
    opportunityType: o.opportunityType,
    opportunityTypeLabel: o.opportunityTypeLabel || "Corporate Account Relationship",
    eventDateDisplay: o.eventDateDisplay || "Ongoing corporate relationship",
    timing: o.timing || "Timing to be confirmed with the account",
    growthSignals: o.growthSignals,
    growthSignalLabels: o.growthSignalLabels,
    growthEvidenceDate: o.growthEvidenceDate,
    inferredHotelDemand: sales.inferredHotelDemand ?? o.inferredHotelDemand,
    constraints: Array.isArray(o.constraints)
      ? o.constraints.map(scrubInternalResearchLanguage).filter(Boolean)
      : o.constraints,
    uncertainties: Array.isArray(sales.uncertainties)
      ? sales.uncertainties
      : Array.isArray(o.uncertainties)
        ? o.uncertainties.map(scrubInternalResearchLanguage).filter(Boolean)
        : o.uncertainties,
  };
}

export function excerptIsInternalOpsNote(excerpt) {
  return /Requires qualification|before publication|DISCOVERY_HIT|HOLD_CONTACT|REVIEWED_PACKAGE/i.test(
    String(excerpt || ""),
  );
}
