/**
 * Segment-aware semantic CIB writer for GDI drawers.
 * Rebuilds customer-facing sections from existing Discovery + Deep Research only.
 * Never invents demand, contacts, or booking commitments.
 */
import { resolveDemandSegment } from "./eight-demand-segment-registry-v1.js";
import {
  SEGMENT_SEMANTIC_CIB_CONTRACT_V1,
  normalizeContactDisplayTitle,
  pickPersonEmail,
  text,
} from "./segment-semantic-cib-contract-v1.js";
import { buildUniversalOpportunityIdentity } from "./universal-opportunity-identity-v1.js";

export const SEGMENT_SEMANTIC_CIB_WRITER_V1 =
  "gdi-segment-semantic-cib-writer-v1";

function hotelLabel(hotel = {}) {
  return text(hotel.name || hotel.displayName) || "this hotel";
}

function hotelMarket(hotel = {}) {
  const n = `${hotel.name || ""} ${hotel.hotelId || ""}`;
  if (/chapultepec|mexico city/i.test(n)) return "Mexico City";
  if (/condesa|cancún|cancun/i.test(n)) return "Cancún";
  return text(hotel.market || hotel.city) || null;
}

function dedupeSources(sources = []) {
  const seen = new Set();
  const out = [];
  for (const s of sources) {
    const url = text(s?.url);
    if (!url) continue;
    const key = url.toLowerCase().replace(/\/$/, "");
    if (seen.has(key)) continue;
    seen.add(key);
    out.push({
      url,
      title: text(s.title || s.claim) || url,
      claim: text(s.claim) || null,
      establishes: text(s.claim || s.establishes) || null,
      publisher: text(s.publisher) || null,
      publicationDate: s.publicationDate || s.publishedDate || null,
    });
  }
  return out;
}

function mapContacts(deep = {}, candidate = {}) {
  const list = [
    ...(Array.isArray(deep.namedContacts) ? deep.namedContacts : []),
    ...(Array.isArray(candidate.namedContacts) ? candidate.namedContacts : []),
  ].filter((c) => text(c?.name));
  const seen = new Set();
  const out = [];
  for (const c of list) {
    const k = text(c.name).toLowerCase();
    if (seen.has(k)) continue;
    seen.add(k);
    const why = text(c.whyRelevant);
    const title = normalizeContactDisplayTitle(c.title, why);
    const email = pickPersonEmail({ email: c.email, whyRelevant: why });
    out.push({
      name: text(c.name),
      title,
      organization: text(c.organization) || null,
      whyRelevant: why || null,
      email,
      sourceUrl: text(c.sourceUrl) || null,
      contractingAuthorityVerified: false,
      buyerEligible: false,
      displayEligible: true,
      isRelevantCommercialEntryPoint: true,
      isDirectBuyer: false,
    });
  }
  return out;
}

function guestroomOverviewLabel(statusRaw) {
  const s = text(statusRaw);
  if (/NONE_CONFIRMED|No demonstrated guestroom|zero room nights|No guestroom demand confirmed/i.test(s)) {
    return "No guestroom demand confirmed — catering/meeting revenue does not require rooms";
  }
  if (/EVIDENCE_SUPPORTED/i.test(s)) {
    return "Guestroom demand evidenced — qualify volumes before forecasting";
  }
  return "Guestroom demand not yet verified (unknown — not confirmed zero)";
}

function conciseCateringDemand(candidate = {}, deep = {}) {
  const simple = Array.isArray(candidate.revenueTypes)
    ? candidate.revenueTypes.filter((t) => text(t).length < 40)
    : [];
  if (simple.length) {
    return text(
      simple
        .map((t) => text(t))
        .join(", ")
        .replace(/av and coffee breaks/i, "coffee breaks / AV"),
    );
  }
  return "Executive breakfasts, seated lunches and meeting-room/catering potential";
}

function buildOverviewValues(segmentId, candidate, deep, hotel) {
  const market = hotelMarket(hotel) || "Not yet verified";
  const guestroom = guestroomOverviewLabel(
    deep.guestroomDemandStatus || candidate.guestroomDemandStatus,
  );
  const factsBlob = (deep.establishedFacts || []).join(" ");

  if (segmentId === "local_catering_day_meetings") {
    const hasMonthly = /monthly|mensual/i.test(
      `${candidate.programmeName} ${factsBlob}`,
    );
    const incumbent = /Club de Industriales/i.test(factsBlob)
      ? "Club de Industriales (verified incumbent)"
      : "Venue sourcing unconfirmed";
    return {
      timing: hasMonthly
        ? "Recurring monthly lunch evidenced in 2026; breakfast cadence unverified"
        : "Programme timing partially evidenced — next bookable date unverified",
      location: /Polanco/i.test(factsBlob)
        ? `${market} (Polanco incumbent; hotel in same market)`
        : market,
      venueSourcing: incumbent,
      meetingCateringDemand: conciseCateringDemand(candidate, deep),
      demandCycle: hasMonthly
        ? "Monthly lunch cycle evidenced; full annual calendar not verified"
        : "Demand cycle not yet verified",
      attendees: "Attendance not yet published",
      guestroomDemand: guestroom,
    };
  }

  if (segmentId === "government_public_sector") {
    return {
      timing: text(deep.timing) || "Programme timing — verify next edition",
      location: market,
      procurementStatus:
        text(deep.procurementOrAccessibilityNotes)?.slice(0, 220) ||
        "Procurement accessibility not yet verified",
      travelLodgingDemand: guestroom,
      programmeCycle: "Programme cycle not fully verified",
      meetingRequirements: conciseCateringDemand(candidate, deep),
      contractingPathway:
        "Contracting pathway / signatory not verified from public research",
    };
  }

  if (segmentId === "relocation_extended_stay") {
    return {
      assignmentTiming:
        "Assignment / stay timing not verified from public research",
      destination: market,
      accommodationSourcing: "Accommodation sourcing unconfirmed",
      stayDemand: guestroom,
      assignmentCycle: "Assignment cycle not verified",
      expectedStayPattern:
        "Do not treat employment expansion or congress stays as verified temporary-housing duration",
      mobilityTravelPathway:
        text(deep.procurementOrAccessibilityNotes)?.slice(0, 220) ||
        "Mobility / travel pathway not verified",
    };
  }

  return {};
}

function buildWhatIsHappening(segmentId, candidate, deep) {
  const discovery = text(candidate.demandMechanismSummary);
  const facts = Array.isArray(deep.establishedFacts) ? deep.establishedFacts : [];
  // Prefer English activity narrative assembled from facts when discovery is Spanish-only mixed markets
  if (segmentId === "local_catering_day_meetings" && facts.length) {
    const bullets = facts
      .filter((f) =>
        /Desayunos|lunch|Comida|Club de Industriales|breakfast|monthly/i.test(f),
      )
      .slice(0, 4);
    if (bullets.length >= 2) {
      return bullets.join(" ");
    }
  }
  if (discovery && discovery.length > 40) return discovery;
  return (
    text(deep.programmeIdentity)?.split(/\.\s/).slice(0, 2).join(". ") ||
    text(candidate.programmeName) ||
    "Programme activity is documented; details remain under qualification."
  );
}

function splitHotelThesis(thesis, hotelName) {
  const t = text(thesis);
  if (!t) {
    return {
      potential: null,
      whyBody: null,
      advantages: [],
      limitations: [],
    };
  }
  const advantages = [];
  const limitations = [];
  const capacityBits = t.match(
    /(?:Castillo|Bosques|Salón|Salon|banquet places|meeting rooms|flexible meeting)[^.]*\./gi,
  );
  if (capacityBits) advantages.push(...capacityBits.map((x) => text(x)));
  if (/commercially relevant alternative|same commercial|corridor|Anzures|Chapultepec|Polanco|Cancún|Cancun/i.test(t)) {
    advantages.push(
      `${hotelName} is commercially positioned against the evidenced activity geography — evaluate as an alternative or overflow venue, not a confirmed booking.`,
    );
  }
  const incumbent = t.match(
    /(?:Club de Industriales|incumbent|Hilton|Grand Oasis|verified incumbent)[^.]*\./gi,
  );
  if (incumbent) limitations.push(...incumbent.map((x) => text(x)));
  if (/JW Marriott|competitor|competitive benchmark/i.test(t)) {
    const comp = t.match(/(?:JW Marriott|competitive)[^.]*\./gi);
    if (comp) limitations.push(...comp.map((x) => text(x)));
  }
  if (/conditional on actual attendance|Do not apply theater|availability and pricing|unverified/i.test(t)) {
    limitations.push(
      "Facility fit, hotel availability, pricing, and layout requirements remain unverified until attendance and programme specs are confirmed.",
    );
  }
  if (!advantages.length) {
    advantages.push(
      `${hotelName} sits in the same commercial market as the documented programme activity.`,
    );
  }
  if (!limitations.length) {
    limitations.push(
      "No verified hotel booking, RFP, or preferred-supplier agreement is established from public research alone.",
    );
  }
  const whyBody =
    t.split(/(?=Competition:|Winning a pilot)/i)[0] || t.slice(0, 500);
  return {
    potential: t,
    whyBody: text(whyBody),
    advantages: [
      ...new Set(advantages.map(text).filter((x) => x.length > 24)),
    ],
    limitations: [
      ...new Set(limitations.map(text).filter((x) => x.length > 24)),
    ],
  };
}

function buildWhyActNow(segmentId, candidate, deep) {
  const facts = (deep.establishedFacts || []).join(" ");
  if (segmentId === "local_catering_day_meetings") {
    if (/monthly|September|October|recurring/i.test(facts)) {
      return "The verified recurring lunch programme creates a legitimate opening to approach organizers about future venue rotations, pilot breakfast/lunch dates, or overflow requirements. No verified open sourcing window or next bookable date after the research cut-off has been established — treat as active monitoring, not urgent displacement of the incumbent.";
    }
  }
  if (segmentId === "relocation_extended_stay") {
    return "Historical expansion or mobility signals may justify a qualified outreach to mobility/HR contacts, but no verified future inbound assignment cohort or temporary-lodging RFP has been established. Do not invent urgency from employment-expansion headlines.";
  }
  if (segmentId === "government_public_sector") {
    return "Public-sector programme history may support relationship development for future editions, but no verified open procurement window or forthcoming bookable edition has been confirmed from public research alone.";
  }
  return (
    "No time-critical sourcing trigger is established. Monitor until a programme owner confirms dates and venue openness."
  );
}

function buildNextStep(segmentId, hotel, contacts, deep) {
  const hotelName = hotelLabel(hotel);
  const primary = contacts[0];
  const secondary = contacts[1];
  if (segmentId === "local_catering_day_meetings") {
    const who = primary?.name || "the event-routing contact";
    const fromDeep = text(deep.recommendedNextStep);
    // Prefer deep copy only when it is catering-shaped (not Scaling/Events fallback).
    if (
      fromDeep &&
      !/corporate travel|visiting-staff lodging|meetings\s*\/\s*congress\s*\/\s*(operations|housing)/i.test(
        fromDeep,
      )
    ) {
      // Prefer "Approach" over leading "Contact <Name>" so Events buyer rewrite
      // cannot swap this into congress/housing boilerplate if mis-gated.
      return fromDeep.replace(/^Contact\s+/i, "Approach ");
    }
    return `Approach ${who} to confirm future breakfast/lunch dates, identify who selects venues, and understand the existing Club de Industriales arrangement. Propose a no-room-night pilot breakfast or lunch-conference at ${hotelName}, then qualify attendance, banquet layout, F&B budget and AV. Do not lead with corporate lodging.`;
  }
  if (segmentId === "government_public_sector") {
    return (
      text(deep.recommendedNextStep) ||
      `Identify the programme or procurement owner and confirm whether private-hotel lodging or meeting venues are eligible. Introduce ${hotelName} only after accessibility and contracting pathway are clarified.`
    );
  }
  if (segmentId === "relocation_extended_stay") {
    return (
      text(deep.recommendedNextStep) ||
      `Qualify whether any inbound assignees, trainers or visiting implementation teams require temporary lodging distinct from local hiring. Introduce ${hotelName} only after mobility/travel ownership and stay pattern are confirmed — do not treat job-creation headlines as room-night forecasts.`
    );
  }
  return text(deep.recommendedNextStep) || null;
}

function defaultQualificationQuestions(segmentId, deep) {
  const fromDeep = Array.isArray(deep.qualificationQuestions)
    ? deep.qualificationQuestions.map(text).filter(Boolean)
    : [];
  if (fromDeep.length) return fromDeep;
  if (segmentId === "local_catering_day_meetings") {
    return [
      "What are the next available breakfast or lunch dates, and who selects the venue?",
      "What seating capacity, F&B budget and AV requirements apply — and is a pilot or overflow date acceptable?",
    ];
  }
  if (segmentId === "relocation_extended_stay") {
    return [
      "Are any employees or client teams traveling to this location from outside the city for temporary stays?",
      "Who coordinates temporary accommodation or corporate lodging for those visitors?",
    ];
  }
  if (segmentId === "government_public_sector") {
    return [
      "Is there a forthcoming edition with open venue or lodging sourcing?",
      "Who owns programme logistics versus formal procurement for hotel spend?",
    ];
  }
  return [];
}

/**
 * Write a full semantic CIB + opportunity presentation fields from existing research.
 */
export function writeSegmentSemanticCib({
  hotel = {},
  segmentId,
  candidate = {},
  deepResearch = {},
} = {}) {
  const segment = resolveDemandSegment(segmentId);
  const deep = deepResearch || {};
  const identityPack = buildUniversalOpportunityIdentity({
    segmentId: segment?.id || segmentId,
    candidate,
    deepResearch: deep,
  });
  const org =
    identityPack.primaryOrganizationName ||
    text(candidate.organizationName) ||
    "Organization";
  const programme =
    identityPack.verifiedProgrammeName || identityPack.displayTitle || null;
  const identity = identityPack.displayTitle;
  const hotelName = hotelLabel(hotel);
  const contacts = mapContacts(deep, candidate).map((c) => ({
    ...c,
    organization: c.organization || org,
  }));
  const primary = contacts[0] || null;
  const secondary = contacts[1] || null;
  const deepSources = dedupeSources(deep.supportingSources || []);
  const deepUrlKeys = new Set(
    deepSources.map((s) => text(s.url).toLowerCase().replace(/\/$/, "")),
  );
  const discoveryOnly = (candidate.evidenceUrls || [])
    .map((url) => ({ url, claim: "Discovery evidence URL" }))
    .filter((s) => {
      const key = text(s.url).toLowerCase().replace(/\/$/, "");
      return key && !deepUrlKeys.has(key);
    });
  const sources = dedupeSources([...deepSources, ...discoveryOnly]);
  const thesisSplit = splitHotelThesis(deep.hotelDemandThesis, hotelName);
  const what = buildWhatIsHappening(segment?.id || segmentId, candidate, deep);
  const potential =
    thesisSplit.potential ||
    `Potential hospitality revenue for ${hotelName} remains a hypothesis until programme owners confirm dates, attendance and venue openness.`;
  const whyHotel =
    thesisSplit.whyBody ||
    `Evaluate ${hotelName} against the evidenced programme type and destination only when commercial accessibility supports outreach.`;
  const whyActNow = buildWhyActNow(segment?.id || segmentId, candidate, deep);
  const nextStep = buildNextStep(segment?.id || segmentId, hotel, contacts, deep);
  const questions = defaultQualificationQuestions(segment?.id || segmentId, deep);
  const overviewValues = buildOverviewValues(
    segment?.id || segmentId,
    candidate,
    deep,
    hotel,
  );
  const facts = (deep.establishedFacts || [])
    .map(text)
    .filter((f) => f.length > 20);
  const uncertainties = (deep.uncertainties || candidate.uncertainties || [])
    .map(text)
    .filter(Boolean);

  const cib = {
    version: "gdi-segments-4-9-12-commercial-intelligence-brief-v2",
    sourceMapping: SEGMENT_SEMANTIC_CIB_WRITER_V1,
    semanticContract: SEGMENT_SEMANTIC_CIB_CONTRACT_V1,
    demandSegmentId: segment?.id || segmentId,
    segmentDisplayLabel: segment?.label || segmentId,
    opportunityIdentity: identityPack,
    opportunityOverview: identity,
    whatIsHappening: what,
    companyAndLocalActivity: what,
    potentialBusinessForHotel: potential,
    potentialBusinessForThisHotel: potential,
    potentialBusiness: potential,
    whyThisHotel: whyHotel,
    whyThisHotelAdvantages: thesisSplit.advantages,
    whyThisHotelLimitations: thesisSplit.limitations,
    whyActNow,
    establishedFactsAndUncertainties: { facts, uncertainties },
    evidenceAndUncertainty: {
      establishedFacts: facts,
      uncertainties,
      sources,
    },
    whoToContact: {
      primary: primary
        ? {
            name: primary.name,
            title: primary.title,
            organization: primary.organization,
            whyRelevant: primary.whyRelevant,
            email: primary.email,
            sourceUrl: primary.sourceUrl,
          }
        : null,
      secondary: secondary
        ? {
            name: secondary.name,
            title: secondary.title,
            organization: secondary.organization,
            whyRelevant: secondary.whyRelevant,
            email: secondary.email,
            sourceUrl: secondary.sourceUrl,
          }
        : null,
      narrative: primary
        ? null
        : "Named commercially relevant contact not yet verified from public sources.",
    },
    recommendedNextStep: nextStep,
    recommendedSalesApproach: {
      nextAction: nextStep,
      qualificationQuestions: questions.slice(0, 8),
    },
    qualificationQuestions: questions,
    supportingEvidence: sources,
    segmentOverview: overviewValues,
    hotelValidationActionOutcome: {
      validation: "Pending hotel team review",
      action: nextStep,
      outcome: null,
    },
    scalingBoilerplateExcluded: true,
  };

  return {
    identity,
    opportunityIdentity: identityPack,
    programmeName: programme,
    organizationName: org,
    organizationNameRaw: identityPack.identityEvidence?.rawOrganizationField || org,
    relatedOrganizations: identityPack.relatedOrganizations || [],
    title: identity,
    displayTitle: identity,
    drawerHeaderTitle: identity,
    whatIsHappening: what,
    whyThisHotel: whyHotel,
    whyThisHotelAdvantages: thesisSplit.advantages,
    whyThisHotelLimitations: thesisSplit.limitations,
    whyNow: whyActNow,
    recommendedNextStep: nextStep,
    recommendedAction: nextStep,
    establishedFacts: facts,
    uncertainties,
    segmentOverview: {
      ...overviewValues,
      identityLine: identity,
      primaryOrganization: org,
      verifiedProgrammeName: programme,
      relatedOrganizations: identityPack.relatedOrganizations || [],
      opportunityLevel: identityPack.opportunityLevel,
    },
    commercialIntelligenceBrief: cib,
    primaryContact: primary
      ? {
          name: primary.name,
          title: primary.title,
          role: primary.title,
          organization: primary.organization,
          email: primary.email,
        }
      : null,
    secondaryContact: secondary
      ? {
          name: secondary.name,
          title: secondary.title,
          role: secondary.title,
          organization: secondary.organization,
          email: secondary.email,
        }
      : null,
    sources,
    evidenceUrls: sources.map((s) => s.url),
  };
}
