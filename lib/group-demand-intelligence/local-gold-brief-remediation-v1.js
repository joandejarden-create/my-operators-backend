/**
 * Local gold-brief / drawer remediation V1 (run-034).
 * Recovers FullE2E evidence into opportunity-specific commercial briefs.
 * Does NOT invent demand, room nights, or SALES_READY.
 */
import { scrubAssociationEventsPhone } from "./research/association-events-buyer-integrity-v1.js";
import { isAssociationEventsValidPersonName } from "./research/association-events-buyer-integrity-v1.js";
import {
  DOS_GRADE,
  GOLD_BRIEF_STATUS,
  assessOutreachRoutes,
} from "./commercial-sales-quality-v1.js";
import { applyCommercialContactQualityOverlay } from "./local-shadow-commercial-portfolio-v1.js";

export const LOCAL_GOLD_BRIEF_REMEDIATION_V1 = "gdi-local-gold-brief-remediation-v1";

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

/** Collapse duplicated facility / limitation phrases in whyThisHotel boilerplate. */
export function dedupeFacilityProse(raw) {
  let s = text(raw);
  if (!s) return "";
  // Split on common join points then unique
  const parts = s
    .split(/(?<=\d{3})\s+(?=[A-ZÁÉÍÓÚ])|(?<=convention)\s+(?=Limitation)|(?<=demand)\s+(?=Limitation)/i)
    .map((p) => text(p))
    .filter(Boolean);
  const seen = new Set();
  const out = [];
  for (const p of parts.length > 1 ? parts : [s]) {
    const key = p.toLowerCase();
    if (seen.has(key)) continue;
    // Also skip near-duplicates
    let dup = false;
    for (const prev of seen) {
      if (prev.includes(key) || key.includes(prev)) {
        if (key.length <= prev.length) {
          dup = true;
          break;
        }
      }
    }
    if (dup) continue;
    seen.add(key);
    out.push(p);
  }
  // Fallback sentence-level uniqueness
  if (out.length <= 1 && /Chapultepec \/ Polanco/.test(s)) {
    const sentences = s
      .replace(/(Limitation to respect:)/gi, "| $1")
      .split(/\s*\|\s*/)
      .map((x) => text(x))
      .filter(Boolean);
    const uniq = [];
    const sk = new Set();
    for (const sent of sentences) {
      const k = sent.toLowerCase();
      if (sk.has(k)) continue;
      sk.add(k);
      uniq.push(sent);
    }
    return uniq.join(" ");
  }
  return out.join(" ");
}

export function interpretGeographyFromSources(sources = []) {
  const blob = (sources || [])
    .map((s) => `${s?.url || ""} ${s?.excerpt || ""}`)
    .join(" \n ");
  const paris =
    /in[- ]person in Paris|Congress in Paris|Paris,\s*France|May 26[–-]30,\s*2026/i.test(
      blob,
    );
  const cdmx2027 =
    /LASA2027|Junio 16\s*[–-]\s*20,\s*2027|June 16\s*[–-]\s*20,\s*2027|Ciudad de M[eé]xico,\s*M[eé]xico.*2027|2027.*Ciudad de M[eé]xico/i.test(
      blob,
    );
  const cdmxGeneric =
    /Ciudad de M[eé]xico|Mexico City|CDMX|Expo Reforma|Chapultepec|Polanco|Reforma/i.test(
      blob,
    );
  return {
    paris2026Congress: paris,
    lasa2027MexicoCity: cdmx2027,
    mexicoCityMention: cdmxGeneric,
    blobSample: blob.slice(0, 240),
  };
}

export function scrubContactPhone(phone) {
  return scrubAssociationEventsPhone(phone) || null;
}

export function computeEvidenceConfidence(sources = []) {
  const n = (sources || []).filter((s) => text(s?.url || s)).length;
  if (!n) return { score: null, explanation: "No source URLs attached to this opportunity." };
  const firstParty = (sources || []).filter((s) =>
    /lasaweb\.org|andellac|anade\.org|canaco|medicosalfrente|urgentcareassociation|fia\.org|myvarsity/i.test(
      String(s.url || ""),
    ),
  ).length;
  const score = Math.min(92, 35 + n * 8 + firstParty * 5);
  return {
    score,
    explanation: `${n} source URL(s) attached (${firstParty} look association/official). Evidence confidence is not hotel fit or win probability.`,
  };
}

function factInferenceSplit(sources = [], claims = []) {
  const facts = [];
  const inferences = [];
  for (const c of claims) {
    if (!c?.text) continue;
    if (c.kind === "FACT") facts.push(c.text);
    else inferences.push(c.text);
  }
  return { establishedFacts: facts, reliableInferences: inferences };
}

/**
 * Rebuild LASA opportunity: 2026 Paris is not Chapultepec lodging demand;
 * surface LASA2027 CDMX as continuation lead from existing call PDF evidence.
 */
export function remediateLasaChapultepecOpportunity(opp = {}) {
  const sources = opp.sources || opp.evidenceUrls || [];
  const geo = interpretGeographyFromSources(sources);
  const evidence = computeEvidenceConfidence(sources);

  const claims = factInferenceSplit(sources, [
    {
      kind: "FACT",
      text: "LASA2026 (XLIV International Congress) is published as exclusively in-person in Paris, France, May 26–30, 2026 (lasaweb.org general information).",
    },
    {
      kind: "FACT",
      text: "LASA2027 call materials publish the next congress in Ciudad de México, México, June 16–20, 2027 (lasaweb.org call PDF already retrieved).",
    },
    {
      kind: "INFERENCE",
      text: "Grand Fiesta Americana Chapultepec cannot pursue LASA2026 guestroom/meeting demand for a Paris congress; any Chapultepec thesis must attach to a Mexico City cycle (e.g. LASA2027), not to LASA2026 lodging.",
    },
    {
      kind: "INFERENCE",
      text: "LASA2027 CDMX may create mid-size speaker/delegate overflow or side-meeting demand near Reforma/Chapultepec — hosting the full congress is not assumed and must be researched separately.",
    },
  ]);

  const salesBrief = {
    version: LOCAL_GOLD_BRIEF_REMEDIATION_V1,
    opportunityOverview:
      "LASA2026 is the Latin American Studies Association XLIV Congress — verified in-person in Paris, France, 26–30 May 2026. It is not a Mexico City lodging event. Captured evidence also includes the LASA2027 call placing the following congress in Ciudad de México, 16–20 June 2027.",
    companyAndLocalActivity:
      "LASA publishes congress programs and calls on lasaweb.org. Retrieved sources include the LASA2026 final program / FAQs (Paris) and the LASA2027 proposal call (Ciudad de México).",
    potentialBusinessForThisHotel:
      "No credible guestroom/meeting demand for Grand Fiesta Americana Chapultepec from LASA2026 itself (Paris). Future-demand lead: research LASA2027 CDMX (June 2027) for overflow, speaker housing, board/side meetings near Reforma/Chapultepec — without assuming Chapultepec is the congress host.",
    whyThisHotel:
      "Chapultepec / Reforma / Polanco positioning is relevant only if pursuing Mexico City-cycle demand (LASA2027+). Same-city presence alone is not proof of win. Hotel is not a substitute host for a Paris congress.",
    whyThisHotelAdvantages: [
      "Reforma / Chapultepec corridor useful for CDMX congress overflow and executive side meetings if LASA2027 demand materializes",
      "Meeting inventory suited to mid-size committee / speaker groups (not assumed citywide host)",
    ],
    whyThisHotelLimitations: [
      "LASA2026 demand geography is Paris — not actionable for this hotel",
      "Not positioned as host for every large citywide convention",
      "Named hotel-buyer for LASA2027 not yet verified",
    ],
    buyingAndSourcingStatus:
      "LASA2026 lodging for Paris is irrelevant to Chapultepec. LASA2027 CDMX venue/housing control not confirmed in captured sources — research continuation required before outreach.",
    whoToContact: {
      primary: null,
      secondary: {
        name: "Jo-Marie Burt",
        title: "Program Committee Chair (LASA/Americas; former LASA President) — research candidate, not verified hotel buyer",
        email: null,
        phone: null,
        url: "https://americas.lasaweb.org/en/organizing-committee/",
        personhood: "RESEARCH_CANDIDATE",
      },
      routes: {
        email: "lasa@lasaweb.org",
        phone: null,
        url: "https://lasaweb.org/uploads/5-call2027-en.pdf",
        label: "Official LASA association route (functional) — not a named hotel buyer",
      },
    },
    recommendedSalesApproach: {
      nextAction:
        "Do not pitch Chapultepec against LASA2026 Paris lodging. Open a LASA2027 CDMX research continuation: confirm venue, housing bureau, and meetings owner via lasa@lasaweb.org / congress pages before any hotel sales assignment.",
      conversationOpener:
        "We're tracking LASA's 2027 Ciudad de México congress dates from your published call — who on the LASA meetings or local organizing side handles attendee lodging and satellite meeting hotels?",
      qualificationQuestions: [
        "Is LASA2027 confirming a headquarters hotel or housing bureau in CDMX?",
        "Will overflow / speaker housing be released to hotels near Reforma/Chapultepec?",
        "Who owns supplier conversations for lodging vs venue?",
      ],
      timingRationale:
        "LASA2026 (May 2026, Paris) is not a Chapultepec pursuit window. LASA2027 (June 2027, CDMX) is the forward research window — proposal deadline evidence in call PDF is 9 Sep 2026 ET for paper proposals, not hotel RFPs.",
    },
    evidenceAndUncertainty: {
      sources: sources.slice(0, 8),
      uncertainties: [
        "LASA2027 headquarters hotel / housing control unknown",
        "No verified named hotel buyer for CDMX cycle",
        "Peak room nights unknown — do not invent",
      ],
      originalActivityDate: "2026-05-26",
      factVsInference: claims,
    },
    investigationHistory: {
      researched: [
        "Association Mexico FullE2E continuation row (missing named buyer)",
        "Official LASA2026 FAQ/program sources (Paris dates/city)",
        "LASA2027 call PDF (CDMX dates) already in evidence set",
      ],
      learned: [
        "SOURCE_NOT_INTERPRETED: Paris geography was ignored while CDMX hotel thesis was applied",
        "PRESENTATION_DUPLICATION: facility boilerplate repeated as hotel fit",
        "DEMAND_THESIS_UNSUPPORTED for Chapultepec × LASA2026 lodging",
      ],
      unresolved: [
        "LASA2027 CDMX housing/venue owner",
        "Whether Chapultepec corridor is commercially reachable for overflow",
      ],
      nextResearchAction:
        "Targeted zero/paid continuation on LASA2027 CDMX organizers and housing — do not re-spend on LASA2026 Paris lodging for this hotel",
    },
  };

  const primaryContact = {
    name: null,
    organization: "Latin American Studies Association (LASA)",
    email: "lasa@lasaweb.org",
    phone: null,
    url: "https://lasaweb.org/en/lasa2026/faqs_generalinfo/",
    title: "Association functional route",
    role: "Functional entity",
  };

  const routes = assessOutreachRoutes(primaryContact, {
    organizationName: "Latin American Studies Association",
    evidenceUrls: sources,
    contactPageUrl: primaryContact.url,
    organizationContactEmail: "lasa@lasaweb.org",
  });

  return applyCommercialContactQualityOverlay({
    ...opp,
    title: "LASA2026 (Paris) — historical; LASA2027 CDMX continuation lead",
    organizationName: "Latin American Studies Association (LASA)",
    eventStartDate: "2026-05-26",
    eventEndDate: "2026-05-30",
    eventDateDisplay: "26–30 May 2026 (Paris) · LASA2027 CDMX 16–20 Jun 2027",
    eventLocationSummary: "LASA2026: Paris, France (verified). Forward lead: Ciudad de México, June 2027.",
    hotelOpportunityThesis: salesBrief.whyThisHotel,
    roomDemandStatus: "NOT_APPLICABLE_WRONG_GEOGRAPHY",
    roomDemandRationale: salesBrief.potentialBusinessForThisHotel,
    opportunityQualification: "CLOSED",
    priority: "DISQUALIFIED",
    summaryWhat: salesBrief.opportunityOverview,
    summaryWhyHotel: salesBrief.whyThisHotel,
    summaryWhyMatters: salesBrief.potentialBusinessForThisHotel,
    recommendedAction: salesBrief.recommendedSalesApproach.nextAction,
    salesBrief,
    commercialIntelligenceBrief: salesBrief,
    primaryContact,
    organizationContactEmail: "lasa@lasaweb.org",
    functionalContactEmail: "lasa@lasaweb.org",
    contactUrl: primaryContact.url,
    sources,
    evidenceUrls: sources,
    evidenceConfidence: evidence.score,
    evidenceConfidenceExplanation: evidence.explanation,
    commercialProspectTier: "REJECTED_WRONG_GEOGRAPHY",
    prospectReviewSection: "EXPLORATORY",
    dosGrade: DOS_GRADE.DO_NOT_PURSUE,
    goldBriefStatus: GOLD_BRIEF_STATUS.INSUFFICIENT_COMMERCIAL_EVIDENCE,
    goldBriefComplete: false,
    goldBriefActionable: false,
    goldBriefMissing: [
      "chapultepec_demand_for_lasa2026_unsupported",
      "lasa2027_cdmx_buyer_unverified",
    ],
    researchClassification: "HISTORICAL_WRONG_GEOGRAPHY_WITH_CONTINUATION_LEAD",
    continuationLead: {
      label: "LASA2027 Ciudad de México",
      dates: "2027-06-16 to 2027-06-20",
      city: "Mexico City",
      evidenceUrl: "https://lasaweb.org/uploads/5-call2027-en.pdf",
      status: "RESEARCH_CONTINUATION",
    },
    rootCauseTags: [
      "SOURCE_NOT_INTERPRETED",
      "DEMAND_THESIS_UNSUPPORTED",
      "PRESENTATION_DUPLICATION",
      "RESEARCH_NOT_COMPLETED",
    ],
    localRemediationRun: "run-034",
    contactPersonhood: routes.personhood,
    hasVerifiedNamedRelevantContact: false,
    isFunctionalOrganizationRoute: true,
    hasCredibleOutreachRoute: routes.hasCredibleOutreachRoute,
    outreachRouteKinds: routes.routeKinds,
    inventoryPresentationOverride: "EXCLUDED",
    customerVisible: true,
    updatedAt: new Date().toISOString(),
  });
}

/**
 * Rebuild association salesBrief from FullE2E gold brief + source interpretation.
 */
export function remediateAssociationOpportunity(opp = {}, goldBriefRow = null) {
  if (opp.id === "gdi_e2d_dbb9e91857b5890b") {
    return remediateLasaChapultepecOpportunity(opp);
  }

  const g = goldBriefRow?.brief || {};
  const sources = (g.supportingEvidenceAndUncertainties?.sources || opp.sources || []).slice(
    0,
    8,
  );
  const geo = interpretGeographyFromSources(sources);
  const evidence = computeEvidenceConfidence(sources);
  const who = g.whoToContact?.primary || {};
  const phone = scrubContactPhone(who.phone || opp.primaryContact?.phone);
  const nameOk = isAssociationEventsValidPersonName(who.name || opp.primaryContact?.name);
  const primaryContact = {
    name: nameOk ? text(who.name || opp.primaryContact?.name) : null,
    fullName: nameOk ? text(who.name || opp.primaryContact?.name) : null,
    title: nameOk ? text(who.title) || null : text(who.title) || null,
    role: nameOk ? text(who.title) || null : "Functional / organizational label",
    email: who.email || g.whoToContact?.routes?.email || opp.primaryContact?.email || null,
    phone,
    organization: opp.organizationName,
    url: who.url || g.whoToContact?.routes?.url || opp.primaryContact?.url || null,
    source: who.url || null,
    sourceUrl: who.url || null,
  };
  if (!nameOk && who.name) {
    primaryContact.functionalLabel = text(who.name);
    primaryContact.personhoodNote =
      "Label was not accepted as a verified named person — treat as organizational/functional route when a route exists.";
  }

  const whyHotel = dedupeFacilityProse(g.whyThisHotel || opp.hotelOpportunityThesis || "");
  const activity = text(g.eventHistoryAndActivity || "").slice(0, 900);
  const overview =
    text(g.opportunityOverview) ||
    `${text(opp.title)} — ${text(opp.organizationName)}.`;

  // Geography honesty
  let locationSummary = opp.eventLocationSummary || "Mexico City / CDMX (verify)";
  let demandNote = text(g.potentialBusinessForHotel || opp.roomDemandRationale || "");
  if (geo.paris2026Congress && !geo.lasa2027MexicoCity) {
    locationSummary = "Verified non-CDMX geography in sources — re-check hotel demand thesis";
    demandNote =
      "Source geography conflicts with a Chapultepec lodging thesis — do not treat as local demand until location is reconciled.";
  }

  const salesBrief = {
    version: LOCAL_GOLD_BRIEF_REMEDIATION_V1,
    opportunityOverview: overview.replace(/Timing: to be verified\.?/i, "").trim(),
    companyAndLocalActivity: activity || text(g.organizationBackground) || null,
    potentialBusinessForThisHotel: demandNote,
    whyThisHotel: whyHotel || null,
    whyThisHotelAdvantages: whyHotel
      ? [
          "Reforma / Chapultepec / Polanco access for CDMX association programs when lodging catchment is open",
          "Meeting inventory suited to mid-size speaker/committee groups (not assumed citywide host)",
        ]
      : [],
    whyThisHotelLimitations: [
      "Not positioned as host for every large citywide convention",
      "Peak rooms / housing control not verified in public sources",
      ...(geo.paris2026Congress ? ["Source set includes non-CDMX geography — verify before outreach"] : []),
    ],
    buyingAndSourcingStatus: text(g.buyingAndSourcingStatus) || null,
    whoToContact: {
      primary: nameOk
        ? {
            name: primaryContact.name,
            title: primaryContact.title,
            email: primaryContact.email,
            phone: primaryContact.phone,
            url: primaryContact.url,
          }
        : null,
      secondary: g.whoToContact?.secondary || null,
      routes: {
        email: primaryContact.email || g.whoToContact?.routes?.email || null,
        phone: primaryContact.phone,
        url: primaryContact.url || g.whoToContact?.routes?.url || null,
      },
    },
    recommendedSalesApproach: {
      nextAction:
        text(g.recommendedSalesApproach) ||
        opp.recommendedAction ||
        "Confirm housing/venue owner and whether lodging remains open before pitching the hotel.",
      conversationOpener: `Calling about ${text(opp.title)} — we support association programs needing guestrooms and meetings near Reforma/Chapultepec. Who handles hotel sourcing for this edition?`,
      qualificationQuestions: g.qualificationQuestions || [],
      timingRationale: "Confirm published dates vs venue control before outreach cadence.",
    },
    evidenceAndUncertainty: {
      sources,
      uncertainties: [
        ...(g.supportingEvidenceAndUncertainties?.uncertainties || []),
        "Peak room nights unknown",
        !nameOk ? "Verified named hotel buyer incomplete" : null,
      ].filter(Boolean),
      factVsInference: factInferenceSplit(sources, [
        activity
          ? { kind: "FACT", text: `Public activity excerpt retained from research sources (${sources.length} URLs).` }
          : null,
        demandNote
          ? {
              kind: "INFERENCE",
              text: "Hotel demand thesis is inferred from association activity + property positioning — not a confirmed room block.",
            }
          : null,
      ].filter(Boolean)),
    },
    investigationHistory: {
      researched: ["Mexico Association FullE2E", "Gold brief generation", "Local shadow ingest run-033"],
      learned: ["Recovered source excerpts into drawer salesBrief", "Deduped facility boilerplate"],
      unresolved: [
        !nameOk ? "Named buyer verification" : null,
        "Housing/venue contracting status",
      ].filter(Boolean),
      nextResearchAction: !nameOk
        ? "Continue Spanish/English organizer + housing research before ASSIGN_NOW"
        : "Confirm lodging control and approach window; then DOS assign",
    },
  };

  const routes = assessOutreachRoutes(primaryContact, {
    organizationName: opp.organizationName,
    evidenceUrls: sources,
    contactPageUrl: primaryContact.url,
    organizationContactEmail: primaryContact.email,
  });

  const tier =
    opp.commercialProspectTier ||
    (nameOk && routes.hasCredibleOutreachRoute
      ? "PRIORITY_SALES_PROSPECT"
      : routes.hasCredibleOutreachRoute || nameOk
        ? "EXPLORATORY_SALES_PROSPECT"
        : "RESEARCH_CONTINUATION");

  return applyCommercialContactQualityOverlay({
    ...opp,
    eventLocationSummary: locationSummary,
    hotelOpportunityThesis: salesBrief.whyThisHotel,
    roomDemandRationale: salesBrief.potentialBusinessForThisHotel,
    summaryWhat: salesBrief.opportunityOverview,
    summaryWhyHotel: salesBrief.whyThisHotel,
    summaryWhyMatters: salesBrief.potentialBusinessForThisHotel,
    recommendedAction: salesBrief.recommendedSalesApproach.nextAction,
    salesBrief,
    commercialIntelligenceBrief: salesBrief,
    primaryContact,
    organizationContactEmail: primaryContact.email,
    sources: sources.length ? sources : opp.sources,
    evidenceUrls: sources.length ? sources : opp.evidenceUrls,
    evidenceConfidence: evidence.score,
    evidenceConfidenceExplanation: evidence.explanation,
    commercialProspectTier: tier,
    prospectReviewSection:
      tier === "PRIORITY_SALES_PROSPECT" ? "PRIORITY" : "EXPLORATORY",
    dosGrade:
      tier === "PRIORITY_SALES_PROSPECT"
        ? DOS_GRADE.ASSIGN_FOR_DISCOVERY
        : tier === "EXPLORATORY_SALES_PROSPECT"
          ? DOS_GRADE.ASSIGN_FOR_DISCOVERY
          : DOS_GRADE.RESEARCH_FIRST,
    goldBriefStatus:
      tier === "RESEARCH_CONTINUATION"
        ? GOLD_BRIEF_STATUS.RESEARCH_CONTINUATION
        : GOLD_BRIEF_STATUS.ACTIONABLE_WITH_DISCLOSED_GAPS,
    localRemediationRun: "run-034",
    contactPersonhood: routes.personhood,
    hasVerifiedNamedRelevantContact: routes.hasVerifiedNamedRelevantContact,
    isFunctionalOrganizationRoute: routes.isFunctionalOrganizationRoute,
    hasCredibleOutreachRoute: routes.hasCredibleOutreachRoute,
    outreachRouteKinds: routes.routeKinds,
    updatedAt: new Date().toISOString(),
  });
}

/**
 * Enrich Waterstone store opportunity with salesBrief from existing fields (no invention).
 */
export function remediateWaterstoneOpportunity(opp = {}) {
  if (opp.salesBrief?.version === LOCAL_GOLD_BRIEF_REMEDIATION_V1 && opp.salesBrief?.investigationHistory) {
    return applyCommercialContactQualityOverlay(opp);
  }
  const sources = opp.sources || [];
  const evidence = computeEvidenceConfidence(sources);
  const thesis = text(opp.hotelOpportunityThesis || opp.summaryWhyHotel || "");
  const what = text(opp.summaryWhat || opp.title || "");
  const matters = text(opp.summaryWhyMatters || opp.roomDemandRationale || "");
  const alreadyContracted =
    /already an official|already (a )?hotel|existing contracted|boutiqu?e official|official .*hotel|Not a cold prospect/i.test(
      thesis + " " + what,
    );

  const salesBrief = {
    version: LOCAL_GOLD_BRIEF_REMEDIATION_V1,
    opportunityOverview: what,
    companyAndLocalActivity: what,
    potentialBusinessForThisHotel: matters || null,
    whyThisHotel: thesis || null,
    whyThisHotelAdvantages: thesis ? [thesis.slice(0, 220)] : [],
    whyThisHotelLimitations: alreadyContracted
      ? [
          "Existing / official hotel relationship — pursue incremental or retention value, not net-new cold demand",
        ]
      : ["Peak rooms / housing control may still need confirmation"],
    buyingAndSourcingStatus: text(opp.venueSourcingStatusLabel || opp.venueSourcingStatus) || null,
    whoToContact: {
      primary: opp.primaryContact
        ? {
            name: opp.primaryContact.name,
            title: opp.primaryContact.title || opp.primaryContact.role,
            email: opp.primaryContact.email,
            phone: scrubContactPhone(opp.primaryContact.phone),
            url: opp.primaryContact.sourceUrl || opp.primaryContact.source || opp.primaryContact.url,
          }
        : null,
      secondary: opp.secondaryContact || (opp.backupContacts || [])[0] || null,
      routes: {
        email: opp.primaryContact?.email || opp.organizationContactEmail || null,
        phone: scrubContactPhone(opp.primaryContact?.phone),
        url: opp.primaryContact?.sourceUrl || opp.contactUrl || null,
      },
    },
    recommendedSalesApproach: {
      nextAction: text(opp.recommendedAction || opp.recommendedNextStep) || null,
      conversationOpener: null,
      qualificationQuestions: [],
      timingRationale: text(opp.whyNow || opp.bookingWindowLabel) || null,
    },
    evidenceAndUncertainty: {
      sources: sources.slice(0, 8),
      uncertainties: Array.isArray(opp.unknowns) ? opp.unknowns : [],
      factVsInference: factInferenceSplit(sources, [
        what ? { kind: "FACT", text: "Opportunity summary retained from hotel research store." } : null,
        matters
          ? { kind: "INFERENCE", text: "Commercial use-case narrative is research-assisted — validate with the buyer." }
          : null,
      ].filter(Boolean)),
    },
    investigationHistory: {
      researched: ["Waterstone FullE2E / local store"],
      learned: alreadyContracted
        ? ["Existing contracted / official hotel relationship flagged"]
        : ["Brief rebuilt from stored thesis/contact/sources"],
      unresolved: [],
      nextResearchAction: alreadyContracted
        ? "Coordinate with existing account owner; seek incremental nights / upgrades only"
        : "Confirm lodging control and approach timing",
    },
    commercialRelationship: alreadyContracted ? "EXISTING_OR_OFFICIAL_HOTEL" : "PROSPECT",
  };

  const dosGrade = alreadyContracted
    ? DOS_GRADE.ASSIGN_FOR_DISCOVERY
    : opp.dosGrade || DOS_GRADE.ASSIGN_FOR_DISCOVERY;

  return applyCommercialContactQualityOverlay({
    ...opp,
    salesBrief,
    commercialIntelligenceBrief: salesBrief,
    evidenceConfidence: opp.evidenceConfidence ?? evidence.score,
    evidenceConfidenceExplanation:
      opp.evidenceConfidenceExplanation || evidence.explanation,
    commercialRelationship: salesBrief.commercialRelationship,
    dosGrade,
    // Existing official hotel relationship is account work — not a net-new Priority discovery
    commercialProspectTier: alreadyContracted
      ? "EXPLORATORY_SALES_PROSPECT"
      : opp.commercialProspectTier,
    prospectReviewSection: alreadyContracted
      ? "EXPLORATORY"
      : opp.prospectReviewSection,
    localRemediationRun: "run-034",
    primaryContact: opp.primaryContact
      ? { ...opp.primaryContact, phone: scrubContactPhone(opp.primaryContact.phone) }
      : opp.primaryContact,
    updatedAt: new Date().toISOString(),
  });
}

export function classifyRemediatedOpportunity(opp = {}) {
  const tier = String(opp.commercialProspectTier || "").toUpperCase();
  const dos = String(opp.dosGrade || "").toUpperCase();
  const rel = String(opp.commercialRelationship || opp.researchClassification || "");
  if (
    tier.includes("REJECTED") ||
    dos === DOS_GRADE.DO_NOT_PURSUE ||
    /WRONG_GEOGRAPHY|HISTORICAL/i.test(rel)
  ) {
    return "HISTORICAL_OR_IRRELEVANT";
  }
  if (tier === "RESEARCH_CONTINUATION" || dos === DOS_GRADE.RESEARCH_FIRST) {
    return "INCOMPLETE_CONTINUATION";
  }
  if (opp.goldBriefStatus === GOLD_BRIEF_STATUS.COMPLETE_AND_ACTIONABLE) {
    return "COMPLETE_ACTIONABLE";
  }
  if (
    tier === "PRIORITY_SALES_PROSPECT" ||
    tier === "EXPLORATORY_SALES_PROSPECT" ||
    dos === DOS_GRADE.ASSIGN_NOW ||
    dos === DOS_GRADE.ASSIGN_FOR_DISCOVERY
  ) {
    return "ACTIONABLE_WITH_GAPS";
  }
  return "INCOMPLETE_CONTINUATION";
}
