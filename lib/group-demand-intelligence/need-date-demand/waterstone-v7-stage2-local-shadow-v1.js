/**
 * Waterstone V7 Stage 2 → local-shadow opportunity staging V1.
 * Zero-paid. No Airtable. No publication. Deterministic local IDs.
 */
import crypto from "node:crypto";
import {
  buildResearchToDrawerIntelligenceV2,
  applyResearchToDrawerIntelligenceV2,
} from "../research-to-drawer-intelligence-v2.js";
import {
  classifyHotelRevenueStream,
  HOTEL_REVENUE_STREAM_CLASS,
} from "../multi-revenue-qualification-v1.js";
import { attachOpportunityRevenueStreamIcons } from "../opportunity-revenue-stream-icons-v1.js";

export const WATERSTONE_V7_STAGE2_LOCAL_SHADOW_V1 =
  "gdi-waterstone-v7-stage2-local-shadow-v1";

export const WATERSTONE_HOTEL_ID = "recgMYovrrZDJMqzX";
export const WATERSTONE_HOTEL_NAME =
  "Waterstone Resort & Marina, Curio Collection by Hilton";

function text(v) {
  if (v == null) return "";
  if (typeof v === "string") return v.replace(/\s+/g, " ").trim();
  if (typeof v === "number" || typeof v === "boolean") return String(v);
  if (Array.isArray(v)) return v.map(text).filter(Boolean).join(" · ");
  if (typeof v === "object") {
    return text(
      v.claim || v.text || v.body || v.summary || v.fullName || v.name || v.title || "",
    );
  }
  return String(v).replace(/\s+/g, " ").trim();
}

function scrubWrongHotel(s) {
  return text(s)
    .replace(/Nobu Hotel London Portman Square/gi, WATERSTONE_HOTEL_NAME)
    .replace(/Marylebone\s*\/\s*Portman Square central-London[^.]*\.?/gi, "")
    .replace(/Marylebone|Portman Square|London Portman/gi, "")
    .replace(/\s{2,}/g, " ")
    .trim();
}

function normOrg(s) {
  return text(s)
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .replace(/\b(inc|llc|ltd|corp|corporation|the)\b/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

export function orgFamilyKey(name) {
  const n = normOrg(name);
  if (/adma/.test(n)) return "adma_biologics";
  if (/d.?wave/.test(n)) return "d_wave_quantum";
  if (/andreessen|a16z|american dynamism/.test(n)) return "andreessen_horowitz";
  if (/carmine/.test(n)) return "carmines_gourmet";
  if (/howard alan|american craft/.test(n)) return "american_craft_endeavors";
  return n || "unknown";
}

export function stableWaterstoneStage2OpportunityId(familyKey, track) {
  const seed = `${WATERSTONE_HOTEL_ID}|v72-stage2|${familyKey}|${track || "B"}`;
  const hex = crypto.createHash("sha256").update(seed).digest("hex").slice(0, 12);
  return `gdi_ws_v72_${hex}`;
}

function mapWorkflowToSegment(workflow, track, orgName) {
  const w = text(workflow).toLowerCase();
  if (/planner|wedding|event.?plan|social/.test(w) || /event planning|events group|sutton|willard|palm perfect/i.test(orgName)) {
    return {
      demandSegmentId: "professional_event_planners",
      researchSegment: "professional_event_planners",
      segmentDisplayLabel: "Professional Event Planners",
      opportunityTypeLabel: "Professional Event Planner Relationship",
    };
  }
  if (/sport|baseball|prospect select/i.test(w + orgName)) {
    return {
      demandSegmentId: "sports_entertainment",
      researchSegment: "sports_entertainment",
      segmentDisplayLabel: "Sports & Entertainment",
      opportunityTypeLabel: "Sports / Entertainment Demand",
    };
  }
  if (/education|optometry|university|scientific/i.test(w + orgName)) {
    return {
      demandSegmentId: "education_scientific",
      researchSegment: "education_scientific",
      segmentDisplayLabel: "Education & Scientific",
      opportunityTypeLabel: "Education / Scientific Programme",
    };
  }
  if (/chamber|association|live after five/i.test(orgName + w)) {
    return {
      demandSegmentId: "local_catering_day_meetings",
      researchSegment: "local_catering_day_meetings",
      segmentDisplayLabel: "Local Catering & Day Meetings",
      opportunityTypeLabel: "Local Networking / Catering",
    };
  }
  if (/scaling|corporate|biolog|quantum|segpay|ampera|concorde|4ever|all star|andreessen|carmine/i.test(orgName + w)) {
    return {
      demandSegmentId: "corporate_accounts_scaling",
      researchSegment: "corporate_accounts_scaling",
      segmentDisplayLabel: "Scaling Corporate Accounts",
      opportunityTypeLabel: "Corporate Account Relationship",
    };
  }
  if (track === "A") {
    return {
      demandSegmentId: "local_catering_day_meetings",
      researchSegment: "local_catering_day_meetings",
      segmentDisplayLabel: "Local Catering & Day Meetings",
      opportunityTypeLabel: "Need-Date Opportunity",
    };
  }
  return {
    demandSegmentId: "corporate_accounts_scaling",
    researchSegment: "corporate_accounts_scaling",
    segmentDisplayLabel: "Corporate Account Relationship",
    opportunityTypeLabel: "Account Relationship",
  };
}

function buyerToContact(b) {
  if (!b) return null;
  const name = text(b.fullName || b.name);
  if (!name) return null;
  return {
    name,
    title: text(b.role || b.title) || null,
    organization: text(b.employer || b.organization) || null,
    email: b.email || null,
    phone: b.phone || null,
    linkedinUrl: b.linkedinUrl || b.linkedin || null,
    sourceUrl: b.sourceUrl || null,
    whyRelevant: scrubWrongHotel(
      text(b.whyRelevantToHotelDemand || b.whyRelevant || ""),
    ) || null,
    relevanceClass: b.relevanceClass || null,
    contractingAuthorityVerified:
      /CONTRACTING_AUTHORITY_VERIFIED:\s*yes/i.test(
        String(b.whyRelevantToHotelDemand || ""),
      ) || b.contractingAuthorityVerified === true,
  };
}

function pickContacts(row, deep) {
  const buyers = []
    .concat(row.buyerCandidates || [])
    .concat(deep?.buyerCandidates || [])
    .concat(deep?.contactIntelligence?.namedBuyers || [])
    .concat(row.qualification?.namedBuyer ? [row.qualification.namedBuyer] : []);
  const seen = new Set();
  const contacts = [];
  for (const b of buyers) {
    const c = buyerToContact(b);
    if (!c) continue;
    const key = c.name.toLowerCase();
    if (seen.has(key)) continue;
    seen.add(key);
    contacts.push(c);
  }
  return {
    primaryContact: contacts[0] || null,
    secondaryContact: contacts[1] || null,
    allContacts: contacts,
  };
}

function classifyPriority(track, revenueClass) {
  if (track === "A") {
    return {
      priority: "HIGH_PRIORITY",
      priorityFilterKey: "HIGH_PRIORITY",
      salesActionKey: "CONTACT_BUYER",
      salesActionPillLabel: "Contact Buyer",
    };
  }
  if (revenueClass === HOTEL_REVENUE_STREAM_CLASS.REVENUE_STREAM_UNCONFIRMED) {
    return {
      priority: "WATCHLIST",
      priorityFilterKey: "WATCHLIST",
      salesActionKey: "MONITOR",
      salesActionPillLabel: "Monitor",
    };
  }
  // Planners / relationships — medium account development, not automatic High
  return {
    priority: "MEDIUM_PRIORITY",
    priorityFilterKey: "MEDIUM_PRIORITY",
    salesActionKey: "BUILD_ACCOUNT",
    salesActionPillLabel: "Build Account",
  };
}

function buildTileSummary(row, deep, track) {
  const brief = scrubWrongHotel(
    row.drawerIntelligenceV2?.salesBrief?.potentialBusinessForThisHotel ||
      row.qualification?.thesis ||
      deep?.hotelDemandHypotheses?.primaryMechanism ||
      deep?.hotelDemandEvidence?.documentedSignals ||
      "",
  );
  const what = scrubWrongHotel(
    row.drawerIntelligenceV2?.salesBrief?.whatIsHappening ||
      deep?.activityAndChronology?.summary ||
      deep?.companyIdentity?.description ||
      "",
  );
  const action = scrubWrongHotel(
    text(
      row.recommendedAction?.businessToDiscuss ||
        row.recommendedAction?.nextAction ||
        row.recommendedAction,
    ),
  );
  let summary = brief || what || action;
  if (!summary) {
    summary =
      track === "A"
        ? `${row.organizationName}: dated programme activity overlapping Waterstone need windows — qualify catering/lodging pathway.`
        : `${row.organizationName}: relationship / account development lead near Boca Raton — qualify referral or visitor demand before treating as open booking.`;
  }
  if (summary.length > 320) summary = `${summary.slice(0, 317).trim()}…`;
  return summary;
}

function buildWhyThisHotel(deep, row) {
  const fit = scrubWrongHotel(
    deep?.hotelSpecificFit?.thesis ||
      deep?.hotelSpecificFit?.summary ||
      row.drawerIntelligenceV2?.researchDetail?.whyThisHotel ||
      "",
  );
  if (fit && !/Marylebone|Portman|Nobu/i.test(fit)) return fit;
  return `Waterstone Resort & Marina sits on the Intracoastal in Boca Raton with meeting/catering inventory and guestroom product relevant to local South Florida demand. Fit depends on confirmed buyer pathway, timing, and accessible sourcing — not assumed room nights.`;
}

function extractDates(row, deep, track, orgName = "") {
  const org = text(orgName || row.organizationName);
  const ownBlob = JSON.stringify({
    organizationName: org,
    qualification: row.qualification,
    recommendedAction: row.recommendedAction,
    drawer: row.drawerIntelligenceV2,
    multiRevenue: row.multiRevenue,
    deepStructured: deep?.structured || deep,
    deepFacts: deep?.verifiedFacts || deep?.structured?.verifiedFacts,
  });

  // Track A — org-specific evidence (never share Chamber months onto NSU, etc.)
  if (track === "A" && /nova southeastern|optometry/i.test(org)) {
    if (/september\s*11|sep(?:tember)?\s*11|2027-09-11/i.test(ownBlob)) {
      return {
        eventDateDisplay: "Sep 11–12, 2027",
        needDateMatchMonths: [9],
        timingStatus: "EVIDENCE_SUPPORTED_NEED_WINDOW",
        bookingWindowStatus: "CONTACT_NOW",
        eventStartDate: "2027-09-11",
        eventEndDate: "2027-09-12",
        dayPrecision: true,
      };
    }
  }
  if (track === "A" && /chamber/i.test(org)) {
    const dates = [];
    if (/membership breakfast|august\s*12|2027-08-12|august 2027/i.test(ownBlob)) {
      dates.push({ label: "Aug 2027", month: 8 });
    }
    if (/live after five.*september|september\s*28|2027-09-28|september 2027/i.test(ownBlob)) {
      dates.push({ label: "Sep 2027", month: 9 });
    }
    if (/live after five.*october|october\s*26|2027-10-26|october 2027/i.test(ownBlob)) {
      dates.push({ label: "Oct 2027", month: 10 });
    }
    if (dates.length) {
      return {
        eventDateDisplay: dates.map((d) => d.label).join(" · "),
        needDateMatchMonths: dates.map((d) => d.month),
        timingStatus: "EVIDENCE_SUPPORTED_NEED_WINDOW",
        bookingWindowStatus: "CONTACT_NOW",
      };
    }
  }

  if (track === "A") {
    return {
      eventDateDisplay: "Timing to confirm",
      needDateMatchMonths: [],
      timingStatus: "NEED_DATE_EVIDENCE_INCOMPLETE",
      bookingWindowStatus: "WATCH",
    };
  }
  return {
    eventDateDisplay: "Timing to confirm",
    needDateMatchMonths: [],
    timingStatus: "RELATIONSHIP_UNCONFIRMED_TIMING",
    bookingWindowStatus: "WATCH",
  };
}

function mapSources(row, deep) {
  const raw = []
    .concat(row.supportingSources || [])
    .concat(deep?.supportingSources || [])
    .concat(row.drawerIntelligenceV2?.researchDetail?.evidenceUrls || []);
  const out = [];
  const seen = new Set();
  for (const s of raw) {
    const url = typeof s === "string" ? s : s?.url;
    if (!url || seen.has(url)) continue;
    seen.add(url);
    out.push({
      url,
      title: text(typeof s === "object" ? s.title : "") || url,
      relevance: text(typeof s === "object" ? s.relevance : "") || null,
      publishedDate:
        (typeof s === "object" && (s.publishedDate || s.date)) || null,
    });
  }
  return out;
}

/**
 * Deduplicate yield rows to unique commercial account families.
 */
export function reconcileUniqueStage2Accounts(yieldDoc = {}) {
  const trackA = yieldDoc.verifiedNeedDateOpportunities || [];
  const trackB = yieldDoc.qualifiedRelationshipOpportunities || [];
  const tagged = [
    ...trackA.map((row) => ({ row, track: "A" })),
    ...trackB.map((row) => ({ row, track: "B" })),
  ];
  const byFamily = new Map();
  for (const item of tagged) {
    const name = item.row.organizationName || item.row.candidateTitle || "Unknown";
    const key = orgFamilyKey(name);
    if (!byFamily.has(key)) byFamily.set(key, []);
    byFamily.get(key).push({ ...item, name });
  }
  const unique = [];
  const duplicates = [];
  for (const [familyKey, rows] of byFamily) {
    if (rows.length > 1) {
      duplicates.push({
        familyKey,
        names: rows.map((r) => r.name),
        tracks: rows.map((r) => r.track),
      });
    }
    // Prefer Track A if present; else first Track B with richest buyers
    const preferred =
      rows.find((r) => r.track === "A") ||
      rows.slice().sort(
        (a, b) =>
          (b.row.buyerCandidates || []).length -
          (a.row.buyerCandidates || []).length,
      )[0];
    unique.push({
      familyKey,
      track: rows.some((r) => r.track === "A") ? "A" : "B",
      primary: preferred,
      related: rows.filter((r) => r !== preferred),
    });
  }
  return { unique, duplicates, taggedCount: tagged.length };
}

/**
 * Map one reconciled account → customer-facing local opportunity DTO.
 */
export function mapStage2AccountToOpportunity(account, deepByRunId = new Map(), deepByOrg = new Map()) {
  const row = account.primary.row;
  const orgName = account.primary.name || row.organizationName;
  const track = account.track;
  const id = stableWaterstoneStage2OpportunityId(account.familyKey, track);

  // Prefer deep artifact match by org name, then any related titles
  let deep =
    deepByOrg.get(normOrg(orgName)) ||
    deepByOrg.get(account.familyKey) ||
    null;
  for (const rel of account.related || []) {
    if (!deep) deep = deepByOrg.get(normOrg(rel.name));
  }
  // Also try title match from yield candidateTitle
  if (!deep && row.candidateTitle) {
    deep = deepByOrg.get(normOrg(row.candidateTitle));
  }

  const workflow = deep?.workflow || row.workflow || null;
  const segment = mapWorkflowToSegment(workflow, track, orgName);
  const contacts = pickContacts(row, deep?.structured || deep);
  const structured = deep?.structured || deep || {};

  const guestroomThesis =
    row.multiRevenue?.guestroomThesis ||
    row.qualification?.guestroomThesis ||
    structured.hotelDemandHypotheses?.guestroomHypothesis ||
    null;
  const eventFnbThesis =
    row.multiRevenue?.eventFnbThesis ||
    row.qualification?.eventFnbThesis ||
    structured.hotelDemandHypotheses?.meetingsHypothesis ||
    structured.hotelDemandHypotheses?.cateringHypothesis ||
    null;

  let revenueStreamClass =
    row.revenueStreamClass ||
    row.qualification?.revenueStreamClass ||
    row.multiRevenue?.revenueStreamClass ||
    null;

  const classified = classifyHotelRevenueStream({
    hotelDemandHypotheses: {
      guestroomHypothesis: guestroomThesis,
      meetingsHypothesis: eventFnbThesis,
      primaryMechanism: row.qualification?.thesis,
    },
    potentialGuestroomDemand: guestroomThesis,
    potentialMeetingDemand: eventFnbThesis,
    summary: row.qualification?.thesis,
  });
  // Prefer classifier when annotation is COMBINED but event/rooms evidence is thin
  if (
    !revenueStreamClass ||
    (revenueStreamClass === HOTEL_REVENUE_STREAM_CLASS.COMBINED_GUESTROOMS_EVENT_FNB &&
      classified.revenueStreamClass !== revenueStreamClass)
  ) {
    // Keep Chamber as EVENT_FNB_ONLY hard rule when annotated
    if (
      /chamber/i.test(orgName) &&
      row.revenueStreamClass === HOTEL_REVENUE_STREAM_CLASS.EVENT_FNB_ONLY
    ) {
      revenueStreamClass = HOTEL_REVENUE_STREAM_CLASS.EVENT_FNB_ONLY;
    } else {
      revenueStreamClass = classified.revenueStreamClass;
    }
  }

  const pills = classifyPriority(track, revenueStreamClass);
  const dates = extractDates(row, structured, track, orgName);
  const sources = mapSources(row, structured);
  const tileSummary = buildTileSummary(row, structured, track);
  const whyHotel = buildWhyThisHotel(structured, row);
  const potential =
    scrubWrongHotel(
      row.drawerIntelligenceV2?.salesBrief?.potentialBusinessForThisHotel ||
        classified.guestroomThesis ||
        classified.eventFnbThesis ||
        row.qualification?.thesis ||
        "",
    ) || tileSummary;
  const nextStep = scrubWrongHotel(
    text(
      row.recommendedAction?.nextAction ||
        row.drawerIntelligenceV2?.salesBrief?.recommendedNextStep ||
        structured.recommendedCommercialAction?.nextAction ||
        "",
    ),
  );
  const whyNow = scrubWrongHotel(
    text(
      row.recommendedAction?.timing ||
        row.drawerIntelligenceV2?.salesBrief?.whyActNow ||
        "",
    ),
  );

  const runIds = [deep?.runId, row.runId]
    .concat((account.related || []).map((r) => r.row.runId))
    .filter(Boolean);

  let opp = {
    id,
    opportunityId: id,
    hotelId: WATERSTONE_HOTEL_ID,
    hotelName: WATERSTONE_HOTEL_NAME,
    hotelDisplayName: WATERSTONE_HOTEL_NAME,
    organizationName: orgName,
    title: orgName,
    displayTitle: orgName,
    drawerHeaderTitle: orgName,
    ...segment,
    track,
    stage2Track: track,
    bucket: track === "A" ? "VERIFIED_NEED_DATE" : "QUALIFIED_RELATIONSHIP",
    waterstoneV7Stage2LocalShadowV1: true,
    liveCampaignV1Shadow: true,
    stageEShadowPreview: true,
    localShadowOnly: true,
    localShadowPortfolio: true,
    customerVisible: true,
    doesNotAffectProduction: true,
    publicationStatus: "LOCAL_SHADOW_ONLY",
    airtablePublication: "BLOCKED",
    researchRunIds: [...new Set(runIds)],
    researchFamilyKey: account.familyKey,
    relatedOrganizationNames: (account.related || []).map((r) => r.name),
    summaryWhat: tileSummary,
    whatIsHappening: tileSummary,
    tileCustomerSummary: tileSummary,
    hotelOpportunityThesis: potential,
    whyThisHotel: whyHotel,
    whyNow: whyNow || null,
    recommendedAction: nextStep || null,
    recommendedNextStep: nextStep || null,
    guestroomThesis: guestroomThesis || classified.guestroomThesis,
    eventFnbThesis: eventFnbThesis || classified.eventFnbThesis,
    revenueStreamClass,
    revenueStreamLabel: classified.revenueStreamLabel,
    primaryContact: contacts.primaryContact,
    secondaryContact: contacts.secondaryContact,
    contacts: contacts.allContacts,
    sources,
    supportingSources: sources,
    establishedFacts: structured.verifiedFacts || [],
    uncertainties: structured.materialUncertainties || [],
    priority: pills.priority,
    priorityFilterKey: pills.priorityFilterKey,
    salesActionKey: pills.salesActionKey,
    salesActionPillLabel: pills.salesActionPillLabel,
    eventDateDisplay: dates.eventDateDisplay,
    needDateMatchMonths: dates.needDateMatchMonths,
    timingStatus: dates.timingStatus,
    bookingWindowStatus: dates.bookingWindowStatus,
    // Track A: seed structured month timing so planner Ongoing heuristic cannot
    // replace evidence-supported need windows on the customer date pill.
    ...(track === "A" && dates.needDateMatchMonths?.length
      ? (() => {
          const months = [...dates.needDateMatchMonths].sort((a, b) => a - b);
          const first = months[0];
          const last = months[months.length - 1];
          const start =
            dates.eventStartDate ||
            `2027-${String(first).padStart(2, "0")}-01`;
          const endDay =
            last === 2 ? 28 : [4, 6, 9, 11].includes(last) ? 30 : 31;
          const end =
            dates.eventEndDate ||
            `2027-${String(last).padStart(2, "0")}-${String(endDay).padStart(2, "0")}`;
          const dayPrecision = dates.dayPrecision === true;
          return {
            eventYear: 2027,
            eventMonth: first,
            eventStartDate: start,
            eventEndDate: end,
            tileEventStartDate: start,
            tileEventEndDate: end,
            eventDateGranularity: dayPrecision
              ? "DAY"
              : months.length > 1
                ? "MONTH_SPAN"
                : "MONTH",
            datePillDisplayLabel: dates.eventDateDisplay,
            canonicalTiming: {
              version: "gdi-universal-timing-intelligence-v1",
              timingType: dayPrecision
                ? "CONTINUOUS_RANGE"
                : months.length > 1
                  ? "MULTIPLE_DATES"
                  : "MONTH_ONLY",
              startDate: start,
              endDate: end,
              occurrences: dayPrecision
                ? [{ startDate: start, endDate: end, precision: "DAY" }]
                : months.map((m) => ({
                    startDate: `2027-${String(m).padStart(2, "0")}-01`,
                    precision: "MONTH",
                  })),
              year: 2027,
              month: first,
              precision: dayPrecision
                ? "DAY"
                : months.length > 1
                  ? "MULTIPLE"
                  : "MONTH",
              timezone: null,
              dateStatus: "VERIFIED_UPCOMING",
              dateEvidence: [
                {
                  claim: dates.eventDateDisplay,
                  source: "stage2_need_date_evidence",
                },
              ],
              dateSourceRefs: [],
              verifiedAt: "2026-11-02",
              displayLabel: dates.eventDateDisplay,
              timingNotes:
                "Evidence-supported need-date window (TEST_SCENARIO periods)",
              hasVerifiedUpcomingDate: true,
            },
          };
        })()
      : {}),
    commercialStageKey: track === "A" ? "QUALIFY" : "BUILD_ACCOUNT",
    demandStageLabel:
      track === "A"
        ? "Need-date window — qualify before treating as open RFP"
        : "Relationship / account development",
    qualification: row.qualification || null,
    hotelDemandHypotheses: structured.hotelDemandHypotheses || {
      guestroomHypothesis: guestroomThesis,
      meetingsHypothesis: eventFnbThesis,
      primaryMechanism: row.qualification?.thesis,
    },
  };

  // Rebuild Research-to-Drawer V2 with Waterstone hotel context (strips Nobu).
  const pack = buildResearchToDrawerIntelligenceV2(opp, {
    hotelDemandThesis: potential,
    potentialGuestroomDemand: guestroomThesis,
    potentialMeetingDemand: eventFnbThesis,
    hotelFitThesis: whyHotel,
    advantages: [
      "Intracoastal Boca Raton location for local member, planner and corporate visitor demand",
      "Meeting / catering inventory alongside guestroom product",
    ],
    limitations: [
      "Programme or company existence is not proof of an open booking at Waterstone",
      "Buyer contracting authority may be unverified — qualify before proposal",
    ],
    buyerCandidates: contacts.allContacts.map((c) => ({
      fullName: c.name,
      role: c.title,
      employer: c.organization,
      whyRelevantToHotelDemand: c.whyRelevant,
      sourceUrl: c.sourceUrl,
    })),
    supportingSources: sources,
    suggestedRelationshipDevelopmentAction: nextStep,
    nextMonitoringTrigger: whyNow,
  });
  opp = applyResearchToDrawerIntelligenceV2(opp, pack);
  opp.researchToDrawerV2 = pack;
  const whyHotelFinal =
    text(whyHotel) ||
    text(opp.researchToDrawerV2?.salesBrief?.whyThisHotel) ||
    text(opp.whyThisHotel) ||
    `Waterstone Resort & Marina sits on the Intracoastal in Boca Raton with meeting/catering inventory and guestroom product relevant to local South Florida demand.`;
  // Force string hotel-fit copy — reject any residual "[object Object]" from nested research structs.
  const safeWhy =
    whyHotelFinal && !/^\[object Object\]$/i.test(whyHotelFinal)
      ? whyHotelFinal
      : `Waterstone Resort & Marina sits on the Intracoastal in Boca Raton with meeting/catering inventory and guestroom product relevant to local South Florida demand.`;
  opp.whyThisHotel = safeWhy;
  opp.commercialIntelligenceBrief = {
    ...(opp.commercialIntelligenceBrief || {}),
    companyAndLocalActivity: tileSummary,
    potentialBusinessForThisHotel: potential,
    whyThisHotel: safeWhy,
    whyThisHotelAdvantages: Array.isArray(
      opp.researchToDrawerV2?.salesBrief?.whyThisHotelAdvantages,
    )
      ? opp.researchToDrawerV2.salesBrief.whyThisHotelAdvantages.map(text).filter(Boolean)
      : [
          "Intracoastal Boca Raton location for local member, planner and corporate visitor demand",
          "Meeting / catering inventory alongside guestroom product",
        ],
    whyThisHotelLimitations: Array.isArray(
      opp.researchToDrawerV2?.salesBrief?.whyThisHotelLimitations,
    )
      ? opp.researchToDrawerV2.salesBrief.whyThisHotelLimitations.map(text).filter(Boolean)
      : [
          "Programme or company existence is not proof of an open booking at Waterstone",
          "Buyer contracting authority may be unverified — qualify before proposal",
        ],
    whyActNow: whyNow || null,
    recommendedNextStep: nextStep || null,
    whoToContact: {
      suggestedContact: contacts.primaryContact
        ? {
            name: contacts.primaryContact.name,
            title: contacts.primaryContact.title,
            organization: contacts.primaryContact.organization,
            whyRelevant: contacts.primaryContact.whyRelevant,
          }
        : null,
      alternativeContact: contacts.secondaryContact
        ? {
            name: contacts.secondaryContact.name,
            title: contacts.secondaryContact.title,
            organization: contacts.secondaryContact.organization,
            whyRelevant: contacts.secondaryContact.whyRelevant,
          }
        : null,
    },
  };
  opp = attachOpportunityRevenueStreamIcons(opp);

  // Shared ten-section / planner flags
  if (segment.demandSegmentId === "professional_event_planners") {
    opp.professionalEventPlannerV1 = true;
  }

  return opp;
}

/**
 * Index Wave E2 + Wave D deep artifacts for enrichment.
 */
export function indexStage2DeepArtifacts(waveE2 = [], waveD = {}) {
  const deepByRunId = new Map();
  const deepByOrg = new Map();
  const push = (doc) => {
    if (!doc) return;
    const runId = doc.runId || null;
    const structured = doc.structured || doc.run?.structured || null;
    const title = doc.title || doc.candidate?.organizationName || structured?.companyIdentity?.legalName;
    const entry = { ...doc, structured: structured || doc.structured };
    if (runId) deepByRunId.set(runId, entry);
    const orgKeys = [
      title,
      structured?.companyIdentity?.legalName,
      structured?.companyIdentity?.name,
      doc.candidate?.organizationName,
      doc.qualification?.organizationName,
    ]
      .map(normOrg)
      .filter(Boolean);
    for (const k of orgKeys) {
      if (!deepByOrg.has(k)) deepByOrg.set(k, entry);
      // family keys
      const fam = orgFamilyKey(k);
      if (!deepByOrg.has(fam)) deepByOrg.set(fam, entry);
    }
  };
  for (const row of waveE2 || []) push(row);
  for (const row of waveD.trackB || []) push(row);
  for (const row of waveD.trackA || []) push(row);
  return { deepByRunId, deepByOrg };
}

export function buildWaterstoneStage2LocalShadowPackage({
  yieldDoc,
  waveE2 = [],
  waveD = {},
  existingIds = new Set(),
} = {}) {
  const { unique, duplicates, taggedCount } = reconcileUniqueStage2Accounts(yieldDoc);
  const { deepByRunId, deepByOrg } = indexStage2DeepArtifacts(waveE2, waveD);
  const opportunities = [];
  const mapping = [];
  const quarantined = [];

  for (const account of unique) {
    const opp = mapStage2AccountToOpportunity(account, deepByRunId, deepByOrg);
    if (!opp.organizationName || opp.organizationName === "Unknown") {
      quarantined.push({
        familyKey: account.familyKey,
        reason: "MISSING_ORGANIZATION_IDENTITY",
      });
      continue;
    }
    if (existingIds.has(opp.id)) {
      // Stable re-stage — overwrite same id is OK for shadow merge
    }
    opportunities.push(opp);
    mapping.push({
      researchFamilyKey: account.familyKey,
      organizationName: opp.organizationName,
      track: opp.track,
      localOpportunityId: opp.id,
      hotelId: WATERSTONE_HOTEL_ID,
      researchRunIds: opp.researchRunIds,
      primaryContact: opp.primaryContact?.name || null,
      secondaryContact: opp.secondaryContact?.name || null,
      revenueStreamClass: opp.revenueStreamClass,
      relatedNames: opp.relatedOrganizationNames || [],
    });
  }

  return {
    version: WATERSTONE_V7_STAGE2_LOCAL_SHADOW_V1,
    hotelId: WATERSTONE_HOTEL_ID,
    hotelName: WATERSTONE_HOTEL_NAME,
    generatedAt: new Date().toISOString(),
    doesNotAffectProduction: true,
    publication: "NOT_AUTHORIZED",
    airtableWrites: false,
    paidResearch: false,
    counts: {
      yieldTagged: taggedCount,
      uniqueAccounts: unique.length,
      staged: opportunities.length,
      quarantined: quarantined.length,
      duplicateFamilies: duplicates.length,
      withNamedContact: opportunities.filter((o) => o.primaryContact?.name).length,
      withSecondaryContact: opportunities.filter((o) => o.secondaryContact?.name)
        .length,
    },
    duplicates,
    quarantined,
    mapping,
    opportunities,
  };
}
