/**
 * GDI customer-facing CSV export — full content parity with opportunity detail.
 *
 * Preserves the original 23 sales columns, then adds customer-visible
 * identification, intelligence, contact, action, evidence, hotel-fit, and
 * sales-feedback fields. No internal research budgets, raw model traces,
 * or administrative metadata.
 */

import {
  BOOKING_WINDOW_LABEL,
  DEMAND_TERRITORY_FIT_LABEL,
  OPPORTUNITY_QUALIFICATION_LABEL,
} from "./claim-types.js";
import { resolveCanonicalCustomerFacingTitle } from "./demand-segments/universal-opportunity-identity-v1.js";
import { projectOpportunityForPresentation } from "./presentation/gdi-universal-presentation-pipeline-v1.js";
import { GDI_PRESENTATION_SURFACES } from "./contracts/gdi-presentation-contract-v1.js";

export const GDI_CUSTOMER_CSV_V1 = "gdi_customer_csv_v1";
export const GDI_CUSTOMER_CSV_FULL_DETAIL_V1 =
  "gdi_customer_csv_full_detail_v1";

/** Original 23 customer columns (binding prefix — do not reorder). */
export const GDI_CUSTOMER_CSV_CORE_HEADERS = Object.freeze([
  "Dealality Opportunity ID",
  "Opportunity",
  "Organization",
  "Segment",
  "Priority",
  "Start Date",
  "End Date",
  "Qualification",
  "Booking Window",
  "Demand Territory",
  "Opportunity Summary",
  "Why Now",
  "Recommended Action",
  "Contact Name",
  "Contact Role",
  "Email",
  "Phone",
  "Phone Type",
  "Current Status",
  "Latest Action",
  "Latest Action Date",
  "Latest Outcome",
  "Latest Outcome Date",
]);

/** Additional full-detail columns (customer-visible only). */
export const GDI_CUSTOMER_CSV_DETAIL_HEADERS = Object.freeze([
  // Identification
  "Commercial Maturity",
  "Revenue Opportunity Type",
  "Demand Confidence",
  // Opportunity intelligence
  "Opportunity Overview",
  "What Is Happening",
  "Why It Is Happening",
  "Potential Guestroom Business",
  "Potential Catering Meeting Business",
  "Why This Hotel Fits",
  "Why Act Now Full",
  "Known Facts",
  "Uncertainties",
  "Hotel Sourcing Status",
  "Future Cycle Or Overflow",
  "Competitive Commercial Action",
  "Confirmed Competitor Business",
  "Potential Future Opportunity",
  "Opportunity Timing",
  // Contact intelligence
  "Contact Organization",
  "Contact Buying Role",
  "Why Contact Relevant",
  "Buyer Or Intermediary",
  "Organization Contact Route",
  "Contact Source Evidence",
  "Alternate Contacts",
  // Commercial action
  "Full Recommended Outreach",
  "Sales Qualification Questions",
  "Timing Recommendation",
  "Commercial Engagement Strategy",
  "Supplier Venue Constraints",
  "Opportunity Risks",
  // Evidence
  "Source URLs",
  "Supporting Evidence Summary",
  "Evidence Verification Date",
  "Inferred Assumptions",
  "Uncertainty Disclosures",
  // Hotel-specific
  "Hotel Fit Explanation",
  "Need Date Match",
  "Matching Need Period",
  "Revenue Priority Match",
  "Commercial Considerations",
  "Hotel Specific Limitations",
  // Sales feedback extras
  "Hotel Validation Status",
  "Hotel Validation Comments",
]);

/** Exact customer column order (binding). */
export const GDI_CUSTOMER_CSV_HEADERS = Object.freeze([
  ...GDI_CUSTOMER_CSV_CORE_HEADERS,
  ...GDI_CUSTOMER_CSV_DETAIL_HEADERS,
]);

const PRIORITY_LABEL = Object.freeze({
  HIGH_PRIORITY: "High Priority",
  MEDIUM_PRIORITY: "Medium Priority",
  WATCHLIST: "Watchlist",
  DISQUALIFIED: "Disqualified",
  HIGH: "High Priority",
  MEDIUM: "Medium Priority",
});

/** Fields that must never appear in customer CSV body or header. */
export const GDI_CSV_FORBIDDEN_TOKENS = Object.freeze([
  "hotelId",
  "runId",
  "schemaVersion",
  "_listDto",
  "hotelFitScore",
  "evidenceConfidence",
  '"ok"',
  "view:",
  "count:",
]);

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

function narrative(v) {
  // Preserve paragraphs / newlines for Excel cells; collapse only runs of spaces.
  return String(v == null ? "" : v)
    .replace(/\r\n/g, "\n")
    .replace(/[ \t]+\n/g, "\n")
    .replace(/\n{3,}/g, "\n\n")
    .replace(/[^\S\n]+/g, " ")
    .trim();
}

function preferLonger(...candidates) {
  let best = "";
  for (const c of candidates) {
    const t = narrative(c);
    if (t.length > best.length) best = t;
  }
  return best;
}

function joinLines(items, sep = "\n") {
  return (Array.isArray(items) ? items : [])
    .map((x) => {
      if (x == null) return "";
      if (typeof x === "string" || typeof x === "number") return narrative(x);
      if (typeof x === "object") {
        return narrative(
          x.text ||
            x.label ||
            x.body ||
            x.statement ||
            x.fact ||
            x.uncertainty ||
            x.question ||
            x.summary ||
            x.establishes ||
            ""
        );
      }
      return "";
    })
    .filter(Boolean)
    .join(sep);
}

function joinUrls(items) {
  const urls = [];
  const seen = new Set();
  for (const x of Array.isArray(items) ? items : []) {
    const u = text(
      typeof x === "string"
        ? x
        : x?.url || x?.sourceUrl || x?.href || x?.link || ""
    );
    if (!u || seen.has(u)) continue;
    if (!/^https?:\/\//i.test(u)) continue;
    seen.add(u);
    urls.push(u);
  }
  return urls.join("\n");
}

function humanizeEnum(value) {
  if (value == null || value === "") return "";
  return String(value)
    .replace(/_/g, " ")
    .toLowerCase()
    .replace(/\b\w/g, (c) => c.toUpperCase());
}

function toCustomerTitleCase(label) {
  const s = String(label || "").trim();
  if (!s) return "";
  if (/^[A-Z0-9][A-Z0-9\s/-]*$/.test(s) && /[A-Z]/.test(s)) {
    return s.toLowerCase().replace(/\b\w/g, (c) => c.toUpperCase());
  }
  return s;
}

function labelFrom(map, value, fallbackHumanize = true) {
  if (value == null || value === "") return "";
  const key = String(value);
  if (map && map[key]) return toCustomerTitleCase(map[key]);
  if (/[a-z]/.test(key) && /[A-Z]/.test(key)) return key;
  return fallbackHumanize ? humanizeEnum(key) : key;
}

/** ISO date YYYY-MM-DD for Excel sort; blank when unknown. Never invent. */
export function toCsvIsoDate(value) {
  if (value == null || value === "") return "";
  const s = String(value).trim();
  if (/^\d{4}-\d{2}-\d{2}/.test(s)) return s.slice(0, 10);
  const t = Date.parse(s);
  if (Number.isNaN(t)) return "";
  return new Date(t).toISOString().slice(0, 10);
}

/**
 * CSV cell escape + Excel formula-injection guard.
 * Leading = + - @ | tab CR → prefix apostrophe (Excel text).
 */
export function escapeCsvCell(value) {
  let s = value == null ? "" : String(value);
  if (/^[=+\-@|\t\r]/.test(s)) {
    s = `'${s}`;
  }
  if (/[",\n\r]/.test(s)) {
    return `"${s.replace(/"/g, '""')}"`;
  }
  return s;
}

export function rowsToCustomerCsv(rows = []) {
  const headers = GDI_CUSTOMER_CSV_HEADERS;
  const lines = [headers.join(",")];
  for (const row of rows) {
    lines.push(headers.map((h) => escapeCsvCell(row[h])).join(","));
  }
  return lines.join("\r\n") + "\r\n";
}

function drawerSections(opp = {}) {
  return (
    opp.sharedTenSectionDrawer?.sections ||
    opp.scalingDrawerModel?.sections ||
    opp.eventsDrawerModel?.sections ||
    opp.tenSectionDrawer?.sections ||
    {}
  );
}

function sectionBody(sections, id) {
  const s = sections?.[id];
  if (!s) return "";
  return preferLonger(
    s.body,
    s.overviewBody,
    Array.isArray(s.paragraphs) ? s.paragraphs.join("\n\n") : ""
  );
}

function formatAlternateContacts(opp = {}) {
  const alts = [];
  const secondary = opp.secondaryContact || opp.scalingTwoContactPresentation?.secondary;
  if (secondary?.name) {
    alts.push(
      [
        text(secondary.name),
        text(secondary.role || secondary.title),
        text(secondary.organization || secondary.company),
        text(secondary.email),
      ]
        .filter(Boolean)
        .join(" — ")
    );
  }
  for (const c of opp.backupContacts || opp.contacts || []) {
    const name = text(c?.name);
    if (!name) continue;
    if (text(opp.primaryContact?.name) && name === text(opp.primaryContact.name)) {
      continue;
    }
    alts.push(
      [name, text(c.role || c.title), text(c.organization), text(c.email)]
        .filter(Boolean)
        .join(" — ")
    );
  }
  return [...new Set(alts)].join("\n");
}

function buyerOrIntermediary(contact = {}) {
  const cls = text(
    contact.commercialClass ||
      contact.contactRelevanceLabel ||
      contact.contactClass ||
      ""
  ).toUpperCase();
  if (!cls) return "";
  if (/INTERMEDIARY|THIRD.?PARTY|PLANNER|DMC|AGENCY/.test(cls)) {
    return "Credible intermediary";
  }
  if (/BUYER|DECISION|AUTHORITY|DIRECT|ENTRY_POINT|RELEVANT_COMMERCIAL/.test(cls)) {
    return "Direct / relevant commercial entry point";
  }
  return humanizeEnum(cls);
}

function needDateMatchText(opp = {}) {
  const nd = opp.needDateMatchV1 || opp.needDateIndicator || opp.tileNeedDatePreview;
  if (!nd) return "";
  if (typeof nd === "string") return narrative(nd);
  return preferLonger(
    nd.label,
    nd.matchLabel,
    nd.summary,
    nd.displayLabel,
    nd.statusLabel,
    nd.relevanceClass ? humanizeEnum(nd.relevanceClass) : ""
  );
}

function matchingNeedPeriodText(opp = {}) {
  const nd = opp.needDateMatchV1 || {};
  const period =
    nd.matchingNeedPeriod ||
    nd.needPeriod ||
    nd.matchedPeriod ||
    opp.matchingNeedPeriod ||
    null;
  if (!period) {
    return preferLonger(nd.periodLabel, nd.needPeriodId, nd.matchedPeriodId);
  }
  if (typeof period === "string") return narrative(period);
  return preferLonger(
    period.periodLabel,
    period.label,
    period.needPeriodId,
    [period.startDate, period.endDate].filter(Boolean).join(" → ")
  );
}

/**
 * Merge presentation overlay without blanking filled source fields.
 * Drawer models / nested objects from presentation still win when present.
 */
function mergeExportEnrichment(base = {}, overlay = {}) {
  const out = { ...base };
  for (const [key, value] of Object.entries(overlay || {})) {
    if (value == null) continue;
    if (value === "" && base[key] != null && base[key] !== "") continue;
    if (
      Array.isArray(value) &&
      value.length === 0 &&
      Array.isArray(base[key]) &&
      base[key].length > 0
    ) {
      continue;
    }
    if (
      typeof value === "object" &&
      !Array.isArray(value) &&
      value &&
      typeof base[key] === "object" &&
      base[key] &&
      !Array.isArray(base[key])
    ) {
      out[key] = mergeExportEnrichment(base[key], value);
      continue;
    }
    out[key] = value;
  }
  return out;
}

/**
 * Enrich a list-row opportunity with the same detail presentation the drawer uses.
 * Pure / sync — no Airtable. Safe to batch across the export set.
 */
export function enrichOpportunityForCustomerExport(opportunity = {}, opts = {}) {
  const hotelId = opts.hotelId || opportunity.hotelId || null;
  const surface =
    opts.surface === GDI_PRESENTATION_SURFACES.EXTERNAL_SHARE
      ? GDI_PRESENTATION_SURFACES.EXTERNAL_SHARE
      : GDI_PRESENTATION_SURFACES.LOCAL_AUTH;
  try {
    const projected = projectOpportunityForPresentation(opportunity, {
      hotelId,
      surface,
      view: "detail",
      forceLocked: opts.forceLocked === true,
    });
    if (!projected?.dto) return { ...opportunity };
    const merged = mergeExportEnrichment(
      mergeExportEnrichment(opportunity, projected.prepared || {}),
      projected.dto || {}
    );
    return {
      ...merged,
      id: opportunity.id || projected.dto.id || merged.id,
      hotelId,
      commercialProgression:
        opportunity.commercialProgression ||
        projected.dto.commercialProgression ||
        merged.commercialProgression ||
        null,
      shareValidation:
        opportunity.shareValidation ||
        projected.dto.shareValidation ||
        merged.shareValidation ||
        null,
    };
  } catch {
    return { ...opportunity };
  }
}

/**
 * Flatten one opportunity into customer sales + full-detail columns.
 */
export function toCustomerExportRow(opp = {}) {
  const c = opp.primaryContact || {};
  const cp = opp.commercialProgression || {};
  const sv = opp.shareValidation || {};
  const sections = drawerSections(opp);
  const priorityRaw = opp.priority || "";
  const bookingRaw = opp.bookingWindowStatus || "";
  const qualRaw =
    opp.opportunityQualification || opp.opportunityQualificationLabel || "";
  const territoryRaw = opp.demandTerritoryFit || "";

  const canonicalTitle =
    resolveCanonicalCustomerFacingTitle(opp) ||
    opp.displayTitle ||
    opp.title ||
    "";

  const overview = preferLonger(
    sectionBody(sections, "opportunity_overview"),
    opp.tileCustomerSummary,
    opp.summaryWhat,
    opp.summaryWhyMatters
  );
  const whatHappening = preferLonger(
    sectionBody(sections, "what_is_happening"),
    opp.whatIsHappening,
    opp.summaryWhat,
    opp.salesBriefActivity
  );
  const whyHappening = preferLonger(
    opp.summaryWhyMatters,
    opp.demandMechanism?.summary,
    opp.whyItIsHappening
  );
  const potential = preferLonger(
    sectionBody(sections, "potential_business"),
    opp.potentialBusiness,
    opp.potentialHotelBusiness,
    opp.hotelOpportunityThesis
  );
  const guestroom = preferLonger(
    opp.guestroomThesis,
    opp.hotelDemandThesis,
    /guest|room|lodging|housing|overflow/i.test(potential) ? potential : ""
  );
  const catering = preferLonger(
    opp.eventFnbThesis,
    /cater|meeting|banquet|fnb|food/i.test(potential) ? potential : ""
  );
  const whyHotel = preferLonger(
    sectionBody(sections, "why_this_hotel"),
    opp.whyThisHotel,
    opp.hotelOpportunityThesis,
    opp.summaryWhyHotel,
    opp.fitExplanation,
    opp.fitRationale,
    opp.hotelFitRationale
  );
  const whyActFull = preferLonger(
    sectionBody(sections, "why_act_now"),
    opp.whyActNow,
    opp.whyNow,
    opp.summaryWhyNow,
    opp.cardWhyNowLine
  );
  const facts = preferLonger(
    joinLines(sections.facts_and_uncertainties?.facts),
    joinLines(opp.establishedFacts),
    joinLines(opp.facts)
  );
  const uncertainties = preferLonger(
    joinLines(sections.facts_and_uncertainties?.uncertainties),
    joinLines(opp.uncertainties),
    sections.facts_and_uncertainties?.uncEmptyNote
  );
  const nextStep = preferLonger(
    sectionBody(sections, "recommended_next_step"),
    opp.recommendedNextStep,
    opp.recommendedAction,
    opp.suggestedAction,
    opp.watchCardRecommendedNextStep
  );
  const questions = preferLonger(
    joinLines(sections.recommended_next_step?.qualificationQuestions),
    joinLines(opp.qualificationQuestions)
  );
  const sourcesSec = sections.supporting_evidence || {};
  const sourceUrls = preferLonger(
    joinUrls(sourcesSec.sourcesFull || sourcesSec.sources || []),
    joinUrls(opp.sources || []),
    text(opp.contactSourceUrl),
    text(c.sourceUrl)
  );
  const evidenceSummary = preferLonger(
    joinLines(
      (sourcesSec.sourcesFull || sourcesSec.sources || []).map((s) =>
        [text(s?.title || s?.label), text(s?.establishes || s?.summary)]
          .filter(Boolean)
          .join(": ")
      )
    ),
    joinLines(
      (opp.sources || []).map((s) =>
        typeof s === "string"
          ? s
          : [text(s?.title), text(s?.establishes || s?.summary)]
              .filter(Boolean)
              .join(": ")
      )
    )
  );
  const advantages = preferLonger(
    joinLines(sections.why_this_hotel?.advantages),
    joinLines(opp.hotelFitQualitativeComponents?.advantages)
  );
  const limitations = preferLonger(
    joinLines(sections.why_this_hotel?.limitations),
    joinLines(opp.hotelFitQualitativeComponents?.limitations),
    opp.hotelFitUnassessedCustomerNote
  );
  const overflow = preferLonger(
    opp.overflowPlausible === true
      ? ["Overflow / housing pathway plausible", joinLines(opp.overflowReasons)]
          .filter(Boolean)
          .join("\n")
      : joinLines(opp.overflowReasons),
    opp.futureCycleEvidenceStateLabel,
    humanizeEnum(opp.futureCycleEvidenceState),
    joinLines(opp.futureCycleSignals)
  );

  const orgEmail = text(
    opp.organizationContactEmail ||
      c.organizationContactRoute ||
      c.organizationEmail ||
      ""
  );
  const personEmail = text(c.email || opp.primaryContactEmail || "");
  // Prefer verified person email; keep org route separately.
  const emailForCore = personEmail;

  return {
    "Dealality Opportunity ID": opp.id || opp.opportunityId || "",
    Opportunity: canonicalTitle,
    Organization: opp.organizationName || "",
    Segment: opp.segment || opp.segmentDisplayLabel || "",
    Priority: opp.priorityLabel || labelFrom(PRIORITY_LABEL, priorityRaw),
    "Start Date": toCsvIsoDate(opp.eventStartDate),
    "End Date": toCsvIsoDate(opp.eventEndDate),
    Qualification:
      opp.opportunityQualificationLabel ||
      labelFrom(OPPORTUNITY_QUALIFICATION_LABEL, qualRaw),
    "Booking Window": toCustomerTitleCase(
      opp.bookingWindowLabel || labelFrom(BOOKING_WINDOW_LABEL, bookingRaw)
    ),
    "Demand Territory":
      opp.demandTerritoryFitLabel ||
      labelFrom(DEMAND_TERRITORY_FIT_LABEL, territoryRaw),
    "Opportunity Summary": preferLonger(
      opp.summaryWhat,
      opp.summaryWhyMatters,
      overview,
      whatHappening
    ),
    "Why Now": preferLonger(opp.whyNow, opp.summaryWhyNow, whyActFull),
    "Recommended Action": preferLonger(
      opp.recommendedAction,
      opp.recommendedNextStep,
      nextStep
    ),
    "Contact Name": c.name || opp.primaryContactName || "",
    "Contact Role": c.role || c.title || opp.primaryContactRole || "",
    Email: emailForCore,
    Phone: c.phone || opp.primaryContactPhone || "",
    "Phone Type":
      c.phoneTypeLabel ||
      c.phoneType ||
      opp.primaryContactPhoneTypeLabel ||
      "",
    "Current Status":
      cp.currentStatusLabel ||
      cp.compactStatusLabel ||
      cp.funnelStageLabel ||
      "",
    "Latest Action": cp.latestActionLabel || "",
    "Latest Action Date": toCsvIsoDate(cp.latestActionDate),
    "Latest Outcome": cp.latestOutcomeLabel || "",
    "Latest Outcome Date": toCsvIsoDate(cp.latestOutcomeDate),

    "Commercial Maturity": preferLonger(
      opp.gdiMaturityStateLabel,
      humanizeEnum(opp.gdiMaturityState),
      opp.commercialSurfaceLabel,
      humanizeEnum(opp.commercialInventoryClass || opp.commercialClassification)
    ),
    "Revenue Opportunity Type": preferLonger(
      opp.commercialDemandTypeLabel,
      humanizeEnum(opp.commercialDemandType),
      opp.revenueStreamLabel,
      humanizeEnum(opp.opportunityType)
    ),
    "Demand Confidence": preferLonger(
      opp.demandEvidenceLabel,
      humanizeEnum(opp.guestroomDemandConfidence),
      humanizeEnum(opp.eventFnbDemandConfidence),
      humanizeEnum(opp.roomDemandConfidence),
      humanizeEnum(opp.demandStatus)
    ),
    "Opportunity Overview": overview,
    "What Is Happening": whatHappening,
    "Why It Is Happening": whyHappening,
    "Potential Guestroom Business": guestroom,
    "Potential Catering Meeting Business": catering,
    "Why This Hotel Fits": whyHotel,
    "Why Act Now Full": whyActFull,
    "Known Facts": facts,
    Uncertainties: uncertainties,
    "Hotel Sourcing Status": preferLonger(
      opp.targetHotelSourcingStatusLabel,
      humanizeEnum(opp.targetHotelSourcingStatus),
      opp.venueSourcingStatusLabel,
      humanizeEnum(opp.venueSourcingStatus),
      opp.sourcingStatusLabel,
      humanizeEnum(opp.sourcingStatus),
      opp.alreadySourcedToHotelDisplay
    ),
    "Future Cycle Or Overflow": overflow,
    "Competitive Commercial Action": preferLonger(
      opp.competitiveCommercialActionLabel,
      humanizeEnum(opp.competitiveCommercialActionKey)
    ),
    "Confirmed Competitor Business": preferLonger(
      opp.competitiveFutureCyclePresentation?.confirmedCompetitorBusiness
        ?.headline,
      opp.cmiCompetitorHotel || opp.competitorHotel
        ? `${opp.title || opp.opportunityName || "Event"} — competitor: ${
            opp.cmiCompetitorHotel || opp.competitorHotel
          }`
        : ""
    ),
    "Potential Future Opportunity": preferLonger(
      opp.competitiveFutureCyclePresentation?.potentialFutureOpportunity
        ?.headline,
      humanizeEnum(opp.futureCycleOpportunityStatus)
    ),
    "Opportunity Timing": preferLonger(
      opp.eventDateDisplay,
      opp.timingLabel,
      opp.nextCycleDisplay,
      [toCsvIsoDate(opp.eventStartDate), toCsvIsoDate(opp.eventEndDate)]
        .filter(Boolean)
        .join(" → ")
    ),
    "Contact Organization": preferLonger(
      c.organization,
      c.company,
      opp.organizationName
    ),
    "Contact Buying Role": preferLonger(
      c.contactRelevanceLabel,
      c.commercialClass,
      c.role,
      c.title
    ),
    "Why Contact Relevant": preferLonger(
      c.whyThisContact,
      c.whyRelevant,
      opp.whyThisContact,
      sections.who_to_contact?.primaryContact?.whyRelevant
    ),
    "Buyer Or Intermediary": buyerOrIntermediary(c),
    "Organization Contact Route": orgEmail,
    "Contact Source Evidence": preferLonger(
      c.sourceUrl,
      opp.contactSourceUrl,
      opp.contactOfficialUrl
    ),
    "Alternate Contacts": formatAlternateContacts(opp),
    "Full Recommended Outreach": nextStep,
    "Sales Qualification Questions": questions,
    "Timing Recommendation": preferLonger(
      opp.salesActionPillLabel,
      opp.actionTimingFilterKey ? humanizeEnum(opp.actionTimingFilterKey) : "",
      opp.bookingWindowLabel,
      whyActFull
    ),
    "Commercial Engagement Strategy": preferLonger(
      opp.salesActionReason,
      opp.salesActionRecommended,
      opp.commercialMotionForWho,
      opp.whoPrimaryReason
    ),
    "Supplier Venue Constraints": preferLonger(
      opp.venueSourcingRationale,
      opp.competitors
        ? Array.isArray(opp.competitors)
          ? joinLines(opp.competitors.map((x) => x?.name || x))
          : narrative(opp.competitors)
        : "",
      opp.likelyCompetitor,
      opp.likelyGroupCompetitor
    ),
    "Opportunity Risks": preferLonger(
      uncertainties,
      opp.qualificationFailureReasonLabel,
      humanizeEnum(opp.qualificationFailureReason),
      opp.inventoryWhyNotSalesReady
    ),
    "Source URLs": sourceUrls,
    "Supporting Evidence Summary": evidenceSummary,
    "Evidence Verification Date": preferLonger(
      toCsvIsoDate(sourcesSec.researchVerificationDate),
      toCsvIsoDate(opp.evidenceFreshnessAt),
      toCsvIsoDate(opp.lastVerifiedAt),
      toCsvIsoDate(c.verifiedAt || opp.contactVerifiedAt)
    ),
    "Inferred Assumptions": preferLonger(
      sections.potential_business?.inferred
        ? narrative(sections.potential_business.inferred)
        : "",
      opp.modeledDemandDisclaimer,
      joinLines(opp.inferredAssumptions)
    ),
    "Uncertainty Disclosures": preferLonger(
      uncertainties,
      sections.facts_and_uncertainties?.uncEmptyNote,
      opp.hotelFitUnassessedCustomerNote
    ),
    "Hotel Fit Explanation": whyHotel,
    "Need Date Match": needDateMatchText(opp),
    "Matching Need Period": matchingNeedPeriodText(opp),
    "Revenue Priority Match": preferLonger(
      advantages,
      opp.applicableHotelRevenueStreamsLabel,
      joinLines(opp.applicableHotelRevenueStreams)
    ),
    "Commercial Considerations": preferLonger(
      opp.demandTerritoryRationale,
      opp.hotelFitPostureNote,
      opp.fitExplanation
    ),
    "Hotel Specific Limitations": limitations,
    "Hotel Validation Status": preferLonger(
      sv.familiarityStatus ? humanizeEnum(sv.familiarityStatus) : "",
      sv.commercialValue ? humanizeEnum(sv.commercialValue) : "",
      cp.hotelValidationStatusLabel,
      humanizeEnum(cp.hotelValidationStatus)
    ),
    "Hotel Validation Comments": preferLonger(
      sv.note,
      cp.hotelValidationComment,
      cp.validationComment,
      opp.hotelValidation?.note
    ),
  };
}

/**
 * Build UTF-8 BOM + CSV body for Excel.
 * Optionally enrich each opportunity via detail presentation (default true).
 */
export function buildCustomerExportCsv(opportunities = [], opts = {}) {
  const hotelId = opts.hotelId || null;
  const enrich = opts.enrich !== false;
  const surface = opts.surface || GDI_PRESENTATION_SURFACES.LOCAL_AUTH;
  const rows = (opportunities || []).map((o) =>
    toCustomerExportRow(
      enrich
        ? enrichOpportunityForCustomerExport(o, { hotelId, surface })
        : o
    )
  );
  const body = rowsToCustomerCsv(rows);
  return `\uFEFF${body}`;
}

/** Readable download filename — no record IDs. */
export function buildCustomerCsvFilename(hotelName = "Hotel") {
  const base = String(hotelName || "Hotel")
    .replace(/[<>:"/\\|?*\x00-\x1f]/g, "")
    .replace(/\s+/g, " ")
    .trim()
    .slice(0, 80);
  const safe = base || "Hotel";
  return `${safe} - GDI Opportunities.csv`;
}

export function customerCsvContentDisposition(hotelName) {
  const filename = buildCustomerCsvFilename(hotelName);
  const ascii = filename.replace(/[^\x20-\x7E]/g, "_");
  const encoded = encodeURIComponent(filename);
  return `attachment; filename="${ascii}"; filename*=UTF-8''${encoded}`;
}

export function assertCustomerCsvClean(csvText = "") {
  const textIn = String(csvText || "");
  const hits = [];
  if (textIn.trimStart().startsWith("{")) {
    hits.push("json_wrapper");
  }
  for (const tok of GDI_CSV_FORBIDDEN_TOKENS) {
    if (textIn.includes(tok)) hits.push(tok);
  }
  if (/primaryContact\s*:/.test(textIn) || /commercialProgression\s*:/.test(textIn)) {
    hits.push("nested_object");
  }
  return { ok: hits.length === 0, hits };
}

/**
 * Field-level parity: customer-visible detail narratives vs export cells.
 * Empty source → not_applicable (not a failure).
 */
export function compareDetailToExportRow(detailOpp = {}, exportRow = {}) {
  const enriched = enrichOpportunityForCustomerExport(detailOpp, {
    hotelId: detailOpp.hotelId,
  });
  const sections = drawerSections(enriched);
  const checks = [
    {
      field: "What Is Happening",
      source: preferLonger(
        sectionBody(sections, "what_is_happening"),
        enriched.whatIsHappening,
        enriched.summaryWhat
      ),
      export: exportRow["What Is Happening"] || exportRow["Opportunity Summary"],
    },
    {
      field: "Why This Hotel Fits",
      source: preferLonger(
        sectionBody(sections, "why_this_hotel"),
        enriched.whyThisHotel,
        enriched.hotelOpportunityThesis,
        enriched.fitExplanation
      ),
      export: exportRow["Why This Hotel Fits"] || exportRow["Hotel Fit Explanation"],
    },
    {
      field: "Why Act Now",
      source: preferLonger(
        sectionBody(sections, "why_act_now"),
        enriched.whyActNow,
        enriched.whyNow
      ),
      export: exportRow["Why Act Now Full"] || exportRow["Why Now"],
    },
    {
      field: "Recommended Next Step",
      source: preferLonger(
        sectionBody(sections, "recommended_next_step"),
        enriched.recommendedNextStep,
        enriched.recommendedAction
      ),
      export:
        exportRow["Full Recommended Outreach"] ||
        exportRow["Recommended Action"],
    },
    {
      field: "Potential Business",
      source: preferLonger(
        sectionBody(sections, "potential_business"),
        enriched.potentialBusiness,
        enriched.guestroomThesis
      ),
      export: preferLonger(
        exportRow["Potential Guestroom Business"],
        exportRow["Potential Catering Meeting Business"]
      ),
    },
    {
      field: "Contact Name",
      source: text(enriched.primaryContact?.name || enriched.primaryContactName),
      export: exportRow["Contact Name"],
    },
    {
      field: "Source URLs",
      source: joinUrls(
        sections.supporting_evidence?.sourcesFull ||
          sections.supporting_evidence?.sources ||
          enriched.sources ||
          []
      ),
      export: exportRow["Source URLs"],
    },
  ];

  return checks.map((c) => {
    const src = narrative(c.source);
    const exp = narrative(c.export);
    let classification = "present_and_complete";
    if (!src) classification = "source_unavailable";
    else if (!exp) classification = "present_in_source_omitted_by_export";
    else if (exp.length + 20 < src.length && !src.includes(exp) && !exp.includes(src.slice(0, Math.min(80, src.length)))) {
      classification = "present_in_export_but_truncated";
    } else if (exp.length + 40 < src.length) {
      classification = "present_in_export_but_truncated";
    }
    return {
      field: c.field,
      sourceLength: src.length,
      exportLength: exp.length,
      classification,
    };
  });
}
