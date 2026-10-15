/**
 * Idempotent importer for reviewed hotel-facing prospect packages.
 * Uses canonical GDI repository (GDI_DATA_ROOT aware). No hotel-specific
 * account hardcoding — match keys derived from package orgKey + cardId.
 */

import fs from "node:fs";
import path from "node:path";
import crypto from "node:crypto";
import { fileURLToPath } from "node:url";
import {
  applyContactPackageToOpportunity,
  evaluateProspectPublicationEligibility,
  classifyContactCompletenessGroup,
} from "./prospect-publication-eligibility-v1.js";
import {
  REVIEWED_PROSPECT_PUBLICATION_BASIS,
  REVIEWED_PROSPECT_PACKAGE_VERSION,
  REVIEWED_PROSPECT_HOTEL_IDS,
  customerFacingLabelForProspect,
  contactResponsibilityLabel,
  phoneTypeCustomerLabel,
} from "./reviewed-prospect-publication-v1.js";
import { PRIORITY, BOOKING_WINDOW } from "./claim-types.js";
import {
  loadOpportunities,
  saveOpportunities,
  loadHotelProfile,
  saveHotelProfile,
  hotelDir,
  getGdiDataRoot,
  clearGdiRepositoryReadCache,
  createId,
} from "./repository.js";
import {
  getGdiOpportunityPersistenceMode,
  isGdiOpportunityAirtableConfigured,
  loadOpportunitiesCanonical,
  saveOpportunitiesCanonical,
} from "./opportunity-persistence.js";
import { invalidateGdiHotelReadCache } from "./read-cache.js";
import {
  isGenericThesis,
  evaluateDrawerContentCompleteness,
} from "./workflow/drawer-content-contract-v1.js";
import { stripClaimLabel, toCustomerProse } from "./workflow/hotel-relevance-research-v1.js";
import { applyPriorityActionAssignment } from "./priority-action-window/assignment-contract-v1.js";
import { applyHotelFitAssessmentToOpportunity } from "./workflow/hotel-fit-assessment-v1.js";

function sanitizePeakRoomsValue(raw) {
  if (raw == null || raw === "") return null;
  const s = String(raw).trim();
  if (!s || /^UNKNOWN$/i.test(s)) return null;
  if (/housing\s*program|verified housing|room demand/i.test(s) && !/\d/.test(s)) {
    return null;
  }
  if (!/\d/.test(s)) return null;
  return s;
}

const PROTECTED_USER_FIELDS = [
  "salesOwner",
  "owner",
  "ownerUserId",
  "salesWorkflowState",
  "pursuitId",
  "pursuitStatus",
  "notes",
  "hotelNotes",
  "salespersonNotes",
  "feedback",
  "legacyVisibilityPreserved",
  "customerVisible",
  "decisionId",
  "validationFeedback",
  "hotelValidation",
  "commercialProgression",
];

/** Content fields that must not be overwritten by weaker generic defaults. */
const RICH_CONTENT_FIELDS = [
  "hotelOpportunityThesis",
  "summaryWhyHotel",
  "fitExplanation",
  "fitRationale",
  "hotelFitRationale",
  "recommendedAction",
  "recommendedNextStep",
  "whyNow",
  "summaryWhat",
  "travelingCohort",
  "relevantTravelers",
  "opportunityType",
  "destinationStatus",
  "eventCity",
  "venueCity",
  "city",
];

/** Preferred live Bethesda opportunity IDs for known review cardIds (stable match). */
const KNOWN_ID_MATCHES = Object.freeze({
  "beth-ndss-2027": "gdi_opp_ndss_advocacy_2027",
  "beth-ahima-advocacy-2027": "gdi_opp_ahima_advocacy_2027",
  "beth-acc-leg-2027": "gdi_opp_acc_legislative_2027",
  "beth-acts-ts27": "gdi_opp_acts_ts27",
  "beth-bebpa-usb-2027": "gdi_opp_bebpa_usb_2027",
  "beth-amwa-annual-2027": "gdi_opp_amwa_2027_annual",
  "beth-nar-gad-2027": "gdi_opp_nar_gad_institute_2027",
  "beth-nado-2028": "gdi_opp_nado_ddaa_washcon_2028",
  "beth-nado-2027-overflow": "gdi_opp_nado_ddaa_washcon_2027_overflow",
  "beth-loudoun-showcase-2027": "gdi_opp_loudoun_college_showcase_2027",
  "beth-nci-rna-2027": "gdi_opp_2027_nci_rna_biology_symposium_7",
  "beth-nih-hrhr-2027": "gdi_opp_series_high_risk_high_reward_research_symposium_2027",
  "beth-ctn-2027": "gdi_opp_series_ctn_annual_conference_2027",
  "beth-afcea-hits-2027": "gdi_opp_afcea_hits_2027",
});

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

function sha256File(filePath) {
  const buf = fs.readFileSync(filePath);
  return crypto.createHash("sha256").update(buf).digest("hex");
}

function parseCsv(textIn) {
  const lines = textIn.trim().split(/\r?\n/);
  const headers = splitCsvLine(lines[0]);
  return lines.slice(1).filter(Boolean).map((line) => {
    const cols = splitCsvLine(line);
    const row = {};
    headers.forEach((h, i) => {
      row[h] = cols[i] ?? "";
    });
    return row;
  });
}

function splitCsvLine(line) {
  const cols = [];
  let cur = "";
  let q = false;
  for (const ch of line) {
    if (ch === '"') q = !q;
    else if (ch === "," && !q) {
      cols.push(cur);
      cur = "";
    } else cur += ch;
  }
  cols.push(cur);
  return cols;
}

function parseRegisterSources(sourcesCell = "", verificationDate = null) {
  const raw = text(sourcesCell);
  if (!raw) return [];
  const parts = raw
    .split(/;|\n/)
    .map((s) => s.trim())
    .filter(Boolean);
  const out = [];
  for (const part of parts) {
    const urlMatch = part.match(/https?:\/\/[^\s)]+/i);
    if (!urlMatch) continue;
    const url = urlMatch[0].replace(/[.,;]+$/, "");
    const retrieved = (part.match(/retrieved\s+(\d{4}-\d{2}-\d{2})/i) || [])[1];
    out.push({
      url,
      verificationDate: retrieved || verificationDate || null,
      sourceType: "official_web",
      claimKind: "FACT",
      fromRegister: true,
    });
  }
  return out;
}

function parseLocationFromText(...parts) {
  const t = text(parts.filter(Boolean).join(" | "));
  if (/arlington/i.test(t)) {
    return {
      eventCity: "Arlington",
      destinationStatus: "Arlington, Virginia",
      venueCity: "Arlington",
    };
  }
  if (/alexandria/i.test(t)) {
    return {
      eventCity: "Alexandria",
      destinationStatus: "Alexandria, Virginia",
      venueCity: "Alexandria",
    };
  }
  if (/frederick/i.test(t)) {
    return {
      eventCity: "Frederick",
      destinationStatus: "Frederick, Maryland",
      venueCity: "Frederick",
    };
  }
  if (/college park/i.test(t)) {
    return {
      eventCity: "College Park",
      destinationStatus: "College Park, Maryland",
      venueCity: "College Park",
    };
  }
  if (/bethesda/i.test(t)) {
    return {
      eventCity: "Bethesda",
      destinationStatus: "Bethesda, Maryland",
      venueCity: "Bethesda",
    };
  }
  if (
    /washington,\s*dc|washington\s*dc|district of columbia|\bwashington\b.*\bdc\b|\bdc\b.*advocacy|\bcapitol hill\b/i.test(
      t
    )
  ) {
    return {
      eventCity: "Washington",
      destinationStatus: "Washington, District of Columbia",
      venueCity: "Washington",
    };
  }
  return {};
}

function parseLocationFromTiming(timing = "") {
  return parseLocationFromText(timing);
}

function preferRicher(existingVal, incomingVal) {
  const ex = text(existingVal);
  const inc = text(incomingVal);
  if (!inc) return existingVal ?? null;
  if (!ex) return incomingVal;
  if (isGenericThesis(inc) && !isGenericThesis(ex)) return existingVal;
  if (isGenericThesis(ex) && !isGenericThesis(inc)) return incomingVal;
  if (inc.length > ex.length + 20) return incomingVal;
  return existingVal;
}

/**
 * Editorial overlay refresh: when forceEditorial is set, substantive overlay
 * prose replaces thinner stored copy so drawer repairs persist through reimport.
 */
function preferEditorial(existingVal, incomingVal, forceEditorial = false) {
  const inc = text(incomingVal);
  if (!inc) return existingVal ?? null;
  if (forceEditorial && !isGenericThesis(inc) && inc.length >= 40) {
    return incomingVal;
  }
  return preferRicher(existingVal, incomingVal);
}

const CUSTOMER_SAFE_FIT_SCORE_NOTE =
  "Qualitative hotel-fit assessment — no numeric score on this reviewed prospect.";

/** Concise account + occasion titles — research-note titles are not customer-ready. */
const CARD_TITLE_BY_ID = Object.freeze({
  "may-ndss-2027": "NDSS Advocacy Conference 2027",
  "may-ahima-2027": "AHIMA Advocacy Summit 2027",
  "may-acc-leg-2027": "ACC Legislative Conference 2027",
  "may-amwa-2027": "AMWA Annual Meeting 2027",
  "may-acts-ts27": "ACTS Translational Science 2027",
  "may-nar-gad-2027": "NAR GAD Institute 2027",
  "may-nado-2028": "NADO / DDAA Washington Conference 2028",
  "may-nado-2027": "NADO Arlington Washington Conference 2027",
  "may-aad-2028": "AAD Annual Meeting 2028",
  "may-nchv-2027": "NCHV Annual Conference 2027",
  "may-uscm": "U.S. Conference of Mayors — next DC cycle",
  "may-ustravel-dch": "U.S. Travel Association Destination Capitol Hill",
  "may-hcaoa-2027": "HCAOA Annual Meeting 2027",
  "may-cmss-2027": "CMSS Annual Meeting 2027",
  "may-arts-summit": "Arts Advocacy Summit — next DC cycle",
  "beth-ahima-advocacy-2027": "AHIMA Advocacy Summit 2027",
  "beth-ndss-2027": "NDSS Advocacy Conference 2027",
  "beth-acc-leg-2027": "ACC Legislative Conference 2027",
  "beth-amwa-annual-2027": "AMWA Annual Meeting 2027",
  "beth-acts-ts27": "ACTS Translational Science 2027",
  "beth-nar-gad-2027": "NAR GAD Institute 2027",
  "beth-nado-2028": "NADO Arlington Washington Conference 2028",
  "beth-nado-2027-overflow": "NADO Arlington Washington Conference 2027 (overflow)",
  "beth-ctn-2027": "CTN / clinical trials network lodging 2027",
  "beth-nci-rna-2027": "NCI RNA biology meeting 2027",
  "beth-loudoun-showcase-2027": "Loudoun showcase / corridor demand 2027",
  "beth-bebpa-usb-2027": "BEBPA USB meeting 2027",
  "beth-nih-hrhr-2027": "NIH HRHR program lodging 2027",
  "beth-afcea-hits-2027": "AFCEA HITS 2027",
});

function isResearchNoteTitle(title) {
  const t = text(title);
  if (!t) return true;
  if (
    /verified via|verified:|hotel not named|hotel block tba|not verified|unconfirmed this pass|recurring program pattern claimed|exact next dates|exact 2027 dates not locked/i.test(
      t
    )
  ) {
    return true;
  }
  if (t.length > 90 && /—/.test(t)) return true;
  return false;
}

function resolveConciseProspectTitle({
  overlayTitle,
  account,
  existingTitle,
  timing,
  section,
  cardId,
} = {}) {
  const fromOverlay = text(overlayTitle);
  if (fromOverlay && !isResearchNoteTitle(fromOverlay)) return fromOverlay.slice(0, 100);

  const fromCard = CARD_TITLE_BY_ID[text(cardId)];
  if (fromCard) return fromCard;

  const existing = text(existingTitle);
  if (existing && !isResearchNoteTitle(existing)) return existing.slice(0, 100);

  const acct = text(account);
  const year =
    (text(timing).match(/\b(202[6-9]|2030)\b/) ||
      existing.match(/\b(202[6-9]|2030)\b/) ||
      [])[0] || "";
  if (existing && /—/.test(existing)) {
    const left = existing.split("—")[0].trim();
    if (left && left.length <= 90) return left;
  }
  if (acct && year) {
    return `${acct.replace(/\s*\([^)]*\)\s*$/, "").trim().slice(0, 48)} ${year}`.slice(
      0,
      100
    );
  }
  if (acct) return `${acct.slice(0, 70)} — ${text(section) || "prospect"}`.slice(0, 100);
  return existing.slice(0, 100) || "Prospect";
}

function parseEventDates(timing = "") {
  const t = text(timing);
  // ISO range: 2027-04-02 → 2027-04-05
  const isoRange = t.match(
    /(20\d{2}-\d{2}-\d{2})\s*[→\-–—]\s*(20\d{2}-\d{2}-\d{2})/
  );
  if (isoRange) {
    return { eventStartDate: isoRange[1], eventEndDate: isoRange[2] };
  }
  const isoSingle = t.match(/\b(20\d{2}-\d{2}-\d{2})\b/);
  if (isoSingle && !/[A-Za-z]{3,}/.test(t.replace(isoSingle[0], ""))) {
    return { eventStartDate: isoSingle[1], eventEndDate: isoSingle[1] };
  }
  const range = t.match(
    /(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{1,2})\s*[–-]\s*(\d{1,2}),?\s*(20\d{2})/i
  );
  const months = {
    january: "01",
    february: "02",
    march: "03",
    april: "04",
    may: "05",
    june: "06",
    july: "07",
    august: "08",
    september: "09",
    october: "10",
    november: "11",
    december: "12",
  };
  if (range) {
    const mo = months[range[1].toLowerCase()];
    return {
      eventStartDate: `${range[4]}-${mo}-${String(range[2]).padStart(2, "0")}`,
      eventEndDate: `${range[4]}-${mo}-${String(range[3]).padStart(2, "0")}`,
    };
  }
  const single = t.match(
    /(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{1,2}),?\s*(20\d{2})/i
  );
  if (single) {
    const mo = months[single[1].toLowerCase()];
    const d = `${single[3]}-${mo}-${String(single[2]).padStart(2, "0")}`;
    return { eventStartDate: d, eventEndDate: d };
  }
  const monthYear = t.match(
    /(January|February|March|April|May|June|July|August|September|October|November|December)\s+(20\d{2})/i
  );
  if (monthYear) {
    const mo = months[monthYear[1].toLowerCase()];
    const d = `${monthYear[2]}-${mo}-01`;
    return { eventStartDate: d, eventEndDate: d, granularity: "month" };
  }
  const y = t.match(/20\d{2}/);
  return { eventStartDate: null, eventEndDate: null, yearHint: y ? y[0] : null };
}

function normalizePhoneType(pkg = {}) {
  let phoneType = text(pkg.phoneType);
  const note = text(pkg.phoneNote || "");
  if (/helpline/i.test(note) || /helpline/i.test(phoneType)) {
    phoneType = "organization_helpline";
  }
  return phoneType || null;
}

function normalizeCompletenessGroup(pkg = {}) {
  const stamped = applyContactPackageToOpportunity({}, { ...pkg, phoneType: normalizePhoneType(pkg) });
  const computed = classifyContactCompletenessGroup(stamped);
  // CMSS: person + email + HQ phone → group 1 when person established
  if (
    pkg.cardId === "may-cmss-2027" &&
    text(pkg.person) &&
    !/^not\s*established$/i.test(pkg.person) &&
    text(pkg.personEmail || pkg.contactMethod) &&
    text(pkg.businessPhone) &&
    !/^not\s*established$/i.test(pkg.businessPhone)
  ) {
    return 1;
  }
  return computed;
}

function matchIdentityKey(row = {}, pkg = {}) {
  const orgKey = text(pkg.orgKey || row.orgKey);
  const cardId = text(row.cardId || pkg.cardId);
  return `${orgKey}::${cardId}`;
}

function findExisting(
  opportunities,
  row,
  pkg,
  { claimedIds = new Set(), allowOrgHeuristic = false } = {}
) {
  const available = (opportunities || []).filter((o) => !claimedIds.has(o.id));

  const known = KNOWN_ID_MATCHES[row.cardId];
  if (known) {
    const byKnown = available.find((o) => o.id === known);
    if (byKnown) return { opp: byKnown, matchType: "known_id" };
  }
  const byCard = available.find(
    (o) =>
      o.prospectReviewCardId === row.cardId ||
      o.reviewedProspectCardId === row.cardId ||
      o.id === row.cardId
  );
  if (byCard) return { opp: byCard, matchType: "card_id" };

  const identity = matchIdentityKey(row, pkg);
  const byIdentity = available.find((o) => o.prospectIdentityKey === identity);
  if (byIdentity) return { opp: byIdentity, matchType: "identity_key" };

  // Default: create. Org heuristics are opt-in only — Mayflower already has a
  // distinct research corpus that must not be overwritten by guessing.
  if (!allowOrgHeuristic) {
    return { opp: null, matchType: "create" };
  }

  const org = text(row.account || pkg.company).toLowerCase();
  const orgKey = text(pkg.orgKey || row.orgKey).toLowerCase();
  const year = (row.timing || "").match(/20\d{2}/)?.[0];
  const stop = new Set([
    "national",
    "association",
    "american",
    "america",
    "society",
    "conference",
    "hotel",
    "the",
    "and",
    "for",
    "of",
    "inc",
    "development",
    "organizations",
  ]);
  const orgTokens = org
    .split(/[^a-z0-9]+/)
    .filter((t) => t.length >= 6 && !stop.has(t));
  if (org || orgKey) {
    const candidates = available.filter((o) => {
      if (o.prospectReviewCardId && o.prospectReviewCardId !== row.cardId) return false;
      if (o.prospectIdentityKey && o.prospectIdentityKey !== identity) return false;
      const name = String(o.organizationName || "").toLowerCase();
      if (!name) return false;
      if (orgKey) {
        const keyBits = orgKey
          .replace(/-/g, " ")
          .split(/\s+/)
          .filter((t) => t.length >= 5 && !stop.has(t));
        if (keyBits.length && keyBits.every((t) => name.includes(t))) return true;
      }
      return orgTokens.some((t) => name.includes(t));
    });
    if (year) {
      const yHits = candidates.filter(
        (o) =>
          String(o.title || "").includes(year) ||
          String(o.eventStartDate || "").startsWith(year)
      );
      if (yHits.length === 1) return { opp: yHits[0], matchType: "org_year_heuristic" };
      if (yHits.length > 1) {
        return {
          opp: null,
          matchType: "ambiguous_org_year",
          ambiguousIds: yHits.map((o) => o.id),
        };
      }
    }
    if (candidates.length === 1) {
      return { opp: candidates[0], matchType: "org_unique_deterministic" };
    }
    if (candidates.length > 1) {
      return {
        opp: null,
        matchType: "ambiguous_org",
        ambiguousIds: candidates.map((o) => o.id),
      };
    }
  }
  return { opp: null, matchType: "create" };
}

function buildOpportunityFromPackage({
  row,
  pkg,
  existing = null,
  hotelId,
  contentOverlay = null,
}) {
  const dates = parseEventDates(row.timing || "");
  const overlay = contentOverlay || {};
  const forceEditorial = overlay.forceEditorial === true;
  const locFromTiming = parseLocationFromText(
    row.timing,
    overlay.travelers,
    overlay.whatWeKnow,
    overlay.whyFits,
    overlay.timing,
    overlay.destinationStatus,
    row.account
  );
  const phoneType = normalizePhoneType(pkg || {});
  const group = pkg ? normalizeCompletenessGroup({ ...pkg, phoneType }) : 4;
  const section = text(
    overlay.section || row.section || existing?.prospectReviewSection || "PRIORITY"
  ).toUpperCase();
  const commercialLabel =
    section === "EXPLORATORY" ? "Exploratory account" : "Prospect to qualify";

  let base = existing
    ? { ...existing }
    : {
        id: createId("gdi_opp_prospect"),
        hotelId,
        createdAt: new Date().toISOString(),
      };

  // Preserve user/workflow fields
  const preserved = {};
  for (const k of PROTECTED_USER_FIELDS) {
    if (base[k] !== undefined) preserved[k] = base[k];
  }

  const title = resolveConciseProspectTitle({
    overlayTitle: overlay.title,
    account: row.account || pkg?.company,
    existingTitle: existing?.title,
    timing: row.timing || overlay.timing,
    section,
    cardId: row.cardId,
  });

  // commercialStatus is a browse label — NEVER use it as hotel-specific thesis.
  const thesisCandidate = toCustomerProse(
    text(overlay.whyFits) ||
      text(overlay.hotelOpportunityThesis) ||
      text(base.hotelOpportunityThesis) ||
      text(base.summaryWhyHotel) ||
      text(base.fitExplanation) ||
      ""
  );
  const thesis =
    thesisCandidate && !isGenericThesis(thesisCandidate)
      ? thesisCandidate
      : null;
  const fitAssessmentObj =
    overlay.hotelFitAssessment && typeof overlay.hotelFitAssessment === "object"
      ? overlay.hotelFitAssessment
      : base.hotelFitAssessment && typeof base.hotelFitAssessment === "object"
        ? base.hotelFitAssessment
        : null;
  const fitAssessment =
    (fitAssessmentObj && text(fitAssessmentObj.summary)) ||
    toCustomerProse(text(overlay.fitExplanation) || "") ||
    thesis;
  const whatWeKnow = toCustomerProse(
    text(overlay.whatWeKnow) || text(overlay.summaryWhat) || ""
  ) || null;
  const whyMatters = toCustomerProse(
    text(overlay.whyMatters) || text(overlay.summaryWhyMatters) || ""
  ) || null;
  const whyNowEditorial = toCustomerProse(
    text(overlay.whyNow) || text(overlay.summaryWhyNow) || ""
  ) || null;
  const limitations = toCustomerProse(
    text(overlay.limitations) || text(overlay.competitiveContextNote) || ""
  ) || null;
  const whyHotelDemandEditorial = toCustomerProse(
    text(overlay.whyHotelDemand) || ""
  ) || null;

  const nextActionCandidate =
    text(overlay.nextAction) ||
    text(base.recommendedAction) ||
    text(base.recommendedNextStep) ||
    null;
  const defaultAction =
    section === "EXPLORATORY"
      ? "Qualify whether overflow or secondary lodging is needed before pursuit."
      : "Ask whether hotel selection / lodging plans are open for this occasion, and what peak-room pattern to check.";

  const registerSources = parseRegisterSources(
    row.sources,
    text(pkg?.verificationDate) || null
  );
  const overlaySources = (Array.isArray(overlay.sourceUrls) ? overlay.sourceUrls : [])
    .filter(Boolean)
    .map((url) => ({
      url: text(url),
      verificationDate: text(pkg?.verificationDate) || null,
      sourceType: "official_web",
      claimKind: "FACT",
      fromSalesReviewOverlay: true,
    }));
  const contactSources = [
    ...(text(pkg?.sourceUrl)
      ? [
          {
            url: pkg.sourceUrl,
            verificationDate: pkg.verificationDate || null,
            supportsFact: "contact",
            claimKind: "FACT",
          },
        ]
      : []),
    ...(text(pkg?.sourceUrlSecondary)
      ? [
          {
            url: pkg.sourceUrlSecondary,
            verificationDate: pkg.verificationDate || null,
            supportsFact: "contact_or_context",
            claimKind: "FACT",
          },
        ]
      : []),
  ];
  const mergedSources = [
    ...(Array.isArray(base.sources) ? base.sources : []),
    ...registerSources,
    ...overlaySources,
    ...contactSources,
  ].filter((s, i, arr) => s?.url && arr.findIndex((x) => x.url === s.url) === i);

  const officialFromRegister =
    registerSources.find((s) => s.url && !/^https?:\/\/[^/]+\/?$/i.test(s.url))
      ?.url ||
    overlaySources[0]?.url ||
    registerSources[0]?.url ||
    null;

  let next = {
    ...base,
    hotelId,
    title,
    organizationName: text(row.account || pkg?.company || base.organizationName),
    company: text(row.account || pkg?.company || base.company),
    segment: base.segment || "Association",
    participationRole: base.participationRole || "ORGANIZER",
    prospectReviewCardId: row.cardId,
    prospectReviewSection: section,
    prospectOrgKey: text(pkg?.orgKey || row.orgKey),
    prospectIdentityKey: matchIdentityKey(row, pkg || {}),
    prospectPackageVersion: REVIEWED_PROSPECT_PACKAGE_VERSION,
    prospectPublicationBasis: REVIEWED_PROSPECT_PUBLICATION_BASIS,
    prospectCommercialLabel: commercialLabel,
    opportunityQualificationLabel: commercialLabel,
    venueSourcingStatus: base.venueSourcingStatus || "UNKNOWN",
    venueSourcingStatusLabel:
      base.venueSourcingStatusLabel || "Hotel sourcing unconfirmed",
    roomDemandStatus: base.roomDemandStatus || "UNKNOWN",
    roomDemandStatusNote:
      base.roomDemandStatusNote ||
      "Not established — peak rooms / attendance not published for this occasion.",
    confirmedBuying: false,
    commercialStatus: text(row.commercialStatus) || commercialLabel,
    timingNote: text(row.timing),
    eventStartDate: dates.eventStartDate || base.eventStartDate || null,
    eventEndDate: dates.eventEndDate || base.eventEndDate || null,
    eventCity:
      overlay.eventCity ||
      locFromTiming.eventCity ||
      base.eventCity ||
      null,
    venueCity:
      overlay.venueCity ||
      locFromTiming.venueCity ||
      base.venueCity ||
      null,
    destinationStatus:
      preferEditorial(
        base.destinationStatus,
        overlay.destinationStatus || locFromTiming.destinationStatus,
        forceEditorial
      ) ||
      overlay.destinationStatus ||
      locFromTiming.destinationStatus ||
      base.destinationStatus ||
      null,
    eventLocationSummary:
      preferEditorial(
        base.eventLocationSummary,
        overlay.eventLocationSummary || overlay.destinationStatus,
        forceEditorial
      ) || base.eventLocationSummary,
    city: overlay.city || locFromTiming.eventCity || base.city || null,
    travelingCohort:
      preferEditorial(base.travelingCohort, overlay.travelers, forceEditorial) ||
      base.travelingCohort ||
      null,
    relevantTravelers:
      preferEditorial(base.relevantTravelers, overlay.travelers, forceEditorial) ||
      base.relevantTravelers ||
      null,
    futureCycleEvidenceState:
      overlay.futureCycleEvidenceState ||
      base.futureCycleEvidenceState ||
      "FUTURE_CYCLE_EXPECTED",
    futureCycleEvidenceLabel:
      overlay.futureCycleEvidenceLabel ||
      base.futureCycleEvidenceLabel ||
      null,
    opportunityType: (() => {
      const fromOverlay = text(overlay.opportunityType);
      if (fromOverlay) return fromOverlay;
      // Prefer concrete lodging roles over opaque FUTURE_CYCLE for reviewed packages.
      const existing = text(base.opportunityType);
      if (existing && !/^FUTURE_CYCLE$/i.test(existing)) return existing;
      return section === "EXPLORATORY" ? "OVERFLOW_HOUSING" : "ASSOCIATION_MEETING";
    })(),
    opportunityTypeLabel:
      text(overlay.opportunityTypeLabel) ||
      (/OVERFLOW/i.test(text(overlay.opportunityType) || text(base.opportunityType))
        ? "Overflow / preferred lodging"
        : "Association meeting"),
    fitExplanation: preferEditorial(base.fitExplanation, fitAssessment, forceEditorial),
    fitRationale: preferEditorial(base.fitRationale, fitAssessment, forceEditorial),
    summaryWhyHotel: preferEditorial(base.summaryWhyHotel, thesis, forceEditorial),
    hotelOpportunityThesis:
      preferEditorial(base.hotelOpportunityThesis, thesis, forceEditorial) ||
      thesis ||
      null,
    whyHotelDemand:
      preferEditorial(
        base.whyHotelDemand,
        // Prefer mechanism/account prose — never fall back to travelers
        whyHotelDemandEditorial || text(overlay.whyHotelDemand) || whatWeKnow,
        forceEditorial
      ) ||
      whyHotelDemandEditorial ||
      whatWeKnow ||
      base.whyHotelDemand,
    summaryWhat:
      preferEditorial(
        base.summaryWhat,
        whatWeKnow || thesis,
        forceEditorial
      ) ||
      whatWeKnow ||
      base.summaryWhat ||
      null,
    summaryWhyNow:
      preferEditorial(
        base.summaryWhyNow,
        whyNowEditorial || text(row.timing),
        forceEditorial
      ) ||
      text(row.timing) ||
      base.summaryWhyNow ||
      "Dated occasion in primary horizon",
    summaryWhyMatters:
      preferEditorial(
        base.summaryWhyMatters,
        whyMatters,
        forceEditorial
      ) ||
      whyMatters ||
      base.summaryWhyMatters ||
      null,
    whyNow:
      preferEditorial(
        base.whyNow,
        whyNowEditorial || text(row.timing),
        forceEditorial
      ) ||
      whyNowEditorial ||
      text(row.timing) ||
      commercialLabel,
    cardWhyNowLine: whyNowEditorial || text(row.timing),
    // Numeric fit scores only via supported scoring model with full component inputs.
    hotelFitScore: (() => {
      if (fitAssessmentObj?.scored && fitAssessmentObj.numericScore != null) {
        return fitAssessmentObj.numericScore;
      }
      const rationale = text(
        preferEditorial(base.fitRationale, fitAssessment || thesis, forceEditorial) ||
          thesis
      );
      const hasCanonicalBasis =
        Boolean(base.hotelFitScoreBasis || base.fitScoreMethod || base.fitScoreVersion) &&
        rationale &&
        !isGenericThesis(rationale);
      if (hasCanonicalBasis && base.hotelFitScore != null) return base.hotelFitScore;
      return null;
    })(),
    hotelFitComponents: fitAssessmentObj?.components || base.hotelFitComponents || null,
    hotelFitAssessment: fitAssessmentObj || base.hotelFitAssessment || null,
    hotelFitDimensionAssessments:
      overlay.hotelFitDimensionAssessments ||
      fitAssessmentObj?.dimensions ||
      base.hotelFitDimensionAssessments ||
      null,
    hotelFitComponentLabels:
      fitAssessmentObj?.componentLabels || base.hotelFitComponentLabels || null,
    hotelFitScoreBasis: (() => {
      if (fitAssessmentObj?.scored && fitAssessmentObj.scoringVersion) {
        return fitAssessmentObj.scoringVersion;
      }
      return base.hotelFitScoreBasis || null;
    })(),
    fitScoreMethod: (() => {
      if (fitAssessmentObj?.scored) {
        return (
          fitAssessmentObj.version ||
          fitAssessmentObj.scoringVersion ||
          "gdi-hotel-fit-assessment-v2"
        );
      }
      return base.fitScoreMethod || null;
    })(),
    fitScoreVersion: (() => {
      if (fitAssessmentObj?.scored && fitAssessmentObj.scoringVersion) {
        return fitAssessmentObj.scoringVersion;
      }
      return base.fitScoreVersion || null;
    })(),
    hotelFitScoreNote: (() => {
      if (fitAssessmentObj?.scored && fitAssessmentObj.scoringVersion) {
        return `Scored with ${fitAssessmentObj.scoringVersion}.`;
      }
      const note = text(overlay.hotelFitScoreNote);
      if (note && !/reviewed package did not run canonical|canonical scoring|Qualitative hotel-fit/i.test(note)) {
        return note;
      }
      // Prefer dimension assessments over a vague "Qualitative" note.
      return null;
    })(),
    peakRooms: sanitizePeakRoomsValue(
      overlay.peakRooms ?? base.peakRooms ?? base.estimatedPeakRooms
    ),
    estimatedPeakRooms: sanitizePeakRoomsValue(
      overlay.estimatedPeakRooms ?? base.estimatedPeakRooms ?? base.peakRooms
    ),
    officialSource:
      officialFromRegister ||
      text(pkg?.sourceUrl) ||
      base.officialSource ||
      null,
    discoverySource: officialFromRegister || text(pkg?.sourceUrl) || base.discoverySource,
    sources: mergedSources,
    timingEvidenceStatus: registerSources.length
      ? "source_linked"
      : "unverified_or_staff_only",
    bookingResponsibilityClass:
      text(pkg?.bookingResponsibility) || base.bookingResponsibilityClass || null,
    bookingResponsibilityLabel:
      contactResponsibilityLabel(pkg?.bookingResponsibility) ||
      base.bookingResponsibilityLabel ||
      null,
    contactCompletenessGroup: group,
    recommendedAction:
      (forceEditorial && text(overlay.nextAction)
        ? text(overlay.nextAction)
        : preferRicher(nextActionCandidate, defaultAction)) || defaultAction,
    recommendedNextStep:
      (forceEditorial && text(overlay.nextAction)
        ? text(overlay.nextAction)
        : preferRicher(nextActionCandidate, defaultAction)) || defaultAction,
    specificQuestion: overlay.specificQuestion || base.specificQuestion || null,
    roomDemandStatusNote:
      preferEditorial(
        base.roomDemandStatusNote,
        overlay.roomDemandNote || overlay.roomDemandStatusNote,
        forceEditorial
      ) ||
      base.roomDemandStatusNote ||
      "Not established — peak rooms / attendance not published for this occasion.",
    venueHistoryNote:
      preferEditorial(
        base.venueHistoryNote,
        overlay.venueHistory || overlay.venueHistoryNote,
        forceEditorial
      ) ||
      base.venueHistoryNote ||
      "Not researched — prior host hotels for this occasion were not captured in this package.",
    competitiveContextNote:
      preferEditorial(
        base.competitiveContextNote,
        limitations,
        forceEditorial
      ) ||
      base.competitiveContextNote ||
      "Not researched — STR / group alternatives were not assessed for this reviewed package.",
    reactivationNote:
      preferEditorial(
        base.reactivationNote,
        overlay.reactivationNote,
        forceEditorial
      ) ||
      base.reactivationNote ||
      "Not applicable — not framed as a reactivation of a prior hotel relationship.",
    claimEvidence:
      overlay.claimEvidence || base.claimEvidence || null,
    hotelRelevanceResearch:
      overlay.hotelRelevanceResearch || base.hotelRelevanceResearch || null,
    hotelRelevanceResearchVersion:
      overlay.hotelRelevanceResearch?.version ||
      overlay.editorialVersion ||
      base.hotelRelevanceResearchVersion ||
      null,
    hotelRelevanceDisposition:
      overlay.hotelRelevanceDisposition ||
      overlay.hotelRelevanceResearch?.disposition ||
      base.hotelRelevanceDisposition ||
      null,
    hotelRelevanceResearchedAt:
      overlay.hotelRelevanceResearch?.researchedAt ||
      base.hotelRelevanceResearchedAt ||
      null,
    hotelFitAssessment:
      overlay.hotelFitAssessmentStructured ||
      overlay.hotelRelevanceResearch?.hotelFitAssessment ||
      base.hotelFitAssessment ||
      null,
    hotelFitAssessmentVersion:
      overlay.hotelFitAssessmentStructured?.version ||
      base.hotelFitAssessmentVersion ||
      null,
    hotelFitPosture:
      overlay.hotelFitAssessmentStructured?.posture ||
      base.hotelFitPosture ||
      null,
    hotelFitDimensionAssessments:
      overlay.hotelFitDimensionAssessments ||
      overlay.hotelFitAssessmentStructured?.dimensions ||
      base.hotelFitDimensionAssessments ||
      null,
    hotelFitQualitativeComponents:
      base.hotelFitQualitativeComponents || null,
    hotelFitScore:
      overlay.hotelFitAssessmentStructured?.hotelFitScore ??
      base.hotelFitScore ??
      null,
    hotelFitScoreBasis:
      overlay.hotelFitAssessmentStructured?.scoringVersion ||
      base.hotelFitScoreBasis ||
      null,
    hotelFitScoreNote:
      overlay.hotelFitAssessmentStructured?.summary ||
      base.hotelFitScoreNote ||
      null,
    hotelFitRationale:
      overlay.hotelFitAssessmentStructured?.summary ||
      base.hotelFitRationale ||
      null,
    hotelFitOverallStatus:
      overlay.hotelFitAssessmentStructured?.overallStatus ||
      base.hotelFitOverallStatus ||
      null,
    hotelFitCalculationBasis:
      overlay.hotelFitAssessmentStructured?.calculationBasis ||
      base.hotelFitCalculationBasis ||
      null,
    fitComponents:
      overlay.hotelFitAssessmentStructured?.components ||
      base.fitComponents ||
      null,
    hotelFitComponentLabels:
      overlay.hotelFitAssessmentStructured?.componentLabels ||
      base.hotelFitComponentLabels ||
      null,
    physicalFitScore: base.physicalFitScore ?? null,
    geographyFitScore: base.geographyFitScore ?? null,
    timingScore: base.timingScore ?? null,
    commercialValueScore: base.commercialValueScore ?? null,
    historicalFitScore: base.historicalFitScore ?? null,
    competitiveAccessibilityScore: base.competitiveAccessibilityScore ?? null,
    contactabilityScore: base.contactabilityScore ?? null,
    evidenceConfidence:
      overlay.hotelFitAssessmentStructured?.evidence?.evidenceConfidence ??
      base.evidenceConfidence ??
      null,
    evidenceConfidenceExplanation:
      overlay.hotelFitAssessmentStructured?.evidence
        ?.evidenceConfidenceExplanation ||
      base.evidenceConfidenceExplanation ||
      null,
    contactResearchAttempted: true,
    whoResearchAttempted: true,
    drawerContentContractVersion: "gdi-drawer-content-contract-v3",
    drawerEditorialVersion: overlay.editorialVersion || base.drawerEditorialVersion || null,
    updatedAt: new Date().toISOString(),
    ...preserved,
  };

  // Preserve richer existing content field-by-field after spread
  // (skip when forceEditorial — overlay prose is the intentional repair)
  if (!forceEditorial) {
    for (const k of RICH_CONTENT_FIELDS) {
      if (
        base[k] != null &&
        text(base[k]) &&
        isGenericThesis(next[k]) &&
        !isGenericThesis(base[k])
      ) {
        next[k] = base[k];
      }
    }
  }

  // Canonical hotel-fit assessment: expand structured assessment onto flat
  // score / dimension / evidence fields so Airtable payload + drawer stay aligned.
  const structuredFit =
    next.hotelFitAssessment && typeof next.hotelFitAssessment === "object"
      ? next.hotelFitAssessment
      : overlay.hotelFitAssessmentStructured || null;
  if (structuredFit?.dimensions || structuredFit?.version) {
    next = applyHotelFitAssessmentToOpportunity(next, structuredFit);
    // Do not let assessment defaults overwrite justified sales fields from package
    if (base.opportunityQualification) {
      next.opportunityQualification = base.opportunityQualification;
      next.opportunityQualificationLabel =
        base.opportunityQualificationLabel || next.opportunityQualificationLabel;
    }
    if (base.bookingWindowStatus) {
      next.bookingWindowStatus = base.bookingWindowStatus;
      next.bookingWindowLabel =
        base.bookingWindowLabel || next.bookingWindowLabel;
    }
    if (text(base.recommendedAction) && !forceEditorial) {
      next.recommendedAction = preferRicher(
        next.recommendedAction,
        base.recommendedAction
      );
    }
  }

  const verified =
    text(pkg?.verificationDate) ||
    text(base.lastVerifiedAt) ||
    text(base.lastResearchedAt);
  if (verified && !next.lastVerifiedAt) {
    next.lastVerifiedAt = /^\d{4}-\d{2}-\d{2}$/.test(verified)
      ? `${verified}T12:00:00.000Z`
      : verified;
  }

  if (pkg) {
    const pkgNorm = {
      ...pkg,
      phoneType,
      completenessGroup: group,
      businessPhone: /^not\s*established$/i.test(text(pkg.businessPhone))
        ? "Not established"
        : pkg.businessPhone,
    };
    // Prefer personEmail for CMSS when functional education@ is primary method
    if (pkg.personEmail && text(pkg.person) && !/^not\s*established$/i.test(pkg.person)) {
      pkgNorm.contactMethod = pkg.personEmail;
      pkgNorm.contactMethodType = "named_email";
      next.functionalContactEmail = text(pkg.contactMethod);
    }
    next = applyContactPackageToOpportunity(next, pkgNorm);
    if (next.primaryContact) {
      next.primaryContact.phoneType = phoneType || next.primaryContact.phoneType;
      next.primaryContact.phoneTypeLabel = phoneTypeCustomerLabel(phoneType);
      next.primaryContact.relevanceEvidence =
        preferRicher(
          text(pkg.relevanceEvidence),
          text(overlay.contactRelevance)
        ) || text(pkg.relevanceEvidence);
      next.primaryContact.whyThisContact =
        next.primaryContact.relevanceEvidence ||
        next.primaryContact.whyThisContact ||
        null;
      next.primaryContact.verificationDate = text(pkg.verificationDate);
    }
    next.contactRelevance =
      preferRicher(
        next.contactRelevance,
        text(overlay.contactRelevance) || text(pkg.relevanceEvidence)
      ) || next.contactRelevance;
    next.primaryContactPhoneType = phoneType;
    next.primaryContactPhoneTypeLabel = phoneTypeCustomerLabel(phoneType);
  } else if (text(overlay.contactRelevance)) {
    next.contactRelevance = preferRicher(
      next.contactRelevance,
      overlay.contactRelevance
    );
  }

  const elig = evaluateProspectPublicationEligibility(next);
  next.prospectHotelFacing = elig.hotelFacing === true;
  next.prospectEligibility = elig.eligibility;
  next.prospectEnrichmentQueue = elig.enrichmentQueue === true;
  next.prospectEligibilityReasons = elig.reasons || [];
  next.prospectContactRouteReason = elig.route?.reason || null;
  next.prospectCustomerLabel = customerFacingLabelForProspect(next);
  if (!next.prospectHotelFacing) {
    // Keep internally for enrichment; do not force customerVisible false if already true from other path
    next.prospectInternalOnly = true;
  }

  // Never invent Ready/ACTIONABLE
  if (!existing?.gdiMaturityState || existing.gdiMaturityState === "SIGNAL") {
    // leave maturity as-is / unset — publication path is independent
  }

  // Assign Priority + Action Window AFTER contact package merge so usable-route
  // and fit thesis are visible. Never invent HIGH / Pursue Now.
  // prospectReviewSection (Core/Exploratory) stays independent packaging.
  {
    const stamped = applyPriorityActionAssignment(next, {
      reclassifyWatchDefaults: true,
    });
    next = stamped.opportunity;
    if (!next.priority) next.priority = PRIORITY.WATCHLIST;
    if (!next.bookingWindowStatus) next.bookingWindowStatus = BOOKING_WINDOW.WATCH;
  }

  return { opportunity: next, eligibility: elig };
}

function ensureMayflowerProfile(hotelId) {
  const existing = loadHotelProfile(hotelId);
  if (existing) return { profile: existing, created: false };
  const profile = {
    hotelId,
    canonicalIds: {
      censusRecordId: hotelId,
      note: "Initialized for reviewed-prospect publication local path",
    },
    identity: {
      hotelName: "The Mayflower Hotel, Autograph Collection",
      address: "1127 Connecticut Avenue NW, Washington, DC 20036, United States",
      city: "Washington",
      state: "District of Columbia",
      country: "US",
      brand: "Autograph Collection",
      parentBrandCompany: "Marriott International",
      chainScale: "Upper Upscale",
      website: "https://www.marriott.com/en-us/hotels/wasor-the-mayflower-hotel-autograph-collection/overview/",
    },
    guestrooms: {
      totalGuestrooms: 581,
      totalGuestroomsClaimKind: "ESTIMATED",
      totalGuestroomsConfidence: "MEDIUM",
      note: "From prior Mayflower GDI registration notes — confirm before commercial claims",
    },
    meetingCapability: {
      meetingRooms: 27,
      meetingRoomsClaimKind: "ESTIMATED",
    },
    hotelCharacteristics: {
      serviceLevel: "full_service",
      classification: "urban_downtown",
      publicTransit: "dc_metro_access",
    },
    gdiOnboarding: {
      reviewedProspectPublication: true,
      initializedAt: new Date().toISOString(),
      source: "reviewed-prospect-import-v1",
    },
    createdAt: new Date().toISOString(),
  };
  saveHotelProfile(hotelId, profile);
  return { profile, created: true };
}

function ensureMayflowerConfig() {
  // Config lives under config/ — for selectable hotels; write only if missing
  const configPath = path.resolve(
    path.dirname(fileURLToPath(import.meta.url)),
    "../../config/group-demand-intelligence/hotels/rec0eTEnCqlsGJJt9.json"
  );
  if (fs.existsSync(configPath)) return { created: false, path: configPath };
  const config = {
    hotelId: "rec0eTEnCqlsGJJt9",
    displayName: "The Mayflower Hotel, Autograph Collection",
    commercialPriorities: {
      targetSegments: ["Associations", "Corporate", "Government", "Advocacy"],
      notes: "Downtown DC Autograph — reviewed prospect publication onboarding",
    },
    demandTerritory: {
      label: "Downtown DC / National Mall corridor",
      evaluationQuestion: "Could The Mayflower realistically compete for this lodging?",
    },
    gdiOnboarding: {
      reviewedProspectPublication: true,
    },
  };
  fs.mkdirSync(path.dirname(configPath), { recursive: true });
  fs.writeFileSync(configPath, JSON.stringify(config, null, 2) + "\n");
  return { created: true, path: configPath };
}

function backupHotelStore(hotelId, backupDir, opportunitiesDoc = null) {
  const src = path.join(hotelDir(hotelId), "opportunities.json");
  const profileSrc = path.join(hotelDir(hotelId), "profile.json");
  fs.mkdirSync(backupDir, { recursive: true });
  const stamp = new Date().toISOString().replace(/[:.]/g, "-");
  const out = {
    opportunities: null,
    profile: null,
    adapterSnapshot: null,
    persistenceMode: getGdiOpportunityPersistenceMode(),
  };
  if (opportunitiesDoc) {
    const dest = path.join(
      backupDir,
      `${hotelId}-adapter-snapshot-${stamp}.json`
    );
    fs.writeFileSync(dest, JSON.stringify(opportunitiesDoc, null, 2) + "\n");
    out.adapterSnapshot = dest;
  }
  if (fs.existsSync(src)) {
    const dest = path.join(backupDir, `${hotelId}-opportunities-${stamp}.json`);
    fs.copyFileSync(src, dest);
    out.opportunities = dest;
  }
  if (fs.existsSync(profileSrc)) {
    const dest = path.join(backupDir, `${hotelId}-profile-${stamp}.json`);
    fs.copyFileSync(profileSrc, dest);
    out.profile = dest;
  }
  return out;
}

/**
 * Detect conflicts when production facts are newer than the package and
 * would be overwritten without an explicit force.
 */
function detectNewerFactConflict(existing, opportunity, pkg) {
  if (!existing) return null;
  const existingUpdated = Date.parse(existing.updatedAt || existing.lastVerifiedAt || "") || 0;
  const pkgVerified = Date.parse(pkg?.verificationDate || "") || 0;
  const existingEmail = text(existing.primaryContact?.email);
  const nextEmail = text(opportunity.primaryContact?.email);
  const existingPhone = text(existing.primaryContact?.phone);
  const nextPhone = text(opportunity.primaryContact?.phone);
  if (
    existingUpdated &&
    pkgVerified &&
    existingUpdated > pkgVerified + 24 * 60 * 60 * 1000 &&
    ((existingEmail && nextEmail && existingEmail !== nextEmail) ||
      (existingPhone && nextPhone && existingPhone !== nextPhone))
  ) {
    return {
      reason: "newer_production_contact_facts",
      existingUpdatedAt: existing.updatedAt || existing.lastVerifiedAt || null,
      packageVerificationDate: pkg?.verificationDate || null,
      existingEmail: existingEmail || null,
      nextEmail: nextEmail || null,
      existingPhone: existingPhone || null,
      nextPhone: nextPhone || null,
    };
  }
  return null;
}

/**
 * Load versioned hotel-review package inputs.
 */
export function loadReviewedProspectPackage(packageDir) {
  const dir = path.resolve(packageDir);
  const registerPath = path.join(dir, "final-prospect-register.csv");
  const packagesPath = path.join(dir, "contact-packages.json");
  const register = parseCsv(fs.readFileSync(registerPath, "utf8"));
  const packagesDoc = JSON.parse(fs.readFileSync(packagesPath, "utf8"));
  const byId = Object.fromEntries((packagesDoc.packages || []).map((p) => [p.cardId, p]));
  return {
    packageDir: dir,
    registerPath,
    packagesPath,
    registerHash: sha256File(registerPath),
    packagesHash: sha256File(packagesPath),
    packageVersion: REVIEWED_PROSPECT_PACKAGE_VERSION,
    register,
    packagesById: byId,
    packagesDoc,
  };
}

/**
 * Import reviewed prospects for one hotel through the canonical persistence
 * adapter (Airtable when configured; filesystem when explicitly selected).
 */
/**
 * Optional sales-review overlay (hotel-specific thesis / next actions)
 * recovered from hotel-review markdown — never from commercialStatus labels.
 */
export function loadSalesReviewContentOverlay(overlayPath = null) {
  const candidates = [
    overlayPath,
    path.resolve(
      path.dirname(fileURLToPath(import.meta.url)),
      "../../reports/gdi-drawer-completeness/sales-review-overlay.json"
    ),
  ].filter(Boolean);
  for (const p of candidates) {
    if (!fs.existsSync(p)) continue;
    try {
      const doc = JSON.parse(fs.readFileSync(p, "utf8"));
      const byCardId = Object.fromEntries(
        (doc.cards || []).map((c) => [c.cardId, c])
      );
      return { path: p, byCardId, count: Object.keys(byCardId).length };
    } catch (err) {
      console.warn(
        `[reviewed-prospect-import] overlay load failed: ${p}: ${err.message}`
      );
    }
  }
  return { path: null, byCardId: {}, count: 0 };
}

export async function importReviewedProspectsForHotel({
  hotelId,
  packageDir,
  dryRun = true,
  backupDir = null,
  initMayflower = true,
  skipConflictedUpdates = true,
  // Bethesda has known_id map; Mayflower and new hotels default to create-only
  // unless identity/card stamps already exist.
  allowOrgHeuristic = false,
  contentOverlayPath = null,
} = {}) {
  clearGdiRepositoryReadCache();
  invalidateGdiHotelReadCache(hotelId);
  const persistenceMode = getGdiOpportunityPersistenceMode();
  const pkgBundle = loadReviewedProspectPackage(packageDir);
  const overlayBundle = loadSalesReviewContentOverlay(contentOverlayPath);
  const rows = pkgBundle.register.filter((r) => r.hotelId === hotelId);
  const conflicts = [];
  const quarantined = [];
  const manifest = [];
  const drawerResults = [];

  let profileInit = { created: false };
  let configInit = { created: false };
  if (hotelId === "rec0eTEnCqlsGJJt9" && initMayflower) {
    if (!dryRun) {
      profileInit = ensureMayflowerProfile(hotelId);
      configInit = ensureMayflowerConfig();
    } else {
      profileInit = { created: !loadHotelProfile(hotelId), dryRun: true };
      configInit = { created: true, dryRun: true };
    }
  }

  const doc = await loadOpportunitiesCanonical(hotelId);
  const existingList = [...(doc.opportunities || [])];
  const nextList = [...existingList];
  const claimedIds = new Set();
  let backup = null;

  if (!dryRun && backupDir) {
    backup = backupHotelStore(hotelId, backupDir, doc);
  }

  for (const row of rows) {
    const pkg = pkgBundle.packagesById[row.cardId] || null;
    const contentOverlay = overlayBundle.byCardId[row.cardId] || null;
    const found = findExisting(nextList, row, pkg || {}, {
      claimedIds,
      allowOrgHeuristic,
    });
    const { opp: existing, matchType, ambiguousIds = [] } = found;

    if (matchType === "ambiguous_org" || matchType === "ambiguous_org_year") {
      conflicts.push({
        cardId: row.cardId,
        reason: matchType,
        ambiguousIds,
      });
      quarantined.push({
        cardId: row.cardId,
        reason: matchType,
        ambiguousIds,
      });
      // Force create with a new id rather than overwrite ambiguous production rows.
    }

    const { opportunity, eligibility } = buildOpportunityFromPackage({
      row,
      pkg,
      existing: matchType.startsWith("ambiguous_") ? null : existing,
      hotelId,
      contentOverlay,
    });
    const drawerEval = evaluateDrawerContentCompleteness(opportunity);
    opportunity.drawerContentCompleteness = {
      ok: drawerEval.ok,
      failed: drawerEval.failed,
      warnings: drawerEval.warnings,
      version: drawerEval.version,
    };
    drawerResults.push({
      cardId: row.cardId,
      opportunityId: opportunity.id,
      hotelFacing: eligibility.hotelFacing === true,
      drawerOk: drawerEval.ok,
      failed: drawerEval.failed,
      warnings: drawerEval.warnings,
      layers: drawerEval.layers,
    });

    if (existing && matchType === "org_year_heuristic") {
      if (
        existing.prospectIdentityKey &&
        existing.prospectIdentityKey !== opportunity.prospectIdentityKey
      ) {
        conflicts.push({
          cardId: row.cardId,
          existingId: existing.id,
          reason: "identity_key_mismatch",
        });
      }
    }

    const newerConflict = detectNewerFactConflict(existing, opportunity, pkg);
    if (newerConflict) {
      conflicts.push({
        cardId: row.cardId,
        existingId: existing.id,
        ...newerConflict,
      });
      if (skipConflictedUpdates) {
        quarantined.push({
          cardId: row.cardId,
          opportunityId: existing.id,
          reason: newerConflict.reason,
        });
        claimedIds.add(existing.id);
        manifest.push({
          cardId: row.cardId,
          hotelId,
          section: row.section,
          account: row.account,
          opportunityId: existing.id,
          action: "quarantined_conflict",
          matchType,
          hotelFacing: eligibility.hotelFacing ? "YES" : "NO",
          enrichmentQueue: eligibility.enrichmentQueue ? "YES" : "NO",
          eligibility: eligibility.eligibility,
          contactGroup: opportunity.contactCompletenessGroup,
          person: existing.primaryContact?.name || pkg?.person || "",
          contactMethod:
            existing.primaryContact?.email ||
            pkg?.contactMethod ||
            "",
          businessPhone: existing.primaryContact?.phone || pkg?.businessPhone || "",
          phoneType:
            existing.primaryContactPhoneType ||
            existing.primaryContact?.phoneType ||
            pkg?.phoneType ||
            "",
        });
        continue;
      }
    }

    const action = existing ? "update" : "create";
    if (existing) {
      const idx = nextList.findIndex((o) => o.id === existing.id);
      opportunity.id = existing.id;
      nextList[idx] = opportunity;
      claimedIds.add(existing.id);
    } else {
      nextList.push(opportunity);
      claimedIds.add(opportunity.id);
    }

    manifest.push({
      cardId: row.cardId,
      hotelId,
      section: row.section,
      account: row.account,
      opportunityId: opportunity.id,
      action,
      matchType,
      hotelFacing: eligibility.hotelFacing ? "YES" : "NO",
      enrichmentQueue: eligibility.enrichmentQueue ? "YES" : "NO",
      eligibility: eligibility.eligibility,
      contactGroup: opportunity.contactCompletenessGroup,
      person: opportunity.primaryContact?.name || pkg?.person || "",
      contactMethod:
        opportunity.primaryContact?.email ||
        opportunity.functionalContactEmail ||
        pkg?.contactMethod ||
        "",
      businessPhone: opportunity.primaryContact?.phone || pkg?.businessPhone || "",
      phoneType: opportunity.primaryContactPhoneType || pkg?.phoneType || "",
    });
  }

  let saveResult = null;
  if (!dryRun) {
    const importedAt = new Date().toISOString();
    saveResult = await saveOpportunitiesCanonical(hotelId, {
      ...doc,
      hotelId,
      opportunities: nextList,
      reviewedProspectImport: {
        packageVersion: pkgBundle.packageVersion,
        registerHash: pkgBundle.registerHash,
        packagesHash: pkgBundle.packagesHash,
        importedAt,
        cardCount: rows.length,
        persistenceMode,
      },
    });
    try {
      const { appendHotelActivityEvent, ACTIVITY_KIND } = await import(
        "./hotel-activity-history-v1.js"
      );
      appendHotelActivityEvent(hotelId, {
        kind: ACTIVITY_KIND.REVIEWED_IMPORT,
        status: "completed",
        at: importedAt,
        runId: `reviewed_import_${pkgBundle.packageVersion || "v1"}`,
        source: "reviewed-prospect-import-v1",
        provenance: "live_write",
        note: `cardCount=${rows.length}`,
      });
    } catch (actErr) {
      console.warn(
        "[reviewed-import] activity ledger:",
        actErr?.message || actErr
      );
    }
    clearGdiRepositoryReadCache();
    invalidateGdiHotelReadCache(hotelId);
  }

  const facing = manifest.filter((m) => m.hotelFacing === "YES");
  const priorityFacing = facing.filter((m) => m.section === "PRIORITY");
  const exploratoryFacing = facing.filter((m) => m.section === "EXPLORATORY");
  const enrichment = manifest.filter((m) => m.enrichmentQueue === "YES");
  const facingDrawer = drawerResults.filter((d) => d.hotelFacing);
  const drawerPass = facingDrawer.filter((d) => d.drawerOk).length;

  return {
    ok: true,
    dryRun,
    hotelId,
    persistenceMode,
    airtableConfigured: isGdiOpportunityAirtableConfigured(),
    dataRoot: getGdiDataRoot(),
    packageVersion: pkgBundle.packageVersion,
    registerHash: pkgBundle.registerHash,
    packagesHash: pkgBundle.packagesHash,
    contentOverlay: {
      path: overlayBundle.path,
      cardsLoaded: overlayBundle.count,
    },
    profileInit,
    configInit,
    backup,
    savePersistence: saveResult?.persistence || null,
    counts: {
      packageRows: rows.length,
      create: manifest.filter((m) => m.action === "create").length,
      update: manifest.filter((m) => m.action === "update").length,
      quarantined: quarantined.length,
      hotelFacing: facing.length,
      priorityHotelFacing: priorityFacing.length,
      exploratoryHotelFacing: exploratoryFacing.length,
      enrichmentQueue: enrichment.length,
      drawerCompletenessPass: drawerPass,
      drawerCompletenessFail: facingDrawer.length - drawerPass,
      existingUnmatchedPreserved:
        existingList.length -
        manifest.filter((m) => m.action === "update" || m.action === "quarantined_conflict")
          .length,
    },
    conflicts,
    quarantined,
    manifest,
    drawerCompleteness: {
      version: "gdi-drawer-content-contract-v2",
      facingTotal: facingDrawer.length,
      pass: drawerPass,
      fail: facingDrawer.length - drawerPass,
      results: drawerResults,
    },
  };
}

export async function importReviewedProspectsBothHotels(opts = {}) {
  const hotels = opts.hotelIds || REVIEWED_PROSPECT_HOTEL_IDS;
  const results = [];
  for (const hotelId of hotels) {
    results.push(await importReviewedProspectsForHotel({ ...opts, hotelId }));
  }
  return results;
}

export { KNOWN_ID_MATCHES, PROTECTED_USER_FIELDS };
