/**
 * Deal Opportunities — read HIF Discovery Inventory + Findings from HPC base.
 * Server-side only. CUSTOMER_BLOCKED. No schema invention.
 */

import {
  GRAPH_BASE_ID_CANONICAL,
  MAP_DISCOVERY_INVENTORY,
  MAP_DISCOVERY_FINDINGS,
} from "../sec-edgar/discovery-inventory-airtable-contract-v23.js";
import { FOUNDATION_DESTINATIONS } from "../sec-edgar/hif-foundation-persistence-v1.js";
import { buildDrawerV2 } from "./hif-deal-opportunities-drawer-v2.js";
import {
  assessCommercialEligibility,
  narrativeForEligibility,
  statusLabelForEligibility,
} from "./hif-deal-opportunities-commercial-eligibility-v1.js";

export const DEAL_OPPORTUNITIES_READER = "hif-deal-opportunities-reader-v1";
export { assessCommercialEligibility } from "./hif-deal-opportunities-commercial-eligibility-v1.js";

const INV = FOUNDATION_DESTINATIONS.discoveryInventory.tableId;
const FIND = FOUNDATION_DESTINATIONS.discoveryFindings.tableId;

function apiKey() {
  return String(process.env.AIRTABLE_API_KEY || "").trim();
}

async function listAll(tableId, { filterByFormula, fields } = {}) {
  const key = apiKey();
  if (!key) throw new Error("AIRTABLE_API_KEY_MISSING");
  const out = [];
  let offset;
  do {
    const qs = new URLSearchParams({ pageSize: "100" });
    if (filterByFormula) qs.set("filterByFormula", filterByFormula);
    if (fields?.length) fields.forEach((f) => qs.append("fields[]", f));
    if (offset) qs.set("offset", offset);
    const res = await fetch(`https://api.airtable.com/v0/${GRAPH_BASE_ID_CANONICAL}/${tableId}?${qs}`, {
      headers: { Authorization: `Bearer ${key}` },
    });
    const body = await res.json().catch(() => ({}));
    if (!res.ok) {
      const err = new Error(body?.error?.message || `AIRTABLE_${res.status}`);
      err.status = res.status;
      err.body = body;
      throw err;
    }
    out.push(...(body.records || []));
    offset = body.offset;
  } while (offset);
  return out;
}

function firstLinked(v) {
  if (Array.isArray(v) && v.length) return String(v[0]);
  if (typeof v === "string" && v.startsWith("rec")) return v;
  return null;
}

function regionBucket(country) {
  const c = String(country || "").trim().toUpperCase();
  if (!c) return "Other";
  if (c === "US" || c === "USA" || c === "UNITED STATES") return "US";
  if (["TT", "JM", "DO", "PR", "BB", "BS", "CU", "HT", "LC", "GD"].includes(c) || /CARIB|TRINIDAD|TOBAGO/.test(c)) {
    return "Caribbean";
  }
  if (c === "IN" || c === "INDIA") return "India";
  return "Other";
}

const COUNTRY_DISPLAY_NAMES = {
  US: "United States",
  USA: "United States",
  TT: "Trinidad and Tobago",
  IN: "India",
  BR: "Brazil",
  CA: "Canada",
  MX: "Mexico",
  DO: "Dominican Republic",
  PR: "Puerto Rico",
  GB: "United Kingdom",
  UK: "United Kingdom",
  ES: "Spain",
  PT: "Portugal",
  MV: "Maldives",
  TC: "Turks and Caicos",
  JM: "Jamaica",
  BB: "Barbados",
  BS: "Bahamas",
  CU: "Cuba",
  HT: "Haiti",
  LC: "Saint Lucia",
  GD: "Grenada",
};

function formatCountryDisplay(country) {
  const raw = String(country || "").trim();
  if (!raw) return "";
  const key = raw.toUpperCase().replace(/\./g, "");
  if (COUNTRY_DISPLAY_NAMES[key]) return COUNTRY_DISPLAY_NAMES[key];
  if (/^[A-Za-z]{2,3}$/.test(raw)) return COUNTRY_DISPLAY_NAMES[raw.toUpperCase()] || raw.toUpperCase();
  return raw;
}

function formatStateDisplay(state) {
  const raw = String(state || "").trim();
  if (!raw) return "";
  if (/^[A-Za-z]{2}$/.test(raw)) return raw.toUpperCase();
  return raw;
}

function marketLabel(inv) {
  const city = inv.city || "";
  const state = formatStateDisplay(inv.stateRegion || "");
  const country = formatCountryDisplay(inv.country || "");
  const parts = [city, state, country].filter(Boolean);
  if (parts.length) return parts.join(", ");
  return "Market to verify";
}

function classifyKind(fieldName) {
  const f = String(fieldName || "");
  if (f.includes("BCT")) return "BCT";
  if (f.includes("MCT")) return "MCT";
  return "OTHER";
}

function typeLabel(kind, portfolio) {
  if (portfolio) return "Portfolio management";
  if (kind === "BCT") return "Brand conversion";
  if (kind === "MCT") return "Management opportunity";
  return "Commercial intelligence";
}

/**
 * Exclude completed / historical / weak handoffs from qualified-open.
 * Keep them available under emerging/all when commercially informative.
 * Central gate: hif-deal-opportunities-commercial-eligibility-v1.js
 * (VERIFIED ≠ open; PRINCIPAL field alone is insufficient.)
 */
function classifyBucket(finding, inventory) {
  const assessment = assessCommercialEligibility({ finding, inventory });
  return {
    group: assessment.group,
    reason: assessment.reason,
    disposition: assessment.disposition,
    destination: assessment.destination,
    details: assessment.details,
    timingTag: assessment.timingTag,
    relevanceTag: assessment.relevanceTag,
    eligible: assessment.eligible,
  };
}

function parseDecisionWindow(finding, inventory) {
  const blob = `${finding.valueObserved || ""} ${finding.notes || ""} ${inventory?.notesQuarantine || ""}`;
  const yearMatch = blob.match(/\b(202[6-9]|203[0-9])\b/);
  let year = yearMatch ? yearMatch[1] : "unknown";
  let window = "Timing to verify";

  if (/end of 2026|by end of 2026|year-end 2026|YE ?2026/i.test(blob)) {
    window = "By end of 2026";
    year = "2026";
  } else if (/2027-04-30|30 April 2027|Apr(?:il)? 2027/i.test(blob)) {
    window = "Apr 2027";
    year = "2027";
  } else if (/2027-02-28|28 Feb(?:ruary)? 2027|Feb(?:ruary)? 2027/i.test(blob)) {
    window = "Feb 2027";
    year = "2027";
  } else if (/Jan(?:uary)? 2027|2027-01/i.test(blob)) {
    window = "Jan 2027";
    year = "2027";
  } else if (/\b2027\b/.test(blob)) {
    window = "2027";
    year = "2027";
  } else if (/month-to-month|current evaluation|evaluating/i.test(blob)) {
    window = "Current evaluation";
    year = "unknown";
  } else if (/award pending/i.test(blob)) {
    window = "Award pending";
    year = "unknown";
  } else if (year !== "unknown") {
    window = year;
  }

  return { window, year };
}

/**
 * Estimated decision horizon buckets (relative to "now").
 * Separate from Decision Window (calendar label / year).
 * @returns {"0_6_MONTHS"|"6_12_MONTHS"|"12_24_MONTHS"|"UNKNOWN"}
 */
function deriveTimingHorizon(window, year, finding, inventory) {
  const blob = `${finding?.valueObserved || ""} ${finding?.notes || ""} ${inventory?.notesQuarantine || ""} ${window || ""}`;
  if (/\b0[_-]?\s*6\s*months?\b|NOW_ACTIVE_PROCESS|current evaluation|award pending|month-to-month/i.test(blob)) {
    return "0_6_MONTHS";
  }
  if (/\b6[_-]?\s*12\s*months?\b/i.test(blob)) return "6_12_MONTHS";
  if (/\b12[_-]?\s*24\s*months?\b/i.test(blob)) return "12_24_MONTHS";

  const MONTH = {
    jan: 0, january: 0, feb: 1, february: 1, mar: 2, march: 2, apr: 3, april: 3,
    may: 4, jun: 5, june: 5, jul: 6, july: 6, aug: 7, august: 7,
    sep: 8, sept: 8, september: 8, oct: 9, october: 9, nov: 10, november: 10, dec: 11, december: 11,
  };
  let decisionDate = null;
  const w = String(window || "").trim();
  if (/current evaluation|award pending|month-to-month/i.test(w)) {
    decisionDate = new Date();
  } else {
    const byEnd = w.match(/by end of\s+(20\d{2})/i);
    if (byEnd) decisionDate = new Date(Number(byEnd[1]), 11, 31);
    const monYear = w.match(
      /^(Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:t(?:ember)?)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)\s+(20\d{2})$/i
    );
    if (!decisionDate && monYear) {
      const mi = MONTH[monYear[1].toLowerCase()];
      if (mi != null) decisionDate = new Date(Number(monYear[2]), mi, 15);
    }
    if (!decisionDate && /^20\d{2}$/.test(w)) decisionDate = new Date(Number(w), 5, 30);
    if (!decisionDate && year && /^20\d{2}$/.test(String(year))) {
      decisionDate = new Date(Number(year), 5, 30);
    }
  }

  if (!decisionDate || Number.isNaN(decisionDate.getTime())) return "UNKNOWN";
  const months = (decisionDate.getTime() - Date.now()) / (1000 * 60 * 60 * 24 * 30.4375);
  if (months <= 6) return "0_6_MONTHS";
  if (months <= 12) return "6_12_MONTHS";
  if (months <= 24) return "12_24_MONTHS";
  return "12_24_MONTHS";
}

function accessAssessment(group, notes) {
  if (group === "qualified") {
    if (/Probable|OPEN_|FIRST_100|do_not_requalify/i.test(notes)) return "Probable";
    return "Probable";
  }
  if (/Unverified|EMERGING|ADJACENT|appeal/i.test(notes)) return "Unverified";
  return "Unverified";
}

function statusLabel(group, notes, kind, classed = null) {
  if (classed) return statusLabelForEligibility(classed, kind, notes);
  if (/Award pending/i.test(notes)) return "Award pending";
  if (group === "watch") return "Emerging";
  if (kind === "BCT") return "Private selection";
  if (/portfolio|PORTFOLIO|NOT_A_SINGLE_HOT/i.test(notes)) return "Portfolio selection";
  return "Operator selection";
}

function buildWhy(finding) {
  const v = String(finding.valueObserved || "").trim();
  const s = String(finding.evidenceSnippet || "").trim();
  if (v) return v.length > 420 ? `${v.slice(0, 417)}…` : v;
  if (s) return s.length > 420 ? `${s.slice(0, 417)}…` : s;
  return "Commercial signal retained in HIF findings; narrative requires research enrichment.";
}

function mapInventory(rec) {
  const f = rec.fields || {};
  return {
    airtableId: rec.id,
    discoveryPropertyId: f[MAP_DISCOVERY_INVENTORY.discoveryPropertyId] || null,
    hotelName: f[MAP_DISCOVERY_INVENTORY.hotelNameRaw] || f[MAP_DISCOVERY_INVENTORY.hotelNameNormalized] || "Unnamed hotel",
    city: f[MAP_DISCOVERY_INVENTORY.city] || "",
    stateRegion: f[MAP_DISCOVERY_INVENTORY.stateRegion] || "",
    country: f[MAP_DISCOVERY_INVENTORY.country] || "",
    currentBrand: f[MAP_DISCOVERY_INVENTORY.currentBrand] || f[MAP_DISCOVERY_INVENTORY.brandLabel] || "",
    hpcMatchStatus: f[MAP_DISCOVERY_INVENTORY.hpcMatchStatus] || "",
    hpcHotelId: firstLinked(f[MAP_DISCOVERY_INVENTORY.hpcHotel]),
    sourceFilingUrl: f[MAP_DISCOVERY_INVENTORY.sourceFilingUrl] || null,
    notesQuarantine: f[MAP_DISCOVERY_INVENTORY.notesQuarantine] || "",
    discoveryMethod: f[MAP_DISCOVERY_INVENTORY.discoveryMethod] || "",
    findingIds: Array.isArray(f["HI Discovery Findings"]) ? f["HI Discovery Findings"] : [],
  };
}

function mapFinding(rec) {
  const f = rec.fields || {};
  return {
    airtableId: rec.id,
    findingId: f[MAP_DISCOVERY_FINDINGS.findingId] || null,
    discoveryPropertyId: firstLinked(f[MAP_DISCOVERY_FINDINGS.discoveryProperty]),
    findingType: f[MAP_DISCOVERY_FINDINGS.findingType] || null,
    fieldName: f[MAP_DISCOVERY_FINDINGS.fieldName] || null,
    valueObserved: f[MAP_DISCOVERY_FINDINGS.valueObserved] || "",
    agreementType: f[MAP_DISCOVERY_FINDINGS.agreementType] || null,
    evidenceSnippet: f[MAP_DISCOVERY_FINDINGS.evidenceSnippet] || "",
    sourceUrl: f[MAP_DISCOVERY_FINDINGS.sourceUrl] || null,
    confidence: f[MAP_DISCOVERY_FINDINGS.confidence] || null,
    relatedOrgName: f[MAP_DISCOVERY_FINDINGS.relatedOrgName] || "",
    notes: f[MAP_DISCOVERY_FINDINGS.notes] || "",
    effectiveDate: f[MAP_DISCOVERY_FINDINGS.effectiveDate] || null,
    reportingDate: f[MAP_DISCOVERY_FINDINGS.reportingDate] || null,
  };
}

function buildOpportunity(finding, inventory, relatedHotels = []) {
  const classed = classifyBucket(finding, inventory || {});
  if (classed.group === "component") return null;

  const kind = classifyKind(finding.fieldName);
  // Only true multi-hotel parent mandates — do not treat "portfolio-level optionality"
  // wording on a single named hotel as a portfolio opportunity.
  const isPortfolio =
    relatedHotels.length > 1 ||
    /NOT_A_SINGLE_HOT|children=\d+/i.test(inventory?.notesQuarantine || "") ||
    /Principal MCT for remaining portfolio|AHIP Remaining|one parent MCT/i.test(
      `${finding.notes || ""} ${inventory?.notesQuarantine || ""}`
    );

  let { window, year } = parseDecisionWindow(finding, inventory || {});
  // Do not present future contractual expiry as an open decision window on historical records.
  if (classed.disposition === "historical") {
    const ann =
      finding.reportingDate ||
      finding.effectiveDate ||
      (String(finding.valueObserved || "").match(/\b(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\s+20\d{2}\b/i) ||
        [])[0] ||
      null;
    if (ann) {
      window = String(ann).slice(0, 32);
      year = String(ann).match(/20\d{2}/)?.[0] || year;
    } else if (/^20\d{2}$/.test(String(window)) && Number(window) >= 2030) {
      window = "Historical decision (expiry is not an open selection)";
      year = "unknown";
    }
  }
  const timingHorizon =
    classed.disposition === "historical"
      ? "UNKNOWN"
      : deriveTimingHorizon(window, year, finding, inventory || {});
  const notesBlob = `${finding.notes || ""} ${inventory?.notesQuarantine || ""} ${finding.valueObserved || ""}`;
  const narrative = narrativeForEligibility(classed, kind);
  const hpcId = inventory?.hpcHotelId || null;

  const facts = [
    ["Commercial decision", finding.agreementType || finding.fieldName || "Requires research"],
    ["Portfolio scope", isPortfolio ? `Portfolio (${relatedHotels.length || "multi"} hotels)` : "Single hotel"],
    ["HPC identity", hpcId || inventory?.hpcMatchStatus || "Research pending"],
    ["Discovery Property ID", inventory?.discoveryPropertyId || "Unavailable"],
    ["Finding ID", finding.findingId || "Unavailable"],
    ["Evidence confidence", finding.confidence || "Unavailable"],
    ["Commercial disposition", classed.disposition || classed.reason || "Requires research"],
    [
      "Outreach readiness",
      classed.disposition === "historical"
        ? "Historical only — do not outreach as open opportunity"
        : "Decision-maker pathway requires validation",
    ],
  ];

  if (isPortfolio && relatedHotels.length) {
    facts.push(["Related hotels linked", String(relatedHotels.length)]);
  }

  const sources = [];
  if (finding.sourceUrl) {
    sources.push({
      title: finding.sourceUrl,
      url: finding.sourceUrl,
      caption: "Reported research reference · CUSTOMER_BLOCKED · original verification required for customer release",
    });
  }
  if (inventory?.sourceFilingUrl && inventory.sourceFilingUrl !== finding.sourceUrl) {
    sources.push({
      title: inventory.sourceFilingUrl,
      url: inventory.sourceFilingUrl,
      caption: "Discovery inventory source filing · CUSTOMER_BLOCKED",
    });
  }
  if (!sources.length) {
    sources.push({
      title: "Source URL unavailable in Airtable finding",
      url: null,
      caption: "Requires research",
    });
  }

  const heStatus = hpcId
    ? {
        ready: true,
        hpcRecordId: hpcId,
        message: "Verified HPC identity available — open existing Hotel Explorer.",
      }
    : {
        ready: false,
        hpcRecordId: null,
        message:
          inventory?.hpcMatchStatus === "DISCOVERED_NOT_IN_HPC"
            ? "Hotel Explorer pending — discovery identity not yet linked to HPC."
            : "Hotel Explorer pending — canonical HPC identity requires research.",
      };

  const opportunity = {
    id: finding.findingId || finding.airtableId,
    airtableFindingId: finding.airtableId,
    airtableInventoryId: inventory?.airtableId || null,
    name: inventory?.hotelName || finding.findingId || "Unnamed opportunity",
    sub:
      inventory?.currentBrand ||
      (isPortfolio ? `${relatedHotels.length || "Multi"} properties` : finding.findingType || "HIF finding"),
    city: marketLabel(inventory || {}),
    region: regionBucket(inventory?.country),
    country: inventory?.country || "",
    kind,
    type: typeLabel(kind, isPortfolio),
    status: statusLabel(classed.group, notesBlob, kind, classed),
    group: classed.group,
    classifyReason: classed.reason,
    commercialDisposition: classed.disposition || null,
    commercialDestination: classed.destination || null,
    eligibilityDetails: classed.details || null,
    window,
    year,
    timingHorizon,
    trigger: String(finding.valueObserved || finding.evidenceSnippet || "Commercial signal").slice(0, 120),
    access: accessAssessment(classed.group, notesBlob),
    why: buildWhy(finding),
    facts,
    sources,
    next: narrative.nextStep,
    he: heStatus,
    narrativeHints: narrative,
    relatedHotels: relatedHotels.map((h) => ({
      inventoryId: h.airtableId,
      discoveryPropertyId: h.discoveryPropertyId,
      name: h.hotelName,
      brand: h.currentBrand || "",
      hpcRecordId: h.hpcHotelId,
      city: h.city,
      stateRegion: h.stateRegion,
      country: h.country,
    })),
    publicationState: "CUSTOMER_BLOCKED",
    fieldName: finding.fieldName,
    findingType: finding.findingType,
  };

  opportunity.drawerV2 = buildDrawerV2({
    finding,
    inventory,
    opportunity,
    relatedHotels,
  });

  // Preserve distinct Committed/Closed classifications from research overlays
  // without promoting them into Verified/qualified buckets.
  const overlayStatus = String(opportunity.drawerV2?.commercialStatus || "").toUpperCase();
  if (overlayStatus === "COMMITTED") {
    opportunity.status = "Committed";
    if (!/COMMITTED|CLOSED/i.test(opportunity.classifyReason || "")) {
      opportunity.classifyReason = "COMMITTED_OR_DESTINATION_CONTROL";
    }
  } else if (overlayStatus === "CLOSED") {
    opportunity.status = "Closed";
    if (!/CLOSED/i.test(opportunity.classifyReason || "")) {
      opportunity.classifyReason = "CLOSED_OR_HISTORICAL";
    }
  }

  return opportunity;
}

/**
 * Load commercial findings + inventory and assemble opportunity list.
 */
export async function loadDealOpportunities() {
  const findingsRaw = await listAll(FIND, {
    filterByFormula:
      "OR(FIND('COMMERCIAL_OPPORTUNITY',{Field Name}&''),FIND('COMMERCIAL_SIGNAL',{Field Name}&''))",
  });
  const findings = findingsRaw.map(mapFinding);

  const invIds = [
    ...new Set(findings.map((f) => f.discoveryPropertyId).filter(Boolean)),
  ];

  // Also pull AHIP parent + children via discovery id pattern for related hotels.
  const ahipInv = await listAll(INV, {
    filterByFormula: "FIND('ahip',LOWER({Discovery Property ID}&''))",
  });

  const invById = new Map();
  for (const rec of ahipInv) {
    const mapped = mapInventory(rec);
    invById.set(mapped.airtableId, mapped);
  }

  // Fetch any missing discovery inventory records linked from findings.
  const missing = invIds.filter((id) => !invById.has(id));
  if (missing.length) {
    // Airtable OR() formula chunks of 20
    for (let i = 0; i < missing.length; i += 20) {
      const chunk = missing.slice(i, i + 20);
      const formula = `OR(${chunk.map((id) => `RECORD_ID()='${id}'`).join(",")})`;
      const rows = await listAll(INV, { filterByFormula: formula });
      for (const rec of rows) {
        const mapped = mapInventory(rec);
        invById.set(mapped.airtableId, mapped);
      }
    }
  }

  const ahipChildren = [...invById.values()].filter((h) =>
    /AHIP portfolio component/i.test(h.notesQuarantine || "")
  );

  const opportunities = [];
  for (const finding of findings) {
    const inventory = finding.discoveryPropertyId
      ? invById.get(finding.discoveryPropertyId)
      : null;

    const related =
      inventory && /disc_cb4_ahip_remaining_portfolio/i.test(inventory.discoveryPropertyId || "")
        ? ahipChildren
        : [];

    const opp = buildOpportunity(finding, inventory, related);
    if (opp) opportunities.push(opp);
  }

  // Deduplicate by finding id
  const byId = new Map();
  for (const o of opportunities) {
    if (!byId.has(o.id)) byId.set(o.id, o);
  }
  const list = [...byId.values()];

  const qualified = list.filter((o) => o.group === "qualified");
  const watch = list.filter((o) => o.group === "watch");
  const bct = qualified.filter((o) => o.kind === "BCT");
  const mct = qualified.filter((o) => o.kind === "MCT");

  const propertyIds = new Set();
  for (const o of qualified) {
    if (o.relatedHotels?.length) {
      o.relatedHotels.forEach((h) => propertyIds.add(h.inventoryId || h.discoveryPropertyId));
    } else if (o.airtableInventoryId) {
      propertyIds.add(o.airtableInventoryId);
    }
  }

  const countries = new Set(
    qualified.map((o) => regionBucket(o.country)).filter((c) => c && c !== "Other")
  );
  // Also count distinct country codes from qualified
  const countryCodes = new Set(qualified.map((o) => String(o.country || "").toUpperCase()).filter(Boolean));

  // Additive reconciliation taxonomy (does not change qualified/watch page buckets).
  const portfolioParents = qualified.filter(
    (o) => (o.relatedHotels || []).length > 1 || /portfolio/i.test(o.type || "")
  );
  const verifiedCoreBctMct = qualified.filter((o) => /^find_cb|^find_cfa_/i.test(o.id || ""));
  const earlyEngagementTheses = list.filter((o) =>
    /opew|eci_v1|_early_|exp_v2/i.test(o.id || "")
  );
  const emergingBctMct = watch.filter((o) => o.kind === "BCT" || o.kind === "MCT");
  const adjacentCommercial = watch.filter(
    (o) => o.kind === "OTHER" || /EMERGING_OR_ADJACENT|ADJACENT/i.test(o.classifyReason || "")
  );
  const historicalClosed = watch.filter((o) =>
    /CLOSED_OR_HISTORICAL/i.test(o.classifyReason || "")
  );
  const enrichmentSiblingWatch = watch.filter((o) => /enrich_v1/i.test(o.id || ""));

  return {
    ok: true,
    publicationState: "CUSTOMER_BLOCKED",
    reader: DEAL_OPPORTUNITIES_READER,
    baseId: GRAPH_BASE_ID_CANONICAL,
    asOf: new Date().toISOString(),
    metrics: {
      qualifiedDecisions: qualified.length,
      brandCount: bct.length,
      managementCount: mct.length,
      propertiesRepresented: propertyIds.size,
      countriesCovered: countryCodes.size || countries.size,
      countryLabels: [...countryCodes],
      actionReadyContacts: 0,
      emergingWatchlist: watch.length,
      allIntelligence: list.length,
      // Separate commercial counts for founder reconciliation (additive).
      verifiedCoreBctMctDecisions: verifiedCoreBctMct.length,
      verifiedCoreBctMctIds: verifiedCoreBctMct.map((o) => o.id),
      portfolioParentDecisions: portfolioParents.length,
      portfolioParentIds: portfolioParents.map((o) => o.id),
      earlyEngagementTheses: earlyEngagementTheses.length,
      earlyEngagementIds: earlyEngagementTheses.map((o) => o.id),
      emergingBctMctOpportunities: emergingBctMct.length,
      adjacentCommercialOpportunities: adjacentCommercial.length,
      watchlistSignals: watch.length,
      historicalClosedDecisions: historicalClosed.length,
      enrichmentSiblingWatchCount: enrichmentSiblingWatch.length,
      enrichmentSiblingWatchIds: enrichmentSiblingWatch.map((o) => o.id),
      hotelsRepresented: propertyIds.size,
    },
    opportunities: list,
  };
}
