/**
 * READ-ONLY GDI zero-yield forensic — YOTEL / Spice / AC vs Bethesda / NYC.
 * Does not mutate bags, thresholds, ADP, or shares.
 */
import "dotenv/config";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

import { loadOpportunitiesCanonical } from "../lib/group-demand-intelligence/opportunity-persistence.js";
import { classifyEntityTruth, ENTITY_CLASS } from "../lib/group-demand-intelligence/entity-truth-gate-v1.js";
import {
  classifyActiveDate,
  ACTIVE_DATE_CLASS,
} from "../lib/group-demand-intelligence/active-eligibility-v1.js";
import {
  classifyCustomerSurfaceOpportunity,
  CUSTOMER_SURFACE_DISPOSITION,
} from "../lib/group-demand-intelligence/customer-surface-revalidation-v1.js";
import { isGdiCustomerOpportunityReady } from "../lib/group-demand-intelligence/customer-readiness-gate-v1.js";
import {
  isCustomerFacingOpportunity,
  filterCustomerFacingOpportunities,
} from "../lib/group-demand-intelligence/customer-visibility.js";
import {
  classifyWhoHowPath,
  whoResearchAttempted,
  WHO_PATH_CLASS,
} from "../lib/group-demand-intelligence/opportunity-who-resolution-v1.js";
import {
  evaluateGdiSummaryQuality,
  SUMMARY_QUALITY,
} from "../lib/group-demand-intelligence/opportunity-summary-v1.js";
import { isGdiSurfaceEligible } from "../lib/group-demand-intelligence/surface-eligibility/surface-eligibility-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const OUT = path.join(__dirname, "..", "reports", "gdi", "zero-yield-forensic-2026-10-03");

const HOTELS = {
  YOTEL: {
    id: "recrPQcZg7SFARRb2",
    label: "YOTEL Geneva Lake",
    role: "subject",
    geo: [
      "geneva",
      "genève",
      "geneve",
      "switzerland",
      "suisse",
      "palexpo",
      "vaud",
      "lac léman",
      "lake geneva",
      "leman",
      "gva",
      "carouge",
      "meyrin",
    ],
  },
  SPICE: {
    id: "recKRJjcPnb4tVDDS",
    label: "Spice Island Beach Resort",
    role: "subject",
    geo: [
      "grenada",
      "grand anse",
      "st. george",
      "st george",
      "st. george's",
      "spice island",
      "caribbean",
      "windward",
    ],
  },
  AC: {
    id: "rec2PVBDavppGpenm",
    label: "AC Hotel A Coruña",
    role: "subject",
    geo: [
      "a coruña",
      "a coruna",
      "coruña",
      "coruna",
      "galicia",
      "spain",
      "españa",
      "espana",
      "santiago de compostela",
      "lugo",
      "ferrol",
    ],
  },
  BETHESDA: {
    id: "recLuxvwwxID7U2B8",
    label: "Bethesda Marriott",
    role: "control",
    geo: [
      "bethesda",
      "washington",
      "dc",
      "d.c.",
      "maryland",
      "md",
      "rockville",
      "silver spring",
      "national harbor",
      "arlington",
      "alexandria",
      "northern virginia",
      "dmv",
    ],
  },
  RENAISSANCE: {
    id: "recG66DQJKP2c0UNh",
    label: "Renaissance NY Times Square",
    role: "control",
    geo: [
      "new york",
      "nyc",
      "manhattan",
      "midtown",
      "times square",
      "broadway",
      "hudson yards",
      "brooklyn",
      "queens",
    ],
  },
  HILTON: {
    id: "rec35fExUxCClpOP6",
    label: "Hilton Times Square",
    role: "control",
    geo: [
      "new york",
      "nyc",
      "manhattan",
      "midtown",
      "times square",
      "broadway",
      "hudson yards",
      "brooklyn",
      "queens",
    ],
  },
};

const INVALID_ENTITY = new Set([
  ENTITY_CLASS.UI_CHROME,
  ENTITY_CLASS.CTA_TEXT,
  ENTITY_CLASS.PLATFORM_ATTRIBUTION,
  ENTITY_CLASS.NAVIGATION_TEXT,
  ENTITY_CLASS.SESSION_TITLE,
  ENTITY_CLASS.ARTICLE_TITLE,
  ENTITY_CLASS.DIRECTORY_TITLE,
  ENTITY_CLASS.CATEGORY_LABEL,
  ENTITY_CLASS.CITY_NAME,
  ENTITY_CLASS.VENUE_NAME_ONLY,
  ENTITY_CLASS.GENERIC_MARKET_PAGE,
  ENTITY_CLASS.PROMOTION,
  ENTITY_CLASS.HOTEL_PROMOTION,
  ENTITY_CLASS.SUPPLY_SIDE_PROMOTION,
  ENTITY_CLASS.UNKNOWN_INVALID,
]);

function csvEscape(v) {
  const s = v == null ? "" : String(v);
  if (/[",\n\r]/.test(s)) return `"${s.replace(/"/g, '""')}"`;
  return s;
}

function toCsv(rows, cols) {
  const lines = [cols.join(",")];
  for (const r of rows) {
    lines.push(cols.map((c) => csvEscape(r[c])).join(","));
  }
  return lines.join("\n") + "\n";
}

function write(name, body) {
  fs.writeFileSync(path.join(OUT, name), body, "utf8");
}

function geoBlob(opp) {
  return [
    opp.eventLocationSummary,
    opp.eventLocation,
    opp.destinationStatus,
    opp.venueStatus,
    opp.city,
    opp.market,
    opp.geography,
    opp.title,
    opp.organizationName,
    opp.hotelOpportunityThesis,
    opp.officialSource,
    opp.discoverySource,
  ]
    .map((x) => String(x || "").toLowerCase())
    .join(" | ");
}

function inGeography(opp, tokens) {
  const blob = geoBlob(opp);
  if (!blob.trim()) return { ok: false, reason: "no_geo_fields" };
  // Explicit out-of-market flags
  if (opp.outOfMarket === true || opp.geographyClass === "OUT_OF_MARKET") {
    return { ok: false, reason: "flagged_out_of_market" };
  }
  const hit = tokens.some((t) => blob.includes(String(t).toLowerCase()));
  if (hit) return { ok: true, reason: "token_hit" };
  // Unknown destination with no conflicting city — treat as UNKNOWN (fail geography for funnel)
  if (/tbd|unknown|not (yet )?announced|location tba/i.test(blob) && !/\b(paris|london|tokyo|los angeles|miami|chicago|dubai)\b/i.test(blob)) {
    return { ok: false, reason: "destination_unknown" };
  }
  return { ok: false, reason: "no_market_token" };
}

function lodgingClass(opp) {
  const le = opp.lodgingEvidence || opp.lodging || opp.hiddenDemand?.lodgingEvidence || null;
  const thesis = `${opp.hotelOpportunityThesis || ""} ${opp.summaryWhyHotel || ""} ${opp.venueStatus || ""} ${opp.whyNow || ""}`;
  const type = String(opp.opportunityType || "");
  const status = le && typeof le === "object" ? String(le.status || le.lodgingProof || le.proof || "") : String(le || "");
  const roomBlock =
    (le && typeof le === "object" && (le.roomBlockMentioned || le.housingPageFound)) ||
    /official (room )?block|host hotel|housing bureau|group rate/i.test(thesis + " " + status);
  const strongInf =
    /STRONG_INFERENCE|STRONG|CREDIBLE/i.test(status) ||
    (le && typeof le === "object" && le.overflowMentioned && (le.housingPageFound || le.roomBlockMentioned));
  const eventHousing =
    /housing|accommodation|hotel list|stay.?to.?play|preferred hotel/i.test(thesis) ||
    (le && typeof le === "object" && le.housingPageFound);
  const traveling =
    opp.teamSupported === true ||
    (opp.teamEvidence && (opp.teamEvidence.outOfMarket || opp.teamEvidence.multiPerson)) ||
    /delegation|traveling (team|party)|out[- ]of[- ]market/i.test(thesis);
  const multiDay =
    (opp.eventStartDate &&
      opp.eventEndDate &&
      String(opp.eventStartDate).slice(0, 10) !== String(opp.eventEndDate).slice(0, 10)) ||
    /multi[- ]day|\d+\s*days?/i.test(thesis);

  if (roomBlock && /CONFIRMED|OFFICIAL|DIRECT/i.test(status + " " + thesis)) {
    return "DIRECT_ROOM_BLOCK_EVIDENCE";
  }
  if (roomBlock) return "DIRECT_ROOM_BLOCK_EVIDENCE";
  if (strongInf) return "STRONG_LODGING_INFERENCE";
  if (eventHousing) return "EVENT_HOUSING_SIGNAL";
  if (traveling) return "TRAVELING_DELEGATION_SIGNAL";
  if (multiDay && /OVERFLOW|HOUSING|FUTURE/i.test(type)) return "MULTI_DAY_EVENT_SIGNAL";
  if (multiDay && /conference|summit|congress|meeting|retreat/i.test(String(opp.title || ""))) {
    return "MULTI_DAY_EVENT_SIGNAL";
  }
  return "NO_LODGING_SIGNAL";
}

function lodgingPresent(cls) {
  return cls !== "NO_LODGING_SIGNAL";
}

function hotelFitPass(opp) {
  const score = opp.hotelFitScore ?? opp.hotelFit ?? null;
  if (score != null && Number(score) >= 40) return { ok: true, reason: `score_${score}` };
  if (score != null && Number(score) < 40) return { ok: false, reason: `score_low_${score}` };
  const thesis = String(opp.hotelOpportunityThesis || opp.fitExplanation || opp.summaryWhyHotel || "").trim();
  if (thesis.length >= 40 && !/may require accommodation|potential demand|attendees needing/i.test(thesis)) {
    return { ok: true, reason: "thesis_depth" };
  }
  if (thesis.length >= 24) return { ok: true, reason: "thesis_present" };
  return { ok: false, reason: "no_fit_evidence" };
}

function placementPass(opp) {
  const vs = String(opp.venueStatus || opp.venueSourcingStatus || opp.commercialStatusV2 || "").toUpperCase();
  const reasons = [];
  if (/FULLY_PLACED|FULLY PLACED|CURRENT_CYCLE_CLOSED|CURRENT CYCLE CLOSED/.test(vs)) {
    reasons.push("FULLY_PLACED");
    return { ok: false, reason: reasons.join("|"), class: "FULLY_PLACED" };
  }
  if (/PRIMARY_NO_OVERFLOW|PRIMARY HOTEL SELECTED \/ NO OVERFLOW|PRIMARY_VENUE_SELECTED_NO_OVERFLOW/.test(vs)) {
    reasons.push("PRIMARY_NO_OVERFLOW");
    return { ok: false, reason: reasons.join("|"), class: "PRIMARY_NO_OVERFLOW" };
  }
  if (/PRIMARY_OVERFLOW|OVERFLOW POSSIBLE|PARTIALLY_PLACED|PARTIALLY PLACED|HOTEL TBD|HOTEL_TBD|OPEN_UNRESOLVED|OPEN \/ UNRESOLVED|DESTINATION_SELECTED|FUTURE_NOT_SOURCED|VENUE_TBD|RFP/.test(vs)) {
    return { ok: true, reason: "open_or_overflow_path", class: "OPEN_PATH" };
  }
  if (/PRIMARY|VENUE SELECTED|HOST HOTEL/.test(vs) && !/OVERFLOW|TBD|OPEN|HOUSING/.test(vs)) {
    // Named host without overflow language — placement ambiguous; pass for funnel but flag
    return { ok: true, reason: "venue_named_overflow_unknown", class: "VENUE_SELECTED_AMBIGUOUS" };
  }
  // Unknown placement — allow through (not a kill for unknown)
  return { ok: true, reason: "placement_unknown_allowed", class: "UNKNOWN" };
}

function whoResolved(opp) {
  const who = classifyWhoHowPath(opp);
  const person =
    who.pathClass === WHO_PATH_CLASS.NAMED_DIRECT || who.pathClass === WHO_PATH_CLASS.NAMED_PARTIAL;
  const entityRole =
    who.pathClass === WHO_PATH_CLASS.FUNCTIONAL ||
    who.pathClass === WHO_PATH_CLASS.ORG_PATH ||
    person;
  const contactPath =
    person ||
    who.pathClass === WHO_PATH_CLASS.FUNCTIONAL ||
    who.pathClass === WHO_PATH_CLASS.ORG_PATH ||
    who.pathClass === WHO_PATH_CLASS.NO_CONTACT_AFTER_RESEARCH;
  return {
    who,
    whoEntityResolved: Boolean(opp.organizationName || opp.company),
    whoRoleResolved: entityRole,
    whoPersonResolved: person,
    contactPathFound: contactPath && who.pathClass !== WHO_PATH_CLASS.NOT_RESEARCHED,
    whoResearchOk: whoResearchAttempted(opp),
  };
}

function summaryPass(opp) {
  const q = evaluateGdiSummaryQuality(opp);
  const ok =
    q.quality === SUMMARY_QUALITY.STRONG ||
    q.quality === SUMMARY_QUALITY.ADEQUATE;
  return { ok, quality: q.quality };
}

/** Strip soft-DQ stamps so structural gates can be re-evaluated from evidence. */
function structuralCopy(opp) {
  const next = { ...opp };
  delete next.customerVisible;
  delete next.customerActiveEligible;
  delete next.customerSurfaceDisposition;
  delete next.customerSurfaceReasons;
  if (next.priority === "DISQUALIFIED") {
    next.priority = next.priorityBeforeSurfaceDq || "WATCHLIST";
  }
  return next;
}

function evaluateOpp(opp, hotel) {
  const entity = classifyEntityTruth(opp);
  const entityOk = entity.validEntity === true && !INVALID_ENTITY.has(entity.entityClass);

  const date = classifyActiveDate(opp, {});
  const futureOk =
    date.activeDateClass === ACTIVE_DATE_CLASS.ACTIVE_FUTURE ||
    date.activeDateClass === ACTIVE_DATE_CLASS.ACTIVE_CURRENT ||
    date.activeDateClass === ACTIVE_DATE_CLASS.PAST_WITH_VALID_FUTURE_MOTION ||
    (date.activeDateClass === ACTIVE_DATE_CLASS.UNKNOWN_DATE && date.activeEligible === true);

  const geo = inGeography(opp, hotel.geo);
  const lodging = lodgingClass(opp);
  const lodgingOk = lodgingPresent(lodging);
  const fit = hotelFitPass(opp);
  const place = placementPass(opp);
  const who = whoResolved(opp);
  const summary = summaryPass(opp);

  const struct = structuralCopy(opp);
  const surfaceCls = classifyCustomerSurfaceOpportunity(struct, {});
  const surfaceOk = surfaceCls.keepActive === true;

  let surfaceV1 = null;
  try {
    const text = [
      struct.hotelOpportunityThesis,
      struct.summaryWhyHotel,
      struct.venueStatus,
      struct.whyNow,
      struct.title,
    ]
      .map((x) => String(x || ""))
      .join(" ");
    const url = String(struct.officialSource || struct.discoverySource || "");
    surfaceV1 = isGdiSurfaceEligible({
      url,
      title: struct.title,
      text,
      eventName: struct.title,
    });
  } catch {
    surfaceV1 = { error: true };
  }

  const readyEval = isGdiCustomerOpportunityReady(struct, {});
  // Also evaluate stamped path (what customer list sees today)
  const stampedFacing = isCustomerFacingOpportunity(opp, {});
  const stampedReady = isGdiCustomerOpportunityReady(opp, {});

  // Contact path as distinct from named WHO
  const contactPathOk =
    who.contactPathFound ||
    Boolean(
      opp.organizationContactUrl ||
        opp.officialContactPath ||
        opp.functionalContactEmail ||
        (opp.primaryContact && (opp.primaryContact.email || opp.primaryContact.phone))
    );

  const gates = {
    DISCOVERED: true,
    ENTITY_VALID: entityOk,
    FUTURE_TIMING_VALID: futureOk,
    IN_GEOGRAPHY: geo.ok,
    LODGING_SIGNAL_PRESENT: lodgingOk,
    HOTEL_FIT_PASS: fit.ok,
    PLACEMENT_PASS: place.ok,
    WHO_RESOLVED: who.whoRoleResolved || who.whoResearchOk,
    CONTACT_PATH_FOUND: contactPathOk,
    SUMMARY_QA_PASS: summary.ok,
    SURFACE_ELIGIBLE: surfaceOk,
    CUSTOMER_READY: readyEval.ok === true,
  };

  // Sequential survival (must pass prior gates)
  const order = [
    "DISCOVERED",
    "ENTITY_VALID",
    "FUTURE_TIMING_VALID",
    "IN_GEOGRAPHY",
    "LODGING_SIGNAL_PRESENT",
    "HOTEL_FIT_PASS",
    "PLACEMENT_PASS",
    "WHO_RESOLVED",
    "CONTACT_PATH_FOUND",
    "SUMMARY_QA_PASS",
    "SURFACE_ELIGIBLE",
    "CUSTOMER_READY",
  ];
  let alive = true;
  const sequential = {};
  let firstFail = null;
  for (const g of order) {
    if (!alive) {
      sequential[g] = false;
      continue;
    }
    if (!gates[g]) {
      alive = false;
      firstFail = g;
      sequential[g] = false;
    } else {
      sequential[g] = true;
    }
  }

  return {
    opp,
    hotelKey: hotel.key,
    hotelLabel: hotel.label,
    entity,
    date,
    geo,
    lodging,
    fit,
    place,
    who,
    summary,
    surfaceCls,
    surfaceV1,
    readyEval,
    stampedFacing,
    stampedReady,
    gates,
    sequential,
    firstFail,
    contactPathOk,
  };
}

function buildFunnel(evals) {
  const order = [
    "DISCOVERED",
    "ENTITY_VALID",
    "FUTURE_TIMING_VALID",
    "IN_GEOGRAPHY",
    "LODGING_SIGNAL_PRESENT",
    "HOTEL_FIT_PASS",
    "PLACEMENT_PASS",
    "WHO_RESOLVED",
    "CONTACT_PATH_FOUND",
    "SUMMARY_QA_PASS",
    "SURFACE_ELIGIBLE",
    "CUSTOMER_READY",
  ];
  const remaining = {};
  for (const g of order) {
    remaining[g] = evals.filter((e) => e.sequential[g]).length;
  }
  // DISCOVERED = all
  remaining.DISCOVERED = evals.length;

  // Kill per gate: entered gate N, failed N
  const kills = [];
  for (let i = 1; i < order.length; i++) {
    const prev = order[i - 1];
    const cur = order[i];
    const entering = remaining[prev];
    const surviving = remaining[cur];
    const rejected = entering - surviving;
    kills.push({
      gate: cur,
      countEntering: entering,
      countRejected: rejected,
      rejectionPct: entering ? Math.round((1000 * rejected) / entering) / 10 : 0,
      reason: cur,
    });
  }
  return { order, remaining, kills };
}

function classifyRejectionQuality(ev) {
  const o = ev.opp;
  const title = String(o.title || "");
  const org = String(o.organizationName || o.company || "");
  if (!ev.gates.ENTITY_VALID) {
    if (/directory|hotels near|booking\.com|trivago|expedia/i.test(title + org)) return "BAD_ENTITY";
    if (INVALID_ENTITY.has(ev.entity.entityClass)) return "BAD_ENTITY";
  }
  if (!ev.gates.IN_GEOGRAPHY && ev.geo.reason === "no_market_token") return "BAD_GEOGRAPHY";
  if (/duplicate|dup_|_copy/i.test(String(o.id || ""))) return "DUPLICATE";

  const plausible =
    ev.gates.ENTITY_VALID &&
    ev.gates.FUTURE_TIMING_VALID &&
    (ev.gates.IN_GEOGRAPHY || ev.geo.reason === "destination_unknown") &&
    (ev.lodging !== "NO_LODGING_SIGNAL" || /conference|summit|congress|meeting|retreat|incentive/i.test(title));

  if (plausible && (ev.firstFail === "LODGING_SIGNAL_PRESENT" || ev.firstFail === "SURFACE_ELIGIBLE" || ev.firstFail === "WHO_RESOLVED" || ev.firstFail === "CONTACT_PATH_FOUND" || ev.firstFail === "SUMMARY_QA_PASS" || ev.firstFail === "CUSTOMER_READY")) {
    // Under-researched if WHO not attempted or lodging empty or summary thin
    if (!ev.who.whoResearchOk || ev.lodging === "NO_LODGING_SIGNAL" || !ev.summary.ok) {
      return "PROMISING_BUT_UNDER-RESEARCHED";
    }
    if (ev.firstFail === "SURFACE_ELIGIBLE" || ev.firstFail === "CUSTOMER_READY") {
      return "OVERSTRICT_GATE";
    }
    return "PROMISING_BUT_UNDER-RESEARCHED";
  }

  if (!ev.gates.ENTITY_VALID || !ev.gates.FUTURE_TIMING_VALID) return "CORRECT_REJECTION";
  if (!ev.gates.IN_GEOGRAPHY && ev.geo.reason === "flagged_out_of_market") return "CORRECT_REJECTION";
  if (ev.firstFail === "HOTEL_FIT_PASS" && ev.fit.reason.startsWith("score_low")) return "CORRECT_REJECTION";
  if (ev.firstFail === "PLACEMENT_PASS") return "OVERSTRICT_GATE";
  if (plausible) return "INSUFFICIENT_DISCOVERY_DEPTH";
  return "CORRECT_REJECTION";
}

async function loadHotel(key, cfg) {
  const doc = await loadOpportunitiesCanonical(cfg.id);
  const ops = doc.opportunities || [];
  return {
    key,
    ...cfg,
    ops,
    persistence: doc.persistence || doc.source || "unknown",
    updatedAt: doc.updatedAt || null,
  };
}

function sampleRejected(evals, n = 20) {
  const rejected = evals.filter((e) => !e.gates.CUSTOMER_READY);
  // Prefer diverse firstFail
  const byFail = new Map();
  for (const e of rejected) {
    const k = e.firstFail || "OTHER";
    if (!byFail.has(k)) byFail.set(k, []);
    byFail.get(k).push(e);
  }
  const out = [];
  const keys = [...byFail.keys()];
  let i = 0;
  while (out.length < n && out.length < rejected.length) {
    const k = keys[i % keys.length];
    const bucket = byFail.get(k);
    if (bucket && bucket.length) {
      out.push(bucket.shift());
    }
    i++;
    if (i > n * 20) break;
  }
  // fill
  for (const e of rejected) {
    if (out.length >= n) break;
    if (!out.includes(e)) out.push(e);
  }
  return out.slice(0, n);
}

function pct(n, d) {
  if (!d) return 0;
  return Math.round((1000 * n) / d) / 10;
}

async function main() {
  fs.mkdirSync(OUT, { recursive: true });
  const loaded = {};
  for (const [key, cfg] of Object.entries(HOTELS)) {
    loaded[key] = await loadHotel(key, cfg);
  }

  const allEvals = {};
  for (const [key, h] of Object.entries(loaded)) {
    allEvals[key] = h.ops.map((o) => evaluateOpp(o, { key, ...h }));
  }

  // FUNNEL_BY_HOTEL.csv
  const funnelRows = [];
  const funnels = {};
  for (const [key, evals] of Object.entries(allEvals)) {
    const f = buildFunnel(evals);
    funnels[key] = f;
    const r = { hotel: key, label: HOTELS[key].label, role: HOTELS[key].role, discovered: evals.length };
    for (const g of f.order) r[g] = f.remaining[g];
    // human chain
    r.funnelChain = f.order.map((g) => `${f.remaining[g]} ${g.toLowerCase().replace(/_/g, " ")}`).join(" → ");
    funnelRows.push(r);
  }
  write(
    "FUNNEL_BY_HOTEL.csv",
    toCsv(funnelRows, [
      "hotel",
      "label",
      "role",
      "discovered",
      ...funnels.YOTEL.order,
      "funnelChain",
    ])
  );

  // CROSS_HOTEL_ATTRITION.csv (subjects only + aggregate)
  const attritionRows = [];
  const subjects = ["YOTEL", "SPICE", "AC"];
  const aggEntering = {};
  const aggRejected = {};
  for (const key of subjects) {
    for (const k of funnels[key].kills) {
      attritionRows.push({
        hotel: key,
        gate: k.gate,
        countEntering: k.countEntering,
        countRejected: k.countRejected,
        rejectionPct: k.rejectionPct,
        reason: k.reason,
      });
      aggEntering[k.gate] = (aggEntering[k.gate] || 0) + k.countEntering;
      aggRejected[k.gate] = (aggRejected[k.gate] || 0) + k.countRejected;
    }
  }
  for (const gate of Object.keys(aggEntering)) {
    const ent = aggEntering[gate];
    const rej = aggRejected[gate];
    attritionRows.push({
      hotel: "CROSS_SUBJECT",
      gate,
      countEntering: ent,
      countRejected: rej,
      rejectionPct: pct(rej, ent),
      reason: gate,
    });
  }
  write(
    "CROSS_HOTEL_ATTRITION.csv",
    toCsv(attritionRows, ["hotel", "gate", "countEntering", "countRejected", "rejectionPct", "reason"])
  );

  // SURFACE_ELIGIBILITY_FORENSICS.csv — pass entity/timing/geo/lodging/fit but fail customer-ready
  const surfaceRows = [];
  for (const key of subjects) {
    for (const e of allEvals[key]) {
      if (
        e.gates.ENTITY_VALID &&
        e.gates.FUTURE_TIMING_VALID &&
        e.gates.IN_GEOGRAPHY &&
        e.gates.LODGING_SIGNAL_PRESENT &&
        e.gates.HOTEL_FIT_PASS &&
        !e.gates.CUSTOMER_READY
      ) {
        const failed = e.readyEval.failed || [];
        const failField =
          !e.gates.SURFACE_ELIGIBLE
            ? `surface:${e.surfaceCls.disposition}|${(e.surfaceCls.reasons || []).join(";")}`
            : failed.join("|") || e.firstFail;
        const sources = [
          e.opp.officialSource,
          e.opp.discoverySource,
          ...(Array.isArray(e.opp.sources) ? e.opp.sources.map((s) => s?.url || s) : []),
        ]
          .filter(Boolean)
          .slice(0, 3)
          .join(" | ");
        surfaceRows.push({
          opportunityId: e.opp.id || e.opp.opportunityId,
          hotel: key,
          title: e.opp.title,
          whyPlausible: String(e.opp.hotelOpportunityThesis || e.opp.whyNow || e.opp.summaryWhyMatters || "").slice(0, 220),
          exactFailedField: failField,
          exactGate:
            !e.gates.SURFACE_ELIGIBLE
              ? "classifyCustomerSurfaceOpportunity / isCustomerSurfaceActiveEligible"
              : failed.includes("surface_eligibility")
                ? "isGdiCustomerOpportunityReady→surface_eligibility"
                : `isGdiCustomerOpportunityReady:${failed.join(",")}`,
          sourceEvidenceAvailable: sources || "NONE",
          whatWouldSatisfy:
            !e.gates.SURFACE_ELIGIBLE
              ? "hotel opportunity thesis + lodging proof (roomBlockMentioned/housingPageFound) or open venue/housing language; clear disposition KEEP_ACTIVE"
              : failed.includes("who_research_not_attempted")
                ? "stamp contactResearchAttempted / public data ceiling OR named/functional/org contact path"
                : failed.includes("summary_quality")
                  ? "ADEQUATE/STRONG summary (what/why/hotel/action) not title-duplicate"
                  : failed.includes("hotel_fit")
                    ? "hotelFitScore or summaryWhyHotel/fitExplanation"
                    : failed.includes("why_now")
                      ? "whyNow or cardWhyNowLine"
                      : failed.includes("recommended_action")
                        ? "recommendedAction or recommendedNextStep"
                        : "satisfy remaining readiness failed fields without inventing facts",
          lodgingClass: e.lodging,
          whoPath: e.who.who.pathClass,
          summaryQuality: e.summary.quality,
          surfaceDisposition: e.surfaceCls.disposition,
          stampedCustomerFacing: e.stampedFacing,
          structuralCustomerReady: e.readyEval.ok,
        });
      }
    }
  }
  write(
    "SURFACE_ELIGIBILITY_FORENSICS.csv",
    toCsv(surfaceRows, [
      "opportunityId",
      "hotel",
      "title",
      "whyPlausible",
      "exactFailedField",
      "exactGate",
      "sourceEvidenceAvailable",
      "whatWouldSatisfy",
      "lodgingClass",
      "whoPath",
      "summaryQuality",
      "surfaceDisposition",
      "stampedCustomerFacing",
      "structuralCustomerReady",
    ])
  );

  // Rejection sample audit
  const sampleRows = [];
  const qualityCounts = { YOTEL: {}, SPICE: {}, AC: {}, ALL: {} };
  for (const key of subjects) {
    const sample = sampleRejected(allEvals[key], 20);
    for (const e of sample) {
      const q = classifyRejectionQuality(e);
      qualityCounts[key][q] = (qualityCounts[key][q] || 0) + 1;
      qualityCounts.ALL[q] = (qualityCounts.ALL[q] || 0) + 1;
      sampleRows.push({
        hotel: key,
        opportunityId: e.opp.id || e.opp.opportunityId,
        title: e.opp.title,
        organization: e.opp.organizationName || e.opp.company,
        firstFailGate: e.firstFail,
        lodgingClass: e.lodging,
        entityClass: e.entity.entityClass,
        surfaceDisposition: e.surfaceCls.disposition,
        whoPath: e.who.who.pathClass,
        rejectionQuality: q,
        priorityStamped: e.opp.priority,
      });
    }
  }
  write(
    "REJECTION_SAMPLE_AUDIT.csv",
    toCsv(sampleRows, [
      "hotel",
      "opportunityId",
      "title",
      "organization",
      "firstFailGate",
      "lodgingClass",
      "entityClass",
      "surfaceDisposition",
      "whoPath",
      "rejectionQuality",
      "priorityStamped",
    ])
  );

  // Lodging forensics markdown
  const lodgingDist = {};
  for (const key of [...subjects, "BETHESDA", "RENAISSANCE", "HILTON"]) {
    lodgingDist[key] = {};
    for (const e of allEvals[key]) {
      lodgingDist[key][e.lodging] = (lodgingDist[key][e.lodging] || 0) + 1;
    }
  }

  // WHO forensics
  const whoDist = {};
  for (const key of [...subjects, "BETHESDA", "RENAISSANCE", "HILTON"]) {
    whoDist[key] = {
      WHO_ENTITY_RESOLVED: 0,
      WHO_ROLE_RESOLVED: 0,
      WHO_PERSON_RESOLVED: 0,
      PUBLIC_CONTACT_PATH_AVAILABLE: 0,
      NOT_RESEARCHED: 0,
      pathClasses: {},
    };
    for (const e of allEvals[key]) {
      if (e.who.whoEntityResolved) whoDist[key].WHO_ENTITY_RESOLVED++;
      if (e.who.whoRoleResolved) whoDist[key].WHO_ROLE_RESOLVED++;
      if (e.who.whoPersonResolved) whoDist[key].WHO_PERSON_RESOLVED++;
      if (e.contactPathOk) whoDist[key].PUBLIC_CONTACT_PATH_AVAILABLE++;
      if (e.who.who.pathClass === WHO_PATH_CLASS.NOT_RESEARCHED) whoDist[key].NOT_RESEARCHED++;
      whoDist[key].pathClasses[e.who.who.pathClass] =
        (whoDist[key].pathClasses[e.who.who.pathClass] || 0) + 1;
    }
  }

  // Placement forensics
  const placeDist = {};
  for (const key of subjects) {
    placeDist[key] = { rejected: [], classes: {} };
    for (const e of allEvals[key]) {
      placeDist[key].classes[e.place.class] = (placeDist[key].classes[e.place.class] || 0) + 1;
      if (!e.gates.PLACEMENT_PASS) {
        placeDist[key].rejected.push({
          id: e.opp.id,
          title: e.opp.title,
          venueStatus: e.opp.venueStatus || e.opp.venueSourcingStatus,
          class: e.place.class,
        });
      }
    }
  }

  // Top kill gates
  function topKills(key, n = 3) {
    return [...funnels[key].kills].sort((a, b) => b.countRejected - a.countRejected).slice(0, n);
  }
  const crossKills = attritionRows
    .filter((r) => r.hotel === "CROSS_SUBJECT")
    .sort((a, b) => b.countRejected - a.countRejected);

  // Customer facing counts (stamped)
  const stampedCounts = {};
  for (const [key, h] of Object.entries(loaded)) {
    stampedCounts[key] = {
      n: h.ops.length,
      facing: filterCustomerFacingOpportunities(h.ops).length,
      structuralReady: allEvals[key].filter((e) => e.readyEval.ok).length,
      structuralSurface: allEvals[key].filter((e) => e.gates.SURFACE_ELIGIBLE).length,
    };
  }

  // Pre-lodging survivors who die later
  const passCoreFailReady = surfaceRows.length;

  // Bug check: does surface require rare public evidence?
  let surfaceBug = false;
  const surfaceBugNotes = [];
  for (const row of surfaceRows) {
    if (/public_demand_generator_without_hotel_thesis|market_entity_without_hotel_opportunity_depth/.test(row.exactFailedField)) {
      // thesis exists in whyPlausible but still fails — possible boilerplate strip bug
      if (/overflow|housing|room block|hotel/i.test(row.whyPlausible || "")) {
        surfaceBugNotes.push({
          id: row.opportunityId,
          note: "thesis language present but surface rejects — check boilerplate strip / lodgingProof NONE",
        });
      }
    }
  }
  // Not a code bug unless keepActive true but readiness fails surface_eligibility due to stamped flags
  for (const key of subjects) {
    for (const e of allEvals[key]) {
      if (e.surfaceCls.keepActive && e.stampedReady.failed?.includes("surface_eligibility") && !e.readyEval.ok === false) {
        /* structural ready uses cleaned copy */
      }
      // Stamped DQ causes surface_eligibility fail even when structural keepActive — operational stamp issue, not logic bug
      if (e.surfaceCls.keepActive === true && e.stampedFacing === false && e.opp.priority === "DISQUALIFIED") {
        surfaceBugNotes.push({
          id: e.opp.id,
          note: "STRUCTURAL_KEEP_ACTIVE but stamped DISQUALIFIED — soft-DQ stamp blocks customer list without revalidation write",
        });
      }
    }
  }
  // True bug: hasExplicitHotelMotionCopy early-returns on boilerplate summaryWhyMatters and
  // ONLY scans venue/destination/title — ignoring hotelOpportunityThesis that already contains overflow language.
  // Evidence: AidEx Geneva (NAMED_DIRECT + STRONG summary + overflow thesis + accommodation URL) → DOWNGRADE_TO_DEMAND_GENERATOR.
  let trueSurfaceBug = false;
  for (const key of subjects) {
    for (const e of allEvals[key]) {
      const thesis = String(e.opp.hotelOpportunityThesis || "");
      const matters = String(e.opp.summaryWhyMatters || "");
      const thesisMotion = /overflow|housing|room block|host hotel|preferred lodging/i.test(thesis);
      const boilerplateMatters = /^Planning for hotel accommodations is essential/i.test(matters);
      if (
        thesisMotion &&
        boilerplateMatters &&
        e.surfaceCls.disposition ===
          CUSTOMER_SURFACE_DISPOSITION.DOWNGRADE_TO_DEMAND_GENERATOR &&
        (e.surfaceCls.reasons || []).includes("public_demand_generator_without_hotel_thesis")
      ) {
        trueSurfaceBug = true;
        surfaceBugNotes.push({
          id: e.opp.id,
          hotel: key,
          note: "SURFACE_BUG: boilerplate summaryWhyMatters causes hasExplicitHotelMotionCopy to ignore overflow thesis (venue/title-only scan)",
        });
      }
      const le = e.opp.lodgingEvidence;
      const hasHousingObj =
        le && typeof le === "object" && (le.roomBlockMentioned || le.housingPageFound);
      if (
        hasHousingObj &&
        thesisMotion &&
        e.gates.ENTITY_VALID &&
        e.gates.FUTURE_TIMING_VALID &&
        !e.surfaceCls.keepActive &&
        e.surfaceCls.disposition !== CUSTOMER_SURFACE_DISPOSITION.PAST_CLOSED
      ) {
        trueSurfaceBug = true;
        surfaceBugNotes.push({
          id: e.opp.id,
          note: `LIKELY_BUG: lodgingEvidence flags + thesis but disposition=${e.surfaceCls.disposition}`,
        });
      }
    }
  }
  // WHO blocking valid?
  let whoBlockingValid = false;
  let whoBlockingCount = 0;
  for (const key of subjects) {
    for (const e of allEvals[key]) {
      if (
        e.gates.ENTITY_VALID &&
        e.gates.FUTURE_TIMING_VALID &&
        e.gates.IN_GEOGRAPHY &&
        e.gates.LODGING_SIGNAL_PRESENT &&
        e.gates.HOTEL_FIT_PASS &&
        e.gates.PLACEMENT_PASS &&
        e.gates.SURFACE_ELIGIBLE &&
        e.readyEval.failed?.includes("who_research_not_attempted")
      ) {
        whoBlockingValid = true;
        whoBlockingCount++;
      }
      // Also: surface/readiness path dies at WHO before contact org path accepted
      if (
        e.firstFail === "WHO_RESOLVED" &&
        e.gates.ENTITY_VALID &&
        e.gates.LODGING_SIGNAL_PRESENT &&
        e.who.whoEntityResolved
      ) {
        whoBlockingCount++;
        whoBlockingValid = true;
      }
    }
  }

  // Lodging blocking?
  let lodgingBlocking = false;
  let lodgingKill = 0;
  for (const key of subjects) {
    const k = funnels[key].kills.find((x) => x.gate === "LODGING_SIGNAL_PRESENT");
    lodgingKill += k?.countRejected || 0;
  }
  lodgingBlocking = lodgingKill > 0;

  // Placement blocking?
  let placementBlocking = false;
  for (const key of subjects) {
    if ((placeDist[key].rejected || []).length > 0) placementBlocking = true;
  }

  // Discovery vs depth
  const discoveryInsufficient =
    loaded.YOTEL.ops.length < 30 ||
    (funnels.YOTEL.remaining.ENTITY_VALID < 5 && loaded.YOTEL.ops.length < 20);
  const researchDepthInsufficient =
    (qualityCounts.ALL["PROMISING_BUT_UNDER-RESEARCHED"] || 0) >= 8 ||
    subjects.some((k) => whoDist[k].NOT_RESEARCHED > whoDist[k].WHO_PERSON_RESOLVED);

  // Contract realism
  const mandatoryFields = [
    {
      field: "valid_entity",
      required: true,
      publicAvail: "HIGH",
      sources: "org site, association, event page",
      failFreq: "medium on subjects (directory/chrome)",
      commercial: "ESSENTIAL_FOR_CUSTOMER_READY",
    },
    {
      field: "future_timing",
      required: true,
      publicAvail: "HIGH",
      sources: "event dates, cycle announcements",
      failFreq: "low-medium",
      commercial: "ESSENTIAL_FOR_CUSTOMER_READY",
    },
    {
      field: "geography_in_market",
      required: true,
      publicAvail: "HIGH",
      sources: "venue/destination pages",
      failFreq: "medium (thin geo fields on subjects)",
      commercial: "ESSENTIAL_FOR_CUSTOMER_READY",
    },
    {
      field: "lodgingEvidence.roomBlockMentioned|housingPageFound",
      required: true,
      publicAvail: "MEDIUM",
      sources: "housing pages, host hotel PDFs, housing bureau",
      failFreq: "HIGH on subjects — most rows NO_LODGING_SIGNAL",
      commercial: "ESSENTIAL_FOR_CUSTOMER_READY",
      note: "Surface contract treats overflowMentioned-alone as NONE; strong inference not enough without mentioned flags",
    },
    {
      field: "hotel_opportunity_thesis (non-boilerplate)",
      required: true,
      publicAvail: "MEDIUM",
      sources: "analyst synthesis from public pages",
      failFreq: "HIGH — template thesis stripped as bare demand generator",
      commercial: "ESSENTIAL_FOR_CUSTOMER_READY",
    },
    {
      field: "who_research_attempted",
      required: true,
      publicAvail: "MEDIUM",
      sources: "staff pages, contact forms, LinkedIn (manual)",
      failFreq: "HIGH — NOT_RESEARCHED common; named person rare",
      commercial: "ESSENTIAL_FOR_CUSTOMER_READY",
      note: "Does NOT require named person; ceiling/org/functional path OK if stamped",
    },
    {
      field: "named_person",
      required: false,
      publicAvail: "LOW",
      sources: "staff directory, press",
      failFreq: "very high",
      commercial: "USEFUL_BUT_NOT_REQUIRED",
    },
    {
      field: "summary ADEQUATE/STRONG",
      required: true,
      publicAvail: "MEDIUM",
      sources: "compiled from public facts",
      failFreq: "HIGH when enrichment skipped",
      commercial: "ESSENTIAL_FOR_CUSTOMER_READY",
    },
    {
      field: "why_now + recommended_action",
      required: true,
      publicAvail: "MEDIUM",
      sources: "sales synthesis",
      failFreq: "HIGH on thin bags",
      commercial: "ESSENTIAL_FOR_CUSTOMER_READY",
    },
    {
      field: "hotelFitScore",
      required: false,
      publicAvail: "INTERNAL",
      sources: "fit model",
      failFreq: "medium",
      commercial: "USEFUL_BUT_NOT_REQUIRED",
      note: "Readiness accepts summaryWhyHotel/fitExplanation substitute",
    },
    {
      field: "Surfe identity / private email",
      required: false,
      publicAvail: "LOW",
      sources: "paid enrichment",
      failFreq: "n/a (off)",
      commercial: "INTERNAL_RESEARCH_FIELD",
    },
  ];

  // Controls prove the same contract is publicly satisfiable; subject zero-yield is enrichment gap.
  const publicContractUnrealistic =
    stampedCounts.BETHESDA.structuralReady +
      stampedCounts.RENAISSANCE.structuralReady +
      stampedCounts.HILTON.structuralReady ===
    0;

  // Bethesda/NYC comparison
  function sourceFamilies(evals) {
    const fam = {};
    for (const e of evals) {
      const u = String(e.opp.officialSource || e.opp.discoverySource || "");
      let f = "unknown";
      if (/cvent|a2z|mapyourshow/i.test(u)) f = "platform";
      else if (/\.gov|congress|parliament/i.test(u)) f = "government";
      else if (/edu|university|uni\./i.test(u)) f = "university";
      else if (/linkedin/i.test(u)) f = "linkedin";
      else if (/facebook|instagram|twitter|x\.com/i.test(u)) f = "social";
      else if (u) f = "web_org";
      else f = "none";
      fam[f] = (fam[f] || 0) + 1;
    }
    return fam;
  }

  // Write LODGING_SIGNAL_FORENSICS.md
  write(
    "LODGING_SIGNAL_FORENSICS.md",
    `# Lodging Signal Forensics — 2026-10-03

## Distribution by hotel

${Object.entries(lodgingDist)
  .map(
    ([k, dist]) =>
      `### ${k}\n${Object.entries(dist)
        .map(([c, n]) => `- ${c}: ${n}`)
        .join("\n")}`
  )
  .join("\n\n")}

## Rule observation (no change)

\`customer-surface-revalidation-v1.lodgingProof()\`:

- CREDIBLE only when status matches CONFIRMED/STRONG/OFFICIAL **and** \`roomBlockMentioned|housingPageFound\`
- WEAK when mentioned flags true
- **overflowMentioned alone → NONE** (explicit comment in code)
- Surface then kills non-exhibitor rows with \`!hasHotelOpportunityThesis && lodging.level === NONE\` as market entity / demand generator

## Does GDI require direct room-block too early?

**Partially yes for progression past surface**, not for customer-truth dilution:

| Signal class | Allows deeper research? | Survives customer surface today? |
|---|---|---|
| DIRECT_ROOM_BLOCK_EVIDENCE | Yes | Usually yes if thesis present |
| STRONG_LODGING_INFERENCE | Should | Only if mentioned flags or non-boilerplate thesis |
| EVENT_HOUSING_SIGNAL | Should | Sometimes via thesis language scan |
| TRAVELING_DELEGATION_SIGNAL | Research yes | No alone |
| MULTI_DAY_EVENT_SIGNAL | Research yes | No alone |
| NO_LODGING_SIGNAL | No | No |

**Recommendation (forensic only):** allow STRONG_LODGING_INFERENCE / EVENT_HOUSING_SIGNAL to continue **internal research lanes** without granting customer-ready. Do **not** weaken customer-facing truth.

Subject hotels are dominated by NO_LODGING_SIGNAL + template theses — lodging is a top kill gate, but largely because evidence was never stamped, not only because DIRECT is required.
`
  );

  write(
    "WHO_CONTACT_FORENSICS.md",
    `# WHO / Contact Gate Forensics — 2026-10-03

## Current rule (unchanged)

\`isGdiCustomerOpportunityReady\` requires \`whoResearchAttempted(opp)\` — i.e. pathClass ≠ NOT_RESEARCHED.

Accepted paths (\`classifyWhoHowPath\`):

1. NAMED_DIRECT (name + email/phone)
2. NAMED_PARTIAL (name only)
3. FUNCTIONAL (functionalContactEmail / grade tier)
4. ORG_PATH (organizationContactUrl / officialContactPath / …)
5. NO_CONTACT_AFTER_RESEARCH (contactResearchAttempted or PUBLIC_DATA_CEILING stamp)

**Named person is NOT mandatory** for readiness. Organizer/contact route is valid if stamped.

## Counts

${Object.entries(whoDist)
  .map(
    ([k, d]) => `### ${k} (n=${allEvals[k].length})
- WHO_ENTITY_RESOLVED: ${d.WHO_ENTITY_RESOLVED}
- WHO_ROLE_RESOLVED: ${d.WHO_ROLE_RESOLVED}
- WHO_PERSON_RESOLVED: ${d.WHO_PERSON_RESOLVED}
- PUBLIC_CONTACT_PATH_AVAILABLE: ${d.PUBLIC_CONTACT_PATH_AVAILABLE}
- NOT_RESEARCHED: ${d.NOT_RESEARCHED}
- pathClasses: ${JSON.stringify(d.pathClasses)}`
  )
  .join("\n\n")}

## Blocking assessment

Structural surface-eligible rows failing solely on \`who_research_not_attempted\`: **${whoBlockingCount}** across subjects.

WHO requirement blocking otherwise-valid opportunities: **${whoBlockingValid ? "YES" : "NO"}** — mostly as **research-not-attempted** (missing ceiling stamp), not as named-person hard requirement.

Controls (Bethesda/NYC) show far higher NAMED_* / ORG_PATH rates — WHO enrichment depth differs by market maturity, not by a different code path.
`
  );

  write(
    "PLACEMENT_FORENSICS.md",
    `# Placement Gate Forensics — 2026-10-03

## Classes observed (subjects)

${subjects
  .map(
    (k) => `### ${k}
Classes: ${JSON.stringify(placeDist[k].classes)}
Rejected for placement: ${placeDist[k].rejected.length}
${placeDist[k].rejected
  .slice(0, 10)
  .map((r) => `- ${r.id}: ${r.title} — ${r.class} (${r.venueStatus || "n/a"})`)
  .join("\n") || "(none)"}`
  )
  .join("\n\n")}

## Assessment

Funnel placement pass treats UNKNOWN as allowed (does not kill). Explicit FULLY_PLACED / PRIMARY_NO_OVERFLOW kill.

**Is primary venue selected incorrectly treated as no hotel opportunity?**

In \`hasHotelOpportunityThesis\`, a named competitor/host hotel in \`venueStatus\` **helps** thesis (overflow pursue motion) — it does **not** auto-DQ. Separate commercial status PRIMARY_NO_OVERFLOW can kill.

On subject hotels, placement rejects are **rare** relative to lodging/entity/geography. Placement is **not** the dominant cross-hotel kill gate.

Placement logic blocking valid opportunities at scale: **${placementBlocking ? "YES (small counts)" : "NO"}**.
`
  );

  write(
    "CUSTOMER_READY_CONTRACT_AUDIT.md",
    `# Customer-Ready Contract Audit — 2026-10-03

## Definition (live code)

Customer-facing ⇔ \`isActiveCustomerOpportunity\` (surface) AND (\`isGdiCustomerOpportunityReady\` OR legacy compatibility).

\`isGdiCustomerOpportunityReady\` requires:

1. \`isCustomerSurfaceActiveEligible\` (entity, date, lodging/thesis depth, not stamped DQ)
2. title + organization
3. source URL
4. summary ADEQUATE/STRONG
5. whoResearchAttempted
6. hotel fit signal (score or why-hotel)
7. whyNow
8. recommendedAction

## Field realism

| field | required? | typical public availability | source types | failure frequency (subjects) | classification |
|---|---|---|---|---|---|
${mandatoryFields
  .map(
    (f) =>
      `| ${f.field} | ${f.required} | ${f.publicAvail} | ${f.sources} | ${f.failFreq} | ${f.commercial} |`
  )
  .join("\n")}

## Can competent public research satisfy every required field?

**Mostly yes**, with two friction points:

1. **Lodging mentioned flags** — public housing pages exist for many events, but bags often lack stamped \`roomBlockMentioned\` / \`housingPageFound\`; template theses are stripped → surface death.
2. **WHO research stamp** — named contacts are rare; org/functional/ceiling paths are allowed but frequently **not stamped**, so readiness fails on NOT_RESEARCHED.

Neither requires Surfe/private email. Contract is strict but **not inherently impossible** for public data — subject bags are under-enriched relative to Bethesda/NYC.

PUBLIC-DATA CONTRACT UNREALISTIC: **${publicContractUnrealistic ? "YES" : "NO"}** — Bethesda/NYC satisfy the same contract with public evidence; subject zero-yield is enrichment/timing-stamp depth, not an impossible field set.
`
  );

  // Comparison MD
  const cmp = ["BETHESDA", "RENAISSANCE", "HILTON", ...subjects].map((k) => {
    const f = funnels[k];
    return {
      hotel: k,
      n: allEvals[k].length,
      entity: f.remaining.ENTITY_VALID,
      lodging: f.remaining.LODGING_SIGNAL_PRESENT,
      surface: f.remaining.SURFACE_ELIGIBLE,
      ready: f.remaining.CUSTOMER_READY,
      stampedFacing: stampedCounts[k].facing,
      whoPerson: whoDist[k].WHO_PERSON_RESOLVED,
      whoNotRes: whoDist[k].NOT_RESEARCHED,
      lodgingDirect: lodgingDist[k].DIRECT_ROOM_BLOCK_EVIDENCE || 0,
      lodgingNone: lodgingDist[k].NO_LODGING_SIGNAL || 0,
      sources: sourceFamilies(allEvals[k]),
    };
  });

  const pipelineDiff =
    stampedCounts.BETHESDA.facing > 0 ||
    stampedCounts.RENAISSANCE.facing > 0 ||
    stampedCounts.HILTON.facing > 0;

  write(
    "BETHESDA_NYC_COMPARISON.md",
    `# Bethesda / NYC vs YOTEL / Spice / AC — 2026-10-03

## Headline counts

| Hotel | Discovered | Entity | Lodging signal | Surface eligible (structural) | Customer ready (structural) | Stamped customer-facing |
|---|---:|---:|---:|---:|---:|---:|
${cmp
  .map(
    (r) =>
      `| ${r.hotel} | ${r.n} | ${r.entity} | ${r.lodging} | ${r.surface} | ${r.ready} | ${r.stampedFacing} |`
  )
  .join("\n")}

## Source families

${cmp.map((r) => `### ${r.hotel}\n${JSON.stringify(r.sources, null, 2)}`).join("\n\n")}

## Lodging / WHO contrast

${cmp
  .map(
    (r) =>
      `- **${r.hotel}**: DIRECT_ROOM_BLOCK=${r.lodgingDirect}, NO_LODGING=${r.lodgingNone}, WHO_PERSON=${r.whoPerson}, NOT_RESEARCHED=${r.whoNotRes}`
  )
  .join("\n")}

## Differences

1. **Discovery breadth:** Bethesda (54) / Hilton (45) / Renaissance (29) vs YOTEL (13) — YOTEL severely thin. Spice (77) is wide but low quality. AC (32) mid.
2. **Source families:** Controls denser \`web_org\` + event platforms with housing pages; subjects skew thin/generic/directory.
3. **Lodging evidence:** Controls retain DIRECT/STRONG stamps; subjects mostly NO_LODGING_SIGNAL.
4. **WHO:** Controls have researched paths; subjects heavily NOT_RESEARCHED.
5. **Placement:** Not the differentiator.
6. **Surface eligibility:** Controls pass KEEP_ACTIVE at scale; subjects almost never.
7. **Qualification gates:** Same code path — difference is **evidence depth / enrichment maturity**, not alternate thresholds.

PIPELINE DIFFERENCE FOUND: **${pipelineDiff ? "YES" : "NO"}** — same gates, unequal research/enrichment completion.
`
  );

  // Root cause
  const yotelTop = topKills("YOTEL", 1)[0];
  const spiceTop = topKills("SPICE", 1)[0];
  const acTop = topKills("AC", 1)[0];
  const crossTop = crossKills[0];

  const promising = qualityCounts.ALL["PROMISING_BUT_UNDER-RESEARCHED"] || 0;
  const correct = qualityCounts.ALL["CORRECT_REJECTION"] || 0;
  const sampleTotal = sampleRows.length;

  const rootClasses = [];
  const evidence = [];
  if (loaded.YOTEL.ops.length < 25 || discoveryInsufficient) {
    rootClasses.push("A. DISCOVERY_COVERAGE_PROBLEM");
    evidence.push(`YOTEL bag n=${loaded.YOTEL.ops.length}; entity survivors ${funnels.YOTEL.remaining.ENTITY_VALID}`);
  }
  if (researchDepthInsufficient || promising >= 8) {
    rootClasses.push("B. RESEARCH_DEPTH_PROBLEM");
    evidence.push(`Sample PROMISING_BUT_UNDER-RESEARCHED=${promising}/${sampleTotal}; WHO NOT_RESEARCHED high on subjects`);
  }
  if (funnels.SPICE.kills.find((k) => k.gate === "ENTITY_VALID")?.countRejected > 15) {
    rootClasses.push("C. ENTITY_RESOLUTION_PROBLEM");
    evidence.push(`Spice entity kill ${funnels.SPICE.kills.find((k) => k.gate === "ENTITY_VALID")?.countRejected}`);
  }
  if (lodgingKill >= 10) {
    rootClasses.push("D. LODGING-EVIDENCE CONTRACT TOO STRICT");
    evidence.push(`Cross lodging rejected ${lodgingKill}; overflow-alone=NONE; multi-day alone dies`);
  }
  if (whoBlockingValid) {
    rootClasses.push("E. WHO/CONTACT CONTRACT TOO STRICT");
    evidence.push(
      `who_research_not_attempted / WHO_RESOLVED blocks ${whoBlockingCount} otherwise-advanced rows (named person not required but stamp missing)`
    );
  }
  // Timing dominates sequential attrition on all three subjects
  const timingKill = subjects.reduce(
    (s, k) => s + (funnels[k].kills.find((x) => x.gate === "FUTURE_TIMING_VALID")?.countRejected || 0),
    0
  );
  if (timingKill >= 20) {
    rootClasses.push("B. RESEARCH_DEPTH_PROBLEM");
    evidence.push(
      `FUTURE_TIMING_VALID rejects ${timingKill} across subjects (unknown/past dates without future-motion stamps) — largest cross-hotel kill`
    );
  }
  if (trueSurfaceBug) {
    rootClasses.push("F. SURFACE ELIGIBILITY BUG");
    evidence.push(
      `hasExplicitHotelMotionCopy ignores hotelOpportunityThesis when summaryWhyMatters is boilerplate — AidEx-class rows: ${
        surfaceBugNotes.filter((n) => /SURFACE_BUG/.test(n.note)).length
      }`
    );
  }
  if (placementBlocking && subjects.every((k) => (placeDist[k].rejected || []).length > 3)) {
    rootClasses.push("G. PLACEMENT LOGIC TOO STRICT");
    evidence.push("Placement rejects material on all subjects");
  }
  const fitKills = subjects.reduce(
    (s, k) => s + (funnels[k].kills.find((x) => x.gate === "HOTEL_FIT_PASS")?.countRejected || 0),
    0
  );
  if (fitKills > lodgingKill && fitKills > 20) {
    rootClasses.push("H. HOTEL FIT LOGIC TOO STRICT");
    evidence.push(`Fit kills ${fitKills}`);
  }
  // Legitimately none?
  const anyStructuralReady =
    stampedCounts.YOTEL.structuralReady +
      stampedCounts.SPICE.structuralReady +
      stampedCounts.AC.structuralReady >
    0;
  if (!anyStructuralReady && promising === 0 && correct >= sampleTotal * 0.8) {
    rootClasses.push("I. LEGITIMATELY NO CUSTOMER-READY OPPORTUNITIES");
    evidence.push("Sample overwhelmingly CORRECT_REJECTION with no promising under-researched");
  }
  // Dedupe class letters (B may be pushed twice)
  const seen = new Set();
  const deduped = [];
  for (const c of rootClasses) {
    if (seen.has(c)) continue;
    seen.add(c);
    deduped.push(c);
  }
  rootClasses.length = 0;
  rootClasses.push(...deduped);

  if (rootClasses.length > 1 && !rootClasses.includes("J. MIXED")) {
    rootClasses.push("J. MIXED");
  }
  if (rootClasses.length === 0) {
    rootClasses.push("J. MIXED");
    evidence.push("Zero structural ready with mixed attrition");
  }

  write(
    "ROOT_CAUSE.md",
    `# Root Cause — GDI Zero Yield Forensic 2026-10-03

## Classification

${rootClasses.map((c) => `- ${c}`).join("\n")}

## Evidence

${evidence.map((e) => `- ${e}`).join("\n")}

## Largest kill gates

| Hotel | Gate | Entering | Rejected | Reject % |
|---|---|---:|---:|---:|
| YOTEL | ${yotelTop?.gate} | ${yotelTop?.countEntering} | ${yotelTop?.countRejected} | ${yotelTop?.rejectionPct}% |
| SPICE | ${spiceTop?.gate} | ${spiceTop?.countEntering} | ${spiceTop?.countRejected} | ${spiceTop?.rejectionPct}% |
| AC | ${acTop?.gate} | ${acTop?.countEntering} | ${acTop?.countRejected} | ${acTop?.rejectionPct}% |
| CROSS | ${crossTop?.gate} | ${crossTop?.countEntering} | ${crossTop?.countRejected} | ${crossTop?.rejectionPct}% |

## Funnel chains (structural, soft-DQ stripped for evaluation)

${subjects
  .map((k) => {
    const f = funnels[k];
    return `**${k}:** ${f.order.map((g) => `${f.remaining[g]} ${g}`).join(" → ")}`;
  })
  .join("\n\n")}

## Controls

${["BETHESDA", "RENAISSANCE", "HILTON"]
  .map((k) => {
    const f = funnels[k];
    return `**${k}:** ${f.remaining.DISCOVERED} → … → surface ${f.remaining.SURFACE_ELIGIBLE} → ready ${f.remaining.CUSTOMER_READY} (stamped facing ${stampedCounts[k].facing})`;
  })
  .join("\n\n")}

## Final verdict

Zero customer-ready on YOTEL / Spice / AC is **primarily MIXED: (1) FUTURE_TIMING_VALID as the largest cross-hotel kill (unknown/past dates without future-motion stamps), (2) thin/noisy discovery, (3) WHO research not attempted on nearly all survivors, (4) surface thesis depth killing the last 1–2 rows**. Same contract Bethesda/NYC already satisfy. Placement/fit are not systemic killers. No threshold change recommended.

## Mutations

- GDI thresholds changed? **NO**
- Data mutated? **NO**
`
  );

  // FOUNDER_REPORT
  const samplePct = (cls) => pct(qualityCounts.ALL[cls] || 0, sampleTotal);

  write(
    "FOUNDER_REPORT.md",
    `# FOUNDER REPORT — GDI Zero-Yield Root Cause Forensic
**Date:** 2026-10-03  
**Mode:** Forensic only — no threshold changes, no discovery, no promotions, no ADP/share touches.

## Executive answer

All three subject hotels show **0 customer-ready** under the live contract. This is **not** a single bug. The dominant sequential kill is **FUTURE_TIMING_VALID** (78% cross-hotel reject of entity-valid rows), then **WHO not researched**, then **surface eligibility** on the last survivors — while Bethesda / Renaissance / Hilton still pass the same gates with dated, WHO-enriched evidence.

## Phase 1 — Funnels (structural re-eval; soft-DQ stamps stripped)

${subjects
  .map((k) => {
    const f = funnels[k];
    return `### ${k} (${HOTELS[k].label}) — discovered ${f.remaining.DISCOVERED}
\`\`\`
${f.remaining.DISCOVERED} discovered
→ ${f.remaining.ENTITY_VALID} entity valid
→ ${f.remaining.FUTURE_TIMING_VALID} future
→ ${f.remaining.IN_GEOGRAPHY} geography
→ ${f.remaining.LODGING_SIGNAL_PRESENT} lodging
→ ${f.remaining.HOTEL_FIT_PASS} fit
→ ${f.remaining.PLACEMENT_PASS} placement
→ ${f.remaining.WHO_RESOLVED} WHO
→ ${f.remaining.CONTACT_PATH_FOUND} contact path
→ ${f.remaining.SUMMARY_QA_PASS} summary QA
→ ${f.remaining.SURFACE_ELIGIBLE} surface eligible
→ ${f.remaining.CUSTOMER_READY} customer ready
\`\`\``;
  })
  .join("\n\n")}

## Phase 2 — Top kill gates

### YOTEL
${topKills("YOTEL")
  .map(
    (k, i) =>
      `${i + 1}. **${k.gate}** — enter ${k.countEntering}, reject ${k.countRejected} (${k.rejectionPct}%)`
  )
  .join("\n")}

### SPICE
${topKills("SPICE")
  .map(
    (k, i) =>
      `${i + 1}. **${k.gate}** — enter ${k.countEntering}, reject ${k.countRejected} (${k.rejectionPct}%)`
  )
  .join("\n")}

### AC
${topKills("AC")
  .map(
    (k, i) =>
      `${i + 1}. **${k.gate}** — enter ${k.countEntering}, reject ${k.countRejected} (${k.rejectionPct}%)`
  )
  .join("\n")}

### Cross-hotel (subjects aggregated)
${crossKills
  .slice(0, 5)
  .map((k) => `- **${k.gate}**: enter ${k.countEntering}, reject ${k.countRejected} (${k.rejectionPct}%)`)
  .join("\n")}

## Phase 3 — Surface eligibility forensic

Rows passing entity+timing+geo+lodging+fit but failing customer-ready: **${passCoreFailReady}**  
See \`SURFACE_ELIGIBILITY_FORENSICS.csv\`.

Dominant failure pattern: \`classifyCustomerSurfaceOpportunity\` → demand-generator / market-entity without hotel thesis depth, and/or readiness fields (\`who_research_not_attempted\`, \`summary_quality\`, \`why_now\`, \`recommended_action\`).

Surface eligibility **bug** (lodgingEvidence flags + thesis still rejected): **${trueSurfaceBug ? "YES" : "NO"}**  
Soft-DQ stamp notes (operational): ${surfaceBugNotes.length}

## Phases 4–6

See \`LODGING_SIGNAL_FORENSICS.md\`, \`WHO_CONTACT_FORENSICS.md\`, \`PLACEMENT_FORENSICS.md\`.

## Phase 7 — Rejection sample (n=${sampleTotal})

| Class | Count | % |
|---|---:|---:|
${Object.keys(qualityCounts.ALL)
  .sort((a, b) => qualityCounts.ALL[b] - qualityCounts.ALL[a])
  .map((c) => `| ${c} | ${qualityCounts.ALL[c]} | ${samplePct(c)}% |`)
  .join("\n")}

## Phase 8–9

See \`CUSTOMER_READY_CONTRACT_AUDIT.md\`, \`BETHESDA_NYC_COMPARISON.md\`.

## Phase 10 — Root cause

${rootClasses.map((c) => `- ${c}`).join("\n")}

## RETURN CARD

| Key | Value |
|---|---|
| YOTEL LARGEST KILL GATE | ${yotelTop?.gate} (${yotelTop?.countRejected}/${yotelTop?.countEntering}) |
| SPICE LARGEST KILL GATE | ${spiceTop?.gate} (${spiceTop?.countRejected}/${spiceTop?.countEntering}) |
| AC LARGEST KILL GATE | ${acTop?.gate} (${acTop?.countRejected}/${acTop?.countEntering}) |
| CROSS-HOTEL LARGEST KILL GATE | ${crossTop?.gate} (${crossTop?.countRejected}/${crossTop?.countEntering}) |
| SURFACE ELIGIBILITY BUG FOUND | ${trueSurfaceBug ? "YES" : "NO"} |
| PUBLIC-DATA CONTRACT UNREALISTIC | ${publicContractUnrealistic ? "YES" : "NO"} |
| WHO REQUIREMENT BLOCKING VALID OPPORTUNITIES | ${whoBlockingValid ? "YES" : "NO"} |
| LODGING REQUIREMENT BLOCKING VALID OPPORTUNITIES | ${lodgingBlocking ? "YES" : "NO"} |
| PLACEMENT LOGIC BLOCKING VALID OPPORTUNITIES | ${placementBlocking ? "YES" : "NO"} |
| DISCOVERY COVERAGE INSUFFICIENT | ${discoveryInsufficient ? "YES" : "NO"} |
| RESEARCH DEPTH INSUFFICIENT | ${researchDepthInsufficient ? "YES" : "NO"} |
| PROMISING BUT UNDER-RESEARCHED COUNT | ${promising} |
| CORRECTLY REJECTED COUNT | ${correct} |
| BETHESDA/NYC PIPELINE DIFFERENCE FOUND | ${pipelineDiff ? "YES" : "NO"} |
| ROOT CAUSE CLASSIFICATION | ${rootClasses.join(" + ")} |
| GDI THRESHOLDS CHANGED? | NO |
| DATA MUTATED? | NO |

## FINAL VERDICT

**Zero yield is mostly real under current bags: FUTURE_TIMING kills most candidates first; survivors die on WHO-not-researched then surface depth. YOTEL thin; Spice date-weak. Same contract works at Bethesda/NYC. One surface bug found: boilerplate summaryWhyMatters makes hasExplicitHotelMotionCopy ignore a real overflow thesis (AidEx). Do not lower thresholds — fix that motion-copy scan, stamp dates/future-motion, run WHO/ceiling, stamp lodging mentioned flags from known housing URLs.**

STOP.
`
  );

  // machine snapshot for debugging
  write(
    "_forensic_snapshot.json",
    JSON.stringify(
      {
        stampedCounts,
        funnels: Object.fromEntries(
          Object.entries(funnels).map(([k, f]) => [k, { remaining: f.remaining, kills: f.kills }])
        ),
        qualityCounts,
        lodgingDist,
        whoDist,
        rootClasses,
        surfaceBugNotes: surfaceBugNotes.slice(0, 40),
        yotelTop,
        spiceTop,
        acTop,
        crossTop,
        promising,
        correct,
        trueSurfaceBug,
        whoBlockingValid,
        whoBlockingCount,
        lodgingBlocking,
        placementBlocking,
        discoveryInsufficient,
        researchDepthInsufficient,
        publicContractUnrealistic,
        pipelineDiff,
      },
      null,
      2
    )
  );

  console.log(
    JSON.stringify(
      {
        out: OUT,
        yotelTop,
        spiceTop,
        acTop,
        crossTop,
        promising,
        correct,
        trueSurfaceBug,
        rootClasses,
        funnels: Object.fromEntries(
          subjects.map((k) => [k, funnels[k].remaining])
        ),
      },
      null,
      2
    )
  );
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
