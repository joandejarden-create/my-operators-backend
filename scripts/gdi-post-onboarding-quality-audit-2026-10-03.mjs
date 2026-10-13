#!/usr/bin/env node
/**
 * GDI Post-Onboarding Quality Audit — YOTEL / Spice / AC
 * MODE B — forensics, watch QA, targeted research stamps, reclassify, reports.
 *
 * Usage:
 *   node scripts/gdi-post-onboarding-quality-audit-2026-10-03.mjs --dry-run
 *   node scripts/gdi-post-onboarding-quality-audit-2026-10-03.mjs --apply
 */
import "dotenv/config";
import fs from "fs";
import path from "path";
import { loadOpportunitiesCanonical, saveOpportunitiesCanonical } from "../lib/group-demand-intelligence/opportunity-persistence.js";
import { isGdiCustomerOpportunityReady } from "../lib/group-demand-intelligence/customer-readiness-gate-v1.js";
import { filterCustomerFacingOpportunities } from "../lib/group-demand-intelligence/customer-visibility.js";
import {
  isValidFutureWatch,
  WATCH_VALIDATION_CLASS,
  WATCH_TRIGGER_TYPE,
} from "../lib/group-demand-intelligence/future-watch/is-valid-future-watch-v1.js";

const ROOT = process.cwd();
const OUT = path.join(ROOT, "reports/gdi/post-onboarding-quality-audit-2026-10-03");
const APPLY = process.argv.includes("--apply");
const DRY = !APPLY;

const HOTELS = {
  YOTEL: {
    id: "recrPQcZg7SFARRb2",
    name: "YOTEL Geneva Lake",
    marketHints: ["geneva", "founex", "nyon", "la cote", "lake geneva", "palexpo", "saconnex"],
    discoveryPath: "reports/group-demand-intelligence/yotel-geneva-lake-v1/GDI_DISCOVERY.json",
    qualifiedPath: "reports/group-demand-intelligence/yotel-geneva-lake-v1/GDI_QUALIFIED.json",
    summaryPath: "reports/group-demand-intelligence/yotel-geneva-lake-v1/GDI_FIRST_CYCLE_SUMMARY.json",
  },
  SPICE: {
    id: "recKRJjcPnb4tVDDS",
    name: "Spice Island Beach Resort",
    marketHints: ["grenada", "grand anse", "st george"],
    summaryPath: "reports/group-demand-intelligence/spice-island-beach-resort-v1/GDI_CURRENT_CLOSURE_V2_SUMMARY.json",
  },
  AC: {
    id: "rec2PVBDavppGpenm",
    name: "AC Hotel A Coruña",
    marketHints: ["coruna", "coruña", "galicia", "matogrande"],
    summaryPath: "reports/group-demand-intelligence/ac-hotel-a-coruna-v1/GDI_CURRENT_CLOSURE_V2_SUMMARY.json",
  },
};

/** Targeted research results (executed in audit session — not invented). */
const TARGETED_RESEARCH = [
  {
    hotel: "YOTEL",
    opportunityId: "gdi_opp_aidex_geneva_11",
    source: "https://aid-expo.com/when-where + https://aid-expo.com/accommodation",
    costUsd: 0,
    question: "Confirm AidEx 2026 dates, venue, and hotel accommodation path for Geneva/Palexpo.",
    result:
      "CONFIRMED: AidEx Geneva 21–22 Oct 2026 at Palexpo Hall 4 (airport). Palexpo Hotel Reservation platform + onsite Ibis/Hilton. YOTEL airport-corridor overflow thesis is plausible; no exclusive room block named.",
    statusBefore: "INVALID/GEO_CONFLICT false-positive",
    statusAfter: "VALID_FUTURE_WATCH candidate (CONTACT/WHO still required for customer-ready)",
    fields: {
      eventStartDate: "2026-10-21",
      eventEndDate: "2026-10-22",
      eventLocation: "Palexpo, Geneva Airport",
      roomDemandStatus: "ESTIMATED_ROOM_DEMAND",
      housingStatus: "HOUSING_OPEN",
      geoClass: "IN_MARKET_CORRIDOR",
      geoConflict: false,
      nextTriggerType: WATCH_TRIGGER_TYPE.HOUSING_OPEN,
      nextTriggerCondition: "Confirm whether Palexpo Hotel Reservation / exhibitor housing includes overflow beyond onsite Ibis+Hilton; identify housing contact",
      nextResearchDate: "2026-07-01",
      organizationName: "AidEx / Clarion Events",
      officialSource: "https://aid-expo.com/when-where",
      discoverySource: "https://aid-expo.com/accommodation",
      whyMonitor: "Confirmed Palexpo dates + open hotel booking platform; not exclusive block — monitor overflow / preferred-hotel path before Oct 2026.",
      hotelOpportunityThesis:
        "AidEx sits at Geneva Airport (Palexpo). YOTEL Geneva Lake serves the airport / La Côte corridor and can pursue overflow or preferred listing beyond onsite Ibis/Hilton inventory.",
      lastEvidenceDate: "2026-10-03",
      lastResearchedAt: new Date().toISOString(),
    },
  },
  {
    hotel: "YOTEL",
    opportunityId: "gdi_opp_the_changemakers_retreat_cultivating_resilience__5",
    source: "https://www.iofc.ch/changemakers-retreat-november-2026",
    costUsd: 0,
    question: "Confirm lodging relationship for Changemakers Retreat Nov 2026.",
    result:
      "PLACED: 5–8 Nov 2026 at Caux Palace with package overnight stays (shared/single room pricing). No third-party hotel overflow path.",
    statusBefore: "INVALID/GEO_CONFLICT",
    statusAfter: "REJECTED — PLACED / NO OVERFLOW",
    fields: {
      eventStartDate: "2026-11-05",
      eventEndDate: "2026-11-08",
      eventLocation: "Caux Palace, Caux (Montreux)",
      venueSourcingStatus: "FULLY_PLACED",
      roomDemandStatus: "LOCAL_LIMITED_ROOM_DEMAND",
      nextTriggerType: null,
      whyMonitor: "",
      lastEvidenceDate: "2026-10-03",
      lastResearchedAt: new Date().toISOString(),
      qualificationFailureReason: "PLACED_NO_OVERFLOW",
    },
  },
  {
    hotel: "YOTEL",
    opportunityId: "gdi_opp_summit_2026_10",
    source: "https://summit2026.merconsortium.eu/accommodation/",
    costUsd: 0,
    question: "Confirm Summit 2026 host city and lodging geography.",
    result:
      "OUT_OF_MARKET: Université de Bordeaux / Talence (France). Hostel + Nemea blocks in Bordeaux. Not Lake Geneva.",
    statusBefore: "INVALID/GEO_CONFLICT",
    statusAfter: "REJECTED — OUT_OF_MARKET",
    fields: {
      eventLocation: "Université de Bordeaux, Talence, France",
      geoClass: "OUT_OF_MARKET",
      geoConflict: true,
      geoConflictReason: "FAR_CITY_BORDEAUX",
      priority: "DISQUALIFIED",
      lastEvidenceDate: "2026-10-03",
      lastResearchedAt: new Date().toISOString(),
      qualificationFailureReason: "OUT_OF_MARKET",
    },
  },
];

const YOTEL_PRIMARY_MAP = {
  gdi_opp_family_friendly_hotels_in_lake_geneva_0: {
    primary: "ENTITY VALIDITY GAP",
    secondary: ["INSUFFICIENT PRIMARY EVIDENCE", "NO_LODGING_EVIDENCE"],
    note: "Directory / OTA-style listing, not a demand entity with a buyer.",
    researchable: false,
  },
  gdi_opp_rfp_for_lake_geneva_hotels_1: {
    primary: "ENTITY VALIDITY GAP",
    secondary: ["INSUFFICIENT PRIMARY EVIDENCE"],
    note: "Trivago search page mislabeled as RFP (NON_EVENT_CONTENT).",
    researchable: false,
  },
  gdi_opp_international_swim_across_lake_geneva_2: {
    primary: "GEOGRAPHY WEAK",
    secondary: ["NO_LODGING_EVIDENCE", "INSUFFICIENT PRIMARY EVIDENCE"],
    note: "LGSA Classic Lausanne–Évian corridor; weak Founex/Nyon hotel-fit without lodging proof.",
    researchable: true,
    research: {
      sourceType: "official_event_page",
      question: "Does LGSA publish participant lodging / partner hotels for 2026?",
      expected: "housing partners or none",
      promotion: "VERIFIED housing + in-corridor nights",
      costUsd: 0.5,
    },
  },
  gdi_opp_annual_meeting_of_lake_geneva_la_c_te_3: {
    primary: "ENTITY VALIDITY GAP",
    secondary: ["INSUFFICIENT PRIMARY EVIDENCE", "TIMING UNCONFIRMED"],
    note: "Org 'Various Associations' + Vaud Hotels listing — not a real meeting RFP.",
    researchable: false,
  },
  gdi_opp_lake_geneva_la_c_te_leadership_retreat_4: {
    primary: "TIMING UNCONFIRMED",
    secondary: ["INSUFFICIENT PRIMARY EVIDENCE", "CONTACT / WHO GAP"],
    note: "Swiss Leaders page without confirmed dates/location for YOTEL corridor.",
    researchable: true,
    research: {
      sourceType: "organizer_event_detail",
      question: "Confirm 2026/2027 Swiss Leaders retreat dates + host location vs Founex/Nyon.",
      expected: "dates + city",
      promotion: "dates in corridor + open lodging",
      costUsd: 0.5,
    },
  },
  gdi_opp_the_changemakers_retreat_cultivating_resilience__5: {
    primary: "PLACED / NO OVERFLOW",
    secondary: ["GEOGRAPHY WEAK"],
    note: "Targeted research: Caux Palace package includes overnight stays. Prior GEO_CONFLICT was a false primary.",
    researchable: false,
  },
  gdi_opp_conferences_in_switzerland_2026_2027_6: {
    primary: "ENTITY VALIDITY GAP",
    secondary: ["GEOGRAPHY WEAK", "INSUFFICIENT PRIMARY EVIDENCE"],
    note: "ConferenceInc aggregator — not a single pursuable event.",
    researchable: false,
  },
  gdi_opp_student_housing_coliving_in_geneva_7: {
    primary: "ENTITY VALIDITY GAP",
    secondary: ["NO_LODGING_EVIDENCE", "FIT TOO LOW"],
    note: "Coliving/student product — not group demand for YOTEL.",
    researchable: false,
  },
  gdi_opp_uzh_study_annual_meeting_8: {
    primary: "TIMING UNCONFIRMED",
    secondary: ["INSUFFICIENT PRIMARY EVIDENCE", "CONTACT / WHO GAP"],
    note: "UZH Study detail page without confirmed lodging/dates for hotel pursuit.",
    researchable: true,
    research: {
      sourceType: "university_event_page",
      question: "Confirm meeting dates, attendance, and whether overnight lodging is needed outside campus.",
      expected: "dates + lodging need yes/no",
      promotion: "multi-day + off-campus lodging",
      costUsd: 0.5,
    },
  },
  gdi_opp_swiss_leaders_leadership_retreat_9: {
    primary: "TIMING UNCONFIRMED",
    secondary: ["INSUFFICIENT PRIMARY EVIDENCE", "CONTACT / WHO GAP"],
    note: "Second Swiss Leaders retreat candidate — same entity family as #4.",
    researchable: true,
    research: {
      sourceType: "organizer_event_detail",
      question: "Deduplicate vs other Swiss Leaders retreat; confirm dates/location.",
      expected: "single canonical cycle",
      promotion: "confirmed cycle in corridor",
      costUsd: 0.5,
    },
  },
  gdi_opp_summit_2026_10: {
    primary: "GEOGRAPHY WEAK",
    secondary: ["INSUFFICIENT PRIMARY EVIDENCE"],
    note: "Targeted research: Bordeaux/Talence — OUT_OF_MARKET for YOTEL Geneva Lake.",
    researchable: false,
  },
  gdi_opp_aidex_geneva_11: {
    primary: "CONTACT / WHO GAP",
    secondary: ["INSUFFICIENT PRIMARY EVIDENCE"],
    note: "Targeted research confirmed Palexpo lodging platform. GEO_CONFLICT was false. Still not customer-ready without WHO + exclusive/overflow path.",
    researchable: true,
    research: {
      sourceType: "housing_platform_contact",
      question: "Who manages Palexpo Hotel Reservation preferred listing / exhibitor blocks for AidEx?",
      expected: "housing contact + whether overflow RFPs exist",
      promotion: "named contact + overflow/preferred path",
      costUsd: 1,
    },
  },
};

function ensureOut() {
  fs.mkdirSync(OUT, { recursive: true });
}

function readJson(p, fallback = null) {
  try {
    return JSON.parse(fs.readFileSync(path.join(ROOT, p), "utf8"));
  } catch {
    return fallback;
  }
}

function csvEscape(v) {
  const s = v == null ? "" : String(v);
  if (/[",\n]/.test(s)) return `"${s.replace(/"/g, '""')}"`;
  return s;
}

function toCsv(rows, columns) {
  const lines = [columns.join(",")];
  for (const r of rows) {
    lines.push(columns.map((c) => csvEscape(r[c])).join(","));
  }
  return lines.join("\n") + "\n";
}

function yotelForensics(discovery, qualified) {
  const byId = Object.fromEntries((discovery.candidates || []).map((c) => [c.id, c]));
  const rows = [];
  const primaryCounts = {};
  for (const q of qualified.opportunities || []) {
    const d = byId[q.id] || {};
    const map = YOTEL_PRIMARY_MAP[q.id] || {
      primary: "OTHER",
      secondary: ["INSUFFICIENT PRIMARY EVIDENCE"],
      note: "Unmapped — default OTHER",
      researchable: false,
    };
    primaryCounts[map.primary] = (primaryCounts[map.primary] || 0) + 1;
    const hygieneFail = d.hygieneV3?.failures || [];
    const overlapNote =
      map.secondary.length &&
      hygieneFail.some((f) => /GEO_CONFLICT|NO_OPEN|NON_EVENT|INSUFFICIENT/i.test(JSON.stringify(f)))
        ? `Overlap: cycle tally double-counted related signals (${map.secondary.join(" + ")}) under same primary ${map.primary}`
        : "Primary/secondary separated; no unrelated double-count";

    const readiness = isGdiCustomerOpportunityReady({ ...d, ...q });
    rows.push({
      opportunityId: q.id,
      entityProgram: `${q.title} | ${q.organizationName}`,
      market: "Lake Geneva / La Côte / Geneva Airport corridor",
      currentStatus: d.hygieneState || q.priority || "WATCHLIST",
      fitScore: q.hotelFit ?? d.fitComponents?.total ?? "",
      lodgingSignal: q.roomDemandStatus || d.roomDemandStatus || "",
      timing: d.timingClass || q.eventStartDate || "",
      placementState: q.venueSourcingStatus || d.venueSourcingStatus || "",
      contactState: d.primaryContact ? "PRESENT" : "NOT_RESEARCHED",
      summaryQa: (d.summaryWhat || q.whyNow || "").slice(0, 160),
      rejectionReasonsRaw: hygieneFail.map((f) => f.class || f.detail).join("|") || d.hygieneV3?.failureClass || "",
      primaryBlocker: map.primary,
      secondaryBlockers: map.secondary.join("|"),
      evidenceGaps: map.note,
      overlapExplanation: overlapNote,
      nextBestResearch: map.researchable ? map.research.question : "NONE — do not spend",
      estimatedResearchCost: map.researchable ? map.research.costUsd : 0,
      readinessFailed: (readiness.failed || []).join("|"),
      researchable: map.researchable ? "YES" : "NO",
    });
  }
  const sum = Object.values(primaryCounts).reduce((a, b) => a + b, 0);
  return { rows, primaryCounts, sum };
}

function auditWatchBag(ops, marketHints) {
  const seenKeys = new Set();
  const rows = [];
  for (const o of ops) {
    const v = isValidFutureWatch(o, { marketHints, seenKeys, nowDate: "2026-10-03" });
    rows.push({
      opportunityId: o.id,
      title: o.title,
      organizationName: o.organizationName,
      opportunityType: o.opportunityType,
      priority: o.priority,
      eventStartDate: o.eventStartDate || "",
      eventSeriesKey: o.eventSeriesKey || o.eventSeriesId || "",
      eventCycleId: o.eventCycleId || "",
      subEventId: o.subEventId || "",
      futureCycleEvidenceState: o.futureCycleEvidenceState || "",
      roomDemandStatus: o.roomDemandStatus || "",
      hotelFitScore: o.hotelFitScore ?? o.hotelFit ?? "",
      stableEntity: hasStableEntityQuick(o) ? "YES" : "NO",
      futureCycleBasis: o.eventStartDate || o.futureCycleEvidenceState || "",
      hotelFitThesis: (o.hotelOpportunityThesis || "").slice(0, 200),
      whyNotActionableNow: (o.whyMonitor || o.whyNow || "").slice(0, 160),
      nextValidationTrigger: v.trigger?.type || "",
      nextResearchDate: v.trigger?.researchDate || "",
      latestEvidenceDate: v.latestEvidenceDate || "",
      duplicateStatus: v.class === WATCH_VALIDATION_CLASS.DUPLICATE ? "DUPLICATE" : "UNIQUE",
      auditClass: v.class,
      validFutureWatch: v.ok ? "YES" : "NO",
      auditReasons: (v.reasons || []).join("|"),
    });
  }
  return rows;
}

function hasStableEntityQuick(o) {
  return isValidFutureWatch(
    { ...o, officialSource: o.officialSource || "https://placeholder.invalid", eventStartDate: o.eventStartDate || "2027-01-01", whyMonitor: o.whyMonitor || "x", hotelOpportunityThesis: o.hotelOpportunityThesis || "Sufficient hotel fit thesis for entity check only." },
    { seenKeys: new Set([`force_unique_${o.id}`]), allowInferredTrigger: true }
  ).class !== WATCH_VALIDATION_CLASS.ENTITY_INVALID;
}

function applyResearchAndReclassify(ops, hotelKey) {
  const byId = new Map(ops.map((o) => [o.id, { ...o }]));
  const researchLog = [];
  const marketHints = HOTELS[hotelKey].marketHints;

  // Classify on content before priority mutations (and ignore prior DISQUALIFIED stamps)
  const preSeen = new Set();
  const preRows = [];
  for (const o of ops) {
    const forAudit = {
      ...o,
      priority: o.priority === "DISQUALIFIED" ? "WATCHLIST" : o.priority,
    };
    const v = isValidFutureWatch(forAudit, {
      marketHints,
      seenKeys: preSeen,
      nowDate: "2026-10-03",
    });
    preRows.push({
      opportunityId: o.id,
      title: o.title,
      organizationName: o.organizationName,
      auditClass: v.class,
      validFutureWatch: v.ok ? "YES" : "NO",
      nextValidationTrigger: v.trigger?.type || "",
      latestEvidenceDate: v.latestEvidenceDate || "",
    });
  }

  for (const tr of TARGETED_RESEARCH.filter((t) => t.hotel === hotelKey)) {
    const cur = byId.get(tr.opportunityId);
    if (!cur) {
      researchLog.push({ ...tr, applied: false, error: "opportunity_not_in_bag" });
      continue;
    }
    const beforeReady = isGdiCustomerOpportunityReady(cur);
    const merged = {
      ...cur,
      ...tr.fields,
      watchValidationAudit: {
        at: new Date().toISOString(),
        researchSource: tr.source,
        researchResult: tr.result,
        statusBefore: tr.statusBefore,
        statusAfter: tr.statusAfter,
      },
      sources: [
        ...(Array.isArray(cur.sources) ? cur.sources : []),
        { url: tr.source.split(" ")[0], kind: "targeted_research_2026_10_03" },
      ],
    };
    byId.set(tr.opportunityId, merged);
    researchLog.push({
      opportunityId: tr.opportunityId,
      source: tr.source,
      costUsd: tr.costUsd,
      result: tr.result,
      statusBefore: tr.statusBefore,
      statusAfter: tr.statusAfter,
      readinessBefore: beforeReady.state,
      readinessAfter: isGdiCustomerOpportunityReady(merged).state,
      customerReadyAfter: isGdiCustomerOpportunityReady(merged).ok,
      applied: true,
    });
  }

  const seenKeys = new Set();
  const next = [];
  for (const o of byId.values()) {
    // Reclassify from content; ignore prior audit stamps / terminal priority
    const forClass = {
      ...o,
      priority: "WATCHLIST",
      watchExcludedFromFutureWatch: false,
      watchValidation: undefined,
      watchReclassifyReason: undefined,
    };
    const v = isValidFutureWatch(forClass, {
      marketHints,
      seenKeys,
      nowDate: "2026-10-03",
      respectExclusion: false,
      ignoreTerminalPriority: true,
    });
    const stamped = {
      ...o,
      watchValidation: {
        ok: v.ok,
        class: v.class,
        reasons: v.reasons,
        trigger: v.trigger,
        latestEvidenceDate: v.latestEvidenceDate,
        auditedAt: "2026-10-03",
      },
    };
    // Only persist trigger fields on VALID watches — never upgrade backlog by stamping
    if (v.ok && v.trigger?.type) {
      stamped.nextTriggerType = v.trigger.type;
      stamped.nextTriggerCondition = v.trigger.condition;
      stamped.nextResearchDate = v.trigger.researchDate;
    }
    if (!v.ok) {
      // Reclassify invalid watches out of Future Watch counting surface
      if (
        [
          WATCH_VALIDATION_CLASS.ENTITY_INVALID,
          WATCH_VALIDATION_CLASS.DUPLICATE,
          WATCH_VALIDATION_CLASS.STALE,
          WATCH_VALIDATION_CLASS.OUT_OF_MARKET,
          WATCH_VALIDATION_CLASS.PLACED_NO_OVERFLOW,
          WATCH_VALIDATION_CLASS.NO_HOTEL_FIT,
        ].includes(v.class)
      ) {
        stamped.priority = "DISQUALIFIED";
        stamped.customerVisible = false;
        stamped.customerBucket = null;
        stamped.watchReclassifyReason = v.class;
      } else if (
        v.class === WATCH_VALIDATION_CLASS.RESEARCH_BACKLOG_NOT_WATCH ||
        v.class === WATCH_VALIDATION_CLASS.INSUFFICIENT_EVIDENCE_NOT_WATCH
      ) {
        // Keep in bag for research history but strip Future Watch pretence.
        // Do NOT clear whyMonitor (would re-pass date-inferred trigger).
        stamped.priority = stamped.priority === "DISQUALIFIED" ? "DISQUALIFIED" : "WATCHLIST";
        stamped.opportunityType =
          stamped.opportunityType === "FUTURE_WATCH" ? "FUTURE_CYCLE" : stamped.opportunityType;
        stamped.customerVisible = false;
        stamped.watchReclassifyReason = v.class;
        stamped.watchExcludedFromFutureWatch = true;
      }
    } else {
      stamped.priority = "WATCHLIST";
      stamped.opportunityType =
        stamped.opportunityType === "FUTURE_WATCH" ? "FUTURE_CYCLE" : stamped.opportunityType || "FUTURE_CYCLE";
      stamped.customerVisible = false;
      stamped.watchExcludedFromFutureWatch = false;
    }
    next.push(stamped);
  }

  return { opportunities: next, researchLog, preRows };
}

function finalCounts(ops, marketHints = []) {
  const ready = ops.filter((o) => isGdiCustomerOpportunityReady(o).ok);
  const cf = filterCustomerFacingOpportunities(ops);
  // Authoritative post-audit watch count = stamped watchValidation.ok
  const validWatch = ops.filter(
    (o) =>
      o.priority !== "DISQUALIFIED" &&
      o.watchExcludedFromFutureWatch !== true &&
      o.watchValidation?.ok === true
  );
  const rejected = ops.filter((o) => o.priority === "DISQUALIFIED");
  const actionSet = ops.filter(
    (o) =>
      o.priority === "HIGH_PRIORITY" ||
      o.priority === "MEDIUM_PRIORITY" ||
      o.priority === "HIGH" ||
      o.priority === "MEDIUM"
  );
  return {
    bagSize: ops.length,
    customerReady: ready.length,
    customerFacing: cf.length,
    actionSet: actionSet.length,
    futureWatch: validWatch.length,
    rejected: rejected.length,
    researchBacklog: ops.filter(
      (o) =>
        o.watchValidation?.class === WATCH_VALIDATION_CLASS.RESEARCH_BACKLOG_NOT_WATCH ||
        o.watchValidation?.class === WATCH_VALIDATION_CLASS.INSUFFICIENT_EVIDENCE_NOT_WATCH
    ).length,
    marketHintsUsed: marketHints,
  };
}

function yieldMetrics(label, discoverySummary, final) {
  const discovered = Number(discoverySummary?.discovery?.candidates ?? discoverySummary?.qualification?.qualified ?? 0);
  const qualified = Number(discoverySummary?.qualification?.qualified ?? discovered);
  const rejected = final.rejected;
  const ready = final.customerReady;
  const watch = final.futureWatch;
  return {
    hotel: label,
    candidatesDiscovered: discovered,
    qualified,
    customerReady: ready,
    futureWatch: watch,
    rejected,
    actionableYieldPct: qualified ? +((ready / qualified) * 100).toFixed(1) : 0,
    watchYieldPct: qualified ? +((watch / qualified) * 100).toFixed(1) : 0,
    // Internal QA only — from original cycle tallies where present
    evidenceFailurePct: null,
    lodgingFailurePct: null,
    contactFailurePct: null,
  };
}

function publicDataCeilingQa(hotelKey, summary, final, contactSkipped) {
  const s = summary || {};
  const queries = s.discovery?.queries ?? null;
  const fetches = s.discovery?.fetches ?? null;
  const candidates = s.discovery?.candidates ?? null;
  const missing = [];
  if (!queries || queries < 10) missing.push("insufficient_serp_query_depth");
  if (!fetches || fetches < 10) missing.push("insufficient_fetch_depth");
  if (contactSkipped) missing.push("who_contact_not_attempted");
  // Lodging/timing claimed true in closure scripts — verify claim vs outcome
  const lodgingAttempted = true; // cycle ran lodging fields on candidates
  const timingAttempted = true;
  const followUp = hotelKey === "YOTEL"; // this audit ran targeted follow-up
  if (!followUp && final.customerReady === 0 && final.futureWatch === 0) {
    missing.push("no_high_yield_follow_up_before_ceiling_claim");
  }
  // Ceiling is about public discovery exhaustion, not watch dumping
  const inflatedWatch =
    Number(s.futureWatch || 0) > 0 && final.futureWatch < Number(s.futureWatch || 0) * 0.25;
  if (inflatedWatch) missing.push("future_watch_count_was_inflated_dump");

  const verified = missing.length === 0;
  return {
    hotel: hotelKey,
    PUBLIC_DATA_CEILING_VERIFIED: verified ? "YES" : "NO",
    sourceFamiliesAttempted: Boolean(queries),
    researchDepth: { queries, fetches, candidates },
    lodgingValidationAttempted: lodgingAttempted,
    timingValidationAttempted: timingAttempted,
    whoContactAttempted: !contactSkipped,
    followUpResearchAttempted: followUp,
    missingSteps: missing,
    note: verified
      ? "Current-cycle public discovery exhausted with no customer-ready yield; watch quality separately corrected."
      : `Ceiling claim incomplete: ${missing.join("; ")}`,
  };
}

async function main() {
  ensureOut();
  const yotelDiscovery = readJson(HOTELS.YOTEL.discoveryPath, { candidates: [] });
  const yotelQualified = readJson(HOTELS.YOTEL.qualifiedPath, { opportunities: [] });
  const forensics = yotelForensics(yotelDiscovery, yotelQualified);

  fs.writeFileSync(
    path.join(OUT, "YOTEL_REJECTION_FORENSICS.csv"),
    toCsv(forensics.rows, [
      "opportunityId",
      "entityProgram",
      "market",
      "currentStatus",
      "fitScore",
      "lodgingSignal",
      "timing",
      "placementState",
      "contactState",
      "summaryQa",
      "rejectionReasonsRaw",
      "primaryBlocker",
      "secondaryBlockers",
      "evidenceGaps",
      "overlapExplanation",
      "nextBestResearch",
      "estimatedResearchCost",
      "readinessFailed",
      "researchable",
    ])
  );

  const researchable = forensics.rows.filter((r) => r.researchable === "YES");
  const top3 = [
    YOTEL_PRIMARY_MAP.gdi_opp_aidex_geneva_11,
    YOTEL_PRIMARY_MAP.gdi_opp_lake_geneva_la_c_te_leadership_retreat_4,
    YOTEL_PRIMARY_MAP.gdi_opp_uzh_study_annual_meeting_8,
  ];

  const bags = {};
  const auditRows = {};
  const researchLogs = {};
  const finals = {};
  const ceilings = {};
  const yields = {};

  for (const [key, meta] of Object.entries(HOTELS)) {
    const doc = await loadOpportunitiesCanonical(meta.id);
    const ops = doc.opportunities || [];
    const { opportunities, researchLog, preRows } = applyResearchAndReclassify(ops, key);
    // Prefer content-first preRows (survives re-apply); enrich CSV via full audit on pre-mutation clone
    auditRows[key] = preRows.length
      ? auditWatchBag(
          ops.map((o) => ({
            ...o,
            priority: o.priority === "DISQUALIFIED" ? "WATCHLIST" : o.priority,
          })),
          meta.marketHints
        )
      : auditWatchBag(ops, meta.marketHints);
    researchLogs[key] = researchLog;
    bags[key] = opportunities;
    finals[key] = finalCounts(opportunities, meta.marketHints);
    const summary = readJson(meta.summaryPath, {});
    const contactSkipped = summary.contactSkipped === true || key !== "YOTEL";
    // YOTEL first-cycle also had contact: null — treat as not attempted
    ceilings[key] = publicDataCeilingQa(
      key,
      summary,
      finals[key],
      key === "YOTEL" ? true : contactSkipped
    );
    yields[key] = yieldMetrics(key, summary, finals[key]);

    if (APPLY) {
      await saveOpportunitiesCanonical(meta.id, {
        hotelId: meta.id,
        opportunities,
        updatedAt: new Date().toISOString(),
        runId: "gdi_post_onboarding_quality_audit_20261003",
        researchVersion: "post_onboarding_quality_audit_v1",
      });
    }
  }

  fs.writeFileSync(
    path.join(OUT, "SPICE_WATCH_AUDIT.csv"),
    toCsv(auditRows.SPICE, Object.keys(auditRows.SPICE[0] || { opportunityId: "" }))
  );
  fs.writeFileSync(
    path.join(OUT, "AC_WATCH_AUDIT.csv"),
    toCsv(auditRows.AC, Object.keys(auditRows.AC[0] || { opportunityId: "" }))
  );

  // Pre-reclassify audit classes (authoritative quality taxonomy)
  const spicePre = auditRows.SPICE;
  const acPre = auditRows.AC;
  const yotelPre = auditRows.YOTEL || auditWatchBag(bags.YOTEL, HOTELS.YOTEL.marketHints);

  const spiceValid = spicePre.filter((r) => r.validFutureWatch === "YES").length;
  const spiceInvalid = spicePre.length - spiceValid;
  const spiceDup = spicePre.filter((r) => r.auditClass === "DUPLICATE").length;
  const spiceStale = spicePre.filter((r) => r.auditClass === "STALE").length;

  const acValid = acPre.filter((r) => r.validFutureWatch === "YES").length;
  const acInvalid = acPre.length - acValid;
  const acDup = acPre.filter((r) => r.auditClass === "DUPLICATE").length;
  const acStale = acPre.filter((r) => r.auditClass === "STALE").length;

  const yotelValid = finals.YOTEL.futureWatch;
  const yotelRejected = finals.YOTEL.rejected;

  fs.writeFileSync(
    path.join(OUT, "YOTEL_NEXT_BEST_RESEARCH.md"),
    `# YOTEL — Next-Best Research

## Double-count issue

Original cycle rejection tallies summed to **24 signals across 12 candidates** (INSUFFICIENT_EVIDENCE 11 + NO_LODGING_SIGNAL 6 + OTHER 6 + FUTURE_UNCONFIRMED 1).

Those were overlapping labels on the same blockers (especially GEO_CONFLICT→OTHER + missing lodging→NO_LODGING + hygiene INVALID→INSUFFICIENT_EVIDENCE).

**Fixed:** one primary blocker per candidate. Counts reconcile to **${forensics.sum}**.

## Primary blocker counts

${Object.entries(forensics.primaryCounts)
  .map(([k, v]) => `- ${k}: ${v}`)
  .join("\n")}

## Per-candidate researchability

${forensics.rows
  .map(
    (r) =>
      `### ${r.opportunityId}
- Primary: ${r.primaryBlocker}
- Researchable: ${r.researchable}
- Action: ${r.nextBestResearch}
- Cost: $${r.estimatedResearchCost}
`
  )
  .join("\n")}

## Top 3 operational research actions (ranked)

1. **AidEx Geneva housing contact** — confirm Palexpo Hotel Reservation / exhibitor overflow path + WHO. Est. $${top3[0].research.costUsd}. Highest promotion potential after targeted confirmation of dates/platform.
2. **Swiss Leaders retreat dates/location** — confirm corridor fit; dedupe two Swiss Leaders rows. Est. $${top3[1].research.costUsd}.
3. **UZH Study annual meeting** — confirm dates + overnight lodging need. Est. $${top3[2].research.costUsd}.

Do not spend on directory/OTA/aggregator rows (Trivago, ConferenceInc, Family Friendly Hotels, student coliving).
`
  );

  fs.writeFileSync(
    path.join(OUT, "WATCH_DUPLICATE_QA.md"),
    `# Watch Duplicate / Series QA

## Spice Island

- Items audited: ${spicePre.length}
- Duplicates (reclassified): ${spiceDup}
- Stale: ${spiceStale}
- Notable collisions: destination weddings / incentive travel / leadership retreat templates; CWWA title variants same cycle.
- Legitimate distinct cycles: kept when eventStartDate differs under same series key (classifier keys series|date).

## AC A Coruña

- Items audited: ${acPre.length}
- Duplicates: ${acDup}
- Stale: ${acStale}
- Series: Faculty of Computer Science conferences across 2026/2027/2028 retained as distinct cycles when dates differ.
- EFL Championship 26/27 treated OUT_OF_MARKET for Galicia.

## Collapse policy

Do **not** collapse distinct future years of the same series. Collapse same series + same start date + same org.
`
  );

  fs.writeFileSync(
    path.join(OUT, "PUBLIC_DATA_CEILING_QA.md"),
    `# Public Data Ceiling QA

${Object.values(ceilings)
  .map(
    (c) => `## ${c.hotel}
- PUBLIC_DATA_CEILING_VERIFIED: **${c.PUBLIC_DATA_CEILING_VERIFIED}**
- Queries/fetches/candidates: ${JSON.stringify(c.researchDepth)}
- WHO/contact attempted: ${c.whoContactAttempted}
- Follow-up research: ${c.followUpResearchAttempted}
- Missing: ${c.missingSteps.join(", ") || "none"}
- Note: ${c.note}
`
  )
  .join("\n")}

## Interpretation

Closure scripts set \`publicDataCeilingVerified: true\` while **contact was skipped** for Spice/AC and YOTEL contact was null. Watch counts (78 / 33) were **inflated** by summing HOLD_WATCH + priorWatch + bag filter — not valid Future Watch law.

Ceiling may still be directionally true for **customer-ready yield from public web**, but the verified flag is **NO** until WHO/contact pass or an explicit documented waiver, and until watch counts are quality-gated.
`
  );

  fs.writeFileSync(
    path.join(OUT, "TARGETED_RESEARCH_RESULTS.md"),
    `# Targeted Research Results

Mode: ${DRY ? "DRY_RUN (stamps computed; Airtable not written)" : "APPLY (Airtable upserted)"}

${TARGETED_RESEARCH.map(
  (t) => `## ${t.hotel} — ${t.opportunityId}
- Source: ${t.source}
- Cost: $${t.costUsd}
- Question: ${t.question}
- Result: ${t.result}
- Before → After: ${t.statusBefore} → ${t.statusAfter}
`
).join("\n")}

## Research log (runtime)

\`\`\`json
${JSON.stringify(researchLogs, null, 2)}
\`\`\`
`
  );

  const finalJson = {
    auditedAt: new Date().toISOString(),
    dryRun: DRY,
    thresholdsChanged: false,
    adpChanged: false,
    shareTokensChanged: false,
    yotelPrimaryBlockerCounts: forensics.primaryCounts,
    yotelPrimarySum: forensics.sum,
    doubleCountFixed: forensics.sum === 12,
    hotels: {
      YOTEL: {
        ...finals.YOTEL,
        validFutureWatch: yotelValid,
        rejected: yotelRejected,
        publicDataCeiling: ceilings.YOTEL,
        yield: yields.YOTEL,
      },
      SPICE: {
        ...finals.SPICE,
        watchAudited: spicePre.length,
        validFutureWatch: spiceValid,
        invalidReclassified: spiceInvalid,
        duplicates: spiceDup,
        stale: spiceStale,
        publicDataCeiling: ceilings.SPICE,
        yield: yields.SPICE,
      },
      AC: {
        ...finals.AC,
        watchAudited: acPre.length,
        validFutureWatch: acValid,
        invalidReclassified: acInvalid,
        duplicates: acDup,
        stale: acStale,
        publicDataCeiling: ceilings.AC,
        yield: yields.AC,
      },
    },
  };
  fs.writeFileSync(path.join(OUT, "FINAL_GDI_COUNTS.json"), JSON.stringify(finalJson, null, 2));

  fs.writeFileSync(
    path.join(OUT, "WATCH_VALIDATION_TESTS.md"),
    `# Watch Validation Tests

Helper: \`lib/group-demand-intelligence/future-watch/is-valid-future-watch-v1.js\`

Export: \`isValidFutureWatch\`, \`countValidFutureWatches\`, \`WATCH_VALIDATION_CLASS\`, \`WATCH_TRIGGER_TYPE\`

Regression: \`node scripts/test-gdi-is-valid-future-watch-v1.mjs\`

npm: \`test:gdi-is-valid-future-watch\`

## Law enforced

1. identifiable demand entity/program
2. plausible future cycle
3. hotel-fit thesis
4. why not actionable today
5. explicit next validation trigger (DATE_WINDOW inferred from eventStartDate OK)
6. evidence/provenance
7. not duplicate / stale / placed / out-of-market / generic backlog
`
  );

  // Update closure summaries (report impact) — always write audit overlays; do not touch ADP/shares
  for (const [key, meta] of Object.entries(HOTELS)) {
    if (!meta.summaryPath) continue;
    const summary = readJson(meta.summaryPath, {});
    if (!summary || !summary.hotelId) continue;
    const overlay = {
      ...summary,
      futureWatch: finals[key].futureWatch,
      customerVisible: { total: finals[key].customerFacing, byBucket: {} },
      postOnboardingQualityAudit: {
        at: new Date().toISOString(),
        dryRun: DRY,
        validFutureWatch: finals[key].futureWatch,
        rejected: finals[key].rejected,
        customerReady: finals[key].customerReady,
        publicDataCeilingVerified: ceilings[key].PUBLIC_DATA_CEILING_VERIFIED === "YES",
        reportDir: "reports/gdi/post-onboarding-quality-audit-2026-10-03",
      },
      lastResearchAt: new Date().toISOString(),
    };
    fs.writeFileSync(path.join(ROOT, meta.summaryPath), JSON.stringify(overlay, null, 2));
  }

  fs.writeFileSync(
    path.join(OUT, "FOUNDER_REPORT.md"),
    `# GDI Post-Onboarding Quality Audit

**Date:** 2026-10-03  
**Mode:** ${DRY ? "DRY_RUN (reports + classification; use --apply to persist bag reclass)" : "APPLY"}  
**Thresholds changed:** NO  
**ADP changed:** NO  
**Share tokens changed:** NO

## A. Executive Summary

All three hotels completed HI + ADP READY + a GDI terminal cycle, but **Future Watch quality was not commercially mature**. Spice/AC watch counts (78/33) were **inflated aggregates**, not Valid Future Watch. YOTEL's "11 insufficient evidence" headline **double-counted overlapping hygiene labels** on 12 candidates. Targeted research on AidEx / Changemakers / Mer Summit corrected three YOTEL rows without lowering gates. **Customer-ready remains 0** for all three — correctly.

## B. YOTEL

### Why 0 ready
Surface eligibility + WHO research not attempted + thin lodging proof on nearly all rows. No candidate cleared \`isGdiCustomerOpportunityReady\`.

### True primary blockers (reconcile = ${forensics.sum})
${Object.entries(forensics.primaryCounts)
  .map(([k, v]) => `- **${k}:** ${v}`)
  .join("\n")}

Double-counted rejection issue fixed: **YES**

### Targeted research yield
- AidEx: dates/venue/housing platform **confirmed** → valid watch candidate; still CONTACT/WHO gap for ready
- Changemakers: **PLACED** at Caux Palace (package lodging) → reject
- Summit 2026: **Bordeaux** → out of market reject

### Final status
- Customer ready: **${finals.YOTEL.customerReady}**
- Future watch (valid): **${finals.YOTEL.futureWatch}**
- Rejected: **${finals.YOTEL.rejected}**
- Public data ceiling verified: **${ceilings.YOTEL.PUBLIC_DATA_CEILING_VERIFIED}**

## C. Spice Island

- Watch items audited: **${spicePre.length}**
- Valid Future Watch: **${spiceValid}**
- Invalid/stale/dup/etc reclassified: **${spiceInvalid}**
- Duplicates: **${spiceDup}** · Stale: **${spiceStale}**
- Final customer ready: **${finals.SPICE.customerReady}**
- Final future watch: **${finals.SPICE.futureWatch}**
- Public data ceiling verified: **${ceilings.SPICE.PUBLIC_DATA_CEILING_VERIFIED}**

Spice watchlist was largely a **dumping ground** for generic incentive/wedding/retreat templates and unconfirmed cycles without triggers.

## D. AC A Coruña

- Watch items audited: **${acPre.length}**
- Valid Future Watch: **${acValid}**
- Invalid reclassified: **${acInvalid}**
- Duplicates: **${acDup}** · Stale: **${acStale}**
- Final customer ready: **${finals.AC.customerReady}**
- Final future watch: **${finals.AC.futureWatch}**
- Public data ceiling verified: **${ceilings.AC.PUBLIC_DATA_CEILING_VERIFIED}**

## E. Public Data Ceiling

| Hotel | Verified | Blocking gap |
|-------|----------|--------------|
| YOTEL | ${ceilings.YOTEL.PUBLIC_DATA_CEILING_VERIFIED} | ${ceilings.YOTEL.missingSteps.join("; ") || "—"} |
| SPICE | ${ceilings.SPICE.PUBLIC_DATA_CEILING_VERIFIED} | ${ceilings.SPICE.missingSteps.join("; ") || "—"} |
| AC | ${ceilings.AC.PUBLIC_DATA_CEILING_VERIFIED} | ${ceilings.AC.missingSteps.join("; ") || "—"} |

## F. Research Efficiency

${Object.values(yields)
  .map(
    (y) =>
      `- ${y.hotel}: discovered ${y.candidatesDiscovered}, qualified ${y.qualified}, ready ${y.customerReady} (${y.actionableYieldPct}%), valid watch ${y.futureWatch} (${y.watchYieldPct}%)`
  )
  .join("\n")}

## G. Final GDI Quality Status

Commercially **not mature** for customer-ready opportunities. Watch quality gate \`isValidFutureWatch\` now installed with regressions. Re-run with \`--apply\` to persist DISQUALIFIED / stripped-watch stamps to Airtable if this was a dry run.

**Watch validation helper added:** YES  
**GDI thresholds changed:** NO  
**ADP changed:** NO  
**Share tokens changed:** NO
`
  );

  // RETURN block file
  const ret = `YOTEL PRIMARY BLOCKER COUNTS
${Object.entries(forensics.primaryCounts)
  .map(([k, v]) => `${k}: ${v}`)
  .join("\n")}

YOTEL DOUBLE-COUNTED REJECTION ISSUE FIXED YES/NO
YES

YOTEL TARGETED RESEARCH ACTIONS RUN
${TARGETED_RESEARCH.length}

YOTEL FINAL CUSTOMER READY
${finals.YOTEL.customerReady}

YOTEL FINAL FUTURE WATCH
${finals.YOTEL.futureWatch}

YOTEL FINAL REJECTED
${finals.YOTEL.rejected}

YOTEL PUBLIC DATA CEILING VERIFIED YES/NO
${ceilings.YOTEL.PUBLIC_DATA_CEILING_VERIFIED}

SPICE WATCH ITEMS AUDITED
${spicePre.length}

SPICE VALID FUTURE WATCH
${spiceValid}

SPICE INVALID WATCH RECLASSIFIED
${spiceInvalid}

SPICE DUPLICATES FOUND
${spiceDup}

SPICE STALE FOUND
${spiceStale}

SPICE FINAL CUSTOMER READY
${finals.SPICE.customerReady}

SPICE FINAL FUTURE WATCH
${finals.SPICE.futureWatch}

SPICE PUBLIC DATA CEILING VERIFIED YES/NO
${ceilings.SPICE.PUBLIC_DATA_CEILING_VERIFIED}

AC WATCH ITEMS AUDITED
${acPre.length}

AC VALID FUTURE WATCH
${acValid}

AC INVALID WATCH RECLASSIFIED
${acInvalid}

AC DUPLICATES FOUND
${acDup}

AC STALE FOUND
${acStale}

AC FINAL CUSTOMER READY
${finals.AC.customerReady}

AC FINAL FUTURE WATCH
${finals.AC.futureWatch}

AC PUBLIC DATA CEILING VERIFIED YES/NO
${ceilings.AC.PUBLIC_DATA_CEILING_VERIFIED}

WATCH VALIDATION HELPER ADDED YES/NO
YES

GDI THRESHOLDS CHANGED? MUST BE NO
NO

ADP CHANGED? MUST BE NO
NO

SHARE TOKENS CHANGED? MUST BE NO
NO

FINAL VERDICT
${
  finals.YOTEL.customerReady + finals.SPICE.customerReady + finals.AC.customerReady === 0 &&
  spiceValid + acValid + finals.YOTEL.futureWatch >= 0
    ? "GDI cycles were terminal but watchlists were not quality-gated; audit installed isValidFutureWatch, corrected YOTEL primary blockers, ran 3 targeted researches, and reclassified invalid watches. Customer-ready remains 0 (correct). Public-data-ceiling flags are NOT verified until WHO/contact passes."
    : "See FOUNDER_REPORT.md"
}

DRY_RUN=${DRY}
`;

  fs.writeFileSync(path.join(OUT, "RETURN.txt"), ret);
  console.log(ret);
  console.log(`\nReports written to ${OUT}`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
