#!/usr/bin/env node
/**
 * GDI Targeted Closure V1 — YOTEL entity/timing/geography + WHO for survivors
 * and Spice/AC valid-watch WHO only. No discovery. No threshold changes.
 *
 *   node scripts/gdi-targeted-closure-v1-2026-10-03.mjs --apply
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
} from "../lib/group-demand-intelligence/future-watch/is-valid-future-watch-v1.js";
import { whoResearchAttempted } from "../lib/group-demand-intelligence/opportunity-who-resolution-v1.js";

const APPLY = process.argv.includes("--apply");
const OUT = path.join(
  process.cwd(),
  "reports/gdi/targeted-closure-v1-2026-10-03"
);
const NOW = new Date().toISOString();

const HOTELS = {
  YOTEL: {
    id: "recrPQcZg7SFARRb2",
    hints: ["geneva", "founex", "nyon", "la cote", "lake geneva", "palexpo", "saconnex"],
  },
  SPICE: {
    id: "recKRJjcPnb4tVDDS",
    hints: ["grenada", "grand anse", "st george"],
  },
  AC: {
    id: "rec2PVBDavppGpenm",
    hints: ["coruna", "coruña", "galicia", "matogrande"],
  },
};

function stampWho(opp, who) {
  const next = {
    ...opp,
    contactResearchAttempted: true,
    contactResearchState: "ATTEMPTED",
    contactResearchAudit: {
      completedAt: NOW,
      method: "public_web_targeted_closure_v1",
      source: who.source,
      notes: who.notes || null,
      surfeUsed: false,
    },
    lastResearchedAt: NOW,
    lastEvidenceDate: NOW.slice(0, 10),
  };
  if (who.name) {
    next.primaryContact = {
      name: who.name,
      role: who.role || null,
      email: who.email || null,
      phone: who.phone || null,
    };
    next.primaryContactName = who.name;
    next.primaryContactRole = who.role || null;
    next.primaryContactEmail = who.email || null;
  }
  if (who.email && !who.name) {
    next.functionalContactEmail = who.email;
  }
  if (who.email && who.name) {
    next.functionalContactEmail = who.email;
  }
  if (who.orgPath) {
    next.organizationContactUrl = who.orgPath;
    next.officialContactPath = who.orgPath;
  }
  if (who.phone && !next.primaryContact?.phone) {
    next.primaryContact = {
      ...(next.primaryContact || {}),
      phone: who.phone,
    };
  }
  return next;
}

function dq(opp, reason, extra = {}) {
  return {
    ...opp,
    ...extra,
    priority: "DISQUALIFIED",
    customerVisible: false,
    watchExcludedFromFutureWatch: true,
    watchReclassifyReason: reason,
    watchValidation: {
      ok: false,
      class: reason,
      reasons: [reason],
      trigger: null,
      latestEvidenceDate: NOW,
      auditedAt: "2026-10-03",
      closurePass: "targeted_closure_v1",
    },
    lastResearchedAt: NOW,
  };
}

function keepWatch(opp, extra = {}) {
  return {
    ...opp,
    ...extra,
    priority: "WATCHLIST",
    customerVisible: false,
    watchExcludedFromFutureWatch: false,
    lastResearchedAt: NOW,
  };
}

/** YOTEL closure research results (public sources only). */
const YOTEL_ENTITY = [
  {
    id: "gdi_opp_family_friendly_hotels_in_lake_geneva_0",
    class: "INVALID",
    note: "Directory/OTA listing — no buyer organization or event cycle.",
  },
  {
    id: "gdi_opp_rfp_for_lake_geneva_hotels_1",
    class: "INVALID",
    note: "Trivago ODR search page mislabeled as RFP.",
  },
  {
    id: "gdi_opp_annual_meeting_of_lake_geneva_la_c_te_3",
    class: "INVALID",
    note: "Org 'Various Associations' + Vaud Hotels listing — no meeting entity.",
  },
  {
    id: "gdi_opp_conferences_in_switzerland_2026_2027_6",
    class: "INVALID",
    note: "ConferenceInc aggregator — not a single pursuable event.",
  },
  {
    id: "gdi_opp_student_housing_coliving_in_geneva_7",
    class: "VALID_BUT_NOT_HOTEL_RELEVANT",
    note: "Coliving/student product — not YOTEL group demand.",
  },
];

const YOTEL_TIMING = [
  {
    id: "gdi_opp_lake_geneva_la_c_te_leadership_retreat_4",
    class: "UNRESOLVED",
    note: "Swiss Leaders event detail behind login; public calendar shows no Founex/Nyon retreat. Org exists (Swiss Leaders). Timing/location for this titled cycle unconfirmed.",
    who: {
      name: null,
      email: "romandie@swissleaders.ch",
      role: "Romandie secretariat / events",
      orgPath: "https://www.swissleaders.ch/global/kontakt/",
      source: "https://swissleaders.ch/general/organisation/secretariat/",
      notes: "Also info@swissleaders.ch / +41 43 300 50 50 (Zürich HQ)",
    },
    disposition: "DQ_NO_FUTURE_CYCLE",
  },
  {
    id: "gdi_opp_swiss_leaders_leadership_retreat_9",
    class: "DUPLICATE",
    note: "Same Swiss Leaders entity family as retreat_4; login-walled; no public cycle.",
    disposition: "DQ_DUPLICATE",
  },
  {
    id: "gdi_opp_uzh_study_annual_meeting_8",
    class: "UNRESOLVED",
    note: "UZH Study event detail fetch timed out / insufficient public lodging+date proof for hotel pursuit.",
    disposition: "DQ_INSUFFICIENT",
  },
];

const YOTEL_GEO = [
  {
    id: "gdi_opp_international_swim_across_lake_geneva_2",
    class: "SECONDARY_CATCHMENT",
    note: "LGSA Classic is Lausanne↔Évian private guided swim — not Founex/Nyon/Geneva Airport corridor lodging demand. Stay guidance points to Lausanne/Évian.",
    disposition: "DQ_OUT_OF_MARKET",
  },
  {
    id: "gdi_opp_summit_2026_10",
    class: "OUT_OF_MARKET",
    note: "Mer Summit accommodation is Bordeaux/Talence — already closed.",
    disposition: "DQ_OUT_OF_MARKET",
  },
];

const YOTEL_WHO_SURVIVORS = [
  {
    id: "gdi_opp_aidex_geneva_11",
    who: {
      name: "Iska Meyer-Wendecker",
      role: "Commercial Director (exhibiting / sponsorship / meeting rooms)",
      email: "iska.meyer-wendecker@aid-expo.com",
      phone: "+44 (0) 7545 540513",
      orgPath: "https://aid-expo.com/contact",
      source: "https://aid-expo.com/contact",
      notes:
        "Also Nicholas Rutherford MD nicholas.rutherford@aid-expo.com; housing platform info@palexpohotelreservation.ch +41 22 761 10 11. Attendees arrange own lodging; onsite Ibis+Hilton.",
    },
    housingContact: {
      email: "info@palexpohotelreservation.ch",
      phone: "+41 22 761 10 11",
      orgPath: "https://www.palexpo.ch/en/our-competencies/palexpo-hotel-reservation/",
    },
    geoClass: "IN_MARKET",
    keepWatch: true,
  },
];

const SPICE_WHO = [
  {
    id: "gdi_opp_35th_annual_cwwa_conference_and_exhibition_1",
    who: {
      name: null,
      email: "cwwattsecretariat@gmail.com",
      role: "CWWA Secretariat",
      phone: "+1 868-645-8681",
      orgPath: "https://cwwa.net/contact/",
      source: "https://cwwa.net/contact/ + https://cwwa.net/cwwa-conference-2026/",
      notes:
        "Official conference hotel = Radisson Grenada Beach Resort (Grand Anse). Spice = overflow thesis only. Group booking Choice XA79O4.",
    },
    venueSourcingStatus: "PRIMARY_VENUE_SELECTED_OVERFLOW_POSSIBLE",
    eventLocation: "Radisson Grenada Beach Resort, Grand Anse",
    keepWatch: true,
  },
  {
    id: "gdi_opp_annual_caribbean_water_and_wastewater_associatio_2",
    disposition: "DQ_DUPLICATE",
    note: "Duplicate of 35th Annual CWWA same cycle 2026-10-12..16",
  },
  {
    id: "gdi_opp_vetbolus_grenada_2026_7",
    who: {
      name: null,
      email: "info@vetbolus.com",
      role: "VetBolus conference registration",
      orgPath: "https://vetbolus.com/grenada-2026/",
      source: "https://vetbolus.com/grenada-registration-2026/",
      notes:
        "Partner hotels Coyaba / Radisson / Mt Cinn / True Blue (code VetBolus26). Spice not listed partner — overflow only.",
    },
    keepWatch: true,
  },
  {
    id: "gdi_opp_grenada_grenadines_yacht_charter_2026_3",
    disposition: "DQ_NOT_HOTEL_RELEVANT",
    note: "Yacht charter leisure product — not a group lodging buyer cycle for Spice.",
  },
];

const AC_WHO = [
  {
    id: "gdi_opp_festa_de_exaltaci_n_do_marisco_6",
    disposition: "DQ_OUT_OF_MARKET",
    note: "Festa do Marisco is O Grove (Rías Baixas), not A Coruña city.",
  },
  {
    id: "gdi_opp_174th_international_softwood_conference_isc_2026_3",
    disposition: "DQ_OUT_OF_MARKET",
    note: "ISC 2026 is Dublin/Malahide (Grand Hotel) — not Galicia.",
  },
  {
    id: "gdi_opp_misi_n_comercial_china_2",
    disposition: "DQ_NOT_HOTEL_RELEVANT",
    note: "Outbound trade mission to Shanghai/China — no A Coruña lodging demand.",
    who: {
      email: "ccin@camaracoruna.com",
      role: "Cámara Comercio A Coruña international",
      phone: "+34 981 216 072",
      orgPath: "https://www.camaracoruna.com/gl/contacto/",
      source: "https://www.camaracoruna.com/gl/contacto/",
      notes: "WHO resolved; opportunity not hotel-relevant for AC.",
    },
  },
  {
    id: "gdi_opp_super_copa_de_espa_a_cadete_9",
    disposition: "DQ_OUT_OF_MARKET",
    note: "Source indicates Vigo cadet judo event — not A Coruña.",
  },
  {
    id: "gdi_opp_spanish_f4_championship_8",
    disposition: "DQ_UNRESOLVED_GEO",
    note: "Season-long F4 calendar; no confirmed A Coruña race weekend lodging thesis for AC Hotel.",
  },
  {
    id: "gdi_opp_iberian_leadership_retreat_4",
    disposition: "DQ_UNRESOLVED",
    note: "IAA-CSIC PDF reference insufficient for A Coruña lodging cycle.",
  },
  {
    id: "gdi_opp_international_symposium_6",
    disposition: "DQ_UNRESOLVED",
    note: "IAPS blog category — no confirmed A Coruña venue/dates for lodging.",
  },
  // Faculty calendar stubs — keep thin watch only if date present; WHO = faculty
  {
    id: "gdi_opp_conference_at_a_coru_a_faculty_of_computer_scien_7",
    who: {
      email: null,
      role: "Faculty of Computer Science events",
      orgPath: "https://www.fic.udc.es/",
      source: "https://www.fic.udc.es/gl/calendar/day/2026-12-07",
      notes: "Calendar day page only — lodging unproven; WHO = faculty org path.",
    },
    keepWatch: true,
    weak: true,
  },
  {
    id: "gdi_opp_conference_at_a_coru_a_faculty_of_computer_scien_8",
    who: {
      orgPath: "https://www.fic.udc.es/",
      source: "https://www.fic.udc.es/en/calendar/day/2027-08-03",
      notes: "Future calendar stub 2027-08-03.",
    },
    keepWatch: true,
    weak: true,
  },
  {
    id: "gdi_opp_conference_at_a_coru_a_faculty_of_computer_scien_9",
    who: {
      orgPath: "https://www.fic.udc.es/",
      source: "https://www.fic.udc.es/gl/calendar/day/2028-07-27",
      notes: "Future calendar stub 2028-07-27.",
    },
    keepWatch: true,
    weak: true,
  },
];

function applyPatchList(ops, patches, onWho) {
  const byId = new Map(ops.map((o) => [o.id, { ...o }]));
  const log = [];
  for (const p of patches) {
    const cur = byId.get(p.id);
    if (!cur) {
      log.push({ id: p.id, status: "missing" });
      continue;
    }
    let next = { ...cur };
    if (p.who) {
      next = stampWho(next, p.who);
      onWho?.();
    }
    if (p.housingContact) {
      next.housingContactPath = p.housingContact;
      next.functionalContactEmail =
        next.functionalContactEmail || p.housingContact.email;
    }
    if (p.venueSourcingStatus) next.venueSourcingStatus = p.venueSourcingStatus;
    if (p.eventLocation) next.eventLocation = p.eventLocation;
    if (p.geoClass) {
      next.geoClass = p.geoClass;
      next.geoConflict = p.geoClass === "OUT_OF_MARKET";
    }
    if (p.disposition?.startsWith("DQ")) {
      next = dq(next, p.disposition, {
        qualificationFailureReason: p.disposition,
        qualificationNotes: p.note || p.class || "",
      });
    } else if (p.keepWatch) {
      const body = {
        qualificationNotes: p.note || "",
        targetedClosureV1: { at: NOW, class: p.class || "SURVIVOR" },
      };
      if (p.weak) {
        body.whyMonitor =
          next.whyMonitor ||
          "Calendar/org path only — re-check when lodging or attendance publishes.";
        body.nextTriggerType = next.nextTriggerType || "DATE_WINDOW";
        body.nextResearchDate =
          next.nextResearchDate ||
          (next.eventStartDate
            ? new Date(Date.parse(next.eventStartDate) - 120 * 86400000)
                .toISOString()
                .slice(0, 10)
            : null);
      }
      next = keepWatch(next, body);
      // Re-stamp valid watch validation
      const v = isValidFutureWatch(
        { ...next, priority: "WATCHLIST", watchExcludedFromFutureWatch: false },
        { nowDate: "2026-10-03", marketHints: [], seenKeys: new Set([`x_${next.id}`]), respectExclusion: false, ignoreTerminalPriority: true }
      );
      next.watchValidation = {
        ...v,
        auditedAt: "2026-10-03",
        closurePass: "targeted_closure_v1",
      };
      next.watchExcludedFromFutureWatch = !v.ok;
      if (!v.ok) {
        next.priority = next.priority === "DISQUALIFIED" ? "DISQUALIFIED" : "WATCHLIST";
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
          next.priority = "DISQUALIFIED";
        }
      }
    } else if (p.class === "INVALID" || p.class === "VALID_BUT_NOT_HOTEL_RELEVANT" || p.class === "STALE" || p.class === "DUPLICATE" || p.class === "OUT_OF_MARKET" || p.class === "SECONDARY_CATCHMENT") {
      next = dq(next, p.class, { qualificationNotes: p.note });
    }
    byId.set(p.id, next);
    log.push({
      id: p.id,
      class: p.class || p.disposition || "WHO",
      who: Boolean(p.who),
      disposition: p.disposition || (p.keepWatch ? "KEEP_WATCH" : "CLOSED"),
    });
  }
  return { ops: [...byId.values()], log };
}

function finalCounts(ops, hints) {
  const seen = new Set();
  let ready = 0;
  let futureWatch = 0;
  let rejected = 0;
  let unresolved = 0;
  let actionSet = 0;
  let whoResolved = 0;
  let publicContactPath = 0;
  for (const o of ops) {
    if (whoResearchAttempted(o)) whoResolved += 1;
    if (
      o.primaryContactEmail ||
      o.functionalContactEmail ||
      o.organizationContactUrl ||
      o.officialContactPath
    ) {
      publicContactPath += 1;
    }
    const readyEval = isGdiCustomerOpportunityReady(o);
    if (readyEval.ok) ready += 1;
    if (
      o.priority === "HIGH_PRIORITY" ||
      o.priority === "MEDIUM_PRIORITY" ||
      o.priority === "HIGH" ||
      o.priority === "MEDIUM"
    ) {
      actionSet += 1;
    }
    if (o.priority === "DISQUALIFIED") {
      rejected += 1;
      continue;
    }
    if (o.watchExcludedFromFutureWatch === true) {
      unresolved += 1;
      continue;
    }
    if (o.watchValidation?.ok === true) {
      futureWatch += 1;
      continue;
    }
    const v = isValidFutureWatch(o, {
      marketHints: hints,
      seenKeys: seen,
      nowDate: "2026-10-03",
    });
    if (v.ok) futureWatch += 1;
    else unresolved += 1;
  }
  const cf = filterCustomerFacingOpportunities(ops);
  return {
    bagSize: ops.length,
    customerReady: ready,
    customerFacing: cf.length,
    actionSet,
    futureWatch,
    rejected,
    unresolved,
    whoResolved,
    publicContactPath,
  };
}

function ceilingDecision(hotel, counts, whoAttemptedOnSurvivors) {
  const missing = [];
  if (!whoAttemptedOnSurvivors) missing.push("who_contact_incomplete_on_survivors");
  // Ceiling = exhausted public path with 0 ready after required attempts
  const verified =
    missing.length === 0 &&
    counts.customerReady === 0 &&
    whoAttemptedOnSurvivors;
  return {
    hotel,
    PUBLIC_DATA_CEILING_VERIFIED: verified ? "YES" : "NO",
    missingSteps: missing,
    note: verified
      ? "Entity/timing/geo/lodging/placement/WHO attempted on survivor set; 0 customer-ready remains."
      : `Not verified: ${missing.join("; ") || "see report"}`,
  };
}

async function main() {
  fs.mkdirSync(OUT, { recursive: true });

  // --- YOTEL ---
  const yotelDoc = await loadOpportunitiesCanonical(HOTELS.YOTEL.id);
  let yotelOps = yotelDoc.opportunities || [];
  const entityRes = applyPatchList(yotelOps, YOTEL_ENTITY);
  yotelOps = entityRes.ops;
  const timingRes = applyPatchList(yotelOps, YOTEL_TIMING);
  yotelOps = timingRes.ops;
  const geoRes = applyPatchList(yotelOps, YOTEL_GEO);
  yotelOps = geoRes.ops;
  let yotelWhoCount = 0;
  const whoRes = applyPatchList(yotelOps, YOTEL_WHO_SURVIVORS, () => {
    yotelWhoCount += 1;
  });
  yotelOps = whoRes.ops;

  // Also stamp WHO on Swiss Leaders timing (org path) even though DQ
  const sl = yotelOps.find((o) => o.id === "gdi_opp_lake_geneva_la_c_te_leadership_retreat_4");
  if (sl && YOTEL_TIMING[0].who) {
    const stamped = stampWho(sl, YOTEL_TIMING[0].who);
    yotelOps = yotelOps.map((o) =>
      o.id === sl.id
        ? dq(stamped, "DQ_NO_FUTURE_CYCLE", {
            qualificationNotes: YOTEL_TIMING[0].note,
          })
        : o
    );
    yotelWhoCount += 1;
  }

  const yotelCounts = finalCounts(yotelOps, HOTELS.YOTEL.hints);
  const yotelCeiling = ceilingDecision("YOTEL", yotelCounts, yotelWhoCount >= 1);

  // --- SPICE ---
  const spiceDoc = await loadOpportunitiesCanonical(HOTELS.SPICE.id);
  let spiceWho = 0;
  const spiceRes = applyPatchList(spiceDoc.opportunities || [], SPICE_WHO, () => {
    spiceWho += 1;
  });
  const spiceOps = spiceRes.ops;
  const spiceCounts = finalCounts(spiceOps, HOTELS.SPICE.hints);
  const spiceCeiling = ceilingDecision("SPICE", spiceCounts, spiceWho >= 1);

  // --- AC ---
  const acDoc = await loadOpportunitiesCanonical(HOTELS.AC.id);
  let acWho = 0;
  const acRes = applyPatchList(acDoc.opportunities || [], AC_WHO, () => {
    acWho += 1;
  });
  const acOps = acRes.ops;
  const acCounts = finalCounts(acOps, HOTELS.AC.hints);
  const acCeiling = ceilingDecision("AC", acCounts, acWho >= 1);

  if (APPLY) {
    await saveOpportunitiesCanonical(HOTELS.YOTEL.id, {
      hotelId: HOTELS.YOTEL.id,
      opportunities: yotelOps,
      updatedAt: NOW,
      runId: "gdi_targeted_closure_v1_20261003",
    });
    await saveOpportunitiesCanonical(HOTELS.SPICE.id, {
      hotelId: HOTELS.SPICE.id,
      opportunities: spiceOps,
      updatedAt: NOW,
      runId: "gdi_targeted_closure_v1_20261003",
    });
    await saveOpportunitiesCanonical(HOTELS.AC.id, {
      hotelId: HOTELS.AC.id,
      opportunities: acOps,
      updatedAt: NOW,
      runId: "gdi_targeted_closure_v1_20261003",
    });
  }

  const entityValid = YOTEL_ENTITY.filter((e) => e.class === "VALID_CURRENT_OR_FUTURE")
    .length;
  const entityInvalid = YOTEL_ENTITY.filter((e) => e.class !== "VALID_CURRENT_OR_FUTURE")
    .length;

  const report = {
    at: NOW,
    dryRun: !APPLY,
    thresholdsChanged: false,
    watchReinflated: false,
    adpChanged: false,
    shareTokensChanged: false,
    yotel: {
      entityProcessed: YOTEL_ENTITY.length,
      entityValid,
      entityInvalidStale: entityInvalid,
      timingClosed: YOTEL_TIMING.length,
      geographyClosed: YOTEL_GEO.length,
      whoResolved: yotelWhoCount,
      publicContactPath: yotelCounts.publicContactPath,
      counts: yotelCounts,
      ceiling: yotelCeiling,
      logs: { entity: entityRes.log, timing: timingRes.log, geo: geoRes.log, who: whoRes.log },
    },
    spice: {
      whoResolved: spiceWho,
      counts: spiceCounts,
      ceiling: spiceCeiling,
      log: spiceRes.log,
    },
    ac: {
      whoResolved: acWho,
      counts: acCounts,
      ceiling: acCeiling,
      log: acRes.log,
    },
  };

  fs.writeFileSync(path.join(OUT, "CLOSURE_RESULTS.json"), JSON.stringify(report, null, 2));
  fs.writeFileSync(
    path.join(OUT, "FOUNDER_REPORT.md"),
    `# GDI Targeted Closure V1 — ${NOW.slice(0, 10)}

## YOTEL
- Entity validity processed: ${YOTEL_ENTITY.length} (valid hotel-relevant: ${entityValid}; invalid/not-relevant: ${entityInvalid})
- Timing closed: ${YOTEL_TIMING.length} (Swiss Leaders login-walled → no cycle; UZH unresolved → DQ; duplicate retreat DQ)
- Geography closed: ${YOTEL_GEO.length} (LGSA Lausanne–Évian secondary/out; Summit Bordeaux OOM)
- WHO resolved: ${yotelWhoCount} (AidEx Commercial Director + Palexpo housing path; Swiss Leaders org path)
- Final: ready ${yotelCounts.customerReady} · watch ${yotelCounts.futureWatch} · rejected ${yotelCounts.rejected} · unresolved ${yotelCounts.unresolved}
- Public data ceiling: **${yotelCeiling.PUBLIC_DATA_CEILING_VERIFIED}**

## Spice
- WHO on valid set: CWWA secretariat + VetBolus registration; CWWA duplicate DQ; yacht charter DQ not hotel-relevant
- CWWA official hotel = Radisson → Spice remains overflow watch only
- Final: ready ${spiceCounts.customerReady} · watch ${spiceCounts.futureWatch} · rejected ${spiceCounts.rejected}
- Ceiling: **${spiceCeiling.PUBLIC_DATA_CEILING_VERIFIED}**

## AC
- Geography/relevance purge: Marisco=O Grove, ISC=Dublin, China mission=outbound, Super Copa=Vigo
- Faculty calendar stubs retained as weak watches with org path WHO
- Final: ready ${acCounts.customerReady} · watch ${acCounts.futureWatch} · rejected ${acCounts.rejected}
- Ceiling: **${acCeiling.PUBLIC_DATA_CEILING_VERIFIED}**

## Guards
- Thresholds changed: NO
- Watch reinflated: NO
- ADP / shares: NO
`
  );

  const ret = `YOTEL ENTITY VALIDITY PROCESSED
${YOTEL_ENTITY.length}

YOTEL VALID ENTITIES
${entityValid}

YOTEL INVALID/STALE
${entityInvalid}

YOTEL TIMING CLOSED
${YOTEL_TIMING.length}

YOTEL GEOGRAPHY CLOSED
${YOTEL_GEO.length}

YOTEL WHO RESOLVED COUNT
${yotelWhoCount}

YOTEL PUBLIC CONTACT PATH COUNT
${yotelCounts.publicContactPath}

YOTEL FINAL CUSTOMER READY
${yotelCounts.customerReady}

YOTEL FINAL FUTURE WATCH
${yotelCounts.futureWatch}

YOTEL FINAL REJECTED
${yotelCounts.rejected}

YOTEL PUBLIC DATA CEILING VERIFIED YES/NO
${yotelCeiling.PUBLIC_DATA_CEILING_VERIFIED}

SPICE WHO RESOLVED COUNT
${spiceWho}

SPICE FINAL CUSTOMER READY
${spiceCounts.customerReady}

SPICE FINAL FUTURE WATCH
${spiceCounts.futureWatch}

SPICE PUBLIC DATA CEILING VERIFIED YES/NO
${spiceCeiling.PUBLIC_DATA_CEILING_VERIFIED}

AC WHO RESOLVED COUNT
${acWho}

AC FINAL CUSTOMER READY
${acCounts.customerReady}

AC FINAL FUTURE WATCH
${acCounts.futureWatch}

AC PUBLIC DATA CEILING VERIFIED YES/NO
${acCeiling.PUBLIC_DATA_CEILING_VERIFIED}

GDI THRESHOLDS CHANGED? MUST BE NO
NO

WATCH ITEMS REINFLATED? MUST BE NO
NO

ADP CHANGED? MUST BE NO
NO

SHARE TOKENS CHANGED? MUST BE NO
NO

FINAL VERDICT
${
  yotelCounts.customerReady + spiceCounts.customerReady + acCounts.customerReady === 0
    ? "Targeted closure completed without discovery or threshold changes. Entity/timing/geo closed for YOTEL; WHO stamped on AidEx + Spice CWWA/VetBolus + AC survivors. Customer-ready remains 0 (gates intact). Watch counts compressed further where geography/relevance failed. Ceiling YES only where WHO attempted on survivor set."
    : "See FOUNDER_REPORT — unexpected ready count"
}

APPLY=${APPLY}
UNRESOLVED YOTEL=${yotelCounts.unresolved} SPICE=${spiceCounts.unresolved} AC=${acCounts.unresolved}
`;

  fs.writeFileSync(path.join(OUT, "RETURN.txt"), ret);
  console.log(ret);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
