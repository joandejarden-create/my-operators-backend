#!/usr/bin/env node
/**
 * GDI Live Commercial Quality V1.1 Closure
 * - Partition ACTIONABLE_NOW vs WATCH/FUTURE/INSUFFICIENT/CLOSED
 * - Contact upgrade (official enrichments + resolution pass) on ACTIONABLE_NOW only
 * - Defensible action + source + thesis bar
 * - Attendance/peak rooms from existing published evidence only (no fabrication)
 * - Persist via canonical Airtable/FS
 *
 * Usage:
 *   node scripts/gdi-live-commercial-quality-v1-1-closure.mjs --dry-run
 *   node scripts/gdi-live-commercial-quality-v1-1-closure.mjs --apply
 */
import "../load-env.js";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  applyLiveCommercialQuality,
  buildRelatedOpportunityGroups,
  LIVE_COMMERCIAL_QUALITY_V1,
} from "../lib/group-demand-intelligence/live-commercial-quality-v1.js";
import {
  hasMeaningfulSource,
  classifyContactPath,
  CONTACT_PATH_CLASS,
} from "../lib/group-demand-intelligence/commercial-evidence-v4.js";
import { applyContactResolutionPass } from "../lib/group-demand-intelligence/contact-resolution-pass.js";
import {
  loadOpportunitiesCanonical,
  saveOpportunitiesCanonical,
} from "../lib/group-demand-intelligence/opportunity-persistence.js";
import { BOOKING_WINDOW, PRIORITY } from "../lib/group-demand-intelligence/claim-types.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const BETHESDA = "recLuxvwwxID7U2B8";
const AS_OF = "2026-09-23";
const MARKER = "gdi_live_commercial_quality_v1_1_closure";
const REPORTS = path.join(ROOT, "reports/group-demand-intelligence");

const apply = process.argv.includes("--apply");
const dryRun = !apply;

function writeJson(p, obj) {
  fs.mkdirSync(path.dirname(p), { recursive: true });
  fs.writeFileSync(p, JSON.stringify(obj, null, 2));
}
function writeMd(p, lines) {
  fs.mkdirSync(path.dirname(p), { recursive: true });
  fs.writeFileSync(p, (Array.isArray(lines) ? lines : [lines]).join("\n"));
}
function pct(n, d) {
  return d ? Math.round((1000 * n) / d) / 10 : 0;
}
function known(v) {
  if (v == null) return false;
  const s = String(v).trim();
  return Boolean(s) && !/^(unknown|unk|n\/?a|tbd|—|-)$/i.test(s);
}

/** Canonical partition — no new competing status system. */
export function partitionSalesState(o) {
  const p = String(o.priority || "").toUpperCase();
  const t = String(o.opportunityType || "").toUpperCase();
  const cfs = String(o.customerFacingState || "").toUpperCase();
  const bw = String(o.bookingWindowStatus || "").toUpperCase();

  if (p === "DISQUALIFIED" || /CLOSED|CANCELLED/.test(p)) return "CLOSED";
  if (cfs === "INSUFFICIENT_EVIDENCE") return "INSUFFICIENT";
  if (
    t === "FUTURE_WATCH" ||
    cfs === "FUTURE_WATCH" ||
    o.nextCycleStatus === "UNCONFIRMED" ||
    bw === "TOO_EARLY"
  ) {
    return "FUTURE_WATCH";
  }
  if (
    bw === "WATCH" ||
    bw === "LIKELY_TOO_LATE" ||
    t === "FUTURE_CYCLE" ||
    p === "WATCHLIST" ||
    cfs === "WATCH" ||
    o.eventDateStatus === "PAST"
  ) {
    return "WATCH";
  }
  if (
    p === "HIGH_PRIORITY" ||
    p === "MEDIUM_PRIORITY" ||
    bw === "CONTACT_NOW" ||
    bw === "QUALIFY_NOW" ||
    bw === "RESEARCH_FURTHER"
  ) {
    return "ACTIONABLE_NOW";
  }
  return "OTHER";
}

function contactTier(o) {
  const pathClass =
    o.contactPathClass ||
    classifyContactPath(o.primaryContact || {}, o.backupContacts || []);
  const c = o.primaryContact || {};
  const email = String(c.email || "");
  const named =
    pathClass === CONTACT_PATH_CLASS.NAMED_CONTACT &&
    c.name &&
    String(c.name).split(/\s+/).length >= 2 &&
    !/^unknown$/i.test(c.name);
  const direct =
    named &&
    email &&
    !/^(info|contact|support|registrar|events|office|associatedirector|tournaments)@/i.test(
      email
    );
  if (direct) return "NAMED_DIRECT";
  if (named) return "NAMED_PARTIAL";
  if (pathClass === CONTACT_PATH_CLASS.FUNCTIONAL_CONTACT) return "FUNCTIONAL";
  if (pathClass === CONTACT_PATH_CLASS.GENERIC_CONTACT) return "GENERIC";
  return "NO_CONTACT";
}

function roleClass(o) {
  const match = String(
    o.primaryContact?.targetRoleMatch || o.contactRoleMatch || ""
  ).toUpperCase();
  if (/DIRECT_DECISION/.test(match)) return "PRIMARY_DECISION_MAKER";
  if (/EVENT_MEETINGS|HOUSING_SOURCING|INFLUENCER/.test(match)) return "STRONG_INFLUENCER";
  if (/OPERATIONS|FUNCTIONAL/.test(match)) return "OPERATIONAL_CONTACT";
  if (o.backupContacts?.length) return "BACKUP_CONTACT";
  return "OPERATIONAL_CONTACT";
}

function buildPreciseAction(o) {
  const c = o.primaryContact || {};
  const name = c.name && !/^unknown$/i.test(c.name) ? c.name : null;
  const email = c.email || null;
  const type = String(o.opportunityType || "").toUpperCase();
  const venue = String(o.venueSourcingStatus || "").toUpperCase();
  const who = name || (email ? email : "the published event contact");

  if (/PRIMARY_VENUE_SELECTED|FULLY_PLACED|VENUE_LOCKED/.test(venue)) {
    if (type.includes("OVERFLOW")) {
      return {
        recommendedAction: `Primary venue already selected. Contact ${who} only to confirm whether overflow / secondary lodging is needed for Bethesda Marriott; do not pitch as host venue.`,
        actionBasis: "venue_selected_overflow_qualify",
      };
    }
    return {
      recommendedAction: `Do not pursue as venue — primary venue already selected. Monitor for overflow/housing needs via ${who}.`,
      actionBasis: "venue_already_selected",
    };
  }

  if (type.includes("OVERFLOW") || type.includes("HOUSING")) {
    return {
      recommendedAction: `Contact ${who}${email && name ? ` (${email})` : ""} to request official housing-list / room-block inclusion and confirm peak-room need and stay dates for this overflow opportunity.`,
      actionBasis: "overflow_housing_outreach",
    };
  }

  return {
    recommendedAction: `Contact ${who}${email && name ? ` (${email})` : ""} to confirm venue/sourcing status is still open, introduce the hotel, and request RFP / site-visit inclusion.`,
    actionBasis: "primary_pursuit_outreach",
  };
}

function syncAttendancePeak(o) {
  const changes = [];
  let attendance = o.attendance ?? o.estimatedAttendance ?? o.publishedAttendance ?? null;
  let peakRooms = o.peakRooms ?? o.estimatedPeakRooms ?? o.publishedPeakRooms ?? null;
  let attendanceStatus = o.attendanceStatus || null;
  let peakRoomsStatus = o.peakRoomsStatus || null;

  if (known(attendance) && (!attendanceStatus || attendanceStatus === "UNKNOWN")) {
    attendanceStatus = o.publishedAttendance ? "CONFIRMED" : "ESTIMATED";
    changes.push("ATTENDANCE_ENRICHMENT");
  }
  if (known(peakRooms) && (!peakRoomsStatus || peakRoomsStatus === "UNKNOWN")) {
    peakRoomsStatus = o.publishedPeakRooms ? "CONFIRMED" : "ESTIMATED";
    changes.push("PEAK_ROOMS_ENRICHMENT");
  }
  if (!known(attendance)) {
    attendance = null;
    attendanceStatus = "UNKNOWN";
  }
  if (!known(peakRooms)) {
    peakRooms = null;
    peakRoomsStatus = "UNKNOWN";
  }

  return {
    attendance,
    estimatedAttendance: attendance ?? o.estimatedAttendance ?? null,
    attendanceStatus,
    peakRooms,
    estimatedPeakRooms: peakRooms ?? o.estimatedPeakRooms ?? null,
    peakRoomsStatus,
    changes,
  };
}

function salespersonReady(o, state) {
  if (state !== "ACTIONABLE_NOW") return "N/A";
  const hasSource = hasMeaningfulSource(o);
  const thesis = Boolean(o.hotelDemandThesis || o.hotelOpportunityThesis);
  const action = Boolean(o.recommendedAction && o.actionBasis);
  const when = Boolean(
    o.eventDateDisplay && o.eventDateDisplay !== "Date not yet confirmed"
  );
  const where = Boolean(o.eventLocationSummary || o.destinationStatus || o.venueStatus);
  const who = contactTier(o) !== "NO_CONTACT";
  const contactable = ["NAMED_DIRECT", "NAMED_PARTIAL", "FUNCTIONAL", "GENERIC"].includes(
    contactTier(o)
  );
  const checks = [hasSource, thesis, action, when, where, who, contactable];
  const pass = checks.filter(Boolean).length;
  if (pass === checks.length) return "READY";
  if (pass >= 5) return "PARTIAL";
  return "NOT_READY";
}

async function main() {
  const doc = await loadOpportunitiesCanonical(BETHESDA);
  const before = doc.opportunities || [];

  // Contact resolution pass first (official enrichments, preserves history)
  const resolution = applyContactResolutionPass(before);
  let working = resolution.opportunities;

  const related = buildRelatedOpportunityGroups(working);
  const rows = [];
  const counts = {
    ACTIONABLE_NOW: 0,
    WATCH: 0,
    FUTURE_WATCH: 0,
    INSUFFICIENT: 0,
    CLOSED: 0,
    OTHER: 0,
  };
  let actionCorrections = 0;
  let contactUpgrades = 0;
  let attendanceConfirmed = 0;
  let attendanceEstimated = 0;
  let peakConfirmed = 0;
  let peakEstimated = 0;
  let downgrades = 0;

  const after = working.map((raw) => {
    const priorContact = before.find((b) => b.id === raw.id)?.primaryContact || null;
    let o = applyLiveCommercialQuality(raw, { nowDate: AS_OF });
    o.relatedOpportunityIds = related[raw.id] || [];
    o.relatedOpportunityCount = o.relatedOpportunityIds.length;

    // Preserve history
    o.id = raw.id;
    o.firstSeenAt = raw.firstSeenAt;
    o.firstSeenRunId = raw.firstSeenRunId;
    o.lastSeenAt = raw.lastSeenAt;
    o.lastSeenRunId = raw.lastSeenRunId;
    o.weeklyDeltaState = raw.weeklyDeltaState;
    o.isNewThisWeek = raw.isNewThisWeek;
    o.createdAt = raw.createdAt;
    o.decisionId = raw.decisionId;

    const attPeak = syncAttendancePeak(o);
    o = { ...o, ...attPeak };
    if (attPeak.changes.includes("ATTENDANCE_ENRICHMENT")) {
      if (attPeak.attendanceStatus === "CONFIRMED") attendanceConfirmed += 1;
      else attendanceEstimated += 1;
    }
    if (attPeak.changes.includes("PEAK_ROOMS_ENRICHMENT")) {
      if (attPeak.peakRoomsStatus === "CONFIRMED") peakConfirmed += 1;
      else peakEstimated += 1;
    }

    let state = partitionSalesState(o);

    // Actionable bar: source + thesis + geography + defensible action
    if (state === "ACTIONABLE_NOW") {
      const hasSource = hasMeaningfulSource(o);
      const thesis = Boolean(o.hotelDemandThesis || o.hotelOpportunityThesis);
      const geo = Boolean(
        o.eventLocationSummary || o.destinationStatus || o.venueStatus || o.eventLocationStatus
      );

      // Fix contact display: strip UNKNOWN name
      if (o.primaryContact?.name && /^unknown$/i.test(o.primaryContact.name)) {
        o.priorPrimaryContact = o.priorPrimaryContact || o.primaryContact;
        o.primaryContact = {
          ...o.primaryContact,
          name: null,
          contactQuality: "GENERIC_INBOX",
        };
        o.contactPathClass = classifyContactPath(o.primaryContact, o.backupContacts || []);
      }

      if (priorContact && o.primaryContact) {
        const beforeKey = `${priorContact.name}|${priorContact.email}|${priorContact.phone}`;
        const afterKey = `${o.primaryContact.name}|${o.primaryContact.email}|${o.primaryContact.phone}`;
        if (beforeKey !== afterKey) {
          o.priorPrimaryContact = o.priorPrimaryContact || priorContact;
          contactUpgrades += 1;
        }
      } else if (!priorContact && o.primaryContact) {
        contactUpgrades += 1;
      }

      const precise = buildPreciseAction(o);
      const prevAction = String(raw.recommendedAction || "");
      if (
        !prevAction ||
        /housing provider \/ tournament housing lead/i.test(prevAction) ||
        /contact UNKNOWN/i.test(prevAction) ||
        /Reach out/i.test(prevAction) ||
        prevAction !== precise.recommendedAction
      ) {
        if (prevAction !== precise.recommendedAction) actionCorrections += 1;
        o.recommendedAction = precise.recommendedAction;
        o.suggestedAction = precise.recommendedAction;
        o.actionBasis = precise.actionBasis;
        o.actionReconciledV1 = true;
      }

      const tierNow = contactTier(o);
      if (!hasSource || !thesis || !geo || !o.recommendedAction) {
        // Downgrade — do not invent evidence
        o.priority = PRIORITY.WATCHLIST;
        o.bookingWindowStatus = BOOKING_WINDOW.WATCH;
        o.customerFacingState = "WATCH";
        o.recommendedAction =
          o.recommendedAction ||
          "Monitor — insufficient commercial path for immediate pursuit; gather source/thesis/geography before outreach.";
        o.actionBasis = "downgraded_insufficient_actionable_bar";
        downgrades += 1;
        state = "WATCH";
      } else if (tierNow === "NO_CONTACT" || (!o.primaryContact?.email && !o.primaryContact?.phone)) {
        // Actionable requires a contactable path — downgrade rather than invent WHO
        o.priority = PRIORITY.WATCHLIST;
        o.bookingWindowStatus = BOOKING_WINDOW.WATCH;
        o.customerFacingState = "WATCH";
        o.recommendedAction =
          "Watch — research once a named or functional meetings/housing contact is published on an official source.";
        o.actionBasis = "downgraded_no_contact_path";
        downgrades += 1;
        state = "WATCH";
      }
    } else if (state === "WATCH" || state === "FUTURE_WATCH") {
      if (!/Monitor|Watch|Research once|Establish relationship/i.test(String(o.recommendedAction || ""))) {
        o.recommendedAction =
          state === "FUTURE_WATCH"
            ? "Monitor next-cycle announcement — do not treat as confirmed active pursuit."
            : "Watch — qualify further before outreach; do not force immediate sales action.";
        o.actionBasis =
          state === "FUTURE_WATCH" ? "future_cycle_watch" : "watch_not_immediate_pursuit";
        actionCorrections += 1;
      }
    }

    o.salesPartitionV11 = state;
    o.contactTierV11 = contactTier(o);
    o.contactRoleClassV11 = roleClass(o);
    o.salespersonReadinessV11 = salespersonReady(o, state);
    counts[state] = (counts[state] || 0) + 1;

    rows.push({
      id: o.id,
      title: o.title,
      state,
      readiness: o.salespersonReadinessV11,
      contactTier: o.contactTierV11,
      action: o.recommendedAction,
      source: hasMeaningfulSource(o),
      thesis: Boolean(o.hotelDemandThesis || o.hotelOpportunityThesis),
    });

    return o;
  });

  if (!dryRun) {
    await saveOpportunitiesCanonical(BETHESDA, {
      ...doc,
      hotelId: BETHESDA,
      opportunities: after,
      updatedAt: new Date().toISOString(),
      commercialQualityApplied: `${LIVE_COMMERCIAL_QUALITY_V1}+v1.1_closure`,
      closureMarker: MARKER,
    });
  }

  const actionable = after.filter((o) => o.salesPartitionV11 === "ACTIONABLE_NOW");
  const nA = actionable.length || 1;
  const scorecard = {
    date: pct(
      actionable.filter(
        (o) => o.eventDateDisplay && o.eventDateDisplay !== "Date not yet confirmed"
      ).length,
      nA
    ),
    thesis: pct(
      actionable.filter((o) => o.hotelDemandThesis || o.hotelOpportunityThesis).length,
      nA
    ),
    venue: pct(
      actionable.filter(
        (o) => o.venueSourcingStatus && o.venueSourcingStatus !== "UNKNOWN"
      ).length,
      nA
    ),
    source: pct(actionable.filter((o) => hasMeaningfulSource(o)).length, nA),
    defensibleAction: pct(
      actionable.filter((o) => o.recommendedAction && o.actionBasis).length,
      nA
    ),
    namedWho: pct(
      actionable.filter((o) =>
        ["NAMED_DIRECT", "NAMED_PARTIAL"].includes(o.contactTierV11)
      ).length,
      nA
    ),
    contactable: pct(
      actionable.filter((o) => o.contactTierV11 !== "NO_CONTACT").length,
      nA
    ),
    attendance: pct(
      actionable.filter((o) => o.attendanceStatus && o.attendanceStatus !== "UNKNOWN")
        .length,
      nA
    ),
    peakRooms: pct(
      actionable.filter((o) => o.peakRoomsStatus && o.peakRoomsStatus !== "UNKNOWN")
        .length,
      nA
    ),
  };

  const readyCounts = { READY: 0, PARTIAL: 0, NOT_READY: 0 };
  for (const o of actionable) {
    readyCounts[o.salespersonReadinessV11] =
      (readyCounts[o.salespersonReadinessV11] || 0) + 1;
  }

  const tierCounts = {
    NAMED_DIRECT: 0,
    NAMED_PARTIAL: 0,
    FUNCTIONAL: 0,
    GENERIC: 0,
    NO_CONTACT: 0,
  };
  for (const o of actionable) {
    tierCounts[o.contactTierV11] = (tierCounts[o.contactTierV11] || 0) + 1;
  }

  const out = {
    marker: MARKER,
    asOf: AS_OF,
    apply: !dryRun,
    idsPreserved: before.every((b, i) => b.id === after[i]?.id),
    counts,
    actionableCount: actionable.length,
    scorecard,
    readyCounts,
    tierCounts,
    actionCorrections,
    contactUpgrades,
    downgrades,
    attendanceConfirmed,
    attendanceEstimated,
    peakConfirmed,
    peakEstimated,
    providerSpend: {
      surfeCalls: 0,
      surfeEmailHits: 0,
      surfePhoneHits: 0,
      pdlCalls: 0,
      pdlIncremental: 0,
      note: "Official L1 enrichments + resolution pass only; no Surfe/PDL this closure",
    },
    resolutionSummary: resolution.summary || null,
    rows,
  };

  writeJson(path.join(REPORTS, "gdi-live-commercial-quality-v1-1-closure.json"), out);

  writeMd(path.join(REPORTS, "gdi-live-commercial-quality-v1-1-closure.md"), [
    "# GDI Live Commercial Quality V1.1 Closure",
    "",
    `Marker: \`${MARKER}\``,
    `Apply: **${!dryRun}**`,
    `IDs preserved: **${out.idsPreserved}**`,
    "",
    "## State mix",
    "",
    ...Object.entries(counts).map(([k, v]) => `- ${k}: ${v}`),
    "",
    "## ACTIONABLE_NOW scorecard",
    "",
    `Count: **${actionable.length}**`,
    ...Object.entries(scorecard).map(([k, v]) => `- ${k}: ${v}%`),
    "",
    `READY ${readyCounts.READY} · PARTIAL ${readyCounts.PARTIAL} · NOT_READY ${readyCounts.NOT_READY}`,
    `Contact upgrades: ${contactUpgrades} · Action corrections: ${actionCorrections} · Downgrades: ${downgrades}`,
    "",
  ]);

  console.error(JSON.stringify({
    apply: !dryRun,
    counts,
    actionable: actionable.length,
    scorecard,
    readyCounts,
    tierCounts,
    actionCorrections,
    contactUpgrades,
    downgrades,
  }, null, 2));
}

const isMain =
  process.argv[1] &&
  path.resolve(process.argv[1]) === fileURLToPath(import.meta.url);

if (isMain) {
  main().catch((err) => {
    console.error(err);
    process.exit(1);
  });
}
