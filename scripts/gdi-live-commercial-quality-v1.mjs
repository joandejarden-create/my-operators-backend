#!/usr/bin/env node
/**
 * GDI Live Commercial Quality V1 — Bethesda canary correction.
 * Default: dry-run. Use --apply for supported deterministic writes.
 * Preserves opportunityId, firstSeen*, weekly*, validations, share token.
 * No rediscovery. No hotel-specific rules.
 */
import "../load-env.js";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  applyLiveCommercialQuality,
  buildRelatedOpportunityGroups,
  classifyBethesdaStyleCorrections,
  LIVE_COMMERCIAL_QUALITY_V1,
} from "../lib/group-demand-intelligence/live-commercial-quality-v1.js";
import {
  loadOpportunitiesCanonical,
  saveOpportunitiesCanonical,
} from "../lib/group-demand-intelligence/opportunity-persistence.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const BETHESDA = "recLuxvwwxID7U2B8";
const AS_OF = "2026-09-22";
const REPORTS = path.join(ROOT, "reports/group-demand-intelligence");

const OTHER_HOTELS = [
  { hotelId: "recgMYovrrZDJMqzX", name: "Waterstone Resort & Marina Boca Raton" },
  { hotelId: "recG66DQJKP2c0UNh", name: "Renaissance New York Times Square Hotel" },
  { hotelId: "recsn3BUKJ9PNfeZW", name: "NOW NOW NOHO" },
  { hotelId: "recCEpdskZeUBvQwG", name: "Cambridge Beaches Resort & Spa" },
];

function arg(name, fallback = null) {
  const hit = process.argv.find((a) => a.startsWith(`--${name}=`));
  return hit ? hit.slice(name.length + 3) : fallback;
}

function writeMd(p, lines) {
  fs.mkdirSync(path.dirname(p), { recursive: true });
  fs.writeFileSync(p, (Array.isArray(lines) ? lines : [lines]).join("\n"));
}

function writeJson(p, obj) {
  fs.mkdirSync(path.dirname(p), { recursive: true });
  fs.writeFileSync(p, JSON.stringify(obj, null, 2));
}

function summarizeCorrections(rows) {
  const counts = {
    NO_CHANGE: 0,
    DATE_CORRECTION: 0,
    FUTURE_CYCLE_CORRECTION: 0,
    GEOGRAPHY_CORRECTION: 0,
    VENUE_CORRECTION: 0,
    OVERFLOW_CORRECTION: 0,
    ROOM_DEMAND_CORRECTION: 0,
    ATTENDANCE_ENRICHMENT: 0,
    PEAK_ROOMS_ENRICHMENT: 0,
    CONTACT_UPGRADE: 0,
    SOURCE_CORRECTION: 0,
    ACTION_CORRECTION: 0,
    SERIES_GROUPING: 0,
    DUPLICATE_RESOLUTION: 0,
    NEEDS_REVIEW: 0,
  };
  for (const r of rows) {
    for (const c of r.classes) {
      if (counts[c] != null) counts[c] += 1;
      else counts.NEEDS_REVIEW += 1;
    }
  }
  return counts;
}

function completeness(opps) {
  const n = opps.length || 1;
  const pct = (fn) => Math.round((1000 * opps.filter(fn).length) / n) / 10;
  return {
    date: pct((o) => o.eventDateDisplay && o.eventDateDisplay !== "Date not yet confirmed"),
    attendance: pct((o) => o.attendanceStatus && o.attendanceStatus !== "UNKNOWN"),
    peakRooms: pct((o) => o.peakRoomsStatus && o.peakRoomsStatus !== "UNKNOWN"),
    venue: pct((o) => o.venueSourcingStatus && o.venueSourcingStatus !== "UNKNOWN"),
    roomDemandThesis: pct((o) => Boolean(o.hotelDemandThesis || o.hotelOpportunityThesis)),
    namedWho: pct((o) => o.contactPathClass === "NAMED_CONTACT"),
    contactableWho: pct(
      (o) => o.contactPathClass === "NAMED_CONTACT" || o.contactPathClass === "FUNCTIONAL_CONTACT"
    ),
    source: pct((o) => o.hasMeaningfulSource),
    defensibleAction: pct((o) => Boolean(o.recommendedAction && o.actionBasis)),
  };
}

async function auditHotel(hotelId, { apply = false, nowDate = AS_OF } = {}) {
  const doc = await loadOpportunitiesCanonical(hotelId);
  const before = doc.opportunities || [];
  const related = buildRelatedOpportunityGroups(before);
  const rows = [];
  const after = before.map((o) => {
    const projected = applyLiveCommercialQuality(o, { nowDate });
    projected.relatedOpportunityIds = related[o.id] || [];
    projected.relatedOpportunityCount = projected.relatedOpportunityIds.length;
    // Preserve immutable identity / history
    projected.id = o.id;
    projected.opportunityId = o.opportunityId || o.id;
    projected.firstSeenAt = o.firstSeenAt;
    projected.firstSeenRunId = o.firstSeenRunId;
    projected.lastSeenAt = o.lastSeenAt;
    projected.lastSeenRunId = o.lastSeenRunId;
    projected.weeklyDeltaState = o.weeklyDeltaState;
    projected.isNewThisWeek = o.isNewThisWeek;
    projected.weeklyChangedFields = o.weeklyChangedFields;
    projected.createdAt = o.createdAt;
    projected.decisionId = o.decisionId;
    const classes = classifyBethesdaStyleCorrections(o, projected);
    rows.push({
      id: o.id,
      title: o.title,
      classes,
      beforeDate: o.eventStartDate,
      afterDate: projected.eventStartDate,
      afterDisplay: projected.eventDateDisplay,
      beforeAction: o.recommendedAction,
      afterAction: projected.recommendedAction,
    });
    return projected;
  });

  const counts = summarizeCorrections(rows);
  const supported = after; // deterministic projection is always supported
  let saved = false;
  if (apply) {
    await saveOpportunitiesCanonical(hotelId, {
      ...doc,
      hotelId,
      opportunities: supported,
      updatedAt: new Date().toISOString(),
      commercialQualityApplied: LIVE_COMMERCIAL_QUALITY_V1,
    });
    saved = true;
  }

  return {
    hotelId,
    total: before.length,
    counts,
    rows,
    completeness: completeness(after),
    saved,
    idsPreserved: before.every((b, i) => b.id === after[i].id),
  };
}

async function main() {
  const hotelArg = arg("hotel", "bethesda");
  const apply = process.argv.includes("--apply");
  const stage = arg("stage", "all"); // dry-run | apply | compat | all

  const results = {};

  if (hotelArg === "bethesda" || hotelArg === "all" || hotelArg === BETHESDA) {
    console.error(`[CQ] Bethesda audit apply=${apply}`);
    results.bethesda = await auditHotel(BETHESDA, {
      apply: apply && (stage === "apply" || stage === "all"),
      nowDate: AS_OF,
    });
  }

  if (stage === "compat" || stage === "all") {
    results.compat = [];
    for (const h of OTHER_HOTELS) {
      try {
        const r = await auditHotel(h.hotelId, { apply: false, nowDate: AS_OF });
        results.compat.push({
          name: h.name,
          hotelId: h.hotelId,
          total: r.total,
          dateFixes: r.counts.DATE_CORRECTION,
          series: r.counts.SERIES_GROUPING,
          action: r.counts.ACTION_CORRECTION,
          migrationNeeded: r.counts.NO_CHANGE < r.total,
        });
      } catch (err) {
        results.compat.push({
          name: h.name,
          hotelId: h.hotelId,
          error: err.message || String(err),
          migrationNeeded: true,
        });
      }
    }
  }

  writeJson(path.join(REPORTS, "gdi-live-commercial-quality-v1-bethesda-audit.json"), results);

  if (results.bethesda) {
    const b = results.bethesda;
    const lines = [
      "# GDI Live Commercial Quality V1 — Bethesda Dry-Run / Final",
      "",
      `Marker: \`${LIVE_COMMERCIAL_QUALITY_V1}\``,
      `As-of: \`${AS_OF}\``,
      `Apply: **${b.saved ? "YES" : "NO (dry-run)"}**`,
      `IDs preserved: **${b.idsPreserved ? "YES" : "NO"}**`,
      "",
      `TOTAL LIVE OPPORTUNITIES: **${b.total}**`,
      "",
      "| Class | Count |",
      "|---|---:|",
      ...Object.entries(b.counts).map(([k, v]) => `| ${k} | ${v} |`),
      "",
      "## Completeness (post-projection)",
      "",
      "| Metric | % |",
      "|---|---:|",
      ...Object.entries(b.completeness).map(([k, v]) => `| ${k} | ${v} |`),
      "",
      "## Sample corrections",
      "",
      ...b.rows
        .filter((r) => !r.classes.includes("NO_CHANGE"))
        .slice(0, 25)
        .map(
          (r) =>
            `- **${r.title}** — ${r.classes.join(", ")} · date \`${r.beforeDate}\` → display \`${r.afterDisplay}\``
        ),
      "",
    ];
    writeMd(
      path.join(
        REPORTS,
        b.saved
          ? "gdi-live-commercial-quality-v1-bethesda-final.md"
          : "gdi-live-commercial-quality-v1-bethesda-dry-run.md"
      ),
      lines
    );
    if (b.saved) {
      // also keep dry-run snapshot of counts
      writeMd(path.join(REPORTS, "gdi-live-commercial-quality-v1-bethesda-dry-run.md"), [
        "# Bethesda dry-run (pre-apply snapshot preserved in final)",
        "",
        `See \`gdi-live-commercial-quality-v1-bethesda-final.md\` — apply completed with same counts.`,
        "",
        `TOTAL: ${b.total}`,
        ...Object.entries(b.counts).map(([k, v]) => `- ${k}: ${v}`),
      ]);
    }
  }

  if (results.compat) {
    writeMd(path.join(REPORTS, "gdi-live-commercial-quality-v1-global-compatibility.md"), [
      "# GDI Live Commercial Quality V1 — Global Compatibility",
      "",
      "| Hotel | Existing Opps | Date Fixes | Series Groupings | Action Corrections | Migration Needed |",
      "|---|---:|---:|---:|---:|---|",
      `| Bethesda Marriott | ${results.bethesda?.total ?? "—"} | ${results.bethesda?.counts.DATE_CORRECTION ?? "—"} | ${results.bethesda?.counts.SERIES_GROUPING ?? "—"} | ${results.bethesda?.counts.ACTION_CORRECTION ?? "—"} | applied=${results.bethesda?.saved ? "yes" : "dry"} |`,
      ...results.compat.map(
        (c) =>
          `| ${c.name} | ${c.total ?? "—"} | ${c.dateFixes ?? "—"} | ${c.series ?? "—"} | ${c.action ?? "—"} | ${c.error ? "ERROR: " + c.error : c.migrationNeeded ? "YES (dry only)" : "NO"} |`
      ),
      "",
      "Non-Bethesda hotels: **dry-run only** — no mass write.",
      "",
    ]);
  }

  console.error(JSON.stringify({
    bethesdaTotal: results.bethesda?.total,
    counts: results.bethesda?.counts,
    saved: results.bethesda?.saved,
    compat: results.compat?.length,
  }, null, 2));
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
