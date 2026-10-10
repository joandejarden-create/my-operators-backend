#!/usr/bin/env node
/**
 * Bethesda Pilot Day-2 — HI forensic inventory + ADP attribute reconciliation.
 * Default: dry-run. Pass --apply to sync ADP Attributes only (never rewrites Oct-1 baseline).
 */
import "../load-env.js";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { createHash } from "node:crypto";
import {
  loadHotelIntelligenceFromAirtable,
} from "../lib/hotel-intelligence/schema/hi-airtable-store.js";
import { HI_TABLES } from "../lib/hotel-intelligence/schema/hotel-intelligence-schema-v1.js";
import { buildAdpHotelAttributes } from "../lib/hotel-intelligence/adp-attributes/build-adp-hotel-attributes.js";
import {
  syncHotelAdpAttributesToAirtable,
} from "../lib/hotel-intelligence/adp-attributes/airtable-store.js";
import {
  MAP_HOTEL_ADP_ATTRIBUTE as F,
} from "../lib/hotel-intelligence/adp-attributes/field-map.js";
import Airtable from "airtable";
import { CANONICAL_INTELLIGENCE_BASE_ID } from "../lib/decision-outcomes/airtable-base.js";
import { isHotelIntelligenceComplete } from "../lib/hotel-intelligence/onboarding/hi-completeness-gate.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const OUT = path.join(ROOT, "reports/bethesda-pilot/day2/2026-10-02");
const HOTEL = "recLuxvwwxID7U2B8";
const APPLY = process.argv.includes("--apply");
const TOKEN_ID = "gdisht_47c25d74c79216021fb36150";

fs.mkdirSync(OUT, { recursive: true });

function sha256File(p) {
  return createHash("sha256").update(fs.readFileSync(p)).digest("hex");
}

function getBase() {
  const token = process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT || "";
  const baseId =
    process.env.ADP_AIRTABLE_BASE_ID ||
    process.env.AIRTABLE_INTELLIGENCE_BASE_ID ||
    CANONICAL_INTELLIGENCE_BASE_ID ||
    "appa2cE7FTRmIbB32";
  if (!token) throw new Error("missing_airtable_token");
  return { base: new Airtable({ apiKey: token }).base(baseId), baseId, tokenPresent: true };
}

function escapeFormula(s) {
  return String(s || "").replace(/"/g, '\\"');
}

async function loadAllByHpc(table, hpcField, hotelId) {
  const rows = [];
  await table
    .select({
      filterByFormula: `{${hpcField}} = "${escapeFormula(hotelId)}"`,
      pageSize: 100,
    })
    .eachPage((records, next) => {
      for (const r of records) {
        rows.push({ id: r.id, fields: r.fields || {} });
      }
      next();
    });
  return rows;
}

function classifySourceLineage(attr) {
  const src = String(attr.sourceType || "").toLowerCase();
  const name = String(attr.attributeName || "").toLowerCase();
  const cat = String(attr.attributeCategory || "").toLowerCase();
  if (/commercial|rooms|guest|brand|position/i.test(cat + name)) return "COMMERCIAL_PROFILE";
  if (/event|meeting|ballroom|space|capacity/i.test(cat + name)) return "EVENT_SPACE";
  if (/demand.?node|generator|nih|university|airport/i.test(cat + name)) return "DEMAND_NODE";
  if (/season/i.test(cat + name)) return "SEASONALITY";
  if (/need.?period/i.test(cat + name)) return "NEED_PERIOD";
  if (/market|destination/i.test(cat + name + src)) return "MARKET_RESEARCH";
  if (/static|method/i.test(src)) return "STATIC_METHOD_FIELD";
  if (/derived|composite|computed/i.test(src + cat)) return "DERIVED_COMPOSITE";
  if (attr.sourceRecordId) {
    if (/commercial/i.test(src)) return "COMMERCIAL_PROFILE";
    if (/event/i.test(src)) return "EVENT_SPACE";
    if (/demand/i.test(src)) return "DEMAND_NODE";
    if (/season/i.test(src)) return "SEASONALITY";
  }
  return "OTHER";
}

function classifyDelta(oldVal, newVal, attr) {
  if (oldVal == null && newVal != null) return "EXPECTED_HI_IMPROVEMENT";
  if (oldVal != null && newVal == null) return "UNEXPECTED_DRIFT";
  if (String(oldVal).trim() === String(newVal).trim()) return "UNCHANGED";
  if (String(oldVal).toLowerCase() === String(newVal).toLowerCase()) return "VALUE_NORMALIZATION";
  if (/confidence|source|url|verified/i.test(attr.attributeName || "")) return "SOURCE_CORRECTION";
  return "DERIVATION_CHANGE";
}

async function main() {
  // --- Freeze safety ---
  const day1Dir = path.join(ROOT, "reports/bethesda-pilot/day1/2026-10-01");
  const freezeFiles = {
    baseline: path.join(day1Dir, "ADP_BASELINE_MANIFEST.json"),
    control: path.join(day1Dir, "ADP_CONTROL_QUERY_SET.json"),
    gdiSnap: path.join(day1Dir, "GDI_DAY1_SNAPSHOT.json"),
    pilot: path.join(ROOT, "data/pilots/bethesda-marriott-001/pilot-master.json"),
  };
  const freezeHashes = {};
  for (const [k, p] of Object.entries(freezeFiles)) {
    if (!fs.existsSync(p)) throw new Error(`missing_freeze_artifact:${k}`);
    freezeHashes[k] = sha256File(p);
  }
  const pilot = JSON.parse(fs.readFileSync(freezeFiles.pilot, "utf8"));
  const baseline = JSON.parse(fs.readFileSync(freezeFiles.baseline, "utf8"));
  const control = JSON.parse(fs.readFileSync(freezeFiles.control, "utf8"));
  const gdiSnap = JSON.parse(fs.readFileSync(freezeFiles.gdiSnap, "utf8"));

  const freezeSafety = {
    pilotStatus: pilot.status,
    baselineId: baseline.baselineId,
    baselineImmutable: baseline.immutable === true,
    controlLocked: control.locked === true,
    gdiSnapshotId: gdiSnap.snapshotId,
    gdiReadyFreeze: gdiSnap.counts.strictReady,
    gdiVisibleFreeze: gdiSnap.counts.visible,
    gdiWatchFreeze: gdiSnap.counts.futureWatchStatus,
    shareTokenId: pilot.gdi?.share_token_id,
    shareTokenUnchanged: pilot.gdi?.share_token_id === TOKEN_ID,
    freezeHashes,
    ok:
      pilot.status === "ACTIVE" &&
      baseline.immutable === true &&
      control.locked === true &&
      !!gdiSnap.snapshotId &&
      pilot.gdi?.share_token_id === TOKEN_ID,
  };
  if (!freezeSafety.ok) {
    console.error("FREEZE_SAFETY_FAIL", freezeSafety);
    process.exit(2);
  }

  const { base, baseId } = getBase();
  if (baseId === "appvtnDurnMSjINP6") {
    throw new Error("FORBIDDEN_LEGACY_BASE");
  }

  // --- HI inventory ---
  const hiLoaded = await loadHotelIntelligenceFromAirtable(HOTEL);
  const commercialObj = hiLoaded.commercial || null;
  const commercial = commercialObj ? [commercialObj] : [];
  const eventSpaces = hiLoaded.eventSpaces || [];
  const demandNodes = hiLoaded.demandNodes || [];
  const seasonality = hiLoaded.seasonality || [];
  const needPeriods = hiLoaded.needPeriods || [];
  const evidence = hiLoaded.evidence || [];

  const { HOTEL_ADP_ATTRIBUTES_TABLE } = await import(
    "../lib/hotel-intelligence/adp-attributes/field-map.js"
  );
  const attrTbl = base(HOTEL_ADP_ATTRIBUTES_TABLE);
  const allAttrs = await loadAllByHpc(attrTbl, F.hpcHotelId, HOTEL);
  const activeAttrs = allAttrs.filter((r) => r.fields?.[F.active] === true || r.fields?.[F.active] === 1);
  const inactiveAttrs = allAttrs.filter((r) => !(r.fields?.[F.active] === true || r.fields?.[F.active] === 1));

  // Duplicate-active check
  const activeKeys = new Map();
  let duplicateActive = 0;
  for (const r of activeAttrs) {
    const key = r.fields?.[F.dedupeKey] || r.fields?.[F.attributeKey];
    if (!key) continue;
    if (activeKeys.has(key)) duplicateActive += 1;
    else activeKeys.set(key, r.id);
  }

  const hiInventory = {
    hotelId: HOTEL,
    baseId,
    loadedAt: new Date().toISOString(),
    counts: {
      commercial: commercial.length,
      eventSpaces: eventSpaces.length,
      demandNodes: demandNodes.length,
      seasonality: seasonality.length,
      needPeriods: needPeriods.length,
      evidence: evidence.length,
      adpAttributesTotal: allAttrs.length,
      adpAttributesActive: activeAttrs.length,
      adpAttributesInactive: inactiveAttrs.length,
      duplicateActive,
    },
    commercial: commercial.map((r) => ({
      id: r.airtableRecordId,
      hotelName: r.hotelName,
      brand: r.brand,
      roomsKeys: r.roomsKeys,
      totalMeetingSpaceSqFt: r.totalMeetingSpaceSqFt,
      meetingRoomCount: r.meetingRoomCount,
      largestMeetingSpaceSqFt: r.largestMeetingSpaceSqFt,
      largestEventCapacity: r.largestEventCapacity,
      confidence: r.confidence,
      researchStatus: r.researchStatus,
      conflictStatus: r.conflictStatus,
      officialPropertyUrl: r.officialPropertyUrl,
      officialEventsUrl: r.officialEventsUrl,
    })),
    eventSpaces: eventSpaces.map((r) => ({
      id: r.airtableRecordId,
      name: r.name,
      type: r.type,
      sqFt: r.sqFt,
      theaterCapacity: r.theaterCapacity,
      confidence: r.confidence,
      sourceUrl: r.sourceUrl,
    })),
    demandNodes: demandNodes.map((r) => ({
      id: r.airtableRecordId,
      name: r.name,
      type: r.type,
      confidence: r.confidence,
      demandStrength: r.demandStrength,
      sourceUrl: r.sourceUrl,
    })),
    seasonality: seasonality.map((r) => ({
      id: r.airtableRecordId,
      periodType: r.periodType,
      label: r.label,
      hotelSupplied: r.hotelSupplied === true,
      confidence: r.confidence,
      sourceUrl: r.sourceUrl,
    })),
    needPeriods: needPeriods.map((r) => ({
      id: r.airtableRecordId,
      periodType: r.periodType,
      label: r.label,
      hotelSupplied: r.hotelSupplied === true,
    })),
    evidence: evidence.map((r) => ({
      id: r.airtableRecordId,
      evidenceId: r.evidenceId,
      entityType: r.entityType,
      fieldName: r.fieldName,
      source: r.sourceName,
      url: r.sourceUrl,
      confidence: r.confidence,
    })),
    hotelNeedPeriodStatus:
      needPeriods.some(
        (r) =>
          r.hotelSupplied === true || /Hotel-Supplied/i.test(String(r.periodType || ""))
      )
        ? "PROVIDED"
        : "NOT_PROVIDED",
  };

  // --- Completeness ---
  const completeness = await isHotelIntelligenceComplete(HOTEL);

  // --- Build regenerated attributes (dry) ---
  const packet = await buildAdpHotelAttributes(HOTEL);
  const beforeActiveMap = new Map();
  for (const r of activeAttrs) {
    const key = r.fields?.[F.dedupeKey] || r.fields?.[F.attributeKey];
    if (!key) continue;
    beforeActiveMap.set(key, {
      id: r.id,
      name: r.fields?.[F.attributeName],
      value: r.fields?.[F.attributeValue],
      category: r.fields?.[F.attributeCategory],
      sourceType: r.fields?.[F.sourceType],
      sourceRecordId: r.fields?.[F.sourceRecordId],
      confidence: r.fields?.[F.confidence],
    });
  }

  const afterMap = new Map();
  for (const a of packet.attributes || []) {
    afterMap.set(a.dedupeKey, a);
  }

  const deltas = [];
  const allKeys = new Set([...beforeActiveMap.keys(), ...afterMap.keys()]);
  for (const key of allKeys) {
    const oldA = beforeActiveMap.get(key);
    const newA = afterMap.get(key);
    const oldVal = oldA?.value ?? null;
    const newVal = newA?.attributeValue ?? null;
    const cls = classifyDelta(oldVal, newVal, newA || oldA || {});
    if (cls === "UNCHANGED") continue;
    deltas.push({
      attributeKey: key,
      attributeName: newA?.attributeName || oldA?.name,
      old: oldVal,
      new: newVal,
      classification: cls,
      sourceType: newA?.sourceType || oldA?.sourceType,
      sourceRecordId: newA?.sourceRecordId || oldA?.sourceRecordId,
      lineage: classifySourceLineage(newA || oldA || {}),
    });
  }

  // Source parity for regenerated (desired) attributes
  const parityRows = [];
  let missingSource = 0;
  let valueMismatch = 0;
  let staleActive = 0;
  for (const a of packet.attributes || []) {
    const lineage = classifySourceLineage(a);
    const hasSource =
      !!a.sourceRecordId ||
      !!a.sourceUrl ||
      !!a.sourceName ||
      ["STATIC_METHOD_FIELD", "DERIVED_COMPOSITE", "MARKET_RESEARCH"].includes(lineage);
    let parity = "PASS";
    if (!hasSource) {
      parity = "MISSING_SOURCE";
      missingSource += 1;
    } else if (a.sourceRecordId) {
      const knownIds = new Set([
        ...commercial.map((r) => r.airtableRecordId),
        ...eventSpaces.map((r) => r.airtableRecordId),
        ...demandNodes.map((r) => r.airtableRecordId),
        ...seasonality.map((r) => r.airtableRecordId),
        ...needPeriods.map((r) => r.airtableRecordId),
        ...evidence.map((r) => r.airtableRecordId),
      ]);
      if (!knownIds.has(a.sourceRecordId) && !a.sourceUrl && !a.sourceName) {
        parity = "AMBIGUOUS";
      }
    }
    parityRows.push({
      attribute: a.attributeName,
      key: a.dedupeKey,
      value: a.attributeValue,
      canonicalSourceTable: lineage,
      sourceRecord: a.sourceRecordId || "",
      evidence: a.sourceUrl || a.sourceName || "",
      derived: lineage === "DERIVED_COMPOSITE",
      derivation: a.sourceType || "",
      parityResult: parity,
    });
  }

  // Stale actives = active in Airtable but not in regenerated packet
  for (const [key, oldA] of beforeActiveMap) {
    if (!afterMap.has(key)) {
      staleActive += 1;
      deltas.push({
        attributeKey: key,
        attributeName: oldA.name,
        old: oldA.value,
        new: null,
        classification: "STALE_ACTIVE",
        sourceType: oldA.sourceType,
        sourceRecordId: oldA.sourceRecordId,
        lineage: classifySourceLineage(oldA),
      });
    }
  }

  const unexpected = deltas.filter((d) =>
    ["UNEXPECTED_DRIFT", "MISSING_SOURCE", "DUPLICATE_ACTIVE"].includes(d.classification)
  );

  // Sync (dry or apply)
  const syncResult = await syncHotelAdpAttributesToAirtable(packet, {
    dryRun: !APPLY,
  });

  // Post-apply reload counts if applied
  let afterActiveCount = packet.attributes?.length || 0;
  let afterInactiveCount = inactiveAttrs.length;
  let afterDupes = duplicateActive;
  if (APPLY && syncResult.ok) {
    const reloaded = await loadAllByHpc(attrTbl, F.hpcHotelId, HOTEL);
    const act = reloaded.filter((r) => r.fields?.[F.active] === true || r.fields?.[F.active] === 1);
    const inact = reloaded.filter((r) => !(r.fields?.[F.active] === true || r.fields?.[F.active] === 1));
    afterActiveCount = act.length;
    afterInactiveCount = inact.length;
    const keys = new Map();
    afterDupes = 0;
    for (const r of act) {
      const k = r.fields?.[F.dedupeKey] || r.fields?.[F.attributeKey];
      if (!k) continue;
      if (keys.has(k)) afterDupes += 1;
      else keys.set(k, r.id);
    }
  }

  // Freeze hashes after (must match)
  const freezeHashesAfter = {};
  for (const [k, p] of Object.entries(freezeFiles)) {
    freezeHashesAfter[k] = sha256File(p);
  }
  const freezeUnchanged = Object.keys(freezeHashes).every(
    (k) => freezeHashes[k] === freezeHashesAfter[k]
  );

  const hiApplyReport = {
    hotelId: HOTEL,
    mode: APPLY ? "APPLY" : "DRY_RUN",
    freezeSafety,
    hiInventorySummary: hiInventory.counts,
    hotelNeedPeriodStatus: hiInventory.hotelNeedPeriodStatus,
    completeness: {
      complete: completeness.complete,
      overallStatus: completeness.overallStatus,
      coverage: completeness.coverage,
      blockingDomains: completeness.blockingDomains,
    },
    hiRecordsCreated: 0,
    hiRecordsUpdated: 0,
    note: "HI canonical rows already present; Day-2 did not create duplicate HI packets. ADP Attributes regenerated from current HI.",
    generatedAt: new Date().toISOString(),
  };

  const reconciliation = {
    hotelId: HOTEL,
    mode: APPLY ? "APPLY" : "DRY_RUN",
    activeBefore: activeAttrs.length,
    activeAfter: afterActiveCount,
    inactiveSuperseded: afterInactiveCount,
    duplicateActiveBefore: duplicateActive,
    duplicateActiveAfter: afterDupes,
    proposedAttributeCount: packet.attributes?.length || 0,
    missingCritical: packet.missingCritical || [],
    profileCompleteness: packet.profileCompleteness || null,
    deltas,
    unexpectedDriftCount: unexpected.length,
    unexpected,
    syncResult,
    freezeUnchanged,
    freezeHashesBefore: freezeHashes,
    freezeHashesAfter,
    generatedAt: new Date().toISOString(),
  };

  // CSV parity
  const csvLines = [
    "Attribute,Value,Canonical Source Table,Source Record,Evidence,Derived?,Derivation,Parity Result",
  ];
  for (const row of parityRows) {
    csvLines.push(
      [
        csv(row.attribute),
        csv(row.value),
        csv(row.canonicalSourceTable),
        csv(row.sourceRecord),
        csv(row.evidence),
        row.derived ? "YES" : "NO",
        csv(row.derivation),
        row.parityResult,
      ].join(",")
    );
  }
  function csv(v) {
    const s = String(v ?? "");
    if (/[",\n]/.test(s)) return `"${s.replace(/"/g, '""')}"`;
    return s;
  }

  fs.writeFileSync(path.join(OUT, "HI_APPLY_REPORT.json"), JSON.stringify(hiApplyReport, null, 2));
  fs.writeFileSync(path.join(OUT, "HI_FORENSIC_INVENTORY.json"), JSON.stringify(hiInventory, null, 2));
  fs.writeFileSync(
    path.join(OUT, "ADP_ATTRIBUTE_RECONCILIATION.json"),
    JSON.stringify(reconciliation, null, 2)
  );
  fs.writeFileSync(path.join(OUT, "ADP_ATTRIBUTE_SOURCE_PARITY.csv"), csvLines.join("\n"));
  fs.writeFileSync(
    path.join(OUT, "ADP_ATTRIBUTE_SOURCE_PARITY.json"),
    JSON.stringify(
      {
        missingSource,
        valueMismatch,
        staleActive,
        duplicateActive: afterDupes,
        parityPass:
          missingSource === 0 &&
          valueMismatch === 0 &&
          afterDupes === 0,
        rows: parityRows,
      },
      null,
      2
    )
  );

  console.log(
    JSON.stringify(
      {
        ok: true,
        mode: APPLY ? "APPLY" : "DRY_RUN",
        freezeOk: freezeSafety.ok,
        freezeUnchanged,
        hiComplete: completeness.complete,
        hotelNeedPeriodStatus: hiInventory.hotelNeedPeriodStatus,
        activeBefore: activeAttrs.length,
        activeAfter: afterActiveCount,
        inactive: afterInactiveCount,
        duplicateActive: afterDupes,
        missingSource,
        staleActive,
        unexpectedDrift: unexpected.length,
        syncOk: syncResult.ok !== false,
        creates: syncResult.createCount,
        updates: syncResult.updateCount,
        deactivates: syncResult.deactivateCount,
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
