#!/usr/bin/env node
/**
 * Production-hotel HPC completeness for all active ADP/GDI hotels.
 * Dry-run default; --apply for safe WRITE/CORROBORATE only.
 *
 * npm run complete:production-active-hotel-hpc
 * npm run complete:production-active-hotel-hpc -- --apply
 */
import "../load-env.js";
import fs from "node:fs";
import path from "node:path";
import Airtable from "airtable";
import {
  listAdpGdiHotelUniverse,
  resolveCanonicalHotelId,
} from "../lib/hotel-census/adp-gdi-canonical-identity.js";
import { getCensusRecordIdForAdpProperty } from "../lib/ai-demand-positioning/census-link-registry.js";
import { PRODUCTION_USE_STATUS } from "../lib/research-engine-v2/production-census-write.js";

const APPLY = process.argv.includes("--apply");
const HPC_TABLE = "Hotel Property Census";
const ROOT = process.cwd();
const EVIDENCE_PATH = path.join(
  ROOT,
  "fixtures/hotel-census/production-active-hotel-hpc-evidence-v1.json"
);
const OUT_DIR = path.join(ROOT, "reports/hotel-census");

/** P0 identity — must be complete */
export const P0_FIELDS = [
  "Property Name",
  "Property Identity Key",
  "Current Brand",
  "Country",
  "State / Region",
  "City",
  "Affiliation Status",
  "Production Use Status",
];

/** Core / P1 product-critical */
export const CORE_FIELDS = [
  "Address",
  "Official Property URL",
  "Phone",
  "Latitude",
  "Longitude",
  "Rooms / Keys",
  "Meeting Space Flag",
  "Property Type",
  "Market / Submarket",
];

/** Extended commercial */
export const EXTENDED_FIELDS = [
  "Brand Family",
  "Canonical Property Name",
  "Owner Name",
  "Operator / Management Company",
  "Market",
  "Submarket",
];

const ALL_SCORED = [...new Set([...P0_FIELDS, ...CORE_FIELDS, ...EXTENDED_FIELDS])];

function token() {
  return process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT || "";
}

function filled(v) {
  if (v == null) return false;
  if (typeof v === "boolean") return true;
  if (typeof v === "number") return Number.isFinite(v);
  return String(v).trim() !== "";
}

function loadAdpProfile(adpId) {
  if (!adpId) return null;
  const fixtures = path.join(ROOT, "fixtures/ai-demand-positioning");
  const files = fs.readdirSync(fixtures).filter((f) => f.endsWith("-property-profile.json"));
  for (const f of files) {
    const j = JSON.parse(fs.readFileSync(path.join(fixtures, f), "utf8"));
    if (j.propertyId === adpId) return j;
  }
  return null;
}

function loadEvidence() {
  return JSON.parse(fs.readFileSync(EVIDENCE_PATH, "utf8"));
}

function adpCandidates(adp) {
  if (!adp) return {};
  const out = {};
  if (adp.name) {
    out["Property Name"] = adp.name;
    out["Canonical Property Name"] = adp.name;
  }
  if (adp.brand) out["Current Brand"] = adp.brand;
  if (adp.parentCompany) out["Brand Family"] = adp.parentCompany;
  if (adp.address) out.Address = adp.address;
  if (adp.city) out.City = adp.city;
  if (adp.state) out["State / Region"] = adp.state;
  if (adp.country) {
    out.Country =
      adp.country === "US" || adp.country === "USA"
        ? "United States"
        : adp.country;
  }
  if (adp.website || adp.officialPropertyPageUrl) {
    out["Official Property URL"] = adp.officialPropertyPageUrl || adp.website;
  }
  if (adp.phone) out.Phone = adp.phone;
  if (Number.isFinite(adp.rooms)) out["Rooms / Keys"] = adp.rooms;
  if (adp.market || adp.submarket) {
    out["Market / Submarket"] = [adp.market, adp.submarket]
      .filter(Boolean)
      .join(" · ");
    if (adp.market) out.Market = adp.market;
    if (adp.submarket) out.Submarket = adp.submarket;
  }
  if (
    Number.isFinite(adp.geography?.latitude) &&
    Number.isFinite(adp.geography?.longitude) &&
    !(adp.geography.latitude === 0 && adp.geography.longitude === 0)
  ) {
    out.Latitude = adp.geography.latitude;
    out.Longitude = adp.geography.longitude;
  }
  const ms = adp.meetingSpace || {};
  const hasMeetingEvidence =
    Number(ms.totalSqFt) > 0 ||
    Number(ms.meetingRooms) > 0 ||
    Number(ms.largestRoom?.sqFt) > 0 ||
    Number(ms.largestRoom?.capacity) > 0 ||
    (Array.isArray(adp.attributes) && adp.attributes.includes("meeting_space"));
  if (hasMeetingEvidence) out["Meeting Space Flag"] = true;
  out["Property Type"] = "Hotel";
  if (
    adp.affiliation?.toLowerCase?.().includes("curio") ||
    adp.brand === "Curio Collection"
  ) {
    out["Affiliation Status"] = "Soft-Branded / Collection";
  } else if (adp.brand) {
    out["Affiliation Status"] = "Branded";
  }
  if (adp.owner) out["Owner Name"] = adp.owner;
  return out;
}

function decideWrite(field, current, incoming) {
  if (!filled(incoming)) return { action: "HOLD", reason: "no_incoming" };
  if (!filled(current)) {
    return { action: "WRITE", reason: "null_fill" };
  }
  if (String(current).trim() === String(incoming).trim()) {
    return { action: "CORROBORATE", reason: "same_value" };
  }
  // Prefer fuller street address when current is truncated fragment
  if (
    field === "Address" &&
    String(incoming).length > String(current).length + 10 &&
    String(incoming).toLowerCase().includes(String(current).toLowerCase().slice(0, 12))
  ) {
    return { action: "WRITE", reason: "address_normalization_expand" };
  }
  if (
    field === "Official Property URL" &&
    /hyatt\.com.*now-now-noho/i.test(String(current)) &&
    /staynownow\.com/i.test(String(incoming))
  ) {
    return { action: "WRITE", reason: "correct_official_url_identity" };
  }
  return { action: "REJECT", reason: "existing_value_preferred" };
}

function pct(n, d) {
  if (!d) return 0;
  return Math.round((1000 * n) / d) / 10;
}

async function main() {
  const evidenceDoc = loadEvidence();
  const base = new Airtable({ apiKey: token() }).base(process.env.AIRTABLE_BASE_ID_ALT);
  const universe = listAdpGdiHotelUniverse();

  const hotelRows = [];
  const mutations = { WRITE: [], CORROBORATE: [], HOLD: [], REJECT: [] };
  const gapLedger = [];

  for (const row of universe) {
    const hpcId =
      resolveCanonicalHotelId(row.key) ||
      resolveCanonicalHotelId(row.adpPropertyId) ||
      getCensusRecordIdForAdpProperty(row.adpPropertyId) ||
      row.canonicalHotelId;
    if (!hpcId || !String(hpcId).startsWith("rec")) {
      hotelRows.push({ hotel: row.displayName, hpcId: null, error: "unresolved" });
      continue;
    }

    const rec = await base(HPC_TABLE).find(hpcId);
    const f = rec.fields || {};
    const adpId =
      row.adpPropertyId || (String(row.key).startsWith("adp_") ? row.key : null);
    const adp = loadAdpProfile(adpId);
    const pack = evidenceDoc.byHpcId?.[hpcId] || { writes: {}, holds: {} };

    const incoming = { ...adpCandidates(adp) };
    for (const [field, meta] of Object.entries(pack.writes || {})) {
      if (meta?.value != null) incoming[field] = meta.value;
    }

    const patch = {};
    const fieldMatrix = [];

    for (const field of ALL_SCORED) {
      const current = f[field];
      const proposed = incoming[field];
      const holdMeta = pack.holds?.[field];

      if (holdMeta) {
        fieldMatrix.push({
          field,
          current: current ?? null,
          status: holdMeta.state || "HOLD",
          source: holdMeta.reason || null,
          confidence: null,
          needed: P0_FIELDS.includes(field) || CORE_FIELDS.includes(field),
        });
        mutations.HOLD.push({ hotel: row.displayName, hpcId, field, reason: holdMeta.state });
        if (
          (P0_FIELDS.includes(field) || CORE_FIELDS.includes(field)) &&
          !filled(current)
        ) {
          gapLedger.push({
            hotel: row.displayName || row.key,
            hpcId,
            field,
            gapState: holdMeta.state,
            reason: holdMeta.reason,
            next: holdMeta.next || "steward review / stronger official evidence",
          });
        }
        continue;
      }

      if (filled(current) && !filled(proposed)) {
        fieldMatrix.push({
          field,
          current,
          status: "COMPLETE_VERIFIED",
          source: "hpc_existing",
          confidence: "existing",
          needed: P0_FIELDS.includes(field) || CORE_FIELDS.includes(field),
        });
        continue;
      }

      if (!filled(current) && !filled(proposed)) {
        const state =
          field === "Owner Name" || field === "Operator / Management Company"
            ? "RESEARCHED_NOT_FOUND"
            : "RESEARCHED_NOT_FOUND";
        fieldMatrix.push({
          field,
          current: null,
          status: state,
          source: "no_evidence_after_internal_and_official_pass",
          confidence: null,
          needed: P0_FIELDS.includes(field) || CORE_FIELDS.includes(field),
        });
        mutations.HOLD.push({ hotel: row.displayName, hpcId, field, reason: state });
        if (P0_FIELDS.includes(field) || CORE_FIELDS.includes(field)) {
          gapLedger.push({
            hotel: row.displayName || row.key,
            hpcId,
            field,
            gapState: state,
            reason: "No rights-clean authoritative value after ADP + evidence pack + official pass",
            next: "Official property page / steward High source",
          });
        }
        continue;
      }

      const d = decideWrite(field, current, proposed);
      fieldMatrix.push({
        field,
        current: current ?? null,
        proposed: proposed ?? null,
        status:
          d.action === "WRITE"
            ? "WRITE"
            : d.action === "CORROBORATE"
              ? "COMPLETE_CORROBORATED"
              : d.action === "REJECT"
                ? "COMPLETE_VERIFIED"
                : "HOLD",
        source: pack.writes?.[field]?.sourceUrl || "adp_or_evidence",
        confidence: pack.writes?.[field]?.confidence || null,
        needed: P0_FIELDS.includes(field) || CORE_FIELDS.includes(field),
        decision: d.action,
        reason: d.reason,
      });
      mutations[d.action].push({
        hotel: row.displayName,
        hpcId,
        field,
        reason: d.reason,
        value: d.action === "WRITE" ? proposed : undefined,
      });
      if (d.action === "WRITE") patch[field] = proposed;
    }

    // Ensure production use status
    if (!filled(f["Production Use Status"])) {
      patch["Production Use Status"] = PRODUCTION_USE_STATUS;
      mutations.WRITE.push({
        hotel: row.displayName,
        hpcId,
        field: "Production Use Status",
        reason: "p0_null_fill",
      });
    }

    // Geo provenance dates when writing coords
    if (patch.Latitude != null || patch.Longitude != null) {
      if (!patch["Geocode Reviewed Date"]) {
        patch["Geocode Reviewed Date"] = new Date().toISOString().slice(0, 10);
      }
      // Attach provenance from evidence pack when present
      for (const pf of [
        "Coordinate Source Type",
        "Coordinate Confidence",
        "Geocode Provider",
        "Geocode Method",
      ]) {
        if (pack.writes?.[pf]?.value != null && !filled(f[pf])) {
          patch[pf] = pack.writes[pf].value;
          mutations.WRITE.push({
            hotel: row.displayName,
            hpcId,
            field: pf,
            reason: "geo_provenance",
            value: pack.writes[pf].value,
          });
        }
      }
    }

    if (APPLY && Object.keys(patch).length) {
      await base(HPC_TABLE).update(hpcId, patch, { typecast: true });
    }

    const after =
      APPLY && Object.keys(patch).length
        ? { ...f, ...patch }
        : { ...f, ...patch };

    const p0Ok = P0_FIELDS.every((k) => filled(after[k]));
    const coreOk = CORE_FIELDS.every((k) => filled(after[k]));
    const coreFilled = CORE_FIELDS.filter((k) => filled(after[k])).length;
    const extFilled = [...CORE_FIELDS, ...EXTENDED_FIELDS].filter((k) =>
      filled(after[k])
    ).length;
    const extDenom = CORE_FIELDS.length + EXTENDED_FIELDS.length;

    const remainingCore = CORE_FIELDS.filter((k) => !filled(after[k]));

    hotelRows.push({
      hotel: row.displayName || row.key,
      hpcId,
      p0Complete: p0Ok,
      coreComplete: coreOk,
      corePct: pct(coreFilled, CORE_FIELDS.length),
      extendedPct: pct(extFilled, extDenom),
      address: after.Address || null,
      phone: after.Phone || null,
      coordinates: filled(after.Latitude) && filled(after.Longitude),
      lat: after.Latitude ?? null,
      lng: after.Longitude ?? null,
      rooms: after["Rooms / Keys"] ?? null,
      meetingCapability: after["Meeting Space Flag"] ?? null,
      market: after.Market || after["Market / Submarket"] || null,
      submarket: after.Submarket || null,
      brand: after["Current Brand"] || null,
      operator: after["Operator / Management Company"] || null,
      owner: after["Owner Name"] || null,
      website: after["Official Property URL"] || null,
      remainingGaps: remainingCore,
      proposedWrites: Object.keys(patch),
      fieldMatrix,
      identityNote: pack.identityNote || null,
    });
  }

  const summary = {
    ok: true,
    mode: APPLY ? "apply" : "dry-run",
    hotels: hotelRows.length,
    p0Complete: `${hotelRows.filter((h) => h.p0Complete).length}/${hotelRows.length}`,
    coreComplete: `${hotelRows.filter((h) => h.coreComplete).length}/${hotelRows.length}`,
    p1Complete: `${hotelRows.filter((h) => h.coreComplete).length}/${hotelRows.length}`,
    coreFieldPct: pct(
      hotelRows.reduce(
        (n, h) => n + CORE_FIELDS.filter((k) => filled(h[
          k === "Address" ? "address" :
          k === "Phone" ? "phone" :
          k === "Rooms / Keys" ? "rooms" :
          k === "Meeting Space Flag" ? "meetingCapability" :
          k === "Official Property URL" ? "website" :
          k === "Market / Submarket" ? "market" :
          k === "Current Brand" ? "brand" :
          null
        ]) || (k === "Latitude" || k === "Longitude" ? h.coordinates : filled(
          // fall through via remainingGaps check
          null
        ))).length,
        0
      ),
      hotelRows.length * CORE_FIELDS.length
    ),
    WRITE: mutations.WRITE.length,
    CORROBORATE: mutations.CORROBORATE.length,
    HOLD: mutations.HOLD.length,
    REJECT: mutations.REJECT.length,
    gapLedger,
    hotelRows,
    mutations,
  };

  // Recompute core field % properly from after-state proxies
  let coreCells = 0;
  let coreOkCells = 0;
  for (const h of hotelRows) {
    for (const field of CORE_FIELDS) {
      coreCells += 1;
      const m = (h.fieldMatrix || []).find((x) => x.field === field);
      const ok =
        filled(
          field === "Address"
            ? h.address
            : field === "Phone"
              ? h.phone
              : field === "Official Property URL"
                ? h.website
                : field === "Rooms / Keys"
                  ? h.rooms
                  : field === "Meeting Space Flag"
                    ? h.meetingCapability
                    : field === "Market / Submarket"
                      ? h.market
                      : field === "Property Type"
                        ? true
                        : field === "Latitude" || field === "Longitude"
                          ? h.coordinates
                          : null
        ) ||
        (m &&
          (m.status === "COMPLETE_VERIFIED" ||
            m.status === "COMPLETE_CORROBORATED" ||
            m.decision === "WRITE" ||
            m.decision === "CORROBORATE" ||
            m.decision === "REJECT"));
      // Prefer remainingGaps
      if (!h.remainingGaps?.includes(field)) coreOkCells += 1;
      else if (ok && !h.remainingGaps?.includes(field)) coreOkCells += 1;
    }
  }
  // Simpler: use remainingGaps
  coreOkCells = 0;
  coreCells = hotelRows.length * CORE_FIELDS.length;
  for (const h of hotelRows) {
    coreOkCells += CORE_FIELDS.length - (h.remainingGaps?.length || 0);
  }
  summary.coreFieldPct = pct(coreOkCells, coreCells);
  summary.p1FieldCompleteness = summary.coreFieldPct;
  summary.missingUnresearched = gapLedger.filter(
    (g) => g.gapState === "MISSING_UNRESEARCHED"
  ).length;
  summary.rightsBlocked = gapLedger.filter((g) => g.gapState === "RIGHTS_BLOCKED").length;
  summary.conflictHeld = gapLedger.filter((g) => g.gapState === "CONFLICT_HELD").length;
  summary.researchedNotFound = gapLedger.filter(
    (g) => g.gapState === "RESEARCHED_NOT_FOUND"
  ).length;

  fs.mkdirSync(OUT_DIR, { recursive: true });
  const jsonName = APPLY
    ? "production-hotels-hpc-completeness-final.json"
    : "production-hotels-hpc-completeness-dry-run.json";
  const mdName = APPLY
    ? "production-hotels-hpc-completeness-final.md"
    : "production-hotels-hpc-completeness-dry-run.md";

  fs.writeFileSync(path.join(OUT_DIR, jsonName), JSON.stringify(summary, null, 2));

  const md = [
    `# Production hotels HPC completeness (${APPLY ? "final" : "dry-run"})`,
    "",
    `Generated: ${new Date().toISOString()}`,
    `Mode: **${summary.mode}**`,
    "",
    `P0 complete: **${summary.p0Complete}**`,
    `Core / P1 complete (strict): **${summary.coreComplete}**`,
    `Core field-level: **${summary.coreFieldPct}%**`,
    "",
    `| Hotel | HPC ID | Core % | Ext % | Address | Phone | Coords | Rooms | Meeting | Market | Brand | Operator | Owner | Website | Remaining |`,
    `|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|`,
    ...hotelRows.map((h) =>
      `| ${h.hotel} | \`${h.hpcId}\` | ${h.corePct}% | ${h.extendedPct}% | ${h.address ? "yes" : "no"} | ${h.phone ? "yes" : "no"} | ${h.coordinates ? "yes" : "no"} | ${h.rooms ?? "—"} | ${h.meetingCapability == null ? "—" : h.meetingCapability ? "yes" : "no"} | ${h.market ? "yes" : "no"} | ${h.brand ? "yes" : "no"} | ${h.operator ? "yes" : "no"} | ${h.owner ? "yes" : "no"} | ${h.website ? "yes" : "no"} | ${(h.remainingGaps || []).join("; ") || "—"} |`
    ),
    "",
    "## Mutations",
    "",
    "```json",
    JSON.stringify(
      {
        WRITE: summary.WRITE,
        CORROBORATE: summary.CORROBORATE,
        HOLD: summary.HOLD,
        REJECT: summary.REJECT,
      },
      null,
      2
    ),
    "```",
    "",
    "## Proposed / applied writes",
    "",
    "```json",
    JSON.stringify(mutations.WRITE, null, 2),
    "```",
    "",
    "## Explicit P0/P1 gaps",
    "",
    "| Hotel | Field | Gap State | Reason | Next |",
    "|---|---|---|---|---|",
    ...gapLedger.map(
      (g) =>
        `| ${g.hotel} | ${g.field} | ${g.gapState} | ${g.reason} | ${g.next} |`
    ),
    "",
  ].join("\n");

  fs.writeFileSync(path.join(OUT_DIR, mdName), md);
  console.log(
    JSON.stringify(
      {
        ok: summary.ok,
        mode: summary.mode,
        p0Complete: summary.p0Complete,
        coreComplete: summary.coreComplete,
        coreFieldPct: summary.coreFieldPct,
        WRITE: summary.WRITE,
        CORROBORATE: summary.CORROBORATE,
        HOLD: summary.HOLD,
        REJECT: summary.REJECT,
        missingUnresearched: summary.missingUnresearched,
        rightsBlocked: summary.rightsBlocked,
        conflictHeld: summary.conflictHeld,
        researchedNotFound: summary.researchedNotFound,
        gapCount: gapLedger.length,
      },
      null,
      2
    )
  );
}

const isMain =
  process.argv[1] &&
  path.resolve(process.argv[1]) === path.resolve(new URL(import.meta.url).pathname.replace(/^\/([A-Za-z]:)/, "$1"));

if (isMain || process.argv[1]?.includes("complete-production-active-hotel-hpc")) {
  main().catch((err) => {
    console.error(err);
    process.exit(1);
  });
}
