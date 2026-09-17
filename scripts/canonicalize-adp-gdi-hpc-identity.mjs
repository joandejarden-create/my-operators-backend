#!/usr/bin/env node
/**
 * Canonicalize ADP + GDI hotels into Hotel Property Census, then rewrite
 * provisional hotelIds on GDI opportunities / Decisions / Decision Events.
 *
 * Default: --dry-run
 * Apply:   --apply
 *
 * Does NOT touch research engines.
 */
import "../load-env.js";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { createHash } from "node:crypto";
import Airtable from "airtable";
import {
  loadAdpGdiAliasMap,
  saveAdpGdiAliasMap,
  listAdpGdiHotelUniverse,
  identityStatusForHotel,
  isAirtableRecordId,
  resolveCanonicalHotelId,
} from "../lib/hotel-census/adp-gdi-canonical-identity.js";
import { PRODUCTION_USE_STATUS } from "../lib/research-engine-v2/production-census-write.js";
import {
  DECISIONS_TABLE_NAME,
  DECISION_EVENTS_TABLE_NAME,
  MAP_DECISION,
  MAP_DECISION_EVENT,
} from "../lib/decision-outcomes/field-map.js";
import { getDecisionOutcomesAirtableBaseId } from "../lib/decision-outcomes/airtable-base.js";
import {
  GDI_OPPORTUNITIES_TABLE_NAME,
  MAP_GDI_OPPORTUNITY,
} from "../lib/group-demand-intelligence/opportunity-field-map.js";
import { getGdiOpportunitiesAirtableBaseId } from "../lib/group-demand-intelligence/airtable-opportunity-store.js";
import { ensureGdiOpportunityDecision } from "../lib/decision-outcomes/gdi-bridge.js";
import { setOpportunityDecisionId } from "../lib/group-demand-intelligence/airtable-opportunity-store.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const APPLY = process.argv.includes("--apply");
const SKIP_LINKAGE = process.argv.includes("--skip-linkage");
const SKIP_MIGRATE_IDS = process.argv.includes("--skip-id-migrate");

const HPC_TABLE = "Hotel Property Census";
const HPC_TABLE_ID = "tbl9aY5ijiuIzzWam";

const CREATE_SPECS = [
  {
    rootKey: "gdi_hotel_waterstone_boca_raton",
    adpPropertyId: "adp_waterstone_boca_raton",
    provisionalGdiHotelId: "gdi_hotel_waterstone_boca_raton",
    displayName: "Waterstone Resort & Marina",
    canonicalName: "Waterstone Resort & Marina Boca Raton",
    identityKey: "ind_hilton_us_bocar",
    city: "Boca Raton",
    state: "Florida",
    country: "United States",
    address: null,
    brand: "Curio Collection",
    brandFamily: "Hilton",
    affiliationStatus: "Soft-Branded / Collection",
    sourceFamily: "Hilton",
    officialUrl:
      "https://www.hilton.com/en/hotels/bocar-curio-waterstone-resort-marina-boca-raton/",
    sourceUrl: "https://www.waterstoneresort.com/",
    lat: 26.3459,
    lng: -80.0731,
    rooms: 139,
    roomsSourceUrl:
      "https://www.hilton.com/en/hotels/bocar-curio-waterstone-resort-marina-boca-raton/",
    marketSubmarket: "South Florida · Boca Raton / Palm Beach County",
    propertyType: "Hotel",
    meetingSpaceFlag: true,
    resortFlag: true,
    beachWaterfrontFlag: true,
    adp: true,
    gdi: true,
    gdiConfigPath:
      "config/group-demand-intelligence/hotels/gdi_hotel_waterstone_boca_raton.json",
  },
  {
    rootKey: "gdi_hotel_renaissance_times_square",
    adpPropertyId: "adp_renaissance_times_square",
    provisionalGdiHotelId: "gdi_hotel_renaissance_times_square",
    displayName: "Renaissance New York Times Square Hotel",
    canonicalName: "Renaissance New York Times Square Hotel",
    identityKey: "ind_marriott_us_nycrn",
    city: "New York",
    state: "New York",
    country: "United States",
    address: "Two Times Square, New York, NY",
    brand: "Renaissance Hotels",
    brandFamily: "Marriott International",
    affiliationStatus: "Branded",
    sourceFamily: "Marriott",
    officialUrl:
      "https://www.marriott.com/en-us/hotels/nycrn-renaissance-new-york-times-square-hotel/overview/",
    sourceUrl:
      "https://www.marriott.com/en-us/hotels/nycrn-renaissance-new-york-times-square-hotel/overview/",
    lat: 40.758,
    lng: -73.9855,
    rooms: 310,
    roomsSourceUrl:
      "https://www.marriott.com/en-us/hotels/nycrn-renaissance-new-york-times-square-hotel/overview/",
    marketSubmarket: "New York City · Midtown Manhattan / Times Square",
    propertyType: "Hotel",
    meetingSpaceFlag: true,
    resortFlag: false,
    beachWaterfrontFlag: false,
    adp: true,
    gdi: true,
    gdiConfigPath:
      "config/group-demand-intelligence/hotels/gdi_hotel_renaissance_times_square.json",
  },
  {
    rootKey: "adp_now_now_noho",
    adpPropertyId: "adp_now_now_noho",
    provisionalGdiHotelId: null,
    displayName: "NOW NOW NOHO",
    canonicalName: "NOW NOW NOHO",
    identityKey: "ind_hyatt_us_nycnh",
    city: "New York",
    state: "New York",
    country: "United States",
    address: null,
    brand: "Hyatt",
    brandFamily: "Hyatt Hotels Corporation",
    affiliationStatus: "Branded",
    sourceFamily: "Other",
    officialUrl:
      "https://www.hyatt.com/en-US/hotel/new-york/now-now-noho/nycnh",
    sourceUrl: "https://www.nownownyc.com/",
    lat: null,
    lng: null,
    rooms: 115,
    roomsSourceUrl: "https://www.nownownyc.com/",
    marketSubmarket: "New York City · NoHo / Lower Manhattan",
    propertyType: "Hotel",
    meetingSpaceFlag: true,
    resortFlag: false,
    beachWaterfrontFlag: false,
    adp: true,
    gdi: false,
  },
  {
    rootKey: "adp_hotel_phillips_kansas_city",
    adpPropertyId: "adp_hotel_phillips_kansas_city",
    provisionalGdiHotelId: null,
    displayName: "Hotel Phillips Kansas City, Curio Collection by Hilton",
    canonicalName: "Hotel Phillips Kansas City",
    identityKey: "ind_hilton_us_mkccuqq",
    city: "Kansas City",
    state: "Missouri",
    country: "United States",
    address: "106 W 12th Street, Kansas City, MO 64105, USA",
    brand: "Curio Collection",
    brandFamily: "Hilton",
    affiliationStatus: "Soft-Branded / Collection",
    sourceFamily: "Hilton",
    officialUrl:
      "https://www.hilton.com/en/hotels/mkccuqq-hotel-phillips-kansas-city/",
    sourceUrl: "https://hotelphillips.com/",
    lat: null,
    lng: null,
    rooms: 216,
    roomsSourceUrl: "https://hotelphillips.com/",
    marketSubmarket: "Kansas City · Downtown / Power & Light District",
    propertyType: "Hotel",
    meetingSpaceFlag: true,
    resortFlag: false,
    beachWaterfrontFlag: false,
    adp: true,
    gdi: false,
  },
];

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function token() {
  return process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT || "";
}

function hpcBase() {
  const baseId = process.env.AIRTABLE_BASE_ID_ALT;
  if (!token() || !baseId) throw new Error("hpc_airtable_not_configured");
  return { base: new Airtable({ apiKey: token() }).base(baseId), baseId };
}

function intelBase(baseId) {
  return new Airtable({ apiKey: token() }).base(baseId);
}

function buildCreateFields(spec) {
  const fields = {
    "Property Name": spec.displayName,
    "Canonical Property Name": spec.canonicalName,
    "Property Identity Key": spec.identityKey,
    Country: spec.country,
    "State / Region": spec.state,
    City: spec.city,
    "Current Brand": spec.brand,
    "Brand Family": spec.brandFamily,
    "Affiliation Status": spec.affiliationStatus,
    "Family / Source Family": spec.sourceFamily,
    "Source URL": spec.sourceUrl || spec.officialUrl,
    "Source Type": "official_property_page",
    "Source Confidence": "High",
    "Identity Confidence": "High",
    "Brand Confidence": "High",
    "Production Use Status": PRODUCTION_USE_STATUS,
    "Steward Review Status": "none",
    "Data Eligible": true,
    "Discovery Date": new Date().toISOString().slice(0, 10),
  };
  if (spec.officialUrl) fields["Official Property URL"] = spec.officialUrl;
  if (spec.address) fields.Address = spec.address;
  if (Number.isFinite(spec.lat) && Number.isFinite(spec.lng)) {
    if (!(spec.lat === 0 && spec.lng === 0)) {
      fields.Latitude = spec.lat;
      fields.Longitude = spec.lng;
    }
  }
  if (Number.isFinite(spec.rooms) && spec.rooms > 0) {
    fields["Rooms / Keys"] = spec.rooms;
    if (spec.roomsSourceUrl) fields["Rooms Source URL"] = spec.roomsSourceUrl;
    fields["Rooms Confidence"] = "Medium";
  }
  if (spec.marketSubmarket) fields["Market / Submarket"] = spec.marketSubmarket;
  if (spec.propertyType) fields["Property Type"] = spec.propertyType;
  if (spec.meetingSpaceFlag) fields["Meeting Space Flag"] = true;
  if (spec.resortFlag) fields["Resort / Leisure Flag"] = true;
  if (spec.beachWaterfrontFlag) fields["Beach / Waterfront Flag"] = true;
  return fields;
}

async function findByIdentityKey(base, identityKey) {
  const rows = [];
  const formula = `{Property Identity Key} = "${identityKey.replace(/"/g, '\\"')}"`;
  await base(HPC_TABLE)
    .select({ filterByFormula: formula, maxRecords: 5 })
    .eachPage((recs, next) => {
      rows.push(...recs);
      next();
    });
  return rows;
}

async function findByNameCity(base, name, city) {
  const rows = [];
  const formula = `AND(FIND("${name.replace(/"/g, "")}", {Property Name} & ""), FIND("${city.replace(/"/g, "")}", {City} & ""))`;
  await base(HPC_TABLE)
    .select({ filterByFormula: formula, maxRecords: 10 })
    .eachPage((recs, next) => {
      rows.push(...recs);
      next();
    });
  return rows;
}

async function ensureHpcRecord(base, spec, report) {
  const byKey = await findByIdentityKey(base, spec.identityKey);
  if (byKey.length === 1) {
    report.reused.push({ identityKey: spec.identityKey, id: byKey[0].id });
    return { id: byKey[0].id, created: false, reason: "identity_key_match" };
  }
  if (byKey.length > 1) {
    report.duplicatesPrevented.push({
      identityKey: spec.identityKey,
      ids: byKey.map((r) => r.id),
    });
    return { id: byKey[0].id, created: false, reason: "duplicate_identity_key_reuse_first" };
  }

  const byName = await findByNameCity(base, spec.displayName, spec.city);
  const strong = byName.filter((r) => {
    const n = String(r.fields["Property Name"] || "").toLowerCase();
    const c = String(r.fields.City || "").toLowerCase();
    return (
      n.includes(spec.displayName.toLowerCase().slice(0, 12)) &&
      c.includes(spec.city.toLowerCase())
    );
  });
  if (strong.length === 1) {
    report.reused.push({
      identityKey: spec.identityKey,
      id: strong[0].id,
      via: "name_city",
    });
    return { id: strong[0].id, created: false, reason: "name_city_match" };
  }
  if (strong.length > 1) {
    report.duplicatesPrevented.push({
      name: spec.displayName,
      city: spec.city,
      ids: strong.map((r) => r.id),
    });
    return {
      id: strong[0].id,
      created: false,
      reason: "duplicate_name_city_reuse_first",
    };
  }

  const fields = buildCreateFields(spec);
  if (!APPLY) {
    report.wouldCreate.push({ identityKey: spec.identityKey, fields });
    return { id: null, created: false, reason: "dry_run_would_create", fields };
  }
  const created = await base(HPC_TABLE).create([{ fields }], { typecast: true });
  const id = created[0].id;
  report.created.push({ identityKey: spec.identityKey, id });
  await sleep(250);
  return { id, created: true, reason: "created" };
}

function updateAliasMap(createdMap) {
  const map = loadAdpGdiAliasMap({ force: true });
  for (const spec of CREATE_SPECS) {
    const canonicalHotelId = createdMap[spec.rootKey];
    if (!canonicalHotelId) continue;

    map.aliases[spec.rootKey] = {
      ...(map.aliases[spec.rootKey] || {}),
      canonicalHotelId,
      displayName: spec.displayName,
      identityKey: spec.identityKey,
      adpPropertyId: spec.adpPropertyId,
      provisionalGdiHotelId: spec.provisionalGdiHotelId,
      adp: spec.adp,
      gdi: spec.gdi,
      linkStatus: "canonical",
    };

    if (spec.adpPropertyId) {
      map.aliases[spec.adpPropertyId] = {
        canonicalHotelId,
        aliasOf: canonicalHotelId,
        linkStatus: "canonical",
      };
    }
    if (spec.provisionalGdiHotelId && spec.provisionalGdiHotelId !== spec.rootKey) {
      map.aliases[spec.provisionalGdiHotelId] = {
        canonicalHotelId,
        aliasOf: canonicalHotelId,
        provisionalGdiHotelId: spec.provisionalGdiHotelId,
        linkStatus: "canonical_alias",
      };
    }
    map.aliases[canonicalHotelId] = {
      canonicalHotelId,
      displayName: spec.displayName,
      identityKey: spec.identityKey,
      adpPropertyId: spec.adpPropertyId,
      provisionalGdiHotelId: spec.provisionalGdiHotelId,
      adp: spec.adp,
      gdi: spec.gdi,
      linkStatus: "canonical",
    };
  }
  // Keep Bethesda explicit
  map.aliases.recLuxvwwxID7U2B8 = {
    ...(map.aliases.recLuxvwwxID7U2B8 || {}),
    canonicalHotelId: "recLuxvwwxID7U2B8",
    displayName: "Bethesda Marriott",
    identityKey: "ind_marriott_us_wasbt",
    adpPropertyId: "adp_bethesda_marriott",
    adp: true,
    gdi: true,
    linkStatus: "canonical",
  };
  if (APPLY) saveAdpGdiAliasMap(map);
  return map;
}

function updateCensusLinks(createdMap) {
  const p = path.join(ROOT, "fixtures/ai-demand-positioning/census-links-v1.json");
  const links = JSON.parse(fs.readFileSync(p, "utf8"));
  for (const spec of CREATE_SPECS) {
    const id = createdMap[spec.rootKey];
    if (!id || !spec.adpPropertyId) continue;
    links.links[spec.adpPropertyId] = {
      censusRecordId: id,
      propertyName: spec.displayName,
      canonicalHotelId: spec.identityKey,
      linkStatus: "linked",
      notes: `ADP_GDI_HPC_CANONICALIZE · identity_key ${spec.identityKey}`,
    };
  }
  if (APPLY) fs.writeFileSync(p, JSON.stringify(links, null, 2) + "\n", "utf8");
  return links;
}

function migrateGdiConfig(spec, canonicalHotelId, report) {
  if (!spec.gdi || !spec.provisionalGdiHotelId) return;
  const oldPath = path.join(ROOT, spec.gdiConfigPath);
  const newPath = path.join(
    ROOT,
    "config/group-demand-intelligence/hotels",
    `${canonicalHotelId}.json`
  );
  if (!fs.existsSync(oldPath)) {
    report.configMigrations.push({ status: "missing_old", oldPath });
    return;
  }
  // Never overwrite a complete canonical config with a redirect stub on re-run.
  if (fs.existsSync(newPath)) {
    try {
      const existing = JSON.parse(fs.readFileSync(newPath, "utf8"));
      if (existing.displayName && existing.commercialPriorities) {
        report.configMigrations.push({
          status: "canonical_already_complete",
          newPath,
          hotelId: canonicalHotelId,
        });
        return;
      }
    } catch {
      /* continue rebuild */
    }
  }
  const cfg = JSON.parse(fs.readFileSync(oldPath, "utf8"));
  if (!cfg.displayName || cfg.redirectTo) {
    report.configMigrations.push({
      status: "old_is_stub_skip_overwrite",
      oldPath,
      newPath,
    });
    return;
  }
  cfg.hotelId = canonicalHotelId;
  cfg.aliases = {
    ...(cfg.aliases || {}),
    censusRecordId: canonicalHotelId,
    provisionalGdiHotelId: spec.provisionalGdiHotelId,
    adpPropertyId: spec.adpPropertyId,
    note: "Canonical hotelId is HPC record. Provisional GDI key retained as alias only.",
  };
  cfg.hotelIdentityStatus = "CANONICAL_HPC";
  if (!APPLY) {
    report.configMigrations.push({
      status: "would_write",
      oldPath,
      newPath,
      hotelId: canonicalHotelId,
    });
    return;
  }
  fs.writeFileSync(newPath, JSON.stringify(cfg, null, 2) + "\n", "utf8");
  // Keep old file as redirect stub
  fs.writeFileSync(
    oldPath,
    JSON.stringify(
      {
        hotelId: canonicalHotelId,
        deprecatedProvisionalId: spec.provisionalGdiHotelId,
        redirectTo: canonicalHotelId,
        aliases: cfg.aliases,
        note: "DEPRECATED provisional config — use canonical HPC hotelId file.",
      },
      null,
      2
    ) + "\n",
    "utf8"
  );
  report.configMigrations.push({ status: "migrated", oldPath, newPath });
}

function migrateFsHotelDir(relRoot, fromId, toId, report) {
  const from = path.join(ROOT, relRoot, fromId);
  const to = path.join(ROOT, relRoot, toId);
  if (!fs.existsSync(from)) {
    report.fsMigrations.push({ status: "missing", from });
    return;
  }
  if (!APPLY) {
    report.fsMigrations.push({ status: "would_move", from, to });
    return;
  }
  if (!fs.existsSync(to)) {
    fs.mkdirSync(to, { recursive: true });
  }
  // Windows-safe: copy then leave legacy dir (rename often EPERM when locked).
  for (const name of fs.readdirSync(from)) {
    const src = path.join(from, name);
    const dest = path.join(to, name);
    if (!fs.existsSync(dest)) {
      fs.cpSync(src, dest, { recursive: true });
    }
  }
  // Marker in legacy dir
  fs.writeFileSync(
    path.join(from, "_MIGRATED_TO_CANONICAL_HPC.json"),
    JSON.stringify(
      {
        fromId,
        toId,
        migratedAt: new Date().toISOString(),
        note: "LEGACY_MIGRATED_SOURCE — prefer canonical HPC hotel dir",
      },
      null,
      2
    ) + "\n",
    "utf8"
  );
  report.fsMigrations.push({ status: "copied_to_canonical", from, to });
  // rewrite hotelId inside JSON files under target
  for (const name of fs.readdirSync(to)) {
    if (!name.endsWith(".json")) continue;
    const fp = path.join(to, name);
    try {
      const data = JSON.parse(fs.readFileSync(fp, "utf8"));
      let changed = false;
      if (data.hotelId === fromId) {
        data.hotelId = toId;
        changed = true;
      }
      if (Array.isArray(data.opportunities)) {
        for (const o of data.opportunities) {
          if (o.hotelId === fromId) {
            o.hotelId = toId;
            changed = true;
          }
        }
      }
      if (Array.isArray(data.items)) {
        for (const it of data.items) {
          if (it.hotelId === fromId) {
            it.hotelId = toId;
            changed = true;
          }
        }
      }
      if (changed) fs.writeFileSync(fp, JSON.stringify(data, null, 2) + "\n", "utf8");
    } catch {
      /* ignore non-json object shapes */
    }
  }
}

async function listAllTableRecords(base, tableName, fields) {
  const rows = [];
  await base(tableName)
    .select(fields ? { fields } : {})
    .eachPage((recs, next) => {
      rows.push(...recs);
      next();
    });
  return rows;
}

async function migrateAirtableHotelIds(createdMap, report) {
  if (SKIP_MIGRATE_IDS) {
    report.idMigrate = { skipped: true };
    return;
  }
  const remaps = [];
  for (const spec of CREATE_SPECS) {
    if (!spec.provisionalGdiHotelId) continue;
    const toId = createdMap[spec.rootKey];
    if (!toId) continue;
    remaps.push({ from: spec.provisionalGdiHotelId, to: toId });
  }
  if (!remaps.length) {
    report.idMigrate = { remaps: [], note: "no provisional remaps" };
    return;
  }

  const decisionBaseId = getDecisionOutcomesAirtableBaseId();
  const gdiBaseId = getGdiOpportunitiesAirtableBaseId();
  const dBase = intelBase(decisionBaseId);
  const gBase = intelBase(gdiBaseId);

  const summary = {
    remaps,
    opportunities: { updated: 0, skipped: 0, wouldUpdate: 0 },
    decisions: { updated: 0, skipped: 0, wouldUpdate: 0 },
    events: { updated: 0, skipped: 0, wouldUpdate: 0 },
  };

  for (const { from, to } of remaps) {
    const opps = await listAllTableRecords(gBase, GDI_OPPORTUNITIES_TABLE_NAME, [
      MAP_GDI_OPPORTUNITY.hotelId,
      MAP_GDI_OPPORTUNITY.opportunityId,
      MAP_GDI_OPPORTUNITY.hotelIdentityStatus,
    ]);
    for (const rec of opps) {
      if (rec.fields[MAP_GDI_OPPORTUNITY.hotelId] !== from) continue;
      if (!APPLY) {
        summary.opportunities.wouldUpdate += 1;
        continue;
      }
      await gBase(GDI_OPPORTUNITIES_TABLE_NAME).update(rec.id, {
        [MAP_GDI_OPPORTUNITY.hotelId]: to,
        [MAP_GDI_OPPORTUNITY.hotelIdentityStatus]: "CANONICAL",
      });
      summary.opportunities.updated += 1;
      await sleep(120);
    }

    const decisions = await listAllTableRecords(dBase, DECISIONS_TABLE_NAME, [
      MAP_DECISION.hotelId,
      MAP_DECISION.decisionId,
    ]);
    for (const rec of decisions) {
      if (rec.fields[MAP_DECISION.hotelId] !== from) continue;
      if (!APPLY) {
        summary.decisions.wouldUpdate += 1;
        continue;
      }
      await dBase(DECISIONS_TABLE_NAME).update(rec.id, {
        [MAP_DECISION.hotelId]: to,
      });
      summary.decisions.updated += 1;
      await sleep(120);
    }

    const events = await listAllTableRecords(dBase, DECISION_EVENTS_TABLE_NAME, [
      MAP_DECISION_EVENT.hotelId,
      MAP_DECISION_EVENT.eventId,
      MAP_DECISION_EVENT.decisionId,
    ]);
    for (const rec of events) {
      if (rec.fields[MAP_DECISION_EVENT.hotelId] !== from) continue;
      if (!APPLY) {
        summary.events.wouldUpdate += 1;
        continue;
      }
      await dBase(DECISION_EVENTS_TABLE_NAME).update(rec.id, {
        [MAP_DECISION_EVENT.hotelId]: to,
      });
      summary.events.updated += 1;
      await sleep(100);
    }
  }

  report.idMigrate = summary;
}

async function repairDecisionLinkage(createdMap, report) {
  if (SKIP_LINKAGE) {
    report.linkage = { skipped: true };
    return;
  }
  const gBase = intelBase(getGdiOpportunitiesAirtableBaseId());
  const opps = await listAllTableRecords(gBase, GDI_OPPORTUNITIES_TABLE_NAME);
  const { airtableRecordToOpportunity } = await import(
    "../lib/group-demand-intelligence/airtable-opportunity-store.js"
  );

  let actionable = 0;
  let linked = 0;
  let missing = 0;
  let repaired = 0;
  let broken = 0;
  const missingRows = [];

  for (const rec of opps) {
    const opp = airtableRecordToOpportunity(rec);
    if (!opp?.id) continue;
    if (opp.priority === "DISQUALIFIED") continue;
    actionable += 1;
    if (opp.decisionId) {
      linked += 1;
      continue;
    }
    missing += 1;
    missingRows.push({
      opportunityId: opp.id,
      hotelId: opp.hotelId,
      priority: opp.priority,
    });
    if (!APPLY) continue;
    try {
      const result = await ensureGdiOpportunityDecision({
        hotelId: opp.hotelId,
        opportunity: opp,
      });
      const decisionId = result?.decision?.decisionId;
      if (decisionId) {
        await setOpportunityDecisionId(opp.id, decisionId);
        repaired += 1;
        linked += 1;
        missing -= 1;
      } else if (result?.skipped) {
        /* intentional skip */
      } else {
        broken += 1;
      }
    } catch (err) {
      broken += 1;
      missingRows[missingRows.length - 1].error = err.message;
    }
    await sleep(150);
  }

  report.linkage = {
    totalNonDisqualified: actionable,
    actionable,
    decisionLinked: linked,
    missing: Math.max(0, missing),
    repaired,
    broken,
    missingPreview: missingRows.slice(0, 40),
  };
}

function completenessForFields(f) {
  const checks = {
    identity: ["Property Name", "Property Identity Key", "Current Brand"],
    location: ["Country", "State / Region", "City", "Address"],
    property: ["Rooms / Keys", "Property Type", "Meeting Space Flag"],
    brand: ["Current Brand", "Brand Family", "Affiliation Status"],
    contact: ["Official Property URL", "Phone"],
    crosswalk: ["Property Identity Key", "Official Property URL"],
  };
  const out = {};
  for (const [k, fields] of Object.entries(checks)) {
    const present = fields.filter((name) => {
      const v = f[name];
      return v != null && String(v).trim() !== "";
    }).length;
    out[k] = {
      present,
      total: fields.length,
      missing: fields.filter((name) => {
        const v = f[name];
        return v == null || String(v).trim() === "";
      }),
      pct: Math.round((present / fields.length) * 100),
    };
  }
  return out;
}

async function buildReports(createdMap, runReport) {
  const { base, baseId } = hpcBase();
  const universe = listAdpGdiHotelUniverse();
  const inventoryRows = [];
  const completenessRows = [];

  for (const row of universe) {
    let canon =
      row.canonicalHotelId ||
      createdMap[row.key] ||
      createdMap[row.provisionalGdiHotelId] ||
      createdMap[row.adpPropertyId] ||
      resolveCanonicalHotelId(row.key) ||
      resolveCanonicalHotelId(row.adpPropertyId);
    let fields = null;
    if (canon && isAirtableRecordId(canon)) {
      try {
        const rec = await base(HPC_TABLE).find(canon);
        fields = rec.fields;
      } catch (err) {
        fields = { _error: err.message };
      }
    }
    const status = canon
      ? "CANONICAL_HPC"
      : identityStatusForHotel({ ...row, canonicalHotelId: canon });
    inventoryRows.push({
      hotel: row.displayName,
      adp: row.adp || Boolean(row.adpPropertyId),
      gdi: row.gdi || Boolean(row.provisionalGdiHotelId),
      currentHotelId: row.key,
      hpcMatch: canon || "—",
      identityStatus: status,
    });
    completenessRows.push({
      hotel: row.displayName,
      hpcId: canon || null,
      completeness: fields && !fields._error ? completenessForFields(fields) : null,
      error: fields?._error || null,
    });
  }

  const outDir = path.join(ROOT, "reports/hotel-census");
  fs.mkdirSync(outDir, { recursive: true });
  fs.mkdirSync(path.join(ROOT, "reports/decision-outcomes"), { recursive: true });

  const invMd = [
    "# ADP + GDI hotel inventory",
    "",
    `Generated: ${new Date().toISOString()}`,
    `Mode: ${APPLY ? "apply" : "dry-run"}`,
    `HPC base: \`${baseId}\` · table \`${HPC_TABLE}\` (\`${HPC_TABLE_ID}\`)`,
    "",
    "| Hotel | ADP | GDI | Current hotelId | HPC match | Identity status |",
    "|---|---|---|---|---|---|",
    ...inventoryRows.map(
      (r) =>
        `| ${r.hotel} | ${r.adp ? "yes" : "no"} | ${r.gdi ? "yes" : "no"} | \`${r.currentHotelId}\` | ${r.hpcMatch === "—" ? "—" : `\`${r.hpcMatch}\``} | ${r.identityStatus} |`
    ),
    "",
  ].join("\n");
  fs.writeFileSync(path.join(outDir, "adp-gdi-hotel-inventory.md"), invMd);

  const compMd = [
    "# ADP + GDI HPC completeness",
    "",
    `Generated: ${new Date().toISOString()}`,
    "",
    ...completenessRows.map((r) => {
      if (!r.hpcId) return `## ${r.hotel}\n\nUNRESOLVED — no HPC record.\n`;
      if (r.error) return `## ${r.hotel}\n\nError reading \`${r.hpcId}\`: ${r.error}\n`;
      const lines = Object.entries(r.completeness).map(
        ([k, v]) =>
          `- **${k}**: ${v.present}/${v.total} (${v.pct}%)${v.missing.length ? ` — missing: ${v.missing.join(", ")}` : ""}`
      );
      return `## ${r.hotel}\n\nHPC: \`${r.hpcId}\`\n\n${lines.join("\n")}\n`;
    }),
  ].join("\n");
  fs.writeFileSync(path.join(outDir, "adp-gdi-hpc-completeness.md"), compMd);

  const migMd = [
    "# ADP + GDI HPC migration",
    "",
    `Generated: ${new Date().toISOString()}`,
    `Mode: ${APPLY ? "apply" : "dry-run"}`,
    "",
    "## Creates / reuses",
    "",
    "```json",
    JSON.stringify(
      {
        created: runReport.created,
        reused: runReport.reused,
        wouldCreate: runReport.wouldCreate?.map((w) => ({
          identityKey: w.identityKey,
          propertyName: w.fields?.["Property Name"],
        })),
        duplicatesPrevented: runReport.duplicatesPrevented,
        createdMap,
      },
      null,
      2
    ),
    "```",
    "",
    "## ID migrations",
    "",
    "```json",
    JSON.stringify(runReport.idMigrate || {}, null, 2),
    "```",
    "",
    "## Config / FS",
    "",
    "```json",
    JSON.stringify(
      {
        configMigrations: runReport.configMigrations,
        fsMigrations: runReport.fsMigrations,
      },
      null,
      2
    ),
    "```",
    "",
    "## Decision linkage",
    "",
    "```json",
    JSON.stringify(runReport.linkage || {}, null, 2),
    "```",
    "",
    "OLD BASE (Decision/Outcome legacy): `appvtnDurnMSjINP6` — not used for new writes.",
    "NEW CANONICAL intelligence base: `appa2cE7FTRmIbB32`.",
    "HPC base: Deal Capture Platform via `AIRTABLE_BASE_ID_ALT`.",
    "",
  ].join("\n");
  fs.writeFileSync(path.join(outDir, "adp-gdi-hpc-migration.md"), migMd);

  const linkMd = [
    "# Actionable GDI opportunity → Decision linkage",
    "",
    `Generated: ${new Date().toISOString()}`,
    "",
    "```json",
    JSON.stringify(runReport.linkage || {}, null, 2),
    "```",
    "",
    "Actionable = priority !== DISQUALIFIED (salesperson-visible).",
    "Target: 100% of actionable opportunities decision-linked.",
    "",
  ].join("\n");
  fs.writeFileSync(
    path.join(ROOT, "reports/decision-outcomes/actionable-opportunity-decision-linkage.md"),
    linkMd
  );

  fs.writeFileSync(
    path.join(outDir, "adp-gdi-hpc-migration.json"),
    JSON.stringify({ createdMap, runReport, inventoryRows, completenessRows }, null, 2)
  );

  return { inventoryRows, completenessRows };
}

async function main() {
  const { base, baseId } = hpcBase();
  const runReport = {
    mode: APPLY ? "apply" : "dry-run",
    hpcBaseId: baseId,
    created: [],
    reused: [],
    wouldCreate: [],
    duplicatesPrevented: [],
    configMigrations: [],
    fsMigrations: [],
  };

  console.log(`Mode: ${APPLY ? "APPLY" : "DRY-RUN"}`);
  console.log(`HPC base: ${baseId}`);

  const createdMap = {};
  // Existing linked hotels
  createdMap.recLuxvwwxID7U2B8 = "recLuxvwwxID7U2B8";

  for (const spec of CREATE_SPECS) {
    const result = await ensureHpcRecord(base, spec, runReport);
    if (result.id) createdMap[spec.rootKey] = result.id;
    console.log(
      JSON.stringify({
        hotel: spec.displayName,
        result: result.reason,
        id: result.id,
      })
    );
  }

  // Also map already-linked ADP hotels into createdMap for reports
  const existingLinks = JSON.parse(
    fs.readFileSync(
      path.join(ROOT, "fixtures/ai-demand-positioning/census-links-v1.json"),
      "utf8"
    )
  );
  for (const [adpId, entry] of Object.entries(existingLinks.links || {})) {
    if (entry.censusRecordId) createdMap[adpId] = entry.censusRecordId;
  }

  updateAliasMap(createdMap);
  updateCensusLinks(createdMap);

  for (const spec of CREATE_SPECS) {
    const id = createdMap[spec.rootKey];
    if (!id) continue;
    migrateGdiConfig(spec, id, runReport);
    if (spec.provisionalGdiHotelId) {
      migrateFsHotelDir(
        "data/group-demand-intelligence/hotels",
        spec.provisionalGdiHotelId,
        id,
        runReport
      );
      migrateFsHotelDir(
        "data/decision-outcomes/hotels",
        spec.provisionalGdiHotelId,
        id,
        runReport
      );
    }
  }

  await migrateAirtableHotelIds(createdMap, runReport);
  await repairDecisionLinkage(createdMap, runReport);
  const reports = await buildReports(createdMap, runReport);

  console.log(
    JSON.stringify(
      {
        ok: true,
        mode: runReport.mode,
        created: runReport.created,
        reused: runReport.reused,
        duplicatesPrevented: runReport.duplicatesPrevented.length,
        createdMap: Object.fromEntries(
          CREATE_SPECS.map((s) => [s.rootKey, createdMap[s.rootKey] || null])
        ),
        idMigrate: runReport.idMigrate,
        linkage: runReport.linkage,
        inventoryCount: reports.inventoryRows.length,
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
