/**
 * AC + Spice — Hotel Intelligence apply (persistence reconciliation).
 *
 * Uses shared HI schema + applyHotelIntelligencePacket (same path as Bethesda).
 * Demand nodes are evidence-backed market entities (not empty demandAnchorFocus).
 * Seasonality / need periods left empty unless public tourism-season evidence is attached.
 *
 *   node scripts/apply-hotel-intelligence-ac-spice-reconciliation.mjs --dry-run
 *   node scripts/apply-hotel-intelligence-ac-spice-reconciliation.mjs --apply
 *   node scripts/apply-hotel-intelligence-ac-spice-reconciliation.mjs --apply --hotel=ac|spice|both
 */
import "../load-env.js";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  commercialProfileDedupeKey,
  eventSpaceDedupeKey,
  demandNodeDedupeKey,
  evidenceDedupeKey,
  HOTEL_INTELLIGENCE_SCHEMA_VERSION,
} from "../lib/hotel-intelligence/schema/hotel-intelligence-schema-v1.js";
import { applyHotelIntelligencePacket, loadHotelIntelligenceFromAirtable } from "../lib/hotel-intelligence/schema/hi-airtable-store.js";
import { buildHotelIntelligenceProfile } from "../lib/hotel-intelligence/adp-attributes/build-hotel-intelligence-profile.js";
import { buildAdpHotelAttributes } from "../lib/hotel-intelligence/adp-attributes/build-adp-hotel-attributes.js";
import { syncHotelAdpAttributesToAirtable } from "../lib/hotel-intelligence/adp-attributes/airtable-store.js";
import {
  CANONICAL_INTELLIGENCE_BASE_ID,
  assertNotLegacyMvpCanonicalBase,
  getGdiOpportunitiesAirtableBaseId,
} from "../lib/decision-outcomes/airtable-base.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const OUT_DIR = path.join(ROOT, "reports", "hotel-intelligence", "ac-spice-reconciliation-v1");
const APPLY = process.argv.includes("--apply");
const hotelArg = (process.argv.find((a) => a.startsWith("--hotel=")) || "--hotel=both").slice(8);

const intel =
  getGdiOpportunitiesAirtableBaseId() ||
  process.env.ADP_AIRTABLE_BASE_ID ||
  CANONICAL_INTELLIGENCE_BASE_ID;
assertNotLegacyMvpCanonicalBase(intel, { surface: "hi-ac-spice-apply" });
if (intel !== CANONICAL_INTELLIGENCE_BASE_ID) {
  throw new Error(`wrong_intel_base:${intel}`);
}

function evidence(hpcHotelId, entityType, fieldName, value, source) {
  return {
    evidenceId: evidenceDedupeKey(hpcHotelId, entityType, fieldName, source.url, value),
    hpcHotelId,
    entityType,
    fieldName,
    valueObserved: value,
    sourceName: source.name,
    sourceType: source.sourceType || "Public Research",
    sourceUrl: source.url,
    retrievedAt: new Date().toISOString(),
    evidenceSnippet: source.snippet || null,
    confidence: source.confidence || "MEDIUM",
    evidenceStrength: source.strength || "MODERATE",
    current: true,
    schemaVersion: HOTEL_INTELLIGENCE_SCHEMA_VERSION,
  };
}

async function fetchOk(url) {
  try {
    const ctrl = new AbortController();
    const t = setTimeout(() => ctrl.abort(), 20000);
    const res = await fetch(url, {
      signal: ctrl.signal,
      redirect: "follow",
      headers: {
        "User-Agent": "DealalityHotelIntelligence/1.0 (+reconciliation)",
        Accept: "text/html",
      },
    });
    clearTimeout(t);
    const text = await res.text();
    return { ok: res.ok, status: res.status, url: res.url || url, text: text.slice(0, 200000) };
  } catch (e) {
    return { ok: false, status: 0, url, error: String(e.message || e), text: "" };
  }
}

function sqMtoSqFt(m2) {
  if (m2 == null) return null;
  return Math.round(Number(m2) * 10.7639);
}

const HOTELS = {
  ac: {
    label: "AC Hotel A Coruña",
    hpcId: "rec2PVBDavppGpenm",
    adpPropertyId: "adp_ac_hotel_a_coruna",
    officialUrl: "https://www.marriott.com/es/hotels/lcgco-ac-hotel-a-coruna/overview/",
    eventsUrl: "https://www.marriott.com/en-us/hotels/lcgco-ac-hotel-a-coruna/events/",
    brand: "AC Hotels by Marriott",
    rooms: 116,
    meeting: {
      totalSqM: 674,
      totalSqFt: 7254,
      meetingRooms: 6,
      largest: { name: "Banquets", sqM: 302, sqFt: 3251, theater: 220, reception: 290, banquet: 220 },
      spaces: [
        { name: "Banquets", type: "Ballroom", sqM: 302, theater: 220, reception: 290, banquet: 220 },
        { name: "Consejo", type: "Boardroom", sqM: 21, conference: 14 },
        { name: "Fórum A", type: "Meeting Room", sqM: 38, theater: 28, reception: 40 },
        { name: "Fórum B", type: "Meeting Room", sqM: 43, theater: 30, reception: 40 },
        { name: "Gran Fórum", type: "Meeting Room", sqM: 105, theater: 90, reception: 90 },
        { name: "Congress", type: "Meeting Room", sqM: 165, theater: 70 },
      ],
    },
    demandNodes: [
      {
        name: "Aeropuerto de A Coruña (LCG)",
        type: "Airport",
        city: "Culleredo",
        state: "Galicia",
        url: "https://www.aena.es/es/a-coruna.html",
        sourceName: "Aena — Aeropuerto de A Coruña",
        confidence: "HIGH",
      },
      {
        name: "Puerto de A Coruña",
        type: "Industrial",
        city: "A Coruña",
        state: "Galicia",
        url: "https://www.puertocoruna.com/",
        sourceName: "Autoridad Portuaria de A Coruña",
        confidence: "HIGH",
      },
      {
        name: "Expocoruña",
        type: "Convention / Conference",
        city: "A Coruña",
        state: "Galicia",
        url: "https://www.expocoruna.com/",
        sourceName: "Expocoruña official",
        confidence: "HIGH",
      },
      {
        name: "Coliseum da Coruña",
        type: "Sports",
        city: "A Coruña",
        state: "Galicia",
        url: "https://www.coruna.gal/web/es/ayuntamiento/areas-municipales/deportes/instalaciones-deportivas/coliseum",
        sourceName: "Concello da Coruña — Coliseum",
        confidence: "MEDIUM",
      },
      {
        name: "Universidade da Coruña",
        type: "University",
        city: "A Coruña",
        state: "Galicia",
        url: "https://www.udc.es/",
        sourceName: "Universidade da Coruña",
        confidence: "HIGH",
      },
      {
        name: "CHUAC — Complexo Hospitalario Universitario A Coruña",
        type: "Medical / Healthcare",
        city: "A Coruña",
        state: "Galicia",
        url: "https://www.sergas.es/",
        sourceName: "SERGAS / CHUAC public listing",
        confidence: "MEDIUM",
      },
      {
        name: "Inditex / Arteixo industrial-corporate corridor",
        type: "Corporate",
        city: "Arteixo",
        state: "Galicia",
        url: "https://www.inditex.com/",
        sourceName: "Inditex corporate (Arteixo HQ market)",
        confidence: "MEDIUM",
      },
    ],
  },
  spice: {
    label: "Spice Island Beach Resort",
    hpcId: "recKRJjcPnb4tVDDS",
    adpPropertyId: "adp_spice_island_beach_resort",
    officialUrl: "https://www.spiceislandbeachresort.com/",
    eventsUrl: "https://www.spiceislandbeachresort.com/",
    brand: "Small Luxury Hotels of the World",
    rooms: 64,
    suites: 64,
    meeting: {
      totalSqFt: null,
      meetingRooms: null,
      largest: null,
      outdoorEventSpace: true,
      weddingVenue: true,
      spaces: [
        {
          name: "Grand Anse Beach / outdoor wedding venue",
          type: "Outdoor",
          outdoor: true,
          notes: "First-party wedding/social beach venue evidenced; indoor inventory not quantified",
        },
      ],
    },
    demandNodes: [
      {
        name: "Grand Anse Beach",
        type: "Tourism",
        city: "St George's",
        state: "Grenada",
        url: "https://www.puregrenada.com/",
        sourceName: "Pure Grenada / tourism destination",
        confidence: "HIGH",
      },
      {
        name: "Maurice Bishop International Airport (GND)",
        type: "Airport",
        city: "St George's",
        state: "Grenada",
        url: "https://www.mbiagrenada.com/",
        sourceName: "Maurice Bishop International Airport",
        confidence: "HIGH",
      },
      {
        name: "St. George's",
        type: "Tourism",
        city: "St George's",
        state: "Grenada",
        url: "https://www.puregrenada.com/",
        sourceName: "Pure Grenada destination pages",
        confidence: "HIGH",
      },
      {
        name: "Port Louis Marina / yachting",
        type: "Other",
        city: "St George's",
        state: "Grenada",
        url: "https://www.camperandnicholsons.com/marina/port-louis-marina/",
        sourceName: "Port Louis Marina",
        confidence: "MEDIUM",
      },
      {
        name: "Grenada tourism / travel-trade",
        type: "Tourism",
        city: "St George's",
        state: "Grenada",
        url: "https://www.puregrenada.com/",
        sourceName: "Grenada Tourism Authority (Pure Grenada)",
        confidence: "HIGH",
      },
      {
        name: "Wedding / honeymoon destination ecosystem",
        type: "Tourism",
        city: "St George's",
        state: "Grenada",
        url: "https://www.spiceislandbeachresort.com/",
        sourceName: "Spice Island first-party weddings positioning",
        confidence: "HIGH",
      },
    ],
    seasonality: [
      {
        periodType: "Public Seasonality",
        seasonLabel: "Caribbean high season (approx Dec–Apr)",
        startMonthDay: "12-01",
        endMonthDay: "04-30",
        description:
          "Public Caribbean leisure high season commonly cited by regional tourism boards; not hotel-supplied need period.",
        url: "https://www.puregrenada.com/",
        sourceName: "Pure Grenada / regional tourism seasonality",
        confidence: "MEDIUM",
      },
      {
        periodType: "Public Seasonality",
        seasonLabel: "Atlantic hurricane season travel caution (Jun–Nov)",
        startMonthDay: "06-01",
        endMonthDay: "11-30",
        description:
          "NOAA Atlantic hurricane season window — public travel-pattern context only; not a hotel commercial need period.",
        url: "https://www.nhc.noaa.gov/",
        sourceName: "NOAA National Hurricane Center",
        confidence: "HIGH",
      },
    ],
  },
};

async function buildPacket(spec) {
  const profile = await buildHotelIntelligenceProfile(spec.hpcId);
  if (!profile.ok) throw new Error(`profile_fail_${spec.hpcId}:${profile.error}`);

  const official = await fetchOk(spec.officialUrl);
  const events = spec.eventsUrl ? await fetchOk(spec.eventsUrl) : { ok: false };

  const evidenceRows = [];
  if (official.ok) {
    evidenceRows.push(
      evidence(spec.hpcId, "Commercial Profile", "Official Property URL", spec.officialUrl, {
        name: "Official property page",
        url: official.url,
        confidence: "HIGH",
        strength: "STRONG",
        snippet: official.text.slice(0, 200),
      })
    );
  }
  if (events.ok) {
    evidenceRows.push(
      evidence(spec.hpcId, "Commercial Profile", "Official Events URL", spec.eventsUrl, {
        name: "Official events / venue page",
        url: events.url,
        confidence: "HIGH",
        strength: "STRONG",
        snippet: events.text.slice(0, 200),
      })
    );
  }

  const m = spec.meeting;
  const commercial = {
    profileKey: commercialProfileDedupeKey(spec.hpcId),
    hpcHotelId: spec.hpcId,
    dealalityHotelId: profile.identity?.dealalityHotelId || null,
    adpPropertyId: spec.adpPropertyId,
    hotelName: spec.label,
    brand: spec.brand,
    roomsKeys: spec.rooms,
    suites: spec.suites ?? null,
    officialPropertyUrl: spec.officialUrl,
    officialEventsUrl: spec.eventsUrl,
    meetingSpaceFlag: Boolean(m.meetingRooms || m.outdoorEventSpace || m.weddingVenue),
    totalMeetingSpaceSqFt: m.totalSqFt ?? null,
    meetingRoomCount: m.meetingRooms ?? null,
    largestMeetingSpaceSqFt: m.largest?.sqFt ?? null,
    largestEventCapacity: m.largest?.reception ?? m.largest?.theater ?? null,
    researchStatus: "Partial",
    researchVersion: "hotel_intelligence_research_v1_reconciliation",
    confidence: m.totalSqFt != null ? "HIGH" : "MEDIUM",
    lastResearchedAt: new Date().toISOString(),
    notes:
      m.totalSqFt == null
        ? "Meeting inventory partially evidenced (outdoor/wedding); indoor sq ft not quantified on first-party pages."
        : "Meeting inventory from Marriott official events page (revalidated).",
    schemaVersion: HOTEL_INTELLIGENCE_SCHEMA_VERSION,
  };

  if (spec.rooms != null) {
    evidenceRows.push(
      evidence(spec.hpcId, "Commercial Profile", "Rooms / Keys", spec.rooms, {
        name: "HPC + official inventory",
        url: spec.officialUrl,
        confidence: "HIGH",
        strength: "STRONG",
        sourceType: "HPC",
      })
    );
  }

  const eventSpaces = (m.spaces || []).map((s) => {
    const sqFt = s.sqFt ?? (s.sqM != null ? sqMtoSqFt(s.sqM) : null);
    if (sqFt != null) {
      evidenceRows.push(
        evidence(spec.hpcId, "Event Space", s.name, sqFt, {
          name: "Official events inventory",
          url: spec.eventsUrl || spec.officialUrl,
          confidence: "HIGH",
          strength: "STRONG",
          sourceType: "Hotel Event Space",
        })
      );
    }
    return {
      spaceKey: eventSpaceDedupeKey(spec.hpcId, s.name),
      hpcHotelId: spec.hpcId,
      spaceName: s.name,
      spaceType: s.type || "Meeting Room",
      sqFt,
      theaterCapacity: s.theater ?? null,
      banquetCapacity: s.banquet ?? null,
      receptionCapacity: s.reception ?? null,
      conferenceCapacity: s.conference ?? null,
      outdoor: Boolean(s.outdoor),
      notes: s.notes || null,
      sourceUrl: spec.eventsUrl || spec.officialUrl,
      lastVerifiedAt: new Date().toISOString(),
      confidence: sqFt != null ? "HIGH" : "MEDIUM",
      active: true,
      schemaVersion: HOTEL_INTELLIGENCE_SCHEMA_VERSION,
    };
  });

  const demandNodes = [];
  for (const n of spec.demandNodes) {
    const page = await fetchOk(n.url);
    evidenceRows.push(
      evidence(spec.hpcId, "Demand Node", n.name, n.url, {
        name: n.sourceName,
        url: page.url || n.url,
        confidence: n.confidence,
        strength: page.ok ? "STRONG" : "MODERATE",
        snippet: page.ok ? page.text.slice(0, 160) : page.error || `http_${page.status}`,
      })
    );
    demandNodes.push({
      nodeKey: demandNodeDedupeKey(spec.hpcId, n.name, n.type),
      hpcHotelId: spec.hpcId,
      demandNodeName: n.name,
      demandNodeType: n.type,
      city: n.city || null,
      state: n.state || null,
      sourceType: "Public Research",
      sourceName: n.sourceName,
      sourceUrl: n.url,
      confidence: n.confidence,
      demandStrength: "model-derived-unscored",
      active: true,
      lastVerifiedAt: new Date().toISOString(),
      schemaVersion: HOTEL_INTELLIGENCE_SCHEMA_VERSION,
    });
  }

  const seasonality = [];
  for (const s of spec.seasonality || []) {
    const periodKey = `${spec.hpcId}::${String(s.seasonLabel)
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, "_")
      .slice(0, 80)}`;
    evidenceRows.push(
      evidence(spec.hpcId, "Seasonality", s.seasonLabel, s.url, {
        name: s.sourceName,
        url: s.url,
        confidence: s.confidence,
        strength: "MODERATE",
      })
    );
    seasonality.push({
      periodKey,
      hpcHotelId: spec.hpcId,
      periodType: s.periodType,
      startMonthDay: s.startMonthDay,
      endMonthDay: s.endMonthDay,
      seasonLabel: s.seasonLabel,
      description: s.description,
      sourceUrl: s.url,
      sourceName: s.sourceName,
      hotelSupplied: false,
      lastVerifiedAt: new Date().toISOString(),
      confidence: s.confidence,
      active: true,
      schemaVersion: HOTEL_INTELLIGENCE_SCHEMA_VERSION,
    });
  }

  return {
    packet: {
      commercial,
      eventSpaces,
      demandNodes,
      seasonality,
      evidence: evidenceRows,
    },
    profile,
    sources: {
      officialOk: official.ok,
      eventsOk: events.ok,
      demandNodeFetchOk: demandNodes.length,
    },
  };
}

async function applyOne(key) {
  const spec = HOTELS[key];
  console.error(`[hi] building ${spec.label} (${spec.hpcId}) apply=${APPLY}`);
  const { packet, profile, sources } = await buildPacket(spec);

  let applyResult = null;
  let adpSyncResult = null;
  if (APPLY) {
    applyResult = await applyHotelIntelligencePacket(packet, { dryRun: false });
  } else {
    applyResult = await applyHotelIntelligencePacket(packet, { dryRun: true });
  }

  const attrs = await buildAdpHotelAttributes(spec.hpcId, {
    profile: {
      ...profile,
      commercialProfile: {
        ...profile.commercialProfile,
        rooms: packet.commercial.roomsKeys,
        meetingSpace: {
          totalSqFt: packet.commercial.totalMeetingSpaceSqFt,
          meetingRooms: packet.commercial.meetingRoomCount,
          largestRoom: packet.eventSpaces[0]
            ? {
                name: packet.eventSpaces[0].spaceName,
                sqFt: packet.eventSpaces[0].sqFt,
                capacity: packet.eventSpaces[0].receptionCapacity || packet.eventSpaces[0].theaterCapacity,
              }
            : null,
          source: packet.commercial.officialEventsUrl,
          confidence: packet.commercial.confidence,
        },
      },
      eventSpaces: packet.eventSpaces.map((s) => ({
        name: s.spaceName,
        type: s.spaceType,
        sqFt: s.sqFt,
        capacities: {
          theaterOrEvent: s.theaterCapacity,
          banquet: s.banquetCapacity,
          reception: s.receptionCapacity,
        },
        sourceUrl: s.sourceUrl,
        confidence: s.confidence,
        fromHiAirtable: APPLY,
      })),
      demandNodes: packet.demandNodes.map((n) => ({
        name: n.demandNodeName,
        type: n.demandNodeType,
        sourceType: n.sourceType,
        sourceName: n.sourceName,
        confidence: n.confidence,
        fromHiAirtable: APPLY,
      })),
      seasonality: packet.seasonality.map((s) => ({
        label: s.seasonLabel,
        type: s.periodType,
        confidence: s.confidence,
      })),
      needPeriods: [],
    },
  });

  if (APPLY) {
    adpSyncResult = await syncHotelAdpAttributesToAirtable(attrs, { dryRun: false });
  }

  const hiLoaded = APPLY
    ? await loadHotelIntelligenceFromAirtable(spec.hpcId)
    : { commercial: null, eventSpaces: [], demandNodes: [], seasonality: [], needPeriods: [], evidence: [] };

  const completeness = {
    identity: true,
    commercial: Boolean(packet.commercial.roomsKeys),
    meeting:
      packet.commercial.totalMeetingSpaceSqFt != null ||
      packet.commercial.meetingRoomCount != null ||
      Boolean(packet.commercial.meetingSpaceFlag),
    demandNodes: packet.demandNodes.length > 0,
    eventSpaces: packet.eventSpaces.length > 0,
    seasonality: packet.seasonality.length > 0,
    needPeriods: false,
  };
  const score = Object.values(completeness).filter(Boolean).length;

  return {
    hotelKey: key,
    label: spec.label,
    hpcId: spec.hpcId,
    adpPropertyId: spec.adpPropertyId,
    apply: APPLY,
    sources,
    applyResult: APPLY
      ? {
          ok: applyResult.ok,
          baseId: applyResult.baseId,
          commercialId: applyResult.commercial?.id,
          eventSpaceIds: (applyResult.eventSpaces || []).map((r) => r.id),
          demandNodeIds: (applyResult.demandNodes || []).map((r) => r.id),
          seasonalityIds: (applyResult.seasonality || []).map((r) => r.id),
          evidenceIds: (applyResult.evidence || []).map((r) => r.id),
          actions: {
            commercial: applyResult.commercial?.action,
            eventSpaces: (applyResult.eventSpaces || []).map((r) => r.action),
            demandNodes: (applyResult.demandNodes || []).map((r) => r.action),
          },
        }
      : { dryRun: true, wouldWrite: applyResult },
    adpAttributes: {
      total: attrs.counts?.total,
      usedInAdp: attrs.counts?.usedInAdp,
      unused: (attrs.attributes || []).filter((a) => !a.usedInAdp).length,
      missingCritical: attrs.missingCritical || [],
      sync: adpSyncResult
        ? {
            createCount: adpSyncResult.createCount,
            updateCount: adpSyncResult.updateCount,
            deactivateCount: adpSyncResult.deactivateCount,
          }
        : null,
    },
    hiLoaded: {
      commercial: hiLoaded.commercial?.airtableRecordId || null,
      eventSpaces: (hiLoaded.eventSpaces || []).map((s) => ({ id: s.airtableRecordId, name: s.name })),
      demandNodes: (hiLoaded.demandNodes || []).map((n) => ({ id: n.airtableRecordId, name: n.name })),
      seasonality: (hiLoaded.seasonality || []).map((s) => s.airtableRecordId),
      needPeriods: (hiLoaded.needPeriods || []).map((s) => s.airtableRecordId),
      evidence: (hiLoaded.evidence || []).map((e) => e.airtableRecordId),
    },
    completeness: { ...completeness, score, max: 7, label: `${score}/7` },
  };
}

const keys =
  hotelArg === "both" ? ["ac", "spice"] : hotelArg === "ac" || hotelArg === "spice" ? [hotelArg] : ["ac", "spice"];

fs.mkdirSync(OUT_DIR, { recursive: true });
const results = {};
for (const k of keys) {
  results[k] = await applyOne(k);
  const p = path.join(OUT_DIR, `${k}-hi-apply-${APPLY ? "apply" : "dry"}.json`);
  fs.writeFileSync(p, JSON.stringify(results[k], null, 2) + "\n");
  console.error(`[hi] wrote ${p}`);
}

const summaryPath = path.join(OUT_DIR, `SUMMARY-${APPLY ? "apply" : "dry"}.json`);
fs.writeFileSync(
  summaryPath,
  JSON.stringify({ generatedAt: new Date().toISOString(), apply: APPLY, baseId: intel, results }, null, 2) + "\n"
);
console.log(JSON.stringify({ summaryPath, apply: APPLY, results: Object.fromEntries(Object.entries(results).map(([k, v]) => [k, { completeness: v.completeness, commercialId: v.applyResult.commercialId || v.hiLoaded.commercial, eventSpaces: v.hiLoaded.eventSpaces?.length || v.applyResult.eventSpaceIds?.length || 0, demandNodes: v.hiLoaded.demandNodes?.length || v.applyResult.demandNodeIds?.length || 0, adpAttrs: v.adpAttributes }])) }, null, 2));
