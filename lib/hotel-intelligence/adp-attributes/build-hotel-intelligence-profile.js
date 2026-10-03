/**
 * buildHotelIntelligenceProfile(hpcHotelId)
 *
 * Canonical read aggregator for Hotel Intelligence. Sources (priority):
 * 1) Live HPC identity (when available)
 * 2) Hotel Intelligence Airtable tables (commercial / event / demand / seasonality)
 * 3) ADP property profile fixture (legacy interim)
 * 4) GDI hotel demand config (legacy interim for demand nodes)
 *
 * Does NOT invent facts. Does NOT create a parallel hotel master.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import Airtable from "airtable";
import {
  resolveCanonicalHotelId,
  loadAdpGdiAliasMap,
  isAirtableRecordId,
} from "../../hotel-census/adp-gdi-canonical-identity.js";
import { getCensusLinkEntry } from "../../ai-demand-positioning/census-link-registry.js";
import { loadHotelDemandConfig } from "../../group-demand-intelligence/hotel-profile.js";
import { loadHotelIntelligenceFromAirtable } from "../schema/hi-airtable-store.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const REPO_ROOT = path.resolve(__dirname, "../../..");
const PROFILES_DIR = path.join(REPO_ROOT, "fixtures", "ai-demand-positioning");

function readJsonSafe(p) {
  if (!fs.existsSync(p)) return null;
  try {
    return JSON.parse(fs.readFileSync(p, "utf8"));
  } catch {
    return null;
  }
}

function findAdpPropertyIdForHpc(hpcHotelId) {
  const id = String(hpcHotelId || "").trim();
  const map = loadAdpGdiAliasMap();
  const entry = map.aliases?.[id];
  if (entry?.adpPropertyId) return entry.adpPropertyId;
  for (const [key, e] of Object.entries(map.aliases || {})) {
    if (e?.canonicalHotelId === id && String(key).startsWith("adp_")) return key;
    if (e?.canonicalHotelId === id && e.adpPropertyId) return e.adpPropertyId;
  }
  const linksPath = path.join(PROFILES_DIR, "census-links-v1.json");
  const links = readJsonSafe(linksPath)?.links || {};
  for (const [adpId, link] of Object.entries(links)) {
    if (link?.censusRecordId === id) return adpId;
  }
  return null;
}

function loadPropertyProfile(adpPropertyId) {
  if (!adpPropertyId) return null;
  const files = fs.readdirSync(PROFILES_DIR).filter((f) => f.endsWith("-property-profile.json"));
  for (const f of files) {
    const j = readJsonSafe(path.join(PROFILES_DIR, f));
    if (j?.propertyId === adpPropertyId) return { file: f, profile: j };
  }
  return null;
}

async function loadHpcRecord(hpcHotelId, opts = {}) {
  if (opts.skipLiveHpc) return null;
  const apiKey = process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT;
  const baseId = process.env.AIRTABLE_BASE_ID_ALT;
  if (!apiKey || !baseId || !isAirtableRecordId(hpcHotelId)) return null;
  try {
    const base = new Airtable({ apiKey }).base(baseId);
    const rec = await base("Hotel Property Census").find(hpcHotelId);
    return { id: rec.id, fields: rec.fields || {} };
  } catch (err) {
    return { error: err.message || String(err) };
  }
}

function demandNodesFromGdiConfig(gdiConfig) {
  const anchors = gdiConfig?.commercialPriorities?.demandAnchorFocus || [];
  return anchors.map((name) => ({
    name: String(name),
    type: inferDemandNodeType(name),
    sourceType: "Hotel Supplied",
    sourceName: "GDI hotel demand config",
    confidence: "MEDIUM",
  }));
}

function inferDemandNodeType(name = "") {
  const n = String(name).toLowerCase();
  if (/nih|walter reed|fda|medical|health|hospital|zoo/.test(n)) return "Medical / Healthcare";
  if (/federal|government|dc\b|district of columbia/.test(n)) return "Government";
  if (/military|walter reed/.test(n)) return "Military";
  if (/university|american university|college/.test(n)) return "University";
  if (/metro|transit|airport/.test(n)) return "Transit";
  if (/downtown|strathmore|tourism/.test(n)) return "Tourism";
  if (/employer|corporate/.test(n)) return "Corporate";
  return "Other";
}

function completenessFromParts(parts) {
  const checks = {
    identity: Boolean(parts.identity?.hpcHotelId && parts.identity?.hotelName),
    commercial: Boolean(parts.commercialProfile?.rooms != null),
    meeting: Boolean(
      parts.commercialProfile?.meetingSpace?.totalSqFt != null ||
        parts.commercialProfile?.meetingSpace?.meetingRooms != null
    ),
    demandNodes: (parts.demandNodes || []).length > 0,
    eventSpaces: (parts.eventSpaces || []).length > 0,
    seasonality: (parts.seasonality || []).length > 0,
    needPeriods: (parts.needPeriods || []).length > 0,
  };
  const score = Object.values(checks).filter(Boolean).length;
  return { ...checks, score, max: 7, label: `${score}/7` };
}

/**
 * @param {string} hpcHotelId
 * @param {{ skipLiveHpc?: boolean }} [opts]
 */
export async function buildHotelIntelligenceProfile(hpcHotelId, opts = {}) {
  const resolved =
    resolveCanonicalHotelId(hpcHotelId) ||
    (isAirtableRecordId(hpcHotelId) ? hpcHotelId : null);
  if (!resolved) {
    return {
      ok: false,
      error: "unresolved_hpc_hotel_id",
      hotelId: hpcHotelId || null,
    };
  }

  const adpPropertyId = findAdpPropertyIdForHpc(resolved);
  const link = adpPropertyId ? getCensusLinkEntry(adpPropertyId) : null;
  const loaded = loadPropertyProfile(adpPropertyId);
  const profile = loaded?.profile || null;
  const gdiConfig = loadHotelDemandConfig(resolved) || null;
  const hpc = await loadHpcRecord(resolved, opts);
  const hpcFields = hpc?.fields || {};

  let hiAirtable = null;
  if (opts.skipHiAirtable !== true) {
    try {
      hiAirtable = await loadHotelIntelligenceFromAirtable(resolved);
    } catch (err) {
      hiAirtable = { ok: false, error: err.message || String(err) };
    }
  }
  const hiCommercial = hiAirtable?.commercial || null;

  const identity = {
    hpcHotelId: resolved,
    dealalityHotelId:
      hpcFields["Dealality Hotel ID"] || hiCommercial?.dealalityHotelId || null,
    adpPropertyId: adpPropertyId || hiCommercial?.adpPropertyId || null,
    hotelName:
      hpcFields["Property Name"] ||
      hiCommercial?.hotelName ||
      profile?.name ||
      gdiConfig?.displayName ||
      link?.propertyName ||
      null,
    brand:
      hpcFields["Current Brand"] || hiCommercial?.brand || profile?.brand || null,
    brandFamily: hpcFields["Brand Family"] || profile?.parentCompany || null,
    address: hpcFields["Address"] || profile?.address || null,
    city: hpcFields["City"] || profile?.city || null,
    state: hpcFields["State / Region"] || profile?.state || null,
    postalCode: hpcFields["Postal Code"] || null,
    country: hpcFields["Country"] || profile?.country || null,
    identityKey:
      hpcFields["Property Identity Key"] || link?.canonicalHotelId || profile?.registryKey || null,
    officialPropertyUrl:
      hpcFields["Official Property URL"] ||
      hiCommercial?.officialPropertyUrl ||
      profile?.officialPropertyPageUrl ||
      profile?.website ||
      null,
    roomsKeys:
      hpcFields["Rooms / Keys"] ??
      hiCommercial?.roomsKeys ??
      profile?.rooms ??
      null,
    meetingSpaceFlag:
      hpcFields["Meeting Space Flag"] ?? hiCommercial?.meetingSpaceFlag ?? null,
    latitude: hpcFields["Latitude"] ?? null,
    longitude: hpcFields["Longitude"] ?? null,
    source: hpc?.id
      ? "HPC"
      : hiCommercial
        ? "Hotel Commercial Profile"
        : profile
          ? "Hotel Commercial Profile"
          : "Model Derived",
  };

  const meetingFromHi = hiCommercial
    ? {
        totalSqFt: hiCommercial.totalMeetingSpaceSqFt,
        meetingRooms: hiCommercial.meetingRoomCount,
        largestRoom:
          hiCommercial.largestMeetingSpaceSqFt != null ||
          hiCommercial.largestEventCapacity != null
            ? {
                name:
                  (hiAirtable.eventSpaces || []).find((s) => s.sqFt === hiCommercial.largestMeetingSpaceSqFt)
                    ?.name ||
                  (hiAirtable.eventSpaces || [])[0]?.name ||
                  null,
                sqFt: hiCommercial.largestMeetingSpaceSqFt,
                capacity: hiCommercial.largestEventCapacity,
              }
            : null,
        source: hiCommercial.officialEventsUrl || null,
        confidence: hiCommercial.confidence || "MEDIUM",
      }
    : null;
  const meeting = meetingFromHi || profile?.meetingSpace || null;

  const commercialProfile = {
    rooms: identity.roomsKeys,
    roomCountConfidence: profile?.roomCountConfidence || hiCommercial?.confidence || null,
    meetingSpace: meeting
      ? {
          totalSqFt: meeting.totalSqFt ?? null,
          meetingRooms: meeting.meetingRooms ?? null,
          largestRoom: meeting.largestRoom || null,
          source: meeting.source || null,
          confidence: meeting.confidence || null,
        }
      : identity.meetingSpaceFlag
        ? { totalSqFt: null, meetingRooms: null, largestRoom: null, flagOnly: true }
        : null,
    eventsUrl:
      hiCommercial?.officialEventsUrl || meeting?.source || profile?.eventsUrl || null,
    attributes: Array.isArray(profile?.attributes) ? profile.attributes : [],
    chainScale: profile?.chainScale || null,
    market: profile?.market || hpcFields["Market"] || null,
    submarket: profile?.submarket || hpcFields["Submarket"] || null,
    yearRenovated: hiCommercial?.yearRenovated ?? null,
    siteAcres: hiCommercial?.siteAcres ?? null,
    parkingType: hiCommercial?.parkingType || null,
    parkingDailyRate: hiCommercial?.parkingDailyRate ?? null,
    sourceType: hiCommercial
      ? "Hotel Commercial Profile"
      : profile
        ? "Hotel Commercial Profile"
        : hpc?.id
          ? "HPC"
          : "Model Derived",
    sourceRecordId: hiCommercial?.airtableRecordId || loaded?.file || resolved,
    sourceName: hiCommercial
      ? "Hotel Commercial Profiles (Airtable)"
      : loaded?.file || "Hotel Property Census",
    sourceUrl: identity.officialPropertyUrl,
    fromHiAirtable: Boolean(hiCommercial),
  };

  let eventSpaces = [];
  if ((hiAirtable?.eventSpaces || []).length) {
    eventSpaces = hiAirtable.eventSpaces.map((s) => ({
      name: s.name,
      type: s.type || "Ballroom",
      sqFt: s.sqFt ?? null,
      capacities: { theaterOrEvent: s.theaterCapacity ?? null },
      sourceType: "Hotel Event Space",
      sourceUrl: s.sourceUrl || null,
      confidence: s.confidence || "MEDIUM",
      sourceRecordId: s.airtableRecordId || null,
      fromHiAirtable: true,
    }));
  } else if (meeting?.largestRoom?.name) {
    eventSpaces.push({
      name: meeting.largestRoom.name,
      type: "Ballroom",
      sqFt: meeting.largestRoom.sqFt ?? null,
      capacities: { theaterOrEvent: meeting.largestRoom.capacity ?? null },
      sourceType: "Hotel Event Space",
      sourceUrl: meeting.source || null,
      confidence: meeting.confidence || "MEDIUM",
      sourceRecordId: loaded?.file || null,
      fromHiAirtable: false,
    });
  }

  const demandFromHi = hiAirtable?.demandNodes || [];
  const demandNodes = demandFromHi.length
    ? demandFromHi.map((n) => ({
        name: n.name,
        type: n.type,
        sourceType: n.sourceType || "Hotel Demand Node",
        sourceName: n.sourceName || "Hotel Demand Nodes (Airtable)",
        sourceUrl: n.sourceUrl || null,
        confidence: n.confidence || "MEDIUM",
        sourceRecordId: n.airtableRecordId || null,
        fromHiAirtable: true,
      }))
    : demandNodesFromGdiConfig(gdiConfig);

  const seasonality = hiAirtable?.seasonality || [];
  const needPeriods = hiAirtable?.needPeriods || [];

  const evidenceSummary = {
    hpcLoaded: Boolean(hpc?.id),
    hpcError: hpc?.error || null,
    profileLoaded: Boolean(profile),
    gdiConfigLoaded: Boolean(gdiConfig),
    commercialAirtable: Boolean(hiCommercial),
    demandNodeAirtable: demandFromHi.length > 0,
    eventSpaceAirtable: (hiAirtable?.eventSpaces || []).length > 0,
    evidenceAirtable: (hiAirtable?.evidence || []).length,
    hiAirtableError: hiAirtable?.error || null,
    legacyJsonFallback: {
      attributes: Boolean(profile?.attributes?.length),
      meetingFromJson: Boolean(profile?.meetingSpace) && !hiCommercial,
      demandFromGdi: demandFromHi.length === 0 && Boolean(gdiConfig),
    },
  };

  const parts = {
    identity,
    commercialProfile,
    eventSpaces,
    demandNodes,
    seasonality,
    needPeriods,
  };

  return {
    ok: true,
    hotelId: resolved,
    identity,
    commercialProfile,
    eventSpaces,
    demandNodes,
    seasonality,
    needPeriods,
    evidenceSummary,
    completeness: completenessFromParts(parts),
    generatedAt: new Date().toISOString(),
  };
}

export { findAdpPropertyIdForHpc, loadPropertyProfile };
