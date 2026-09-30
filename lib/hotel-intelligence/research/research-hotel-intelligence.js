/**
 * researchHotelIntelligence(hpcHotelId, { mode: "dry-run"|"apply" })
 *
 * Official-first commercial/meeting/demand research. Does not invent facts.
 * Default dry-run stages proposed HI records without Airtable writes.
 */

import { createHash } from "node:crypto";
import {
  buildHotelIntelligenceProfile,
} from "../adp-attributes/build-hotel-intelligence-profile.js";
import {
  commercialProfileDedupeKey,
  eventSpaceDedupeKey,
  demandNodeDedupeKey,
  evidenceDedupeKey,
  HOTEL_INTELLIGENCE_SCHEMA_VERSION,
} from "../schema/hotel-intelligence-schema-v1.js";
import { buildAdpHotelAttributes } from "../adp-attributes/build-adp-hotel-attributes.js";
import { applyHotelIntelligencePacket } from "../schema/hi-airtable-store.js";
import { syncHotelAdpAttributesToAirtable } from "../adp-attributes/airtable-store.js";

const SOURCE_TIER = Object.freeze({
  T1_OFFICIAL: 1,
  T2_INDUSTRY: 2,
  T3_SECONDARY: 3,
  T4_DISCOVERY: 4,
});

async function fetchText(url, { timeoutMs = 20000 } = {}) {
  const started = Date.now();
  try {
    const ctrl = new AbortController();
    const t = setTimeout(() => ctrl.abort(), timeoutMs);
    const res = await fetch(url, {
      signal: ctrl.signal,
      redirect: "follow",
      headers: {
        "User-Agent": "DealalityHotelIntelligence/1.0 (+research; dry-run)",
        Accept: "text/html,application/xhtml+xml",
      },
    });
    clearTimeout(t);
    const text = await res.text();
    return {
      ok: res.ok,
      status: res.status,
      url: res.url || url,
      bytes: text.length,
      text: text.slice(0, 500_000),
      elapsedMs: Date.now() - started,
      tier: /marriott\.com|hilton\.com|hyatt\.com|ihg\.com/.test(url)
        ? SOURCE_TIER.T1_OFFICIAL
        : /cvent\.com/.test(url)
          ? SOURCE_TIER.T2_INDUSTRY
          : SOURCE_TIER.T3_SECONDARY,
    };
  } catch (err) {
    return {
      ok: false,
      status: 0,
      url,
      error: err.message || String(err),
      elapsedMs: Date.now() - started,
    };
  }
}

function extractNumberNear(text, patterns) {
  for (const re of patterns) {
    const m = text.match(re);
    if (m) {
      const n = Number(String(m[1]).replace(/,/g, ""));
      if (Number.isFinite(n)) return n;
    }
  }
  return null;
}

function stageEvidence(hpcHotelId, entityType, fieldName, value, source) {
  return {
    evidenceId: evidenceDedupeKey(
      hpcHotelId,
      entityType,
      fieldName,
      source.url,
      value
    ),
    hpcHotelId,
    entityType,
    fieldName,
    valueObserved: value,
    sourceName: source.name,
    sourceType: source.sourceType,
    sourceUrl: source.url,
    retrievedAt: new Date().toISOString(),
    evidenceSnippet: source.snippet || null,
    confidence: source.confidence || "MEDIUM",
    evidenceStrength: source.tier === SOURCE_TIER.T1_OFFICIAL ? "STRONG" : "MODERATE",
    current: true,
    schemaVersion: HOTEL_INTELLIGENCE_SCHEMA_VERSION,
  };
}

/**
 * @param {string} hpcHotelId
 * @param {{ mode?: "dry-run"|"apply", forceRefresh?: boolean, maxCostUsd?: number, knownCventUrl?: string }} [opts]
 */
export async function researchHotelIntelligence(hpcHotelId, opts = {}) {
  const mode = opts.mode || "dry-run";
  const cost = {
    httpFetches: 0,
    renderedPages: 0,
    searchQueries: 0,
    estimatedUsd: 0,
  };
  const errors = [];
  const skipped = [];
  const conflicts = [];

  const baseProfile = await buildHotelIntelligenceProfile(hpcHotelId, {
    skipLiveHpc: opts.skipLiveHpc === true,
  });
  if (!baseProfile.ok) {
    return { ok: false, error: baseProfile.error, mode, cost };
  }

  const id = baseProfile.identity;
  const officialUrl = id.officialPropertyUrl;

  // Prefer /meetings/ for Marriott family; keep /events/ as secondary candidate
  const eventsUrlCandidates = [];
  if (baseProfile.commercialProfile?.eventsUrl) {
    eventsUrlCandidates.push(baseProfile.commercialProfile.eventsUrl);
  }
  if (officialUrl) {
    if (/\/overview\/?$/i.test(officialUrl)) {
      eventsUrlCandidates.push(officialUrl.replace(/\/overview\/?$/i, "/meetings/"));
      eventsUrlCandidates.push(officialUrl.replace(/\/overview\/?$/i, "/events/"));
    } else {
      eventsUrlCandidates.push(officialUrl.replace(/\/?$/, "/meetings/"));
      eventsUrlCandidates.push(officialUrl.replace(/\/?$/, "/events/"));
    }
  }
  const uniqueEventUrls = [...new Set(eventsUrlCandidates.filter(Boolean))];
  let eventsUrl = uniqueEventUrls[0] || null;

  const sourcesChecked = [];
  const evidence = [];
  const proposedCommercial = {
    profileKey: commercialProfileDedupeKey(id.hpcHotelId),
    hpcHotelId: id.hpcHotelId,
    dealalityHotelId: id.dealalityHotelId,
    adpPropertyId: id.adpPropertyId,
    hotelName: id.hotelName,
    brand: id.brand,
    roomsKeys: id.roomsKeys,
    officialPropertyUrl: officialUrl,
    officialEventsUrl: eventsUrl,
    meetingSpaceFlag: true,
    totalMeetingSpaceSqFt: baseProfile.commercialProfile?.meetingSpace?.totalSqFt ?? null,
    meetingRoomCount: baseProfile.commercialProfile?.meetingSpace?.meetingRooms ?? null,
    largestMeetingSpaceSqFt:
      baseProfile.commercialProfile?.meetingSpace?.largestRoom?.sqFt ?? null,
    largestEventCapacity:
      baseProfile.commercialProfile?.meetingSpace?.largestRoom?.capacity ?? null,
    researchStatus: "Partial",
    researchVersion: "hotel_intelligence_research_v1",
    confidence: "MEDIUM",
    schemaVersion: HOTEL_INTELLIGENCE_SCHEMA_VERSION,
  };

  // Fetch official overview
  if (officialUrl) {
    const page = await fetchText(officialUrl);
    cost.httpFetches += 1;
    cost.estimatedUsd += 0; // direct HTTP
    sourcesChecked.push({
      role: "official_property",
      ...page,
      name: "Official property page",
      sourceType: "Public Research",
    });
    if (!page.ok) errors.push({ step: "fetch_official", error: page.error || page.status });
    else {
      evidence.push(
        stageEvidence(id.hpcHotelId, "Commercial Profile", "Official Property URL", officialUrl, {
          name: "Official property page",
          sourceType: "Public Research",
          url: page.url,
          tier: page.tier,
          confidence: "HIGH",
          snippet: page.text.slice(0, 240),
        })
      );
    }
  } else {
    skipped.push("no_official_property_url");
  }

  // Fetch official events / meetings pages (bounded)
  for (const candidate of uniqueEventUrls.slice(0, 2)) {
    const page = await fetchText(candidate);
    cost.httpFetches += 1;
    sourcesChecked.push({
      role: "official_events",
      ...page,
      name: "Official events/meetings page",
      sourceType: "Public Research",
    });
    if (page.ok) {
      eventsUrl = page.url || candidate;
      proposedCommercial.officialEventsUrl = eventsUrl;
      const rooms = extractNumberNear(page.text, [
        /(\d{1,3})\s*(?:meeting|event)\s*rooms?/i,
        /meeting rooms[^0-9]{0,40}(\d{1,3})/i,
        /(\d{1,3})\s*(?:salas?|salones?)\s*(?:de\s*)?(?:reuniones|eventos|juntas)/i,
      ]);
      const sqft = extractNumberNear(page.text, [
        /([\d,]+)\s*(?:sq\.?\s*ft|square feet)/i,
        /total[^0-9]{0,30}([\d,]+)\s*(?:sq\.?\s*ft)/i,
        /([\d,]+)\s*(?:m(?:²|2)|metros?\s*cuadrados)/i,
      ]);
      const largestCap = extractNumberNear(page.text, [
        /(?:up to|hasta)\s*([\d,]+)\s*(?:guests?|people|personas|attendees)/i,
        /(?:theater|teatro)[^0-9]{0,20}([\d,]+)/i,
      ]);
      if (rooms != null && proposedCommercial.meetingRoomCount == null) {
        proposedCommercial.meetingRoomCount = rooms;
      }
      if (sqft != null && proposedCommercial.totalMeetingSpaceSqFt == null) {
        // Convert m2 → approx sq ft when Spanish pattern matched raw meters
        const looksMetric = /m(?:²|2)|metros?\s*cuadrados/i.test(page.text.slice(0, 50000));
        proposedCommercial.totalMeetingSpaceSqFt =
          looksMetric && sqft < 5000 ? Math.round(sqft * 10.764) : sqft;
      }
      if (largestCap != null && proposedCommercial.largestEventCapacity == null) {
        proposedCommercial.largestEventCapacity = largestCap;
      }
      evidence.push(
        stageEvidence(id.hpcHotelId, "Commercial Profile", "Official Events URL", eventsUrl, {
          name: "Official events/meetings page",
          sourceType: "Public Research",
          url: eventsUrl,
          tier: SOURCE_TIER.T1_OFFICIAL,
          confidence: "HIGH",
          snippet: page.text.slice(0, 240),
        })
      );
      if (proposedCommercial.meetingRoomCount != null) {
        evidence.push(
          stageEvidence(
            id.hpcHotelId,
            "Commercial Profile",
            "Meeting Room Count",
            proposedCommercial.meetingRoomCount,
            {
              name: "Official events / prior profile",
              sourceType: "Hotel Commercial Profile",
              url: eventsUrl,
              tier: SOURCE_TIER.T1_OFFICIAL,
              confidence: "HIGH",
            }
          )
        );
      }
      if (proposedCommercial.totalMeetingSpaceSqFt != null) {
        evidence.push(
          stageEvidence(
            id.hpcHotelId,
            "Commercial Profile",
            "Total Meeting Space Sq Ft",
            proposedCommercial.totalMeetingSpaceSqFt,
            {
              name: "Official events / prior profile",
              sourceType: "Hotel Commercial Profile",
              url: eventsUrl,
              tier: SOURCE_TIER.T1_OFFICIAL,
              confidence: "HIGH",
            }
          )
        );
      }
      break;
    } else {
      errors.push({ step: "fetch_events", url: candidate, error: page.error || page.status });
    }
  }

  // Cvent (public only)
  const cventUrl =
    opts.knownCventUrl ||
    (id.adpPropertyId === "adp_bethesda_marriott"
      ? "https://www.cvent.com/venues/bethesda/hotel/bethesda-marriott/venue-759e1222-3f0f-4739-9375-e9cb7962c6a4"
      : null);
  let cvent = { available: false, reason: "not_attempted" };
  if (cventUrl) {
    const page = await fetchText(cventUrl);
    cost.httpFetches += 1;
    sourcesChecked.push({
      role: "cvent",
      ...page,
      name: "Cvent Supplier Network",
      sourceType: "Public Research",
    });
    if (!page.ok) {
      cvent = { available: false, reason: page.error || `http_${page.status}`, url: cventUrl };
      skipped.push("cvent_unavailable_or_blocked");
    } else {
      cvent = { available: true, url: page.url, bytes: page.bytes };
      const built = extractNumberNear(page.text, [
        /built[^0-9]{0,20}(19\d{2}|20\d{2})/i,
        /year built[^0-9]{0,10}(19\d{2}|20\d{2})/i,
      ]);
      const renovated = extractNumberNear(page.text, [
        /renovat(?:ed|ion)[^0-9]{0,20}(19\d{2}|20\d{2})/i,
      ]);
      const suites = extractNumberNear(page.text, [/(\d{1,3})\s*suites?/i]);
      const singles = extractNumberNear(page.text, [/(\d{1,3})\s*single[- ]bed/i]);
      const doubles = extractNumberNear(page.text, [/(\d{1,3})\s*double[- ]bed/i]);
      const outdoor = extractNumberNear(page.text, [
        /([\d,]+)\s*(?:sq\.?\s*ft)[^.]{0,40}outdoor/i,
        /outdoor[^0-9]{0,40}([\d,]+)\s*(?:sq\.?\s*ft)/i,
      ]);
      const parkingRate = extractNumberNear(page.text, [
        /\$?\s*(\d{1,3}(?:\.\d+)?)\s*\/?\s*(?:per\s*)?day/i,
      ]);
      const airportMi = extractNumberNear(page.text, [
        /([\d.]+)\s*miles?\s*from\s*(?:the\s*)?airport/i,
      ]);
      const acres = extractNumberNear(page.text, [/([\d.]+)\s*[- ]?acre/i]);

      if (built) proposedCommercial.yearBuilt = built;
      if (renovated) proposedCommercial.yearRenovated = renovated;
      if (suites != null) proposedCommercial.suites = suites;
      if (singles != null) proposedCommercial.singleBedRooms = singles;
      if (doubles != null) proposedCommercial.doubleBedRooms = doubles;
      if (outdoor != null) proposedCommercial.outdoorEventSpaceSqFt = outdoor;
      if (parkingRate != null) {
        proposedCommercial.parkingDailyRate = parkingRate;
        proposedCommercial.parkingType = proposedCommercial.parkingType || "Paid";
      }
      if (airportMi != null) proposedCommercial.airportDistanceMiles = airportMi;
      if (acres != null) proposedCommercial.siteAcres = acres;

      evidence.push(
        stageEvidence(id.hpcHotelId, "Commercial Profile", "Cvent Profile URL", cventUrl, {
          name: "Cvent",
          sourceType: "Public Research",
          url: page.url,
          tier: SOURCE_TIER.T2_INDUSTRY,
          confidence: "MEDIUM",
          snippet: page.text.slice(0, 200),
        })
      );
    }
  } else {
    skipped.push("cvent_url_unknown");
  }

  // Conflict example: Marriott capacity 450 vs Cvent 425 if both present
  const marriottCap = baseProfile.commercialProfile?.meetingSpace?.largestRoom?.capacity;
  if (marriottCap != null && cvent.available) {
    // Keep official preferred; record conflict only if we extracted a different number
    // (extraction may miss — don't invent 425)
  }
  if (
    marriottCap != null &&
    proposedCommercial.largestEventCapacity != null &&
    Number(marriottCap) !== Number(proposedCommercial.largestEventCapacity)
  ) {
    conflicts.push({
      field: "Largest Event Capacity",
      preferredValue: marriottCap,
      preferredSource: "Official / Hotel Commercial Profile",
      alternateValue: proposedCommercial.largestEventCapacity,
      alternateSource: "Research page",
      resolution: "Prefer official hotel/brand page over third-party",
    });
    proposedCommercial.largestEventCapacity = marriottCap;
    proposedCommercial.conflictStatus = true;
  } else if (marriottCap != null) {
    proposedCommercial.largestEventCapacity = marriottCap;
  }

  // Prefer existing high-confidence meeting facts from profile when page extract weak
  const ms = baseProfile.commercialProfile?.meetingSpace;
  if (ms?.totalSqFt != null) proposedCommercial.totalMeetingSpaceSqFt = ms.totalSqFt;
  if (ms?.meetingRooms != null) proposedCommercial.meetingRoomCount = ms.meetingRooms;
  if (ms?.largestRoom?.sqFt != null) {
    proposedCommercial.largestMeetingSpaceSqFt = ms.largestRoom.sqFt;
  }

  // Confidence: official meeting HIGH; if any Cvent-only commercial fields present → MEDIUM overall
  const cventOnlyFields = [
    proposedCommercial.yearBuilt,
    proposedCommercial.yearRenovated,
    proposedCommercial.siteAcres,
    proposedCommercial.parkingDailyRate,
    proposedCommercial.suites,
    proposedCommercial.singleBedRooms,
    proposedCommercial.doubleBedRooms,
    proposedCommercial.outdoorEventSpaceSqFt,
    proposedCommercial.airportDistanceMiles,
  ].filter((v) => v != null);
  const hasOfficialMeeting =
    proposedCommercial.totalMeetingSpaceSqFt != null &&
    proposedCommercial.meetingRoomCount != null;
  if (cventOnlyFields.length && cvent.available) {
    proposedCommercial.confidence = "MEDIUM";
    proposedCommercial.notes = [
      proposedCommercial.notes,
      "Cvent Tier-2 fields present (yearRenovated/parking/siteAcres/etc) — MEDIUM until official corroboration.",
    ]
      .filter(Boolean)
      .join(" ");
  } else if (hasOfficialMeeting) {
    proposedCommercial.confidence = "HIGH";
  }

  proposedCommercial.lastResearchedAt = new Date().toISOString();
  proposedCommercial.researchStatus = "Partial";

  const proposedEventSpaces = [];
  if (ms?.largestRoom?.name) {
    proposedEventSpaces.push({
      spaceKey: eventSpaceDedupeKey(id.hpcHotelId, ms.largestRoom.name),
      hpcHotelId: id.hpcHotelId,
      spaceName: ms.largestRoom.name,
      spaceType: "Ballroom",
      sqFt: ms.largestRoom.sqFt,
      theaterCapacity: ms.largestRoom.capacity,
      sourceUrl: ms.source || eventsUrl,
      confidence: ms.confidence || "HIGH",
      active: true,
      schemaVersion: HOTEL_INTELLIGENCE_SCHEMA_VERSION,
    });
  } else if (
    proposedCommercial.totalMeetingSpaceSqFt != null ||
    proposedCommercial.largestMeetingSpaceSqFt != null ||
    proposedCommercial.meetingRoomCount != null
  ) {
    // Named inventory unavailable — persist aggregate meeting capability as one space row
    const aggregateName = "Aggregate meeting inventory (official totals)";
    proposedEventSpaces.push({
      spaceKey: eventSpaceDedupeKey(id.hpcHotelId, aggregateName),
      hpcHotelId: id.hpcHotelId,
      spaceName: aggregateName,
      spaceType: "Meeting Room",
      sqFt:
        proposedCommercial.largestMeetingSpaceSqFt ??
        proposedCommercial.totalMeetingSpaceSqFt ??
        null,
      theaterCapacity: proposedCommercial.largestEventCapacity ?? null,
      sourceUrl: eventsUrl || officialUrl,
      confidence: "MEDIUM",
      active: true,
      notes: "Aggregate totals from official meetings page — individual named spaces not extracted",
      schemaVersion: HOTEL_INTELLIGENCE_SCHEMA_VERSION,
    });
  }

  const proposedDemandNodes = (baseProfile.demandNodes || []).map((n) => ({
    nodeKey: demandNodeDedupeKey(id.hpcHotelId, n.name, n.type),
    hpcHotelId: id.hpcHotelId,
    demandNodeName: n.name,
    demandNodeType: n.type,
    sourceType: n.sourceType || "Hotel Supplied",
    sourceName: n.sourceName || "GDI hotel demand config",
    confidence: n.confidence || "MEDIUM",
    demandStrength: "model-derived-unscored",
    active: true,
    schemaVersion: HOTEL_INTELLIGENCE_SCHEMA_VERSION,
  }));

  // Do NOT invent seasonality / need periods
  const proposedSeasonality = [];
  const proposedNeedPeriods = [];

  // Completeness
  const filledSignals = [
    proposedCommercial.roomsKeys != null,
    proposedCommercial.totalMeetingSpaceSqFt != null,
    proposedCommercial.meetingRoomCount != null,
    proposedCommercial.largestMeetingSpaceSqFt != null,
    Boolean(proposedCommercial.officialPropertyUrl),
    Boolean(proposedCommercial.officialEventsUrl),
    proposedCommercial.yearBuilt != null || proposedCommercial.yearRenovated != null,
    proposedEventSpaces.length > 0,
    proposedDemandNodes.length > 0,
  ].filter(Boolean).length;

  const adpAttrs = await buildAdpHotelAttributes(id.hpcHotelId, {
    profile: {
      ...baseProfile,
      commercialProfile: {
        ...baseProfile.commercialProfile,
        meetingSpace: {
          ...(baseProfile.commercialProfile?.meetingSpace || {}),
          totalSqFt: proposedCommercial.totalMeetingSpaceSqFt,
          meetingRooms: proposedCommercial.meetingRoomCount,
          largestRoom: {
            name: ms?.largestRoom?.name || "Largest space",
            sqFt: proposedCommercial.largestMeetingSpaceSqFt,
            capacity: proposedCommercial.largestEventCapacity,
          },
          source: eventsUrl,
          confidence: "HIGH",
        },
      },
      eventSpaces: proposedEventSpaces.map((s) => ({
        name: s.spaceName,
        type: s.spaceType,
        sqFt: s.sqFt,
        capacities: { theaterOrEvent: s.theaterCapacity },
        sourceUrl: s.sourceUrl,
        confidence: s.confidence,
      })),
      demandNodes: proposedDemandNodes.map((n) => ({
        name: n.demandNodeName,
        type: n.demandNodeType,
        sourceType: n.sourceType,
        sourceName: n.sourceName,
        confidence: n.confidence,
      })),
      seasonality: [],
      needPeriods: [],
    },
  });

  const used = (adpAttrs.attributes || []).filter((a) => a.usedInAdp);
  const unused = (adpAttrs.attributes || []).filter((a) => !a.usedInAdp);

  const applyPacket = {
    commercial: proposedCommercial,
    eventSpaces: proposedEventSpaces,
    demandNodes: proposedDemandNodes,
    seasonality: proposedSeasonality,
    evidence,
  };

  let applyResult = null;
  let adpSyncResult = null;
  let applied = false;

  if (mode === "apply") {
    applyResult = await applyHotelIntelligencePacket(applyPacket, { dryRun: false });
    adpSyncResult = await syncHotelAdpAttributesToAirtable(adpAttrs, { dryRun: false });
    applied = Boolean(applyResult?.ok);
  }

  const result = {
    ok: true,
    mode,
    applied,
    hotelId: id.hpcHotelId,
    adpPropertyId: id.adpPropertyId,
    hotelName: id.hotelName,
    existingFacts: {
      rooms: id.roomsKeys,
      meetingFromProfile: ms || null,
      demandNodesFromConfig: (baseProfile.demandNodes || []).length,
      hpcLoaded: baseProfile.evidenceSummary?.hpcLoaded,
    },
    discovered: {
      commercial: proposedCommercial,
      eventSpaces: proposedEventSpaces,
      demandNodes: proposedDemandNodes,
      seasonality: proposedSeasonality,
      needPeriodsPending: [
        {
          note: "No hotel-supplied need period frozen yet — left empty by policy",
          staged: false,
          usedInAdp: false,
        },
      ],
      cvent,
    },
    evidence,
    conflicts,
    sourcesChecked: sourcesChecked.map((s) => ({
      role: s.role,
      url: s.url,
      ok: s.ok,
      status: s.status,
      bytes: s.bytes,
      error: s.error || null,
      elapsedMs: s.elapsedMs,
    })),
    skipped,
    errors,
    cost,
    completeness: {
      profileFilledSignals: filledSignals,
      commercialReady:
        proposedCommercial.roomsKeys != null &&
        proposedCommercial.totalMeetingSpaceSqFt != null &&
        proposedCommercial.meetingRoomCount != null,
      eventSpaces: proposedEventSpaces.length,
      demandNodes: proposedDemandNodes.length,
      evidenceRows: evidence.length,
      seasonalityRows: 0,
      needPeriodRows: 0,
      note:
        mode === "apply"
          ? "HI Airtable writes applied for Bethesda canary"
          : "Airtable HI commercial tables not written in dry-run",
    },
    adpAttributes: {
      total: adpAttrs.attributes?.length || 0,
      usedInAdp: used.length,
      unused: unused.length,
      usedNames: used.map((a) => a.attributeName),
      unusedNames: unused.map((a) => a.attributeName),
      missingCritical: adpAttrs.missingCritical || [],
    },
    proposedAirtableMutations:
      mode === "dry-run"
        ? {
            hotelCommercialProfiles: { upsert: 1, key: proposedCommercial.profileKey },
            hotelEventSpaces: { upsert: proposedEventSpaces.length },
            hotelDemandNodes: { upsert: proposedDemandNodes.length },
            hotelSeasonalityNeedPeriods: { upsert: 0, note: "none verified — left empty" },
            hotelIntelligenceEvidence: { upsert: evidence.length },
            hotelAdpAttributes: {
              upsert: adpAttrs.attributes?.length || 0,
              note: "derived; synced on apply",
            },
          }
        : {
            applyResult,
            adpSyncResult: adpSyncResult
              ? {
                  createCount: adpSyncResult.createCount,
                  updateCount: adpSyncResult.updateCount,
                  deactivateCount: adpSyncResult.deactivateCount,
                  applied: adpSyncResult.applied,
                }
              : null,
          },
    applyResult,
    adpSyncResult,
    blockers: [
      mode === "dry-run" ? "Awaiting founder approval before apply" : null,
      !cvent.available ? "Cvent public page unavailable or not confirmed — T2 facts partial" : null,
      "Hotel-supplied need periods not yet provided (tables left empty by policy)",
    ].filter(Boolean),
    generatedAt: new Date().toISOString(),
    contentHash: createHash("sha256")
      .update(JSON.stringify(proposedCommercial))
      .digest("hex")
      .slice(0, 12),
  };

  return result;
}
