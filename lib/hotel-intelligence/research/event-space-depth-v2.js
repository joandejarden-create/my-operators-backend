/**
 * HI Evidence Depth V2 — Event/Meeting source ladder + Jev gap router.
 * Generic across hotels. No per-hotel URL hardcoding.
 */

import {
  EVENT_SPACE_SOURCE_LADDER,
  HI_JEV_ACTIONS,
  RESEARCH_DEPTH,
  SOURCE_FAMILY,
  classifySourceFamily,
  mayDeclareResearchedEmpty,
  researchDepthToDomainStatus,
  resolveResearchDepth,
} from "./evidence-depth-v2.js";
import {
  parseCventVenueHtml,
  parseOfficialMeetingsHtml,
} from "./venue-page-parsers-v2.js";
import { serpapiSearch } from "../../research-engine-v2/providers/serpapi-google-hotels/client.js";
import {
  commercialProfileDedupeKey,
  eventSpaceDedupeKey,
  evidenceDedupeKey,
  HOTEL_INTELLIGENCE_SCHEMA_VERSION,
} from "../schema/hotel-intelligence-schema-v1.js";
import { applyHotelIntelligencePacket } from "../schema/hi-airtable-store.js";
import { buildHotelIntelligenceProfile } from "../adp-attributes/build-hotel-intelligence-profile.js";
import { buildAdpHotelAttributes } from "../adp-attributes/build-adp-hotel-attributes.js";
import { syncHotelAdpAttributesToAirtable } from "../adp-attributes/airtable-store.js";
import { upsertDomainStatus } from "../onboarding/domain-status-store.js";
import { HI_DOMAIN, HI_DOMAIN_STATUS } from "../onboarding/domain-status-v1.js";

async function fetchText(url, { timeoutMs = 20000 } = {}) {
  const started = Date.now();
  try {
    const ctrl = new AbortController();
    const t = setTimeout(() => ctrl.abort(), timeoutMs);
    const res = await fetch(url, {
      signal: ctrl.signal,
      redirect: "follow",
      headers: {
        "User-Agent": "DealalityHotelIntelligence/1.0 (+evidence-depth-v2)",
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
      text: text.slice(0, 700_000),
      elapsedMs: Date.now() - started,
    };
  } catch (err) {
    return {
      ok: false,
      status: 0,
      url,
      error: err.message || String(err),
      elapsedMs: Date.now() - started,
      text: "",
    };
  }
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
    sourceType: source.sourceType || "Public Research",
    sourceUrl: source.url,
    retrievedAt: new Date().toISOString(),
    evidenceSnippet: source.snippet || null,
    confidence: source.confidence || "MEDIUM",
    evidenceStrength: source.strength || "MODERATE",
    current: true,
    schemaVersion: HOTEL_INTELLIGENCE_SCHEMA_VERSION,
    notes: source.notes || null,
  };
}

/**
 * Deterministic Jev-style router for event-space gaps.
 * Does NOT invent facts — only picks next action.
 */
export function decideEventSpaceNextAction(ctx = {}) {
  const attempted = new Set((ctx.sourcesAttempted || []).map((s) => s.family));
  const hasFirst = attempted.has(SOURCE_FAMILY.FIRST_PARTY);
  const hasCvent = attempted.has(SOURCE_FAMILY.CVENT);
  const hasPdf = attempted.has(SOURCE_FAMILY.OFFICIAL_PDF);
  const hasCvb = attempted.has(SOURCE_FAMILY.CVB);

  if (!hasFirst) {
    return {
      action: HI_JEV_ACTIONS.FIND_MEETING_EVENTS_PAGE,
      rationale: "First-party meetings page not yet confirmed",
      sourceFamily: SOURCE_FAMILY.FIRST_PARTY,
      queryHint: `"${ctx.hotelName}" (meetings OR events OR "meeting rooms" OR "salas de reuniones")`,
    };
  }
  if (!hasCvent && !hasPdf) {
    return {
      action: HI_JEV_ACTIONS.SEARCH_STRUCTURED_VENUE_PROFILE,
      rationale:
        "First-party incomplete for meeting inventory — search structured venue profiles",
      sourceFamily: SOURCE_FAMILY.CVENT,
      queryHint: `"${ctx.hotelName}" site:cvent.com/venues ${ctx.city || ""}`,
      fallbackQueryHint: `"${ctx.hotelName}" ("meeting space" OR "meeting rooms" OR "salas de reuniones" OR "venue profile") ${ctx.city || ""}`,
    };
  }
  if (!hasCvb && hasCvent && !ctx.populated) {
    return {
      action: HI_JEV_ACTIONS.FIND_CVB_VENUE_PROFILE,
      rationale: "Structured venue search incomplete — try CVB / destination venue listings",
      sourceFamily: SOURCE_FAMILY.CVB,
      queryHint: `"${ctx.hotelName}" (CVB OR "convention bureau" OR turismo OR "meeting venues") ${ctx.city || ""}`,
    };
  }
  if (ctx.conflicts?.length && !ctx.conflictVerified) {
    return {
      action: HI_JEV_ACTIONS.VERIFY_SOURCE_CONFLICT,
      rationale: "Structured vs narrative area conflict needs explicit retention",
      sourceFamily: SOURCE_FAMILY.OTHER,
    };
  }
  return {
    action: HI_JEV_ACTIONS.STOP_PUBLIC_DATA_CEILING,
    rationale: "Bounded event-space ladder exhausted without supportable inventory",
    sourceFamily: SOURCE_FAMILY.OTHER,
  };
}

async function serpVenueSearch(query, budget) {
  if (!process.env.SERPAPI_KEY && !process.env.SERPAPI_API_KEY) {
    return { ok: false, error: "serpapi_key_missing", hits: [] };
  }
  if (budget.queries <= 0) return { ok: false, error: "query_budget_exhausted", hits: [] };
  budget.queries -= 1;
  try {
    const serp = await serpapiSearch({
      engine: "google",
      q: query,
      num: 8,
      hl: "en",
    });
    const organic = serp?.data?.organic_results || [];
    return {
      ok: Boolean(serp?.ok),
      hits: organic.map((h) => ({
        title: h.title,
        url: h.link || h.url,
        snippet: h.snippet,
      })),
      error: serp?.ok ? null : serp?.error,
    };
  } catch (err) {
    return { ok: false, error: err.message || String(err), hits: [] };
  }
}

function preferStructuredOverNarrative(parsed) {
  // Canonical commercial uses structured inventory; conflicts retained separately
  return {
    commercial: { ...parsed.commercial },
    eventSpaces: parsed.eventSpaces || [],
    conflicts: parsed.conflicts || [],
  };
}

/**
 * Deepen event/meeting intelligence for one hotel via source ladder.
 */
export async function researchEventSpaceDepthV2(hpcHotelId, opts = {}) {
  const mode = opts.mode || "dry-run";
  const budget = {
    queries: opts.maxQueries ?? 4,
    fetches: opts.maxFetches ?? 8,
    jev: opts.maxJev ?? 1,
  };
  const persistLedger = opts.mode === "apply" || opts.persistDomainStatus === true;
  const ledger = {
    queries: 0,
    fetches: 0,
    jevCalls: 0,
    jevMaterial: 0,
    sourcesAttempted: [],
    sourceFamilyYield: {},
  };

  const profile = await buildHotelIntelligenceProfile(hpcHotelId, {
    skipLiveHpc: opts.skipLiveHpc === true,
  });
  if (!profile.ok) {
    return { ok: false, error: profile.error, hotelId: hpcHotelId };
  }

  const id = profile.identity;
  const hotelName = id.hotelName;
  const city = id.city || profile.commercialProfile?.market || "";
  const evidence = [];
  const sourcesChecked = [];
  let proposedCommercial = {
    profileKey: commercialProfileDedupeKey(id.hpcHotelId),
    hpcHotelId: id.hpcHotelId,
    dealalityHotelId: id.dealalityHotelId,
    adpPropertyId: id.adpPropertyId,
    hotelName,
    brand: id.brand,
    roomsKeys: id.roomsKeys,
    officialPropertyUrl: id.officialPropertyUrl,
    officialEventsUrl: profile.commercialProfile?.eventsUrl || null,
    meetingSpaceFlag: true,
    totalMeetingSpaceSqFt:
      profile.commercialProfile?.meetingSpace?.totalSqFt ?? null,
    meetingRoomCount: profile.commercialProfile?.meetingSpace?.meetingRooms ?? null,
    largestMeetingSpaceSqFt:
      profile.commercialProfile?.meetingSpace?.largestRoom?.sqFt ?? null,
    largestEventCapacity:
      profile.commercialProfile?.meetingSpace?.largestRoom?.capacity ?? null,
    researchStatus: "Partial",
    researchVersion: "hotel_intelligence_evidence_depth_v2",
    confidence: "MEDIUM",
    schemaVersion: HOTEL_INTELLIGENCE_SCHEMA_VERSION,
  };
  let proposedSpaces = [];
  let conflicts = [];
  let populated = false;
  let parserFailed = false;
  let providerFailed = false;

  const trackSource = (family, ok, meta = {}) => {
    ledger.sourcesAttempted.push({ family, ok, ...meta });
    ledger.sourceFamilyYield[family] = ledger.sourceFamilyYield[family] || {
      attempts: 0,
      successes: 0,
      facts: 0,
    };
    ledger.sourceFamilyYield[family].attempts += 1;
    if (ok) ledger.sourceFamilyYield[family].successes += 1;
  };

  // Step 1–2: official property + meetings
  const officialUrl = id.officialPropertyUrl;
  const meetingCandidates = [];
  if (profile.commercialProfile?.eventsUrl) {
    meetingCandidates.push(profile.commercialProfile.eventsUrl);
  }
  if (officialUrl) {
    if (/\/overview\/?$/i.test(officialUrl)) {
      meetingCandidates.push(officialUrl.replace(/\/overview\/?$/i, "/meetings/"));
      meetingCandidates.push(officialUrl.replace(/\/overview\/?$/i, "/events/"));
    } else {
      meetingCandidates.push(officialUrl.replace(/\/?$/, "/meetings/"));
      meetingCandidates.push(officialUrl.replace(/\/?$/, "/events/"));
    }
  }

  for (const url of [...new Set(meetingCandidates)].slice(0, 2)) {
    if (budget.fetches <= 0) break;
    budget.fetches -= 1;
    ledger.fetches += 1;
    const page = await fetchText(url);
    sourcesChecked.push({ role: "official_meetings", ...page, family: SOURCE_FAMILY.FIRST_PARTY });
    trackSource(SOURCE_FAMILY.FIRST_PARTY, page.ok, { url });
    if (!page.ok) continue;
    const parsed = parseOfficialMeetingsHtml(page.text, { url: page.url });
    if (parsed.ok) {
      proposedCommercial.officialEventsUrl = page.url;
      if (parsed.commercial.meetingRoomCount != null) {
        proposedCommercial.meetingRoomCount = parsed.commercial.meetingRoomCount;
      }
      if (parsed.commercial.totalMeetingSpaceSqFt != null) {
        proposedCommercial.totalMeetingSpaceSqFt =
          parsed.commercial.totalMeetingSpaceSqFt;
      }
      if (parsed.commercial.largestEventCapacity != null) {
        proposedCommercial.largestEventCapacity =
          parsed.commercial.largestEventCapacity;
      }
      populated =
        proposedCommercial.totalMeetingSpaceSqFt != null ||
        proposedCommercial.meetingRoomCount != null;
      evidence.push(
        stageEvidence(id.hpcHotelId, "Commercial Profile", "Official Events URL", page.url, {
          name: "Official meetings/events page",
          url: page.url,
          confidence: "HIGH",
          strength: "STRONG",
          snippet: page.text.slice(0, 200),
        })
      );
    }
  }

  // If already strongly populated from first-party with named inventory, still allow secondary strengthen
  const needSecondary =
    !populated ||
    (populated &&
      (proposedCommercial.totalMeetingSpaceSqFt == null ||
        (profile.eventSpaces || []).length === 0)) ||
    opts.forceSecondary === true;

  // Jev / deterministic next action for secondary structured sources
  let jev = null;
  if (needSecondary && budget.jev > 0) {
    budget.jev -= 1;
    ledger.jevCalls += 1;
    jev = decideEventSpaceNextAction({
      hotelName,
      city,
      sourcesAttempted: ledger.sourcesAttempted,
      populated,
      conflicts,
    });
  }

  const runStructuredSearch = async (query) => {
    const serp = await serpVenueSearch(query, budget);
    ledger.queries += 1;
    if (!serp.ok && serp.error === "serpapi_key_missing") {
      providerFailed = true;
    }
    return serp;
  };

  // Step 5: structured venue profile search (Cvent etc.) — GENERIC
  if (
    needSecondary &&
    (jev?.action === HI_JEV_ACTIONS.SEARCH_STRUCTURED_VENUE_PROFILE ||
      jev?.action === HI_JEV_ACTIONS.FIND_CVENT_PROFILE ||
      !jev)
  ) {
    const primaryQ =
      jev?.queryHint ||
      `"${hotelName}" site:cvent.com/venues ${city}`;
    const fallbackQ =
      jev?.fallbackQueryHint ||
      `"${hotelName}" ("meeting space" OR "meeting rooms" OR "salas de reuniones" OR "venue profile") ${city}`;

    const collectHits = async (query) => {
      const serp = await runStructuredSearch(query);
      return serp.hits || [];
    };

    let allHits = await collectHits(primaryQ);
    const hasCventHit = allHits.some((h) => /cvent\.com\/venues\//i.test(h.url || ""));
    if (!hasCventHit && budget.queries > 0) {
      const more = await collectHits(fallbackQ);
      allHits = [...allHits, ...more];
    }

    const cventHits = allHits.filter((h) => /cvent\.com\/venues\//i.test(h.url || ""));
    const structuredOther = allHits.filter((h) => {
      if (!h.url || /cvent\.com\/venues\//i.test(h.url)) return false;
      const fam = classifySourceFamily(h.url, h.title);
      return (
        fam === SOURCE_FAMILY.CVB ||
        fam === SOURCE_FAMILY.VENUE_DIRECTORY ||
        fam === SOURCE_FAMILY.OFFICIAL_PDF ||
        fam === SOURCE_FAMILY.TOURISM_AUTHORITY ||
        fam === SOURCE_FAMILY.FIRST_PARTY
      );
    });
    // Prefer structured families; do not burn budget on OTA/weak hits
    const orderedHits = [...cventHits, ...structuredOther];

    for (const hit of orderedHits.slice(0, 4)) {
      if (budget.fetches <= 0) break;
      budget.fetches -= 1;
      ledger.fetches += 1;
      const page = await fetchText(hit.url);
      const family = classifySourceFamily(hit.url, hit.title);
      sourcesChecked.push({ role: "structured_venue", ...page, family, title: hit.title });
      trackSource(family, page.ok, { url: hit.url, title: hit.title });
      if (!page.ok) continue;

      let parsed = null;
      if (family === SOURCE_FAMILY.CVENT) {
        parsed = parseCventVenueHtml(page.text, { url: page.url, hotelName });
        if (!parsed.ok) parserFailed = true;
      } else {
        parsed = parseOfficialMeetingsHtml(page.text, { url: page.url });
      }
      if (!parsed?.ok) continue;

      if (jev) ledger.jevMaterial += 1;
      const preferred = preferStructuredOverNarrative(parsed);
      conflicts = [...conflicts, ...(preferred.conflicts || [])];

      const c = preferred.commercial;
      if (c.roomsKeys != null && proposedCommercial.roomsKeys == null) {
        proposedCommercial.roomsKeys = c.roomsKeys;
      }
      if (c.totalMeetingSpaceSqFt != null) {
        proposedCommercial.totalMeetingSpaceSqFt = c.totalMeetingSpaceSqFt;
      }
      if (c.meetingRoomCount != null) {
        proposedCommercial.meetingRoomCount = c.meetingRoomCount;
      }
      if (c.largestMeetingSpaceSqFt != null) {
        proposedCommercial.largestMeetingSpaceSqFt = c.largestMeetingSpaceSqFt;
      }
      if (c.largestEventCapacity != null) {
        proposedCommercial.largestEventCapacity = c.largestEventCapacity;
      }
      proposedCommercial.confidence = "HIGH";
      proposedCommercial.notes = [
        proposedCommercial.notes,
        `Structured venue source: ${family}`,
      ]
        .filter(Boolean)
        .join(" | ");

      for (const space of preferred.eventSpaces || []) {
        proposedSpaces.push({
          spaceKey: eventSpaceDedupeKey(id.hpcHotelId, space.spaceName),
          hpcHotelId: id.hpcHotelId,
          spaceName: space.spaceName,
          spaceType: space.spaceType || "Meeting Room",
          sqFt: space.sqFt ?? null,
          theaterCapacity: space.theaterCapacity ?? null,
          sourceUrl: space.sourceUrl || page.url,
          confidence: space.confidence || "HIGH",
          active: true,
          notes: space.notes || null,
          schemaVersion: HOTEL_INTELLIGENCE_SCHEMA_VERSION,
        });
      }

      evidence.push(
        stageEvidence(
          id.hpcHotelId,
          "Event Space",
          "Total Meeting Space Sq Ft",
          c.totalMeetingSpaceSqFt,
          {
            name: family === SOURCE_FAMILY.CVENT ? "Cvent venue profile" : "Structured venue source",
            url: page.url,
            confidence: "HIGH",
            strength: "STRONG",
            snippet: (hit.snippet || "").slice(0, 200),
            notes: conflicts.length ? `conflicts_retained=${conflicts.length}` : null,
          }
        )
      );
      if (c.meetingRoomCount != null) {
        evidence.push(
          stageEvidence(
            id.hpcHotelId,
            "Event Space",
            "Meeting Room Count",
            c.meetingRoomCount,
            {
              name: "Structured venue profile",
              url: page.url,
              confidence: "HIGH",
              strength: "STRONG",
            }
          )
        );
      }
      for (const conf of conflicts) {
        evidence.push(
          stageEvidence(
            id.hpcHotelId,
            "Event Space",
            `CONFLICT:${conf.field}`,
            JSON.stringify({
              structured: conf.structuredValue,
              narrative: conf.narrativeValue,
              preferred: conf.authorityPreferred,
            }),
            {
              name: "Source conflict retained",
              url: page.url,
              confidence: "MEDIUM",
              strength: "MODERATE",
              notes: conf.note,
            }
          )
        );
      }

      ledger.sourceFamilyYield[family] = ledger.sourceFamilyYield[family] || {
        attempts: 0,
        successes: 0,
        facts: 0,
      };
      ledger.sourceFamilyYield[family].facts += preferred.eventSpaces?.length || 1;
      populated = true;
      break;
    }
  }

  // Optional CVB pass if still empty and budget remains
  if (!populated && budget.queries > 0 && budget.fetches > 0) {
    const jev2 = decideEventSpaceNextAction({
      hotelName,
      city,
      sourcesAttempted: ledger.sourcesAttempted,
      populated,
    });
    if (jev2.action === HI_JEV_ACTIONS.FIND_CVB_VENUE_PROFILE && budget.jev >= 0) {
      ledger.jevCalls += 1;
      const serp = await runStructuredSearch(jev2.queryHint);
      for (const hit of (serp.hits || []).slice(0, 2)) {
        if (budget.fetches <= 0) break;
        budget.fetches -= 1;
        ledger.fetches += 1;
        const page = await fetchText(hit.url);
        const family = classifySourceFamily(hit.url, hit.title);
        trackSource(family, page.ok, { url: hit.url });
        sourcesChecked.push({ role: "cvb_or_destination", ...page, family });
        if (!page.ok) continue;
        const parsed = parseOfficialMeetingsHtml(page.text, { url: page.url });
        if (parsed.ok) {
          ledger.jevMaterial += 1;
          if (parsed.commercial.totalMeetingSpaceSqFt != null) {
            proposedCommercial.totalMeetingSpaceSqFt =
              parsed.commercial.totalMeetingSpaceSqFt;
            populated = true;
          }
        }
      }
    }
  }

  if (
    populated &&
    proposedSpaces.length === 0 &&
    (proposedCommercial.totalMeetingSpaceSqFt != null ||
      proposedCommercial.meetingRoomCount != null)
  ) {
    proposedSpaces.push({
      spaceKey: eventSpaceDedupeKey(
        id.hpcHotelId,
        "Aggregate meeting inventory (structured venue profile)"
      ),
      hpcHotelId: id.hpcHotelId,
      spaceName: "Aggregate meeting inventory (structured venue profile)",
      spaceType: "Meeting Room",
      sqFt: proposedCommercial.totalMeetingSpaceSqFt,
      theaterCapacity: proposedCommercial.largestEventCapacity,
      sourceUrl: proposedCommercial.officialEventsUrl || sourcesChecked.find((s) => s.ok)?.url,
      confidence: "HIGH",
      active: true,
      schemaVersion: HOTEL_INTELLIGENCE_SCHEMA_VERSION,
    });
  }

  proposedCommercial.lastResearchedAt = new Date().toISOString();
  const ladderExhausted =
    mayDeclareResearchedEmpty(ledger.sourcesAttempted) ||
    (ledger.sourcesAttempted.length >= 2 && budget.fetches <= 0 && budget.queries <= 0);

  const researchDepth = resolveResearchDepth({
    populated,
    sourcesAttempted: ledger.sourcesAttempted,
    conflictUnresolved: conflicts.length > 0 && populated,
    providerFailed,
    parserFailed: parserFailed && !populated,
    ladderExhausted: ladderExhausted && !populated,
  });

  let domainStatus = researchDepthToDomainStatus(researchDepth);
  if (!populated && ladderExhausted && mayDeclareResearchedEmpty(ledger.sourcesAttempted)) {
    domainStatus = HI_DOMAIN_STATUS.PUBLIC_DATA_CEILING;
  }
  if (!populated && !ladderExhausted && researchDepth === RESEARCH_DEPTH.FIRST_PARTY_RESEARCHED) {
    domainStatus = HI_DOMAIN_STATUS.NOT_RESEARCHED; // must continue — not empty yet
  }
  if (populated) domainStatus = HI_DOMAIN_STATUS.POPULATED;

  const packet = {
    commercial: proposedCommercial,
    eventSpaces: proposedSpaces,
    demandNodes: [],
    seasonality: [],
    evidence,
  };

  let applyResult = null;
  let adpSync = null;
  if (mode === "apply" && populated) {
    applyResult = await applyHotelIntelligencePacket(packet, { dryRun: false });
    const attrs = await buildAdpHotelAttributes(id.hpcHotelId);
    adpSync = await syncHotelAdpAttributesToAirtable(attrs, { dryRun: false });
  }

  if (persistLedger) {
    upsertDomainStatus(id.hpcHotelId, HI_DOMAIN.EVENT_SPACES, {
      domainStatus,
      researchDepth,
      lastResearchedAt: new Date().toISOString(),
      researchRunId: opts.researchRunId || `hi_depth_v2_${Date.now()}`,
      evidenceCount: evidence.length,
      rowCount: proposedSpaces.length,
      sourceCoverage: ledger.sourcesAttempted.map((s) => s.family),
      sourcesAttempted: ledger.sourcesAttempted,
      notes: populated
        ? `evidence_depth_v2_populated depth=${researchDepth}`
        : `evidence_depth_v2_empty depth=${researchDepth}`,
      blocker: populated ? null : domainStatus,
      conflicts,
    });
  }

  return {
    ok: true,
    mode,
    hotelId: id.hpcHotelId,
    hotelName,
    adpPropertyId: id.adpPropertyId,
    populated,
    domainStatus,
    researchDepth,
    conflicts,
    jev,
    ledger: {
      ...ledger,
      queriesUsed: ledger.queries,
      fetchesUsed: ledger.fetches,
    },
    discovered: {
      commercial: proposedCommercial,
      eventSpaces: proposedSpaces,
    },
    evidence,
    sourcesChecked: sourcesChecked.map((s) => ({
      role: s.role,
      family: s.family,
      url: s.url,
      ok: s.ok,
      status: s.status,
      title: s.title || null,
    })),
    applyResult: applyResult
      ? { ok: applyResult.ok, eventSpaces: applyResult.eventSpaces?.length }
      : null,
    adpSync: adpSync
      ? {
          createCount: adpSync.createCount,
          updateCount: adpSync.updateCount,
        }
      : null,
    ladder: EVENT_SPACE_SOURCE_LADDER,
  };
}
