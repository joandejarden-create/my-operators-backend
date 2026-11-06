/**
 * Event-to-Demand discovery stage — reusable across hotels.
 * Public/fixture-first; optional Parallel only when authorized.
 * Feeds candidates into Hotel Research Orchestrator queue.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  EVENT_TO_DEMAND_VERSION,
  EVENT_SOURCE_FAMILIES,
  EVENT_DEMAND_DOMAIN,
  EVENT_DEMAND_MECHANISM,
  primarySegmentForDomain,
} from "./event-to-demand-constants-v1.js";
import {
  normalizeVerifiedEvent,
  dedupeVerifiedEvents,
  isFutureEvent,
  mergeVerifiedEvents,
} from "./event-identity-v1.js";
import { extractOpportunitiesFromEventCorpus } from "./event-to-opportunity-extract-v1.js";
import { canonicalizeSegmentId } from "../research-constants-v1.js";
import { runLiveEventCalendarDiscovery } from "./live-event-calendar-discovery-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../../../..");

/**
 * Load market event seed pack (hotel-agnostic city pack + optional hotel overlays).
 * Packs are verified manually; discovery never invents events.
 */
export function loadEventSeedPack({ marketKey, hotelId, root = ROOT } = {}) {
  const candidates = [
    path.join(
      root,
      "fixtures/group-demand-intelligence/event-to-demand",
      `${marketKey || "unknown"}-events-v1.json`
    ),
    hotelId
      ? path.join(
          root,
          "fixtures/group-demand-intelligence/event-to-demand",
          `hotel-${hotelId}-events-v1.json`
        )
      : null,
  ].filter(Boolean);

  const events = [];
  const loaded = [];
  for (const p of candidates) {
    if (!fs.existsSync(p)) continue;
    try {
      const j = JSON.parse(fs.readFileSync(p, "utf8"));
      const rows = Array.isArray(j) ? j : j.events || [];
      events.push(...rows);
      loaded.push(p);
    } catch (err) {
      loaded.push({ path: p, error: err?.message || String(err) });
    }
  }
  return { events: dedupeVerifiedEvents(events.map(normalizeVerifiedEvent)), loaded };
}

/**
 * Filter events relevant to shallow segments / domains for this run.
 */
export function filterEventsForSegments(events = [], shallowSegments = []) {
  if (!shallowSegments?.length) return events;
  const want = new Set(shallowSegments.map(canonicalizeSegmentId));
  return events.filter((e) => {
    const seg = canonicalizeSegmentId(
      e.segment || primarySegmentForDomain(e.domain)
    );
    if (want.has(seg)) return true;
    // Destination-wide signals can feed multiple segments
    if (e.domain === EVENT_DEMAND_DOMAIN.DESTINATION_CITYWIDE) return true;
    const secondaries = e.secondarySegments || [];
    return secondaries.some((s) => want.has(canonicalizeSegmentId(s)));
  });
}

/**
 * Assess discovery source-family breadth for coverage evidence.
 */
export function assessEventDiscoveryBreadth(events = []) {
  const families = new Set();
  for (const e of events) {
    for (const f of e.sourceFamilies || []) families.add(f);
    for (const s of e.sources || []) {
      if (s.sourceFamily) families.add(s.sourceFamily);
    }
  }
  return {
    eventCount: events.length,
    sourceFamilyCount: families.size,
    sourceFamilies: [...families],
    expectedFamilies: [...EVENT_SOURCE_FAMILIES],
    sufficientBreadth: families.size >= 3 && events.length >= 5,
  };
}

function isWeakLiveTitle(name = "") {
  const n = String(name || "")
    .replace(/\s+/g, " ")
    .trim()
    .toLowerCase();
  if (!n || n.length < 8) return true;
  if (/^(olympia|excel|events?|whats on|calendar|home)$/i.test(n)) return true;
  return false;
}

/**
 * For live-discovered events lacking curated stakeholders, attach a single
 * evidence-proportionate mechanism proposal when organizer + domain support it.
 * Never invents attendance, room blocks, or buyers.
 */
export function attachLiveMechanismProposals(event = {}, hotel = {}) {
  if (!event?.liveDiscovered) return event;
  if (event.organizerDemandRationale || (event.stakeholders || []).length) {
    return event;
  }
  if (isWeakLiveTitle(event.eventName)) return event;

  const organizerRaw = event.organizer;
  let organizer =
    organizerRaw && typeof organizerRaw === "object"
      ? String(organizerRaw.name || organizerRaw.title || "").trim()
      : String(organizerRaw || "").trim();
  if (!organizer || organizer === "[object Object]") {
    // Distinctive event brand may stand as provisional org for outreach
    // (trade-show / congress titles), never weak calendar labels.
    organizer = String(event.eventName || "").trim();
  }
  if (!organizer || isWeakLiveTitle(organizer)) return event;

  const orgName = organizer;
  const city = event.city || hotel.city || "the hotel market";
  const domain = event.domain || "";
  const venue = String(event.venue || "").toLowerCase();
  const stretchVenue = /excel|olympia|heathrow|gatwick/.test(venue);
  let mechanism = EVENT_DEMAND_MECHANISM.OFFSITE_MEETING;
  let rationale = null;

  if (/medical|scientific|academic/i.test(domain)) {
    mechanism = EVENT_DEMAND_MECHANISM.SMALL_GROUP_LODGING;
    rationale = `Verified future ${event.eventName} (${event.eventStartDate}) with organizer ${orgName}; travelling faculty/delegates are a proportionate small-group lodging prospect for ${city} without claiming a confirmed room block.`;
  } else if (/exhibition|trade/i.test(domain)) {
    // At stretch convention venues, mass exhibitor lodging is usually unsuitable
    // for luxury boutiques — prefer executive-delegation prospecting.
    mechanism = stretchVenue
      ? EVENT_DEMAND_MECHANISM.EXECUTIVE_DELEGATION
      : EVENT_DEMAND_MECHANISM.EXHIBITOR_SPONSOR_LODGING;
    rationale = stretchVenue
      ? `Verified future exhibition ${event.eventName} at ${event.venue}; organizer/brand ${orgName} supports executive-delegation / spillover lodging prospecting for a luxury ${city} hotel without claiming mass exhibitor room blocks.`
      : `Verified future exhibition ${event.eventName}; organizer ${orgName} is identified for exhibitor/sponsor lodging prospecting subject to hotel-fit and open-demand evidence.`;
  } else if (/entertainment|luxury|cultural/i.test(domain)) {
    mechanism = EVENT_DEMAND_MECHANISM.VIP_HOSPITALITY;
    rationale = `Verified future cultural/luxury event ${event.eventName}; organizer ${orgName} supports VIP/hospitality prospecting near ${city} without inventing contracted lodging.`;
  } else if (/corporate|meeting|intermediary/i.test(domain)) {
    mechanism = EVENT_DEMAND_MECHANISM.EXECUTIVE_DELEGATION;
    rationale = `Verified future corporate/professional programme ${event.eventName}; organizer ${orgName} supports executive-delegation prospecting for ${city}.`;
  } else {
    rationale = `Verified future event ${event.eventName} with organizer ${orgName}; off-site meeting / compact lodging prospecting is proportionate pending further evidence.`;
  }

  return {
    ...event,
    organizer: orgName,
    organizerDemandRationale: rationale,
    proposedOrganizerMechanism: mechanism,
    liveMechanismAttached: true,
  };
}

/**
 * Resolve market key from hotel config (data-driven; not London-only).
 */
export function resolveEventMarketKey(hotel = {}, marketKey = null) {
  if (marketKey) return String(marketKey).toLowerCase();
  const city = hotel.city || hotel.demandTerritory?.label || "";
  if (/london/i.test(city)) return "london";
  return (
    String(city || "generic")
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, "-")
      .replace(/^-|-$/g, "") || "generic"
  );
}

/**
 * Run Event-to-Demand discovery for a hotel.
 *
 * discoveryMode:
 * - seed (default): curated seed pack only
 * - live: public calendar fetch only (held-out from seed / benchmark)
 * - seed_and_live: merge live + seed
 *
 * @returns {Promise<object>}
 */
export async function runEventToDemandDiscovery({
  hotelId,
  hotel = {},
  coverage = null,
  existingOpportunities = [],
  remainingBudgetUsd = 0,
  authorizeLivePaid = false,
  marketKey = null,
  eventSeed = null,
  outDir = null,
  maxNewCandidates = 15,
  discoveryMode = "seed",
  horizonMonths = 18,
  fetchPage = null,
  asOf = new Date(),
  maxLiveSources = 12,
  maxDetailFetches = 24,
} = {}) {
  const shallowSegments = (coverage?.rows || [])
    .filter((r) =>
      ["INSUFFICIENT_RESEARCH_DEPTH", "PENDING", "IN_PROGRESS"].includes(r.status)
    )
    .map((r) => canonicalizeSegmentId(r.segment))
    .filter(Boolean);

  const resolvedMarket = resolveEventMarketKey(hotel, marketKey);
  const mode = String(discoveryMode || "seed").toLowerCase();

  let events = [];
  let seed = { events: [], loaded: [] };
  let live = null;

  if (mode === "seed" || mode === "seed_and_live") {
    seed =
      eventSeed ||
      loadEventSeedPack({
        marketKey: resolvedMarket,
        hotelId,
      });
    events = [...(seed.events || [])];
  }

  if (mode === "live" || mode === "seed_and_live") {
    live = await runLiveEventCalendarDiscovery({
      marketKey: resolvedMarket,
      hotel: { ...hotel, hotelId },
      horizonMonths,
      asOf,
      includeSeedPack: false,
      maxSources: maxLiveSources,
      maxDetailFetches,
      outDir: outDir ? path.join(outDir, "live-calendar") : null,
      ...(fetchPage ? { fetchPage } : {}),
    });
    if (mode === "live") {
      events = live.events || [];
    } else {
      const byKey = new Map();
      for (const e of events) {
        const n = normalizeVerifiedEvent(e);
        byKey.set(n.eventIdentityKey, n);
      }
      for (const e of live.events || []) {
        const n = normalizeVerifiedEvent(e);
        if (byKey.has(n.eventIdentityKey)) {
          byKey.set(n.eventIdentityKey, mergeVerifiedEvents(byKey.get(n.eventIdentityKey), n));
        } else {
          byKey.set(n.eventIdentityKey, n);
        }
      }
      events = [...byKey.values()];
    }
  }

  events = filterEventsForSegments(events, shallowSegments);
  events = events
    .filter((e) => e.verified === true && isFutureEvent(e, asOf))
    .map((e) => attachLiveMechanismProposals(e, hotel));

  const breadth = assessEventDiscoveryBreadth(events);

  const extracted = extractOpportunitiesFromEventCorpus({
    events,
    hotel: { ...hotel, hotelId },
    existingOpportunities,
  });

  const candidates = extracted.candidates.slice(0, maxNewCandidates);

  const result = {
    version: EVENT_TO_DEMAND_VERSION,
    hotelId,
    marketKey: resolvedMarket,
    discoveryMode: mode,
    mode:
      mode === "live"
        ? "live_calendar"
        : mode === "seed_and_live"
          ? "seed_and_live_calendar"
          : authorizeLivePaid && remainingBudgetUsd > 0.05
            ? "seed_plus_live_eligible"
            : "seed_public",
    authorizeLivePaid: authorizeLivePaid === true,
    remainingBudgetUsd,
    shallowSegments,
    seedLoaded: seed.loaded,
    liveDiscovery: live
      ? {
          metrics: live.metrics,
          sourcesAttempted: live.sourcesAttempted,
          sourceErrors: (live.sourceErrors || []).length,
          horizonEnd: live.horizonEnd,
        }
      : null,
    eventsConsidered: events.length,
    events: events.map((e) => ({
      eventId: e.eventId,
      eventName: e.eventName,
      eventStartDate: e.eventStartDate,
      liveDiscovered: e.liveDiscovered === true,
      sourceFamilies: e.sourceFamilies || [],
    })),
    breadth,
    metrics: extracted.metrics,
    candidates,
    signals: extracted.signals,
    rejected: extracted.rejected,
    note:
      candidates.length === 0
        ? "honest_zero_yield_or_no_fit"
        : `event_derived_candidates_${candidates.length}`,
    livePaidUsedUsd: 0,
  };

  if (outDir) {
    fs.mkdirSync(outDir, { recursive: true });
    fs.writeFileSync(
      path.join(outDir, "event-to-demand-result.json"),
      JSON.stringify(result, null, 2) + "\n"
    );
  }

  return result;
}

/**
 * Discovery-recall audit: compare benchmark events to existing corpus.
 */
export function auditEventDiscoveryRecall({
  benchmarkEvents = [],
  existingOpportunities = [],
  hotel = {},
} = {}) {
  const corpusText = existingOpportunities
    .map((o) =>
      [
        o.organizationName,
        o.title,
        o.eventName,
        o.localConnection,
        o.summaryWhat,
        ...(o.sources || []).map((s) => s.url || s),
      ]
        .filter(Boolean)
        .join(" ")
        .toLowerCase()
    )
    .join("\n");

  const rows = [];
  for (const raw of benchmarkEvents) {
    const event = normalizeVerifiedEvent(raw);
    const nameKey = (event.eventName || "").toLowerCase();
    const orgKey = (event.organizer || "").toLowerCase();
    const eventPresent =
      Boolean(nameKey) &&
      (corpusText.includes(nameKey) ||
        (event.officialUrl &&
          corpusText.includes(String(event.officialUrl).toLowerCase())));
    const organizerPresent =
      Boolean(orgKey) && corpusText.includes(orgKey);
    const matchingOpps = existingOpportunities.filter((o) => {
      const blob = `${o.organizationName || ""} ${o.title || ""} ${o.eventName || ""}`.toLowerCase();
      return (
        (nameKey && blob.includes(nameKey)) ||
        (orgKey && blob.includes(orgKey)) ||
        (o.eventId && o.eventId === event.eventId)
      );
    });
    const mechanisms = [
      ...new Set(
        matchingOpps
          .map((o) => o.eventDemandMechanism || o.demandMechanism)
          .filter(Boolean)
      ),
    ];
    const rejectedLegitimate = matchingOpps.some(
      (o) =>
        o.stopReason === "BELOW_COMMERCIAL_RELEVANCE" ||
        o.stopReason === "HOTEL_CATCHMENT_MISMATCH" ||
        o.eventHotelFitClass === "SPECULATIVE_MARKET_SIGNAL"
    );
    let status = "MISSED";
    if (matchingOpps.length && mechanisms.length) status = "RECALLED_WITH_MECHANISM";
    else if (matchingOpps.length) status = "RECALLED_AS_CANDIDATE";
    else if (eventPresent || organizerPresent) status = "PARTIAL_MENTION";
    else if (rejectedLegitimate) status = "LEGITIMATE_FIT_REJECT";

    rows.push({
      eventId: event.eventId,
      eventName: event.eventName,
      domain: event.domain,
      organizer: event.organizer || null,
      eventPresent,
      organizerPresent,
      candidateCount: matchingOpps.length,
      mechanisms,
      status,
      sources: (event.sources || []).map((s) => s.url).filter(Boolean),
    });
  }

  const totals = {
    benchmark: rows.length,
    missed: rows.filter((r) => r.status === "MISSED").length,
    partial: rows.filter((r) => r.status === "PARTIAL_MENTION").length,
    recalledCandidate: rows.filter((r) => r.status === "RECALLED_AS_CANDIDATE").length,
    recalledWithMechanism: rows.filter((r) => r.status === "RECALLED_WITH_MECHANISM").length,
    legitimateReject: rows.filter((r) => r.status === "LEGITIMATE_FIT_REJECT").length,
  };
  const recallRate =
    totals.benchmark === 0
      ? 0
      : (totals.recalledCandidate + totals.recalledWithMechanism + totals.partial) /
        totals.benchmark;

  return {
    version: EVENT_TO_DEMAND_VERSION,
    hotelId: hotel.hotelId || null,
    totals,
    recallRate,
    rows,
  };
}
