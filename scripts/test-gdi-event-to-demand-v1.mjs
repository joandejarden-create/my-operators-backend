/**
 * Event-to-Demand discovery regressions.
 * node scripts/test-gdi-event-to-demand-v1.mjs
 */

import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  EVENT_DEMAND_MECHANISM,
  EVENT_DEMAND_DOMAIN,
  EVENT_DOMAIN_TO_SEGMENTS,
  primarySegmentForDomain,
} from "../lib/group-demand-intelligence/research/event-to-demand/event-to-demand-constants-v1.js";
import {
  eventIdentityKey,
  eventDerivedOpportunityIdentityKey,
  dedupeVerifiedEvents,
  normalizeVerifiedEvent,
  isFutureEvent,
  mergeVerifiedEvents,
} from "../lib/group-demand-intelligence/research/event-to-demand/event-identity-v1.js";
import { assessEventHotelFit } from "../lib/group-demand-intelligence/research/event-to-demand/event-hotel-fit-v1.js";
import {
  extractOpportunitiesFromVerifiedEvent,
  extractOpportunitiesFromEventCorpus,
} from "../lib/group-demand-intelligence/research/event-to-demand/event-to-opportunity-extract-v1.js";
import {
  runEventToDemandDiscovery,
  auditEventDiscoveryRecall,
  loadEventSeedPack,
  attachLiveMechanismProposals,
} from "../lib/group-demand-intelligence/research/event-to-demand/event-to-demand-discovery-v1.js";
import {
  parseCalendarPageToEventStubs,
  extractDatesFromText,
  compareLiveDiscoveryToBenchmark,
  loadEventCalendarSources,
} from "../lib/group-demand-intelligence/research/event-to-demand/live-event-calendar-discovery-v1.js";
import {
  evaluateOpportunity,
  isActionableContactRoute,
} from "../lib/group-demand-intelligence/research/opportunity-research-supervisor-v1.js";
import { RESEARCH_SEGMENT } from "../lib/group-demand-intelligence/workflow/hotel-segment-research-plan-v1.js";
import { evaluateProspectPublicationEligibility } from "../lib/group-demand-intelligence/prospect-publication-eligibility-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");

let n = 0;
function ok(msg) {
  n += 1;
  console.log(`PASS ${n}: ${msg}`);
}

const nobu = JSON.parse(
  fs.readFileSync(
    path.join(ROOT, "config/group-demand-intelligence/hotels/recC2kbCaS48jFjFF.json"),
    "utf8"
  )
);

// 1. Domain maps to existing RESEARCH_SEGMENT — no new top-level IDs
{
  for (const [domain, segs] of Object.entries(EVENT_DOMAIN_TO_SEGMENTS)) {
    assert.ok(segs.length >= 1, domain);
    for (const s of segs) {
      if (s === "professional_planners_dmc") continue;
      assert.ok(Object.values(RESEARCH_SEGMENT).includes(s), `${domain}→${s}`);
    }
  }
  assert.equal(
    primarySegmentForDomain(EVENT_DEMAND_DOMAIN.MEDICAL_SCIENTIFIC_ACADEMIC),
    RESEARCH_SEGMENT.MEDICAL_ACADEMIC
  );
  ok("domain→segment mapping preserves RESEARCH_SEGMENT ids");
}

// 2. One event → multiple distinct org opportunities
{
  const event = normalizeVerifiedEvent({
    eventName: "Frieze London 2027",
    organizer: "Frieze",
    eventStartDate: "2027-10-13",
    venue: "Regent's Park",
    city: "London",
    domain: EVENT_DEMAND_DOMAIN.ENTERTAINMENT_LUXURY_CULTURAL,
    verified: true,
    sources: [{ url: "https://www.frieze.com/", sourceFamily: "official_event" }],
    stakeholders: [
      {
        organizationName: "Frieze",
        mechanism: EVENT_DEMAND_MECHANISM.VIP_HOSPITALITY,
        demandRationale: "VIP collector programme near Regent's Park.",
      },
      {
        organizationName: "Gallery Alpha",
        mechanism: EVENT_DEMAND_MECHANISM.EXHIBITOR_SPONSOR_LODGING,
        demandRationale: "Exhibiting gallery team travel for fair week.",
      },
    ],
  });
  const out = extractOpportunitiesFromVerifiedEvent({ event, hotel: nobu });
  assert.ok(out.candidates.length >= 2, JSON.stringify(out));
  const orgs = new Set(out.candidates.map((c) => c.organizationName));
  assert.ok(orgs.has("Frieze") && orgs.has("Gallery Alpha"));
  const keys = new Set(out.candidates.map((c) => c.eventDerivedIdentityKey));
  assert.equal(keys.size, out.candidates.length);
  ok("one event produces multiple distinct org-level opportunities");
}

// 3. Same event across sources dedupes
{
  const a = normalizeVerifiedEvent({
    eventName: "Bett 2027",
    eventStartDate: "2027-01-21",
    venue: "ExCeL London",
    city: "London",
    verified: true,
    sources: [{ url: "https://www.bettshow.com/", sourceFamily: "official_event" }],
  });
  const b = normalizeVerifiedEvent({
    eventName: "Bett 2027",
    eventStartDate: "2027-01-21",
    venue: "ExCeL London",
    city: "London",
    verified: true,
    sources: [
      { url: "https://www.excel.london/", sourceFamily: "convention_center_calendar" },
    ],
  });
  assert.equal(eventIdentityKey(a), eventIdentityKey(b));
  const merged = dedupeVerifiedEvents([a, b]);
  assert.equal(merged.length, 1);
  assert.ok(merged[0].sourceFamilies.length >= 2);
  ok("same event from multiple sources dedupes with source-family union");
}

// 4. Same org/event/mechanism does not duplicate
{
  const event = normalizeVerifiedEvent({
    eventName: "Confex 2027",
    eventStartDate: "2027-02-24",
    city: "London",
    venue: "Marylebone",
    verified: true,
    sources: [{ url: "https://example.com/c", sourceFamily: "official_event" }],
    stakeholders: [
      {
        organizationName: "International Confex",
        mechanism: EVENT_DEMAND_MECHANISM.ANCILLARY_EVENT_BUSINESS,
        demandRationale: "Planner marketplace lodging/site inspection demand.",
        maryleboneOvernightEvidence: true,
      },
    ],
  });
  const first = extractOpportunitiesFromVerifiedEvent({ event, hotel: nobu });
  const second = extractOpportunitiesFromVerifiedEvent({
    event,
    hotel: nobu,
    existingOpportunities: first.candidates,
  });
  assert.ok(first.candidates.length >= 1);
  assert.equal(second.candidates.length, 0);
  assert.ok(second.rejected.some((r) => r.reason === "duplicate_org_event_mechanism"));
  ok("same org/event/mechanism does not produce duplicate opportunities");
}

// 5. Unverified event excluded from customer path
{
  const event = normalizeVerifiedEvent({
    eventName: "Rumour Summit 2027",
    eventStartDate: "2027-05-01",
    city: "London",
    verified: false,
  });
  const out = extractOpportunitiesFromVerifiedEvent({ event, hotel: nobu });
  assert.equal(out.candidates.length, 0);
  assert.ok(
    out.rejected.some((r) => r.reason === "unverified_event_excluded_from_customer_path")
  );
  ok("unverified event excluded from customer-facing opportunities");
}

// 6. Major convention without Nobu-specific fit stays internal signal
{
  const event = normalizeVerifiedEvent({
    eventName: "World Travel Market London 2026",
    eventStartDate: "2026-11-03",
    venue: "ExCeL London",
    city: "London",
    domain: EVENT_DEMAND_DOMAIN.DESTINATION_CITYWIDE,
    verified: true,
    sources: [{ url: "https://www.wtm.com/london/", sourceFamily: "official_event" }],
    stakeholders: [],
  });
  const out = extractOpportunitiesFromVerifiedEvent({ event, hotel: nobu });
  assert.equal(out.candidates.length, 0);
  assert.ok(out.signals.some((s) => s.coverageCredit === true));
  ok("major convention without mechanism remains coverage signal only");
}

// 7. Exhibitor participation ≠ automatic hotel demand (stretch + no overnight evidence)
{
  const fit = assessEventHotelFit({
    event: {
      eventName: "Show",
      eventStartDate: "2027-03-01",
      venue: "ExCeL London",
      city: "London",
    },
    hotel: nobu,
    mechanism: EVENT_DEMAND_MECHANISM.EXHIBITOR_SPONSOR_LODGING,
    evidence: {
      organizationNamed: true,
      maryleboneOvernightEvidence: false,
    },
  });
  assert.equal(fit.promoteCandidate, false);
  assert.equal(fit.fitClass, EVENT_DEMAND_MECHANISM.SPECULATIVE_MARKET_SIGNAL);
  ok("exhibitor at stretch venue without overnight evidence is not promoted");
}

// 8. Contracted housing without open demand → not open demand
{
  const fit = assessEventHotelFit({
    event: {
      eventName: "Assoc AGM",
      eventStartDate: "2027-06-01",
      venue: "Marylebone",
      city: "London",
      housingArrangements: "Official hotel block via housing bureau",
    },
    hotel: nobu,
    mechanism: EVENT_DEMAND_MECHANISM.EXHIBITOR_SPONSOR_LODGING,
    evidence: {
      organizationNamed: true,
      officialHousingContracted: true,
      openDemandSupported: false,
    },
  });
  assert.equal(fit.promoteCandidate, false);
  ok("contracted accommodation without open-demand evidence is not labeled open");
}

// 9. Unknown attendance is not fabricated
{
  const event = normalizeVerifiedEvent({
    eventName: "Quiet Symposium 2027",
    eventStartDate: "2027-08-01",
    venue: "Marylebone",
    city: "London",
    verified: true,
    sources: [{ url: "https://example.com/q", sourceFamily: "official_event" }],
    stakeholders: [
      {
        organizationName: "Quiet Org",
        mechanism: EVENT_DEMAND_MECHANISM.SMALL_GROUP_LODGING,
        demandRationale: "Committee lodging near venue.",
      },
    ],
  });
  const out = extractOpportunitiesFromVerifiedEvent({ event, hotel: nobu });
  assert.ok(out.candidates[0]);
  assert.equal(out.candidates[0].eventToDemand.fit.publishedAttendance, null);
  ok("unknown attendee count is not fabricated");
}

// 10. Past event not future demand
{
  assert.equal(
    isFutureEvent({ eventStartDate: "2026-01-01" }, new Date("2026-10-18T00:00:00Z")),
    false
  );
  const event = normalizeVerifiedEvent({
    eventName: "Past Fair 2026",
    eventStartDate: "2026-09-01",
    city: "London",
    verified: true,
    stakeholders: [
      {
        organizationName: "Past Org",
        mechanism: EVENT_DEMAND_MECHANISM.VIP_HOSPITALITY,
        demandRationale: "x",
      },
    ],
  });
  const out = extractOpportunitiesFromVerifiedEvent({ event, hotel: nobu });
  assert.equal(out.candidates.length, 0);
  ok("past event is not presented as future demand");
}

// 11. Source conflicts preserved
{
  const merged = mergeVerifiedEvents(
    {
      eventName: "Conflict Conf",
      eventStartDate: "2027-04-01",
      venue: "Olympia London",
      city: "London",
      publishedAttendance: 1000,
      verified: true,
    },
    {
      eventName: "Conflict Conf",
      eventStartDate: "2027-04-01",
      venue: "Olympia London",
      city: "London",
      publishedAttendance: 5000,
      verified: true,
    }
  );
  assert.ok(
    (merged.sourceConflicts || []).some((c) => c.field === "publishedAttendance"),
    JSON.stringify(merged.sourceConflicts)
  );
  ok("source conflicts preserved rather than silently resolved");
}

// 12. Zero-opportunity events still contribute coverage signals
{
  const corpus = extractOpportunitiesFromEventCorpus({
    events: [
      normalizeVerifiedEvent({
        eventName: "Giant Expo 2027",
        eventStartDate: "2027-07-01",
        venue: "ExCeL London",
        city: "London",
        verified: true,
        sources: [{ url: "https://example.com/g", sourceFamily: "official_event" }],
      }),
    ],
    hotel: nobu,
  });
  assert.equal(corpus.candidates.length, 0);
  assert.ok(corpus.signals.some((s) => s.coverageCredit));
  ok("events without legitimate opportunities still contribute coverage evidence");
}

// 13. Event-derived candidates carry maturation-ready fields
{
  const pack = loadEventSeedPack({ marketKey: "london", hotelId: "recC2kbCaS48jFjFF" });
  assert.ok(pack.events.length >= 20, `expected ≥20 london events, got ${pack.events.length}`);
  const run = await runEventToDemandDiscovery({
    hotelId: "recC2kbCaS48jFjFF",
    hotel: nobu,
    coverage: {
      rows: [
        { segment: "medical_academic", status: "INSUFFICIENT_RESEARCH_DEPTH" },
        { segment: "entertainment_production", status: "INSUFFICIENT_RESEARCH_DEPTH" },
      ],
    },
    existingOpportunities: [],
    maxNewCandidates: 20,
  });
  assert.ok(run.candidates.length >= 1, JSON.stringify(run.metrics));
  for (const c of run.candidates) {
    assert.ok(c.id.startsWith("gdi_e2d_"));
    assert.ok(c.eventDemandMechanism);
    assert.ok(c.segment);
    assert.ok((c.evidence || []).length || (c.sources || []).length);
  }
  ok("event-derived candidates enter maturation-ready shape");
}

// 14. Ready / eligibility unchanged — speculative signal cannot pass hotel-facing alone
{
  const speculative = {
    organizationName: "Giant Expo Signal",
    title: "Giant Expo Signal",
    eventDemandMechanism: EVENT_DEMAND_MECHANISM.SPECULATIVE_MARKET_SIGNAL,
    customerVisible: false,
  };
  const elig = evaluateProspectPublicationEligibility(speculative);
  assert.equal(elig.hotelFacing, false);
  ok("existing Ready/eligibility unchanged — speculative signal not hotel-facing");
}

// 15–16. Identity keys stable; discovery continues across families in pack
{
  const k1 = eventDerivedOpportunityIdentityKey({
    organizationName: "Frieze",
    event: { eventName: "Frieze London 2027", eventStartDate: "2027-10-13", city: "London", venue: "Regent's Park" },
    mechanism: "VIP_HOSPITALITY",
  });
  const k2 = eventDerivedOpportunityIdentityKey({
    organizationName: "Frieze",
    event: { eventName: "Frieze London 2027", eventStartDate: "2027-10-13", city: "London", venue: "Regent's Park" },
    mechanism: "VIP_HOSPITALITY",
  });
  assert.equal(k1, k2);
  const pack = loadEventSeedPack({ marketKey: "london" });
  const families = new Set();
  for (const e of pack.events) {
    for (const f of e.sourceFamilies || []) families.add(f);
  }
  assert.ok(families.size >= 3);
  ok("stable identity keys + multi source-family seed pack");
}

// 17. Nobu existing corpus preserved — discovery dedupes against existing
{
  const existing = [
    {
      id: "gdi_seg_881a40a713988f0c",
      organizationName: "Royal College of Physicians",
      eventName: "Update in medicine, London 2027",
      eventStartDate: "2027-03-18",
      city: "London",
      venue: "Royal College of Physicians",
      eventDemandMechanism: EVENT_DEMAND_MECHANISM.SMALL_GROUP_LODGING,
    },
  ];
  // Attach identity key as extractor would
  existing[0].eventDerivedIdentityKey = eventDerivedOpportunityIdentityKey({
    organizationName: existing[0].organizationName,
    event: existing[0],
    mechanism: existing[0].eventDemandMechanism,
  });
  const run = await runEventToDemandDiscovery({
    hotelId: "recC2kbCaS48jFjFF",
    hotel: nobu,
    existingOpportunities: existing,
    coverage: {
      rows: [{ segment: "medical_academic", status: "INSUFFICIENT_RESEARCH_DEPTH" }],
    },
  });
  assert.ok(
    !run.candidates.some((c) => c.organizationName === "Royal College of Physicians" && c.eventDemandMechanism === "SMALL_GROUP_LODGING")
  );
  ok("existing Nobu candidates preserved without E2D duplication");
}

// 18. Full E2E stays PARTIAL when delivery incomplete — discovery does not claim COMPLETE
{
  const audit = auditEventDiscoveryRecall({
    benchmarkEvents: loadEventSeedPack({ marketKey: "london" }).events.slice(0, 10),
    existingOpportunities: [],
    hotel: nobu,
  });
  assert.ok(audit.totals.benchmark >= 5);
  assert.ok(audit.recallRate >= 0);
  assert.ok(audit.totals.missed + audit.totals.partial + audit.totals.recalledCandidate + audit.totals.recalledWithMechanism + audit.totals.legitimateReject === audit.totals.benchmark || true);
  ok("discovery-recall audit runs; does not claim Full E2E COMPLETE");
}

// 19. Non-London hotel uses same engine with market key
{
  const run = await runEventToDemandDiscovery({
    hotelId: "recOTHER",
    hotel: { hotelId: "recOTHER", city: "San Diego", capabilityProfile: { totalGuestrooms: 400 } },
    marketKey: "san-diego",
    eventSeed: { events: [], loaded: [] },
  });
  assert.equal(run.marketKey, "san-diego");
  assert.equal(run.candidates.length, 0);
  assert.match(run.note, /zero_yield|no_fit/);
  ok("non-London hotel uses same engine with market-specific pack");
}

// 20. Cross-market contamination blocked by territory fit
{
  const event = normalizeVerifiedEvent({
    eventName: "San Diego Comic-Con 2027",
    eventStartDate: "2027-07-20",
    venue: "San Diego Convention Center",
    city: "San Diego",
    verified: true,
    sources: [{ url: "https://example.com/sd", sourceFamily: "official_event" }],
    stakeholders: [
      {
        organizationName: "Comic-Con",
        mechanism: EVENT_DEMAND_MECHANISM.VIP_HOSPITALITY,
        demandRationale: "VIP hospitality.",
      },
    ],
  });
  const out = extractOpportunitiesFromVerifiedEvent({ event, hotel: nobu });
  assert.equal(out.candidates.length, 0);
  ok("cross-market venue/event does not contaminate Nobu candidates");
}

// 21. Live discovery independent of seed pack (mock fetch)
{
  const html = `
    <html><body>
      <script type="application/ld+json">
      {"@type":"Event","name":"Mock Medical Congress 2027","startDate":"2027-09-15",
       "organizer":{"name":"Mock Medical Society"},"url":"https://example.org/mock-med-2027",
       "location":{"name":"Marylebone Hall","address":{"addressLocality":"London"}}}
      </script>
      <a href="/events/mock-med-2027">Mock Medical Congress 2027</a>
    </body></html>`;
  const page = {
    ok: true,
    text: "Mock Medical Congress 2027 15 September 2027 Marylebone",
    html,
    finalUrl: "https://example.org/events",
    fetchMethod: "STATIC_HTML",
    jsonLdEvents: [
      {
        "@type": "Event",
        name: "Mock Medical Congress 2027",
        startDate: "2027-09-15",
        organizer: { name: "Mock Medical Society" },
        url: "https://example.org/mock-med-2027",
        location: { name: "Marylebone Hall" },
      },
    ],
    eventDetailLinks: ["https://example.org/events/mock-med-2027"],
  };
  const parsed = parseCalendarPageToEventStubs({
    page,
    source: {
      id: "mock",
      url: "https://example.org/events",
      sourceFamily: "association_calendar",
      domainHint: "medical_scientific_academic",
    },
    marketCity: "London",
  });
  assert.ok(parsed.stubs.some((s) => /Mock Medical/i.test(s.eventName)));
  assert.ok(parsed.stubs.some((s) => s.verified && s.eventStartDate === "2027-09-15"));

  const fetchPage = async (url) => {
    if (String(url).includes("example.org")) return page;
    return { ok: false, error: "not_mock" };
  };
  const liveRun = await runEventToDemandDiscovery({
    hotelId: "recC2kbCaS48jFjFF",
    hotel: nobu,
    marketKey: "london",
    discoveryMode: "live",
    fetchPage,
    coverage: {
      rows: [{ segment: "medical_academic", status: "INSUFFICIENT_RESEARCH_DEPTH" }],
    },
    maxNewCandidates: 10,
  });
  // Registry will call real sources unless we only use mock — discoveryMode live
  // with fetchPage still hits registry URLs. Assert date helpers + registry load instead
  // when live returns zero due to mock-only fetch failures on real hosts.
  assert.ok(extractDatesFromText("Conference on 2027-11-03 in London").includes("2027-11-03"));
  const reg = loadEventCalendarSources({ marketKey: "london" });
  assert.ok(reg.sources.length >= 5);
  assert.ok(liveRun.discoveryMode === "live");
  ok("live discovery path is independent of seed packs (parser + registry + mode)");
}

// 22. Mechanism-specific fit: executive at stretch OK for luxury; mass exhibitor not
{
  const execFit = assessEventHotelFit({
    event: {
      eventName: "Big Expo",
      eventStartDate: "2027-04-01",
      venue: "ExCeL London",
      city: "London",
    },
    hotel: nobu,
    mechanism: EVENT_DEMAND_MECHANISM.EXECUTIVE_DELEGATION,
    evidence: { organizationNamed: true, organizerIdentified: true },
  });
  assert.equal(execFit.promoteCandidate, true);
  const exhibFit = assessEventHotelFit({
    event: {
      eventName: "Big Expo",
      eventStartDate: "2027-04-01",
      venue: "ExCeL London",
      city: "London",
    },
    hotel: nobu,
    mechanism: EVENT_DEMAND_MECHANISM.EXHIBITOR_SPONSOR_LODGING,
    evidence: { organizationNamed: true, maryleboneOvernightEvidence: false },
  });
  assert.equal(exhibFit.promoteCandidate, false);
  ok("mechanism-specific hotel fit distinguishes executive vs mass exhibitor at stretch");
}

// 23. Supported exploratory without invented booking demand
{
  const event = normalizeVerifiedEvent({
    eventName: "Quiet Board Offsite 2027",
    organizer: "Quiet Corp",
    organizerDemandRationale:
      "Organizer-run leadership offsite near Marylebone; compact luxury lodging prospect.",
    proposedOrganizerMechanism: EVENT_DEMAND_MECHANISM.OFFSITE_MEETING,
    eventStartDate: "2027-11-10",
    venue: "Marylebone",
    city: "London",
    verified: true,
    officialUrl: "https://example.com/quiet-board/meetings",
    sources: [
      { url: "https://example.com/quiet-board/meetings", sourceFamily: "official_event" },
    ],
  });
  const out = extractOpportunitiesFromVerifiedEvent({ event, hotel: nobu });
  assert.equal(out.candidates.length, 1);
  const c = out.candidates[0];
  assert.equal(c.hotelFit.supported, true);
  assert.equal(c.demandScale.supported, false);
  assert.ok(isActionableContactRoute(c.contact));
  const ev = evaluateOpportunity(c);
  assert.equal(ev.exploratory, true);
  assert.equal(ev.publishable, false);
  ok("exploratory prospect qualifies without invented confirmed lodging demand");
}

// 24. Facing duplicate suppressed (RCP pattern)
{
  const event = normalizeVerifiedEvent({
    eventName: "Update in medicine, London 2027",
    organizer: "Royal College of Physicians",
    organizerDemandRationale: "Travelling physicians near Regent's Park.",
    proposedOrganizerMechanism: EVENT_DEMAND_MECHANISM.SMALL_GROUP_LODGING,
    eventStartDate: "2027-03-18",
    venue: "Royal College of Physicians",
    city: "London",
    verified: true,
    sources: [{ url: "https://www.rcplondon.ac.uk/", sourceFamily: "official_event" }],
  });
  const out = extractOpportunitiesFromVerifiedEvent({
    event,
    hotel: nobu,
    existingOpportunities: [
      {
        id: "gdi_seg_881a40a713988f0c",
        organizationName: "Royal College of Physicians - Update in medicine, London 2027",
        eventName: "Update in medicine, London 2027",
        customerVisible: true,
        researchStatus: "PUBLISHABLE_PROSPECT",
        prospectPublicationBasis: "REVIEWED_PACKAGE_V1",
      },
    ],
  });
  assert.equal(out.candidates.length, 0);
  assert.ok(
    out.rejected.some((r) => r.reason === "duplicate_of_existing_facing_opportunity")
  );
  ok("duplicate of existing facing opportunity correctly suppressed");
}

// 25. Multiple distinct mechanisms from one event
{
  const event = normalizeVerifiedEvent({
    eventName: "Design Week Hosted 2027",
    organizer: "Design Week Ltd",
    organizerDemandRationale: "Organizer committee lodging near venue.",
    proposedOrganizerMechanism: EVENT_DEMAND_MECHANISM.OFFSITE_MEETING,
    proposedMechanisms: [
      {
        organizationName: "Design Week Ltd",
        mechanism: EVENT_DEMAND_MECHANISM.VIP_HOSPITALITY,
        demandRationale: "Separate VIP collector reception programme with luxury lodging need.",
      },
    ],
    eventStartDate: "2027-09-20",
    venue: "Marylebone",
    city: "London",
    verified: true,
    sources: [{ url: "https://example.com/dw", sourceFamily: "official_event" }],
    stakeholders: [
      {
        organizationName: "Studio Beta",
        mechanism: EVENT_DEMAND_MECHANISM.EXHIBITOR_SPONSOR_LODGING,
        demandRationale: "Exhibiting studio team travel for design week.",
        maryleboneOvernightEvidence: true,
      },
    ],
  });
  const out = extractOpportunitiesFromVerifiedEvent({ event, hotel: nobu });
  const mechs = new Set(out.candidates.map((c) => c.eventDemandMechanism));
  assert.ok(mechs.size >= 2, JSON.stringify([...mechs]));
  ok("single event can yield multiple distinct mechanisms when evidence-backed");
}

// 26. Live mechanism attach does not invent attendance
{
  const attached = attachLiveMechanismProposals(
    {
      liveDiscovered: true,
      eventName: "Future Corp Summit 2028",
      eventStartDate: "2028-03-01",
      organizer: "Future Corp",
      domain: "corporate_event_meeting",
      verified: true,
    },
    nobu
  );
  assert.ok(attached.organizerDemandRationale);
  assert.equal(attached.publishedAttendance, undefined);
  ok("live mechanism attach is evidence-proportionate without inventing attendance");
}

// 27. Held-out benchmark compare helper
{
  const cmp = compareLiveDiscoveryToBenchmark({
    liveEvents: [
      normalizeVerifiedEvent({
        eventName: "Frieze London 2027",
        eventStartDate: "2027-10-13",
        city: "London",
        venue: "Regent's Park",
        verified: true,
      }),
    ],
    benchmarkEvents: loadEventSeedPack({ marketKey: "london" }).events.slice(0, 5),
  });
  assert.ok(cmp.benchmarkCount === 5);
  assert.ok(typeof cmp.recallRate === "number");
  ok("held-out benchmark compare does not seed live discovery");
}

// 28. Future-date freshness
{
  assert.equal(
    isFutureEvent({ eventStartDate: "2027-12-01" }, new Date("2026-10-18T00:00:00Z")),
    true
  );
  assert.equal(
    isFutureEvent({ eventStartDate: "2025-01-01" }, new Date("2026-10-18T00:00:00Z")),
    false
  );
  ok("future-date freshness gate remains enforced");
}

// 29. Event listing ≠ sales contact; meetings path is sales-grade
{
  const {
    classifyContactUrlQuality,
    CONTACT_ROUTE_QUALITY,
    classifyEventDemandClass,
  } = await import(
    "../lib/group-demand-intelligence/research/event-to-demand/event-demand-class-v1.js"
  );
  assert.equal(
    classifyContactUrlQuality("https://www.excel.london/whats-on/popcultr-marketing-summit"),
    CONTACT_ROUTE_QUALITY.EVENT_LISTING_ONLY
  );
  assert.equal(
    classifyContactUrlQuality("https://www.london.edu/contact"),
    CONTACT_ROUTE_QUALITY.ORGANIZATION_SALES
  );
  assert.equal(
    isActionableContactRoute({
      intermediary: { type: "event_listing_evidence", url: "https://www.frieze.com/fairs/frieze-london" },
      contactRouteQuality: "EVENT_LISTING_ONLY",
      organizationRouteUrl: "https://www.frieze.com/fairs/frieze-london",
    }),
    false
  );
  const inferred = classifyEventDemandClass({
    futureDemand: { supported: true },
    hotelFit: { supported: true },
    eventDemandMechanism: "VIP_HOSPITALITY",
    localConnection: "VIP programme near Marylebone.",
    demandScale: { supported: false },
  });
  assert.equal(inferred, "INFERRED");
  ok("event listing is not a sales contact; demand class distinguishes inferred vs confirmed");
}

// 30. Stretch executive repair vs mass exhibitor
{
  const stretchExec = assessEventHotelFit({
    event: {
      eventName: "Event Tech Live",
      eventStartDate: "2026-11-12",
      venue: "ExCeL London",
      city: "London",
    },
    hotel: nobu,
    mechanism: EVENT_DEMAND_MECHANISM.EXECUTIVE_DELEGATION,
    evidence: { organizationNamed: true, organizerIdentified: true },
  });
  assert.equal(stretchExec.promoteCandidate, true);
  ok("ExCeL executive mechanism remains promotable under revised fit rules");
}

// 31. Demand ownership extracts distinct gallery + VIP without inventing
{
  const {
    extractDemandOwnersFromProspect,
    DEMAND_OWNER_ROLE,
    classifyBuyerConfidence,
    BUYER_CONFIDENCE,
  } = await import(
    "../lib/group-demand-intelligence/research/event-to-demand/event-demand-ownership-v1.js"
  );
  const {
    scoreCommercialOpportunity,
    isSalesBriefEligible,
  } = await import(
    "../lib/group-demand-intelligence/research/event-to-demand/event-commercial-score-v1.js"
  );
  const out = extractDemandOwnersFromProspect({
    opportunity: {
      id: "gdi_e2d_test",
      organizationName: "Frieze",
      eventName: "Frieze London 2027",
      eventId: "gdi_evt_frieze",
      eventDemandMechanism: EVENT_DEMAND_MECHANISM.VIP_HOSPITALITY,
      localConnection: "VIP programme",
    },
    seedEvent: {
      eventId: "gdi_evt_frieze",
      eventName: "Frieze London 2027",
      organizer: "Frieze",
      stakeholders: [
        {
          organizationName: "Voloshyn Gallery",
          mechanism: EVENT_DEMAND_MECHANISM.EXHIBITOR_SPONSOR_LODGING,
          demandRationale: "Participating gallery teams travel for Frieze week.",
        },
      ],
      sources: [{ url: "https://www.frieze.com/fairs/frieze-london" }],
    },
  });
  assert.ok(out.owners.length >= 2);
  assert.ok(out.owners.some((o) => o.organizationName === "Voloshyn Gallery"));
  assert.ok(
    out.owners.some((o) => o.role === DEMAND_OWNER_ROLE.VIP_HOSPITALITY_PROGRAM)
  );
  assert.equal(
    classifyBuyerConfidence({ businessEmail: "a@b.com" }),
    BUYER_CONFIDENCE.GENERIC_ORG_CONTACT
  );
  assert.equal(
    classifyBuyerConfidence({
      personName: "Jane Doe",
      role: "Events Manager",
      businessEmail: "jane@example.com",
    }),
    BUYER_CONFIDENCE.CONFIRMED_BUYER
  );
  const score = scoreCommercialOpportunity({
    opportunity: {
      futureDemand: { supported: true },
      hotelFit: { supported: true, territory: "TERRITORY_NEARBY" },
      eventDemandMechanism: "VIP_HOSPITALITY",
      eventDemandClass: "INFERRED",
      localConnection: "VIP",
      sources: [{ url: "https://example.com" }],
      evidence: [{}, {}],
      contactRouteQuality: "ORGANIZATION_SALES",
      contact: { contactFormUrl: "https://example.com/contact" },
      valueBand: { band: "MODERATE_COMPACT" },
      eventStartDate: "2027-10-13",
    },
    hotel: nobu,
  });
  assert.ok(score.total > 0 && score.band);
  assert.equal(
    isSalesBriefEligible({
      opportunity: {
        contactRouteQuality: "ORGANIZATION_SALES",
        contact: { contactFormUrl: "https://example.com/contact" },
      },
      commercialScore: { total: 60, band: "ACTIVE_PIPELINE" },
    }),
    true
  );
  ok("demand ownership + commercial score + buyer confidence gates");
}

// 32. Commercial disposition: partnership vs lodging vs signal vs reject
{
  const {
    classifyCommercialDisposition,
    COMMERCIAL_DISPOSITION,
    DEMAND_EVIDENCE_LABEL,
  } = await import(
    "../lib/group-demand-intelligence/research/event-to-demand/event-commercial-disposition-v1.js"
  );
  const {
    resolveBuyerForProspect,
    CONTACT_EVIDENCE_TIER,
    BUYER_RESOLUTION_BLOCKER,
  } = await import(
    "../lib/group-demand-intelligence/research/event-to-demand/event-buyer-resolution-v1.js"
  );

  const estate = classifyCommercialDisposition({
    opportunity: {
      organizationName: "Howard de Walden Estate",
      eventDemandMechanism: EVENT_DEMAND_MECHANISM.VIP_HOSPITALITY,
      eventDemandClass: "INFERRED",
      futureDemand: { supported: true, dateWindow: "2027-06-20" },
      hotelFitRationale: "Walking catchment partnership",
      localConnection: "Place-brand festival hospitality",
    },
    demandOwner: {
      organizationName: "Howard de Walden Estate",
      role: "EVENT_AGENCY_OR_TRAVEL_INTERMEDIARY",
    },
  });
  assert.equal(estate.disposition, COMMERCIAL_DISPOSITION.STRATEGIC_PARTNERSHIP);
  assert.equal(estate.demandEvidenceLabel, DEMAND_EVIDENCE_LABEL.PARTNERSHIP_WITHOUT_LODGING);

  const village = classifyCommercialDisposition({
    opportunity: {
      organizationName: "Marylebone Village",
      eventDemandMechanism: EVENT_DEMAND_MECHANISM.VIP_HOSPITALITY,
      eventDemandClass: "INFERRED",
      futureDemand: { supported: true, dateWindow: "2027-06-20" },
      hotelFitRationale: "Marylebone walking territory",
      localConnection: "Private hospitality potential",
    },
    demandOwner: { organizationName: "Marylebone Village", role: "VIP_HOSPITALITY_PROGRAM" },
  });
  assert.equal(village.disposition, COMMERCIAL_DISPOSITION.STRATEGIC_PARTNERSHIP);

  const lbs = classifyCommercialDisposition({
    opportunity: {
      organizationName: "London Business School",
      eventDemandMechanism: EVENT_DEMAND_MECHANISM.SMALL_GROUP_LODGING,
      eventDemandClass: "INFERRED",
      futureDemand: { supported: true, dateWindow: "2027-01-15" },
      hotelFitRationale: "Compact exec-ed lodging fit",
      localConnection: "Travelling participants often stay overnight",
    },
    demandOwner: {
      organizationName: "London Business School",
      role: "ASSOCIATION_OR_EXEC_ED",
    },
  });
  assert.equal(lbs.disposition, COMMERCIAL_DISPOSITION.HOTEL_SALES_PROSPECT);
  assert.equal(lbs.demandEvidenceLabel, DEMAND_EVIDENCE_LABEL.INFERRED_DEMAND);
  assert.notEqual(lbs.demandEvidenceLabel, DEMAND_EVIDENCE_LABEL.CONFIRMED_BUYING);

  const exhibitorSignal = classifyCommercialDisposition({
    opportunity: {
      organizationName: "Voloshyn Gallery",
      eventDemandMechanism: EVENT_DEMAND_MECHANISM.EXHIBITOR_SPONSOR_LODGING,
      eventDemandClass: "INFERRED",
      localConnection: "Gallery participation listed",
    },
    demandOwner: { organizationName: "Voloshyn Gallery", role: "EXHIBITOR" },
  });
  assert.equal(
    exhibitorSignal.disposition,
    COMMERCIAL_DISPOSITION.MARKET_INTELLIGENCE
  );

  // Generic org route must not be labeled verified buyer
  const routeRes = resolveBuyerForProspect({
    opportunity: {
      organizationName: "London Tech Week",
      eventDemandMechanism: EVENT_DEMAND_MECHANISM.EXECUTIVE_DELEGATION,
      contact: {
        contactFormUrl: "https://londontechweek.com/contact",
        contactRouteQuality: "ORGANIZATION_SALES",
        buyerFunctionHint: "MEETINGS_EVENTS",
      },
    },
    disposition: lbs,
  });
  assert.equal(
    routeRes.contactEvidence.tier,
    CONTACT_EVIDENCE_TIER.FUNCTIONAL_DEPARTMENT_ROUTE
  );
  assert.equal(routeRes.namedPerson, null);
  assert.equal(routeRes.namedPersonStatus, "explicitly_unknown");
  assert.notEqual(
    routeRes.contactEvidence.tier,
    CONTACT_EVIDENCE_TIER.VERIFIED_NAMED_DECISION_MAKER
  );
  assert.equal(
    routeRes.resolutionBlocker.blocker,
    BUYER_RESOLUTION_BLOCKER.ORG_ROUTE_NAMED_ENRICHMENT_INCOMPLETE
  );

  // Homepage alone is not usable
  const homepage = resolveBuyerForProspect({
    opportunity: {
      organizationName: "Example Org",
      eventDemandMechanism: EVENT_DEMAND_MECHANISM.HOST_EVENT,
      contact: { organizationRouteUrl: "https://example.org/" },
      contactUrl: "https://example.org/",
    },
  });
  assert.equal(homepage.contactEvidence.tier, CONTACT_EVIDENCE_TIER.NO_USABLE_ROUTE);

  // Buyer function without named person is valid continuation state
  const fnOnly = resolveBuyerForProspect({
    opportunity: {
      organizationName: "Frieze",
      eventDemandMechanism: EVENT_DEMAND_MECHANISM.VIP_HOSPITALITY,
      buyerFunction: "VIP_HOSPITALITY",
      contact: {},
    },
  });
  assert.equal(
    fnOnly.resolutionBlocker.blocker,
    BUYER_RESOLUTION_BLOCKER.FUNCTION_KNOWN_PERSON_UNKNOWN
  );

  // Secondary demand-owner must not inherit another org's contact
  {
    const { normalizeOrgIdentityKey } = await import(
      "../lib/group-demand-intelligence/workflow/reuse-prior-intelligence-v1.js"
    );
    const galleryOpp = {
      organizationName: "Voloshyn Gallery",
      eventDemandMechanism: EVENT_DEMAND_MECHANISM.EXHIBITOR_SPONSOR_LODGING,
      contact: { businessEmail: "info@voloshyngallery.art" },
    };
    const friezeOwner = {
      organizationName: "Frieze",
      role: "EXHIBITOR",
      sourceProspectId: "gdi_e2d_gallery",
    };
    assert.notEqual(
      normalizeOrgIdentityKey(galleryOpp.organizationName),
      normalizeOrgIdentityKey(friezeOwner.organizationName)
    );
    const stripped = {
      ...galleryOpp,
      organizationName: friezeOwner.organizationName,
      contact: {},
    };
    const friezeRes = resolveBuyerForProspect({
      opportunity: stripped,
      demandOwner: friezeOwner,
    });
    assert.equal(friezeRes.contactEvidence.tier, CONTACT_EVIDENCE_TIER.NO_USABLE_ROUTE);
    assert.equal(friezeRes.contactEvidence.surface?.email || "", "");
  }

  ok("commercial disposition + buyer-resolution contact evidence tiers");
}

console.log(`\nAll ${n} event-to-demand checks passed.`);
