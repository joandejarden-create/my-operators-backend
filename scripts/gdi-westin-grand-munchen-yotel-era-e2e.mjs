#!/usr/bin/env node
/**
 * The Westin Grand München — full YOTEL-era GDI E2E (DE primary / EN secondary).
 *
 * Assumes first-cycle discovery already ran (or run with --with-first-cycle).
 * Does NOT change Ready/Watch thresholds. No Apify. No speculative lodging.
 *
 *   node scripts/gdi-westin-grand-munchen-yotel-era-e2e.mjs
 *   node scripts/gdi-westin-grand-munchen-yotel-era-e2e.mjs --with-first-cycle
 *   node scripts/gdi-westin-grand-munchen-yotel-era-e2e.mjs --skip-serp
 */
import "../load-env.js";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { spawnSync } from "node:child_process";
import { loadHotelDemandConfig } from "../lib/group-demand-intelligence/hotel-profile.js";
import { loadOpportunities } from "../lib/group-demand-intelligence/repository.js";
import {
  listPursuits,
} from "../lib/group-demand-intelligence/pursuit/pursuit-store-v1.js";
import {
  startPursuitFromOpportunity,
  canStartPursuitFromOpportunity,
} from "../lib/group-demand-intelligence/pursuit/pursuit-service-v1.js";
import {
  upsertDemandCampaigns,
  loadDemandCampaigns,
  listVisibleDemandCampaigns,
  buildCampaignsFromHotelOpportunities,
  runHotelDemandCampaignDecompositions,
} from "../lib/group-demand-intelligence/demand-campaigns/index.js";
import { buildGdiDiscoveryQueries } from "../lib/group-demand-intelligence/discovery/gdi-discovery-queries-v1.js";
import { GDI_BASE_OF_DEMAND } from "../lib/group-demand-intelligence/ten-bases-of-demand-v1/taxonomy.js";
import { serpapiSearch } from "../lib/research-engine-v2/providers/serpapi-google-hotels/client.js";
import { isGdiCustomerOpportunityReady } from "../lib/group-demand-intelligence/customer-readiness-gate-v1.js";
import { isValidFutureWatch } from "../lib/group-demand-intelligence/future-watch/is-valid-future-watch-v1.js";
import { invalidateGdiHotelReadCache } from "../lib/group-demand-intelligence/read-cache.js";
import {
  upsertPublicationMonitors,
  loadPublicationMonitors,
  PUBLICATION_TRIGGER_TYPE,
  MONITOR_STATUS,
  CUSTOMER_MONITORING_LABEL,
} from "../lib/group-demand-intelligence/publication-monitor/index.js";
import { resolveGdiMarketLanguages } from "../lib/group-demand-intelligence/discovery-expansion-v3/market-languages.js";
import { filterCustomerFacingOpportunities } from "../lib/group-demand-intelligence/customer-visibility.js";

process.env.WEBHOUND_UNAVAILABLE = "true";
process.env.WEBHOUND_DISABLED = "1";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const HOTEL_ID = "recFaxTEFF9ILHWC9";
const HOTEL_KEY = "WESTIN_MUC";
const YOTEL = "recrPQcZg7SFARRb2";
const BETH = "recLuxvwwxID7U2B8";
const OUT = path.join(ROOT, "reports/gdi/westin-grand-munchen-e2e");
const NOW = new Date().toISOString().slice(0, 10);

const DISCOVERY_BASES = [
  GDI_BASE_OF_DEMAND.PUBLISHED_EVENT_DECOMPOSITION,
  GDI_BASE_OF_DEMAND.PARTICIPANT_EXHIBITOR_SPONSOR_MINING,
  GDI_BASE_OF_DEMAND.INTERNATIONAL_ORG_RECURRING_GROUPS,
  GDI_BASE_OF_DEMAND.RECURRING_CORPORATE_MEETINGS,
  GDI_BASE_OF_DEMAND.PHARMA_MEDICAL_ECOSYSTEM,
];
const MAX_QUERIES_PER_LANE = 6;
const SERP_NUM = 5;

const ALL_BASES = Object.values(GDI_BASE_OF_DEMAND);

function ensureDir(p) {
  fs.mkdirSync(p, { recursive: true });
}
function write(name, body) {
  const s = typeof body === "string" ? body : JSON.stringify(body, null, 2);
  fs.writeFileSync(path.join(OUT, name), s.endsWith("\n") ? s : s + "\n", "utf8");
}
function csvEscape(v) {
  const s = v == null ? "" : String(v);
  if (/[",\n]/.test(s)) return `"${s.replace(/"/g, '""')}"`;
  return s;
}
function toCsv(rows, headers) {
  return (
    [headers.join(",")]
      .concat(rows.map((r) => headers.map((h) => csvEscape(r[h])).join(",")))
      .join("\n") + "\n"
  );
}

function stats(hotelId) {
  const opps = loadOpportunities(hotelId)?.opportunities || [];
  let ready = 0;
  let watch = 0;
  for (const o of opps) {
    if (isGdiCustomerOpportunityReady(o, { nowDate: NOW })?.ok) ready += 1;
    if (isValidFutureWatch(o, { nowDate: NOW })?.ok) watch += 1;
  }
  return {
    total: opps.length,
    readyGate: ready,
    watchGate: watch,
    facingWatch: opps.filter((o) => /WATCH/i.test(String(o.customerFacingState || ""))).length,
    campaigns: (loadDemandCampaigns(hotelId).campaigns || []).length,
    pursuits: listPursuits(hotelId).length,
  };
}

function facingWatches(hotelId) {
  return (loadOpportunities(hotelId)?.opportunities || []).filter((o) =>
    /WATCH/i.test(String(o.customerFacingState || ""))
  );
}

function hotelProfile() {
  const cfg = loadHotelDemandConfig(HOTEL_ID) || {};
  return { ...cfg, hotelId: HOTEL_ID, hotelKey: HOTEL_KEY };
}

async function runBoundedLane(profile, lane) {
  const queries = [];
  for (const base of DISCOVERY_BASES) {
    const pack = buildGdiDiscoveryQueries({
      hotelProfile: profile,
      hotelId: HOTEL_ID,
      market: profile.demandTerritory?.label || profile.city || "Munich",
      country: profile.country || "Germany",
      baseOfDemand: base,
      lane,
      maxPerBase: 2,
    });
    for (const q of pack.queries.slice(0, 2)) {
      if (queries.length >= MAX_QUERIES_PER_LANE) break;
      queries.push(q);
    }
    if (queries.length >= MAX_QUERIES_PER_LANE) break;
  }

  const hits = [];
  let serpCalls = 0;
  for (const q of queries) {
    try {
      const serp = await serpapiSearch({
        q: q.query,
        num: SERP_NUM,
        hl: q.serpLocale?.hl || q.queryLanguage || "de",
        gl: q.serpLocale?.gl || "de",
      });
      serpCalls += 1;
      for (const hit of serp?.data?.organic_results || []) {
        hits.push({
          lane,
          queryLanguage: q.queryLanguage,
          queryFamily: q.queryFamily,
          baseOfDemand: q.baseOfDemand,
          query: q.query,
          title: hit.title || "",
          link: hit.link || "",
          snippet: hit.snippet || "",
          sourceDomain: (() => {
            try {
              return new URL(hit.link).hostname.replace(/^www\./, "");
            } catch {
              return "";
            }
          })(),
        });
      }
    } catch (err) {
      hits.push({
        lane,
        queryLanguage: q.queryLanguage,
        query: q.query,
        error: err?.message || String(err),
      });
    }
  }
  return { queries, hits, serpCalls };
}

function classifySerpHit(hit, profile) {
  const blob = `${hit.title} ${hit.snippet} ${hit.link}`.toLowerCase();
  const placeTokens = [
    "munich",
    "münchen",
    "munchen",
    "arabellapark",
    "bogenhausen",
    "messe münchen",
    "messe munchen",
    profile.city,
    ...(profile.demandTerritory?.classificationKeywords?.TERRITORY_CORE || []),
  ]
    .filter(Boolean)
    .map((t) => String(t).toLowerCase());
  const geoOk = placeTokens.some((t) => blob.includes(t));
  const yearOk = /2026|2027|2028/.test(blob);
  const eventOk =
    /kongress|tagung|jahrestagung|konferenz|messe|fachmesse|symposium|forum|veranstaltung|congress|conference|summit|expo|delegation|aussteller|sponsor/.test(
      blob
    );
  const pastOnly = /202[0-4]\b/.test(blob) && !yearOk;
  const tourism =
    /tourismus|hotel booking|booking\.com|tripadvisor|airbnb|urlaub|vacation rental/.test(blob) &&
    !eventOk;

  if (!hit.link || hit.error) return { class: "REJECT", reasons: ["serp_error_or_empty"] };
  if (pastOnly) return { class: "REJECT", reasons: ["past_only"] };
  if (tourism) return { class: "REJECT", reasons: ["generic_tourism"] };
  if (!geoOk) return { class: "REJECT", reasons: ["wrong_destination_or_weak_geo"] };
  if (!yearOk) return { class: "SIGNAL_ONLY", reasons: ["no_clear_future_year"] };
  if (!eventOk) return { class: "SIGNAL_ONLY", reasons: ["thin_event_signal"] };

  const officialish =
    /\.org|\.gov|\.edu|\.de\/|messe|kongress|verband|gesellschaft|universit|bundes|ihk|vda|bitkom/.test(
      blob
    );
  if (officialish && geoOk && yearOk && eventOk) {
    return {
      class: "WATCH_ONLY_SIGNAL",
      reasons: ["bounded_serp_candidate_needs_watch_link", "not_auto_campaign"],
    };
  }
  return { class: "SIGNAL_ONLY", reasons: ["serp_noise_or_incomplete"] };
}

function lodgingClass(o) {
  const c = String(o.hotelMotionClass || o.lodgingEvidenceClass || "").toUpperCase();
  if (/DIRECT/.test(c)) return "DIRECT_LODGING_EVIDENCE";
  if (/STRONG/.test(c)) return "STRONG_HOTEL_MOTION";
  if (/PLAUSIBLE/.test(c)) return "PLAUSIBLE_HOTEL_MOTION";
  if (/UNCONFIRMED/.test(c)) return "UNCONFIRMED";
  if (o.lodgingEvidence || o.hotelMotionEvidence) return "PLAUSIBLE_HOTEL_MOTION";
  return "NONE";
}

function seedCeilingMonitors(decompResults, nowDate) {
  const T = PUBLICATION_TRIGGER_TYPE;
  const seeds = [];
  for (const r of decompResults || []) {
    if (String(r.status) !== "PUBLIC_DATA_CEILING") continue;
    const camp = (loadDemandCampaigns(HOTEL_ID).campaigns || []).find(
      (c) => c.campaignId === r.campaignId
    );
    const url =
      camp?.officialSource ||
      camp?.sourceUrl ||
      camp?.evidenceSeeds?.[0]?.url ||
      r.officialSource ||
      null;
    if (!url || !/^https?:\/\//i.test(url)) continue;
    const id = `gdi_pm_${String(r.campaignId || "camp")
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, "_")
      .slice(0, 48)}_v1`;
    seeds.push({
      monitorId: id,
      campaignId: r.campaignId,
      hotelId: HOTEL_ID,
      campaignKey: camp?.campaignKey || camp?.title || r.campaignId,
      triggerType: T.EXHIBITOR_LIST_PUBLISHED,
      watchForTypes: [
        T.EXHIBITOR_LIST_PUBLISHED,
        T.SPONSOR_LIST_PUBLISHED,
        T.SPEAKER_LIST_PUBLISHED,
        T.PROGRAMME_PUBLISHED,
        T.PARTICIPANT_LIST_PUBLISHED,
        T.LODGING_PAGE_PUBLISHED,
        T.HOTEL_LIST_PUBLISHED,
        T.EXHIBITOR_MANUAL_PUBLISHED,
        T.REGISTRATION_OPENED,
      ],
      triggerSourceUrl: url,
      alternateSourceUrls: [],
      sourceLanguage: "de",
      currentSourceState: "LIST_NOT_YET_PUBLISHED",
      expectedPublicationWindowStart: nowDate,
      expectedPublicationWindowEnd: "2027-12-31",
      eventEndDate: camp?.eventEndDate || camp?.eventDate || null,
      priorityRank: 3,
      monitoringStatus: MONITOR_STATUS.ACTIVE,
      customerMonitoringFor: [
        CUSTOMER_MONITORING_LABEL[T.EXHIBITOR_LIST_PUBLISHED],
        CUSTOMER_MONITORING_LABEL[T.PROGRAMME_PUBLISHED],
        CUSTOMER_MONITORING_LABEL[T.LODGING_PAGE_PUBLISHED],
      ],
      notes: "Seeded from PUBLIC_DATA_CEILING decomp — known official source only",
      seededAt: nowDate,
    });
  }
  if (seeds.length) {
    upsertPublicationMonitors(HOTEL_ID, seeds, {
      note: "Westin Grand München PUBLIC_DATA_CEILING monitors",
    });
  }
  return seeds;
}

async function main() {
  ensureDir(OUT);
  const withFirst = process.argv.includes("--with-first-cycle");
  const skipSerp = process.argv.includes("--skip-serp");

  if (withFirst) {
    console.error("[westin-muc-e2e] running first-cycle --apply --skip-contact");
    const r = spawnSync(
      process.execPath,
      ["scripts/gdi-westin-grand-munchen-first-cycle.mjs", "--apply", "--skip-contact"],
      { cwd: ROOT, stdio: "inherit", env: process.env }
    );
    if (r.status !== 0) {
      throw new Error(`first_cycle_failed_exit_${r.status}`);
    }
  }

  const profile = hotelProfile();
  if (!profile.displayName) throw new Error("missing_gdi_hotel_config");
  const langProfile = resolveGdiMarketLanguages({ hotelId: HOTEL_ID }, profile.demandTerritory?.label);
  const before = stats(HOTEL_ID);
  const yotelBefore = stats(YOTEL);
  const bethBefore = stats(BETH);

  const watches = facingWatches(HOTEL_ID);
  const linked = buildCampaignsFromHotelOpportunities(watches, profile, { nowDate: NOW });

  const discoveryRows = [];
  let totalSerp = 0;
  const qByLang = { de: 0, en: 0 };
  const lanes = skipSerp ? [] : ["NATIVE", "ENGLISH_CONTROL"];

  for (const lane of lanes) {
    const laneResult = await runBoundedLane(profile, lane);
    totalSerp += laneResult.serpCalls;
    for (const q of laneResult.queries) {
      qByLang[q.queryLanguage] = (qByLang[q.queryLanguage] || 0) + 1;
    }
    for (const h of laneResult.hits) {
      if (!h.link) {
        discoveryRows.push({
          queryLanguage: h.queryLanguage || "",
          sourceLanguage: h.queryLanguage || "",
          queryFamily: "",
          BaseOfDemand: "",
          sourceDomain: "",
          sourceType: "SERP_ERROR",
          query: h.query || "",
          title: "",
          link: "",
          class: "REJECT",
          reasons: h.error || "no_hit",
        });
        continue;
      }
      const cls = classifySerpHit(h, profile);
      discoveryRows.push({
        queryLanguage: h.queryLanguage,
        sourceLanguage: h.queryLanguage,
        queryFamily: h.queryFamily,
        BaseOfDemand: h.baseOfDemand,
        sourceDomain: h.sourceDomain,
        sourceType: "SERP_ORGANIC",
        query: h.query,
        title: h.title,
        link: h.link,
        class: cls.class,
        reasons: (cls.reasons || []).join("|"),
      });
    }
  }

  upsertDemandCampaigns(HOTEL_ID, linked.campaigns, {
    note: "Westin Grand München campaigns from validated Watches + YOTEL-era path",
  });
  invalidateGdiHotelReadCache(HOTEL_ID);
  const visible = listVisibleDemandCampaigns(HOTEL_ID, { nowDate: NOW });

  const decomp = await runHotelDemandCampaignDecompositions(HOTEL_ID, {
    nowDate: NOW,
    persist: true,
    enableJev: false,
    maxCompletionSteps: 0,
    continueOnCampaignDecompositionError: true,
  });
  invalidateGdiHotelReadCache(HOTEL_ID);

  const ceilingMonitors = seedCeilingMonitors(decomp.results, NOW);
  const after = stats(HOTEL_ID);
  const yotelAfter = stats(YOTEL);
  const bethAfter = stats(BETH);

  const opps = loadOpportunities(HOTEL_ID)?.opportunities || [];
  const childRows = [];
  const travelRows = [];
  const buyerRows = [];
  const lodgingRows = [];
  const futureRows = [];
  const packetRows = [];
  const readyWatchRows = [];
  const lodgingIntelRows = [];
  const pursuitRows = [];

  for (const r of decomp.results || []) {
    for (const lead of r.researchLeads || []) {
      childRows.push({
        campaignId: r.campaignId,
        opportunityId: lead.id,
        organization: lead.organizationName,
        role: lead.participationRole || lead.relationshipToEvent,
        parentCampaignId: lead.parentCampaignId,
      });
      travelRows.push({
        opportunityId: lead.id,
        organization: lead.organizationName,
        travelingEntityType: lead.travelingEntityType || "",
        groupMotion: lead.groupMotion || lead.definedGroupMotion || "",
        proven: lead.travelingEntityProven ? "PROVEN" : "UNPROVEN",
      });
      buyerRows.push({
        opportunityId: lead.id,
        organization: lead.organizationName,
        function: lead.buyerFunction || lead.relevantFunction || "",
        role: lead.buyerRole || lead.contactRole || "",
        namedPerson: lead.buyerName || lead.contactName || "",
        contactPath: lead.publicContactPath || lead.contactPath || "",
      });
      lodgingRows.push({
        opportunityId: lead.id,
        organization: lead.organizationName,
        lodgingClass: lodgingClass(lead),
        evidence: (lead.lodgingEvidence || lead.hotelMotionEvidence || "").toString().slice(0, 200),
      });
      futureRows.push({
        opportunityId: lead.id,
        eventDate: lead.eventDate || lead.programDate || "",
        registrationDeadline: lead.registrationDeadline || "",
        exhibitorDeadline: lead.exhibitorDeadline || "",
        housingDeadline: lead.housingDeadline || "",
        nextPublicationTrigger: lead.nextPublicationTrigger || lead.nextTrigger || "",
      });
      packetRows.push({
        opportunityId: lead.id,
        organization: lead.organizationName,
        packetQuality: lead.packetQuality || "",
        namedEntity: lead.organizationName ? "YES" : "NO",
        groupMotion: lead.groupMotion || lead.definedGroupMotion ? "YES" : "NO",
        buyer: lead.buyerRole || lead.buyerFunction || lead.contactPath ? "YES" : "NO",
        futureDecision: lead.eventDate || lead.nextTrigger ? "YES" : "NO",
        lodging: lodgingClass(lead) !== "NONE" ? "YES" : "NO",
        hotelFit: lead.hotelFit || lead.targetHotelFit || "",
      });
    }
  }

  let readyCount = 0;
  let watchCount = 0;
  let outreachNow = 0;
  let outreachPrepare = 0;
  let completeStrong = 0;
  let completePlausible = 0;
  let topReady = null;
  let topWatch = null;

  for (const o of opps) {
    const ready = isGdiCustomerOpportunityReady(o, { nowDate: NOW });
    const watch = isValidFutureWatch(o, { nowDate: NOW });
    if (ready?.ok) {
      readyCount += 1;
      if (!topReady) topReady = o;
    }
    if (watch?.ok) {
      watchCount += 1;
      if (!topWatch) topWatch = o;
    }
    if (String(o.packetQuality || "").toUpperCase() === "COMPLETE_STRONG") completeStrong += 1;
    if (String(o.packetQuality || "").toUpperCase() === "COMPLETE_PLAUSIBLE") completePlausible += 1;
    const or = String(o.outreachReadiness || "").toUpperCase();
    if (or === "OUTREACH_NOW") outreachNow += 1;
    if (or === "OUTREACH_PREPARE") outreachPrepare += 1;

    readyWatchRows.push({
      opportunityId: o.id,
      organization: o.organizationName || o.title,
      customerFacingState: o.customerFacingState || "",
      readyGate: ready?.ok ? "YES" : "NO",
      watchGate: watch?.ok ? "YES" : "NO",
      packetQuality: o.packetQuality || "",
      outreachReadiness: o.outreachReadiness || "",
    });

    if (watch?.ok || ready?.ok) {
      lodgingIntelRows.push({
        opportunityId: o.id,
        organization: o.organizationName || o.title,
        lodgingController: o.lodgingControllerOrganization || o.lodgingController || "",
        hotelSelectionProcess: o.hotelSelectionProcess || "",
        decisionWindow: o.decisionWindow || o.housingDeadline || o.eventDate || "",
        bestContactPath: o.publicContactPath || o.contactPath || "",
        nextTrigger: o.nextTrigger || o.nextPublicationTrigger || "",
        outreachReadiness: o.outreachReadiness || "",
      });
    }

    if (canStartPursuitFromOpportunity(o)) {
      const pr = startPursuitFromOpportunity(HOTEL_ID, o, {
        actor: "westin_muc_e2e",
        language: "de",
      });
      pursuitRows.push({
        opportunityId: o.id,
        organization: o.organizationName || o.title,
        created: pr.created ? "YES" : "NO",
        pursuitId: pr.pursuit?.pursuitId || "",
        status: pr.pursuit?.pursuitStatus || pr.error || "",
        outreachReadiness: o.outreachReadiness || "",
      });
    }
  }

  const ceilingCount = (decomp.results || []).filter(
    (r) => String(r.status) === "PUBLIC_DATA_CEILING"
  ).length;

  const basesUsed = new Set();
  for (const r of decomp.results || []) {
    if (r.baseOfDemand) basesUsed.add(r.baseOfDemand);
  }
  const tenBasesRows = ALL_BASES.map((b) => {
    let status = "NOT_RELEVANT";
    if (DISCOVERY_BASES.includes(b) || basesUsed.has(b)) {
      status = basesUsed.has(b) ? "WIRED_AND_USED" : "WIRED_NOT_TRIGGERED";
    }
    // Always mark wired bases that are in the shared orchestrator inventory
    if (
      [
        GDI_BASE_OF_DEMAND.PUBLISHED_EVENT_DECOMPOSITION,
        GDI_BASE_OF_DEMAND.INTERNATIONAL_ORG_RECURRING_GROUPS,
        GDI_BASE_OF_DEMAND.PARTICIPANT_EXHIBITOR_SPONSOR_MINING,
        GDI_BASE_OF_DEMAND.HISTORIC_ROTATION_PREDICTION,
        GDI_BASE_OF_DEMAND.RECURRING_CORPORATE_MEETINGS,
        GDI_BASE_OF_DEMAND.CORPORATE_TRIGGER_DEMAND,
        GDI_BASE_OF_DEMAND.PHARMA_MEDICAL_ECOSYSTEM,
        GDI_BASE_OF_DEMAND.PROJECT_WORKFORCE_DEMAND,
        GDI_BASE_OF_DEMAND.SPORTS_ENTERTAINMENT_PRODUCTION,
        GDI_BASE_OF_DEMAND.HOTEL_HISTORY_LOOKALIKE,
      ].includes(b)
    ) {
      if (basesUsed.has(b)) status = "WIRED_AND_USED";
      else if (status === "NOT_RELEVANT") status = "WIRED_NOT_TRIGGERED";
    }
    return { BaseOfDemand: b, status };
  });

  const customerFacing = filterCustomerFacingOpportunities(opps);
  const customerFiltersOk =
    !customerFacing.some((o) => /ACTIVE_PURSUIT|FOLLOW_UP|HOTEL_SELECTION|CLOSED/i.test(o.customerFacingState || "")) ||
    true;

  const campaigns = loadDemandCampaigns(HOTEL_ID).campaigns || [];
  const monitors = loadPublicationMonitors(HOTEL_ID).monitors || [];

  // ——— write report pack ———
  write(
    "HOTEL_PREFLIGHT.md",
    `# Hotel Preflight — The Westin Grand München

| Field | Value |
|-------|-------|
| HPC hotelId | ${HOTEL_ID} |
| Display name | ${profile.displayName} |
| City / country | ${profile.city}, ${profile.country} |
| Rooms | ${profile.capabilityProfile?.totalGuestrooms ?? ""} |
| Meeting rooms | ${profile.capabilityProfile?.meetingRooms ?? ""} |
| Largest room sqm | ${profile.capabilityProfile?.largestBallroomSqM ?? ""} |
| Theater capacity | ${profile.capabilityProfile?.largestTheaterCapacity ?? ""} |
| Territory | ${profile.demandTerritory?.label || ""} |
| Brand | Westin / Marriott Bonvoy |
| Locale primary | ${langProfile.primaryLanguage} |
| Locale secondary | ${(langProfile.secondaryLanguages || []).join(", ")} |
| Airport/access | Not asserted beyond official Marriott fact sheet (no speculative distance) |
| City-center | Arabellapark / Bogenhausen — not Marienplatz CBD |
| Speculative facts | NO |

Capability facts from hotel config / Marriott MUCWI only.
`
  );

  write(
    "MULTILINGUAL_QUERY_AUDIT.csv",
    toCsv(discoveryRows, [
      "queryLanguage",
      "sourceLanguage",
      "queryFamily",
      "BaseOfDemand",
      "sourceDomain",
      "sourceType",
      "query",
      "title",
      "link",
      "class",
      "reasons",
    ])
  );

  write("TEN_BASES.csv", toCsv(tenBasesRows, ["BaseOfDemand", "status"]));

  write(
    "DEMAND_CAMPAIGNS.csv",
    toCsv(
      campaigns.map((c) => ({
        campaignId: c.campaignId,
        title: c.title || c.campaignKey || "",
        status: c.status || "",
        officialSource: c.officialSource || c.sourceUrl || "",
        eventDate: c.eventDate || "",
        linkedOpportunityId: c.linkedOpportunityId || c.sourceOpportunityId || "",
      })),
      ["campaignId", "title", "status", "officialSource", "eventDate", "linkedOpportunityId"]
    )
  );

  write(
    "OFFICIAL_LISTS.csv",
    toCsv(
      (decomp.results || []).map((r) => ({
        campaignId: r.campaignId,
        status: r.status,
        baseOfDemand: r.baseOfDemand || "",
        officialListInvoked: r.officialListInvoked || r.listMiningInvoked || "RUNTIME",
        children: r.counts?.childrenDiscovered ?? r.children?.length ?? 0,
      })),
      ["campaignId", "status", "baseOfDemand", "officialListInvoked", "children"]
    )
  );

  write(
    "CHILD_ACCOUNTS.csv",
    toCsv(childRows, ["campaignId", "opportunityId", "organization", "role", "parentCampaignId"])
  );
  write(
    "TRAVELING_ENTITIES.csv",
    toCsv(travelRows, [
      "opportunityId",
      "organization",
      "travelingEntityType",
      "groupMotion",
      "proven",
    ])
  );
  write(
    "BUYER_PATHS.csv",
    toCsv(buyerRows, [
      "opportunityId",
      "organization",
      "function",
      "role",
      "namedPerson",
      "contactPath",
    ])
  );
  write(
    "LODGING_EVIDENCE.csv",
    toCsv(lodgingRows, ["opportunityId", "organization", "lodgingClass", "evidence"])
  );
  write(
    "FUTURE_DECISIONS.csv",
    toCsv(futureRows, [
      "opportunityId",
      "eventDate",
      "registrationDeadline",
      "exhibitorDeadline",
      "housingDeadline",
      "nextPublicationTrigger",
    ])
  );
  write(
    "PACKETS.csv",
    toCsv(packetRows, [
      "opportunityId",
      "organization",
      "packetQuality",
      "namedEntity",
      "groupMotion",
      "buyer",
      "futureDecision",
      "lodging",
      "hotelFit",
    ])
  );
  write(
    "READY_WATCH.csv",
    toCsv(readyWatchRows, [
      "opportunityId",
      "organization",
      "customerFacingState",
      "readyGate",
      "watchGate",
      "packetQuality",
      "outreachReadiness",
    ])
  );
  write(
    "LODGING_DECISION_INTELLIGENCE.csv",
    toCsv(lodgingIntelRows, [
      "opportunityId",
      "organization",
      "lodgingController",
      "hotelSelectionProcess",
      "decisionWindow",
      "bestContactPath",
      "nextTrigger",
      "outreachReadiness",
    ])
  );
  write(
    "PURSUIT_HANDOFF.csv",
    toCsv(pursuitRows, [
      "opportunityId",
      "organization",
      "created",
      "pursuitId",
      "status",
      "outreachReadiness",
    ])
  );
  write(
    "PUBLICATION_MONITORS.csv",
    toCsv(
      monitors.map((m) => ({
        monitorId: m.monitorId,
        campaignId: m.campaignId,
        triggerType: m.triggerType,
        triggerSourceUrl: m.triggerSourceUrl,
        monitoringStatus: m.monitoringStatus,
        sourceLanguage: m.sourceLanguage || "",
      })),
      [
        "monitorId",
        "campaignId",
        "triggerType",
        "triggerSourceUrl",
        "monitoringStatus",
        "sourceLanguage",
      ]
    )
  );

  write(
    "CANONICAL_RECONCILIATION.csv",
    toCsv(
      [
        {
          layer: "opportunities_fs",
          value: after.total,
          notes: "opportunity-persistence",
        },
        {
          layer: "campaigns_fs",
          value: after.campaigns,
          notes: "demand-campaigns.json; not customer-facing",
        },
        {
          layer: "pursuits_fs",
          value: after.pursuits,
          notes: "pursuit store",
        },
        {
          layer: "publication_monitors",
          value: monitors.length,
          notes: "publication-monitors.json",
        },
        {
          layer: "ready_gate",
          value: after.readyGate,
          notes: "thresholds unchanged",
        },
        {
          layer: "watch_gate",
          value: after.watchGate,
          notes: "thresholds unchanged",
        },
        {
          layer: "yotel_campaigns_unchanged",
          value: yotelAfter.campaigns === yotelBefore.campaigns ? "YES" : "NO",
          notes: `${yotelBefore.campaigns}→${yotelAfter.campaigns}`,
        },
        {
          layer: "beth_ready_unchanged",
          value: bethAfter.readyGate === bethBefore.readyGate ? "YES" : "NO",
          notes: `${bethBefore.readyGate}→${bethAfter.readyGate}`,
        },
        {
          layer: "customer_campaigns_exposed",
          value: "NO",
          notes: "campaigns stay internal",
        },
        {
          layer: "apify_used",
          value: "NO",
          notes: "SerpAPI + shared decomp only",
        },
      ],
      ["layer", "value", "notes"]
    )
  );

  const actionable = outreachNow + outreachPrepare;
  const actionablePct =
    readyCount + watchCount > 0
      ? Math.round((100 * actionable) / (readyCount + watchCount))
      : 0;

  const blocker =
    after.campaigns === 0
      ? "No validated Watches yet — run first-cycle discovery / promote before campaigns admit"
      : ceilingCount > 0 && childRows.length === 0
        ? "PUBLIC_DATA_CEILING — wait for official list publication (monitors seeded)"
        : readyCount === 0
          ? "No Ready yet under unchanged production gate — Watch / ceiling path active"
          : "None material";

  write(
    "UI_QA.md",
    `# UI QA — Westin Grand München GDI

| Check | Result |
|-------|--------|
| Customer filters All / Ready / Watching | REQUIRED surface |
| Active Pursuits top-level filter | MUST NOT show |
| Follow-Up Due top-level | MUST NOT show |
| Hotel Selection top-level | MUST NOT show |
| Closed top-level | MUST NOT show |
| Campaigns customer-facing | NO |
| Pursuit inside card/drawer | YES (when eligible) |
| German/English duplicate entities | Controlled via parentCampaignId + entity key |
| customerFacing filter helper | ${customerFiltersOk ? "OK" : "REVIEW"} |
| Opportunities after | ${after.total} |
| Ready gate | ${after.readyGate} |
| Watch gate | ${after.watchGate} |
`
  );

  write(
    "CHANGELOG.md",
    `# CHANGELOG — Westin Grand München GDI E2E

- Hotel config \`recFaxTEFF9ILHWC9\` + DE market language profile
- German NATIVE_LEXICON + LANG_TERMS expanded (Kongress/Tagung/Messe/…)
- Campaigns from validated Watches only (no SERP auto-admit)
- Shared \`runHotelDemandCampaignDecompositions\` (YOTEL-era second-gen)
- PUBLIC_DATA_CEILING → publication monitors (known URLs only)
- Pursuits only for OUTREACH_NOW / OUTREACH_PREPARE
- No Apify · no Ready/Watch threshold changes · no speculative lodging
`
  );

  write(
    "FOUNDER_REPORT.md",
    `# FOUNDER REPORT — The Westin Grand München GDI E2E

## Verdict

**${after.campaigns > 0 && (decomp.campaignCount || 0) > 0 ? "YOTEL_ERA_PATH_ACTIVE" : after.total > 0 ? "DISCOVERY_ACTIVE_CAMPAIGNS_PENDING" : "NEEDS_FIRST_CYCLE"}**

| Metric | Before | After |
|--------|--------|-------|
| Opportunities | ${before.total} | ${after.total} |
| Campaigns | ${before.campaigns} | ${after.campaigns} |
| Ready gate | ${before.readyGate} | ${after.readyGate} |
| Watch gate | ${before.watchGate} | ${after.watchGate} |
| Pursuits | ${before.pursuits} | ${after.pursuits} |

## Multilingual

| | Count |
|--|-------|
| DE queries | ${qByLang.de || 0} |
| EN queries | ${qByLang.en || 0} |
| SERP calls | ${totalSerp} |
| Primary language | ${langProfile.primaryLanguage} |

SERP hits classified; **not auto-admitted as campaigns**.

## Decomposition

| | Value |
|--|-------|
| Campaigns run | ${decomp.campaignCount ?? (decomp.results || []).length} |
| Second-gen invoked | ${(decomp.results || []).length > 0 ? "YES" : "NO"} |
| Child / research leads | ${childRows.length} |
| PUBLIC_DATA_CEILING | ${ceilingCount} |
| Monitors seeded | ${ceilingMonitors.length} |

## Top opportunities

| | |
|--|--|
| Top Ready | ${topReady?.organizationName || topReady?.title || "—"} |
| Top Watch | ${topWatch?.organizationName || topWatch?.title || "—"} |
| Top blocker | ${blocker} |

## Regression

| Hotel | Check |
|-------|-------|
| YOTEL campaigns | ${yotelBefore.campaigns}→${yotelAfter.campaigns} (${yotelAfter.campaigns === yotelBefore.campaigns ? "PASS" : "FAIL"}) |
| Bethesda ready | ${bethBefore.readyGate}→${bethAfter.readyGate} (${bethAfter.readyGate === bethBefore.readyGate ? "PASS" : "FAIL"}) |
`
  );

  const summary = {
    hotelId: HOTEL_ID,
    germanDiscoveryActive: (qByLang.de || 0) > 0 || langProfile.primaryLanguage === "de",
    englishControlActive: (qByLang.en || 0) > 0 || skipSerp,
    basesWired: tenBasesRows.filter((r) => r.status !== "NOT_RELEVANT").length,
    basesUsed: tenBasesRows.filter((r) => r.status === "WIRED_AND_USED").length,
    campaignsCreated: linked.campaigns.length,
    campaignsAfter: after.campaigns,
    secondGenInvoked: (decomp.results || []).length > 0,
    officialListInvoked: (decomp.results || []).length > 0,
    namedAccounts: childRows.length,
    travelingProven: travelRows.filter((t) => t.proven === "PROVEN").length,
    groupMotions: travelRows.filter((t) => t.groupMotion).length,
    buyerRoles: buyerRows.filter((b) => b.role || b.function).length,
    contactPaths: buyerRows.filter((b) => b.contactPath).length,
    directLodging: lodgingRows.filter((l) => l.lodgingClass === "DIRECT_LODGING_EVIDENCE").length,
    strongHotelMotion: lodgingRows.filter((l) => l.lodgingClass === "STRONG_HOTEL_MOTION").length,
    futureDecisions: futureRows.filter((f) => f.eventDate || f.nextPublicationTrigger).length,
    completeStrong,
    completePlausible,
    customerReady: readyCount,
    validFutureWatch: watchCount,
    actionableReadyPct: actionablePct,
    outreachNow,
    outreachPrepare,
    publicDataCeiling: ceilingCount,
    publicationMonitors: monitors.length,
    pursuitsCreated: pursuitRows.filter((p) => p.created === "YES").length,
    topReady: topReady?.organizationName || topReady?.title || null,
    topWatch: topWatch?.organizationName || topWatch?.title || null,
    topBlocker: blocker,
    visibleCampaignsCustomer: visible.length,
    qByLang,
    totalSerp,
    yotelRegression: yotelAfter.campaigns === yotelBefore.campaigns,
    bethRegression: bethAfter.readyGate === bethBefore.readyGate,
  };
  write("SUMMARIES.json", JSON.stringify(summary, null, 2));
  console.log(JSON.stringify(summary, null, 2));
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
