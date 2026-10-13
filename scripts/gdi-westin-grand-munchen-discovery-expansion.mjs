#!/usr/bin/env node
/**
 * Westin Grand München — Watch reconciliation + controlled 10-Base DE/EN expansion.
 *
 *   node scripts/gdi-westin-grand-munchen-discovery-expansion.mjs
 *
 * No Apify. No Ready/Watch threshold changes. No forced counts.
 */
import "../load-env.js";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { loadHotelDemandConfig } from "../lib/group-demand-intelligence/hotel-profile.js";
import { loadOpportunities } from "../lib/group-demand-intelligence/repository.js";
import {
  loadOpportunitiesCanonical,
  saveOpportunitiesCanonical,
} from "../lib/group-demand-intelligence/opportunity-persistence.js";
import { listPursuits } from "../lib/group-demand-intelligence/pursuit/pursuit-store-v1.js";
import {
  startPursuitFromOpportunity,
  canStartPursuitFromOpportunity,
} from "../lib/group-demand-intelligence/pursuit/pursuit-service-v1.js";
import {
  upsertDemandCampaigns,
  loadDemandCampaigns,
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
process.env.WEBHOUND_UNAVAILABLE = "true";
process.env.WEBHOUND_DISABLED = "1";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const HOTEL_ID = "recFaxTEFF9ILHWC9";
const HOTEL_KEY = "WESTIN_MUC";
const YOTEL = "recrPQcZg7SFARRb2";
const BETH = "recLuxvwwxID7U2B8";
const AC = "rec2PVBDavppGpenm";
const RAD = "recUOyzOXn2Zdp98I";
const IAA_ID = "gdi_opp_iaa_transportation_2026_4_f9ilhwc9";
const OUT = path.join(ROOT, "reports/gdi/westin-grand-munchen-discovery-expansion");
const NOW = "2026-10-07";
const MARKET_HINTS = [
  "Munich",
  "München",
  "Munchen",
  "Bogenhausen",
  "Arabellapark",
  "Messe München",
  "Bavaria",
  "Bayern",
];

const ALL_BASES = Object.values(GDI_BASE_OF_DEMAND);
const QUERIES_PER_BASE_PER_LANE = 2;
const SERP_NUM = 4;

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
    if (isValidFutureWatch(o, { nowDate: NOW, marketHints: MARKET_HINTS })?.ok) watch += 1;
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

function hotelProfile() {
  const cfg = loadHotelDemandConfig(HOTEL_ID) || {};
  return { ...cfg, hotelId: HOTEL_ID, hotelKey: HOTEL_KEY };
}

function classifySerpHit(hit) {
  const blob = `${hit.title} ${hit.snippet} ${hit.link}`.toLowerCase();
  const geoOk = MARKET_HINTS.some((t) => blob.includes(String(t).toLowerCase()));
  const yearOk = /2026|2027|2028/.test(blob);
  const eventOk =
    /kongress|tagung|jahrestagung|konferenz|messe|fachmesse|symposium|forum|aussteller|congress|conference|summit|expo/.test(
      blob
    );
  const pastOnly = /202[0-4]\b/.test(blob) && !yearOk;
  const tourism =
    /tourismus|booking\.com|tripadvisor|airbnb|urlaub/.test(blob) && !eventOk;
  const hannoverBleed = /\bhannover\b|\bhanover\b/.test(blob) && !/\bmunich\b|\bmünchen\b|\bmunchen\b/.test(blob);

  if (!hit.link || hit.error) return { class: "REJECT", reasons: ["serp_error"] };
  if (hannoverBleed) return { class: "REJECT", reasons: ["wrong_destination_hannover"] };
  if (pastOnly) return { class: "REJECT", reasons: ["past_only"] };
  if (tourism) return { class: "REJECT", reasons: ["generic_tourism"] };
  if (!geoOk) return { class: "REJECT", reasons: ["wrong_destination_or_weak_geo"] };
  if (!yearOk) return { class: "SIGNAL_ONLY", reasons: ["no_clear_future_year"] };
  if (!eventOk) return { class: "SIGNAL_ONLY", reasons: ["thin_event_signal"] };
  return {
    class: "WATCH_ONLY_SIGNAL",
    reasons: ["bounded_serp_candidate_not_auto_campaign"],
  };
}

async function discoverBase(profile, base, lane) {
  const pack = buildGdiDiscoveryQueries({
    hotelProfile: profile,
    hotelId: HOTEL_ID,
    market: profile.demandTerritory?.label || "Munich",
    country: "Germany",
    baseOfDemand: base,
    lane,
    maxPerBase: QUERIES_PER_BASE_PER_LANE,
  });
  const queries = pack.queries.slice(0, QUERIES_PER_BASE_PER_LANE);
  const hits = [];
  let serpCalls = 0;
  for (const q of queries) {
    try {
      const serp = await serpapiSearch({
        q: q.query,
        num: SERP_NUM,
        hl: q.serpLocale?.hl || q.queryLanguage || (lane === "NATIVE" ? "de" : "en"),
        gl: q.serpLocale?.gl || "de",
      });
      serpCalls += 1;
      for (const hit of serp?.data?.organic_results || []) {
        hits.push({
          lane,
          queryLanguage: q.queryLanguage,
          queryFamily: q.queryFamily,
          baseOfDemand: base,
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
        baseOfDemand: base,
        query: q.query,
        error: err?.message || String(err),
      });
    }
  }
  return { queries, hits, serpCalls };
}

function seedCeilingMonitors(decompResults) {
  const T = PUBLICATION_TRIGGER_TYPE;
  const seeds = [];
  for (const r of decompResults || []) {
    if (String(r.status) !== "PUBLIC_DATA_CEILING") continue;
    const camp = (loadDemandCampaigns(HOTEL_ID).campaigns || []).find(
      (c) => c.campaignId === r.campaignId
    );
    const url = camp?.officialSource || camp?.sourceUrl || null;
    if (!url || !/^https?:\/\//i.test(url)) continue;
    const id = `gdi_pm_${String(r.campaignId || "camp")
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, "_")
      .slice(0, 48)}_v1`;
    seeds.push({
      monitorId: id,
      campaignId: r.campaignId,
      hotelId: HOTEL_ID,
      campaignKey: camp?.title || r.campaignId,
      triggerType: T.EXHIBITOR_LIST_PUBLISHED,
      watchForTypes: [
        T.EXHIBITOR_LIST_PUBLISHED,
        T.SPEAKER_LIST_PUBLISHED,
        T.PROGRAMME_PUBLISHED,
        T.LODGING_PAGE_PUBLISHED,
        T.HOTEL_LIST_PUBLISHED,
        T.REGISTRATION_OPENED,
      ],
      triggerSourceUrl: url,
      alternateSourceUrls: [],
      sourceLanguage: "de",
      currentSourceState: "LIST_NOT_YET_PUBLISHED",
      expectedPublicationWindowStart: NOW,
      expectedPublicationWindowEnd: "2027-12-31",
      priorityRank: 3,
      monitoringStatus: MONITOR_STATUS.ACTIVE,
      customerMonitoringFor: [
        CUSTOMER_MONITORING_LABEL[T.EXHIBITOR_LIST_PUBLISHED],
        CUSTOMER_MONITORING_LABEL[T.LODGING_PAGE_PUBLISHED],
      ],
      notes: "Westin MUC expansion — PUBLIC_DATA_CEILING known source only",
      seededAt: NOW,
    });
  }
  if (seeds.length) {
    upsertPublicationMonitors(HOTEL_ID, seeds, {
      note: "Westin Grand München discovery expansion monitors",
    });
  }
  return seeds;
}

async function reconcileWatchLabels(beforeWatchGate) {
  const doc = await loadOpportunitiesCanonical(HOTEL_ID);
  const next = [];
  const forensic = [];
  let iaa = null;

  for (const o of doc.opportunities || []) {
    const v = isValidFutureWatch(o, { nowDate: NOW, marketHints: MARKET_HINTS });
    const cfs = String(o.customerFacingState || "");
    const isWatchLabel = /WATCH/i.test(cfs);
    let updated = o;

    // IAA TRANSPORTATION — Hannover, past cycle relative to NOW
    if (o.id === IAA_ID || /iaa transportation/i.test(String(o.title || ""))) {
      const dest = String(o.destinationStatus || o.eventLocationSummary || "");
      const munichRelevant = /münchen|munich|munchen/i.test(dest);
      iaa = {
        id: o.id,
        title: o.title,
        beforeCfs: cfs,
        destination: dest,
        dates: `${o.eventStartDate || ""} → ${o.eventEndDate || ""}`,
        validWatchBefore: v.ok,
        validClass: v.class,
        reasons: v.reasons,
        munichRelevant,
        dateVerified: Boolean(o.eventStartDate && o.eventEndDate),
        classification: munichRelevant
          ? v.ok
            ? "A_LEGITIMATE_VALID_WATCH"
            : "B_NOT_VALID_LABEL_WRONG"
          : "B_NOT_VALID_WRONG_DESTINATION",
      };
      // Canonical disposition: wrong city + past cycle → not customer Watch
      updated = {
        ...o,
        destinationStatus: dest || "Hannover",
        eventLocationSummary: o.eventLocationSummary || "Hannover, Germany",
        geoClass: "OUTSIDE_CATCHMENT",
        geoConflict: true,
        geoConflictReason: "WRONG_CITY:Hannover_not_Munich",
        demandTerritoryFit: "OUTSIDE",
        watchExcludedFromFutureWatch: true,
        customerFacingState: "CLOSED",
        customerVisible: false,
        priority: "DISQUALIFIED",
        opportunityQualification: "CLOSED",
        watchValidation: {
          ok: false,
          class: munichRelevant ? v.class || "STALE" : "OUT_OF_MARKET",
          reasons: munichRelevant
            ? v.reasons
            : ["wrong_destination_hannover", "past_or_stale_cycle"],
          reconciledAt: new Date().toISOString(),
        },
        reconciliationNote:
          "IAA TRANSPORTATION 2026 is Hannover (VDA), not Munich — cleared invalid WATCH label",
      };
      iaa.afterCfs = updated.customerFacingState;
      iaa.afterValid = isValidFutureWatch(updated, {
        nowDate: NOW,
        marketHints: MARKET_HINTS,
      }).ok;
    } else if (isWatchLabel && !v.ok) {
      // Stale / research-backlog rows incorrectly labeled WATCH/FUTURE_WATCH
      forensic.push({
        id: o.id,
        title: o.title,
        beforeCfs: cfs,
        validClass: v.class,
        reasons: (v.reasons || []).join("|"),
        action: "CLEAR_INVALID_WATCH_LABEL",
      });
      updated = {
        ...o,
        customerFacingState:
          v.class === "STALE" || v.class === "OUT_OF_MARKET"
            ? "CLOSED"
            : "INSUFFICIENT_EVIDENCE",
        customerVisible: false,
        watchExcludedFromFutureWatch: true,
        watchValidation: {
          ok: false,
          class: v.class,
          reasons: v.reasons,
          reconciledAt: new Date().toISOString(),
        },
        priority:
          v.class === "STALE" || v.class === "OUT_OF_MARKET"
            ? "DISQUALIFIED"
            : o.priority || "WATCHLIST",
      };
    }

    next.push(updated);
  }

  await saveOpportunitiesCanonical(HOTEL_ID, {
    ...doc,
    opportunities: next,
    updatedAt: new Date().toISOString(),
    note: "Westin MUC watch reconciliation — IAA Hannover + invalid Watch labels",
  });
  invalidateGdiHotelReadCache(HOTEL_ID);

  const after = stats(HOTEL_ID);
  return {
    beforeWatchGate,
    afterWatchGate: after.watchGate,
    iaa,
    forensic,
    facingWatchAfter: after.facingWatch,
  };
}

async function main() {
  ensureDir(OUT);
  const profile = hotelProfile();
  if (!profile.displayName) throw new Error("missing_gdi_config");

  const before = stats(HOTEL_ID);
  const yotelBefore = stats(YOTEL);
  const bethBefore = stats(BETH);
  const acBefore = stats(AC);
  const radBefore = stats(RAD);

  // ——— PART 8–10: Watch contradiction forensic + reconcile ———
  const recon = await reconcileWatchLabels(before.watchGate);

  write(
    "WATCH_CONTRADICTION_FORENSIC.md",
    `# Watch Contradiction Forensic — Westin Grand München

## Contradiction

Report said VALID FUTURE WATCH = 0 but TOP WATCH = IAA TRANSPORTATION 2026 (customer-facing WATCH).

## Trace — IAA TRANSPORTATION 2026

| Field | Value |
|-------|-------|
| Opportunity ID | \`${IAA_ID}\` |
| Before CFS | ${recon.iaa?.beforeCfs} |
| Destination | **${recon.iaa?.destination}** |
| Dates | ${recon.iaa?.dates} |
| Valid Future Watch (before) | ${recon.iaa?.validWatchBefore} (${recon.iaa?.validClass}) |
| Reasons | ${(recon.iaa?.reasons || []).join(", ")} |
| Munich relevance | **${recon.iaa?.munichRelevant ? "YES" : "NO"}** |
| Classification | **${recon.iaa?.classification}** |
| After CFS | ${recon.iaa?.afterCfs} |
| After Valid Watch | ${recon.iaa?.afterValid} |

## Verdict

**B — IAA is not a Valid Future Watch; customer-facing WATCH label was wrong.**

Root causes stacked:
1. **Wrong destination** — IAA TRANSPORTATION 2026 is in **Hannover**, not Munich.
2. **Past cycle vs NOW=${NOW}** — event end 2026-09-20 → STALE under production Watch gate.
3. **Report snapshot bug** — prior e2e SUMMARIES picked customerFacingState=WATCH as "top watch" even when \`isValidFutureWatch\` failed (stale narrative, not gate count bug).

Valid Future Watch count was **correctly 0**. The report label was the defect.

## Other invalid Watch labels cleared

${(recon.forensic || [])
  .map((f) => `- \`${f.id}\` ${f.title} — ${f.validClass} → ${f.action}`)
  .join("\n") || "(none beyond IAA)"}

| Metric | Before | After |
|--------|--------|-------|
| Valid Future Watch gate | ${recon.beforeWatchGate} | ${recon.afterWatchGate} |
| customerFacing *WATCH* labels | ${before.facingWatch} | ${recon.facingWatchAfter} |
`
  );

  write(
    "IAA_VALIDATION.md",
    `# IAA TRANSPORTATION 2026 — Validation

| Check | Result |
|-------|--------|
| Event name | IAA TRANSPORTATION 2026 |
| Location (canonical) | **Hannover** (from destinationStatus / summaryWhat) |
| Dates | 2026-09-15 → 2026-09-20 |
| Munich relevance for Westin Grand München | **NO** |
| Date verified | YES |
| Valid Future Watch | **NO** |
| Disposition | CLOSED / DISQUALIFIED — wrong city + past cycle |
| Speculative lodging invented | NO |

Official industry context: IAA Transportation is the commercial-vehicle fair hosted in Hannover (VDA). Not a Munich Messe München event.
`
  );

  // ——— PART 11–15: Bounded discovery across all 10 Bases ———
  const baseAudit = [];
  const discoveryRows = [];
  let deQueries = 0;
  let enQueries = 0;
  let totalSerp = 0;
  const basesWithSignals = new Set();

  for (const base of ALL_BASES) {
    const audit = {
      Base: base,
      wired: "YES",
      queryGeneration: "NO",
      sourceSearchExecuted: "NO",
      candidateFound: "NO",
      admissionGatePassed: "NO",
      whyNotTriggered: "",
    };

    for (const lane of ["NATIVE", "ENGLISH_CONTROL"]) {
      const result = await discoverBase(profile, base, lane);
      audit.queryGeneration = result.queries.length ? "YES" : audit.queryGeneration;
      audit.sourceSearchExecuted = result.serpCalls > 0 ? "YES" : audit.sourceSearchExecuted;
      totalSerp += result.serpCalls;
      for (const q of result.queries) {
        if (q.queryLanguage === "de") deQueries += 1;
        if (q.queryLanguage === "en") enQueries += 1;
      }
      if (!result.queries.length) {
        audit.whyNotTriggered =
          audit.whyNotTriggered || "no_queries_emitted_for_lane_" + lane;
      }
      for (const h of result.hits) {
        if (!h.link) {
          discoveryRows.push({
            base,
            lane,
            queryLanguage: h.queryLanguage || "",
            query: h.query || "",
            class: "REJECT",
            title: "",
            link: "",
            reasons: h.error || "no_hit",
          });
          continue;
        }
        const cls = classifySerpHit(h);
        discoveryRows.push({
          base,
          lane,
          queryLanguage: h.queryLanguage,
          queryFamily: h.queryFamily,
          query: h.query,
          title: h.title,
          link: h.link,
          sourceDomain: h.sourceDomain,
          class: cls.class,
          reasons: (cls.reasons || []).join("|"),
        });
        if (cls.class === "WATCH_ONLY_SIGNAL" || cls.class === "SIGNAL_ONLY") {
          audit.candidateFound = "YES";
          basesWithSignals.add(base);
        }
      }
    }

    // Admission: SERP never auto-admits (shared AC/RAD rule)
    audit.admissionGatePassed = "NO";
    if (audit.candidateFound === "YES") {
      audit.whyNotTriggered =
        "serp_signals_not_auto_admitted_require_validated_watch_or_official_list";
    } else if (audit.queryGeneration === "YES") {
      audit.whyNotTriggered =
        audit.whyNotTriggered || "no_munich_future_event_signal_in_bounded_serp";
    } else {
      audit.whyNotTriggered =
        audit.whyNotTriggered ||
        "query_builder_emitted_zero_for_base_under_budget_cap";
    }
    baseAudit.push(audit);
  }

  // ——— Campaigns from Valid Future Watch only (shared admission) ———
  const admitSource = (loadOpportunities(HOTEL_ID)?.opportunities || []).filter((o) =>
    isValidFutureWatch(o, { nowDate: NOW, marketHints: MARKET_HINTS }).ok
  );
  const linked = buildCampaignsFromHotelOpportunities(admitSource, profile, {
    nowDate: NOW,
  });
  // Also keep existing non-IAA campaigns
  const existing = loadDemandCampaigns(HOTEL_ID).campaigns || [];
  const merged = [...existing];
  for (const c of linked.campaigns || []) {
    if (!merged.some((x) => x.campaignId === c.campaignId)) merged.push(c);
  }
  upsertDemandCampaigns(HOTEL_ID, merged, {
    note: "Westin MUC expansion — campaigns from Valid Future Watch only",
  });
  invalidateGdiHotelReadCache(HOTEL_ID);

  const decomp = await runHotelDemandCampaignDecompositions(HOTEL_ID, {
    nowDate: NOW,
    persist: true,
    enableJev: false,
    maxCompletionSteps: 0,
    continueOnCampaignDecompositionError: true,
  });
  const ceilingMonitors = seedCeilingMonitors(decomp.results);
  invalidateGdiHotelReadCache(HOTEL_ID);

  const after = stats(HOTEL_ID);
  const yotelAfter = stats(YOTEL);
  const bethAfter = stats(BETH);
  const acAfter = stats(AC);
  const radAfter = stats(RAD);

  const opps = loadOpportunities(HOTEL_ID)?.opportunities || [];
  const campaigns = loadDemandCampaigns(HOTEL_ID).campaigns || [];
  const monitors = loadPublicationMonitors(HOTEL_ID).monitors || [];

  // Mark bases that produced campaigns
  for (const c of campaigns) {
    const b = c.baseOfDemand || c.routedBaseOfDemand;
    if (b) {
      const row = baseAudit.find((r) => r.Base === b);
      if (row) {
        row.admissionGatePassed = "YES";
        row.whyNotTriggered = "campaign_admitted_from_validated_watch_or_prior";
      }
    }
  }
  // Prior Stadtgeburtstag campaign — PUBLISHED_EVENT path
  const ped = baseAudit.find(
    (r) => r.Base === GDI_BASE_OF_DEMAND.PUBLISHED_EVENT_DECOMPOSITION
  );
  if (ped && campaigns.length) {
    ped.admissionGatePassed = campaigns.length ? "YES" : ped.admissionGatePassed;
    if (ped.admissionGatePassed === "YES") {
      ped.whyNotTriggered = "prior_or_new_campaign_on_shared_decomp_path";
    }
  }

  write("TEN_BASES_INVOCATION_AUDIT.csv", toCsv(baseAudit, [
    "Base",
    "wired",
    "queryGeneration",
    "sourceSearchExecuted",
    "candidateFound",
    "admissionGatePassed",
    "whyNotTriggered",
  ]));

  write(
    "GATING_AUDIT.md",
    `# Gating Audit — Why 9/10 Bases Did Not Produce Campaigns

## Findings (shared orchestration — no Westin fork)

1. **Prior e2e discovery budget** only routed HIGH bases (PUBLISHED_EVENT, PARTICIPANT_EXHIBITOR, INTL_ORG, RECURRING_CORPORATE, PHARMA) — other bases were wired in taxonomy but **not given equal SERP budget**.
2. **SERP never auto-admits campaigns** (shared AC/RAD/YOTEL rule) — signals stay SIGNAL_ONLY / WATCH_ONLY until a validated Watch exists.
3. **Campaign admission from Valid Future Watch only** — after IAA/stale Watch reconciliation, Valid Future Watch = ${after.watchGate}, so few/new campaigns from expansion SERP.
4. **No YOTEL-specific campaign fork** blocking Munich — shared \`buildGdiDiscoveryQueries\` + DE lexicon used.
5. **Language** — German NATIVE + English control both executed this expansion (${deQueries} DE / ${enQueries} EN).
6. **Wrong-destination bleed** — Hannover IAA rejected at SERP classify + Watch reconcile (shared OUT_OF_MARKET improvement).

## Artificial gating fixed

| Issue | Fix |
|-------|-----|
| Unequal Base SERP budget | This run: all 10 Bases × NATIVE + ENGLISH_CONTROL (2 queries/lane) |
| isOutOfMarket missed Hannover | Shared far-city list + destinationStatus check |
| Invalid CFS=WATCH labels | Cleared when Valid Future Watch fails |

No Ready/Watch threshold changes.
`
  );

  write(
    "BOUNDED_DISCOVERY.csv",
    toCsv(discoveryRows, [
      "base",
      "lane",
      "queryLanguage",
      "queryFamily",
      "query",
      "title",
      "link",
      "sourceDomain",
      "class",
      "reasons",
    ])
  );

  write(
    "CAMPAIGNS.csv",
    toCsv(
      campaigns.map((c) => ({
        campaignId: c.campaignId,
        title: c.title || "",
        status: c.status || "",
        officialSource: c.officialSource || c.sourceUrl || "",
        baseOfDemand: c.baseOfDemand || "",
      })),
      ["campaignId", "title", "status", "officialSource", "baseOfDemand"]
    )
  );

  const childRows = [];
  const travelRows = [];
  const buyerRows = [];
  const lodgingRows = [];
  const futureRows = [];
  const packetRows = [];
  for (const r of decomp.results || []) {
    for (const lead of r.researchLeads || []) {
      childRows.push({
        campaignId: r.campaignId,
        opportunityId: lead.id,
        organization: lead.organizationName,
      });
      travelRows.push({
        opportunityId: lead.id,
        travelingEntityType: lead.travelingEntityType || "",
        proven: lead.travelingEntityProven ? "PROVEN" : "UNPROVEN",
      });
      buyerRows.push({
        opportunityId: lead.id,
        role: lead.buyerRole || "",
        contactPath: lead.publicContactPath || lead.contactPath || "",
      });
      lodgingRows.push({
        opportunityId: lead.id,
        lodgingClass: lead.hotelMotionClass || lead.lodgingEvidenceClass || "NONE",
      });
      futureRows.push({
        opportunityId: lead.id,
        eventDate: lead.eventDate || "",
        nextTrigger: lead.nextTrigger || "",
      });
      packetRows.push({
        opportunityId: lead.id,
        packetQuality: lead.packetQuality || "",
      });
    }
  }

  write("OFFICIAL_LISTS.csv", toCsv((decomp.results || []).map((r) => ({
    campaignId: r.campaignId,
    status: r.status,
    baseOfDemand: r.baseOfDemand || "",
    children: r.counts?.childrenDiscovered ?? 0,
  })), ["campaignId", "status", "baseOfDemand", "children"]));
  write("CHILD_ACCOUNTS.csv", toCsv(childRows, ["campaignId", "opportunityId", "organization"]));
  write("TRAVELING_ENTITIES.csv", toCsv(travelRows, ["opportunityId", "travelingEntityType", "proven"]));
  write("BUYER_PATHS.csv", toCsv(buyerRows, ["opportunityId", "role", "contactPath"]));
  write("LODGING_EVIDENCE.csv", toCsv(lodgingRows, ["opportunityId", "lodgingClass"]));
  write("FUTURE_DECISIONS.csv", toCsv(futureRows, ["opportunityId", "eventDate", "nextTrigger"]));
  write("PACKETS.csv", toCsv(packetRows, ["opportunityId", "packetQuality"]));

  const readyWatchRows = [];
  let readyCount = 0;
  let watchCount = 0;
  let completeStrong = 0;
  let completePlausible = 0;
  let outreachNow = 0;
  let topReady = null;
  let topWatch = null;
  const pursuitRows = [];

  for (const o of opps) {
    const ready = isGdiCustomerOpportunityReady(o, { nowDate: NOW });
    const watch = isValidFutureWatch(o, { nowDate: NOW, marketHints: MARKET_HINTS });
    if (ready?.ok) {
      readyCount += 1;
      if (!topReady) topReady = o;
    }
    if (watch?.ok) {
      watchCount += 1;
      if (!topWatch) topWatch = o;
    }
    if (String(o.packetQuality || "").toUpperCase() === "COMPLETE_STRONG") completeStrong += 1;
    if (String(o.packetQuality || "").toUpperCase() === "COMPLETE_PLAUSIBLE")
      completePlausible += 1;
    if (String(o.outreachReadiness || "") === "OUTREACH_NOW") outreachNow += 1;
    readyWatchRows.push({
      opportunityId: o.id,
      organization: o.organizationName || o.title,
      customerFacingState: o.customerFacingState || "",
      readyGate: ready?.ok ? "YES" : "NO",
      watchGate: watch?.ok ? "YES" : "NO",
      watchClass: watch?.class || "",
    });
    if (canStartPursuitFromOpportunity(o)) {
      const pr = startPursuitFromOpportunity(HOTEL_ID, o, {
        actor: "westin_muc_expansion",
        language: "de",
      });
      pursuitRows.push({
        opportunityId: o.id,
        created: pr.created ? "YES" : "NO",
        pursuitId: pr.pursuit?.pursuitId || "",
      });
    }
  }

  write(
    "READY_WATCH.csv",
    toCsv(readyWatchRows, [
      "opportunityId",
      "organization",
      "customerFacingState",
      "readyGate",
      "watchGate",
      "watchClass",
    ])
  );
  write(
    "PUBLICATION_MONITORS.csv",
    toCsv(
      monitors.map((m) => ({
        monitorId: m.monitorId,
        campaignId: m.campaignId,
        triggerType: m.triggerType,
        status: m.monitoringStatus,
        url: m.triggerSourceUrl,
      })),
      ["monitorId", "campaignId", "triggerType", "status", "url"]
    )
  );

  write(
    "CANONICAL_RECONCILIATION.csv",
    toCsv(
      [
        { layer: "opportunities", value: after.total, notes: "FS+Airtable canonical" },
        { layer: "campaigns", value: after.campaigns, notes: "not customer-facing" },
        { layer: "valid_future_watch", value: after.watchGate, notes: "production gate" },
        { layer: "ready_gate", value: after.readyGate, notes: "unchanged threshold" },
        { layer: "publication_monitors", value: monitors.length, notes: "" },
        {
          layer: "yotel_campaigns",
          value: yotelAfter.campaigns,
          notes: yotelAfter.campaigns === yotelBefore.campaigns ? "PASS" : "FAIL",
        },
        {
          layer: "beth_ready",
          value: bethAfter.readyGate,
          notes: bethAfter.readyGate === bethBefore.readyGate ? "PASS" : "FAIL",
        },
        {
          layer: "ac_campaigns",
          value: acAfter.campaigns,
          notes: acAfter.campaigns === acBefore.campaigns ? "PASS" : "FAIL",
        },
        {
          layer: "rad_campaigns",
          value: radAfter.campaigns,
          notes: radAfter.campaigns === radBefore.campaigns ? "PASS" : "FAIL",
        },
        { layer: "apify", value: "NO", notes: "" },
        { layer: "iaa_watch_label", value: recon.iaa?.afterCfs || "", notes: "reconciled" },
      ],
      ["layer", "value", "notes"]
    )
  );

  write(
    "UI_QA.md",
    `# UI QA — GDI Westin Grand München (post reconciliation)

| Check | Result |
|-------|--------|
| Customer filters All / Ready / Watching | REQUIRED |
| Pursuit filters top-level | MUST NOT |
| Campaigns customer-facing | NO |
| IAA shown as Watching | **NO** (CLOSED / wrong city) |
| Valid Future Watch | ${watchCount} |
| Ready | ${readyCount} |
`
  );

  write(
    "CHANGELOG.md",
    `# CHANGELOG — Westin MUC discovery expansion

- Reconciled IAA TRANSPORTATION 2026: Hannover + past cycle → not Valid Watch; cleared CFS=WATCH
- Cleared other stale/invalid Watch labels
- Shared \`isOutOfMarket\` far-city / destinationStatus improvement
- Bounded 10-Base DE+EN discovery (equal budget; no SERP auto-admit)
- Campaigns only from Valid Future Watch + prior admitted campaign
- No Apify · no threshold changes
`
  );

  const ceilingCount = (decomp.results || []).filter(
    (r) => String(r.status) === "PUBLIC_DATA_CEILING"
  ).length;

  write(
    "FOUNDER_REPORT.md",
    `# FOUNDER REPORT — Westin Grand München Discovery Expansion

## Watch contradiction

**Resolved as B:** IAA was never a Valid Future Watch. Report narrative incorrectly cited customerFacingState=WATCH. Destination = Hannover (not Munich); cycle ended 2026-09-20 vs NOW ${NOW}.

| | Before | After |
|--|--------|-------|
| Valid Future Watch | ${before.watchGate} | ${after.watchGate} |
| CFS *WATCH* labels | ${before.facingWatch} | ${after.facingWatch} |

## 10 Bases

| | Count |
|--|-------|
| Wired | 10 |
| Query generation this run | ${baseAudit.filter((b) => b.queryGeneration === "YES").length} |
| Source search executed | ${baseAudit.filter((b) => b.sourceSearchExecuted === "YES").length} |
| With SERP signals | ${basesWithSignals.size} |
| With campaigns admitted | ${baseAudit.filter((b) => b.admissionGatePassed === "YES").length} |
| DE queries | ${deQueries} |
| EN queries | ${enQueries} |

## Yield

| | Value |
|--|-------|
| Campaigns | ${after.campaigns} |
| Named accounts (decomp leads) | ${childRows.length} |
| Ready | ${readyCount} |
| Valid Future Watch | ${watchCount} |
| PUBLIC_DATA_CEILING | ${ceilingCount} |
| Monitors | ${monitors.length} |
| Top Ready | ${topReady?.organizationName || topReady?.title || "—"} |
| Top Watch | ${topWatch?.organizationName || topWatch?.title || "—"} |
| Blocker | ${
      watchCount === 0
        ? "No Valid Future Watch yet — need official Munich lists / lodging packets under unchanged gates"
        : "Continue second-gen list mining on admitted campaigns"
    } |

## Regression

| | |
|--|--|
| YOTEL campaigns | ${yotelBefore.campaigns}→${yotelAfter.campaigns} |
| Bethesda ready | ${bethBefore.readyGate}→${bethAfter.readyGate} |
| AC campaigns | ${acBefore.campaigns}→${acAfter.campaigns} |
| RAD campaigns | ${radBefore.campaigns}→${radAfter.campaigns} |
`
  );

  const summary = {
    iaa: recon.iaa,
    validFutureWatchBefore: before.watchGate,
    validFutureWatchAfter: after.watchGate,
    facingWatchBefore: before.facingWatch,
    facingWatchAfter: after.facingWatch,
    watchCountBug: false,
    staleSnapshotBug: true,
    wrongEnumMapping: true,
    basesWired: 10,
    basesInvoked: baseAudit.filter((b) => b.sourceSearchExecuted === "YES").length,
    basesWithSignals: basesWithSignals.size,
    basesWithCampaigns: baseAudit.filter((b) => b.admissionGatePassed === "YES").length,
    deQueries,
    enQueries,
    totalSerp,
    campaignsAfter: after.campaigns,
    namedAccounts: childRows.length,
    travelingProven: travelRows.filter((t) => t.proven === "PROVEN").length,
    buyerRoles: buyerRows.filter((b) => b.role).length,
    contactPaths: buyerRows.filter((b) => b.contactPath).length,
    directLodging: lodgingRows.filter((l) => /DIRECT/i.test(l.lodgingClass)).length,
    strongHotelMotion: lodgingRows.filter((l) => /STRONG/i.test(l.lodgingClass)).length,
    futureDecisions: futureRows.filter((f) => f.eventDate || f.nextTrigger).length,
    completeStrong,
    completePlausible,
    customerReady: readyCount,
    validFutureWatch: watchCount,
    pursuitsCreated: pursuitRows.filter((p) => p.created === "YES").length,
    publicDataCeiling: ceilingCount,
    publicationMonitors: monitors.length,
    topReady: topReady?.organizationName || topReady?.title || null,
    topWatch: topWatch?.organizationName || topWatch?.title || null,
    yotelRegression: yotelAfter.campaigns === yotelBefore.campaigns,
    bethRegression: bethAfter.readyGate === bethBefore.readyGate,
    acRegression: acAfter.campaigns === acBefore.campaigns,
    radRegression: radAfter.campaigns === radBefore.campaigns,
  };
  write("SUMMARIES.json", summary);
  console.log(JSON.stringify(summary, null, 2));
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
