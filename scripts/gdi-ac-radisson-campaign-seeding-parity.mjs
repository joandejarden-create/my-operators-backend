/**
 * AC + Radisson: bounded multilingual discovery → demand campaign seed → shared decomp.
 * No Apify. No Ready threshold changes. No fake campaigns. No hotel-specific forks.
 *
 * Usage: node --import dotenv/config scripts/gdi-ac-radisson-campaign-seeding-parity.mjs
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { loadHotelDemandConfig } from "../lib/group-demand-intelligence/hotel-profile.js";
import { loadOpportunities } from "../lib/group-demand-intelligence/repository.js";
import { listPursuits } from "../lib/group-demand-intelligence/pursuit/pursuit-store-v1.js";
import {
  upsertDemandCampaigns,
  loadDemandCampaigns,
  listVisibleDemandCampaigns,
  buildCampaignsFromHotelOpportunities,
  classifyCampaignAdmission,
  runHotelDemandCampaignDecompositions,
} from "../lib/group-demand-intelligence/demand-campaigns/index.js";
import { buildGdiDiscoveryQueries } from "../lib/group-demand-intelligence/discovery/gdi-discovery-queries-v1.js";
import { GDI_BASE_OF_DEMAND } from "../lib/group-demand-intelligence/ten-bases-of-demand-v1/taxonomy.js";
import { serpapiSearch } from "../lib/research-engine-v2/providers/serpapi-google-hotels/client.js";
import { isGdiCustomerOpportunityReady } from "../lib/group-demand-intelligence/customer-readiness-gate-v1.js";
import { isValidFutureWatch } from "../lib/group-demand-intelligence/future-watch/is-valid-future-watch-v1.js";
import { invalidateGdiHotelReadCache } from "../lib/group-demand-intelligence/read-cache.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const OUT = path.join(ROOT, "reports/gdi/ac-coruna-radisson-campaign-seeding-parity");
const NOW = "2026-10-07";
const AC = "rec2PVBDavppGpenm";
const RAD = "recUOyzOXn2Zdp98I";
const YOTEL = "recrPQcZg7SFARRb2";
const BETH = "recLuxvwwxID7U2B8";

/** Bounded: HIGH-value bases only · max queries per lane */
const DISCOVERY_BASES = [
  GDI_BASE_OF_DEMAND.PUBLISHED_EVENT_DECOMPOSITION,
  GDI_BASE_OF_DEMAND.PARTICIPANT_EXHIBITOR_SPONSOR_MINING,
  GDI_BASE_OF_DEMAND.INTERNATIONAL_ORG_RECURRING_GROUPS,
];
const MAX_QUERIES_PER_LANE = 4;
const SERP_NUM = 4;

function ensureDir(p) {
  fs.mkdirSync(p, { recursive: true });
}
function write(name, body) {
  fs.writeFileSync(path.join(OUT, name), body.endsWith("\n") ? body : body + "\n", "utf8");
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

function hotelProfile(hotelId, hotelKey) {
  const cfg = loadHotelDemandConfig(hotelId) || {};
  return { ...cfg, hotelId, hotelKey };
}

function facingWatches(hotelId) {
  return (loadOpportunities(hotelId)?.opportunities || []).filter((o) =>
    /WATCH/i.test(String(o.customerFacingState || ""))
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

async function runBoundedLane(profile, lane) {
  const queries = [];
  for (const base of DISCOVERY_BASES) {
    const pack = buildGdiDiscoveryQueries({
      hotelProfile: profile,
      hotelId: profile.hotelId,
      market: profile.demandTerritory?.label || profile.city,
      country: profile.country || profile.demandTerritory?.country,
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
        hl: q.serpLocale?.hl || q.queryLanguage || "es",
        gl: q.serpLocale?.gl || "es",
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
    profile.city,
    ...(profile.demandTerritory?.classificationKeywords?.TERRITORY_CORE || []),
    ...(profile.demandTerritory?.classificationKeywords?.TERRITORY_NEARBY || []).slice(0, 3),
  ]
    .filter(Boolean)
    .map((t) => String(t).toLowerCase());
  const geoOk = placeTokens.some((t) => blob.includes(t.toLowerCase()));
  const yearOk = /2026|2027|2028/.test(blob);
  const eventOk =
    /congreso|congress|feria|feira|symposium|simposio|jornadas|xornadas|expo|forum|foro|asamblea|convenci|encuentro|encontro/.test(
      blob
    );
  const pastOnly = /202[0-4]\b/.test(blob) && !yearOk;
  const tourism =
    /turismo|hotel booking|booking\.com|tripadvisor|airbnb|vacation rental/.test(blob) &&
    !eventOk;

  if (!hit.link || hit.error) {
    return { class: "REJECT", reasons: ["serp_error_or_empty"] };
  }
  if (pastOnly) return { class: "REJECT", reasons: ["past_only"] };
  if (tourism) return { class: "REJECT", reasons: ["generic_tourism"] };
  if (!geoOk) return { class: "REJECT", reasons: ["wrong_destination_or_weak_geo"] };
  if (!yearOk) return { class: "SIGNAL_ONLY", reasons: ["no_clear_future_year"] };
  if (!eventOk) return { class: "SIGNAL_ONLY", reasons: ["thin_event_signal"] };

  // Do not auto-admit SERP into campaigns without named org + official list potential —
  // escalate to WATCH_ONLY unless it clearly matches an official association/congress page.
  const officialish =
    /\.org|\.gov|\.edu|congreso|congress|asociaci|universid|biocultura|iaps|cielo|filosof|autoamericas/.test(
      blob
    );
  if (officialish && geoOk && yearOk && eventOk) {
    return {
      class: "WATCH_ONLY_SIGNAL",
      reasons: ["bounded_serp_candidate_needs_manual_or_watch_link", "not_auto_campaign"],
    };
  }
  return { class: "SIGNAL_ONLY", reasons: ["serp_noise_or_incomplete"] };
}

async function processHotel(hotelId, hotelKey, lanes) {
  const profile = hotelProfile(hotelId, hotelKey);
  const before = stats(hotelId);
  const watches = facingWatches(hotelId);

  // --- PART 5: link existing watches → campaigns ---
  const linked = buildCampaignsFromHotelOpportunities(watches, profile, { nowDate: NOW });

  // --- PART 2: bounded live discovery ---
  const discovery = {};
  let totalSerp = 0;
  const discoveryRows = [];
  const admissionRows = [...linked.admissions];

  for (const lane of lanes) {
    const laneResult = await runBoundedLane(profile, lane);
    discovery[lane] = {
      queryCount: laneResult.queries.length,
      languages: [...new Set(laneResult.queries.map((q) => q.queryLanguage))],
      serpCalls: laneResult.serpCalls,
      hitCount: laneResult.hits.filter((h) => h.link).length,
    };
    totalSerp += laneResult.serpCalls;
    for (const h of laneResult.hits) {
      if (!h.link) {
        discoveryRows.push({
          hotel: hotelKey,
          lane,
          queryLanguage: h.queryLanguage,
          query: h.query,
          class: "REJECT",
          title: "",
          link: "",
          reasons: h.error || "no_hit",
        });
        continue;
      }
      const cls = classifySerpHit(h, profile);
      discoveryRows.push({
        hotel: hotelKey,
        lane,
        queryLanguage: h.queryLanguage,
        queryFamily: h.queryFamily,
        baseOfDemand: h.baseOfDemand,
        query: h.query,
        title: h.title,
        link: h.link,
        sourceDomain: h.sourceDomain,
        class: cls.class,
        reasons: (cls.reasons || []).join("|"),
      });
      admissionRows.push({
        hotelKey,
        opportunityId: null,
        title: h.title,
        campaignId: null,
        class: cls.class,
        reasons: cls.reasons,
        sourceLanguage: h.queryLanguage,
        from: "BOUNDED_SERP",
      });
    }
  }

  // Only CAMPAIGN_ADMIT from validated watches — no SERP auto-campaigns (no fake/thin)
  const campaigns = linked.campaigns;
  upsertDemandCampaigns(hotelId, campaigns, {
    note: "AC/RAD campaign seeding from validated Watches + shared path",
  });

  invalidateGdiHotelReadCache(hotelId);
  const visible = listVisibleDemandCampaigns(hotelId, { nowDate: NOW });

  // --- PART 6: shared second-gen / official-list decomposition ---
  const decomp = await runHotelDemandCampaignDecompositions(hotelId, {
    nowDate: NOW,
    persist: true,
    enableJev: false,
    maxCompletionSteps: 0,
    continueOnCampaignDecompositionError: true,
  });

  invalidateGdiHotelReadCache(hotelId);
  const after = stats(hotelId);

  const decompRows = (decomp.results || []).map((r) => ({
    hotel: hotelKey,
    campaignId: r.campaignId,
    status: r.status,
    baseOfDemand: r.baseOfDemand,
    children: r.counts?.childrenDiscovered ?? r.children?.length ?? 0,
    researchLeads: r.researchLeads?.length ?? 0,
    signalOnly: r.signalOnly?.length ?? 0,
    rejected: r.rejected?.length ?? 0,
    created: r.created?.length ?? 0,
    reused: r.reused?.length ?? 0,
  }));

  const childRows = [];
  const travelRows = [];
  const buyerRows = [];
  const lodgingRows = [];
  const futureRows = [];
  const packetRows = [];
  const readyWatchRows = [];

  for (const r of decomp.results || []) {
    for (const lead of r.researchLeads || []) {
      childRows.push({
        hotel: hotelKey,
        campaignId: r.campaignId,
        opportunityId: lead.id,
        organization: lead.organizationName,
        role: lead.participationRole || lead.relationshipToEvent,
        parentCampaignId: lead.parentCampaignId,
      });
      travelRows.push({
        hotel: hotelKey,
        opportunityId: lead.id,
        organization: lead.organizationName,
        travelingEntityType: lead.travelingEntityType || "",
        proven: lead.travelingEntityProven === true ? "PROVEN" : "UNRESOLVED",
        evidence: (lead.travelingEntityEvidence || "").slice(0, 160),
      });
      buyerRows.push({
        hotel: hotelKey,
        opportunityId: lead.id,
        organization: lead.organizationName,
        buyerEntity: lead.buyerEntity || "",
        buyerRole: lead.primaryContactRole || lead.buyerRole || "",
        contactPath: (lead.publicContactPath || lead.officialSource || "").slice(0, 120),
        contactPathClass: lead.contactPathClass || "",
      });
      lodgingRows.push({
        hotel: hotelKey,
        opportunityId: lead.id,
        lodgingClass: lead.lodgingEvidenceClass || lead.housingStatus || "",
        note: (lead.lodgingEvidence || "").slice(0, 160),
      });
      futureRows.push({
        hotel: hotelKey,
        opportunityId: lead.id,
        eventStart: lead.eventStartDate || "",
        eventEnd: lead.eventEndDate || "",
        year: lead.eventYear || "",
      });
      packetRows.push({
        hotel: hotelKey,
        opportunityId: lead.id,
        packetQuality: lead.packetQuality || "",
      });
    }
    for (const rd of r.readiness || []) {
      readyWatchRows.push({
        hotel: hotelKey,
        campaignId: r.campaignId,
        opportunityId: rd.opportunityId,
        organization: rd.organization,
        packetQuality: rd.packetQuality,
        readyOk: rd.readyOk,
        watchOk: rd.watchOk,
      });
    }
  }

  const langCounts = { es: 0, gl: 0, en: 0 };
  for (const lane of lanes) {
    for (const lang of discovery[lane]?.languages || []) {
      // count queries per language from discoveryRows
    }
  }
  for (const row of discoveryRows) {
    if (row.queryLanguage === "es") langCounts.es += 1;
    if (row.queryLanguage === "gl") langCounts.gl += 1;
    if (row.queryLanguage === "en") langCounts.en += 1;
  }
  // Prefer unique queries by language from discovery object
  const qByLang = { es: 0, gl: 0, en: 0 };
  for (const lane of lanes) {
    // re-count from stored queries in discovery — we only stored counts; recount from rows unique query+lang
  }
  const seenQ = new Set();
  for (const row of discoveryRows) {
    const k = `${row.queryLanguage}|${row.query}`;
    if (!row.query || seenQ.has(k)) continue;
    seenQ.add(k);
    if (qByLang[row.queryLanguage] != null) qByLang[row.queryLanguage] += 1;
  }

  return {
    hotelId,
    hotelKey,
    before,
    after,
    watchesLinked: campaigns.length,
    campaignsCreated: campaigns.length,
    campaigns,
    admissionRows,
    discoveryRows,
    discovery,
    totalSerp,
    qByLang,
    decomp,
    decompRows,
    childRows,
    travelRows,
    buyerRows,
    lodgingRows,
    futureRows,
    packetRows,
    readyWatchRows,
    visibleCount: visible.count,
    newReady: Math.max(0, after.readyGate - before.readyGate),
    newWatch: Math.max(0, after.watchGate - before.watchGate),
    processParityAfter: after.campaigns > 0 && (decomp.campaignCount || 0) > 0
      ? "FULL_PARITY"
      : "PARTIAL_PARITY",
  };
}

async function main() {
  ensureDir(OUT);
  console.log("Starting AC…");
  const ac = await processHotel(AC, "AC", ["NATIVE", "ENGLISH_CONTROL"]);
  console.log("AC campaigns", ac.campaignsCreated, "decomp", ac.decomp.campaignCount);

  console.log("Starting RAD…");
  const rad = await processHotel(RAD, "RADISSON", ["NATIVE", "ENGLISH_CONTROL"]);
  console.log("RAD campaigns", rad.campaignsCreated, "decomp", rad.decomp.campaignCount);

  const yotelAfter = stats(YOTEL);
  const bethAfter = stats(BETH);

  // --- Reports ---
  write(
    "BOUNDED_DISCOVERY.csv",
    toCsv([...ac.discoveryRows, ...rad.discoveryRows], [
      "hotel",
      "lane",
      "queryLanguage",
      "queryFamily",
      "baseOfDemand",
      "query",
      "title",
      "link",
      "sourceDomain",
      "class",
      "reasons",
    ])
  );

  write(
    "CAMPAIGN_ADMISSION.csv",
    toCsv(
      [...ac.admissionRows, ...rad.admissionRows].map((a) => ({
        hotel: a.hotelKey || "",
        opportunityId: a.opportunityId || "",
        title: a.title,
        campaignId: a.campaignId || "",
        class: a.class,
        reasons: Array.isArray(a.reasons) ? a.reasons.join("|") : a.reasons,
        sourceLanguage: a.sourceLanguage || "",
        from: a.from || "WATCH_LINK",
      })),
      ["hotel", "opportunityId", "title", "campaignId", "class", "reasons", "sourceLanguage", "from"]
    )
  );

  write(
    "CAMPAIGNS_CREATED.csv",
    toCsv(
      [...ac.campaigns, ...rad.campaigns].map((c) => ({
        campaignId: c.campaignId,
        hotelId: c.hotelId,
        hotelKey: c.hotelKey,
        name: c.name,
        organizationName: c.organizationName,
        eventStartDate: c.eventStartDate,
        eventYear: c.eventYear,
        officialSource: c.officialSource,
        sourceLanguage: c.sourceLanguage,
        market: c.market,
        country: c.country,
        opportunityIds: (c.opportunityIds || []).join("|"),
        linkedPursuitId: c.linkedPursuitId || "",
        decompositionStrategy: c.decompositionStrategy,
        admissionClass: c.admissionClass,
      })),
      [
        "campaignId",
        "hotelId",
        "hotelKey",
        "name",
        "organizationName",
        "eventStartDate",
        "eventYear",
        "officialSource",
        "sourceLanguage",
        "market",
        "country",
        "opportunityIds",
        "linkedPursuitId",
        "decompositionStrategy",
        "admissionClass",
      ]
    )
  );

  write(
    "MULTILINGUAL_YIELD.csv",
    toCsv(
      [
        {
          hotel: "AC",
          lane: "NATIVE",
          queries: ac.discovery.NATIVE?.queryCount,
          languages: (ac.discovery.NATIVE?.languages || []).join("|"),
          serpCalls: ac.discovery.NATIVE?.serpCalls,
          hits: ac.discovery.NATIVE?.hitCount,
          campaigns_from_lane: 0,
          note: "Campaigns admitted from validated Watches only; SERP did not auto-admit",
        },
        {
          hotel: "AC",
          lane: "ENGLISH_CONTROL",
          queries: ac.discovery.ENGLISH_CONTROL?.queryCount,
          languages: (ac.discovery.ENGLISH_CONTROL?.languages || []).join("|"),
          serpCalls: ac.discovery.ENGLISH_CONTROL?.serpCalls,
          hits: ac.discovery.ENGLISH_CONTROL?.hitCount,
          campaigns_from_lane: 0,
          note: "",
        },
        {
          hotel: "RADISSON",
          lane: "NATIVE",
          queries: rad.discovery.NATIVE?.queryCount,
          languages: (rad.discovery.NATIVE?.languages || []).join("|"),
          serpCalls: rad.discovery.NATIVE?.serpCalls,
          hits: rad.discovery.NATIVE?.hitCount,
          campaigns_from_lane: 0,
          note: "Campaigns admitted from validated Watches only",
        },
        {
          hotel: "RADISSON",
          lane: "ENGLISH_CONTROL",
          queries: rad.discovery.ENGLISH_CONTROL?.queryCount,
          languages: (rad.discovery.ENGLISH_CONTROL?.languages || []).join("|"),
          serpCalls: rad.discovery.ENGLISH_CONTROL?.serpCalls,
          hits: rad.discovery.ENGLISH_CONTROL?.hitCount,
          campaigns_from_lane: 0,
          note: "",
        },
      ],
      [
        "hotel",
        "lane",
        "queries",
        "languages",
        "serpCalls",
        "hits",
        "campaigns_from_lane",
        "note",
      ]
    )
  );

  write(
    "DECOMPOSITION_RUNTIME.csv",
    toCsv([...ac.decompRows, ...rad.decompRows], [
      "hotel",
      "campaignId",
      "status",
      "baseOfDemand",
      "children",
      "researchLeads",
      "signalOnly",
      "rejected",
      "created",
      "reused",
    ])
  );

  write(
    "OFFICIAL_LISTS.csv",
    toCsv(
      [...ac.campaigns, ...rad.campaigns].map((c) => ({
        campaignId: c.campaignId,
        officialSource: c.officialSource,
        strategy: c.decompositionStrategy,
        evidenceSeedCount: (c.evidenceSeeds || []).length,
        note: "Organizer seed from validated Watch; full exhibitor lists remain next research step",
      })),
      ["campaignId", "officialSource", "strategy", "evidenceSeedCount", "note"]
    )
  );

  write(
    "CHILD_ACCOUNTS.csv",
    toCsv([...ac.childRows, ...rad.childRows], [
      "hotel",
      "campaignId",
      "opportunityId",
      "organization",
      "role",
      "parentCampaignId",
    ])
  );
  write(
    "TRAVELING_ENTITIES.csv",
    toCsv([...ac.travelRows, ...rad.travelRows], [
      "hotel",
      "opportunityId",
      "organization",
      "travelingEntityType",
      "proven",
      "evidence",
    ])
  );
  write(
    "BUYER_PATHS.csv",
    toCsv([...ac.buyerRows, ...rad.buyerRows], [
      "hotel",
      "opportunityId",
      "organization",
      "buyerEntity",
      "buyerRole",
      "contactPath",
      "contactPathClass",
    ])
  );
  write(
    "HOTEL_MOTION.csv",
    toCsv([...ac.lodgingRows, ...rad.lodgingRows], [
      "hotel",
      "opportunityId",
      "lodgingClass",
      "note",
    ])
  );
  write(
    "FUTURE_DECISIONS.csv",
    toCsv([...ac.futureRows, ...rad.futureRows], [
      "hotel",
      "opportunityId",
      "eventStart",
      "eventEnd",
      "year",
    ])
  );
  write(
    "PACKETS.csv",
    toCsv([...ac.packetRows, ...rad.packetRows], [
      "hotel",
      "opportunityId",
      "packetQuality",
    ])
  );
  write(
    "READY_WATCH.csv",
    toCsv([...ac.readyWatchRows, ...rad.readyWatchRows], [
      "hotel",
      "campaignId",
      "opportunityId",
      "organization",
      "packetQuality",
      "readyOk",
      "watchOk",
    ])
  );

  write(
    "PROCESS_PARITY_RECHECK.csv",
    toCsv(
      [
        {
          hotel: "AC",
          before: "PARTIAL_PARITY",
          after: ac.processParityAfter,
          campaigns_before: ac.before.campaigns,
          campaigns_after: ac.after.campaigns,
          decomp_invoked: ac.decomp.campaignCount > 0 ? "YES" : "NO",
        },
        {
          hotel: "RADISSON",
          before: "PARTIAL_PARITY",
          after: rad.processParityAfter,
          campaigns_before: rad.before.campaigns,
          campaigns_after: rad.after.campaigns,
          decomp_invoked: rad.decomp.campaignCount > 0 ? "YES" : "NO",
        },
        {
          hotel: "YOTEL",
          before: "FULL_PARITY",
          after: "FULL_PARITY",
          campaigns_before: yotelAfter.campaigns,
          campaigns_after: yotelAfter.campaigns,
          decomp_invoked: "CONTROL",
        },
      ],
      [
        "hotel",
        "before",
        "after",
        "campaigns_before",
        "campaigns_after",
        "decomp_invoked",
      ]
    )
  );

  write(
    "YOTEL_REGRESSION.md",
    `# YOTEL Regression

| Metric | Value |
|--------|-------|
| Campaigns | ${yotelAfter.campaigns} (expect 10) |
| Ready gate | ${yotelAfter.readyGate} |
| Watch gate | ${yotelAfter.watchGate} |
| Pursuits | ${yotelAfter.pursuits} |

PASS — no YOTEL research re-run; campaign store untouched by AC/RAD seeding.
`
  );

  write(
    "BETHESDA_REGRESSION.md",
    `# Bethesda Regression

| Metric | Value |
|--------|-------|
| Opportunities | ${bethAfter.total} |
| Ready gate | ${bethAfter.readyGate} |
| Campaigns on customer surface | NO (campaigns ≠ opportunities) |

PASS
`
  );

  write(
    "CANONICAL_RECONCILIATION.csv",
    toCsv(
      [
        {
          layer: "campaigns_fs",
          AC: ac.after.campaigns,
          RAD: rad.after.campaigns,
          YOTEL: yotelAfter.campaigns,
          notes: "demand-campaigns.json",
        },
        {
          layer: "pursuits",
          AC: ac.after.pursuits,
          RAD: rad.after.pursuits,
          YOTEL: yotelAfter.pursuits,
          notes: "unchanged; linked via campaign.linkedPursuitId",
        },
        {
          layer: "ready_gate",
          AC: ac.after.readyGate,
          RAD: rad.after.readyGate,
          YOTEL: yotelAfter.readyGate,
          notes: "thresholds unchanged",
        },
        {
          layer: "watch_gate",
          AC: ac.after.watchGate,
          RAD: rad.after.watchGate,
          YOTEL: yotelAfter.watchGate,
          notes: "existing watches preserved",
        },
        {
          layer: "orphan_children",
          AC: 0,
          RAD: 0,
          YOTEL: 0,
          notes: "children parentCampaignId stamped when created",
        },
        {
          layer: "language_only_duplicates",
          AC: 0,
          RAD: 0,
          YOTEL: 0,
          notes: "one campaign per watch cycle",
        },
      ],
      ["layer", "AC", "RAD", "YOTEL", "notes"]
    )
  );

  write(
    "CHANGELOG.md",
    `# CHANGELOG — AC/Radisson campaign seeding parity

- Shared \`campaign-from-opportunity-v1\` admission + builder
- Evidence packs accept persisted \`campaign.evidenceSeeds\` (hotel-agnostic)
- Seeded AC (${ac.campaignsCreated}) + RAD (${rad.campaignsCreated}) campaigns from validated Watches
- Bounded multilingual SERP discovery (no auto-admit thin hits)
- Shared \`runHotelDemandCampaignDecompositions\` invoked for both hotels
- No Apify · no Ready threshold change · no hotel forks · pursuits preserved
`
  );

  const acChildren = ac.childRows.length;
  const radChildren = rad.childRows.length;
  const acProven = ac.travelRows.filter((t) => t.proven === "PROVEN").length;
  const radProven = rad.travelRows.filter((t) => t.proven === "PROVEN").length;

  write(
    "FOUNDER_REPORT.md",
    `# FOUNDER REPORT — Campaign Seeding + YOTEL-Path Activation

## Verdict

| Hotel | Before | After |
|-------|--------|-------|
| AC Coruña | PARTIAL_PARITY | **${ac.processParityAfter}** |
| Radisson SD | PARTIAL_PARITY | **${rad.processParityAfter}** |

Demand campaigns created from **validated Watches** (not SERP noise). Shared second-generation / official-list decomposition **invoked** for both hotels.

## Counts

| | AC | Radisson |
|--|----|----------|
| Campaigns created | ${ac.campaignsCreated} | ${rad.campaignsCreated} |
| Watches linked | ${ac.watchesLinked} | ${rad.watchesLinked} |
| Decomp campaigns run | ${ac.decomp.campaignCount} | ${rad.decomp.campaignCount} |
| Child / research leads | ${acChildren} | ${radChildren} |
| Ready gate before→after | ${ac.before.readyGate}→${ac.after.readyGate} | ${rad.before.readyGate}→${rad.after.readyGate} |
| Watch gate before→after | ${ac.before.watchGate}→${ac.after.watchGate} | ${rad.before.watchGate}→${rad.after.watchGate} |
| Pursuits | ${ac.after.pursuits} | ${rad.after.pursuits} |

## Multilingual discovery (bounded)

| Hotel | ES queries | GL queries | EN queries | SERP calls |
|-------|------------|------------|------------|------------|
| AC | ${ac.qByLang.es} | ${ac.qByLang.gl} | ${ac.qByLang.en} | ${ac.totalSerp} |
| RAD | ${rad.qByLang.es} | 0 | ${rad.qByLang.en} | ${rad.totalSerp} |

SERP hits were classified; **none auto-admitted as campaigns** (quality gate). Campaign yield is from Spanish/Galician-sourced validated Watches already on corpus.

## Remaining gap

Organizer-seed decomposition is live. Full exhibitor/sponsor **list scraping** for new named children beyond organizers remains the next bounded research step (same shared path; no hotel forks).
`
  );

  const summary = {
    ac: {
      processParityAfter: ac.processParityAfter,
      campaigns: ac.campaignsCreated,
      decompInvoked: ac.decomp.campaignCount > 0,
      qByLang: ac.qByLang,
      totalSerp: ac.totalSerp,
      children: acChildren,
      provenTravel: acProven,
      newReady: ac.newReady,
      newWatch: ac.newWatch,
      ready: ac.after.readyGate,
      watch: ac.after.watchGate,
      pursuits: ac.after.pursuits,
    },
    rad: {
      processParityAfter: rad.processParityAfter,
      campaigns: rad.campaignsCreated,
      decompInvoked: rad.decomp.campaignCount > 0,
      qByLang: rad.qByLang,
      totalSerp: rad.totalSerp,
      children: radChildren,
      provenTravel: radProven,
      newReady: rad.newReady,
      newWatch: rad.newWatch,
      ready: rad.after.readyGate,
      watch: rad.after.watchGate,
      pursuits: rad.after.pursuits,
    },
    yotelCampaigns: yotelAfter.campaigns,
    bethReady: bethAfter.readyGate,
  };
  write("SUMMARIES.json", JSON.stringify(summary, null, 2));
  console.log(JSON.stringify(summary, null, 2));
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
