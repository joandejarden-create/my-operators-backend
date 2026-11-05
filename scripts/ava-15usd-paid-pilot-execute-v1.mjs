/**
 * Execute founder-approved AVA $15 DataForSEO + Context.dev pilot (scrapes OFF).
 * Manifest: AVA_RESEARCH_RECONCILIATION_AND_15USD_PILOT_PREFLIGHT.md
 *
 *   AVA_PAID_RESEARCH_AUTHORIZED=1 node scripts/ava-15usd-paid-pilot-execute-v1.mjs
 */
import "dotenv/config";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { dataForSeoPost } from "../lib/research-engine-v2/dataforseo-client.js";
import {
  contextDevSearch,
  contextDevExtract,
  isContextDevConfigured,
} from "../lib/context-dev/client.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const REPORT = path.join(
  ROOT,
  "reports/dealality-ai-visibility-audit-action-plan-pilot-v1"
);
const OUT_JSON = path.join(REPORT, "AVA_15USD_PAID_PILOT_RESULTS.json");
const OUT_RAW = path.join(REPORT, "AVA_15USD_PAID_PILOT_RAW");

const DFS_CAP = 10;
const CTX_USD_CAP = 5;
const COMBINED_CAP = 15;
const CTX_CREDIT_CAP = 1600;
const CTX_OVERAGE_USD_PER_CREDIT = 0.003; // Developer overage worst-case

const HOTEL_FACT_SCHEMA = {
  type: "object",
  properties: {
    property_name: { type: "string" },
    official_website: { type: "string" },
    address: { type: "string" },
    phone: { type: "string" },
    meeting_space_claims: { type: "array", items: { type: "string" } },
    spa_or_wellness_claims: { type: "array", items: { type: "string" } },
    dining_claims: { type: "array", items: { type: "string" } },
    brand_affiliation: { type: "string" },
    discrepancies_or_conflicts_noted: { type: "array", items: { type: "string" } },
    source_page_urls: { type: "array", items: { type: "string" } },
  },
  required: ["property_name"],
  additionalProperties: false,
};

const hotels = [
  {
    id: "adp_waterstone_boca_raton",
    name: "Waterstone Resort & Marina",
    hotelDomain: "waterstoneboca.com",
    brandUrl: null,
    brandDomainFallback: "hilton.com",
    peerDomain: "hiltonboca.com",
    mapsKeyword: "Waterstone Resort & Marina Boca Raton",
    mapsLocation: "Boca Raton,Florida,United States",
    searchQuery:
      "Waterstone Resort & Marina Boca Raton Curio Collection Hilton official site OR Google Business OR tripadvisor OR booking",
    extractBrandFallback: true,
  },
  {
    id: "adp_ac_hotel_a_coruna",
    name: "AC Hotel A Coruña",
    hotelDomain: "hotelacacoruna.com",
    brandUrl:
      "https://www.marriott.com/en-us/hotels/lcgco-ac-hotel-a-coruna/overview/",
    peerDomain: "nh-hotels.com",
    mapsKeyword: "AC Hotel A Coruña",
    mapsLocation: "A Coruna,Spain",
    searchQuery:
      "AC Hotel A Coruña Marriott Google Business Profile OR Booking.com OR Tripadvisor official listing",
  },
  {
    id: "adp_castillo_hotel_son_vida",
    name: "Castillo Hotel Son Vida, a Luxury Collection Hotel, Mallorca",
    hotelDomain: "castillosonvidamallorca.com",
    brandUrl:
      "https://www.marriott.com/en-us/hotels/pmilc-castillo-hotel-son-vida-a-luxury-collection-hotel-mallorca/overview/",
    peerDomain: "arabellagolfmallorca.com",
    mapsKeyword: "Castillo Hotel Son Vida Mallorca",
    mapsLocation: "Palma,Spain",
    searchQuery:
      "Castillo Hotel Son Vida Luxury Collection Mallorca Google Business OR Booking.com OR Tripadvisor",
  },
];

if (process.env.AVA_PAID_RESEARCH_AUTHORIZED !== "1") {
  console.error("Refusing: set AVA_PAID_RESEARCH_AUTHORIZED=1");
  process.exit(2);
}

fs.mkdirSync(OUT_RAW, { recursive: true });

const SKIP_IDS = new Set(
  (process.argv.find((a) => a.startsWith("--skip-ids=")) || "")
    .replace("--skip-ids=", "")
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean)
);
const SEED_DFS = Number(
  (process.argv.find((a) => a.startsWith("--seed-dfs-usd=")) || "").replace(
    "--seed-dfs-usd=",
    ""
  ) || 0
);

const ledger = {
  contract: "AVA_15USD_PAID_PILOT_RESULTS_V1",
  startedAt: new Date().toISOString(),
  caps: { dfsUsd: DFS_CAP, ctxUsd: CTX_USD_CAP, combinedUsd: COMBINED_CAP, ctxCredits: CTX_CREDIT_CAP },
  dfsUsd: Number.isFinite(SEED_DFS) ? SEED_DFS : 0,
  ctxCredits: 0,
  ctxUsdEstimate: 0,
  combinedUsdEstimate: Number.isFinite(SEED_DFS) ? SEED_DFS : 0,
  calls: [],
  stopped: null,
  hotels: {},
  skipIds: [...SKIP_IDS],
  seedDfsUsd: SEED_DFS || 0,
};

function remainingDfs() {
  return DFS_CAP - ledger.dfsUsd;
}
function remainingCtxCredits() {
  return CTX_CREDIT_CAP - ledger.ctxCredits;
}
function remainingCombined() {
  return COMBINED_CAP - (ledger.dfsUsd + ledger.ctxUsdEstimate);
}

function stop(reason) {
  ledger.stopped = { at: new Date().toISOString(), reason };
  console.error("STOP:", reason);
}

function recordCall(row) {
  ledger.calls.push(row);
  console.log(
    `${row.id} ${row.ok ? "OK" : "FAIL"} cost=${row.costUsd ?? 0} credits=${row.credits ?? 0} | ${row.purpose}`
  );
}

function summarizeDfs(result) {
  const task = result.tasks?.[0] || {};
  const r0 = task.result?.[0] || null;
  if (!r0) return { taskStatus: task.status_code, taskMessage: task.status_message };
  if (r0.referring_domains != null || r0.backlinks != null) {
    return {
      target: r0.target,
      backlinks: r0.backlinks,
      referring_domains: r0.referring_domains,
      referring_main_domains: r0.referring_main_domains,
      rank: r0.rank,
    };
  }
  if (Array.isArray(r0.items)) {
    return {
      total_count: r0.total_count,
      items_count: r0.items_count,
      sample: r0.items.slice(0, 15).map((it) => ({
        url_from: it.url_from || it.domain_from || it.target || it.domain,
        domain_from: it.domain_from || it.domain,
        url_to: it.url_to,
        rank: it.rank,
        dofollow: it.dofollow,
        intersections: it.intersections,
        domain_intersection: it.domain_intersection
          ? Object.keys(it.domain_intersection)
          : undefined,
      })),
    };
  }
  // Maps / business
  if (r0.items || r0.title || r0.place_id) {
    return {
      keyword: r0.keyword,
      items_count: r0.items_count || (r0.items || []).length,
      sample: (r0.items || []).slice(0, 5).map((it) => ({
        title: it.title,
        place_id: it.place_id,
        domain: it.domain,
        url: it.url,
        address: it.address,
        phone: it.phone,
        rating: it.rating?.value || it.rating,
      })),
      title: r0.title,
      place_id: r0.place_id,
      address: r0.address,
      phone: r0.phone,
      category: r0.category,
      url: r0.url,
    };
  }
  return { keys: Object.keys(r0).slice(0, 20) };
}

async function dfsCall({ id, hotelId, purpose, endpoint, task, maxCost }) {
  if (ledger.stopped) return null;
  if (SKIP_IDS.has(id)) {
    recordCall({
      id,
      provider: "dataforseo",
      hotelId,
      purpose,
      endpoint,
      ok: true,
      skipped: true,
      reason: "already_executed_prior_attempt",
      costUsd: 0,
    });
    const prior = path.join(OUT_RAW, `${id}.json`);
    if (fs.existsSync(prior)) {
      try {
        const raw = JSON.parse(fs.readFileSync(prior, "utf8"));
        return { ok: true, summary: raw.result?.tasks ? summarizeDfs({ tasks: raw.result.tasks }) : null, tasks: raw.result?.tasks };
      } catch {
        return { ok: true, skipped: true };
      }
    }
    return { ok: true, skipped: true };
  }
  if (maxCost > remainingDfs() || maxCost > remainingCombined()) {
    stop(`DFS ${id} maxCost ${maxCost} exceeds remaining DFS/combined cap`);
    return null;
  }
  const result = await dataForSeoPost(endpoint, [task], { timeoutMs: 120000 });
  const cost = Number(result.cost) || 0;
  if (cost > maxCost + 0.001) {
    // still charged — record then stop
    ledger.dfsUsd += cost;
    ledger.combinedUsdEstimate = ledger.dfsUsd + ledger.ctxUsdEstimate;
    recordCall({
      id,
      provider: "dataforseo",
      hotelId,
      purpose,
      endpoint,
      ok: result.ok,
      costUsd: cost,
      maxCost,
      overMax: true,
      status: result.status_code,
      message: result.status_message,
    });
    stop(`DFS ${id} actual cost ${cost} exceeded per-call max ${maxCost}`);
    return result;
  }
  if (ledger.dfsUsd + cost > DFS_CAP || ledger.dfsUsd + cost + ledger.ctxUsdEstimate > COMBINED_CAP) {
    ledger.dfsUsd += cost;
    ledger.combinedUsdEstimate = ledger.dfsUsd + ledger.ctxUsdEstimate;
    recordCall({
      id,
      provider: "dataforseo",
      hotelId,
      purpose,
      endpoint,
      ok: result.ok,
      costUsd: cost,
      status: result.status_code,
      message: result.status_message,
      haltedAfter: true,
    });
    stop(`DFS cumulative would exceed cap after ${id}`);
    return result;
  }
  ledger.dfsUsd += cost;
  ledger.combinedUsdEstimate = ledger.dfsUsd + ledger.ctxUsdEstimate;
  const summary = summarizeDfs(result);
  const row = {
    id,
    provider: "dataforseo",
    hotelId,
    purpose,
    endpoint,
    ok: result.ok,
    costUsd: cost,
    status: result.status_code,
    message: result.status_message,
    summary,
  };
  recordCall(row);
  fs.writeFileSync(
    path.join(OUT_RAW, `${id}.json`),
    JSON.stringify({ request: task, result: { ok: result.ok, cost, status_code: result.status_code, tasks: result.tasks } }, null, 2)
  );
  return { ...result, summary };
}

async function ctxSearch({ id, hotelId, purpose, query, reservedCredits = 1 }) {
  if (ledger.stopped) return null;
  if (!isContextDevConfigured()) {
    stop("CONTEXT_DEV_API_KEY missing");
    return null;
  }
  if (reservedCredits > remainingCtxCredits()) {
    stop(`CTX ${id} reserved credits exceed credit cap`);
    return null;
  }
  const estUsd = reservedCredits * CTX_OVERAGE_USD_PER_CREDIT;
  if (estUsd > remainingCombined() || ledger.ctxUsdEstimate + estUsd > CTX_USD_CAP) {
    stop(`CTX ${id} estimated USD exceeds Context/combined cap`);
    return null;
  }
  const result = await contextDevSearch({ query, numResults: 10 });
  const credits =
    result.key_metadata?.credits_consumed != null
      ? Number(result.key_metadata.credits_consumed)
      : reservedCredits;
  if (credits > reservedCredits) {
    ledger.ctxCredits += credits;
    ledger.ctxUsdEstimate = ledger.ctxCredits * CTX_OVERAGE_USD_PER_CREDIT;
    ledger.combinedUsdEstimate = ledger.dfsUsd + ledger.ctxUsdEstimate;
    recordCall({
      id,
      provider: "context_dev",
      hotelId,
      purpose,
      ok: result.ok,
      credits,
      reservedCredits,
      costUsd: credits * CTX_OVERAGE_USD_PER_CREDIT,
      overrun: true,
      error: result.error,
    });
    stop(`CTX ${id} credits_consumed ${credits} > reserved ${reservedCredits}`);
    return result;
  }
  ledger.ctxCredits += credits;
  ledger.ctxUsdEstimate = ledger.ctxCredits * CTX_OVERAGE_USD_PER_CREDIT;
  ledger.combinedUsdEstimate = ledger.dfsUsd + ledger.ctxUsdEstimate;
  const results = result.data?.results || result.data?.organic || result.data || [];
  const urls = (Array.isArray(results) ? results : [])
    .map((r) => r.url || r.link || r.href)
    .filter(Boolean)
    .slice(0, 10);
  recordCall({
    id,
    provider: "context_dev",
    hotelId,
    purpose,
    ok: result.ok,
    credits,
    costUsd: credits * CTX_OVERAGE_USD_PER_CREDIT,
    credits_remaining: result.key_metadata?.credits_remaining ?? null,
    urls,
    error: result.error,
  });
  fs.writeFileSync(
    path.join(OUT_RAW, `${id}.json`),
    JSON.stringify({ query, ok: result.ok, credits, urls, error: result.error, dataKeys: result.data ? Object.keys(result.data) : [] }, null, 2)
  );
  return { ...result, urls };
}

async function ctxExtract({ id, hotelId, purpose, url, reservedCredits = 10 }) {
  if (ledger.stopped) return null;
  if (!url) {
    recordCall({
      id,
      provider: "context_dev",
      hotelId,
      purpose,
      ok: false,
      skipped: true,
      reason: "no_url",
      credits: 0,
      costUsd: 0,
    });
    return null;
  }
  if (reservedCredits > remainingCtxCredits()) {
    stop(`CTX ${id} reserved credits exceed credit cap`);
    return null;
  }
  const estUsd = reservedCredits * CTX_OVERAGE_USD_PER_CREDIT;
  if (estUsd > remainingCombined() || ledger.ctxUsdEstimate + estUsd > CTX_USD_CAP) {
    stop(`CTX ${id} estimated USD exceeds Context/combined cap`);
    return null;
  }
  const result = await contextDevExtract({
    url,
    schema: HOTEL_FACT_SCHEMA,
    instructions:
      "Extract only published hotel facts: name, official website, address, phone, meetings, spa/wellness, dining, brand affiliation, and any conflicting figures. Do not invent.",
    maxPages: 3,
    factCheck: true,
  });
  const credits =
    result.key_metadata?.credits_consumed != null
      ? Number(result.key_metadata.credits_consumed)
      : reservedCredits;
  if (credits > reservedCredits) {
    ledger.ctxCredits += credits;
    ledger.ctxUsdEstimate = ledger.ctxCredits * CTX_OVERAGE_USD_PER_CREDIT;
    ledger.combinedUsdEstimate = ledger.dfsUsd + ledger.ctxUsdEstimate;
    recordCall({
      id,
      provider: "context_dev",
      hotelId,
      purpose,
      url,
      ok: result.ok,
      credits,
      reservedCredits,
      overrun: true,
      costUsd: credits * CTX_OVERAGE_USD_PER_CREDIT,
      error: result.error,
    });
    stop(`CTX ${id} credits_consumed ${credits} > reserved ${reservedCredits}`);
    return result;
  }
  ledger.ctxCredits += credits;
  ledger.ctxUsdEstimate = ledger.ctxCredits * CTX_OVERAGE_USD_PER_CREDIT;
  ledger.combinedUsdEstimate = ledger.dfsUsd + ledger.ctxUsdEstimate;
  const facts = result.data?.data || result.data?.result || result.data;
  recordCall({
    id,
    provider: "context_dev",
    hotelId,
    purpose,
    url,
    ok: result.ok,
    credits,
    costUsd: credits * CTX_OVERAGE_USD_PER_CREDIT,
    credits_remaining: result.key_metadata?.credits_remaining ?? null,
    factsSummary: facts
      ? {
          property_name: facts.property_name,
          official_website: facts.official_website,
          address: facts.address,
          phone: facts.phone,
          brand_affiliation: facts.brand_affiliation,
          conflicts: facts.discrepancies_or_conflicts_noted,
        }
      : null,
    error: result.error,
  });
  fs.writeFileSync(
    path.join(OUT_RAW, `${id}.json`),
    JSON.stringify({ url, ok: result.ok, credits, facts, error: result.error }, null, 2)
  );
  return result;
}

function pickListingUrl(urls = [], hotelDomain) {
  const ranked = [...urls].sort((a, b) => {
    const score = (u) => {
      const s = String(u).toLowerCase();
      if (/google\.(com|es|co)|maps\.app|g\.page|business\.site/i.test(s)) return 100;
      if (/tripadvisor|booking\.com|expedia|hotels\.com/i.test(s)) return 80;
      if (hotelDomain && s.includes(hotelDomain)) return 20;
      if (/hilton\.com\/en\/hotels|marriott\.com\/.*hotels\//i.test(s)) return 90;
      return 10;
    };
    return score(b) - score(a);
  });
  return ranked[0] || null;
}

function pickBrandUrl(urls = [], fallback) {
  const brand = urls.find((u) =>
    /hilton\.com\/en\/hotels\/|marriott\.com\/.*\/hotels\//i.test(u)
  );
  return brand || fallback;
}

async function runHotel(h, ids) {
  const hotelState = {
    id: h.id,
    name: h.name,
    brandUrl: h.brandUrl,
    listingUrl: null,
    dfs: {},
    ctx: {},
  };
  ledger.hotels[h.id] = hotelState;

  // DFS-01 / 08 / 15 summary hotel domain
  hotelState.dfs.summaryHotel = await dfsCall({
    id: ids.summaryHotel,
    hotelId: h.id,
    purpose: "Hotel referring-domain / backlink summary",
    endpoint: "/backlinks/summary/live",
    maxCost: 0.05,
    task: {
      target: h.hotelDomain,
      include_subdomains: true,
      backlinks_status_type: "live",
      internal_list_limit: 10,
      tag: ids.summaryHotel,
    },
  });

  // CTX search early so brand/listing URLs inform later calls
  hotelState.ctx.search = await ctxSearch({
    id: ids.search,
    hotelId: h.id,
    purpose: "Discover GBP/OTA/listing URLs",
    query: h.searchQuery,
  });
  const searchUrls = hotelState.ctx.search?.urls || [];
  hotelState.brandUrl = pickBrandUrl(searchUrls, h.brandUrl);
  hotelState.listingUrl = pickListingUrl(searchUrls, h.hotelDomain);
  const brandTarget =
    hotelState.brandUrl ||
    h.brandUrl ||
    h.brandDomainFallback ||
    h.hotelDomain;

  // DFS brand summary
  hotelState.dfs.summaryBrand = await dfsCall({
    id: ids.summaryBrand,
    hotelId: h.id,
    purpose: "Brand / alternate destination backlink summary",
    endpoint: "/backlinks/summary/live",
    maxCost: 0.05,
    task: {
      target: brandTarget,
      include_subdomains: Boolean(h.brandDomainFallback && !hotelState.brandUrl),
      backlinks_status_type: "live",
      tag: ids.summaryBrand,
    },
  });
  hotelState.brandTargetUsed = brandTarget;

  hotelState.dfs.backlinks = await dfsCall({
    id: ids.backlinks,
    hotelId: h.id,
    purpose: "Sample referring pages",
    endpoint: "/backlinks/backlinks/live",
    maxCost: 0.08,
    task: {
      target: h.hotelDomain,
      limit: 100,
      order_by: ["rank,desc"],
      backlinks_status_type: "live",
      exclude_internal_backlinks: true,
      tag: ids.backlinks,
    },
  });

  hotelState.dfs.referringDomains = await dfsCall({
    id: ids.referringDomains,
    hotelId: h.id,
    purpose: "Referring domains list",
    endpoint: "/backlinks/referring_domains/live",
    maxCost: 0.08,
    task: {
      target: h.hotelDomain,
      limit: 50,
      order_by: ["rank,desc"],
      backlinks_status_type: "live",
      exclude_internal_backlinks: true,
      tag: ids.referringDomains,
    },
  });

  // Link gap: domains linking to peer but not hotel
  hotelState.dfs.intersection = await dfsCall({
    id: ids.intersection,
    hotelId: h.id,
    purpose: "Competitor backlink gaps (peer vs hotel)",
    endpoint: "/backlinks/domain_intersection/live",
    maxCost: 0.1,
    task: {
      targets: { 1: h.peerDomain, 2: h.hotelDomain },
      limit: 20,
      order_by: ["1.rank,desc"],
      intersection_mode: "partial",
      tag: ids.intersection,
    },
  });

  hotelState.dfs.maps = await dfsCall({
    id: ids.maps,
    hotelId: h.id,
    purpose: "Google Maps listing URL discovery",
    endpoint: "/serp/google/maps/live/advanced",
    maxCost: 0.01,
    task: {
      keyword: h.mapsKeyword,
      location_name: h.mapsLocation,
      language_code: "en",
      depth: 20,
      tag: ids.maps,
    },
  });

  const placeId =
    hotelState.dfs.maps?.summary?.sample?.[0]?.place_id ||
    hotelState.dfs.maps?.tasks?.[0]?.result?.[0]?.items?.[0]?.place_id ||
    null;

  if (placeId) {
    hotelState.dfs.gbp = await dfsCall({
      id: ids.gbp,
      hotelId: h.id,
      purpose: "GBP field snapshot via place_id",
      endpoint: "/business_data/google/my_business_info/live",
      maxCost: 0.02,
      task: {
        place_id: placeId,
        language_code: "en",
        tag: ids.gbp,
      },
    });
  } else {
    hotelState.dfs.gbp = await dfsCall({
      id: ids.gbp,
      hotelId: h.id,
      purpose: "GBP field snapshot via keyword",
      endpoint: "/business_data/google/my_business_info/live",
      maxCost: 0.02,
      task: {
        keyword: h.mapsKeyword,
        location_name: h.mapsLocation,
        language_code: "en",
        tag: ids.gbp,
      },
    });
  }

  hotelState.ctx.extractBrand = await ctxExtract({
    id: ids.extractBrand,
    hotelId: h.id,
    purpose: "Brand/hotel page fact extraction",
    url: hotelState.brandUrl || h.brandUrl || `https://www.${h.hotelDomain}/`,
  });

  const extractListingUrl =
    hotelState.listingUrl ||
    hotelState.dfs.maps?.summary?.sample?.[0]?.url ||
    hotelState.dfs.gbp?.summary?.url ||
    null;

  hotelState.ctx.extractListing = await ctxExtract({
    id: ids.extractListing,
    hotelId: h.id,
    purpose: "GBP/OTA listing fact extraction",
    url: extractListingUrl,
  });

  return hotelState;
}

async function main() {
  console.log("Starting $15 paid pilot — scrapes OFF, Ahrefs/Parallel excluded");
  await runHotel(hotels[0], {
    summaryHotel: "DFS-01",
    summaryBrand: "DFS-02",
    backlinks: "DFS-03",
    referringDomains: "DFS-04",
    intersection: "DFS-05",
    maps: "DFS-06",
    gbp: "DFS-07",
    search: "CTX-01",
    extractBrand: "CTX-02",
    extractListing: "CTX-03",
  });
  await runHotel(hotels[1], {
    summaryHotel: "DFS-08",
    summaryBrand: "DFS-09",
    backlinks: "DFS-10",
    referringDomains: "DFS-11",
    intersection: "DFS-12",
    maps: "DFS-13",
    gbp: "DFS-14",
    search: "CTX-04",
    extractBrand: "CTX-05",
    extractListing: "CTX-06",
  });
  await runHotel(hotels[2], {
    summaryHotel: "DFS-15",
    summaryBrand: "DFS-16",
    backlinks: "DFS-17",
    referringDomains: "DFS-18",
    intersection: "DFS-19",
    maps: "DFS-20",
    gbp: "DFS-21",
    search: "CTX-07",
    extractBrand: "CTX-08",
    extractListing: "CTX-09",
  });

  ledger.finishedAt = new Date().toISOString();
  ledger.totals = {
    dfsUsd: Number(ledger.dfsUsd.toFixed(6)),
    ctxCredits: ledger.ctxCredits,
    ctxUsdEstimateWorstCase: Number(ledger.ctxUsdEstimate.toFixed(6)),
    combinedUsdEstimate: Number(ledger.combinedUsdEstimate.toFixed(6)),
    callCount: ledger.calls.length,
    okCalls: ledger.calls.filter((c) => c.ok).length,
    failedCalls: ledger.calls.filter((c) => c.ok === false && !c.skipped).length,
  };
  fs.writeFileSync(OUT_JSON, `${JSON.stringify(ledger, null, 2)}\n`);
  console.log("\n=== PILOT SPEND ===");
  console.log(JSON.stringify(ledger.totals, null, 2));
  if (ledger.stopped) console.log("Stopped early:", ledger.stopped);
  console.log("Wrote", OUT_JSON);
}

main().catch((e) => {
  console.error(e);
  ledger.fatal = String(e?.stack || e);
  fs.writeFileSync(OUT_JSON, `${JSON.stringify(ledger, null, 2)}\n`);
  process.exit(1);
});
