/**
 * Execute founder-approved $15 AVA research pilot manifest.
 * DFS ≤ $10 · Context.dev ≤ $5 · scrapes OFF · no Ahrefs/Parallel.
 *
 *   AVA_PAID_RESEARCH_AUTHORIZED=1 node scripts/ava-15usd-pilot-execute-manifest-v1.mjs
 */
import "dotenv/config";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { dataForSeoPost } from "../lib/research-engine-v2/dataforseo-client.js";
import {
  contextDevSearch,
  contextDevExtract,
} from "../lib/context-dev/client.js";
import { readProviderConfirmedCredits } from "../lib/context-dev/operation-credit-bounds.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const OUT_DIR = path.join(
  ROOT,
  "reports/dealality-ai-visibility-audit-action-plan-pilot-v1/paid-pilot-15usd"
);
const DFS_CAP = 10;
const CTX_USD_CAP = 5;
const COMBINED_CAP = 15;
const CTX_CREDIT_HARD_STOP = 1600;
const CTX_USD_PER_CREDIT = 0.003; // Developer overage worst-case

const HOTEL_FACT_SCHEMA = {
  type: "object",
  properties: {
    hotelName: { type: "string" },
    address: { type: "string" },
    phone: { type: "string" },
    officialWebsite: { type: "string" },
    meetingSpaceSqFtOrSqM: { type: "string" },
    spaSize: { type: "string" },
    roomsKeys: { type: "string" },
    brandAffiliation: { type: "string" },
    notableFacilities: { type: "array", items: { type: "string" } },
    factualConflictsNoted: { type: "array", items: { type: "string" } },
    sourceUrls: { type: "array", items: { type: "string" } },
  },
  required: ["hotelName"],
};

const HOTELS = [
  {
    id: "waterstone",
    propertyId: "adp_waterstone_boca_raton",
    name: "Waterstone Resort & Marina",
    hotelDomain: "waterstoneboca.com",
    brandTarget:
      "https://www.hilton.com/en-gb/locations/usa/florida/boca-raton/",
    peerDomain: "hilton.com",
    peerNote: "Hilton Boca Raton Suites / Embassy peer family surface",
    mapsQuery: "Waterstone Resort & Marina Boca Raton",
    mapsLocation: "Boca Raton,United States",
    searchQuery:
      "Waterstone Resort & Marina Boca Raton Curio Collection by Hilton official site OR google business OR tripadvisor",
    brandExtractFallback:
      "https://www.hilton.com/en-gb/locations/usa/florida/boca-raton/",
    country: "US",
  },
  {
    id: "ac-coruna",
    propertyId: "adp_ac_hotel_a_coruna",
    name: "AC Hotel A Coruña",
    hotelDomain: "hotelacacoruna.com",
    brandTarget:
      "https://www.marriott.com/en-us/hotels/lcgco-ac-hotel-a-coruna/overview/",
    peerDomain: "nh-hotels.com",
    peerNote: "NH Collection A Coruña Finisterre peer family",
    mapsQuery: "AC Hotel A Coruña",
    mapsLocation: "A Coruna,Spain",
    searchQuery:
      "AC Hotel A Coruña Marriott google business OR tripadvisor OR booking official",
    brandExtractFallback:
      "https://www.marriott.com/es/hotels/lcgco-ac-hotel-a-coruna/overview/",
    country: "ES",
  },
  {
    id: "castillo",
    propertyId: "adp_castillo_hotel_son_vida",
    name: "Castillo Hotel Son Vida",
    hotelDomain: "castillosonvidamallorca.com",
    brandTarget:
      "https://www.marriott.com/en-us/hotels/pmilc-castillo-hotel-son-vida-a-luxury-collection-hotel-mallorca/overview/",
    peerDomain: "arabellagolfmallorca.com",
    peerNote: "Sheraton Mallorca Arabella Golf peer",
    mapsQuery: "Castillo Hotel Son Vida Mallorca",
    mapsLocation: "Palma,Spain",
    searchQuery:
      "Castillo Hotel Son Vida Luxury Collection Mallorca google business OR tripadvisor OR visit palma",
    brandExtractFallback:
      "https://www.marriott.com/en-us/hotels/pmilc-castillo-hotel-son-vida-a-luxury-collection-hotel-mallorca/overview/",
    country: "ES",
  },
];

if (String(process.env.AVA_PAID_RESEARCH_AUTHORIZED || "").trim() !== "1") {
  console.error("REFUSED: set AVA_PAID_RESEARCH_AUTHORIZED=1 to run chargeable pilot");
  process.exit(2);
}

fs.mkdirSync(OUT_DIR, { recursive: true });

const ledger = {
  contract: "AVA_15USD_PILOT_EXECUTION_V1",
  authorizedAt: new Date().toISOString(),
  caps: { dfsUsd: DFS_CAP, ctxUsd: CTX_USD_CAP, combinedUsd: COMBINED_CAP, ctxCredits: CTX_CREDIT_HARD_STOP },
  dfsSpentUsd: 0,
  ctxSpentCredits: 0,
  ctxSpentUsdEst: 0,
  calls: [],
  stopped: null,
  hotels: {},
};

function combinedSpent() {
  return ledger.dfsSpentUsd + ledger.ctxSpentUsdEst;
}

function canAffordDfs(maxCost) {
  if (ledger.dfsSpentUsd + maxCost > DFS_CAP + 1e-9) return false;
  if (combinedSpent() + maxCost > COMBINED_CAP + 1e-9) return false;
  return true;
}

function canAffordCtx(credits) {
  const usd = credits * CTX_USD_PER_CREDIT;
  if (ledger.ctxSpentCredits + credits > CTX_CREDIT_HARD_STOP) return false;
  if (ledger.ctxSpentUsdEst + usd > CTX_USD_CAP + 1e-9) return false;
  if (combinedSpent() + usd > COMBINED_CAP + 1e-9) return false;
  return true;
}

function record(call) {
  ledger.calls.push(call);
  const slim = { ...call };
  if (slim.responsePreview) delete slim.responsePreview;
  console.log(
    JSON.stringify({
      id: call.id,
      provider: call.provider,
      ok: call.ok,
      costUsd: call.costUsd,
      credits: call.credits,
      dfsSpent: ledger.dfsSpentUsd,
      ctxCredits: ledger.ctxSpentCredits,
      combinedEst: combinedSpent(),
      note: call.note || call.error || null,
    })
  );
}

async function dfsCall(id, hotelId, endpoint, tasks, maxCost, purpose) {
  if (!canAffordDfs(maxCost)) {
    ledger.stopped = {
      reason: "DFS_OR_COMBINED_CAP_PRECHECK",
      at: id,
      dfsSpent: ledger.dfsSpentUsd,
      combined: combinedSpent(),
    };
    record({
      id,
      hotelId,
      provider: "dataforseo",
      path: endpoint,
      purpose,
      ok: false,
      skipped: true,
      costUsd: 0,
      maxCost,
      error: ledger.stopped.reason,
    });
    return null;
  }
  const res = await dataForSeoPost(endpoint, tasks, { timeoutMs: 90000 });
  const cost = Number(res.cost) || 0;
  if (ledger.dfsSpentUsd + cost > DFS_CAP + 1e-9 || combinedSpent() + cost > COMBINED_CAP + 1e-9) {
    ledger.stopped = {
      reason: "DFS_COST_WOULD_EXCEED_CAP",
      at: id,
      reportedCost: cost,
      dfsSpent: ledger.dfsSpentUsd,
    };
    record({
      id,
      hotelId,
      provider: "dataforseo",
      path: endpoint,
      purpose,
      ok: false,
      skipped: true,
      costUsd: 0,
      reportedButNotAccepted: cost,
      error: ledger.stopped.reason,
    });
    return null;
  }
  ledger.dfsSpentUsd += cost;
  const result = res.tasks?.[0]?.result || null;
  const outPath = path.join(OUT_DIR, `${id}.json`);
  fs.writeFileSync(
    outPath,
    JSON.stringify(
      {
        id,
        hotelId,
        path: endpoint,
        purpose,
        cost,
        ok: res.ok,
        status_code: res.status_code,
        status_message: res.status_message,
        result,
      },
      null,
      2
    )
  );
  record({
    id,
    hotelId,
    provider: "dataforseo",
    path: endpoint,
    purpose,
    ok: res.ok,
    costUsd: cost,
    maxCost,
    artifact: outPath.replace(/\\/g, "/"),
    error: res.ok ? null : res.status_message,
  });
  return res;
}

function pickMapsPlace(mapsResult) {
  const items = mapsResult?.tasks?.[0]?.result?.[0]?.items || [];
  const first = items.find((it) => it.type === "maps_search" || it.title) || items[0];
  if (!first) return null;
  return {
    title: first.title || null,
    placeId: first.place_id || first.feature_id || null,
    cid: first.cid || null,
    address: first.address || null,
    url: first.url || first.book_online_url || null,
    domain: first.domain || null,
    phone: first.phone || null,
    rating: first.rating || null,
  };
}

async function runDfsForHotel(hotel, indexBase) {
  const h = hotel;
  const bag = (ledger.hotels[h.id] = ledger.hotels[h.id] || { dfs: {}, ctx: {} });

  // summary hotel domain
  await dfsCall(
    `DFS-${String(indexBase + 1).padStart(2, "0")}`,
    h.id,
    "/backlinks/summary/live",
    [{ target: h.hotelDomain, include_subdomains: true, backlinks_status_type: "live" }],
    0.05,
    "hotel_referring_domain_summary"
  );
  if (ledger.stopped) return;

  // summary brand target
  await dfsCall(
    `DFS-${String(indexBase + 2).padStart(2, "0")}`,
    h.id,
    "/backlinks/summary/live",
    [{ target: h.brandTarget, include_subdomains: false, backlinks_status_type: "live" }],
    0.05,
    "brand_or_alternate_destination_summary"
  );
  if (ledger.stopped) return;

  // backlinks list
  await dfsCall(
    `DFS-${String(indexBase + 3).padStart(2, "0")}`,
    h.id,
    "/backlinks/backlinks/live",
    [
      {
        target: h.hotelDomain,
        limit: 100,
        mode: "as_is",
        backlinks_status_type: "live",
      },
    ],
    0.08,
    "hotel_backlinks_sample_limit_100"
  );
  if (ledger.stopped) return;

  // referring domains
  await dfsCall(
    `DFS-${String(indexBase + 4).padStart(2, "0")}`,
    h.id,
    "/backlinks/referring_domains/live",
    [{ target: h.hotelDomain, limit: 50, backlinks_status_type: "live" }],
    0.08,
    "hotel_referring_domains_limit_50"
  );
  if (ledger.stopped) return;

  // domain intersection: peer links not to hotel
  await dfsCall(
    `DFS-${String(indexBase + 5).padStart(2, "0")}`,
    h.id,
    "/backlinks/domain_intersection/live",
    [
      {
        targets: { 1: h.peerDomain },
        exclude_targets: [h.hotelDomain],
        limit: 20,
        exclude_large_domains: true,
      },
    ],
    0.1,
    `competitor_gap_vs_${h.peerDomain}`
  );
  if (ledger.stopped) return;

  // maps
  const mapsRes = await dfsCall(
    `DFS-${String(indexBase + 6).padStart(2, "0")}`,
    h.id,
    "/serp/google/maps/live/advanced",
    [
      {
        keyword: h.mapsQuery,
        location_name: h.mapsLocation,
        language_code: "en",
        depth: 20,
      },
    ],
    0.01,
    "google_maps_listing_discovery"
  );
  if (ledger.stopped) return;

  const place = pickMapsPlace(mapsRes);
  bag.dfs.mapsPlace = place;
  if (place?.placeId || place?.cid) {
    const task = {
      language_code: "en",
      location_name: h.mapsLocation,
    };
    if (place.placeId) task.place_id = place.placeId;
    else if (place.cid) task.cid = String(place.cid);
    await dfsCall(
      `DFS-${String(indexBase + 7).padStart(2, "0")}`,
      h.id,
      "/business_data/google/my_business_info/live",
      [task],
      0.02,
      "gbp_my_business_info"
    );
  } else {
    record({
      id: `DFS-${String(indexBase + 7).padStart(2, "0")}`,
      hotelId: h.id,
      provider: "dataforseo",
      path: "/business_data/google/my_business_info/live",
      purpose: "gbp_my_business_info",
      ok: false,
      skipped: true,
      costUsd: 0,
      note: "No place_id/cid from maps — skipped per manifest dependency",
    });
  }
}

async function ctxCall(id, hotelId, kind, reservedCredits, purpose, fn) {
  if (!canAffordCtx(reservedCredits)) {
    ledger.stopped = {
      reason: "CTX_OR_COMBINED_CAP_PRECHECK",
      at: id,
      ctxCredits: ledger.ctxSpentCredits,
      combined: combinedSpent(),
    };
    record({
      id,
      hotelId,
      provider: "context_dev",
      kind,
      purpose,
      ok: false,
      skipped: true,
      credits: 0,
      error: ledger.stopped.reason,
    });
    return null;
  }
  const res = await fn();
  const confirmed = readProviderConfirmedCredits(res);
  let credits = reservedCredits;
  if (confirmed.ok && confirmed.credits != null) {
    credits = Number(confirmed.credits);
    if (credits > reservedCredits) {
      ledger.stopped = {
        reason: "CTX_CREDITS_EXCEEDED_RESERVATION",
        at: id,
        reserved: reservedCredits,
        consumed: credits,
      };
      // Still account for actual spend
      ledger.ctxSpentCredits += credits;
      ledger.ctxSpentUsdEst += credits * CTX_USD_PER_CREDIT;
      record({
        id,
        hotelId,
        provider: "context_dev",
        kind,
        purpose,
        ok: false,
        credits,
        reservedCredits,
        costUsd: credits * CTX_USD_PER_CREDIT,
        error: ledger.stopped.reason,
        credits_remaining: confirmed.credits_remaining,
      });
      return null;
    }
  } else if (!res?.ok) {
    // failed calls: prefer confirmed credits; else 0 if validation/auth
    credits = confirmed.ok ? Number(confirmed.credits) : 0;
  }
  ledger.ctxSpentCredits += credits;
  ledger.ctxSpentUsdEst += credits * CTX_USD_PER_CREDIT;
  const outPath = path.join(OUT_DIR, `${id}.json`);
  fs.writeFileSync(
    outPath,
    JSON.stringify(
      {
        id,
        hotelId,
        kind,
        purpose,
        credits,
        reservedCredits,
        ok: Boolean(res?.ok),
        error: res?.error || null,
        credits_remaining: confirmed.credits_remaining ?? null,
        data: res?.data ?? null,
      },
      null,
      2
    )
  );
  record({
    id,
    hotelId,
    provider: "context_dev",
    kind,
    purpose,
    ok: Boolean(res?.ok),
    credits,
    reservedCredits,
    costUsd: credits * CTX_USD_PER_CREDIT,
    artifact: outPath.replace(/\\/g, "/"),
    credits_remaining: confirmed.credits_remaining ?? null,
    error: res?.ok ? null : res?.error?.message || res?.error?.class || null,
  });
  return res;
}

function pickBestListingUrl(searchData, hotel) {
  const results = searchData?.results || searchData?.data?.results || searchData || [];
  const arr = Array.isArray(results) ? results : results?.items || [];
  const urls = arr
    .map((r) => r.url || r.link || r.href)
    .filter((u) => typeof u === "string" && /^https?:/i.test(u));
  const prefer = [
    /google\.com\/maps/i,
    /business\.site/i,
    /tripadvisor\./i,
    /booking\.com/i,
    /expedia\./i,
    /marriott\.com/i,
    /hilton\.com\/en\/hotels/i,
  ];
  for (const re of prefer) {
    const hit = urls.find((u) => re.test(u));
    if (hit) return hit;
  }
  // avoid first-party hotel site for "listing" extract
  const external = urls.find((u) => !u.includes(hotel.hotelDomain));
  return external || urls[0] || null;
}

async function runCtxForHotel(hotel, indexBase) {
  const h = hotel;
  const bag = ledger.hotels[h.id];

  const searchRes = await ctxCall(
    `CTX-${String(indexBase + 1).padStart(2, "0")}`,
    h.id,
    "search",
    1,
    "listing_url_discovery",
    () => contextDevSearch({ query: h.searchQuery, numResults: 10, country: h.country })
  );
  if (ledger.stopped) return;

  let brandUrl = h.brandExtractFallback;
  const listingUrl = pickBestListingUrl(searchRes?.data, h);
  if (listingUrl && /hilton\.com\/en\/hotels|marriott\.com\/.*hotels/i.test(listingUrl)) {
    brandUrl = listingUrl;
  }
  bag.ctx.listingUrl = listingUrl;
  bag.ctx.brandUrl = brandUrl;

  await ctxCall(
    `CTX-${String(indexBase + 2).padStart(2, "0")}`,
    h.id,
    "extract",
    10,
    "brand_or_access_restricted_facts",
    () =>
      contextDevExtract({
        url: brandUrl,
        schema: HOTEL_FACT_SCHEMA,
        instructions:
          "Extract published hotel identity, address, phone, official website, meetings/spa sizes, and any conflicting figures. Do not invent values.",
        maxPages: 3,
        factCheck: true,
      })
  );
  if (ledger.stopped) return;

  const extractUrl =
    listingUrl && listingUrl !== brandUrl
      ? listingUrl
      : bag.dfs?.mapsPlace?.url ||
        (h.id === "ac-coruna"
          ? "https://acorunacb.com/ficha/ac-by-marriot-a-coruna/"
          : h.id === "castillo"
            ? "https://mallorcaconventionbureau.com/en/miembros/castillo-hotel-son-vida-a-luxury-collection-hotel"
            : "https://www.weddingwire.com/biz/waterstone-resort-marina-boca-raton-curio-collection-by-hilton/e664823149101595.html");

  bag.ctx.secondExtractUrl = extractUrl;
  await ctxCall(
    `CTX-${String(indexBase + 3).padStart(2, "0")}`,
    h.id,
    "extract",
    10,
    "gbp_or_ota_or_cvb_fact_consistency",
    () =>
      contextDevExtract({
        url: extractUrl,
        schema: HOTEL_FACT_SCHEMA,
        instructions:
          "Extract listing identity facts and compare-ready fields (name, address, website, phone, facilities). Flag mismatches vs a typical official hotel site.",
        maxPages: 2,
        factCheck: true,
      })
  );
}

async function main() {
  console.log("Starting AVA $15 pilot — scrapes OFF, Ahrefs/Parallel excluded");
  // Waterstone DFS-01..07 index 0; AC 7; Castillo 14
  await runDfsForHotel(HOTELS[0], 0);
  if (!ledger.stopped) await runDfsForHotel(HOTELS[1], 7);
  if (!ledger.stopped) await runDfsForHotel(HOTELS[2], 14);

  // CTX-01..09
  if (!ledger.stopped) await runCtxForHotel(HOTELS[0], 0);
  if (!ledger.stopped) await runCtxForHotel(HOTELS[1], 3);
  if (!ledger.stopped) await runCtxForHotel(HOTELS[2], 6);

  ledger.completedAt = new Date().toISOString();
  ledger.totals = {
    dfsSpentUsd: Number(ledger.dfsSpentUsd.toFixed(6)),
    ctxSpentCredits: ledger.ctxSpentCredits,
    ctxSpentUsdEstWorstCase: Number(ledger.ctxSpentUsdEst.toFixed(6)),
    combinedUsdEst: Number(combinedSpent().toFixed(6)),
    underCaps:
      ledger.dfsSpentUsd <= DFS_CAP &&
      ledger.ctxSpentUsdEst <= CTX_USD_CAP &&
      combinedSpent() <= COMBINED_CAP,
  };
  const summaryPath = path.join(OUT_DIR, "PILOT_EXECUTION_LEDGER.json");
  fs.writeFileSync(summaryPath, JSON.stringify(ledger, null, 2));
  console.log("\nLEDGER", JSON.stringify(ledger.totals, null, 2));
  console.log("stopped", ledger.stopped);
  console.log("wrote", summaryPath);
  if (ledger.stopped) process.exitCode = 3;
}

main().catch((err) => {
  console.error(err);
  ledger.fatal = String(err?.stack || err);
  fs.writeFileSync(path.join(OUT_DIR, "PILOT_EXECUTION_LEDGER.json"), JSON.stringify(ledger, null, 2));
  process.exit(1);
});
