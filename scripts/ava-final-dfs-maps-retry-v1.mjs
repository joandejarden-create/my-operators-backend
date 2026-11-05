import "dotenv/config";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { dataForSeoPost } from "../lib/research-engine-v2/dataforseo-client.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const REPORT = path.join(
  ROOT,
  "reports/dealality-ai-visibility-audit-action-plan-pilot-v1"
);
const RAW = path.join(REPORT, "final-research-raw");
const SEEDS = path.join(REPORT, "AVA_VERIFIED_PLATFORM_LISTING_SEEDS_V3.json");
const PAYLOAD = path.join(ROOT, "lib/ai-visibility-audit-action-plan/payloads");

const jobs = [
  {
    file: "waterstone-boca-raton-v1.json",
    keyword: "Waterstone Resort & Marina Boca Raton",
    location: "Boca Raton,Florida,United States",
  },
  {
    file: "ac-hotel-a-coruna-v1.json",
    keyword: "AC Hotel A Coruna",
    location: "A Coruna,Spain",
  },
  {
    file: "castillo-hotel-son-vida-v1.json",
    keyword: "Castillo Hotel Son Vida Mallorca",
    location: "Palma,Spain",
  },
  {
    file: "sheraton-mallorca-arabella-golf-v1.json",
    keyword: "Sheraton Mallorca Arabella Golf Hotel",
    location: "Palma,Spain",
  },
  {
    file: "fiesta-americana-condesa-cancun-ai-v1.json",
    keyword: "Fiesta Americana Condesa Cancun All Inclusive",
    location: "Cancun,Mexico",
  },
];

const stop = new Set([
  "hotel",
  "hotels",
  "resort",
  "marina",
  "collection",
  "luxury",
  "grand",
  "americana",
  "fiesta",
  "inclusive",
  "all",
  "golf",
  "spa",
  "by",
  "the",
  "and",
]);

let spent = 0.002;
const ledger = [];
const seeds = JSON.parse(fs.readFileSync(SEEDS, "utf8"));

for (const job of jobs) {
  if (spent >= 24.5) break;
  const payload = JSON.parse(fs.readFileSync(path.join(PAYLOAD, job.file), "utf8"));
  const propertyId = payload.property.propertyId;
  console.log("Maps", propertyId, job.location);
  const result = await dataForSeoPost(
    "/serp/google/maps/live/advanced",
    [
      {
        keyword: job.keyword,
        location_name: job.location,
        language_code: "en",
        depth: 10,
      },
    ],
    { timeoutMs: 120000 }
  );
  const cost = Number(result.cost) || 0;
  spent += cost;
  fs.writeFileSync(
    path.join(RAW, `dfs-maps-retry-${propertyId}.json`),
    JSON.stringify(
      { task: job, result: { ok: result.ok, cost, tasks: result.tasks } },
      null,
      2
    )
  );
  const items = [];
  for (const t of result.tasks || []) {
    for (const r of t.result || []) {
      for (const it of r.items || []) items.push(it);
    }
  }
  console.log(
    "  cost",
    cost,
    "items",
    items.length,
    "status",
    result.tasks?.[0]?.status_message
  );
  const tokens = [
    ...job.keyword.toLowerCase().split(/[^a-z0-9]+/),
    ...String(payload.property.market || "")
      .toLowerCase()
      .split(/[^a-z0-9]+/),
  ].filter((t) => t.length > 3 && !stop.has(t));
  let best = null;
  let bestScore = 0;
  for (const it of items) {
    const hay = `${it.title || ""} ${it.address || ""}`.toLowerCase();
    const score = tokens.filter((t) => hay.includes(t)).length;
    if (score > bestScore) {
      best = it;
      bestScore = score;
    }
  }
  const row = {
    hotel: propertyId,
    provider: "dataforseo",
    objective: "google_maps_place",
    costUsd: cost,
    ok: result.ok,
    evidenceRecovered: false,
  };
  if (best && bestScore > 0 && best.place_id) {
    const url = `https://www.google.com/maps/place/?q=place_id:${best.place_id}`;
    seeds.hotels[propertyId] = seeds.hotels[propertyId] || {};
    seeds.hotels[propertyId].GOOGLE_BUSINESS_PROFILE = {
      url,
      listingExistence: "EXACT_LISTING_VERIFIED",
      notes: `Google Maps place_id from DataForSEO maps SERP. title=${best.title}; address=${best.address || ""}`,
      placeId: best.place_id,
      address: best.address || null,
      phone: best.phone || null,
      verifiedVia: "dataforseo_serp_google_maps_live_advanced",
    };
    row.evidenceRecovered = true;
    row.evidenceSummary = {
      placeId: best.place_id,
      title: best.title,
      address: best.address,
    };
    console.log("  FOUND", best.place_id, best.title);
  } else {
    console.log(
      "  NO MATCH",
      items.slice(0, 3).map((i) => i.title)
    );
  }
  ledger.push(row);
}

const chapPlace = "ChIJL7JsB07_0YURvqjSTse1Qmc";
if (spent < 24) {
  console.log("GBP info Chapultepec");
  const gbp = await dataForSeoPost(
    "/business_data/google/my_business_info/live",
    [{ place_id: chapPlace, language_code: "en" }],
    { timeoutMs: 120000 }
  );
  const cost = Number(gbp.cost) || 0;
  spent += cost;
  fs.writeFileSync(
    path.join(RAW, "dfs-gbp-chapultepec.json"),
    JSON.stringify(
      { place_id: chapPlace, result: { ok: gbp.ok, cost, tasks: gbp.tasks } },
      null,
      2
    )
  );
  ledger.push({
    hotel: "adp_grand_fiesta_americana_chapultepec",
    provider: "dataforseo",
    objective: "gbp_field_snapshot",
    costUsd: cost,
    ok: gbp.ok,
    evidenceRecovered: Boolean(gbp.tasks?.[0]?.result?.[0]),
  });
  console.log("  gbp cost", cost, "status", gbp.tasks?.[0]?.status_message);
}

fs.writeFileSync(SEEDS, `${JSON.stringify(seeds, null, 2)}\n`);
fs.writeFileSync(
  path.join(REPORT, "AVA_SUPPLEMENTAL_SPEND_LEDGER_APPEND.json"),
  `${JSON.stringify({ spentUsd: Number(spent.toFixed(4)), ledger }, null, 2)}\n`
);
console.log("Total spent approx", spent.toFixed(4));
