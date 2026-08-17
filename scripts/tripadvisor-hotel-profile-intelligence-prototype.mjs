#!/usr/bin/env node
/**
 * Read-only Tripadvisor owner-value prototype (cached Actor data only).
 * No Airtable / census writes. No new Apify runs.
 *
 * Usage:
 *   node scripts/tripadvisor-hotel-profile-intelligence-prototype.mjs
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  buildOwnerCompSnapshot,
  competitiveRankPercentile,
} from "../lib/hotel-intelligence/tripadvisor-profile/index.js";

process.env.ENABLE_HOTEL_INTELLIGENCE_AIRTABLE_WRITES = "0";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, "..");
const POOL = path.join(
  ROOT,
  "data/hotel-intelligence/giata-tripadvisor-room-decision-v1/ta-decision-pool.json"
);
const OUT_DIR = path.join(
  ROOT,
  "reports/hotel-intelligence/tripadvisor-hotel-profile-intelligence-v1"
);
const DATA_DIR = path.join(
  ROOT,
  "data/hotel-intelligence/tripadvisor-hotel-profile-intelligence-v1"
);

function writeJson(file, data) {
  fs.mkdirSync(path.dirname(file), { recursive: true });
  fs.writeFileSync(file, `${JSON.stringify(data, null, 2)}\n`, "utf8");
}

function main() {
  const pool = JSON.parse(fs.readFileSync(POOL, "utf8"));
  const hotels = (pool.items || []).filter(
    (i) => i.type === "HOTEL" || i.category === "hotel"
  );

  // Albufeira cluster — richest cached set with rating/rank/amenities
  const albufeira = hotels.filter((h) =>
    /albufeira|gale/i.test(
      [h.addressObj?.city, h.locationString, h.name].join(" ")
    )
  );
  const subjectName = "Auramar Beach Resort";
  const subject =
    albufeira.find((h) => /auramar/i.test(h.name || "")) || albufeira[0];
  if (!subject) throw new Error("No Albufeira subject hotel in cached pool");

  const comps = albufeira
    .filter((h) => String(h.id) !== String(subject.id))
    .filter(
      (h) =>
        h.rating != null &&
        h.rankingPosition != null &&
        Array.isArray(h.amenities) &&
        h.amenities.length > 5
    )
    .slice(0, 4);

  const snap = buildOwnerCompSnapshot(subject, comps);
  writeJson(path.join(DATA_DIR, "owner-comp-prototype.json"), snap);

  const md = renderPrototypeMarkdown(snap, subject, comps);
  fs.mkdirSync(OUT_DIR, { recursive: true });
  fs.writeFileSync(path.join(OUT_DIR, "OWNER_VALUE_PROTOTYPE.md"), md, "utf8");

  console.log(
    JSON.stringify(
      {
        production_writes: false,
        subject: subject.name,
        comps: comps.map((c) => c.name),
        rank_pct: snap.subject.competitive_rank_percentile,
        amenity_gaps: snap.vs_comps.amenity_gaps_majority_comps.length,
        out: path.join(OUT_DIR, "OWNER_VALUE_PROTOTYPE.md"),
      },
      null,
      2
    )
  );
}

function renderPrototypeMarkdown(snap, subject, comps) {
  const v = snap.vs_comps;
  const gaps = v.amenity_gaps_majority_comps
    .slice(0, 8)
    .map(
      (g) =>
        `- **${g.amenity}** — missing; present in ${g.comps_with}/${g.comps_total} comps (${g.comps_share}%)`
    )
    .join("\n");
  const diffs = v.amenity_differentiators
    .slice(0, 6)
    .map(
      (g) =>
        `- **${g.amenity}** — present here; only ${g.comps_share}% of comps`
    )
    .join("\n");
  const cats = v.category_vs_comps
    .map(
      (c) =>
        `| ${c.category} | ${c.hotel ?? "—"} | ${c.comp_median ?? "—"} | ${c.delta ?? "—"} |`
    )
    .join("\n");

  const compRows = snap.comps
    .map(
      (c) =>
        `| ${c.name} | ${c.rating ?? "—"} | ${c.numberOfReviews ?? "—"} | ${c.competitive_rank_percentile ?? "—"} | ${c.hotelClass ?? "—"} | ${c.numberOfRooms ?? "—"} | ${c.amenity_count} | ${c.priceLevel ?? "—"} |`
    )
    .join("\n");

  return `# Owner-value prototype (READ-ONLY)

**Subject (stand-in owner hotel):** ${subject.name}  
**Tripadvisor ID:** ${subject.id}  
**Source:** cached \`ta-decision-pool.json\` (no new Actor runs)  
**Production writes:** 0  

> Illustrative comparison set from the same destination cluster (Albufeira).  
> Not an official Dealality owner comp set. Not investment advice.

## Hotel position

| Metric | Your hotel | Comp median |
| --- | ---: | ---: |
| Rating | ${v.rating_hotel ?? "—"} | ${v.rating_comp_median ?? "—"} |
| Review volume | ${v.reviews_hotel ?? "—"} | ${v.reviews_comp_median ?? "—"} |
| Competitive rank percentile (higher = stronger) | ${v.rank_pct_hotel ?? "—"} | ${v.rank_pct_comp_median ?? "—"} |
| Rooms (TA candidate) | ${v.rooms_hotel ?? "—"} | ${v.rooms_comp_median ?? "—"} |
| Hotel class | ${snap.subject.hotelClass ?? "—"} | — |
| Price level (directional) | ${snap.subject.priceLevel ?? "—"} | — |
| Price range (directional) | ${snap.subject.priceRange ?? "—"} | — |

**Ranking label:** ${snap.subject.rankingString || "—"}  
**Formula:** \`competitive_rank_percentile = 100 × (denom − position + 1) / denom\`  
Example check: position ${snap.subject.rankingPosition}, denom ${snap.subject.rankingDenominator} → ${competitiveRankPercentile(snap.subject.rankingPosition, snap.subject.rankingDenominator)}

## Guest reputation

Histogram shares (4–5 bubble / mid / low): ${
    snap.subject.histogram_shares
      ? `${snap.subject.histogram_shares.share_4_5}% / ${snap.subject.histogram_shares.share_3}% / ${snap.subject.histogram_shares.share_1_2}%`
      : "—"
  }

### Category scores vs comps

| Category | Hotel | Comp median | Delta |
| --- | ---: | ---: | ---: |
${cats || "| — | — | — | — |"}

## Competitive set (cached)

| Hotel | Rating | Reviews | Rank %ile | Class | Rooms | Amenities | Price |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| **${snap.subject.name}** | ${snap.subject.rating ?? "—"} | ${snap.subject.numberOfReviews ?? "—"} | ${snap.subject.competitive_rank_percentile ?? "—"} | ${snap.subject.hotelClass ?? "—"} | ${snap.subject.numberOfRooms ?? "—"} | ${snap.subject.amenity_count} | ${snap.subject.priceLevel ?? "—"} |
${compRows}

## Product / amenity comparison

### Amenities missing vs majority of comps (≥70%)
${gaps || "_None above threshold in this sample._"}

### Relative differentiators (present here; uncommon in comps)
${diffs || "_None below 30% share in this sample._"}

## Key differences (supported)

- Reputation vs comps: rating ${v.rating_hotel} vs median ${v.rating_comp_median}; review volume ${v.reviews_hotel} vs ${v.reviews_comp_median}.
- Market standing: rank percentile ${v.rank_pct_hotel} vs comps ${v.rank_pct_comp_median} (same destination cluster; still verify rankingString geography).
- Product: ${v.amenity_gaps_majority_comps.length} majority-comp amenity gaps surfaced as diagnostics only.

## Potential questions for the owner

1. Is this the right **owner-selected** competitive set, or should neighborhood / brand / scale filters change?
2. Do the amenity gaps reflect intentional positioning or under-investment?
3. Does Tripadvisor ranking geography (\`${snap.subject.rankingString || ""}\`) match how you define your market?
4. Should room count (${snap.subject.numberOfRooms ?? "n/a"}) be verified via official primary source before census use?

## Caveats

${snap.caveats.map((c) => `- ${c}`).join("\n")}
`;
}

main();
