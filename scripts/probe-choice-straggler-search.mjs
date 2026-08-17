#!/usr/bin/env node
import "../load-env.js";
import { readFileSync } from "node:fs";
import { fetchChoiceRegionalHotels } from "../lib/choice-regional-directory-extract.js";
import { nameSimilarity, normalizeText } from "../lib/independent-census/match-current-census.js";
import { choiceBrandFamiliesAlign } from "../lib/choice-census-regional-match.js";

const queries = process.argv.slice(2);
const targets = queries.length
  ? queries
  : [
      "Joinville",
      "Porto Alegre",
      "Rio Preto",
      "Pinheiros",
      "Macae",
      "Flecheiras",
      "Vale dos Vinhedos",
      "Pinhal",
      "Irapuato",
      "Amberes",
      "Chihuahua San Francisco",
      "Queretaro",
      "Torreon",
      "Villahermosa",
    ];

const mx = await fetchChoiceRegionalHotels(
  "https://www.choicehotels.com/en-uk/mexico/regional-hotels?placeId=ChIJU1NoiDs6BIQREZgJa760ZO0"
);
const br = await fetchChoiceRegionalHotels(
  "https://www.choicehotels.com/en-uk/brazil/regional-hotels"
);
const all = [...mx.hotels, ...br.hotels];

for (const q of targets) {
  const nq = normalizeText(q).toLowerCase();
  const hits = all.filter(
    (h) =>
      normalizeText(h.name).toLowerCase().includes(nq) ||
      normalizeText(h.citySlug).replace(/-/g, " ").includes(nq) ||
      h.propertyId.toLowerCase().includes(nq)
  );
  console.log(`\n=== ${q} (${hits.length}) ===`);
  for (const h of hits) {
    console.log(`  ${h.propertyId} | ${h.name} | ${h.propertyUrl}`);
  }
}
