#!/usr/bin/env node
import "../load-env.js";
import Airtable from "airtable";
import { loadChoiceSitemapDirectoryForCountry } from "../lib/choice-sitemap-only-directory.js";
import { mapCensusRowForDirectoryMatch } from "../lib/hotel-census/match-brand-directory-to-census.js";
import {
  matchChoiceRegionalToCensus,
  scoreChoiceRegionalAgainstCensus,
} from "../lib/choice-census-regional-match.js";
import { HOTEL_CENSUS_TABLE } from "../lib/hotel-census/fields.js";

const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(
  process.env.AIRTABLE_BASE_ID_ALT
);
const recs = await base(HOTEL_CENSUS_TABLE)
  .select({ filterByFormula: '{name}="Radisson Hotel Santa Cruz"' })
  .all();
const rec = recs[0];
const census = mapCensusRowForDirectoryMatch(rec);
const hotels = loadChoiceSitemapDirectoryForCountry("Bolivia");
console.log("hotels", hotels);
console.log("score", scoreChoiceRegionalAgainstCensus(hotels[0], census, "Bolivia"));
console.log(
  "assigned",
  matchChoiceRegionalToCensus(hotels, [census], {
    minScore: 55,
    minNameSim: 0.45,
    minConfidence: "low",
    regionalCountry: "Bolivia",
  })
);
