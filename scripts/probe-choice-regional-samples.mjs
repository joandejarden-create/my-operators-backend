#!/usr/bin/env node
import "../load-env.js";
import { fetchChoiceRegionalHotels } from "../lib/choice-regional-directory-extract.js";

const countries = ["brazil", "colombia", "panama", "chile"];
for (const slug of countries) {
  const r = await fetchChoiceRegionalHotels(
    `https://www.choicehotels.com/en-uk/${slug}/regional-hotels`
  );
  console.log(slug, r.hotels.length, "sample:", r.hotels.slice(0, 2));
}
