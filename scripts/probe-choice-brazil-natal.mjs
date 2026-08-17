#!/usr/bin/env node
import "../load-env.js";
import { fetchChoiceRegionalHotels } from "../lib/choice-regional-directory-extract.js";

const r = await fetchChoiceRegionalHotels(
  "https://www.choicehotels.com/en-uk/brazil/regional-hotels"
);
const natal = r.hotels.filter(
  (h) => /natal/i.test(h.name) || /natal/i.test(h.citySlug) || /natal/i.test(h.propertyUrl)
);
console.log(natal);
