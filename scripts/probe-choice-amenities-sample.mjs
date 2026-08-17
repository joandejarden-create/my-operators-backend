#!/usr/bin/env node
import { fetchChoiceHotelAmenities } from "../lib/choice-hotel-content-fetch.js";

const urls = process.argv.slice(2);
const samples = urls.length
  ? urls
  : [
      "https://www.choicehotels.com/mexico/cancun/comfort-hotels/mx006",
      "https://www.choicehotels.com/puerto-rico/luquillo/comfort-hotels/pr123",
    ];

for (const url of samples) {
  console.log("\n---", url);
  try {
    const r = await fetchChoiceHotelAmenities(url);
    console.log("status:", r.status, "source:", r.source);
    console.log("amenities:", r.amenities.length, r.amenities.join("; "));
    if (r.parseErrors?.length) console.log("errors:", r.parseErrors.join("; "));
  } catch (e) {
    console.log("FAIL:", e?.message || e);
  }
}
