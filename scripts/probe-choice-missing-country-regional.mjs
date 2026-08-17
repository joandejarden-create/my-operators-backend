#!/usr/bin/env node
import "../load-env.js";
import { CHOICE_FETCH_HEADERS } from "../lib/choice-regional-directory-extract.js";
import { parseChoiceRegionalHotelsFromHtml } from "../lib/choice-regional-directory-extract.js";

const probes = [
  { label: "Jamaica", slugs: ["jamaica", "caribbean"] },
  { label: "Bolivia", slugs: ["bolivia"] },
  { label: "US Virgin Islands", slugs: ["virgin-islands-us", "us-virgin-islands", "st-thomas", "st-croix"] },
];

for (const probe of probes) {
  console.log(`\n=== ${probe.label} ===`);
  for (const slug of probe.slugs) {
    for (const path of [`/${slug}/regional-hotels`, `/${slug}`]) {
      const url = `https://www.choicehotels.com/en-uk${path}`;
      try {
        const res = await fetch(url, { headers: CHOICE_FETCH_HEADERS, redirect: "follow" });
        const html = await res.text();
        const hotels = parseChoiceRegionalHotelsFromHtml(html);
        const placeIds = [
          ...new Set(
            [...html.matchAll(/regional-hotels\?placeId=([A-Za-z0-9_-]+)/g)].map((m) => m[1])
          ),
        ];
        if (res.status === 200 && (hotels.length > 0 || placeIds.length || html.length > 400000)) {
          console.log({
            slug,
            path,
            status: res.status,
            htmlLength: html.length,
            hotelCount: hotels.length,
            placeIds,
            sample: hotels.slice(0, 2),
            blocked: /access denied/i.test(html),
          });
        }
      } catch (err) {
        console.log(slug, path, "error", err.message);
      }
    }
  }
}
