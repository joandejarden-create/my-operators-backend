#!/usr/bin/env node
import { writeFileSync } from "node:fs";

const SLUG = "poplc-the-ocean-club-a-luxury-collection-resort-costa-norte";
const base = `https://www.marriott.com/en-us/hotels/${SLUG}`;
const paths = [
  "",
  "/",
  "/overview/",
  "/experiences/",
  "/dining/",
  "/photos/",
  "/events/",
  "/rooms/",
  "/reviews/",
  "/location/",
];

const H = {
  "User-Agent":
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
  Accept: "text/html",
  "Accept-Language": "en-US,en;q=0.9",
};

for (const p of paths) {
  const url = base + p;
  const r = await fetch(url, { headers: H, redirect: "follow" });
  const t = await r.text();
  const next = t.match(/<script id="__NEXT_DATA__"[^>]*>([\s\S]*?)<\/script>/i);
  console.log(
    p || "(root)",
    r.status,
    t.length,
    "next",
    !!next,
    "overview",
    /15-minute drive/i.test(t),
    "amenities",
    /Fitness center/i.test(t)
  );
}

const exp = await fetch(`${base}/experiences/`, { headers: H }).then((r) => r.text());
writeFileSync("reports/marriott-poplc-experiences-sample.html", exp.slice(0, 120000));

// Try parse amenity-like list items
const li = [...exp.matchAll(/>([^<]{3,80})<\/(?:li|p|span|div)/g)]
  .map((m) => m[1].trim())
  .filter((s) => /internet|pool|fitness|breakfast|spa|parking|shuttle|concierge/i.test(s));
console.log("\namenity-like snippets", [...new Set(li)].slice(0, 25));

const overviewSnippets = [...exp.matchAll(/>([^<]{40,300})<\//g)]
  .map((m) => m[1].trim())
  .filter((s) => /Puerto Plata|Ocean Club|private beach/i.test(s));
console.log("\noverview-like on experiences", overviewSnippets.slice(0, 3));
