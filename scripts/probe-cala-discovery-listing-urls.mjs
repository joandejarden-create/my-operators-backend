#!/usr/bin/env node
/**
 * Read-only probe of CALA listing URLs for Webhound learning compare.
 * No Airtable writes.
 */
import { writeFileSync, mkdirSync } from "node:fs";
import { join } from "node:path";
import {
  countrySitemapUrl,
  fetchMarriottCountrySitemapPage,
} from "../lib/marriott-brand-directory-extract.js";
import { buildChoiceRegionalPageForCountry } from "../lib/choice-regional-directory-extract.js";

const ROOT = process.cwd();
const out = { generated_at: new Date().toISOString(), marriott: [], choice: [], hilton_probe: [] };

const slugs = ["mexico", "dominican-republic", "costa-rica", "colombia", "panama"];
for (const slug of slugs) {
  const url = countrySitemapUrl(slug);
  try {
    const page = await fetchMarriottCountrySitemapPage(url);
    const hotels = page?.hotels || [];
    out.marriott.push({
      slug,
      url,
      ok: true,
      hotel_count: hotels.length,
      sample: hotels.slice(0, 3).map((h) => ({
        marsha: h.marsha,
        title: h.title,
        url: h.url,
      })),
    });
  } catch (e) {
    out.marriott.push({ slug, url, ok: false, error: e.message });
  }
}

for (const c of ["Mexico", "Dominican Republic", "Costa Rica", "Colombia", "Panama"]) {
  const page = buildChoiceRegionalPageForCountry(c);
  if (!page) {
    out.choice.push({ country: c, ok: false, error: "no_page" });
    continue;
  }
  try {
    const res = await fetch(page.url, {
      headers: { "user-agent": "Mozilla/5.0 DealalityLearning/1.0" },
      redirect: "follow",
      signal: AbortSignal.timeout(45000),
    });
    const html = await res.text();
    const blocked = /access denied|robot check|captcha/i.test(html);
    const ldHotels = [...html.matchAll(/"@type"\s*:\s*"Hotel"/g)].length;
    const ids = [...html.matchAll(/\b(?:MX|DO|CR|CO|PA)\d{2,4}\b/g)]
      .slice(0, 8)
      .map((m) => m[0]);
    out.choice.push({
      country: c,
      url: page.url,
      placeId: page.placeId,
      status: res.status,
      blocked,
      ld_hotel_markers: ldHotels,
      sample_ids: [...new Set(ids)],
      html_len: html.length,
    });
  } catch (e) {
    out.choice.push({ country: c, url: page?.url, ok: false, error: e.message });
  }
}

for (const c of ["mexico", "dominican-republic", "costa-rica", "colombia", "panama"]) {
  const url = `https://www.hilton.com/en/locations/${c}/`;
  try {
    const res = await fetch(url, {
      headers: { "user-agent": "Mozilla/5.0 DealalityLearning/1.0" },
      redirect: "follow",
      signal: AbortSignal.timeout(45000),
    });
    const html = await res.text();
    out.hilton_probe.push({
      country: c,
      url,
      status: res.status,
      blocked: /access denied|robot|captcha/i.test(html),
      html_len: html.length,
      title: (html.match(/<title>([^<]+)<\/title>/i) || [])[1] || null,
      mentions_ctyhocn: /ctyhocn/i.test(html),
    });
  } catch (e) {
    out.hilton_probe.push({ country: c, url, error: e.message });
  }
}

mkdirSync(join(ROOT, "reports/research-engine-v2"), { recursive: true });
const fp = join(ROOT, "reports/research-engine-v2/webhound-cala-source-discovery-code-probe.json");
writeFileSync(fp, JSON.stringify(out, null, 2));
console.log(JSON.stringify(out, null, 2));
console.log("wrote", fp);
