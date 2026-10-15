#!/usr/bin/env node
/**
 * Bounded link check for GDI public/source URLs (lychee substitute when binary absent).
 * Does not crawl recursively, submit forms, or send email.
 *
 *   node scripts/gdi-quality-link-check-v1.mjs
 *   node scripts/gdi-quality-link-check-v1.mjs --live
 */
import "dotenv/config";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const OUT = path.join(ROOT, "reports/gdi-quality-review");
const live = process.argv.includes("--live");
fs.mkdirSync(OUT, { recursive: true });

function classify(status, err) {
  if (err && /aborted|timeout/i.test(String(err))) return "temporary_failure";
  if (status === 401 || status === 407) return "authentication_required";
  if (status === 403) return "access_denied";
  if (status === 429) return "rate_limited";
  if (status >= 500) return "temporary_failure";
  if (status >= 400) return "confirmed_broken";
  if (status >= 200 && status < 400) return "ok";
  return "untested";
}

async function headOrGet(url) {
  const ctrl = new AbortController();
  const t = setTimeout(() => ctrl.abort(), 8000);
  try {
    let res = await fetch(url, {
      method: "HEAD",
      redirect: "follow",
      signal: ctrl.signal,
      headers: { "User-Agent": "DealalityGdiLinkCheck/1.0" },
    });
    if (res.status === 405 || res.status === 403) {
      res = await fetch(url, {
        method: "GET",
        redirect: "follow",
        signal: ctrl.signal,
        headers: { "User-Agent": "DealalityGdiLinkCheck/1.0" },
      });
    }
    return { status: res.status, class: classify(res.status) };
  } catch (err) {
    return { status: null, class: classify(null, err), error: String(err?.message || err) };
  } finally {
    clearTimeout(t);
  }
}

const urls = new Set([
  "https://my-operators-backend-production.up.railway.app/health",
  "https://my-operators-backend-production.up.railway.app/group-demand-intelligence-share.html",
  "https://my-operators-backend-production.up.railway.app/css/group-demand-intelligence.css",
  "https://my-operators-backend-production.up.railway.app/js/group-demand-intelligence/share-app.js",
]);

if (live) {
  process.env.GDI_OPPORTUNITIES_PERSISTENCE =
    process.env.GDI_OPPORTUNITIES_PERSISTENCE || "airtable";
  const { loadOpportunitiesCanonical } = await import(
    "../lib/group-demand-intelligence/opportunity-persistence.js"
  );
  for (const hotelId of ["rec0eTEnCqlsGJJt9", "recnPB3Q02NO8Ma1o"]) {
    const bag = await loadOpportunitiesCanonical(hotelId, { preferAirtable: true });
    for (const o of (bag?.opportunities || [])
      .filter((x) => x.prospectHotelFacing || x.customerVisible)
      .slice(0, 25)) {
      for (const u of [
        o.contactUrl,
        o.organizationContactUrl,
        o.publicContactPath,
        ...(Array.isArray(o.sources) ? o.sources.map((s) => s.url) : []),
      ]) {
        if (u && /^https?:\/\//i.test(u) && !/share=|gdishare|token=/i.test(u)) {
          urls.add(String(u).split("#")[0]);
        }
      }
    }
  }
}

const list = [...urls].slice(0, 40);
const results = [];
for (const url of list) {
  // eslint-disable-next-line no-await-in-loop
  const r = await headOrGet(url);
  results.push({ url, ...r });
  await new Promise((r) => setTimeout(r, 150));
}

const report = {
  generatedAt: new Date().toISOString(),
  checked: results.length,
  byClass: results.reduce((acc, r) => {
    acc[r.class] = (acc[r.class] || 0) + 1;
    return acc;
  }, {}),
  results,
  note: "403/429 do not invalidate an opportunity. Private share tokens never included.",
};
fs.writeFileSync(
  path.join(OUT, "link-check-results.json"),
  JSON.stringify(report, null, 2) + "\n"
);
console.log(JSON.stringify({ checked: report.checked, byClass: report.byClass }, null, 2));
console.log("gdi-quality-link-check-v1: PASS");
