/**
 * Probe Mexico RNT portal URLs (official vs deprecated).
 *   node scripts/probe-mx-rnt-portal.mjs
 */
import {
  MX_RNT_PORTAL_OFFICIAL,
  MX_RNT_PORTAL_TRAMITES,
  MX_RNT_PORTAL_DEPRECATED,
  MX_RNT_PORTAL_FALLBACKS,
} from "../lib/gtm-owner-target/adapters/mx-rnt-portal-config.js";

const URLS = [
  { label: "official_consulta", url: MX_RNT_PORTAL_OFFICIAL },
  { label: "tramites", url: MX_RNT_PORTAL_TRAMITES },
  { label: "deprecated_mirror", url: MX_RNT_PORTAL_DEPRECATED },
];

/** @type {{ label: string, url: string, ok: boolean, status?: number, finalUrl?: string, error?: string }[]} */
const results = [];

for (const entry of URLS) {
  try {
    const res = await fetch(entry.url, {
      redirect: "follow",
      headers: { "User-Agent": "DealCapture-GTM-Registry-Probe/1.0" },
      signal: AbortSignal.timeout(20000),
    });
    results.push({
      label: entry.label,
      url: entry.url,
      ok: res.ok,
      status: res.status,
      finalUrl: res.url,
    });
    console.log(entry.label, entry.url, "→", res.status, res.url);
  } catch (err) {
    const message = err.cause?.code || err.message || String(err);
    results.push({ label: entry.label, url: entry.url, ok: false, error: message });
    console.log(entry.label, entry.url, "→ FAIL", message);
  }
}

console.log("\nRecommended portal:", MX_RNT_PORTAL_OFFICIAL);
console.log("Deprecated (do not use):", MX_RNT_PORTAL_DEPRECATED);
console.log("Fallback list:", MX_RNT_PORTAL_FALLBACKS.join(", "));

const official = results.find((r) => r.label === "official_consulta");
if (!official?.ok) {
  console.warn(
    "\nOfficial RNT consulta unreachable from this network. Use SIGER-first path for direct_entity owners."
  );
}
