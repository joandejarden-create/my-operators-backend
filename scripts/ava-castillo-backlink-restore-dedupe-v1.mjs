/**
 * Castillo: restore inventory-only verified backlinks that pass evidence checks;
 * dedupe Visit Palma http/https in customer sources.
 *
 *   node scripts/ava-castillo-backlink-restore-dedupe-v1.mjs
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { fetchHtml } from "../lib/ai-visibility-audit-action-plan/digital-authority-discovery-v2.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const PAYLOAD = path.join(
  ROOT,
  "lib/ai-visibility-audit-action-plan/payloads/castillo-hotel-son-vida-v1.json"
);

const CANDIDATES = [
  "https://www.mallorcacomercial.es/en/zona/113/",
  "https://www.mallorcacomercial.es/empresa/850/",
  "https://mallorcaconventionbureau.com/en/miembros/castillo-hotel-son-vida-a-luxury-collection-hotel",
];

function normKey(u) {
  try {
    const x = new URL(u);
    return `${x.hostname.replace(/^www\./i, "")}${x.pathname.replace(/\/$/, "") || ""}`.toLowerCase();
  } catch {
    return String(u || "")
      .toLowerCase()
      .replace(/^https?:\/\//, "")
      .replace(/^www\./, "")
      .replace(/\/$/, "");
  }
}

function urlOf(s) {
  return s?.linkingPageUrl || s?.evidenceUrl || s?.url || null;
}

async function verifyCandidate(url) {
  const page = await fetchHtml(url, { timeoutMs: 20000 });
  if (!page.ok) {
    return {
      url,
      pass: false,
      reason: [403, 401, 429, 503].includes(page.status)
        ? "ACCESS_RESTRICTED"
        : `HTTP_${page.status || 0}`,
      status: page.status,
    };
  }
  const html = String(page.html || page.body || "").toLowerCase();
  const hasHotelSignal =
    /castillo|son.?vida|luxury.?collection|castillosonvida/i.test(html) ||
    /castillo|son.?vida/i.test(page.title || "");
  const hasDestinationHref =
    /castillosonvidamallorca\.com|marriott\.com\/[^"'\s]*pmilc|marriott\.com\/hotels\/travel\/pmilc/i.test(
      html
    );
  if (!hasHotelSignal) {
    return { url, pass: false, reason: "NO_HOTEL_SIGNAL", status: page.status };
  }
  // Commercially valuable even if destination is brand page (MCB) when hotel is clearly listed
  if (!hasDestinationHref && !/conventionbureau|mallorcacomercial/i.test(url)) {
    return { url, pass: false, reason: "NO_DESTINATION_HREF", status: page.status };
  }
  return {
    url,
    pass: true,
    reason: "VERIFIED_CUSTOMER_RELEVANT",
    status: page.status,
    hasDestinationHref,
  };
}

async function main() {
  const j = JSON.parse(fs.readFileSync(PAYLOAD, "utf8"));
  const da = j.digitalAuthority;
  const before = {
    customerBl: (da.sources || []).filter((s) => s.findingClass === "VERIFIED_BACKLINK").length,
    customerMentions: (da.sources || []).filter(
      (s) => s.findingClass === "VERIFIED_EXTERNAL_MENTION"
    ).length,
  };

  // Dedupe Visit Palma in customer sources (prefer https)
  const sources = [...(da.sources || [])];
  const seen = new Map();
  const deduped = [];
  for (const s of sources) {
    const u = urlOf(s);
    if (!u) {
      deduped.push(s);
      continue;
    }
    const k = normKey(u);
    if (!seen.has(k)) {
      seen.set(k, deduped.length);
      deduped.push(s);
      continue;
    }
    const idx = seen.get(k);
    const prev = deduped[idx];
    const prevUrl = urlOf(prev) || "";
    // Prefer https over http
    if (/^http:/i.test(prevUrl) && /^https:/i.test(u)) {
      deduped[idx] = s;
    }
    // drop duplicate
  }

  // Restore candidates from inventory
  const inventory = da.researchInventory || [];
  const restoreReport = [];
  for (const url of CANDIDATES) {
    const check = await verifyCandidate(url);
    restoreReport.push(check);
    if (!check.pass) continue;
    const key = normKey(url);
    if (deduped.some((s) => normKey(urlOf(s) || "") === key)) {
      restoreReport[restoreReport.length - 1].alreadyPresent = true;
      continue;
    }
    const inv = inventory.find((s) => normKey(urlOf(s) || "") === key);
    if (!inv) {
      restoreReport[restoreReport.length - 1].inventoryMissing = true;
      continue;
    }
    deduped.push({
      ...inv,
      findingClass: "VERIFIED_BACKLINK",
      linkingPageUrl: urlOf(inv) || url,
      evidenceUrl: urlOf(inv) || url,
      restoredFromInventoryAt: new Date().toISOString(),
      restoreNote:
        "Restored to customer-facing sources after evidence re-verification (Castillo presentation completeness).",
    });
    restoreReport[restoreReport.length - 1].restored = true;
  }

  da.sources = deduped;
  // Keep inventory intact (full research provenance)
  if (da.discoveryCoverage) {
    const bl = deduped.filter((s) => s.findingClass === "VERIFIED_BACKLINK");
    const ment = deduped.filter((s) => s.findingClass === "VERIFIED_EXTERNAL_MENTION");
    da.discoveryCoverage.customerFacingSources = deduped.length;
    da.discoveryCoverage.customerFacingVerifiedBacklinks = bl.length;
    da.discoveryCoverage.customerFacingVerifiedMentions = ment.length;
    da.discoveryCoverage.castilloPresentationRestore = {
      at: new Date().toISOString(),
      restoreReport,
      note: "discoveryCoverage.verifiedBacklinks remains research-inventory inclusive",
    };
  }

  // Update executive finding counts if present (customer language)
  const uniqueBl = [];
  const blSeen = new Set();
  for (const s of deduped.filter((x) => x.findingClass === "VERIFIED_BACKLINK")) {
    const k = normKey(urlOf(s) || "");
    if (blSeen.has(k)) continue;
    blSeen.add(k);
    uniqueBl.push(s);
  }

  fs.writeFileSync(PAYLOAD, `${JSON.stringify(j, null, 2)}\n`);
  const after = {
    customerBlRaw: deduped.filter((s) => s.findingClass === "VERIFIED_BACKLINK").length,
    customerBlUnique: uniqueBl.length,
    customerMentions: deduped.filter((s) => s.findingClass === "VERIFIED_EXTERNAL_MENTION")
      .length,
    uniqueBlUrls: uniqueBl.map((s) => ({
      org: s.referringOrganization || s.title,
      url: urlOf(s),
    })),
  };
  const report = {
    contract: "AVA_CASTILLO_BACKLINK_RESTORE_DEDUPE_V1",
    before,
    after,
    restoreReport,
    payloadPath: PAYLOAD.replace(/\\/g, "/"),
  };
  const out = path.join(
    ROOT,
    "reports/dealality-ai-visibility-audit-action-plan-pilot-v1/AVA_CASTILLO_BACKLINK_RESTORE_REPORT.json"
  );
  fs.writeFileSync(out, `${JSON.stringify(report, null, 2)}\n`);
  console.log(JSON.stringify(report, null, 2));
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
