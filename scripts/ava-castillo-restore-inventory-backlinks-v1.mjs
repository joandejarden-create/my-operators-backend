/**
 * Restore Castillo inventory-only verified backlinks that pass evidence checks;
 * dedupe Visit Palma http/https in customer sources.
 *
 *   node scripts/ava-castillo-restore-inventory-backlinks-v1.mjs
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

function urlKey(u) {
  try {
    const x = new URL(u);
    return `${x.hostname.replace(/^www\./, "")}${x.pathname.replace(/\/$/, "")}`.toLowerCase();
  } catch {
    return String(u || "")
      .toLowerCase()
      .replace(/^https?:\/\//, "")
      .replace(/^www\./, "")
      .replace(/\/$/, "");
  }
}

function linkingUrl(s) {
  return s?.linkingPageUrl || s?.evidenceUrl || s?.url || null;
}

async function verifyCandidate(invRow) {
  const u = linkingUrl(invRow);
  const page = await fetchHtml(u, { timeoutMs: 20000 });
  if (!page.ok) {
    return {
      ok: false,
      reason: page.status === 403 || page.status === 401 ? "ACCESS_RESTRICTED" : `HTTP_${page.status}`,
      status: page.status,
    };
  }
  const body = String(page.body || "").toLowerCase();
  const dest = String(invRow.hotelDestinationUrl || "").toLowerCase();
  const hasHotelHref =
    /castillosonvidamallorca\.com/i.test(body) ||
    /marriott\.com[^\"'\s]*pmilc/i.test(body) ||
    (dest && body.includes(dest.replace(/^https?:\/\//, "").slice(0, 40)));
  const mentionsHotel =
    /castillo\s+hotel\s+son\s+vida|castillo\s+son\s+vida/i.test(page.body || "");
  if (!hasHotelHref && !mentionsHotel) {
    return { ok: false, reason: "NO_HOTEL_SIGNAL", status: page.status };
  }
  // Prefer hyperlink for VERIFIED_BACKLINK; mention-only → keep inventory
  if (!hasHotelHref) {
    return { ok: false, reason: "MENTION_ONLY_NO_HREF", status: page.status };
  }
  return { ok: true, status: page.status, finalUrl: page.finalUrl };
}

async function main() {
  const j = JSON.parse(fs.readFileSync(PAYLOAD, "utf8"));
  const da = j.digitalAuthority;
  const before = {
    sources: (da.sources || []).length,
    bl: (da.sources || []).filter((s) => s.findingClass === "VERIFIED_BACKLINK").length,
    inventoryBl: (da.researchInventory || []).filter((s) => s.findingClass === "VERIFIED_BACKLINK")
      .length,
  };

  // Deduplicate Visit Palma in customer sources (prefer https)
  const sources = [];
  const seen = new Set();
  for (const s of da.sources || []) {
    const u = linkingUrl(s);
    const k = urlKey(u);
    if (seen.has(k)) {
      continue;
    }
    // Prefer https variant if both exist — when we see http, check if https later; 
    // two-pass: collect, then for visit-palma keep https only
    sources.push(s);
    seen.add(k);
  }
  // Second pass: if both http and https visit-palma somehow both keyed same, already deduped.
  // Force canonical https for visit-palma
  for (const s of sources) {
    const u = linkingUrl(s);
    if (/visit-palma\.com\/hotel-castillo-son-vida/i.test(u || "") && /^http:/i.test(u)) {
      s.linkingPageUrl = u.replace(/^http:/i, "https:");
      if (s.evidenceUrl) s.evidenceUrl = s.evidenceUrl.replace(/^http:/i, "https:");
    }
  }

  const customerKeys = new Set(sources.map((s) => urlKey(linkingUrl(s))).filter(Boolean));
  const restored = [];
  const rejected = [];

  for (const cand of CANDIDATES) {
    const invRow = (da.researchInventory || []).find(
      (s) =>
        s.findingClass === "VERIFIED_BACKLINK" && urlKey(linkingUrl(s)) === urlKey(cand)
    );
    if (!invRow) {
      rejected.push({ url: cand, reason: "NOT_IN_INVENTORY" });
      continue;
    }
    if (customerKeys.has(urlKey(cand))) {
      rejected.push({ url: cand, reason: "ALREADY_IN_CUSTOMER_SOURCES" });
      continue;
    }
    const v = await verifyCandidate(invRow);
    if (!v.ok) {
      rejected.push({ url: cand, reason: v.reason, status: v.status });
      continue;
    }
    const row = {
      ...invRow,
      linkingPageUrl: cand.startsWith("http") ? cand : invRow.linkingPageUrl,
      evidenceUrl: cand,
      restoredFromInventoryAt: new Date().toISOString(),
      restoreVerification: {
        httpStatus: v.status,
        finalUrl: v.finalUrl,
        method: "fetchHtml_href_check",
      },
    };
    sources.push(row);
    customerKeys.add(urlKey(cand));
    restored.push({ url: cand, org: invRow.referringOrganization || invRow.title });
  }

  da.sources = sources;
  // Keep researchInventory intact (provenance)
  if (da.discoveryCoverage) {
    const bl = sources.filter((s) => s.findingClass === "VERIFIED_BACKLINK");
    const ment = sources.filter((s) => s.findingClass === "VERIFIED_EXTERNAL_MENTION");
    da.discoveryCoverage.customerFacingSources = sources.length;
    da.discoveryCoverage.customerFacingVerifiedBacklinks = bl.length;
    da.discoveryCoverage.customerFacingVerifiedMentions = ment.length;
    da.discoveryCoverage.castilloRestoreNote =
      "2026-11-05: restored inventory BLs that passed href verification; Visit Palma deduped to unique linking page.";
  }

  // Light executive / snapshot language refresh if present
  if (typeof da.executiveFinding === "string") {
    const blN = sources.filter((s) => s.findingClass === "VERIFIED_BACKLINK").length;
    da.executiveFinding = da.executiveFinding.replace(
      /\b\d+\s+verified (website )?links?\b/i,
      `${blN} verified website links`
    );
  }

  fs.writeFileSync(PAYLOAD, `${JSON.stringify(j, null, 2)}\n`);
  const after = {
    sources: sources.length,
    bl: sources.filter((s) => s.findingClass === "VERIFIED_BACKLINK").length,
    uniqueBl: new Set(
      sources
        .filter((s) => s.findingClass === "VERIFIED_BACKLINK")
        .map((s) => urlKey(linkingUrl(s)))
    ).size,
    mentions: sources.filter((s) => s.findingClass === "VERIFIED_EXTERNAL_MENTION").length,
  };
  const report = {
    contract: "AVA_CASTILLO_INVENTORY_BACKLINK_RESTORE_V1",
    at: new Date().toISOString(),
    before,
    after,
    restored,
    rejected,
  };
  fs.writeFileSync(
    path.join(
      ROOT,
      "reports/dealality-ai-visibility-audit-action-plan-pilot-v1/AVA_CASTILLO_INVENTORY_BACKLINK_RESTORE.json"
    ),
    `${JSON.stringify(report, null, 2)}\n`
  );
  console.log(JSON.stringify(report, null, 2));
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
