#!/usr/bin/env node
/**
 * Ingest authorized production client share URLs into the gitignored sealed
 * catalog used by local Admin Open/Copy. Does not mint tokens, does not touch
 * signing secrets, and does not print bearer values.
 *
 * Usage (env preferred — avoids shell history of full URLs):
 *   GDI_WESTIN_SHARE_URL='https://…/group-demand-intelligence-share.html?share=…' \
 *   GDI_WYNDHAM_SHARE_URL='https://…/group-demand-intelligence-share.html?share=…' \
 *   node scripts/ingest-production-share-sealed-local.mjs
 *
 * Or drop file (gitignored path):
 *   config/client-share/production-share-sealed.drop.json
 *   { "urls": ["https://…?share=…", "https://…?share=…"] }
 *
 * Dry-run: pass --dry-run
 */
import { existsSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = join(dirname(fileURLToPath(import.meta.url)), "..");
const SEALED_PATH = join(ROOT, "config/client-share/production-share-sealed.local.json");
const DROP_PATH = join(ROOT, "config/client-share/production-share-sealed.drop.json");

const KNOWN = {
  "gdisht_30441d3811f87cb4d4f8e16e": {
    product: "GDI",
    hotelId: "recdmo0CnhIZAwHGC",
    label: "The Westin Tampa Waterside",
    pagePath: "/group-demand-intelligence-share.html",
  },
  "gdisht_c09c26841b52009d39698223": {
    product: "GDI",
    hotelId: "recnPB3Q02NO8Ma1o",
    label: "Wyndham San Diego Bayside",
    pagePath: "/group-demand-intelligence-share.html",
  },
  "gdisht_b73f3e698d39fc66f00455f2": {
    product: "GDI",
    hotelId: "recG66DQJKP2c0UNh",
    label: "Renaissance New York Times Square Hotel",
    pagePath: "/group-demand-intelligence-share.html",
  },
  // Optional: six-row ADP Open/Copy (same hotels)
  sht_916fc65700ea6aa430417bcf: {
    product: "ADP",
    hotelId: "recdmo0CnhIZAwHGC",
    propertyId: "adp_westin_tampa_waterside",
    label: "The Westin Tampa Waterside",
    pagePath: "/owner-ai-demand-share.html",
  },
  sht_8ea2781c49c7e6b2a76bbbc5: {
    product: "ADP",
    hotelId: "recnPB3Q02NO8Ma1o",
    propertyId: "adp_wyndham_san_diego_bayside",
    label: "Wyndham San Diego Bayside",
    pagePath: "/owner-ai-demand-share.html",
  },
};

const dryRun = process.argv.includes("--dry-run");

function decodePayload(token) {
  const raw = String(token || "").trim();
  const m = raw.match(/^(gdishare\.v1\.|adpshare\.v1\.)([^.]+)\./);
  if (!m) return null;
  const body = m[2];
  const pad = body.length % 4 === 0 ? "" : "=".repeat(4 - (body.length % 4));
  try {
    return JSON.parse(
      Buffer.from(body.replace(/-/g, "+").replace(/_/g, "/") + pad, "base64").toString("utf8")
    );
  } catch {
    return null;
  }
}

function extractTokenFromUrl(url) {
  try {
    const u = new URL(String(url || "").trim());
    const share = u.searchParams.get("share");
    if (!share) return null;
    return share;
  } catch {
    return null;
  }
}

function collectUrls() {
  const urls = [];
  for (const key of [
    "GDI_WESTIN_SHARE_URL",
    "GDI_WYNDHAM_SHARE_URL",
    "GDI_RENAISSANCE_SHARE_URL",
    "ADP_WESTIN_SHARE_URL",
    "ADP_WYNDHAM_SHARE_URL",
    "DEALALITY_SHARE_URLS",
  ]) {
    const v = String(process.env[key] || "").trim();
    if (!v) continue;
    if (key === "DEALALITY_SHARE_URLS") {
      v.split(/\r?\n|,/).forEach((p) => {
        const s = p.trim();
        if (s) urls.push(s);
      });
    } else {
      urls.push(v);
    }
  }
  if (existsSync(DROP_PATH)) {
    try {
      const drop = JSON.parse(readFileSync(DROP_PATH, "utf8"));
      for (const u of drop.urls || []) {
        if (u) urls.push(String(u).trim());
      }
    } catch (err) {
      console.error("Failed to parse drop file:", err.message);
      process.exit(1);
    }
  }
  return urls.filter(Boolean);
}

function loadSealed() {
  if (!existsSync(SEALED_PATH)) {
    return {
      version: "production-share-sealed-local-v1",
      purpose:
        "Gitignored sealed production client URLs for local Admin resolution. Not a signing secret.",
      tokens: [],
    };
  }
  return JSON.parse(readFileSync(SEALED_PATH, "utf8"));
}

function main() {
  const urls = collectUrls();
  if (!urls.length) {
    console.error(
      [
        "No share URLs provided.",
        "Set GDI_WESTIN_SHARE_URL and GDI_WYNDHAM_SHARE_URL, or write",
        DROP_PATH,
        "with { \"urls\": [\"…\", \"…\"] }.",
      ].join(" ")
    );
    process.exit(2);
  }

  const doc = loadSealed();
  const byTokenId = new Map(
    (doc.tokens || []).map((t) => [String(t.tokenId || ""), t])
  );
  const accepted = [];
  const rejected = [];

  for (const url of urls) {
    const token = extractTokenFromUrl(url);
    if (!token) {
      rejected.push({ reason: "missing_share_query" });
      continue;
    }
    const payload = decodePayload(token);
    const tokenId = payload?.tid || null;
    const known = tokenId ? KNOWN[tokenId] : null;
    if (!known) {
      rejected.push({
        reason: "unknown_or_unsupported_token_id",
        tokenId: tokenId || null,
      });
      continue;
    }
    if (payload.hotelId && payload.hotelId !== known.hotelId) {
      rejected.push({
        reason: "hotel_id_mismatch",
        tokenId,
        expectedHotelId: known.hotelId,
      });
      continue;
    }
    const row = {
      product: known.product,
      hotelId: known.hotelId,
      tokenId,
      token,
      pagePath: known.pagePath,
      label: known.label,
    };
    if (known.propertyId) row.propertyId = known.propertyId;
    byTokenId.set(tokenId, row);
    accepted.push({
      tokenId,
      hotelId: known.hotelId,
      label: known.label,
      product: known.product,
    });
  }

  doc.tokens = Array.from(byTokenId.values());
  doc.updatedAt = new Date().toISOString();

  console.log(
    JSON.stringify(
      {
        dryRun,
        accepted,
        rejected,
        catalogPath: SEALED_PATH,
        tokenCount: doc.tokens.length,
        note: "Bearer values are not printed.",
      },
      null,
      2
    )
  );

  if (!accepted.length) {
    process.exit(1);
  }
  if (dryRun) return;

  mkdirSync(dirname(SEALED_PATH), { recursive: true });
  writeFileSync(SEALED_PATH, JSON.stringify(doc, null, 2) + "\n", "utf8");
}

main();
