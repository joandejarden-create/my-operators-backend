#!/usr/bin/env node
/**
 * HE header actions hydration audit (Directions / Website / Phone).
 *
 * When HPC has verified Official Property URL / Phone, ownership hotel +
 * sharedHotelView must carry them. Missing verified values FAIL.
 * Does not invent org/sponsor websites or unrelated phones.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  formatHotelPropertyCensusAsBrandPresenceHotel,
  HOTEL_PROPERTY_CENSUS_TABLE,
} from "../../lib/hotel-intelligence/radar-internal-preview-hotels-v1.js";
import { config as loadDotenv } from "dotenv";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../..");
const dotenvPath =
  process.env.DOTENV_CONFIG_PATH ||
  path.join("C:/Dev/dealality/my-operators-backend", ".env");
loadDotenv({ path: dotenvPath });
const BASE = process.env.DEALALITY_LOCAL_BASE_URL || "http://127.0.0.1:8080";
const batchPath = path.join(ROOT, "configs/hotel-explorer/he50-eight-hotel-batch-v1.json");

const hotelIdArg = process.argv.find((a) => a.startsWith("--hotelId="));
const singleId = hotelIdArg ? hotelIdArg.slice("--hotelId=".length) : null;

function text(v) {
  const s = String(v == null ? "" : v).trim();
  return s || null;
}

function digits(v) {
  return String(v || "").replace(/\D/g, "");
}

function normalizeUrl(u) {
  const s = text(u);
  if (!s) return null;
  if (/^https?:\/\//i.test(s)) return s;
  return `https://${s}`;
}

async function loadHpc(hotelId) {
  const key = process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT;
  const baseId = process.env.AIRTABLE_BASE_ID_ALT || process.env.AIRTABLE_BASE_ID;
  if (!key || !baseId) return { hotel: null, error: "airtable_not_configured" };
  try {
    const Airtable = (await import("airtable")).default;
    const base = new Airtable({ apiKey: key }).base(baseId);
    const rec = await base(HOTEL_PROPERTY_CENSUS_TABLE).find(hotelId);
    return { hotel: formatHotelPropertyCensusAsBrandPresenceHotel(rec), error: null };
  } catch (err) {
    return { hotel: null, error: String(err?.message || err).slice(0, 240) };
  }
}

async function probe(hotel) {
  const hpcLoaded = await loadHpc(hotel.hotelId);
  const hpc = hpcLoaded.hotel || {};
  const expectedWebsite = normalizeUrl(hpc.website);
  const expectedPhone = text(hpc.telephone || hpc.phone);

  const url = `${BASE}/api/golden-demo/ownership/hotel/${encodeURIComponent(hotel.hotelId)}`;
  const res = await fetch(url, { headers: { Accept: "application/json" } });
  const json = res.ok ? await res.json() : null;
  const h = json?.hotel || {};
  const shared = json?.sharedHotelView || {};
  const corp = json?.report?.corporate_contacts || {};

  const website =
    normalizeUrl(h.website) ||
    normalizeUrl(h.hotelWebsite) ||
    normalizeUrl(shared.website) ||
    (corp.source === "HPC_PROPERTY_FALLBACK" ? normalizeUrl(corp.website) : null);
  const telephone =
    text(h.telephone) ||
    text(h.phone) ||
    text(shared.telephone) ||
    text(shared.phone) ||
    text(corp.property_phone);

  const failReasons = [];
  if (!res.ok) failReasons.push(`ownership_http_${res.status}`);
  if (hpcLoaded.error) failReasons.push(`hpc_load_${hpcLoaded.error}`);

  if (expectedWebsite) {
    if (!website) failReasons.push("verified_website_not_hydrated");
    else if (!/^https?:\/\//i.test(website)) failReasons.push("website_missing_scheme");
    if (expectedWebsite && website) {
      const a = expectedWebsite.toLowerCase().replace(/\/$/, "");
      const b = website.toLowerCase().replace(/\/$/, "");
      if (!b.includes(a.replace(/^https?:\/\//, "").slice(0, 24)) && a !== b) {
        // Soft hostname presence check — require shared + hotel carry a URL when HPC has one.
        if (!shared.website) failReasons.push("sharedHotelView.website_missing");
      }
    }
    if (!shared.website) failReasons.push("sharedHotelView.website_missing");
    if (!h.website && !h.hotelWebsite) failReasons.push("hotel.website_missing");
  }

  if (expectedPhone) {
    if (!telephone) failReasons.push("verified_telephone_not_hydrated");
    const expD = digits(expectedPhone);
    const gotD = digits(telephone);
    if (expD && gotD && !gotD.includes(expD) && !expD.includes(gotD)) {
      failReasons.push("telephone_mismatch_vs_hpc");
    }
    if (!(shared.telephone || shared.phone)) failReasons.push("sharedHotelView.telephone_missing");
    if (!h.telephone && !h.phone) failReasons.push("hotel.telephone_missing");
  }

  // Bethesda hard lock when that hotel is in scope and HPC has values.
  if (hotel.hotelId === "recLuxvwwxID7U2B8" && (expectedWebsite || expectedPhone)) {
    if (!/marriott\.com/i.test(String(website || ""))) failReasons.push("bethesda_website_not_marriott");
    const d = digits(telephone);
    if (!(d.includes("301") && d.includes("8979400"))) failReasons.push("bethesda_phone_not_switchboard");
    if (/mullencap/i.test(String(website || ""))) failReasons.push("bethesda_website_mullen_leak");
  }

  // Never allow org-general mullencap as property website when a property URL is expected.
  if (expectedWebsite && /mullencap/i.test(String(website || ""))) {
    failReasons.push("sponsor_website_leak");
  }

  const uniqueFails = [...new Set(failReasons)];
  const pass = uniqueFails.length === 0;

  return {
    hotelId: hotel.hotelId,
    hotelName: hotel.hotelName || h.name || hpc.name || null,
    status: res.status,
    pass,
    expectedWebsite,
    expectedPhone,
    website,
    telephone,
    sharedWebsite: shared.website || null,
    sharedTelephone: shared.telephone || shared.phone || null,
    corpSource: corp.source || null,
    hpcError: hpcLoaded.error,
    failReasons: uniqueFails,
  };
}

const batch = JSON.parse(fs.readFileSync(batchPath, "utf8"));
const hotels = singleId
  ? batch.hotels?.filter((h) => h.hotelId === singleId) || [{ hotelId: singleId, hotelName: singleId }]
  : batch.hotels || [];

const rows = [];
for (const hotel of hotels) {
  rows.push(await probe(hotel));
}

const ok = rows.every((r) => r.pass);
const outDir = path.join(ROOT, "reports/hotel-explorer");
fs.mkdirSync(outDir, { recursive: true });
const outPath = path.join(outDir, "HE-HEADER-ACTIONS-HYDRATION-V1.json");
const report = {
  version: "he-header-actions-hydration-v1",
  base: BASE,
  ok,
  hotelCount: rows.length,
  passCount: rows.filter((r) => r.pass).length,
  failCount: rows.filter((r) => !r.pass).length,
  rows,
  generatedAt: new Date().toISOString(),
};
fs.writeFileSync(outPath, JSON.stringify(report, null, 2));
console.log(
  JSON.stringify(
    {
      ok,
      outPath,
      passCount: report.passCount,
      failCount: report.failCount,
      fails: rows.filter((r) => !r.pass).map((r) => ({
        hotelId: r.hotelId,
        hotelName: r.hotelName,
        failReasons: r.failReasons,
      })),
    },
    null,
    2
  )
);
process.exit(ok ? 0 : 1);
