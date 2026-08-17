#!/usr/bin/env node
/**
 * Phase 3A.9.1 — read-only audit of all governed IHG Brand Basics rows.
 * No writes. No provider calls.
 */
import "../load-env.js";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import Airtable from "airtable";
import { isBrandStatusActive } from "../lib/brand-status-active.js";
import { normalizeParentCompany } from "../lib/ai-visibility/parent-company-normalize.js";
import {
  loadDecisionEligibilityConfig,
  getBrandDecisionEligibility,
  ACTIVE_SHOWCASE_DECISION_TERRITORIES,
} from "../lib/ai-visibility/brand-decision-eligibility.js";
import { getBrandGeographyEligibility } from "../lib/ai-visibility/brand-geography-eligibility.js";
import { loadPeerSetConfig, PEER_SET_ID_V2 } from "../lib/ai-visibility/peer-sets.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const OUT = path.join(
  __dirname,
  "..",
  "data",
  "ai-visibility",
  "phase3a91-ihg-brand-basics-audit.json"
);

const FIELDS = [
  "Brand Name",
  "Parent Company",
  "Brand Status",
  "Hotel Chain Scale",
  "Brand Model",
  "Region Offered",
  "Branded Residences Status",
];

function cell(v) {
  if (v == null) return null;
  if (Array.isArray(v)) {
    if (v.every((x) => typeof x === "string" || (x && typeof x === "object" && x.name != null))) {
      return v.map((x) => (typeof x === "string" ? x : String(x.name || "").trim())).filter(Boolean);
    }
    return cell(v[0]);
  }
  if (typeof v === "object" && v.name != null) return String(v.name).trim();
  const s = String(v).trim();
  return s || null;
}

const apiKey = process.env.AIRTABLE_API_KEY;
const baseId = process.env.AIRTABLE_BASE_ID;
if (!apiKey || !baseId) {
  console.error("Missing AIRTABLE_API_KEY / AIRTABLE_BASE_ID");
  process.exit(1);
}

const base = new Airtable({ apiKey }).base(baseId);
const table = process.env.AIRTABLE_BRAND_SETUP_BASICS_TABLE || "Brand Setup - Brand Basics";

// Parent Company containing IHG / InterContinental (governed live rows)
const formula =
  "OR(FIND('IHG',{Parent Company}&''),FIND('InterContinental',{Parent Company}&''),FIND('Intercontinental',{Parent Company}&''))";

const recs = await base(table).select({ filterByFormula: formula, fields: FIELDS }).all();

const peerCfg = loadPeerSetConfig();
const peerV2 = (peerCfg.peerSets || []).find((p) => p.peerSetId === PEER_SET_ID_V2);
const peerIds = new Set(peerV2?.entityIds || []);
const eligCfg = loadDecisionEligibilityConfig();

const rows = recs.map((r) => {
  const brandName = cell(r.fields["Brand Name"]);
  const parentRaw = Array.isArray(cell(r.fields["Parent Company"]))
    ? cell(r.fields["Parent Company"])[0]
    : cell(r.fields["Parent Company"]);
  const parentNorm = normalizeParentCompany(
    Array.isArray(parentRaw) ? parentRaw[0] : parentRaw
  );
  const status = Array.isArray(cell(r.fields["Brand Status"]))
    ? cell(r.fields["Brand Status"])[0]
    : cell(r.fields["Brand Status"]);
  const chainScale = Array.isArray(cell(r.fields["Hotel Chain Scale"]))
    ? cell(r.fields["Hotel Chain Scale"])[0]
    : cell(r.fields["Hotel Chain Scale"]);
  const brandModel = Array.isArray(cell(r.fields["Brand Model"]))
    ? cell(r.fields["Brand Model"])[0]
    : cell(r.fields["Brand Model"]);
  const regionOffered = cell(r.fields["Region Offered"]);
  const residences = Array.isArray(cell(r.fields["Branded Residences Status"]))
    ? cell(r.fields["Branded Residences Status"])[0]
    : cell(r.fields["Branded Residences Status"]);

  const inPeer = peerIds.has(r.id);
  const eligibility = {};
  for (const t of ACTIVE_SHOWCASE_DECISION_TERRITORIES) {
    eligibility[t] = getBrandDecisionEligibility(r.id, t, eligCfg).state;
  }
  let geo = null;
  try {
    geo = getBrandGeographyEligibility(r.id);
  } catch {
    geo = { GLOBAL: "UNKNOWN", CALA: "UNKNOWN", EUROPE: "UNKNOWN", NORTH_AMERICA: "UNKNOWN", MEXICO: "UNKNOWN" };
  }

  const activeLive = isBrandStatusActive(status);
  const scale = String(chainScale || "").toLowerCase();
  const model = String(brandModel || "").toLowerCase();
  const uuRelevant =
    scale.includes("upper upscale") ||
    scale.includes("upscale") ||
    model.includes("collection") ||
    model.includes("soft");
  const lifestyleHint =
    /indigo|kimpton|voco|even|atwell|intercontinental|crowne|regent|six senses|hotel indigo/i.test(
      String(brandName || "")
    );

  return {
    BRAND: brandName,
    BRAND_ID: r.id,
    ACTIVE_LIVE_STATUS: status,
    ACTIVE_LIVE: activeLive,
    CURRENT_PARENT: Array.isArray(parentRaw) ? parentRaw[0] || parentRaw : parentRaw,
    CANONICAL_PARENT: parentNorm.canonical,
    PARENT_NORMALIZATION_REQUIRED: parentNorm.normalizationRequired,
    CHAIN_SCALE: chainScale,
    BRAND_MODEL: brandModel,
    REGION_OFFERED: regionOffered,
    BRANDED_RESIDENCES_STATUS: residences,
    CURRENT_PEER_SET_MEMBERSHIP: inPeer ? PEER_SET_ID_V2 : null,
    CURRENT_SHOWCASE_ELIGIBILITY: eligibility,
    GEOGRAPHY_DEVELOPMENT_ELIGIBILITY: geo
      ? {
          GLOBAL: geo.GLOBAL,
          CALA: geo.CALA,
          EUROPE: geo.EUROPE,
          NORTH_AMERICA: geo.NORTH_AMERICA,
          MEXICO: geo.MEXICO,
        }
      : null,
    SHOWCASE_STRATEGY_RELEVANT: Boolean(activeLive && uuRelevant),
    LIFESTYLE_NAME_HINT: Boolean(lifestyleHint),
    SOURCE: "Brand Setup - Brand Basics",
    QUALITY: activeLive && parentNorm.canonical === "IHG" ? "HIGH" : "MEDIUM",
  };
});

rows.sort((a, b) => String(a.BRAND || "").localeCompare(String(b.BRAND || "")));

const report = {
  generatedAt: new Date().toISOString(),
  LIVE_PROVIDER_CALLS: 0,
  AIRTABLE_WRITES: 0,
  TOTAL_IHG_ROWS: rows.length,
  ACTIVE_LIVE_COUNT: rows.filter((r) => r.ACTIVE_LIVE).length,
  IN_PEER_V2: rows.filter((r) => r.CURRENT_PEER_SET_MEMBERSHIP).map((r) => r.BRAND),
  rows,
};

fs.mkdirSync(path.dirname(OUT), { recursive: true });
fs.writeFileSync(OUT, JSON.stringify(report, null, 2), "utf8");
console.log(
  JSON.stringify(
    {
      TOTAL_IHG_ROWS: report.TOTAL_IHG_ROWS,
      ACTIVE_LIVE_COUNT: report.ACTIVE_LIVE_COUNT,
      IN_PEER_V2: report.IN_PEER_V2,
      brands: rows.map((r) => ({
        brand: r.BRAND,
        id: r.BRAND_ID,
        active: r.ACTIVE_LIVE,
        scale: r.CHAIN_SCALE,
        model: r.BRAND_MODEL,
        peer: Boolean(r.CURRENT_PEER_SET_MEMBERSHIP),
        relevant: r.SHOWCASE_STRATEGY_RELEVANT,
      })),
    },
    null,
    2
  )
);
console.log(`\nWrote ${OUT}`);
