/**
 * Load Operator Explorer quality-baseline fixture packs into a prefill-like map.
 * Used by Tab Factory audit when --source=fixtures|merged.
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { applyOperatingPlatformToLegacyPrefill } from "../../api/lib/operator-operating-platform-map.js";
import { applyBrandRelationshipsToLegacyPrefill } from "../../api/lib/operator-brand-relationships-map.js";
import { applyEngagementReportingToLegacyPrefill } from "../../api/lib/operator-engagement-reporting-map.js";
import { applyInfrastructurePlatformToLegacyPrefill } from "../../api/lib/operator-infrastructure-platform-map.js";
import {
  getOperatorQualityBaselineEntry,
} from "./operator-explorer-quality-baseline.js";
import { getOperatorFactoryQueueEntry } from "./operator-explorer-factory-queue.js";
import { GHL_HOTELES_FACTORY_CONTENT } from "./operator-explorer-factory-content-ghl-hoteles.js";
import { AIMBRIDGE_LATAM_FACTORY_CONTENT } from "./operator-explorer-factory-content-aimbridge-latam.js";
import { BRAND_MANAGED_FACTORY_CONTENT_BY_SLUG } from "./operator-explorer-factory-content-brand-managed.js";
import { PLAYA_FACTORY_CONTENT } from "./operator-explorer-factory-content-playa.js";
import { buildFullOperatorExplorerRegistry } from "./operator-explorer-registry-catalog.js";

const FACTORY_CONTENT_BY_SLUG = Object.freeze({
  "ghl-hoteles": GHL_HOTELES_FACTORY_CONTENT,
  "aimbridge-latam": AIMBRIDGE_LATAM_FACTORY_CONTENT,
  ...BRAND_MANAGED_FACTORY_CONTENT_BY_SLUG,
  "playa-hotels-resorts": PLAYA_FACTORY_CONTENT,
});

const __dirname = path.dirname(fileURLToPath(import.meta.url));
export const ROOT = path.resolve(__dirname, "../..");

const KNOWN_NESTED_BAGS = Object.freeze([
  "profileFields",
  "platformFields",
  "brandFields",
  "marketsFields",
  "engagementFields",
  "infrastructureFields",
  "leadershipFields",
  "bestFitFields",
  "commercialFields",
  "recognitionFields",
  "materialsFields",
  "footprintGeoFields",
]);

/** Airtable / fixture display labels → prefill camelCase */
const LABEL_TO_PREFILL = Object.freeze({
  "Active Countries": "activeCountries",
  "Active Markets / Cities": "activeMarkets",
  "Priority / Target Markets": "priorityMarkets",
  "Target Growth Markets": "targetGrowthMarkets",
  "Company History": "companyHistory",
  "Mission Statement": "missionStatement",
  "Parent Company": "parentCompany",
  "Industry Recognition": "industryRecognition",
  "Notable Achievements": "notableAchievements",
});

const PREFILL_PREFIXES = Object.freeze([
  "overview_",
  "cap_",
  "brand_",
  "mkt_",
  "ov_",
  "infra_",
  "risk_",
  "lead_",
  "bf_",
  "tr_",
  "systems_",
  "exec_",
  "op_",
]);

function nz(v) {
  return v == null ? "" : String(v).trim();
}

function fixtureSuffixForSlug(slug) {
  if (slug === "arbor-lodging-cala") return "arbor-cala";
  if (slug === "hotel-equities-cala") return "he-cala";
  if (slug === "ghl-hoteles") return "ghl-hoteles";
  if (slug === "aimbridge-latam") return "aimbridge-latam";
  if (slug === "viento-sur-gestion-hotelera") return "viento-sur";
  if (getOperatorFactoryQueueEntry(slug)) return slug;
  return null;
}

function resolveOperatorFixtureIdentity(slugOrRecordId) {
  const baseline = getOperatorQualityBaselineEntry(slugOrRecordId);
  if (baseline) {
    return {
      slug: baseline.slug,
      recordId: baseline.recordId,
      companyName: baseline.companyName,
      kind: "quality_baseline",
    };
  }
  const queued = getOperatorFactoryQueueEntry(slugOrRecordId);
  if (queued) {
    return {
      slug: queued.slug,
      recordId: queued.recordId,
      companyName: queued.companyName,
      kind: "factory_queue",
    };
  }
  return null;
}

function listFixtureFiles(slug) {
  const entry = resolveOperatorFixtureIdentity(slug);
  if (!entry) return [];
  const suffix = fixtureSuffixForSlug(entry.slug);
  if (!suffix) return [];

  const primaryDir = path.join(ROOT, "fixtures");
  const fallbackDir = path.join(ROOT, "public", "fixtures");
  const byBase = new Map();

  for (const dir of [primaryDir, fallbackDir]) {
    if (!fs.existsSync(dir)) continue;
    for (const name of fs.readdirSync(dir)) {
      if (!name.startsWith("operator-") || !name.endsWith(`-${suffix}.json`)) continue;
      if (byBase.has(name)) continue; // primary wins
      byBase.set(name, path.join(dir, name));
    }
  }
  return [...byBase.values()].sort();
}

function registryPrefillKeys() {
  const keys = new Set();
  for (const f of buildFullOperatorExplorerRegistry()) {
    if (f.prefillKey) keys.add(f.prefillKey);
  }
  return keys;
}

function looksLikePrefillKey(key, registryKeys) {
  if (!key || key.startsWith("_")) return false;
  if (registryKeys.has(key)) return true;
  return PREFILL_PREFIXES.some((p) => key.startsWith(p));
}

function assignIfPresent(target, key, value) {
  if (value == null || value === "") return;
  if (typeof value === "object" && !Array.isArray(value) && Object.keys(value).length === 0) return;
  if (Array.isArray(value) && value.length === 0) return;
  target[key] = value;
}

function assignLabeledOrPrefill(target, key, value, registryKeys) {
  const mapped = LABEL_TO_PREFILL[key] || key;
  if (looksLikePrefillKey(mapped, registryKeys) || LABEL_TO_PREFILL[key]) {
    assignIfPresent(target, mapped, value);
  }
}

/**
 * Flatten one fixture JSON into prefill-like keys.
 * @param {object} fixture
 * @param {Set<string>} registryKeys
 * @returns {Record<string, unknown>}
 */
export function flattenOperatorFixtureToPrefill(fixture, registryKeys = registryPrefillKeys()) {
  const out = {};
  // Root-level diligence / case-study arrays (e.g. operator-diligence-qa-*.json)
  if (Array.isArray(fixture) && fixture.length) {
    const sample = fixture[0] || {};
    if (sample.question || sample.answer || sample.category) {
      assignIfPresent(out, "owner_diligence_json", fixture);
      assignIfPresent(out, "ownerDiligenceQa", fixture);
    } else if (sample.name || sample.title || sample.bio) {
      assignIfPresent(out, "leadership_executives_json", fixture);
      assignIfPresent(out, "leadershipTeam", fixture);
    } else if (sample.hotel_name || sample.caseStudyName || sample.summary) {
      assignIfPresent(out, "case_studies_json", fixture);
      assignIfPresent(out, "caseStudiesDetail", fixture);
    }
    return out;
  }

  if (!fixture || typeof fixture !== "object") return out;

  if (fixture.operatingPlatform && typeof fixture.operatingPlatform === "object") {
    applyOperatingPlatformToLegacyPrefill(out, fixture.operatingPlatform);
  }
  if (fixture.brandRelationships && typeof fixture.brandRelationships === "object") {
    applyBrandRelationshipsToLegacyPrefill(out, fixture.brandRelationships);
  }
  if (fixture.engagementReporting && typeof fixture.engagementReporting === "object") {
    applyEngagementReportingToLegacyPrefill(out, fixture.engagementReporting);
  }
  if (fixture.infrastructurePlatform && typeof fixture.infrastructurePlatform === "object") {
    applyInfrastructurePlatformToLegacyPrefill(out, fixture.infrastructurePlatform);
  }

  for (const bag of KNOWN_NESTED_BAGS) {
    const block = fixture[bag];
    if (!block || typeof block !== "object" || Array.isArray(block)) continue;
    for (const [k, v] of Object.entries(block)) {
      assignLabeledOrPrefill(out, k, v, registryKeys);
    }
  }

  for (const [k, v] of Object.entries(fixture)) {
    if (k === "_meta" || k === "operatingPlatform" || k === "brandRelationships") continue;
    if (k === "engagementReporting" || k === "infrastructurePlatform") continue;
    if (KNOWN_NESTED_BAGS.includes(k)) continue;
    assignLabeledOrPrefill(out, k, v, registryKeys);
  }

  // Leadership executives often live under leadershipTeam / executives
  if (Array.isArray(fixture.leadershipTeam) && fixture.leadershipTeam.length) {
    assignIfPresent(out, "leadershipTeam", fixture.leadershipTeam);
    assignIfPresent(out, "leadership_executives_json", fixture.leadershipTeam);
  }
  if (Array.isArray(fixture.executives) && fixture.executives.length) {
    assignIfPresent(out, "leadership_executives_json", fixture.executives);
  }
  if (Array.isArray(fixture.caseStudies) && fixture.caseStudies.length) {
    assignIfPresent(out, "case_studies_json", fixture.caseStudies);
    assignIfPresent(out, "caseStudiesDetail", fixture.caseStudies);
  }

  // Parse stringified JSON blobs commonly stored in fixtures
  for (const [k, v] of Object.entries(out)) {
    if (typeof v !== "string") continue;
    const t = v.trim();
    if (!(t.startsWith("[") || t.startsWith("{"))) continue;
    if (!/_json$|Json$|bf_/.test(k) && !k.includes("mkt_regional")) continue;
    try {
      out[k] = JSON.parse(t);
    } catch {
      /* keep string */
    }
  }

  return out;
}

/**
 * @param {string} slugOrRecordId
 * @returns {{
 *   slug: string,
 *   recordId: string,
 *   companyName: string,
 *   fixtureFiles: string[],
 *   prefill: Record<string, unknown>,
 *   keyCount: number
 * }}
 */
export function loadOperatorFixturePayload(slugOrRecordId) {
  const entry = resolveOperatorFixtureIdentity(slugOrRecordId);
  if (!entry) {
    throw new Error(`Unknown operator for fixtures: ${slugOrRecordId}`);
  }
  const registryKeys = registryPrefillKeys();
  const files = listFixtureFiles(entry.slug);
  const prefill = {};
  for (const file of files) {
    const raw = JSON.parse(fs.readFileSync(file, "utf8"));
    const flat = flattenOperatorFixtureToPrefill(raw, registryKeys);
    Object.assign(prefill, flat);
  }

  const pack = FACTORY_CONTENT_BY_SLUG[entry.slug];
  if (pack?.intentionalSuppress) {
    prefill.__intentionalSuppressFieldKeys = Object.keys(pack.intentionalSuppress);
    prefill.__intentionalSuppressReasons = { ...pack.intentionalSuppress };
  }

  return {
    slug: entry.slug,
    recordId: entry.recordId,
    companyName: entry.companyName,
    fixtureFiles: files.map((f) => path.relative(ROOT, f).replace(/\\/g, "/")),
    prefill,
    keyCount: Object.keys(prefill).filter((k) => !k.startsWith("__")).length,
  };
}

/**
 * Merge live prefill with fixture fill-gaps (live wins).
 * @param {Record<string, unknown>} livePrefill
 * @param {Record<string, unknown>} fixturePrefill
 */
export function mergeLiveAndFixturePrefill(livePrefill = {}, fixturePrefill = {}) {
  const out = { ...fixturePrefill };
  for (const [k, v] of Object.entries(livePrefill || {})) {
    if (v == null || v === "") continue;
    if (typeof v === "string" && !nz(v)) continue;
    if (Array.isArray(v) && v.length === 0) continue;
    out[k] = v;
  }
  return out;
}
