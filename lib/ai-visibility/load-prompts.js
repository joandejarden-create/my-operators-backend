/**
 * Read-only governed prompt loader (Airtable or fixture).
 * Production mode: no silent fixture fallback.
 */

import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { validatePromptRow } from "./prompt-validation.js";
import { normalizePromptGeography } from "./geography.js";
import { AI_VISIBILITY_PROMPTS_TABLE } from "./airtable-schema-proposal.js";
import { normalizeLanguage } from "./language-dimension.js";
import { attachPromptProvenance } from "./prompt-provenance.js";

export const PROMPT_LOADER_VERSION = "ai_visibility_prompt_loader_v1";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const DEFAULT_FIXTURE = path.join(
  __dirname,
  "..",
  "..",
  "fixtures",
  "ai-visibility",
  "phase2d-prompt-seed.json"
);

function scopeToInternal(scope) {
  if (!scope) return null;
  const s = String(scope).trim().toLowerCase();
  if (s === "global") return "Global";
  if (s === "region") return "Region";
  if (s === "subregion") return "Subregion";
  if (s === "country") return "Country";
  if (s === "market") return "Market";
  // Already titled
  return scope;
}

function mapAirtableRecord(rec) {
  const f = rec.fields || {};
  const languageRaw = f["Language"] || null;
  return {
    recordId: rec.id,
    promptId: f["Prompt ID"] || null,
    promptName: f["Prompt Name"] || null,
    promptFamily: f["Prompt Family"] || null,
    promptText: f["Prompt Text"] || null,
    version: f["Version"] || null,
    intentTerritory: f["Intent Territory"] || null,
    stakeholderRelevance: f["Stakeholder Relevance"] || [],
    entityScope: f["Entity Scope"] || null,
    geographyScope: f["Geography Scope"] || null,
    commercialRegion: f["Commercial Region"] || null,
    subregion: f["Subregion"] || null,
    country: f["Country"] || null,
    countryCode: f["Country Code"] || null,
    market: f["Market"] || null,
    geographyModelVersion: f["Geography Model Version"] || null,
    peerSetId: f["Peer Set ID"] || null,
    language: normalizeLanguage(languageRaw) || languageRaw || null,
    semanticPairId: f["Semantic Pair ID"] || null,
    chainScale: f["Chain Scale"] || null,
    assetType: f["Asset Type"] || null,
    hotelType: f["Hotel Type"] || null,
    developmentType: f["Development Type"] || null,
    brandedResidencesRelevance: Boolean(f["Branded Residences Relevance"]),
    decisionStage: f["Decision Stage"] || null,
    active: Boolean(f["Active"]),
    monitoringEligible: Boolean(f["Monitoring Eligible"]),
    cadence: f["Cadence"] || null,
    governanceStatus: f["Governance Status"] || null,
    reviewStatus: f["Review Status"] || null,
    reviewNotes: f["Review Notes"] || null,
    sourceRationale: f["Source / Rationale"] || null,
    lastMonitoredAt: f["Last Monitored At"] || null,
    promptOrigin: f["Prompt Origin"] || null,
    originSourceType: f["Origin Source Type"] || null,
    originSourceName: f["Origin Source Name"] || null,
    originSourceReference: f["Origin Source Reference"] || null,
    observedQuery: f["Observed Query"] || null,
    observedTheme: f["Observed Theme"] || null,
    demandTier: f["Demand Tier"] || null,
    demandSignalType: f["Demand Signal Type"] || null,
    demandGeography: f["Demand Geography"] || null,
    dateObserved: f["Date Observed"] || null,
    demandEvidenceCount: f["Demand Evidence Count"] ?? null,
    demandMethodology: f["Demand Methodology"] || null,
    derivedFromObservedPromptId: f["Derived From Observed Prompt ID"] || null,
    derivedFromDemandSignalId: f["Derived From Demand Signal ID"] || null,
    ownerIntentSubtheme: f["Owner Intent Subtheme"] || null,
    provenanceStatus: f["Provenance Status"] || null,
    provenanceNotes: f["Provenance Notes"] || null,
    createdByMethod: f["Created By Method"] || null,
    lastProvenanceReviewAt: f["Last Provenance Review At"] || null,
    samplingPriority: f["Sampling Priority"] || null,
  };
}

function mapFixtureRow(row) {
  return {
    recordId: null,
    ...row,
    language: normalizeLanguage(row.language) || row.language || null,
    stakeholderRelevance: row.stakeholderRelevance || [],
    active: row.active !== false,
    monitoringEligible: row.monitoringEligible !== false,
  };
}

function matchesFilters(row, filters = {}) {
  if (filters.activeOnly && !row.active) return false;
  if (filters.monitoringEligible === true && !row.monitoringEligible) return false;
  if (filters.monitoringEligible === false && row.monitoringEligible) return false;

  if (filters.geographyScope) {
    const want = scopeToInternal(filters.geographyScope);
    if (String(row.geographyScope) !== String(want)) return false;
  }
  if (filters.region || filters.commercialRegion) {
    const r = filters.region || filters.commercialRegion;
    if (String(row.commercialRegion || "").toLowerCase() !== String(r).toLowerCase()) {
      return false;
    }
  }
  if (filters.country) {
    if (String(row.country || "").toLowerCase() !== String(filters.country).toLowerCase()) {
      return false;
    }
  }
  if (filters.entityScope) {
    if (String(row.entityScope).toLowerCase() !== String(filters.entityScope).toLowerCase()) {
      return false;
    }
  }
  if (filters.intentTerritory) {
    if (
      String(row.intentTerritory).toLowerCase() !==
      String(filters.intentTerritory).toLowerCase()
    ) {
      return false;
    }
  }
  if (filters.stakeholder) {
    const list = (row.stakeholderRelevance || []).map((s) => String(s).toLowerCase());
    if (!list.includes(String(filters.stakeholder).toLowerCase())) return false;
  }
  if (filters.promptFamily) {
    if (String(row.promptFamily) !== String(filters.promptFamily)) return false;
  }
  return true;
}

/**
 * Load from fixture seed (tests / offline).
 */
export function loadGovernedAiVisibilityPromptsFromFixture(filters = {}, fixturePath = DEFAULT_FIXTURE) {
  const raw = JSON.parse(fs.readFileSync(fixturePath, "utf8"));
  const rows = (raw.prompts || []).map(mapFixtureRow);
  return finalizeLoad(rows, filters, { source: "fixture", fixturePath });
}

/**
 * @param {object} filters
 * @param {{ mode?: "fixture"|"airtable"|"auto", fixturePath?: string, fetchPage?: Function }} options
 */
export async function loadGovernedAiVisibilityPrompts(filters = {}, options = {}) {
  const mode = options.mode || "auto";

  if (mode === "fixture") {
    return loadGovernedAiVisibilityPromptsFromFixture(filters, options.fixturePath);
  }

  if (mode === "airtable" || (mode === "auto" && process.env.AIRTABLE_API_KEY && process.env.AIRTABLE_BASE_ID)) {
    if (!process.env.AIRTABLE_API_KEY || !process.env.AIRTABLE_BASE_ID) {
      throw new Error("Airtable credentials required for production prompt load (no silent fixture fallback).");
    }
    const rows = await fetchAllPromptRows({
      apiKey: process.env.AIRTABLE_API_KEY,
      baseId: process.env.AIRTABLE_BASE_ID,
      fetchPage: options.fetchPage,
    });
    return finalizeLoad(rows, filters, { source: "airtable" });
  }

  if (mode === "auto") {
    // Dev convenience only when explicitly not production
    if (String(process.env.NODE_ENV || "").toLowerCase() === "production") {
      throw new Error("No Airtable credentials; refusing fixture fallback in production.");
    }
    return loadGovernedAiVisibilityPromptsFromFixture(filters, options.fixturePath);
  }

  throw new Error(`Unknown prompt loader mode: ${mode}`);
}

async function fetchAllPromptRows({ apiKey, baseId, fetchPage }) {
  const table = encodeURIComponent(AI_VISIBILITY_PROMPTS_TABLE);
  let offset = null;
  const out = [];
  do {
    const url = new URL(`https://api.airtable.com/v0/${baseId}/${table}`);
    url.searchParams.set("pageSize", "100");
    if (offset) url.searchParams.set("offset", offset);
    let json;
    if (fetchPage) {
      json = await fetchPage(url.toString());
    } else {
      const res = await fetch(url.toString(), {
        headers: { Authorization: `Bearer ${apiKey}` },
      });
      const text = await res.text();
      json = text ? JSON.parse(text) : {};
      if (!res.ok) throw new Error(`Prompt load failed: ${res.status} ${JSON.stringify(json)}`);
    }
    for (const rec of json.records || []) out.push(mapAirtableRecord(rec));
    offset = json.offset || null;
  } while (offset);
  return out;
}

function finalizeLoad(rows, filters, meta) {
  const malformed = [];
  const valid = [];
  for (const row of rows) {
    const v = validatePromptRow(row);
    if (!v.ok) {
      malformed.push({ promptId: row.promptId, errors: v.errors });
      continue;
    }
    const geography = normalizePromptGeography({
      geographyScope: String(row.geographyScope || "").toLowerCase(),
      region: row.commercialRegion,
      country: row.country,
      subregion: row.subregion,
      market: row.market,
      geography: row.country || row.commercialRegion || row.geographyScope,
    });
    valid.push(
      attachPromptProvenance({ ...row, geography, validation: v })
    );
  }

  const filtered = valid.filter((r) => matchesFilters(r, filters));
  return {
    loaderVersion: PROMPT_LOADER_VERSION,
    source: meta.source,
    totalLoaded: rows.length,
    validCount: valid.length,
    malformed,
    prompts: filtered,
  };
}

export { mapAirtableRecord, matchesFilters };
