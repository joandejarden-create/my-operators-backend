/**
 * Read-only inventory of previously researched GDI hotels.
 * Compares the canonical store with historical worktree stores.
 * Does not write hotel data, publication registries, or Airtable.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { listGdiSelectableHotels } from "../lib/group-demand-intelligence/repository.js";
import { classifyCommercialDemandThesis } from "../lib/group-demand-intelligence/commercial-demand-thesis-standard-v1.js";
import { evaluateSharedCommercialEligibility } from "../lib/group-demand-intelligence/commercial-eligibility-contract-v1.js";
import { classifyContactEligibility } from "../lib/group-demand-intelligence/contact-eligibility-standard-v1.js";
import { rehydratePersistedExaEvidence } from "../lib/group-demand-intelligence/workflow/gdi-persisted-exa-evidence-rehydrate-v1.js";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const canonicalHotels = path.join(root, "data", "group-demand-intelligence", "hotels");
const configDir = path.join(root, "config", "group-demand-intelligence", "hotels");
const historicalRoots = [
  {
    id: "aimbridge-worktree",
    hotels: "C:/Dev/dealality/my-operators-backend-aimbridge-three-hotel-e2e/data/group-demand-intelligence/hotels",
    config: "C:/Dev/dealality/my-operators-backend-aimbridge-three-hotel-e2e/config/group-demand-intelligence/hotels",
  },
  {
    id: "tampa-scaling-worktree",
    hotels: "C:/Dev/dealality/_gdi-scaling-powershell-reproducible-baseline-wt/data/group-demand-intelligence/hotels",
    config: null,
  },
];

function readJson(file) {
  try {
    return JSON.parse(fs.readFileSync(file, "utf8"));
  } catch {
    return null;
  }
}

function opportunitiesOf(file) {
  if (!file || !fs.existsSync(file)) return [];
  const doc = readJson(file);
  if (Array.isArray(doc)) return doc;
  return Array.isArray(doc?.opportunities) ? doc.opportunities : [];
}

function personFrom(opportunity) {
  return (
    opportunity.primaryContact ||
    opportunity.contact ||
    opportunity.buyer ||
    {}
  );
}

function hasEvidenceArtifact(opportunity) {
  const sources = Array.isArray(opportunity.sources) ? opportunity.sources : [];
  const evidence = Array.isArray(opportunity.evidence) ? opportunity.evidence : [];
  return Boolean(
    opportunity.exaAgentEvidencePack ||
      opportunity.supportingEvidenceDrawer ||
      sources.length ||
      evidence.length ||
      opportunity.eventDateEvidenceUrl ||
      opportunity.attendanceEvidenceUrl ||
      opportunity.peakRoomsEvidenceUrl
  );
}

export function classifyEvidenceRecovery(opportunity = {}) {
  const before = classifyCommercialDemandThesis(opportunity);
  const supported = new Set(["CONFIRMED_DEMAND", "RELIABLE_INFERRED_DEMAND"]);
  if (supported.has(before.thesisStandard)) {
    return { evidenceClass: "A", thesis: before.thesisStandard, rehydrated: false };
  }
  const rehydrated = rehydratePersistedExaEvidence(opportunity);
  const after = classifyCommercialDemandThesis(rehydrated.opportunity || opportunity);
  if (supported.has(after.thesisStandard)) {
    return { evidenceClass: "B", thesis: after.thesisStandard, rehydrated: true };
  }
  if (hasEvidenceArtifact(opportunity) || rehydrated.recovered) {
    return { evidenceClass: "C", thesis: before.thesisStandard, rehydrated: rehydrated.recovered === true };
  }
  return { evidenceClass: "D", thesis: before.thesisStandard, rehydrated: false };
}

export function shadowQualification(opportunity = {}) {
  const rehydrated = rehydratePersistedExaEvidence(opportunity).opportunity || opportunity;
  const thesis = classifyCommercialDemandThesis(rehydrated);
  const eligibility = evaluateSharedCommercialEligibility(rehydrated);
  const contact = classifyContactEligibility(personFrom(rehydrated), rehydrated);
  const supported = thesis.thesisStandard === "CONFIRMED_DEMAND" || thesis.thesisStandard === "RELIABLE_INFERRED_DEMAND";
  const relevantContact = contact.class === "DIRECT_BUYER" || contact.class === "RELEVANT_COMMERCIAL_ENTRY_POINT";
  let layer = "RESEARCH_CONTINUATION";
  if (eligibility.salesReady) layer = "SALES_READY";
  else if (eligibility.status === "COMMERCIALLY_INELIGIBLE") layer = "COMMERCIALLY_INELIGIBLE";
  else if (supported && relevantContact) layer = "ACCOUNT_DEVELOPMENT";
  return {
    layer,
    thesis: thesis.thesisStandard,
    contactClass: contact.class || "NO_VERIFIED_NAMED_CONTACT",
  };
}

function summarizeStore(file) {
  const rows = opportunitiesOf(file);
  const evidence = { A: 0, B: 0, C: 0, D: 0 };
  const layers = { SALES_READY: 0, ACCOUNT_DEVELOPMENT: 0, RESEARCH_CONTINUATION: 0, COMMERCIALLY_INELIGIBLE: 0 };
  const thesis = {};
  const contacts = {};
  let withContact = 0;
  for (const row of rows) {
    const recovered = classifyEvidenceRecovery(row);
    evidence[recovered.evidenceClass] += 1;
    const shadow = shadowQualification(row);
    layers[shadow.layer] = (layers[shadow.layer] || 0) + 1;
    thesis[shadow.thesis] = (thesis[shadow.thesis] || 0) + 1;
    contacts[shadow.contactClass] = (contacts[shadow.contactClass] || 0) + 1;
    if (shadow.contactClass === "DIRECT_BUYER" || shadow.contactClass === "RELEVANT_COMMERCIAL_ENTRY_POINT") {
      withContact += 1;
    }
  }
  const stat = file && fs.existsSync(file) ? fs.statSync(file) : null;
  return {
    opportunityCount: rows.length,
    updatedAt: stat ? stat.mtime.toISOString() : null,
    evidence,
    layers,
    thesis,
    contacts,
    relevantNamedContacts: withContact,
  };
}

function researchStateSummary(hotelDir) {
  const segmentDir = path.join(hotelDir, "full-e2e-fourteen-segment-state");
  const researchDir = path.join(hotelDir, "opportunity-research-state");
  const candidateIds = new Set();
  let segmentFiles = 0;
  if (fs.existsSync(segmentDir)) {
    for (const file of fs.readdirSync(segmentDir)) {
      if (!file.endsWith(".json")) continue;
      segmentFiles += 1;
      const doc = readJson(path.join(segmentDir, file));
      for (const id of doc?.candidateIds || []) candidateIds.add(id);
    }
  }
  let researchFiles = 0;
  if (fs.existsSync(researchDir)) {
    researchFiles = fs.readdirSync(researchDir).filter((file) => file.endsWith(".json")).length;
  }
  const profile = fs.existsSync(path.join(hotelDir, "profile.json"));
  const needPeriodMirror = fs.existsSync(path.join(hotelDir, "need-periods.airtable-mirror.json"));
  return {
    segmentFiles,
    uniqueCandidateIds: candidateIds.size,
    researchStateFiles: researchFiles,
    profile,
    needPeriodMirror,
  };
}

function loadConfigs() {
  const configs = new Map();
  if (!fs.existsSync(configDir)) return configs;
  for (const file of fs.readdirSync(configDir)) {
    if (!file.endsWith(".json")) continue;
    const config = readJson(path.join(configDir, file));
    if (!config) continue;
    const fileId = file.replace(/\.json$/i, "");
    const hotelId = String(config.hotelId || fileId);
    configs.set(hotelId, {
      fileId,
      displayName: config.displayName || hotelId,
      city: config.city || config.identity?.city || null,
      country: config.country || config.identity?.country || null,
      onboardingStatus: config.gdiOnboarding?.status || null,
      redirectTo: config.redirectTo || null,
      aliases: config.aliases || null,
    });
  }
  return configs;
}

function hotelDirs(dir) {
  if (!fs.existsSync(dir)) return [];
  return fs.readdirSync(dir).filter((name) => fs.existsSync(path.join(dir, name, "opportunities.json")) || fs.existsSync(path.join(dir, name, "profile.json")));
}

function publishedCounts() {
  const file = "C:/Dev/dealality/_dealality-gdi-published-inventory-dev/registry/current-registry-v1.json";
  const doc = readJson(file);
  const counts = {};
  for (const [hotelId, hotel] of Object.entries(doc?.hotels || {})) {
    const rows = Object.values(hotel?.opportunities || {});
    counts[hotelId] = rows.filter((row) => row.publicationState === "PUBLISHED" && !row.revokedAt).length;
  }
  return counts;
}

function buildInventory() {
  const configs = loadConfigs();
  for (const source of historicalRoots) {
    if (!source.config || !fs.existsSync(source.config)) continue;
    for (const file of fs.readdirSync(source.config)) {
      if (!file.endsWith(".json")) continue;
      const config = readJson(path.join(source.config, file));
      const hotelId = String(config?.hotelId || file.replace(/\.json$/i, ""));
      if (!configs.has(hotelId)) {
        configs.set(hotelId, {
          fileId: hotelId,
          displayName: config?.displayName || hotelId,
          city: config?.city || null,
          country: config?.country || null,
          onboardingStatus: config?.gdiOnboarding?.status || null,
          redirectTo: config?.redirectTo || null,
          aliases: config?.aliases || null,
          configSource: source.id,
          canonicalConfig: false,
        });
      }
    }
  }
  const selectable = new Set(listGdiSelectableHotels().map((hotel) => hotel.hotelId));
  const published = publishedCounts();
  const ids = new Set([...configs.keys(), ...hotelDirs(canonicalHotels)]);
  for (const source of historicalRoots) {
    for (const hotelId of hotelDirs(source.hotels)) {
      if (!hotelId.startsWith(".")) ids.add(hotelId);
    }
  }
  const hotels = [];
  for (const hotelId of [...ids].sort()) {
    if (hotelId.startsWith("gdi_hotel_")) continue;
    const config = configs.get(hotelId) || null;
    const canonicalFile = path.join(canonicalHotels, hotelId, "opportunities.json");
    const canonical = summarizeStore(fs.existsSync(canonicalFile) ? canonicalFile : null);
    const historical = [];
    for (const source of historicalRoots) {
      const file = path.join(source.hotels, hotelId, "opportunities.json");
      if (!fs.existsSync(file)) continue;
      historical.push({ source: source.id, ...summarizeStore(file) });
    }
    const stateSources = [];
    for (const source of historicalRoots) {
      const dir = path.join(source.hotels, hotelId);
      if (!fs.existsSync(dir)) continue;
      const state = researchStateSummary(dir);
      if (state.segmentFiles || state.researchStateFiles || state.profile || state.needPeriodMirror) {
        stateSources.push({ source: source.id, ...state });
      }
    }
    const bestHistorical = historical.sort((a, b) => b.opportunityCount - a.opportunityCount)[0] || null;
    const inApp = selectable.has(hotelId);
    let visibility = "VISIBLE_WITH_LOCAL_RESEARCH";
    let importAction = "NONE";
    if (!canonical.opportunityCount && stateSources.some((row) => row.uniqueCandidateIds || row.researchStateFiles)) {
      visibility = "RESEARCH_STATE_ONLY_NOT_IN_CANONICAL_STORE";
      importAction = "DRY_RUN_COPY_RESEARCH_STATE";
    } else if (!canonical.opportunityCount && bestHistorical?.opportunityCount) {
      visibility = inApp ? "SELECTABLE_BUT_LOCAL_STORE_EMPTY" : "MISSING_FROM_SELECTOR_AND_LOCAL_STORE";
      importAction = "DRY_RUN_IMPORT_MISSING";
    } else if (!canonical.opportunityCount && !bestHistorical) {
      visibility = inApp ? "SELECTABLE_CONFIG_ONLY" : "NO_RECOVERABLE_STORE";
      importAction = "NO_STORE";
    } else if (bestHistorical && bestHistorical.opportunityCount > canonical.opportunityCount) {
      visibility = "CANONICAL_STORE_SMALLER_THAN_HISTORICAL";
      importAction = "DELTA_REVIEW_DO_NOT_OVERWRITE";
    } else if (!inApp && canonical.opportunityCount) {
      visibility = "LOCAL_RESEARCH_NOT_IN_SELECTOR";
    }
    const measured = canonical.opportunityCount ? canonical : bestHistorical;
    hotels.push({
      hotelId,
      displayName: config?.displayName || hotelId,
      city: config?.city || null,
      country: config?.country || null,
      onboardingStatus: config?.onboardingStatus || null,
      selectable: inApp,
      visibility,
      importAction,
      canonicalOpportunities: canonical.opportunityCount,
      canonicalUpdatedAt: canonical.updatedAt,
      historicalSources: historical.map((row) => ({
        source: row.source,
        opportunityCount: row.opportunityCount,
        updatedAt: row.updatedAt,
      })),
      researchState: stateSources,
      canonicalConfig: config?.canonicalConfig !== false,
      publishedCount: published[hotelId] || 0,
      evidence: measured?.evidence || { A: 0, B: 0, C: 0, D: 0 },
      qualification: measured?.layers || {},
      thesis: measured?.thesis || {},
      contacts: measured?.contacts || {},
      relevantNamedContacts: measured?.relevantNamedContacts || 0,
      measuredFrom: canonical.opportunityCount ? "canonical" : bestHistorical ? bestHistorical.source : "none",
    });
  }
  return {
    version: "gdi-complete-hotel-inventory-audit-v1",
    readOnly: true,
    hotelCount: hotels.length,
    selectableCount: hotels.filter((hotel) => hotel.selectable).length,
    missingLocalStore: hotels.filter((hotel) => hotel.importAction === "DRY_RUN_IMPORT_MISSING").map((hotel) => ({
      hotelId: hotel.hotelId,
      displayName: hotel.displayName,
      historicalSources: hotel.historicalSources,
    })),
    hotels,
  };
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const inventory = buildInventory();
  const out = process.env.GDI_INVENTORY_OUT;
  if (out) fs.writeFileSync(out, JSON.stringify(inventory, null, 2));
  const totals = { A: 0, B: 0, C: 0, D: 0, salesReady: 0, accountDevelopment: 0, continuation: 0 };
  for (const hotel of inventory.hotels) {
    totals.A += hotel.evidence.A || 0;
    totals.B += hotel.evidence.B || 0;
    totals.C += hotel.evidence.C || 0;
    totals.D += hotel.evidence.D || 0;
    totals.salesReady += hotel.qualification.SALES_READY || 0;
    totals.accountDevelopment += hotel.qualification.ACCOUNT_DEVELOPMENT || 0;
    totals.continuation += hotel.qualification.RESEARCH_CONTINUATION || 0;
  }
  console.log(JSON.stringify({
    hotelCount: inventory.hotelCount,
    selectableCount: inventory.selectableCount,
    missingLocalStore: inventory.missingLocalStore.length,
    totals,
  }));
}
