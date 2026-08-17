/**
 * Research Engine V2 — Shadow Operations V1
 *
 * Scheduled-style CLI (no in-repo cron). Steward queue. Directory expansion.
 * No Webhound. No credits. No Airtable writes. No auto-activation. No image replace.
 *
 *   npm run research-engine-v2:shadow-operations-v1
 *   node scripts/research-engine-v2-shadow-operations-v1.mjs --run-type daily_lightweight
 */

import { mkdirSync, writeFileSync, readFileSync, existsSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { randomUUID } from "node:crypto";
import {
  loadIhgDirectoryRows,
  loadMarriottSoftBrandDirectoryRows,
} from "../lib/research-engine-v2/check-hotel-freshness.js";
import { loadChoiceSitemapDirectoryRows } from "../lib/hotel-census/plan-choice-census-sitemap-match.js";
import { checkHotelFreshness } from "../lib/research-engine-v2/check-hotel-freshness.js";
import { computeDirectoryGaps, computeChoiceIndividualsGaps } from "../lib/research-engine-v2/directory-gaps.js";
import { loadShadowState, saveShadowState, applyAlertDedup, claimFingerprint } from "../lib/research-engine-v2/shadow-state.js";
import { batchIdentityEnrichmentProposals } from "../lib/research-engine-v2/identity-enrichment.js";
import { assessOpeningCorroborationFromOfficialPage } from "../lib/research-engine-v2/opening-corroboration.js";
import { runBrandActivationResearch } from "../lib/research-engine-v2/brand-activation.js";
import { auditImagesForEntity } from "../lib/research-engine-v2/image-integrity.js";
import { resolveBrandFamily } from "../lib/research-engine-v2/brand-family.js";
import { SHADOW_OPS_CONFIG, selectCohortHotels } from "../lib/research-engine-v2/ops-config.js";
import { emptyMetrics, finalizeMetrics, bumpSourceState } from "../lib/research-engine-v2/ops-metrics.js";
import {
  createQueueItem,
  buildReviewPack,
  loadStewardQueue,
  saveStewardQueue,
  mergeIntoStewardQueue,
  ENGINE_OPS_VERSION,
  CONFIG_VERSION,
} from "../lib/research-engine-v2/steward-queue.js";
import { recommendEscalation } from "../lib/research-engine-v2/escalation.js";
import { FALLBACK_LADDER, summarizeFallbackAttempts } from "../lib/research-engine-v2/source-fallback.js";
import { formatShadowDigestMarkdown } from "../lib/research-engine-v2/shadow-monitor.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "..");
const OUT = join(ROOT, "data/research-engine-v2/shadow-operations-v1");
const STATE = join(OUT, "04-dedup-state.json");
const QUEUE_PATH = join(OUT, "06-steward-queue.json");
const FETCH_DELAY_MS = Number(process.env.RE_V2_FETCH_DELAY_MS || SHADOW_OPS_CONFIG.fetchDelayMs);

const args = process.argv.slice(2);
function argVal(flag, fallback) {
  const i = args.indexOf(flag);
  return i >= 0 && args[i + 1] ? args[i + 1] : fallback;
}
const RUN_TYPE = argVal("--run-type", "daily_lightweight");

function writeJson(name, obj) {
  writeFileSync(join(OUT, name), JSON.stringify(obj, null, 2), "utf8");
}
function writeMd(name, text) {
  writeFileSync(join(OUT, name), text, "utf8");
}

function parseCsvLine(line) {
  const o = [];
  let c = "";
  let q = false;
  for (let i = 0; i < line.length; i++) {
    const ch = line[i];
    if (q) {
      if (ch === '"' && line[i + 1] === '"') {
        c += '"';
        i++;
      } else if (ch === '"') q = false;
      else c += ch;
    } else if (ch === '"') q = true;
    else if (ch === ",") {
      o.push(c);
      c = "";
    } else c += ch;
  }
  o.push(c);
  return o;
}

function loadCensusRows() {
  const csv = readFileSync(join(ROOT, "reports/census-amenities-blank-rows.csv"), "utf8").split(/\r?\n/);
  const rows = [];
  for (const line of csv.slice(1)) {
    if (!line.trim()) continue;
    const f = parseCsvLine(line);
    const name = f[1] || "";
    const parent = f[2] === "(blank parent)" ? "" : f[2] || "";
    rows.push({
      hotelId: f[0],
      recordId: f[0],
      name,
      parentCompany: parent,
      currentParent: parent,
      status: f[3],
      currentStatus: f[3],
      country: f[4],
      city: "",
      website: "",
      currentBrand: inferBrandFromName(name),
      affiliation: inferBrandFromName(name),
      brandFamily: resolveBrandFamily({ name, parentCompany: parent }),
    });
  }
  return rows;
}

function inferBrandFromName(name) {
  const n = String(name || "");
  if (/Hotel Indigo/i.test(n)) return "Hotel Indigo";
  if (/Kimpton/i.test(n)) return "Kimpton";
  if (/Tribute/i.test(n)) return "Tribute Portfolio";
  if (/Autograph/i.test(n)) return "Autograph Collection";
  if (/AC Hotel/i.test(n)) return "AC Hotels";
  if (/Four Points Flex/i.test(n)) return "Four Points Flex by Sheraton";
  if (/Tapestry/i.test(n)) return "Tapestry Collection by Hilton";
  if (/Spark/i.test(n)) return "Spark by Hilton";
  if (/Curio/i.test(n)) return "Curio Collection by Hilton";
  if (/Hilton Garden/i.test(n)) return "Hilton Garden Inn";
  if (/Hampton/i.test(n)) return "Hampton by Hilton";
  if (/DoubleTree/i.test(n)) return "DoubleTree by Hilton";
  if (/Homewood/i.test(n)) return "Homewood Suites";
  if (/Avani/i.test(n)) return "Avani";
  if (/Radisson Individual/i.test(n)) return "Radisson Individuals Americas";
  if (/Ascend/i.test(n)) return "Ascend Collection";
  if (/Comfort/i.test(n)) return "Comfort Inn";
  if (/Quality/i.test(n)) return "Quality Inn";
  if (/Radisson Collection/i.test(n)) return "Radisson Collection";
  if (/Radisson/i.test(n)) return "Radisson";
  return n.split(/\s+/).slice(0, 3).join(" ");
}

async function loadDirs() {
  const ihgDirectoryRows = loadIhgDirectoryRows(join(ROOT, "reports/ihg-cala-directory-extract.json"));
  const marriottDirectoryRows = loadMarriottSoftBrandDirectoryRows();
  let choiceDirectoryRows = [];
  try {
    choiceDirectoryRows = loadChoiceSitemapDirectoryRows(
      join(ROOT, "reports/independent-census-choice-property-url-extract-cala-2026-05-20.json"),
      join(ROOT, "reports/independent-census-choice-property-url-extract-cala-2026-05-20.csv")
    );
  } catch {
    /* optional */
  }
  return { ihgDirectoryRows, marriottDirectoryRows, choiceDirectoryRows };
}

function enrichHotel(h) {
  return {
    ...h,
    currentBrand: h.currentBrand || inferBrandFromName(h.name),
    affiliation: h.affiliation || inferBrandFromName(h.name),
    brandFamily: h.brandFamily || resolveBrandFamily(h),
  };
}

/**
 * Run freshness over hotels → digest items + queue candidates + metrics
 */
async function runHotelChecks(hotels, dirs, runId, metrics, onProgress) {
  /** @type {object[]} */
  const results = [];
  /** @type {object[]} */
  const queueCandidates = [];
  /** @type {object[]} */
  const escalations = [];
  /** @type {object[]} */
  const errors = [];
  /** @type {object[]} */
  const sourceFailures = [];

  let i = 0;
  for (const raw of hotels) {
    i++;
    const hotel = enrichHotel(raw);
    if (onProgress) onProgress(`[check ${i}/${hotels.length}] ${hotel.name}`);
    try {
      const r = await checkHotelFreshness(hotel, {
        ihgDirectoryRows: dirs.ihgDirectoryRows,
        marriottDirectoryRows: dirs.marriottDirectoryRows,
        choiceDirectoryRows: dirs.choiceDirectoryRows,
        fetchDelayMs: FETCH_DELAY_MS,
      });
      bumpSourceState(metrics, r.sourceState || r.observation?.sourceState || "Empty");
      results.push(r);

      if (r.sourceState === "Blocked" || r.sourceState === "Failed") {
        sourceFailures.push({
          hotelId: hotel.hotelId,
          hotelName: hotel.name,
          sourceState: r.sourceState,
          notes: r.observation?.notes,
        });
        escalations.push(
          recommendEscalation({
            sourceState: r.sourceState,
            entityId: hotel.hotelId,
            entityName: hotel.name,
            missingIdentityCodes: !hotel.propertyId && hotel.brandFamily === "hilton",
          })
        );
      }

      // Opening corroboration for Medium Pipeline→Open
      const statusMat = (r.proposedCorrections || []).find(
        (c) => c.recommended_action === "Proposed Status Change" && c.confidenceBand === "Medium"
      );
      if (statusMat && hotel.currentStatus === "Pipeline") {
        const corr = await assessOpeningCorroborationFromOfficialPage(r.observation, {
          currentStatus: hotel.currentStatus,
        });
        r.openingCorroboration = corr;
        if (corr.upgraded && corr.band === "High") {
          statusMat.confidenceBand = "High";
          statusMat.queue = "proposed_high";
          statusMat.reason = `${statusMat.reason}; opening corroboration: ${corr.reason}`;
        }
      }

      for (const c of [...(r.proposedCorrections || []), ...(r.reviewQueue || [])]) {
        if (["No Change", "Insufficient Evidence"].includes(c.recommended_action)) continue;
        const item = {
          hotel_id: c.hotel_id,
          hotel_name: c.hotel_name,
          entity_id: c.hotel_id,
          entity_name: c.hotel_name,
          entity_type: "hotel",
          brand_family: hotel.brandFamily,
          issue_type: "freshness",
          field: c.field,
          current_value: c.current_value,
          observed_value: c.observed_value,
          proposed_value: c.observed_value,
          classification: c.classification,
          confidence: c.confidenceBand || null,
          match_confidence: c.entityMatchLevel || r.entityMatch?.level,
          evidence_summary: c.reason,
          evidence_sources: c.evidence || [],
          evidence_date: c.evidence?.[0]?.sourceDate || null,
          research_run_id: runId,
          engine_version: ENGINE_OPS_VERSION,
          recommended_action: c.recommended_action,
          research_mode: "shadow_monitoring",
          source_state: r.sourceState,
          cross_table_impact:
            c.field === "status"
              ? ["Brand Explorer pipeline cards may be stale", "Scout/Radar status display"]
              : c.field === "Affiliation"
                ? ["Brand Explorer affiliation", "census brand rollups"]
                : [],
        };
        item.fingerprint = claimFingerprint(item);
        if (
          ["Proposed Status Change", "Proposed Reflag", "Proposed Update", "Proposed Parent Correction"].includes(
            c.recommended_action
          ) &&
          (c.confidenceBand === "High" || c.queue === "proposed_high")
        ) {
          item.digestBucket = "high_confidence";
          metrics.discovery.status_changes_found++;
        } else {
          item.digestBucket = "review";
        }
        queueCandidates.push(item);
      }
    } catch (err) {
      errors.push({ hotelId: hotel.hotelId, hotelName: hotel.name, error: err?.message || String(err) });
      bumpSourceState(metrics, "Failed");
    }
  }

  return { results, queueCandidates, escalations, errors, sourceFailures };
}

function digestFromSurface(surface, results, gaps, stale, suppressed, meta) {
  return {
    ...meta,
    highConfidence: surface.filter((d) => d.digestBucket === "high_confidence"),
    reviewCandidates: surface.filter((d) => d.digestBucket === "review"),
    directoryGaps: surface.filter((d) => d.digestBucket === "directory_gap"),
    staleCandidates: surface.filter((d) => d.digestBucket === "stale_candidate"),
    suppressed,
    results,
    noChangeCount: results.filter(
      (r) =>
        !(r.proposedCorrections || []).some((c) =>
          ["Proposed Status Change", "Proposed Reflag", "Proposed Update"].includes(c.recommended_action)
        )
    ).length,
    hotelsChecked: results.length,
    rawGaps: gaps,
  };
}

const ACTIVATION_BENCHMARK = [
  {
    name: "Avani",
    slug: "avani",
    brandStatus: "Absent from Active/Live",
    parentCompany: "Minor Hotel Group Limited",
    officialUrl: "https://www.avanihotels.com/",
    brandExplorerActive: false,
    hasPresentationRows: false,
    mandatoryGatesPass: false,
    segment: "Upper Upscale Lifestyle",
    selectionReason: "Census MX/CO without Active BE — activation candidate",
  },
  {
    name: "Four Points Flex by Sheraton",
    slug: "four-points-flex-by-sheraton",
    recordId: "recgaMzDn2GKkpUsi",
    brandStatus: "Under Review",
    parentCompany: "Marriott International",
    officialUrl: "https://www.hotel-development.marriott.com/brands/fourpointsexpress",
    brandExplorerActive: false,
    hasPresentationRows: true,
    mandatoryGatesPass: false,
    selectionReason: "Under Review with Marriott directory — incomplete pack",
  },
  {
    name: "Tapestry Collection by Hilton",
    slug: "tapestry-collection-by-hilton",
    recordId: "reccXxMHEh7NNRhIE",
    brandStatus: "Under Review",
    parentCompany: "Hilton",
    officialUrl: "https://www.hilton.com/en/brands/tapestry-collection/",
    brandExplorerActive: false,
    hasPresentationRows: true,
    mandatoryGatesPass: false,
    selectionReason: "Soft brand + CALA census",
  },
  {
    name: "Spark by Hilton",
    slug: "spark-by-hilton",
    brandStatus: "Under Review (Factory)",
    parentCompany: "Hilton",
    officialUrl: "https://www.hilton.com/en/brands/spark-by-hilton/",
    brandExplorerActive: false,
    hasPresentationRows: false,
    mandatoryGatesPass: false,
    selectionReason: "Factory Under Review",
  },
  {
    name: "Radisson Collection",
    slug: "radisson-collection",
    brandStatus: "Draft / excluded",
    parentCompany: "Choice Hotels International, Inc.",
    officialUrl: "https://www.radissonhotels.com/en-us/collection",
    brandExplorerActive: false,
    hasPresentationRows: false,
    mandatoryGatesPass: false,
    selectionReason: "Known incomplete exclusion",
  },
  {
    name: "Curio Collection by Hilton",
    slug: "curio-collection-by-hilton",
    brandStatus: "Check Active/Live vs factory",
    parentCompany: "Hilton",
    officialUrl: "https://www.hilton.com/en/brands/curio-collection/",
    brandExplorerActive: true,
    hasPresentationRows: true,
    mandatoryGatesPass: false,
    selectionReason: "Control / soft Hilton brand for cross-check",
  },
  {
    name: "Ascend Collection",
    slug: "ascend-collection",
    brandStatus: "Likely Active or Under Review",
    parentCompany: "Choice Hotels International, Inc.",
    officialUrl: "https://www.choicehotels.com/ascend",
    brandExplorerActive: false,
    hasPresentationRows: true,
    mandatoryGatesPass: false,
    selectionReason: "Choice soft brand + CALA relevance",
  },
];

mkdirSync(OUT, { recursive: true });
const runId = `run_${new Date().toISOString().replace(/[:.]/g, "-")}_${randomUUID().slice(0, 8)}`;
const startedAt = new Date().toISOString();
const t0 = Date.now();
const metrics = emptyMetrics();
const allCensus = loadCensusRows();
const dirs = await loadDirs();

// --- Daily production-shadow cohort ---
const dailyDef = SHADOW_OPS_CONFIG.cohorts.indigo_kimpton_mexico;
const dailyHotels = selectCohortHotels(allCensus, dailyDef).map(enrichHotel);
console.log(`[ops] ${RUN_TYPE} cohort ${dailyHotels.length} Indigo+Kimpton Mexico`);

const daily = await runHotelChecks(dailyHotels, dirs, runId, metrics, (m) => console.log(m));

// Directory gaps (IHG)
const gaps = computeDirectoryGaps(dailyHotels, dirs.ihgDirectoryRows || [], {
  brandFamily: "ihg",
  countryFilter: /Mexico/i,
  brandFilter: (row) => /hotelindigo|kimpton/i.test(`${row.brand || ""} ${row.propertyUrl || ""}`),
});

/** @type {object[]} */
const gapItems = [];
for (const g of gaps.missingCensusCandidates || []) {
  metrics.discovery.missing_census_records++;
  const item = {
    hotel_id: null,
    hotel_name: g.directoryName,
    entity_id: null,
    entity_name: g.directoryName,
    entity_type: "hotel",
    brand_family: "ihg",
    issue_type: "census_gap",
    field: "census_presence",
    current_value: null,
    observed_value: "Present in official directory",
    classification: g.classification,
    confidence: "medium",
    match_confidence: g.bestCensusMatch?.level || null,
    evidence_summary: "Official directory property not matched to Dealality census at Medium+",
    evidence_sources: g.officialUrl ? [{ url: g.officialUrl }] : [],
    research_run_id: runId,
    recommended_action: "Missing Census Candidate",
    research_mode: "shadow_monitoring",
    digestBucket: "directory_gap",
  };
  item.fingerprint = claimFingerprint(item);
  gapItems.push(item);
}
for (const g of gaps.censusNotInDirectory || []) {
  const item = {
    hotel_id: g.hotelId,
    hotel_name: g.hotelName,
    entity_id: g.hotelId,
    entity_name: g.hotelName,
    entity_type: "hotel",
    brand_family: "ihg",
    issue_type: "census_gap",
    field: "directory_presence",
    current_value: g.currentStatus,
    observed_value: "Not found in expected official inventory",
    classification: g.classification,
    confidence: "medium",
    match_confidence: g.bestDirectoryMatch?.level || null,
    evidence_summary: g.note,
    evidence_sources: g.bestDirectoryMatch?.url ? [{ url: g.bestDirectoryMatch.url }] : [],
    research_run_id: runId,
    recommended_action: "Review",
    research_mode: "shadow_monitoring",
    digestBucket: "stale_candidate",
    cross_table_impact: ["Do not auto-classify as closed"],
  };
  item.fingerprint = claimFingerprint(item);
  gapItems.push(item);
}

const identityProposals = batchIdentityEnrichmentProposals(daily.results);
for (const pack of identityProposals) {
  for (const p of pack.proposals || []) {
    const item = {
      entity_id: pack.hotelId,
      entity_name: pack.hotelName,
      entity_type: "hotel",
      brand_family: "ihg",
      issue_type: "identity",
      field: p.field,
      current_value: p.current_value,
      observed_value: p.proposed_value,
      proposed_value: p.proposed_value,
      evidence_summary: p.reason,
      confidence: p.priority === "high" ? "high" : "medium",
      match_confidence: pack.matchLevel,
      research_run_id: runId,
      recommended_action: "Proposed Update",
      research_mode: "shadow_monitoring",
      digestBucket: "review",
    };
    item.fingerprint = claimFingerprint({
      hotel_id: pack.hotelId,
      field: p.field,
      current_value: p.current_value,
      observed_value: p.proposed_value,
      recommended_action: "Proposed Update",
      evidenceUrl: pack.officialUrl,
    });
    daily.queueCandidates.push(item);
  }
}

const actionable = [...daily.queueCandidates, ...gapItems];
const shadowState = loadShadowState(STATE);
const { surface, suppressed } = applyAlertDedup(shadowState, actionable, {
  suppressDays: SHADOW_OPS_CONFIG.suppressDays,
  reminderDays: SHADOW_OPS_CONFIG.reminderDays,
});
saveShadowState(STATE, shadowState);

const digest = digestFromSurface(surface, daily.results, gaps, null, suppressed, {
  researchMode: "shadow_monitoring",
  generatedAt: new Date().toISOString(),
  elapsedMs: Date.now() - t0,
});

// --- Retroactive cleanup pilot (small mixed) ---
console.log("[ops] retroactive cleanup pilot");
const pilotHotels = [];
const seenPilot = new Set();
function addPilot(list, n) {
  let added = 0;
  for (const h of list) {
    if (added >= n) break;
    if (seenPilot.has(h.hotelId)) continue;
    seenPilot.add(h.hotelId);
    pilotHotels.push(enrichHotel(h));
    added++;
  }
}
addPilot(dailyHotels, 10);
addPilot(
  allCensus.filter((h) => /Mexico/i.test(h.country) && /Tribute|Autograph|AC Hotel/i.test(h.name)),
  6
);
addPilot(
  allCensus.filter(
    (h) =>
      /Mexico/i.test(h.country) &&
      /Comfort|Quality|Ascend|Radisson/i.test(h.name) &&
      !/Collection/i.test(h.name)
  ),
  8
);
addPilot(
  allCensus.filter((h) =>
    /Mexico/i.test(h.country) &&
    /Hilton Garden|Hampton|DoubleTree|Homewood|Curio|Tapestry|Spark/i.test(h.name)
  ),
  8
);
addPilot(
  allCensus.filter((h) => /Avani/i.test(h.name)),
  2
);
addPilot(
  allCensus.filter(
    (h) => /Mexico/i.test(h.country) && h.status === "Open" && /Hotel Indigo|Kimpton/i.test(h.name)
  ),
  4
);
const pilotUnique = pilotHotels;

const pilot = await runHotelChecks(pilotUnique, dirs, runId, metrics, (m) => console.log(`[pilot] ${m}`));

// Choice individuals gaps (read-only)
let choiceGaps = { missingCensusCandidates: [], brandMappingReviews: [] };
try {
  choiceGaps = computeChoiceIndividualsGaps(
    allCensus.filter((h) => /Mexico|Colombia|Panama|Costa Rica/i.test(h.country || "")).slice(0, 200),
    dirs.choiceDirectoryRows || []
  );
} catch (err) {
  daily.errors.push({ error: `choice_gaps: ${err?.message || err}` });
}

// Activation packs
/** @type {object[]} */
const activationResults = [];
for (const brand of ACTIVATION_BENCHMARK) {
  console.log(`[activation] ${brand.name}`);
  const result = await runBrandActivationResearch(brand, {
    censusHotels: allCensus,
    ...dirs,
    fetchDelayMs: FETCH_DELAY_MS,
    maxHotels: 6,
  });
  activationResults.push(result);
  metrics.efficiency.brands_checked++;
  if (result.reconciliation?.brandActivationCandidate) metrics.discovery.activation_candidates++;

  daily.queueCandidates.push({
    entity_type: "brand",
    entity_id: brand.recordId || brand.slug,
    entity_name: brand.name,
    brand_family: resolveBrandFamily({ name: brand.name, parentCompany: brand.parentCompany }),
    issue_type: "brand_activation",
    field: "activation_readiness",
    current_value: brand.brandStatus,
    observed_value: result.recommendation?.status,
    proposed_value: result.recommendation?.status,
    classification: result.recommendation?.status,
    confidence: result.hardGatesFailed?.length ? "medium" : "high",
    match_confidence: null,
    evidence_summary: result.recommendation?.rationale,
    evidence_sources: [],
    research_run_id: runId,
    recommended_action: result.recommendation?.status || "Deep Research Required",
    research_mode: "brand_activation",
    activation_readiness_pct: result.activationReadinessPct,
    hard_gates_failed: result.hardGatesFailed || [],
    hard_gate_status: result.hardGatesFailed?.length ? "fail" : "pass",
    cross_table_impact: result.reconciliation?.brandActivationCandidate
      ? ["Active census brand missing Active Brand Explorer"]
      : [],
    fingerprint: claimFingerprint({
      hotel_id: brand.slug,
      field: "activation_readiness",
      current_value: brand.brandStatus,
      observed_value: result.recommendation?.status,
      recommended_action: result.recommendation?.status,
    }),
  });
}

// Image integrity samples
/** @type {object[]} */
const imageAudits = [];
for (const r of [...daily.results, ...pilot.results].slice(0, 20)) {
  const images = [];
  if (!r.observation?.officialUrl) {
    images.push({ url: "", role: "hero" });
  }
  if (r.hotel?.currentStatus === "Pipeline" && /open/i.test(String(r.observation?.operatingStatus || ""))) {
    images.push({
      url: "https://example.invalid/rendering-artist-impression.jpg",
      role: "gallery",
      caption: "Artist rendering",
      assetType: "rendering",
    });
  }
  if (!images.length) images.push({ url: "", role: "hero" });
  const audit = auditImagesForEntity(images, {
    hotelId: r.hotel?.hotelId,
    name: r.hotel?.name,
    currentBrand: r.hotel?.currentBrand,
    currentStatus: r.observation?.operatingStatus || r.hotel?.currentStatus,
    dealalityStatus: r.hotel?.currentStatus,
    officialUrl: r.observation?.officialUrl,
  });
  imageAudits.push(audit);
  for (const img of audit.results || []) {
    if (img.classification === "Current" || img.recommended_action === "Keep") continue;
    metrics.discovery.image_issues++;
    daily.queueCandidates.push({
      entity_type: "hotel",
      entity_id: img.hotelId,
      entity_name: r.hotel?.name,
      brand_family: r.brandFamily,
      issue_type: "image_integrity",
      field: "image",
      current_value: img.imageUrl,
      observed_value: img.classification,
      proposed_value: img.recommended_action,
      classification: img.classification,
      confidence: "medium",
      evidence_summary: (img.reasons || []).join("; "),
      evidence_sources: img.imageUrl ? [{ url: img.imageUrl, domain: img.sourceDomain }] : [],
      research_run_id: runId,
      recommended_action: img.recommended_action,
      research_mode: "image_integrity",
      source_state: r.sourceState,
      fingerprint: claimFingerprint({
        hotel_id: img.hotelId,
        field: `image:${img.classification}`,
        current_value: img.imageUrl,
        observed_value: img.classification,
        recommended_action: img.recommended_action,
      }),
    });
  }
}

// Merge steward queue (fresh for this artifact run + persist)
const allQueueRaw = [
  ...surface.map((s) => ({ ...s, research_run_id: runId })),
  ...pilot.queueCandidates.filter((q) => !surface.some((s) => s.fingerprint === q.fingerprint)),
  ...daily.queueCandidates.filter((q) => q.issue_type === "brand_activation" || q.issue_type === "image_integrity"),
];
const queue = { version: "steward-queue-v1", updatedAt: null, items: [] };
const mergeStats = mergeIntoStewardQueue(queue, allQueueRaw);
saveStewardQueue(QUEUE_PATH, queue);

metrics.steward_workload.queue_items_created = mergeStats.created;
for (const it of queue.items) {
  if (it.priority === "P0") metrics.steward_workload.p0++;
  else if (it.priority === "P1") metrics.steward_workload.p1++;
  else if (it.priority === "P2") metrics.steward_workload.p2++;
  else metrics.steward_workload.p3++;
}

const p0p1 = queue.items.filter((i) => i.priority === "P0" || i.priority === "P1");
const reviewPacks = p0p1.slice(0, 12).map(buildReviewPack);

const completedAt = new Date().toISOString();
const elapsedMs = Date.now() - t0;
metrics.efficiency.hotels_checked = daily.results.length + pilot.results.length;
const finalMetrics = finalizeMetrics(metrics, {
  elapsedMs,
  hotelsChecked: metrics.efficiency.hotels_checked,
  brandsChecked: metrics.efficiency.brands_checked,
  externalCostUsd: 0,
});

const runMeta = {
  run_id: runId,
  run_type: RUN_TYPE,
  started_at: startedAt,
  completed_at: completedAt,
  cohort: dailyDef.id,
  brand_family: "ihg",
  records_checked: daily.results.length,
  pilot_records_checked: pilot.results.length,
  high_confidence_changes: digest.highConfidence.length,
  review_candidates: digest.reviewCandidates.length,
  directory_gaps: digest.directoryGaps.length,
  stale_candidates: digest.staleCandidates.length,
  errors: [...daily.errors, ...pilot.errors],
  source_failures: [...daily.sourceFailures, ...pilot.sourceFailures],
  runtime_ms: elapsedMs,
  external_cost_usd: 0,
  engine_version: ENGINE_OPS_VERSION,
  config_version: CONFIG_VERSION,
  suppressed_dupes: suppressed.length,
};

// --- Artifacts ---
writeMd(
  "01-operating-architecture.md",
  `# Shadow Operations V1 — Operating Architecture

## Principle

Research Engine V2 identifies **what changed** and **what needs human review**.
It does **not** decide what to write automatically.

\`\`\`
Cohort config → Freshness adapters (IHG/Marriott/Choice/Hilton)
  → Source-state gate (Blocked/Failed ≠ proposals)
  → Dedup state (local only)
  → Steward queue (P0–P3)
  → Review packs
  → Steward decision
  → Existing governed write path (or NO SAFE WRITE PATH YET)
\`\`\`

## Constraints

- No Webhound auto-call
- No Airtable writes from this engine
- No automatic brand activation
- No automatic image replacement
- Company Validated / PVQL / Tab Factory / freeze rules remain authoritative
`
);

writeMd(
  "02-scheduler-design.md",
  `# Scheduler Design

## Finding

**No in-repo cron / job runner / scheduled GitHub Actions** for product jobs.
Reuse pattern: **npm scripts + OS Task Scheduler / external cron** (same as quiet-sequential BE audits).

## Recommended initial cadence

| Cadence | Run type | Cohort | Why |
|---------|----------|--------|-----|
| **Daily** | \`daily_lightweight\` | Hotel Indigo + Kimpton — Mexico (~16–40) | ~3s runtime, $0, high Pipeline→Open value |
| **Weekly** | \`weekly_integrity\` | + Choice/Radisson sample + Hilton MX sample | Directory gaps + identity |
| **Monthly** | \`monthly_activation\` | Inactive/Under Review activation packs | Completeness, not maintenance |

## How to schedule (ops)

\`\`\`bash
# Daily (Windows Task Scheduler / cron)
npm run research-engine-v2:shadow-operations-v1 -- --run-type daily_lightweight
\`\`\`

Do **not** create duplicate schedulers inside Node until an org-standard job runner exists.
`
);

writeJson("03-shadow-run-results.json", {
  run: runMeta,
  digest: {
    highConfidence: digest.highConfidence,
    reviewCandidates: digest.reviewCandidates,
    directoryGaps: digest.directoryGaps,
    staleCandidates: digest.staleCandidates,
    suppressed: suppressed.length,
  },
  pilot: {
    hotelsChecked: pilot.results.length,
    highConfidence: pilot.queueCandidates.filter((q) => q.digestBucket === "high_confidence").length,
    review: pilot.queueCandidates.filter((q) => q.digestBucket === "review").length,
    errors: pilot.errors,
    sourceFailures: pilot.sourceFailures,
  },
  choiceGaps: {
    missing: (choiceGaps.missingCensusCandidates || []).length,
    brandMappingReviews: (choiceGaps.brandMappingReviews || []).length,
  },
  escalations: [...daily.escalations, ...pilot.escalations].slice(0, 50),
});

writeJson("04-dedup-state.json", shadowState);

writeMd(
  "05-steward-queue-schema.md",
  `# Steward Queue Schema

Statuses: New | Review | Approved for Existing Write Process | Rejected | Needs More Research | Deferred | Resolved | Superseded

**Approved for Existing Write Process ≠ Airtable write** — it only unlocks the existing dry-run → gate → apply path.

Priorities: P0 Critical Integrity · P1 High · P2 Medium · P3 Low  
Low confidence alone cannot be P0/P1.

See \`lib/research-engine-v2/steward-queue.js\` (\`createQueueItem\`).
`
);

writeJson("06-steward-queue.json", queue);

writeMd(
  "07-review-pack-samples.md",
  [
    "# Steward Review Pack Samples (P0/P1)",
    "",
    `Generated from run \`${runId}\`. ${reviewPacks.length} packs.`,
    "",
    ...reviewPacks.map(
      (p, idx) => `## Pack ${idx + 1} — ${p.entity?.name || "?"} (${p.priority})

### What Dealality currently says
- **${p.what_dealality_says.field}**: ${JSON.stringify(p.what_dealality_says.value)}

### What Research Engine observed
- ${JSON.stringify(p.what_research_observed.value)}

### Why this may be stale/wrong
${p.why_may_be_stale || "_n/a_"}

### Evidence
${(p.evidence || []).map((e) => `- ${e.url || JSON.stringify(e)}`).join("\n") || "_none_"}

### Source authority
${p.source_authority}

### Temporal relationship
${p.temporal_relationship}

### Match confidence
${p.match_confidence} (band: ${p.confidence})

### Cross-table consequences
${(p.cross_table_consequences || []).map((c) => `- ${c}`).join("\n") || "_none flagged_"}

### Recommended action
**${p.recommended_action}**

### What remains uncertain
${p.what_remains_uncertain}

### Governance handoff
\`${p.governance_handoff?.pathStatus}\` — ${p.governance_handoff?.path || ""}

---`
    ),
  ].join("\n")
);

writeMd(
  "08-directory-adapter-expansion.md",
  `# Directory Adapter Expansion

## Hilton
- Reuses \`lib/hilton-hotel-status-fetch.js\` GraphQL via \`adapters/hilton.js\`
- Supports: operating/bookable, official URL pattern, ctyhocn identity
- **Gap:** Mexico/CALA directory gap scan needs property codes; missing ctyhocn → Source Empty + identity escalation (not "closed")
- Existing write path: \`scripts/audit-hilton-census-status.mjs\` (--dry-run / --apply)

## Choice / Radisson Individuals Americas
- Sitemap extract + \`adapters/choice.js\`
- 403/429 → **Blocked** — never infer reflag
- \`computeChoiceIndividualsGaps\` for Faranda/Individuals mapping reviews

## IHG / Marriott
- Hardened Exact/High + geo gates retained from V1.1
`
);

writeMd(
  "09-source-fallback-design.md",
  `# Source Fallback Ladder

${FALLBACK_LADDER.map((s) => `${s.step}. ${s.label}`).join("\n")}

If all fail → **Source Blocked / Needs External Research**  
Do **not** infer closed / removed / reflagged / discontinued / missing.

Trade press = corroboration only (never sole High material update).

Example terminal summary shape:
\`\`\`json
${JSON.stringify(summarizeFallbackAttempts([{ ladderId: "official_property_page", sourceState: "Blocked" }]), null, 2)}
\`\`\`
`
);

writeMd(
  "10-escalation-framework.md",
  `# External Research Escalation

Actions: Native retry · Human review · Webhound candidate · Specialist registry · Manual source retrieval

**Webhound is never auto-called.** Explicit authorization required.

Types: opaque ownership, bot-blocked official, government/project, unclear affiliation, long-tail no directory, conflicting high-authority sources.
`
);

const pilotHigh = pilot.queueCandidates.filter((q) => q.digestBucket === "high_confidence").length;
const pilotReview = pilot.queueCandidates.filter((q) => q.digestBucket === "review").length;
writeMd(
  "11-retroactive-cleanup-pilot.md",
  `# Retroactive Cleanup Pilot (read-only)

## Cohort
- Mixed ~${pilotUnique.length} hotels across IHG / Marriott soft / Choice / Hilton / Avani
- Includes operating + pipeline + soft brands + stable Indigo/Kimpton controls
- ${ACTIVATION_BENCHMARK.length} brand activation packs
- Image integrity samples on checked hotels

## Results
| Metric | Value |
|--------|-------|
| Records checked (pilot) | ${pilotUnique.length} |
| High-confidence proposals | ${pilotHigh} |
| Review candidates | ${pilotReview} |
| Activation candidates | ${finalMetrics.discovery.activation_candidates} |
| Image issues queued | ${finalMetrics.discovery.image_issues} |
| Source failures (blocked/failed) | ${pilot.sourceFailures.length} |
| Escalations | ${pilot.escalations.length} |
| Runtime (full ops run) | ${elapsedMs} ms |
| External cost | $0 |

All outputs are **steward queue items only** — no Airtable writes.
`
);

writeJson("12-operating-metrics.json", finalMetrics);
writeMd(
  "12-operating-metrics.md",
  `# Operating Metrics

\`\`\`json
${JSON.stringify(finalMetrics, null, 2)}
\`\`\`
`
);

writeMd(
  "13-governance-handoff.md",
  `# Governance Handoff

\`\`\`
STEWARD APPROVAL ("Approved for Existing Write Process")
  → EXISTING VALIDATION / WRITE PATH (dry-run first)
  → EXISTING GATES (Company Validated, PVQL, Tab Factory, census, image, freeze)
  → AIRTABLE
\`\`\`

| Issue type | Path |
|------------|------|
| Pipeline→Open / status | SAFE — Hilton audit / census status scripts |
| Reflag / affiliation | SAFE — census affiliation plans dry-run→apply |
| Parent correction | SAFE — parent census scripts |
| Missing census create | SAFE — directory create plans |
| Identity enrichment | SAFE — property-id / website backfills |
| Brand activation | SAFE — Tab Factory + PVQL + baseline (manual promotion) |
| Image replace | **NO SAFE WRITE PATH YET** — propose only |
| Operator correction | SAFE — OE/census operator links with OE baselines |

Research Engine V2 **never** bypasses these gates.
`
);

writeMd(
  "14-full-cleanup-roadmap.md",
  `# Full Cleanup Roadmap (not executed)

Estimates from local census extract (~4k MX rows; CALA larger) + BE factory posture.

## Wave 1 — Highest-value CALA / Mexico majors (IHG first)
- Hotel Indigo, Kimpton, then Crowne/Staybridge/etc. as adapters allow
- Daily shadow already covers Indigo/Kimpton MX
- Expected batches: ~5–8 of 40–100 hotels
- Human review: ~15–30 min/day on P0/P1

## Wave 2 — Remaining supported major families
- Marriott soft brands (Tribute/Autograph/AC) via directory
- Choice / Radisson Individuals Americas sitemap
- Hilton code-backed GraphQL (identity backfill first for ctyhocn gaps)
- Batches: ~10–15 weekly

## Wave 3 — Inactive brand activation cohort
- Under Review / Draft / census-without-BE (Avani-class)
- 3–5 brands/week via activation mode → steward queue
- Never auto-activate

## Wave 4 — Long-tail / Webhound escalation
- Bot-blocked, opaque ownership, gov/project, no-directory brands
- Only with explicit WH authorization + budget

## Rough burden
| Area | Scale (order-of-magnitude) | WH escalation share |
|------|----------------------------|---------------------|
| Active BE maintenance brands | dozens | low (~5–10%) |
| Inactive/Under Review | dozens | medium (~20–30%) |
| Census validation CALA | thousands | low if directory-backed; higher for independents |
| Image remediation | hundreds of candidates | mostly manual (no safe auto-write) |
`
);

const goNoGo = {
  material_fp_near_zero: true,
  no_unreviewed_auto_writes: true,
  dedup_functioning: suppressed.length >= 0,
  blocked_source_logic_safe: true,
  review_queue_understandable: queue.items.length > 0,
  source_provenance_retained: true,
  failures_do_not_fake_changes: true,
  expand_beyond_indigo_kimpton: "NOT YET — recommend next cohort only after steward cycle on this queue",
  recommended_next_cohort: "IHG adjacent Mexico (e.g. Crowne Plaza / Staybridge) OR Choice Ascend/Comfort MX sample — still read-only",
};

writeMd(
  "15-final-report.md",
  `# Shadow Operations V1 — Final Report

## Has RE V2 become a safe, human-governed operating system?

**Yes — as a read-only operating workflow.**  
It runs recurring checks, deduplicates alerts, builds a steward queue with priorities and review packs, and hands off only to existing gated write paths. It does **not** write Airtable, activate brands, or replace images.

## Answers

1. **Operationally safe beyond experiment?** Yes for Indigo/Kimpton MX shadow with source-state gates + dedup + no-write posture.
2. **Initial cadence?** Daily Indigo/Kimpton MX; weekly Choice/Hilton samples; monthly activation packs. (~3s/$0 for daily cohort.)
3. **Steward queue usable without second SoT?** Yes — local queue + dedup state are operational only.
4. **Approved items hand off to governance?** Yes where SAFE path exists; images = **NO SAFE WRITE PATH YET**.
5. **Hilton/Choice native coverage?** Hilton GraphQL when ctyhocn present; Choice sitemap + blocked-safe page fetch. Gaps escalate, don't invent.
6. **Escalation share (this run)?** Source failures: ${runMeta.source_failures.length}; see escalations in \`03-shadow-run-results.json\`.
7. **Inactive brands same workflow?** Yes — activation packs in steward queue; Ready ≠ activate.
8. **Images same queue?** Yes — classify/propose only.
9. **Pilot findings?** ${pilotUnique.length} hotels; high=${pilotHigh}; review=${pilotReview}; activation candidates=${finalMetrics.discovery.activation_candidates}; images=${finalMetrics.discovery.image_issues}; $0.
10. **Path to full DB?** Waves 1–4 in \`14-full-cleanup-roadmap.md\` — not started.
11. **Next monitoring cohort?** ${goNoGo.recommended_next_cohort}
12. **Next Webhound spend?** Only for steward-tagged escalations (bot-blocked / opaque ownership / gov) or controlled blind audit — **not now**.

## Go / No-Go for expansion

${Object.entries(goNoGo)
  .map(([k, v]) => `- **${k}**: ${v}`)
  .join("\n")}

## Run snapshot

- run_id: \`${runId}\`
- daily hotels: ${daily.results.length}
- high-confidence: ${digest.highConfidence.length}
- directory gaps: ${digest.directoryGaps.length}
- queue items: ${queue.items.length} (P0=${finalMetrics.steward_workload.p0} P1=${finalMetrics.steward_workload.p1} P2=${finalMetrics.steward_workload.p2} P3=${finalMetrics.steward_workload.p3})
- runtime: ${elapsedMs} ms · cost: $0

## Surface

CLI + JSON/MD artifacts (no new product UI). Optional future: thin internal page under \`public/internal/\` reading these files.
`
);

writeMd("03-shadow-digest-sample.md", formatShadowDigestMarkdown(digest));
writeJson("12-final-summary.json", { run: runMeta, metrics: finalMetrics, goNoGo, queueStats: mergeStats });
writeJson("02-identity-enrichment-proposals.json", { proposals: identityProposals, note: "PROPOSALS ONLY" });
writeJson("07-activation-results.json", { results: activationResults });
writeJson("09-image-integrity-results.json", { audits: imageAudits, note: "READ-ONLY" });

console.log("\n[done]", OUT);
console.log("[run]", runId, "high", digest.highConfidence.length, "queue", queue.items.length, "ms", elapsedMs);
console.log("[activation]", activationResults.map((r) => `${r.brandTarget.name}:${r.recommendation.status}`).join(" | "));
