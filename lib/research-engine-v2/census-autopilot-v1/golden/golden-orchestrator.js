/**
 * Census Autopilot V1.2 — Golden Census 95% completion orchestrator.
 * No Webhound, no credits, no Airtable writes.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { randomUUID } from "node:crypto";

import {
  GOLDEN_SCHEMA_VERSION,
  GOLDEN_FIELD_REGISTRY,
  priorityFields,
  buildApplicabilityMap,
  fieldsByTrack,
} from "./golden-schema.js";
import {
  GOLDEN_GEO_VERSION,
  buildMexicoMarketSubmarketTaxonomy,
  assignDealalityGeography,
} from "./golden-geography.js";
import {
  scoreHotelGoldenCompleteness,
  aggregatePortfolioScores,
  buildFieldMissingness,
  groupCompletion,
  separateTrackScore,
  hasSupportedValue,
  VALUE_STATUS,
} from "./golden-completeness.js";
import { buildGoldenFieldMap } from "./golden-enrichment.js";
import { buildGoldenFieldRoutingPlan } from "./golden-field-routing.js";
import {
  liveDeepResearchHotel,
  warmFamilyDirectoryCaches,
} from "../live-deep-research.js";
import { sleep } from "../../adapters/adapter-utils.js";
import { writeGolden95Artifacts } from "./golden-artifact-writer.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, "../../../..");

const DEFAULT_VIC = path.join(
  ROOT,
  "data/research-engine-v2/verified-independent-census-mexico-combined-4family/01_combined_4family_index.json"
);
const ARTIFACT_DIR = "data/research-engine-v2/census-autopilot-v1-2-golden-95";

function loadMexicoBenchmark(vicPath) {
  const vic = JSON.parse(fs.readFileSync(vicPath, "utf8"));
  return (vic.records || []).filter(
    (r) =>
      r.country === "Mexico" &&
      ["IHG", "Hilton", "Choice"].includes(r.family)
  );
}

function scoreRecords(records, liveById) {
  const hotelScores = [];
  const fieldMaps = [];
  const contexts = [];
  const perHotel = [];

  for (const record of records) {
    const live = liveById.get(record.independent_record_id) || null;
    const { fieldMap, geo } = buildGoldenFieldMap(record, live);
    const ctx = {
      market: geo.Market,
      Market: geo.Market,
      family: record.family,
      brand: record.brand,
      property_id: (record.property_ids && record.property_ids[0]) || record.property_id,
    };
    const score = scoreHotelGoldenCompleteness(fieldMap, ctx);
    fieldMaps.push(fieldMap);
    contexts.push(ctx);
    hotelScores.push(score);
    perHotel.push({
      independent_record_id: record.independent_record_id,
      name: record.name,
      family: record.family,
      brand: record.brand,
      city: record.city,
      market: geo.Market,
      submarket: geo.Submarket,
      ...score,
      rooms: fieldMap["Rooms / Keys"]?.value ?? null,
      unknown_fields: score.unknown_fields,
    });
  }

  const portfolio = aggregatePortfolioScores(hotelScores);
  const missingness = buildFieldMissingness(fieldMaps, contexts);
  return { hotelScores, fieldMaps, contexts, perHotel, portfolio, missingness };
}

async function mapPool(items, concurrency, fn) {
  const results = new Array(items.length);
  let i = 0;
  async function worker() {
    while (i < items.length) {
      const idx = i++;
      results[idx] = await fn(items[idx], idx);
    }
  }
  const n = Math.max(1, concurrency);
  await Promise.all(Array.from({ length: n }, () => worker()));
  return results;
}

/**
 * @param {object} opts
 */
export async function runGolden95Benchmark(opts = {}) {
  const started = Date.now();
  const log = opts.log || console.log;
  const artifactRoot = path.join(ROOT, opts.artifactDir || ARTIFACT_DIR);
  fs.mkdirSync(artifactRoot, { recursive: true });

  const runId =
    opts.runId ||
    `cav12_${new Date().toISOString().replace(/[:.]/g, "-").slice(0, 19)}_${randomUUID().slice(0, 6)}`;
  const runDir = path.join(artifactRoot, "runs", runId);
  fs.mkdirSync(runDir, { recursive: true });

  const records = loadMexicoBenchmark(opts.vicIndexPath || DEFAULT_VIC);
  const limited = opts.maxRecords ? records.slice(0, opts.maxRecords) : records;
  log(`[v1.2] Golden Census 95% — ${limited.length} hotels (IHG+Hilton+Choice Mexico)`);
  log(`[v1.2] constraints: no Webhound, no credits, no Airtable writes`);

  // —— Schema artifacts prep
  const applicability = buildApplicabilityMap();
  const routing = buildGoldenFieldRoutingPlan();
  const taxonomy = buildMexicoMarketSubmarketTaxonomy();

  // —— PASS 0: baseline (VIC + geography/classification only; no live fetch)
  log(`[v1.2] PASS 0 — baseline under Golden Priority Schema (no live fetch)`);
  const liveById = new Map();
  const baseline = scoreRecords(limited, liveById);
  fs.writeFileSync(
    path.join(runDir, "baseline-score.json"),
    JSON.stringify(
      {
        portfolio: baseline.portfolio,
        top_missing: baseline.missingness.slice(0, 20),
      },
      null,
      2
    )
  );
  log(
    `[v1.2] baseline raw avg=${baseline.portfolio.average_raw_priority_completeness_pct}% | ≥95% hotels=${baseline.portfolio.hotels_at_or_above_95_pct}`
  );

  // —— Warm directories once
  log(`[v1.2] warming family directory caches…`);
  try {
    await warmFamilyDirectoryCaches({
      families: ["IHG", "Hilton", "Choice"],
      country: "Mexico",
    });
  } catch (err) {
    log(`[v1.2] directory warm warning: ${err?.message || err}`);
  }

  // —— PASS 1: live deep research all hotels
  log(`[v1.2] PASS 1 — live Lane A/B deep research`);
  const pass1Hotels = [];
  const delayMs = opts.delayMs ?? 300;
  const concurrency = opts.concurrency ?? 3;
  const timeoutMs = opts.timeoutMs ?? 25000;

  await mapPool(limited, concurrency, async (record, idx) => {
    try {
      const live = await liveDeepResearchHotel(record, { delayMs, timeoutMs });
      liveById.set(record.independent_record_id, live);
      pass1Hotels.push({
        id: record.independent_record_id,
        family: record.family,
        page_ok: live.page_ok,
        fields_resolved: live.fields_resolved,
        rooms: (live.fields || []).find((f) => f.field === "Rooms / Keys")?.researched_value ?? null,
      });
      if ((idx + 1) % 25 === 0 || idx === limited.length - 1) {
        log(`[v1.2] pass1 ${idx + 1}/${limited.length}`);
      }
    } catch (err) {
      log(`[v1.2] pass1 error ${record.independent_record_id}: ${err?.message || err}`);
      liveById.set(record.independent_record_id, {
        fields: [],
        page_ok: false,
        error: err?.message || String(err),
        legacy_used_as_source: false,
        cvent_used_as_source: false,
      });
    }
    if (delayMs) await sleep(Math.min(delayMs, 150));
  });

  const afterPass1 = scoreRecords(limited, liveById);
  fs.writeFileSync(
    path.join(runDir, "pass1-score.json"),
    JSON.stringify({ portfolio: afterPass1.portfolio, sample: pass1Hotels.slice(0, 5) }, null, 2)
  );
  log(
    `[v1.2] after pass1 raw avg=${afterPass1.portfolio.average_raw_priority_completeness_pct}% | ≥95%=${afterPass1.portfolio.hotels_at_or_above_95_pct}`
  );

  // —— PASS 2: gap attack — hotels <95% missing high-impact fields
  log(`[v1.2] PASS 2 — autonomous gap attack (rooms + blocked pages retry)`);
  const gapTargets = afterPass1.perHotel
    .filter((h) => !h.meets_95)
    .sort((a, b) => {
      // Prefer missing rooms (critical) then lowest completeness
      const aRooms = a.rooms == null ? 1 : 0;
      const bRooms = b.rooms == null ? 1 : 0;
      if (aRooms !== bRooms) return bRooms - aRooms;
      return a.raw_priority_completeness_pct - b.raw_priority_completeness_pct;
    });

  const pass2Limit = opts.pass2Limit ?? gapTargets.length;
  const pass2Ids = gapTargets.slice(0, pass2Limit).map((h) => h.independent_record_id);
  const pass2Records = limited.filter((r) => pass2Ids.includes(r.independent_record_id));

  await mapPool(pass2Records, concurrency, async (record, idx) => {
    try {
      const live = await liveDeepResearchHotel(record, {
        delayMs: delayMs + 100,
        timeoutMs: Math.max(timeoutMs, 35000),
      });
      // Keep prior if new resolves fewer material fields
      const prev = liveById.get(record.independent_record_id);
      const prevRooms = (prev?.fields || []).find((f) => f.field === "Rooms / Keys")?.researched_value;
      const newRooms = (live.fields || []).find((f) => f.field === "Rooms / Keys")?.researched_value;
      if (newRooms != null || (live.fields_resolved || 0) >= (prev?.fields_resolved || 0)) {
        liveById.set(record.independent_record_id, live);
      } else if (prevRooms == null && live.page_html_snippet) {
        liveById.set(record.independent_record_id, { ...prev, page_html_snippet: live.page_html_snippet });
      }
      if ((idx + 1) % 20 === 0 || idx === pass2Records.length - 1) {
        log(`[v1.2] pass2 ${idx + 1}/${pass2Records.length}`);
      }
    } catch (err) {
      log(`[v1.2] pass2 error ${record.independent_record_id}: ${err?.message || err}`);
    }
  });

  const afterPass2 = scoreRecords(limited, liveById);
  log(
    `[v1.2] after pass2 raw avg=${afterPass2.portfolio.average_raw_priority_completeness_pct}% | ≥95%=${afterPass2.portfolio.hotels_at_or_above_95_pct}`
  );

  // —— PASS 3 (final): diminishing-returns check — optional third pass if still climbing
  let afterFinal = afterPass2;
  let pass3Ran = false;
  const delta12 =
    afterPass2.portfolio.average_raw_priority_completeness_pct -
    afterPass1.portfolio.average_raw_priority_completeness_pct;
  const stillBelow = afterPass2.portfolio.average_raw_priority_completeness_pct < 95;
  const stillHasResearchable =
    afterPass2.missingness.filter(
      (m) =>
        m.hotels_missing > 0 &&
        ["Rooms / Keys", "Phone", "Address", "Latitude", "Amenities - Source Text", "Meeting / Event Space", "F&B Flag"].includes(
          m.field
        )
    ).length > 0;

  if (stillBelow && delta12 >= 0.3 && stillHasResearchable) {
    pass3Ran = true;
    log(`[v1.2] PASS 3 — another gap pass (delta pass1→2=${delta12.toFixed(1)}pp)`);
    const targets = afterPass2.perHotel
      .filter((h) => !h.meets_95 && h.rooms == null)
      .slice(0, opts.pass3Limit ?? 120);
    const recs = limited.filter((r) =>
      targets.some((t) => t.independent_record_id === r.independent_record_id)
    );
    await mapPool(recs, concurrency, async (record) => {
      try {
        const live = await liveDeepResearchHotel(record, {
          delayMs: delayMs + 150,
          timeoutMs: 40000,
        });
        const prev = liveById.get(record.independent_record_id);
        const newRooms = (live.fields || []).find((f) => f.field === "Rooms / Keys")?.researched_value;
        if (newRooms != null || (live.fields_resolved || 0) > (prev?.fields_resolved || 0)) {
          liveById.set(record.independent_record_id, live);
        }
      } catch {
        /* logged via empty */
      }
    });
    afterFinal = scoreRecords(limited, liveById);
    log(
      `[v1.2] after pass3 raw avg=${afterFinal.portfolio.average_raw_priority_completeness_pct}% | ≥95%=${afterFinal.portfolio.hotels_at_or_above_95_pct}`
    );
  } else {
    log(
      `[v1.2] PASS 3 skipped — diminishing or exhausted (delta12=${delta12.toFixed(1)}pp, stillBelow=${stillBelow})`
    );
  }

  // —— Escalation map
  const escalation = buildEscalationMap(afterFinal, limited);

  // —— Group scores
  const groups = {
    identity_geography: groupCompletion(afterFinal.fieldMaps, afterFinal.contexts, "G1_"),
    physical_profile: groupCompletion(afterFinal.fieldMaps, afterFinal.contexts, "G2_"),
    amenities: groupCompletion(afterFinal.fieldMaps, afterFinal.contexts, "G3_"),
    fnb: groupCompletion(afterFinal.fieldMaps, afterFinal.contexts, "G4_"),
    meetings: groupCompletion(afterFinal.fieldMaps, afterFinal.contexts, "G5_"),
    classification: groupCompletion(afterFinal.fieldMaps, afterFinal.contexts, "G6_"),
    content: groupCompletion(afterFinal.fieldMaps, afterFinal.contexts, "G7_"),
  };

  const roomsRow = afterFinal.missingness.find((m) => m.field === "Rooms / Keys");
  const marketRow = afterFinal.missingness.find((m) => m.field === "Market");
  const submarketRow = afterFinal.missingness.find((m) => m.field === "Submarket");
  const continentRow = afterFinal.missingness.find((m) => m.field === "Continent");
  const subContinentRow = afterFinal.missingness.find((m) => m.field === "Sub-Continent");

  const unknownValueCount = afterFinal.perHotel.reduce((s, h) => s + (h.unknown_count || 0), 0);

  const cventLeak = [...liveById.values()].some((l) => l?.cvent_used_as_source);
  const legacyLeak = [...liveById.values()].some((l) => l?.legacy_used_as_source);
  let unsupportedStaged = 0;
  for (const fm of afterFinal.fieldMaps) {
    for (const [k, cell] of Object.entries(fm)) {
      if (cell?.cvent_used || cell?.legacy_used) unsupportedStaged += 1;
      if (cell?.status === VALUE_STATUS.SUPPORTED && !hasSupportedValue(cell.value)) unsupportedStaged += 1;
    }
  }

  const result = {
    run_id: runId,
    version: GOLDEN_SCHEMA_VERSION,
    geo_version: GOLDEN_GEO_VERSION,
    hotels: limited.length,
    elapsed_ms: Date.now() - started,
    external_cost_usd: 0,
    airtable_writes: 0,
    webhound_calls: 0,
    baseline: baseline.portfolio,
    pass1: afterPass1.portfolio,
    pass2: afterPass2.portfolio,
    final: afterFinal.portfolio,
    pass3_ran: pass3Ran,
    delta_baseline_to_final:
      afterFinal.portfolio.average_raw_priority_completeness_pct -
      baseline.portfolio.average_raw_priority_completeness_pct,
    groups,
    rooms_completion_pct: roomsRow?.completion_pct ?? null,
    market_completion_pct: marketRow?.completion_pct ?? null,
    submarket_completion_pct: submarketRow?.completion_pct ?? null,
    continent_completion_pct: continentRow?.completion_pct ?? null,
    sub_continent_completion_pct: subContinentRow?.completion_pct ?? null,
    unknown_applicable_field_cells: unknownValueCount,
    escalation,
    firewall: {
      cvent_production_evidence: cventLeak,
      legacy_production_evidence: legacyLeak,
      unsupported_staged_cells: unsupportedStaged,
    },
    separate_tracks: {
      lifecycle: separateTrackScore(afterFinal.fieldMaps, "LIFECYCLE"),
      ownership_operation: separateTrackScore(afterFinal.fieldMaps, "OWNERSHIP_OPERATION"),
      image: separateTrackScore(afterFinal.fieldMaps, "IMAGE"),
      governance: separateTrackScore(afterFinal.fieldMaps, "GOVERNANCE"),
    },
    top_remaining_gaps: afterFinal.missingness.slice(0, 15),
    artifact_root: artifactRoot,
  };

  // Persist run snapshot (compact — not full HTML)
  fs.writeFileSync(
    path.join(runDir, "final-snapshot.json"),
    JSON.stringify(
      {
        result,
        per_hotel: afterFinal.perHotel,
        missingness: afterFinal.missingness,
      },
      null,
      2
    )
  );

  log(`[v1.2] writing artifacts 01–25…`);
  await writeGolden95Artifacts({
    artifactRoot,
    runId,
    schema: {
      version: GOLDEN_SCHEMA_VERSION,
      registry: GOLDEN_FIELD_REGISTRY,
      priority_field_count: priorityFields().length,
      applicability,
      tracks: {
        priority: priorityFields().map((f) => f.field),
        lifecycle: fieldsByTrack("LIFECYCLE").map((f) => f.field),
        ownership: fieldsByTrack("OWNERSHIP_OPERATION").map((f) => f.field),
        image: fieldsByTrack("IMAGE").map((f) => f.field),
        governance: fieldsByTrack("GOVERNANCE").map((f) => f.field),
      },
    },
    taxonomy,
    routing,
    baseline,
    afterPass1,
    afterPass2,
    afterFinal,
    pass1Hotels,
    pass2Count: pass2Records.length,
    pass3Ran,
    escalation,
    groups,
    result,
    limited,
    liveById,
  });

  log(
    `[v1.2] DONE avg=${result.final.average_raw_priority_completeness_pct}% ≥95% hotels=${result.final.hotels_at_or_above_95_share_pct}% cost=$0`
  );
  return result;
}

function buildEscalationMap(scored, records) {
  const byField = {};
  for (const row of scored.missingness) {
    if (row.hotels_missing <= 0) continue;
    let path = "ACCEPT_UNKNOWN";
    if (row.field === "Rooms / Keys") path = "NATIVE_RETRY → FIRST_PARTY_VALIDATION → WEBHOUND_CANDIDATE";
    else if (["Phone", "Address", "Latitude", "Longitude"].includes(row.field)) path = "NATIVE_RETRY";
    else if (["Owner Name", "Operator / Management Company"].includes(row.field)) {
      path = "ACCEPT_UNKNOWN (opaque OK) / FIRST_PARTY optional";
    } else if (row.group?.startsWith("G5_") || row.group?.startsWith("G4_")) {
      path = "NATIVE_RETRY → FIRST_PARTY_VALIDATION";
    } else if (row.field === "Market" || row.field === "Submarket") {
      path = "HUMAN / SPECIALIST taxonomy review";
    }
    byField[row.field] = {
      hotels_missing: row.hotels_missing,
      hotels_applicable: row.hotels_applicable,
      completion_pct: row.completion_pct,
      escalation: path,
      webhound_candidate: row.field === "Rooms / Keys" || row.field === "Opening Date",
    };
  }

  const roomsMissing = byField["Rooms / Keys"]?.hotels_missing || 0;
  const hotelsNeedingWh = scored.perHotel.filter(
    (h) => !h.meets_95 && h.rooms == null
  ).length;

  return {
    by_field: byField,
    hotels_that_would_benefit_from_webhound: hotelsNeedingWh,
    rooms_keys_still_missing: roomsMissing,
    estimated_pct_census_needing_webhound_for_95: Math.round((1000 * hotelsNeedingWh) / Math.max(1, records.length)) / 10,
    first_party_fields: [
      "Rooms / Keys",
      "Opening Date",
      "Amenities confirmation",
      "F&B counts",
      "Meeting space metrics",
      "Operator (optional)",
    ],
    note: "Webhound NOT called in this run — quantification only.",
  };
}

export { assignDealalityGeography, GOLDEN_SCHEMA_VERSION };
