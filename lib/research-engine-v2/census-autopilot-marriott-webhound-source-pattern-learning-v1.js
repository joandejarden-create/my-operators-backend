/**
 * Marriott Webhound Source Pattern Learning Mission v1.
 *
 * Webhound = pattern discovery only (never Census SoT).
 * Census writes = High-confidence official/revalidated sources only.
 * Mode default: field-completion-only (no inserts).
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

import {
  productionHotelPropertyCensus,
  assertProductionCensusWriteTarget,
} from "./production-census-source-of-truth.js";
import { TABLE_IDS } from "./production-census-write.js";
import {
  checkAutopilotApplyEnv,
  applyPreflight,
  parseAutopilotArgs,
} from "./census-autopilot-apply-guard.js";
import {
  resolvePat,
  resolveTargetBase,
} from "./production-census-schema-create.js";
import { isForbiddenAutopilotField } from "./census-autopilot-field-allowlist.js";
import {
  resolveCensusMode,
  assertNoInsertInFieldCompletionMode,
  CENSUS_MODE,
  runFullLatamCensusAutopilotV3Mission,
} from "./census-autopilot-full-latam-v3.js";
import {
  buildCensusGapLedger,
  buildCompletionScorecard,
  writeCensusGapLedger,
} from "./census-gap-ledger.js";
import { buildActiveBrandIndex } from "./census-brand-governance.js";
import { buildCanonicalBrandDictionary } from "./census-brand-canonical-dictionary.js";
import { buildMarriottGapClusterSamples } from "./marriott-source-pattern-discovery.js";
import {
  buildMarriottPatternLearningCatalog,
  extractPatternsFromWebhoundText,
  assertWebhoundNotCensusSot,
  defaultCatalogPath,
  SEED_MARRIOTT_PATTERNS,
} from "./marriott-webhound-pattern-learner.js";
import {
  extractMarriottOfficialMetadata,
  buildMarriottMetadataPatch,
} from "./marriott-official-metadata-adapter.js";
import { validateMarriottRoomsCandidate } from "./marriott-rooms-source-adapter.js";
import {
  buildDamFactsheetUrlIndex,
  writeDamFactsheetUrlIndex,
  discoverAndExtractMarriottDamFactsheet,
  buildDamFactsheetCensusPatch,
  defaultDamIndexPath,
} from "./marriott-dam-factsheet-discovery.js";
import { extractMarshaCode } from "./marriott-hqv-coordinate-client.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, "../..");

export const MARRIOTT_WEBHOUND_LEARNING_V1_OBJECTIVE =
  "marriott-webhound-source-pattern-learning-v1";
export const MARRIOTT_WEBHOUND_LEARNING_V1_VERSION =
  "marriott-webhound-source-pattern-learning-v1";

export const MARRIOTT_WEBHOUND_LEARNING_STATUS = Object.freeze({
  COMPLETE: "production_census_marriott_webhound_source_pattern_learning_v1_complete",
  PARTIAL_SOURCE:
    "production_census_marriott_webhound_source_pattern_learning_v1_partial_source_remaining",
  PARTIAL_ADAPTER:
    "production_census_marriott_webhound_source_pattern_learning_v1_partial_adapter_backlog_remaining",
  BLOCKED: "production_census_marriott_webhound_source_pattern_learning_v1_blocked",
});

const CENSUS_TABLE_ID =
  TABLE_IDS["Hotel Property Census"] || productionHotelPropertyCensus.tableId;

const READ_FIELDS = [
  "Property Identity Key",
  "Property Name",
  "Canonical Property Name",
  "Current Brand",
  "Brand Family",
  "Country",
  "City",
  "State / Region",
  "Address",
  "Address Confidence",
  "Address Source URL",
  "Source URL",
  "Official Property URL",
  "Family / Source Family",
  "Human Review Required",
  "Phone",
  "Rooms / Keys",
  "Rooms Confidence",
  "Rooms Source URL",
  "Rooms Source Type",
  "Rooms Reviewed Date",
  "Latitude",
  "Longitude",
  "Coordinate Source Type",
  "Coordinate Confidence",
  "Geocode Provider",
  "Geocode Method",
  "Geocode Reviewed Date",
  "Enrichment Status",
  "Enrichment Priority",
  "Last Reviewed Date",
  "Production Use Status",
  "Public Display Review Status",
  "Radar Display Status",
];

const ALLOWED_WRITE = new Set([
  "Official Property URL",
  "Source URL",
  "Family / Source Family",
  "State / Region",
  "Address",
  "Address Confidence",
  "Address Source URL",
  "Phone",
  "Rooms / Keys",
  "Rooms Confidence",
  "Rooms Source URL",
  "Rooms Source Type",
  "Rooms Reviewed Date",
  "Latitude",
  "Longitude",
  "Coordinate Source Type",
  "Coordinate Confidence",
  "Geocode Provider",
  "Geocode Method",
  "Geocode Reviewed Date",
  "Enrichment Status",
  "Enrichment Priority",
  "Human Review Required",
  "Last Reviewed Date",
]);

function todayIsoDate() {
  return new Date().toISOString().slice(0, 10);
}

function writeJson(fp, data) {
  fs.mkdirSync(path.dirname(fp), { recursive: true });
  fs.writeFileSync(fp, `${JSON.stringify(data, null, 2)}\n`, "utf8");
}

function writeText(fp, text) {
  fs.mkdirSync(path.dirname(fp), { recursive: true });
  fs.writeFileSync(fp, text, "utf8");
}

function isMarriottRecord(fields = {}) {
  const fam = String(fields["Brand Family"] || fields["Family / Source Family"] || "");
  const url = String(fields["Official Property URL"] || fields["Source URL"] || "");
  return /marriott/i.test(fam) || /marriott\.com/i.test(url);
}

async function listCensus(baseId, token, tableId) {
  const out = [];
  let offset;
  do {
    const params = new URLSearchParams({ pageSize: "100" });
    if (offset) params.set("offset", offset);
    for (const f of READ_FIELDS) params.append("fields[]", f);
    const res = await fetch(
      `https://api.airtable.com/v0/${encodeURIComponent(baseId)}/${encodeURIComponent(tableId)}?${params}`,
      { headers: { Authorization: `Bearer ${token}` } }
    );
    const json = await res.json();
    if (!res.ok) {
      throw new Error(`census list ${res.status}: ${JSON.stringify(json.error || json)}`);
    }
    out.push(...(json.records || []));
    offset = json.offset;
    await new Promise((r) => setTimeout(r, 120));
  } while (offset);
  return out;
}

async function applyPatches(proposals, { baseId, token, tableId, batchSize, log }) {
  let updatesApplied = 0;
  const writeErrors = [];
  const fieldsWritten = new Set();
  const fieldCounts = {
    hotel_url: 0,
    state: 0,
    address: 0,
    phone: 0,
    rooms: 0,
    coordinates: 0,
  };
  const size = Math.min(100, Math.max(1, batchSize || 100));

  for (let i = 0; i < proposals.length; i += size) {
    const chunk = proposals.slice(i, i + size);
    const updates = chunk
      .map((p) => {
        /** @type {Record<string, unknown>} */
        const fields = {};
        for (const [k, v] of Object.entries(p.patch || {})) {
          if (isForbiddenAutopilotField(k)) continue;
          if (!ALLOWED_WRITE.has(k)) continue;
          if (v === undefined || v === null || v === "") continue;
          // Require provenance for Level 2 core fields
          if (k === "Address" && !p.patch["Address Source URL"]) continue;
          if (k === "Rooms / Keys" && !p.patch["Rooms Source URL"]) continue;
          fields[k] = v;
          fieldsWritten.add(k);
          if (k === "Official Property URL") fieldCounts.hotel_url += 1;
          if (k === "State / Region") fieldCounts.state += 1;
          if (k === "Address") fieldCounts.address += 1;
          if (k === "Phone") fieldCounts.phone += 1;
          if (k === "Rooms / Keys") fieldCounts.rooms += 1;
          if (k === "Latitude" || k === "Longitude") fieldCounts.coordinates += 1;
        }
        return { id: p.record_id, fields };
      })
      .filter((u) => Object.keys(u.fields).length > 0);

    for (let j = 0; j < updates.length; j += 10) {
      const records = updates.slice(j, j + 10);
      if (!records.length) continue;
      try {
        const res = await fetch(
          `https://api.airtable.com/v0/${encodeURIComponent(baseId)}/${encodeURIComponent(tableId)}`,
          {
            method: "PATCH",
            headers: {
              Authorization: `Bearer ${token}`,
              "Content-Type": "application/json",
            },
            body: JSON.stringify({ records, typecast: true }),
          }
        );
        const json = await res.json().catch(() => ({}));
        if (!res.ok) writeErrors.push({ status: res.status, error: json.error || json });
        else updatesApplied += (json.records || []).length;
      } catch (err) {
        writeErrors.push({ error: err?.message || String(err) });
      }
      await new Promise((r) => setTimeout(r, 180));
    }
    log?.(
      `[marriott-webhound-learn] write batch ${Math.floor(i / size) + 1}: applied=${updatesApplied} errors=${writeErrors.length}`
    );
  }
  return { updatesApplied, writeErrors, fieldsWritten: [...fieldsWritten], fieldCounts };
}

function loadWebhoundText(opts = {}) {
  if (opts.webhoundReportText) return String(opts.webhoundReportText);
  const p =
    opts.webhoundReportPath ||
    path.join(
      ROOT,
      "reports/research-engine-v2/marriott-webhound-source-patterns-report.md"
    );
  if (fs.existsSync(p)) return fs.readFileSync(p, "utf8");
  return "";
}

function renderMd(report) {
  return [
    `# Marriott Webhound Source Pattern Learning v1`,
    ``,
    `**Status:** \`${report.status}\``,
    `**Objective:** \`${MARRIOTT_WEBHOUND_LEARNING_V1_OBJECTIVE}\``,
    `**Census mode:** \`${report.census_mode}\` (no inserts)`,
    `**Webhound as Census SoT:** false`,
    `**Write target:** Hotel Property Census (\`${CENSUS_TABLE_ID}\`)`,
    `**Airtable writes:** ${report.airtable_writes ? "yes" : "no"}`,
    `**Brand Setup / Brand Explorer writes:** false`,
    `**Founder gate between passes:** false`,
    ``,
    `## Discovery`,
    ``,
    `- Marriott records scanned: ${report.discovery?.marriott_records_scanned ?? 0}`,
    `- Webhound session: ${report.discovery?.webhound_session_id || "—"}`,
    `- Patterns discovered (parsed): ${report.discovery?.patterns_discovered ?? 0}`,
    `- Repeatable adapter candidates: ${report.discovery?.repeatable_patterns ?? 0}`,
    `- Rejected patterns: ${report.discovery?.rejected_patterns ?? 0}`,
    `- Bot-blocked extraction attempts: ${report.discovery?.bot_blocked_attempts ?? 0}`,
    ``,
    `## Adapter learning`,
    ``,
    `- Seed patterns: ${SEED_MARRIOTT_PATTERNS.length}`,
    `- Adapters in tree: marriott-official-metadata / factsheet / linked-site / rooms`,
    `- Records tested: ${report.adapter_learning?.records_tested ?? 0}`,
    `- Extraction success (any High candidate): ${report.adapter_learning?.extraction_success ?? 0}`,
    `- Success rate: ${report.adapter_learning?.success_rate_pct ?? 0}%`,
    ``,
    `## Writes`,
    ``,
    `- Records updated: ${report.records_updated ?? 0}`,
    `- Records inserted: ${report.records_inserted ?? 0}`,
    `- Hotel URL: ${report.field_counts?.hotel_url ?? 0}`,
    `- State/Region: ${report.field_counts?.state ?? 0}`,
    `- Address: ${report.field_counts?.address ?? 0}`,
    `- Phone: ${report.field_counts?.phone ?? 0}`,
    `- Rooms: ${report.field_counts?.rooms ?? 0}`,
    `- Coordinates: ${report.field_counts?.coordinates ?? 0}`,
    ``,
    `## Remaining blockers`,
    ``,
    ...Object.entries(report.remaining_blockers || {}).map(
      ([k, v]) => `- \`${k}\`: ${v}`
    ),
    ``,
    `## Chain`,
    ``,
    `- full-latam-census-autopilot-v3: ${report.chain_v3?.status || "—"}`,
    ``,
    `## Command to continue`,
    ``,
    "```bash",
    report.command_to_continue || "",
    "```",
    ``,
  ].join("\n");
}

export function writeMarriottWebhoundLearningReports(report) {
  const jsonPath = path.join(
    ROOT,
    "reports/research-engine-v2/production-census-marriott-webhound-source-pattern-learning-v1.json"
  );
  const mdPath = path.join(
    ROOT,
    "reports/research-engine-v2/production-census-marriott-webhound-source-pattern-learning-v1.md"
  );
  const docsPath = path.join(
    ROOT,
    "docs/data-intelligence/production-census-marriott-webhound-source-pattern-learning-v1.md"
  );
  const md = renderMd(report);
  writeJson(jsonPath, report);
  writeText(mdPath, md);
  writeText(docsPath, md);
  return { jsonPath, mdPath, docsPath };
}

/**
 * Mission entrypoint.
 */
export async function runMarriottWebhoundSourcePatternLearningV1Mission(opts = {}) {
  const argv = opts.argv || process.argv.slice(2);
  const args = opts.args || parseAutopilotArgs(argv);
  const env = opts.env || process.env;
  const log = opts.log || ((m) => console.log(m));
  const started = Date.now();
  const censusMode =
    resolveCensusMode(argv, { ...args, censusMode: opts.censusMode }) ||
    CENSUS_MODE.FIELD_COMPLETION_ONLY;
  // Force field-completion-only for this objective unless explicitly overridden to growth
  const effectiveMode =
    censusMode === CENSUS_MODE.GROWTH && opts.allowGrowth === true
      ? CENSUS_MODE.GROWTH
      : CENSUS_MODE.FIELD_COMPLETION_ONLY;

  const envCheck = checkAutopilotApplyEnv(env);
  const preflight = applyPreflight(args, envCheck);
  const enableWrites = Boolean(
    opts.enableProductionWrites &&
      argv.includes("--enable-production-writes") &&
      args.allApplyConfirms &&
      envCheck.allOk &&
      preflight.ok &&
      args.mode === "mission"
  );

  const writeTarget = assertProductionCensusWriteTarget({
    baseName: productionHotelPropertyCensus.baseName,
    tableName: productionHotelPropertyCensus.tableName,
    tableId: CENSUS_TABLE_ID,
  });
  if (!writeTarget.ok) {
    const blocked = {
      ok: false,
      status: MARRIOTT_WEBHOUND_LEARNING_STATUS.BLOCKED,
      objective: MARRIOTT_WEBHOUND_LEARNING_V1_OBJECTIVE,
      blocked_reason: writeTarget.reason,
      airtable_writes: false,
      webhound_as_census_sot: false,
    };
    writeMarriottWebhoundLearningReports(blocked);
    return blocked;
  }

  if (args.mode === "mission" && !preflight.ok) {
    const blocked = {
      ok: false,
      status: MARRIOTT_WEBHOUND_LEARNING_STATUS.BLOCKED,
      objective: MARRIOTT_WEBHOUND_LEARNING_V1_OBJECTIVE,
      blocked_reason: "confirmation_or_env",
      blockers: preflight.blockers,
      airtable_writes: false,
      webhound_as_census_sot: false,
    };
    writeMarriottWebhoundLearningReports(blocked);
    return blocked;
  }

  const token = opts.token ?? resolvePat();
  const bases = opts.bases ?? resolveTargetBase();
  if (!token || !bases?.target_base_id) {
    const blocked = {
      ok: false,
      status: MARRIOTT_WEBHOUND_LEARNING_STATUS.BLOCKED,
      objective: MARRIOTT_WEBHOUND_LEARNING_V1_OBJECTIVE,
      blocked_reason: "missing_airtable_credentials",
      airtable_writes: false,
    };
    writeMarriottWebhoundLearningReports(blocked);
    return blocked;
  }

  const region = args.region || "CALA";
  const runDir =
    opts.runDir ||
    path.join(
      ROOT,
      "reports/research-engine-v2/autopilot",
      `${new Date().toISOString().replace(/[:.]/g, "-").slice(0, 19)}_${region}-marriott-webhound-learning-v1`
    );
  fs.mkdirSync(runDir, { recursive: true });

  log(`[marriott-webhound-learn] SoT OK — Hotel Property Census ${CENSUS_TABLE_ID}`);
  log(
    `[marriott-webhound-learn] mode=${effectiveMode} webhound_as_sot=false writes=${enableWrites}`
  );

  // Guard: Webhound never SoT
  const sotGuard = assertWebhoundNotCensusSot({
    source: "webhound",
    webhound_as_sot: false,
    direct_write: false,
  });
  if (!sotGuard.ok) {
    const blocked = {
      ok: false,
      status: MARRIOTT_WEBHOUND_LEARNING_STATUS.BLOCKED,
      blocked_reason: sotGuard.reason,
      webhound_as_census_sot: false,
    };
    writeMarriottWebhoundLearningReports(blocked);
    return blocked;
  }

  let census = await listCensus(bases.target_base_id, token, CENSUS_TABLE_ID);
  const marriottRecords = census.filter((r) => isMarriottRecord(r.fields || {}));
  const activeIndex = buildActiveBrandIndex({ region });
  const dictionary = buildCanonicalBrandDictionary({ region });

  // Pass 1 — gap clusters + Webhound pattern catalog
  log(`[marriott-webhound-learn] Pass 1 — Marriott gap clusters + pattern catalog`);
  const clusters = buildMarriottGapClusterSamples(marriottRecords, { perCluster: 10 });
  writeJson(path.join(runDir, "marriott-gap-clusters.json"), clusters);

  const webhoundText = loadWebhoundText(opts);
  const discovered = webhoundText
    ? extractPatternsFromWebhoundText(webhoundText)
    : [];
  const catalog = buildMarriottPatternLearningCatalog(discovered, {
    webhoundSessionId:
      opts.webhoundSessionId || env.WEBHOUND_MARRIOTT_SESSION_ID || null,
    writePath: path.join(runDir, "marriott-pattern-catalog.json"),
  });
  writeJson(defaultCatalogPath(), catalog);

  const damIndex = buildDamFactsheetUrlIndex();
  writeDamFactsheetUrlIndex(damIndex);
  writeJson(path.join(runDir, "marriott-dam-factsheet-url-index.json"), damIndex);
  log(
    `[marriott-webhound-learn] DAM factsheet URL index size=${damIndex.count}`
  );

  // Pass 2 — deterministic metadata extraction on prioritized samples / missing Level 2
  log(`[marriott-webhound-learn] Pass 2 — official metadata adapter extraction`);
  const targets = marriottRecords.filter((r) => {
    const f = r.fields || {};
    const need =
      !f.Address ||
      !String(f.Address).trim() ||
      !f.Phone ||
      !f["Rooms / Keys"] ||
      f.Latitude == null;
    return need && (f["Official Property URL"] || f["Source URL"]);
  });
  // Prefer MARSHAs with known DAM factsheet URLs first (highest Level 2 yield)
  const damMarshas = new Set(Object.keys(damIndex.by_marsha || {}));
  targets.sort((a, b) => {
    const ma = extractMarshaCode(
      a.fields?.["Official Property URL"] || a.fields?.["Source URL"] || ""
    );
    const mb = extractMarshaCode(
      b.fields?.["Official Property URL"] || b.fields?.["Source URL"] || ""
    );
    const sa = damMarshas.has(String(ma || "").toUpperCase()) ? 0 : 1;
    const sb = damMarshas.has(String(mb || "").toUpperCase()) ? 0 : 1;
    return sa - sb;
  });
  const sampleLimit = Math.min(
    targets.length,
    Number(opts.extractLimit || env.MARRIOTT_LEARN_EXTRACT_LIMIT || 80)
  );
  // Always include Census records that match known DAM factsheet MARSHAs
  const damPriority = targets.filter((r) => {
    const m = extractMarshaCode(
      r.fields?.["Official Property URL"] || r.fields?.["Source URL"] || ""
    );
    return damMarshas.has(String(m || "").toUpperCase());
  });
  const rest = targets.filter((r) => !damPriority.includes(r));
  const sample = [...damPriority, ...rest].slice(0, sampleLimit);
  log(
    `[marriott-webhound-learn] sample=${sample.length} dam_priority=${damPriority.length} dam_index=${damMarshas.size}`
  );

  const proposals = [];
  let botBlocked = 0;
  let extractionSuccess = 0;
  const remaining = {
    akamai_blocked: 0,
    no_official_source_found: 0,
    insufficient_metadata: 0,
    rooms_source_missing: 0,
    phone_source_missing: 0,
    address_source_missing: 0,
    steward_review_required: 0,
  };

  for (let i = 0; i < sample.length; i += 1) {
    const rec = sample[i];
    if (i % 10 === 0) {
      log(`[marriott-webhound-learn] extract ${i + 1}/${sample.length}`);
    }
    try {
      const extraction = await extractMarriottOfficialMetadata(rec, {
        maxUrlAttempts: 3,
        includeHqv: true,
      });
      if (extraction.blocked) {
        botBlocked += 1;
        remaining.akamai_blocked += 1;
      }
      let built = buildMarriottMetadataPatch(extraction, rec.fields || {});

      // DAM factsheet fallback when overview/HQV insufficient (Akamai common)
      if (!built.has_writes) {
        try {
          const marsha =
            extractMarshaCode(
              rec.fields?.["Official Property URL"] ||
                rec.fields?.["Source URL"] ||
                ""
            ) || rec.fields?.["MARSHA Code"];
          const dam = await discoverAndExtractMarriottDamFactsheet(rec, {
            index: damIndex,
            marsha,
          });
          if (dam.ok) {
            const damBuilt = buildDamFactsheetCensusPatch(dam, rec.fields || {});
            if (damBuilt.has_writes) {
              built = {
                ...damBuilt,
                patch: {
                  ...damBuilt.patch,
                  "Enrichment Status":
                    "Marriott DAM factsheet — Autopilot (revalidated)",
                },
              };
            }
          }
        } catch (damErr) {
          log(
            `[marriott-webhound-learn] DAM extract error ${rec.id}: ${damErr?.message || damErr}`
          );
        }
      }

      if (built.has_writes) {
        // Extra rooms validation if rooms present
        if (built.patch["Rooms / Keys"] != null) {
          const v = validateMarriottRoomsCandidate({
            count: built.patch["Rooms / Keys"],
            source_url: built.patch["Rooms Source URL"],
            evidence: "official_marriott_dam_or_metadata",
            property_name: rec.fields?.["Property Name"],
            source_property_name: rec.fields?.["Property Name"],
          });
          if (!v.ok) {
            delete built.patch["Rooms / Keys"];
            delete built.patch["Rooms Confidence"];
            delete built.patch["Rooms Source URL"];
            delete built.patch["Rooms Source Type"];
            delete built.patch["Rooms Evidence Tier"];
            delete built.patch["Rooms Reviewed Date"];
            delete built.patch["Rooms Review Status"];
            remaining.rooms_source_missing += 1;
          }
        }
        if (Object.keys(built.patch).length) {
          built.patch["Last Reviewed Date"] = todayIsoDate();
          if (!built.patch["Enrichment Status"]) {
            built.patch["Enrichment Status"] =
              "Marriott official metadata — Autopilot";
          }
          proposals.push({
            record_id: rec.id,
            reason: built.patch["Rooms Source Type"]
              ? "marriott_dam_factsheet_high"
              : "marriott_official_metadata_high",
            confidence: "High",
            patch: built.patch,
            webhound_as_sot: false,
          });
          extractionSuccess += 1;
        } else {
          remaining.insufficient_metadata += 1;
        }
      } else {
        remaining.insufficient_metadata += 1;
        const f = rec.fields || {};
        if (!f.Address) remaining.address_source_missing += 1;
        if (!f.Phone) remaining.phone_source_missing += 1;
        if (!f["Rooms / Keys"]) remaining.rooms_source_missing += 1;
      }
    } catch (err) {
      remaining.steward_review_required += 1;
      log(`[marriott-webhound-learn] extract error ${rec.id}: ${err?.message || err}`);
    }
    await new Promise((r) => setTimeout(r, 120));
  }

  writeJson(path.join(runDir, "marriott-high-proposals.json"), {
    count: proposals.length,
    sample: proposals.slice(0, 30),
  });

  // Pass 3 — field-completion-only writes
  log(`[marriott-webhound-learn] Pass 3 — High field-completion writes (${proposals.length})`);
  let recordsUpdated = 0;
  let fieldCounts = {
    hotel_url: 0,
    state: 0,
    address: 0,
    phone: 0,
    rooms: 0,
    coordinates: 0,
  };
  const safetyStops = [];

  const insertGuard = assertNoInsertInFieldCompletionMode(effectiveMode, 0);
  if (!insertGuard.ok) safetyStops.push(insertGuard.reason);

  if (enableWrites && proposals.length && !safetyStops.length) {
    const applied = await applyPatches(proposals, {
      baseId: bases.target_base_id,
      token,
      tableId: CENSUS_TABLE_ID,
      batchSize: args.batchSize || 100,
      log,
    });
    recordsUpdated = applied.updatesApplied;
    fieldCounts = applied.fieldCounts;
    if (applied.writeErrors.length) {
      safetyStops.push(`write_errors:${applied.writeErrors.length}`);
    }
    census = await listCensus(bases.target_base_id, token, CENSUS_TABLE_ID);
  } else {
    log(
      `[marriott-webhound-learn] no write (enable=${enableWrites} proposals=${proposals.length})`
    );
  }

  // Pass 4 — re-audit + chain Autopilot v3 field-completion for Marriott
  log(`[marriott-webhound-learn] Pass 4 — gap ledger re-audit + chain Autopilot v3`);
  const marriottAfter = census.filter((r) => isMarriottRecord(r.fields || {}));
  const ledger = buildCensusGapLedger(marriottAfter, { activeIndex, dictionary });
  const scorecard = buildCompletionScorecard(marriottAfter, { activeIndex, dictionary });
  writeCensusGapLedger(ledger, scorecard, { runDir });

  let chainV3 = null;
  if (opts.skipChainV3 !== true) {
    try {
      const chainArgv = [];
      for (let i = 0; i < argv.length; i += 1) {
        const a = argv[i];
        if (a === "--objective") {
          i += 1;
          continue;
        }
        if (a === MARRIOTT_WEBHOUND_LEARNING_V1_OBJECTIVE) continue;
        if (a === "--max-passes") {
          i += 1;
          continue;
        }
        chainArgv.push(a);
      }
      chainArgv.push(
        "--objective",
        "full-latam-census-autopilot-v3",
        "--census-mode",
        "field-completion-only",
        "--parent-company",
        "Marriott International",
        "--max-passes",
        "2"
      );
      // Ensure enable flag preserved
      if (!chainArgv.includes("--enable-production-writes") && enableWrites) {
        chainArgv.push("--enable-production-writes");
      }
      const chainArgs = parseAutopilotArgs(chainArgv);
      chainArgs.maxPasses = 2;
      chainV3 = await runFullLatamCensusAutopilotV3Mission({
        argv: chainArgv,
        args: chainArgs,
        env,
        enableProductionWrites: enableWrites,
        censusMode: CENSUS_MODE.FIELD_COMPLETION_ONLY,
        maxPasses: 2,
        log,
        chainCala: true,
      });
      census = await listCensus(bases.target_base_id, token, CENSUS_TABLE_ID);
      const marriottFinal = census.filter((r) => isMarriottRecord(r.fields || {}));
      const ledger2 = buildCensusGapLedger(marriottFinal, { activeIndex, dictionary });
      const score2 = buildCompletionScorecard(marriottFinal, { activeIndex, dictionary });
      writeCensusGapLedger(ledger2, score2, { runDir });
    } catch (err) {
      safetyStops.push(`chain_v3_error:${err?.message || err}`);
      log(`[marriott-webhound-learn] chain v3 error: ${err?.message || err}`);
    }
  }

  const successRate =
    sample.length > 0 ? Math.round((1000 * extractionSuccess) / sample.length) / 10 : 0;

  let status = MARRIOTT_WEBHOUND_LEARNING_STATUS.PARTIAL_SOURCE;
  if (safetyStops.some((s) => /wrong_census|missing_airtable|confirmation|sot_forbidden/i.test(s))) {
    status = MARRIOTT_WEBHOUND_LEARNING_STATUS.BLOCKED;
  } else if (
    recordsUpdated > 0 &&
    remaining.insufficient_metadata < sample.length * 0.2 &&
    remaining.akamai_blocked < sample.length * 0.3
  ) {
    status = MARRIOTT_WEBHOUND_LEARNING_STATUS.COMPLETE;
  } else if (
    (catalog.repeatable_adapter_candidates || []).length > 0 &&
    recordsUpdated === 0 &&
    remaining.akamai_blocked > extractionSuccess
  ) {
    status = MARRIOTT_WEBHOUND_LEARNING_STATUS.PARTIAL_ADAPTER;
  } else {
    status = MARRIOTT_WEBHOUND_LEARNING_STATUS.PARTIAL_SOURCE;
  }

  const commandToContinue = [
    "ALLOW_CENSUS_AUTOPILOT_APPLY=1 \\",
    "CONFIRM_WRITE_TO_PRODUCTION_CENSUS=1 \\",
    "CONFIRM_NO_BRAND_EXPLORER_WRITES=1 \\",
    "CONFIRM_NO_OWNER_OPERATOR_WRITES=1 \\",
    'npm run census:autopilot -- --region CALA --scope official-parent-inventory --mode mission \\',
    "  --objective marriott-webhound-source-pattern-learning-v1 \\",
    "  --census-mode field-completion-only \\",
    '  --parent-company "Marriott International" \\',
    "  --strategy highest-yield-safe --run-until-complete --max-passes 8 --batch-size 100 \\",
    "  --confirm-safe-writes --confirm-write-to-production-census \\",
    "  --confirm-no-brand-explorer-writes --confirm-no-owner-operator \\",
    "  --confirm-no-date-writes --confirm-no-recent-momentum \\",
    "  --confirm-no-company-validation --confirm-webhound-not-production-source \\",
    "  --enable-production-writes",
  ].join("\n");

  const report = {
    ok: status !== MARRIOTT_WEBHOUND_LEARNING_STATUS.BLOCKED,
    status,
    version: MARRIOTT_WEBHOUND_LEARNING_V1_VERSION,
    objective: MARRIOTT_WEBHOUND_LEARNING_V1_OBJECTIVE,
    census_mode: effectiveMode,
    region,
    parent_company: args.parentCompany || "Marriott International",
    generated_at: new Date().toISOString(),
    elapsed_ms: Date.now() - started,
    write_target: {
      base: productionHotelPropertyCensus.baseName,
      table: productionHotelPropertyCensus.tableName,
      table_id: CENSUS_TABLE_ID,
    },
    airtable_writes: enableWrites && recordsUpdated > 0,
    brand_setup_writes: false,
    brand_explorer_writes: false,
    webhound_as_census_sot: false,
    founder_gate_between_passes: false,
    records_updated: recordsUpdated,
    records_inserted: 0,
    field_counts: fieldCounts,
    discovery: {
      marriott_records_scanned: marriottRecords.length,
      webhound_session_id: catalog.webhound_session_id,
      webhound_searches_run: webhoundText ? 1 : 0,
      patterns_discovered: discovered.length,
      repeatable_patterns: (catalog.repeatable_adapter_candidates || []).length,
      rejected_patterns: (catalog.rejected_patterns || []).length,
      bot_blocked_attempts: botBlocked,
      gap_clusters: clusters,
    },
    adapter_learning: {
      adapters_created_or_updated: [
        "marriott-source-pattern-discovery.js",
        "marriott-webhound-pattern-learner.js",
        "marriott-official-metadata-adapter.js",
        "marriott-factsheet-adapter.js",
        "marriott-linked-hotel-site-adapter.js",
        "marriott-rooms-source-adapter.js",
      ],
      records_tested: sample.length,
      extraction_success: extractionSuccess,
      success_rate_pct: successRate,
      proposals_prepared: proposals.length,
    },
    remaining_blockers: remaining,
    scorecard_marriott: scorecard?.percents || null,
    chain_v3: chainV3
      ? { status: chainV3.status, updates: chainV3.records_updated }
      : null,
    safety_stops: safetyStops,
    command_to_continue: commandToContinue,
    run_dir: runDir,
  };

  writeJson(path.join(runDir, "final-summary.json"), report);
  writeMarriottWebhoundLearningReports(report);
  log(
    `[marriott-webhound-learn] done status=${status} updated=${recordsUpdated} tested=${sample.length} success=${extractionSuccess} blocked=${botBlocked}`
  );
  return report;
}
