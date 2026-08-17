/**
 * Official Parent Level 2 Completion Mission v1.
 *
 * Separates governance/product approval review from data-quality review,
 * recomputes Clean Core, runs Level 2 (address/phone/rooms + coordinate chain)
 * for eligible Clean Core records including Census Only / evidence-backed
 * non-active brands, then chains cala-census-completion-v1 + source-confirmed-census-v2.
 *
 * Write target: Hotel Property Census only.
 * Brand Setup / Brand Explorer / VIC / old Census: never written.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

import { MAP_FIRST_PASS } from "./production-census-first-pass-enrichment.js";
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
import { buildCanonicalBrandDictionary } from "./census-brand-canonical-dictionary.js";
import { evaluateCleanCorePass } from "./census-map-contact-size-readiness.js";
import { isStreetLevelAddress } from "./production-census-geocoding-providers.js";
import {
  buildActiveBrandIndex,
  classifyBrandGovernanceStatus,
  classifyCensusReviewReasons,
  buildReviewReclassificationPatch,
  evaluateLevel2Eligibility,
  BRAND_GOVERNANCE_STATUS,
  CENSUS_ONLY_PRODUCTION_USE_STATUS,
  REVIEW_REASON,
} from "./census-brand-governance.js";
import {
  runLevel2SourceExtractionV1Mission,
  LEVEL_2_SOURCE_EXTRACTION_STATUS,
} from "./census-autopilot-level-2-source-extraction-v1.js";
import { runCalaCensusCompletionV1Mission } from "./census-autopilot-cala-census-completion-v1.js";
import { runSourceConfirmedCensusV2Mission } from "./census-autopilot-source-confirmed-census-v2.js";
import { snapshotMissionCensusMetrics } from "./census-autopilot-mission.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, "../..");

export const OFFICIAL_PARENT_LEVEL_2_COMPLETION_V1_OBJECTIVE =
  "official-parent-level-2-completion-v1";
export const OFFICIAL_PARENT_LEVEL_2_COMPLETION_V1_VERSION =
  "official-parent-level-2-completion-v1";

export const OFFICIAL_PARENT_LEVEL_2_STATUS = Object.freeze({
  COMPLETE: "production_census_official_parent_level_2_completion_v1_complete",
  PARTIAL:
    "production_census_official_parent_level_2_completion_v1_partial_source_remaining",
  BLOCKED:
    "production_census_official_parent_level_2_completion_v1_blocked_safety_stop",
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
  "Latitude",
  "Longitude",
  "Coordinate Source Type",
  "Coordinate Confidence",
  "Continent",
  "Sub-Continent",
  "Market",
  "Submarket",
  "Data Confidence Tier",
  "Enrichment Status",
  "Enrichment Priority",
  "Last Reviewed Date",
  "Production Use Status",
  "Public Display Review Status",
  "Radar Display Status",
  "Radar Display Reason",
];

const ALLOWED_PATCH_FIELDS = new Set([
  "Production Use Status",
  "Human Review Required",
  "Public Display Review Status",
  "Radar Display Status",
  "Radar Display Reason",
  "Data Confidence Tier",
  "Enrichment Status",
  "Enrichment Priority",
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

function isBlank(v) {
  return v == null || !String(v).trim();
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
  const size = Math.min(100, Math.max(1, batchSize || 100));
  for (let i = 0; i < proposals.length; i += size) {
    const chunk = proposals.slice(i, i + size);
    const updates = chunk
      .map((p) => {
        /** @type {Record<string, unknown>} */
        const fields = {};
        for (const [k, v] of Object.entries(p.patch || {})) {
          if (!ALLOWED_PATCH_FIELDS.has(k)) continue;
          // Allow boolean false (clear Human Review Required checkbox)
          if (v === undefined || v === null || v === "") continue;
          fields[k] = v;
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
        if (!res.ok) {
          writeErrors.push({ status: res.status, error: json.error || json });
        } else {
          updatesApplied += (json.records || []).length;
        }
      } catch (err) {
        writeErrors.push({ error: err?.message || String(err) });
      }
      await new Promise((r) => setTimeout(r, 180));
    }
    log?.(
      `[official-parent-l2] reclassify batch ${Math.floor(i / size) + 1}: written=${updatesApplied} errors=${writeErrors.length}`
    );
  }
  return { updatesApplied, writeErrors };
}

function countGovernance(records, activeIndex) {
  const counts = {
    [BRAND_GOVERNANCE_STATUS.ACTIVE_BRAND_SETUP]: 0,
    [BRAND_GOVERNANCE_STATUS.EVIDENCE_BACKED_NON_ACTIVE]: 0,
    [BRAND_GOVERNANCE_STATUS.PROMOTION_CANDIDATE]: 0,
    [BRAND_GOVERNANCE_STATUS.DIRTY_PARTNER_LABEL]: 0,
    [BRAND_GOVERNANCE_STATUS.BRAND_CODE_UNRESOLVED]: 0,
    [BRAND_GOVERNANCE_STATUS.UNSUPPORTED_OR_AMBIGUOUS]: 0,
  };
  for (const r of records) {
    const g = classifyBrandGovernanceStatus({ fields: r.fields || {} }, { activeIndex });
    counts[g.status] = (counts[g.status] || 0) + 1;
  }
  return counts;
}

function snapshotReviewAndLevel2(records, opts = {}) {
  const activeIndex = opts.activeIndex || buildActiveBrandIndex(opts);
  const dictionary = opts.dictionary || buildCanonicalBrandDictionary(opts);
  let cleanCore = 0;
  let governanceReview = 0;
  let dataQualityReview = 0;
  let hr = 0;
  let publicHold = 0;
  let radarHold = 0;
  let censusOnly = 0;
  let address = 0;
  let addressHigh = 0;
  let addressUrl = 0;
  let latLong = 0;
  let phone = 0;
  let rooms = 0;
  let level2Eligible = 0;
  let needsGovernanceApproval = 0;
  let needsDataSteward = 0;
  let mapReady = 0;
  let contactReady = 0;
  let sizeReady = 0;
  let censusComplete = 0;
  let completeCensusV1 = 0;

  for (const r of records) {
    const f = r.fields || {};
    const review = classifyCensusReviewReasons({ fields: f }, { activeIndex });
    const clean = evaluateCleanCorePass(r, { dictionary, activeIndex });
    if (clean.pass) cleanCore += 1;
    if (review.governance_review_required || review.public_approval_required) {
      governanceReview += 1;
      needsGovernanceApproval += 1;
    }
    if (review.data_quality_review_required || review.steward_review_required) {
      dataQualityReview += 1;
      needsDataSteward += 1;
    }
    if (f[MAP_FIRST_PASS.humanReview] === true) hr += 1;
    if (String(f["Public Display Review Status"] || "") === "Hold") publicHold += 1;
    if (String(f["Radar Display Status"] || "") === "Hold") radarHold += 1;
    if (String(f["Production Use Status"] || "") === CENSUS_ONLY_PRODUCTION_USE_STATUS) {
      censusOnly += 1;
    }

    const addrOk = isStreetLevelAddress(f.Address || "");
    const phoneOk = !isBlank(f.Phone);
    const roomsOk = !isBlank(f["Rooms / Keys"]);
    const coordsOk = f.Latitude != null && f.Longitude != null;
    if (addrOk) address += 1;
    if (String(f["Address Confidence"] || "").toLowerCase() === "high") addressHigh += 1;
    if (!isBlank(f["Address Source URL"])) addressUrl += 1;
    if (coordsOk) latLong += 1;
    if (phoneOk) phone += 1;
    if (roomsOk) rooms += 1;

    const elig = evaluateLevel2Eligibility(r, {
      activeIndex,
      cleanCoreResult: clean,
      allowAutofillableCleanCoreGaps: true,
    });
    if (elig.eligible || elig.clean_core) level2Eligible += 1;

    if (addrOk && coordsOk) mapReady += 1;
    if (phoneOk) contactReady += 1;
    if (roomsOk) sizeReady += 1;
    if (clean.pass && addrOk) censusComplete += 1;
    if (clean.pass && addrOk && coordsOk && phoneOk && roomsOk) completeCensusV1 += 1;
  }

  return {
    total: records.length,
    clean_core: cleanCore,
    governance: countGovernance(records, activeIndex),
    governance_review_required: governanceReview,
    data_quality_review_required: dataQualityReview,
    human_review_required: hr,
    public_hold: publicHold,
    radar_hold: radarHold,
    census_only: censusOnly,
    level2_eligible: level2Eligible,
    address_complete: address,
    address_confidence_high: addressHigh,
    address_source_url_complete: addressUrl,
    lat_long_complete: latLong,
    phone_complete: phone,
    rooms_complete: rooms,
    map_ready: mapReady,
    contact_ready: contactReady,
    size_ready: sizeReady,
    census_complete: censusComplete,
    complete_census_v1: completeCensusV1,
    needs_governance_approval: needsGovernanceApproval,
    needs_data_steward_review: needsDataSteward,
    mission_metrics: snapshotMissionCensusMetrics(records, opts),
  };
}

function renderMd(report) {
  const b = report.before || {};
  const a = report.after || {};
  const bg = b.governance || {};
  const ag = a.governance || {};
  return [
    `# Official Parent Level 2 Completion v1`,
    ``,
    `**Status:** \`${report.status}\``,
    `**Objective:** \`${OFFICIAL_PARENT_LEVEL_2_COMPLETION_V1_OBJECTIVE}\``,
    `**Write target:** Hotel Property Census (\`${CENSUS_TABLE_ID}\`)`,
    `**Airtable writes:** ${report.airtable_writes ? "yes" : "no"}`,
    `**Brand Setup writes:** false`,
    `**Brand Explorer writes:** false`,
    ``,
    `## Review classification`,
    ``,
    `- Governance review = product/public approval (Census Only / Hold / non-active brand)`,
    `- Data quality review = dirty labels, unresolved codes, unsupported, conflicts, duplicates`,
    `- Governance-only review does **not** block Level 2 enrichment`,
    ``,
    `## Before / After`,
    ``,
    `| Metric | Before | After |`,
    `| --- | ---: | ---: |`,
    `| Total records | ${b.total ?? "—"} | ${a.total ?? "—"} |`,
    `| Clean Core | ${b.clean_core ?? "—"} | ${a.clean_core ?? "—"} |`,
    `| active_brand_setup | ${bg.active_brand_setup ?? "—"} | ${ag.active_brand_setup ?? "—"} |`,
    `| evidence_backed_non_active_brand | ${bg.evidence_backed_non_active_brand ?? "—"} | ${ag.evidence_backed_non_active_brand ?? "—"} |`,
    `| Governance Review Required | ${b.governance_review_required ?? "—"} | ${a.governance_review_required ?? "—"} |`,
    `| Data Quality Review Required | ${b.data_quality_review_required ?? "—"} | ${a.data_quality_review_required ?? "—"} |`,
    `| Human Review Required (checkbox) | ${b.human_review_required ?? "—"} | ${a.human_review_required ?? "—"} |`,
    `| Public Hold | ${b.public_hold ?? "—"} | ${a.public_hold ?? "—"} |`,
    `| Radar Hold | ${b.radar_hold ?? "—"} | ${a.radar_hold ?? "—"} |`,
    `| Level 2 eligible | ${b.level2_eligible ?? "—"} | ${a.level2_eligible ?? "—"} |`,
    `| Address complete | ${b.address_complete ?? "—"} | ${a.address_complete ?? "—"} |`,
    `| Address Confidence High | ${b.address_confidence_high ?? "—"} | ${a.address_confidence_high ?? "—"} |`,
    `| Address Source URL | ${b.address_source_url_complete ?? "—"} | ${a.address_source_url_complete ?? "—"} |`,
    `| Lat/Long complete | ${b.lat_long_complete ?? "—"} | ${a.lat_long_complete ?? "—"} |`,
    `| Phone complete | ${b.phone_complete ?? "—"} | ${a.phone_complete ?? "—"} |`,
    `| Rooms complete | ${b.rooms_complete ?? "—"} | ${a.rooms_complete ?? "—"} |`,
    `| Complete Census v1 | ${b.complete_census_v1 ?? "—"} | ${a.complete_census_v1 ?? "—"} |`,
    `| Census Complete | ${b.census_complete ?? "—"} | ${a.census_complete ?? "—"} |`,
    `| Map Ready | ${b.map_ready ?? "—"} | ${a.map_ready ?? "—"} |`,
    `| Contact Ready | ${b.contact_ready ?? "—"} | ${a.contact_ready ?? "—"} |`,
    `| Size Ready | ${b.size_ready ?? "—"} | ${a.size_ready ?? "—"} |`,
    `| Needs Governance Approval | ${b.needs_governance_approval ?? "—"} | ${a.needs_governance_approval ?? "—"} |`,
    `| Needs Data Steward Review | ${b.needs_data_steward_review ?? "—"} | ${a.needs_data_steward_review ?? "—"} |`,
    ``,
    `## Mission writes`,
    ``,
    `- Reclassification patches: ${report.reclassify_updates ?? 0}`,
    `- Level 2 updates: ${report.level2_updates ?? 0}`,
    `- Records updated (approx): ${report.records_updated ?? 0}`,
    `- Bot-blocked: ${report.bot_blocked_count ?? 0}`,
    `- Source-insufficient: ${report.source_insufficient_count ?? 0}`,
    `- Safety stops: ${(report.safety_stops || []).join("; ") || "none"}`,
    ``,
    `## Chains`,
    ``,
    `- cala-census-completion-v1: ${report.chain_cala?.status || "—"}`,
    `- source-confirmed-census-v2: ${report.chain_source_confirmed?.status || "—"}`,
    ``,
    `## Hard constraints`,
    ``,
    `- Brand Setup / Brand Explorer untouched`,
    `- Non-active brands remain Public/Radar Hold`,
    `- No weak address / unofficial phone / room inference`,
    `- No Mapbox on dirty identity`,
    ``,
  ].join("\n");
}

export function writeOfficialParentLevel2Reports(report) {
  const jsonPath = path.join(
    ROOT,
    "reports/research-engine-v2/production-census-official-parent-level-2-completion-v1.json"
  );
  const mdPath = path.join(
    ROOT,
    "reports/research-engine-v2/production-census-official-parent-level-2-completion-v1.md"
  );
  const docsPath = path.join(
    ROOT,
    "docs/data-intelligence/production-census-official-parent-level-2-completion-v1.md"
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
export async function runOfficialParentLevel2CompletionV1Mission(opts = {}) {
  const argv = opts.argv || process.argv.slice(2);
  const args = opts.args || parseAutopilotArgs(argv);
  const env = opts.env || process.env;
  const log = opts.log || ((m) => console.log(m));
  const started = Date.now();
  const chainCala = opts.chainCala !== false;
  const chainSourceConfirmed = opts.chainSourceConfirmed !== false;

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
      status: OFFICIAL_PARENT_LEVEL_2_STATUS.BLOCKED,
      objective: OFFICIAL_PARENT_LEVEL_2_COMPLETION_V1_OBJECTIVE,
      blocked_reason: writeTarget.reason || "wrong_census_target",
      airtable_writes: false,
      brand_setup_writes: false,
      brand_explorer_writes: false,
      safety_stops: ["wrong_census_target"],
    };
    writeOfficialParentLevel2Reports(blocked);
    return blocked;
  }

  if (args.mode === "mission" && !preflight.ok) {
    const blocked = {
      ok: false,
      status: OFFICIAL_PARENT_LEVEL_2_STATUS.BLOCKED,
      objective: OFFICIAL_PARENT_LEVEL_2_COMPLETION_V1_OBJECTIVE,
      blocked_reason: "confirmation_or_env",
      blockers: preflight.blockers,
      airtable_writes: false,
      safety_stops: preflight.blockers || ["confirmation_or_env"],
    };
    writeOfficialParentLevel2Reports(blocked);
    return blocked;
  }

  const token = opts.token ?? resolvePat();
  const bases = opts.bases ?? resolveTargetBase();
  if (!token || !bases?.target_base_id) {
    const blocked = {
      ok: false,
      status: OFFICIAL_PARENT_LEVEL_2_STATUS.BLOCKED,
      objective: OFFICIAL_PARENT_LEVEL_2_COMPLETION_V1_OBJECTIVE,
      blocked_reason: "missing_airtable_credentials",
      airtable_writes: false,
      safety_stops: ["missing_airtable_credentials"],
    };
    writeOfficialParentLevel2Reports(blocked);
    return blocked;
  }

  const region = args.region || "CALA";
  const runDir =
    opts.runDir ||
    path.join(
      ROOT,
      "reports/research-engine-v2/autopilot",
      `${new Date().toISOString().replace(/[:.]/g, "-").slice(0, 19)}_${region}-official-parent-level-2-completion-v1`
    );
  fs.mkdirSync(runDir, { recursive: true });

  log(`[official-parent-l2] Phase 1 SoT OK — Hotel Property Census ${CENSUS_TABLE_ID}`);
  log(`[official-parent-l2] listing Census…`);
  let census = await listCensus(bases.target_base_id, token, CENSUS_TABLE_ID);
  const activeIndex = buildActiveBrandIndex({ region });
  const dictionary = buildCanonicalBrandDictionary({ region });
  const before = snapshotReviewAndLevel2(census, { activeIndex, dictionary, env });
  writeJson(path.join(runDir, "before-snapshot.json"), before);
  log(
    `[official-parent-l2] before clean_core=${before.clean_core} gov_review=${before.governance_review_required} dq_review=${before.data_quality_review_required} hr=${before.human_review_required} l2_elig=${before.level2_eligible}`
  );

  // Phase 2 — reclassification patches
  log(`[official-parent-l2] Phase 2 governance vs data-quality reclassification…`);
  const reclassProposals = [];
  for (const rec of census) {
    const built = buildReviewReclassificationPatch(rec, { activeIndex });
    if (built.patch && Object.keys(built.patch).length) {
      reclassProposals.push(built);
    }
  }
  writeJson(path.join(runDir, "reclassify-proposals.json"), {
    count: reclassProposals.length,
    sample: reclassProposals.slice(0, 50),
  });

  let reclassifyUpdates = 0;
  const safetyStops = [];
  if (enableWrites && reclassProposals.length) {
    const applied = await applyPatches(reclassProposals, {
      baseId: bases.target_base_id,
      token,
      tableId: CENSUS_TABLE_ID,
      batchSize: args.batchSize || 100,
      log,
    });
    reclassifyUpdates = applied.updatesApplied;
    if (applied.writeErrors.length) {
      safetyStops.push(`reclassify_errors:${applied.writeErrors.length}`);
    }
    census = await listCensus(bases.target_base_id, token, CENSUS_TABLE_ID);
  } else {
    log(`[official-parent-l2] reclassify prepared=${reclassProposals.length} (no write)`);
  }

  // Phase 3 — Clean Core snapshot after reclass
  const mid = snapshotReviewAndLevel2(census, { activeIndex, dictionary, env });
  log(
    `[official-parent-l2] Phase 3 clean_core ${before.clean_core}→${mid.clean_core} hr ${before.human_review_required}→${mid.human_review_required}`
  );

  // Phase 4–7 — Level 2 extraction (+ cala chain inside L2 for coords)
  log(`[official-parent-l2] Phase 4–7 Level 2 source extraction…`);
  const level2 = await runLevel2SourceExtractionV1Mission({
    argv,
    args: { ...args, objective: "level-2-source-extraction-v1" },
    env,
    enableProductionWrites: enableWrites,
    token,
    bases,
    log,
    // Skip nested cala — we chain explicitly below for reporting control
    skipCalaChain: true,
  });

  // Re-list after L2
  census = await listCensus(bases.target_base_id, token, CENSUS_TABLE_ID);

  let chainCalaReport = null;
  if (chainCala) {
    log(`[official-parent-l2] Phase 8 chain cala-census-completion-v1…`);
    try {
      chainCalaReport = await runCalaCensusCompletionV1Mission({
        argv,
        args: { ...args, objective: "cala-census-completion-v1" },
        env,
        enableProductionWrites: enableWrites,
        log,
      });
    } catch (err) {
      safetyStops.push(`cala_chain_error:${err?.message || err}`);
    }
    census = await listCensus(bases.target_base_id, token, CENSUS_TABLE_ID);
  }

  let chainScReport = null;
  if (chainSourceConfirmed) {
    log(`[official-parent-l2] Phase 8 chain source-confirmed-census-v2…`);
    try {
      chainScReport = await runSourceConfirmedCensusV2Mission({
        argv,
        args: { ...args, objective: "source-confirmed-census-v2" },
        env,
        enableProductionWrites: enableWrites,
        log,
      });
    } catch (err) {
      safetyStops.push(`source_confirmed_chain_error:${err?.message || err}`);
    }
    census = await listCensus(bases.target_base_id, token, CENSUS_TABLE_ID);
  }

  const after = snapshotReviewAndLevel2(census, { activeIndex, dictionary, env });
  const level2Updates = level2?.updates_applied || 0;
  const remainingL2 =
    (after.address_complete < after.clean_core ? 1 : 0) +
    (after.phone_complete < after.clean_core ? 1 : 0) +
    (after.rooms_complete < after.clean_core ? 1 : 0);

  let status = OFFICIAL_PARENT_LEVEL_2_STATUS.COMPLETE;
  if (safetyStops.some((s) => /wrong_census|missing_airtable|confirmation/.test(s))) {
    status = OFFICIAL_PARENT_LEVEL_2_STATUS.BLOCKED;
  } else if (
    remainingL2 > 0 ||
    after.data_quality_review_required > 0 ||
    level2?.status === LEVEL_2_SOURCE_EXTRACTION_STATUS.PARTIAL ||
    safetyStops.length
  ) {
    status = OFFICIAL_PARENT_LEVEL_2_STATUS.PARTIAL;
  }

  const report = {
    ok: status !== OFFICIAL_PARENT_LEVEL_2_STATUS.BLOCKED,
    status,
    version: OFFICIAL_PARENT_LEVEL_2_COMPLETION_V1_VERSION,
    objective: OFFICIAL_PARENT_LEVEL_2_COMPLETION_V1_OBJECTIVE,
    scope: "official-parent-inventory",
    region,
    mode: args.mode || "mission",
    generated_at: new Date().toISOString(),
    elapsed_ms: Date.now() - started,
    write_target: {
      base: productionHotelPropertyCensus.baseName,
      table: productionHotelPropertyCensus.tableName,
      table_id: CENSUS_TABLE_ID,
    },
    airtable_writes: enableWrites && (reclassifyUpdates > 0 || level2Updates > 0),
    brand_setup_writes: false,
    brand_explorer_writes: false,
    before,
    after,
    mid_after_reclassify: mid,
    reclassify_updates: reclassifyUpdates,
    reclassify_proposals: reclassProposals.length,
    level2_updates: level2Updates,
    level2_status: level2?.status || null,
    records_updated: reclassifyUpdates + level2Updates,
    bot_blocked_count: level2?.counters?.fetch_blocked || level2?.bot_blocked_count || 0,
    source_insufficient_count:
      level2?.counters?.steward_conflicts || level2?.source_insufficient_count || 0,
    safety_stops: safetyStops,
    review_reason_values: Object.values(REVIEW_REASON),
    chain_cala: chainCalaReport
      ? { status: chainCalaReport.status, updates: chainCalaReport.updates_applied }
      : null,
    chain_source_confirmed: chainScReport
      ? { status: chainScReport.status, updates: chainScReport.updates_applied }
      : null,
    run_dir: runDir,
    next_recommended_action:
      status === OFFICIAL_PARENT_LEVEL_2_STATUS.COMPLETE
        ? "Review promotion pack / governance approval for owner-facing"
        : "Continue source adapters for remaining Level 2 gaps; steward data-quality queue",
  };

  writeJson(path.join(runDir, "final-summary.json"), report);
  writeOfficialParentLevel2Reports(report);
  log(
    `[official-parent-l2] done status=${status} reclass=${reclassifyUpdates} l2=${level2Updates} clean_core ${before.clean_core}→${after.clean_core} hr ${before.human_review_required}→${after.human_review_required}`
  );
  return report;
}
