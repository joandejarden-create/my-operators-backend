/**
 * CALA Census Completion Mission v1.
 *
 * Broader Autopilot mission: park dirty partner brand labels, reconfirm brand/core,
 * classify Clean Core, complete geography + Level 2 (address/coords/phone/rooms)
 * for eligible existing Hotel Property Census records only.
 *
 * Brand Setup / Brand Explorer remain read-only. Promotion pack is report-only.
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
import {
  classifyDirtyPartnerLabel,
} from "./census-autopilot-brand-registry-resolution-v1.js";
import { isCensusOfficialBrand } from "./census-official-brand-registry.js";
import {
  runCleanCensusV1Mission,
  MISSION_OBJECTIVE_CALA_CENSUS_COMPLETION_V1,
  CALA_CENSUS_COMPLETION_STATUS,
  snapshotMissionCensusMetrics,
  writeMissionPublicReports,
} from "./census-autopilot-mission.js";

export { CALA_CENSUS_COMPLETION_STATUS };

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, "../..");

export const CALA_CENSUS_COMPLETION_V1_OBJECTIVE = MISSION_OBJECTIVE_CALA_CENSUS_COMPLETION_V1;
export const CALA_CENSUS_COMPLETION_V1_VERSION = "cala-census-completion-v1";

const CENSUS_TABLE_ID =
  TABLE_IDS["Hotel Property Census"] || productionHotelPropertyCensus.tableId;

const PROMOTION_PACK_JSON = path.join(
  ROOT,
  "reports/research-engine-v2/production-census-brand-setup-promotion-candidates.json"
);

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
  "Public Display Review Status",
  "Radar Display Status",
  "Radar Display Reason",
  "Data Confidence Tier",
  "Enrichment Status",
  "Enrichment Priority",
  "Last Reviewed Date",
  "Continent",
  "Sub-Continent",
  "Market",
  "Submarket",
  "Latitude",
  "Longitude",
  "Phone",
  "Rooms / Keys",
  "Production Use Status",
];

function todayIsoDate() {
  return new Date().toISOString().slice(0, 10);
}

function writeJson(fp, data) {
  fs.mkdirSync(path.dirname(fp), { recursive: true });
  fs.writeFileSync(fp, JSON.stringify(data, null, 2), "utf8");
}

function writeText(fp, text) {
  fs.mkdirSync(path.dirname(fp), { recursive: true });
  fs.writeFileSync(fp, text, "utf8");
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
    if (!res.ok) throw new Error(`census list ${res.status}: ${JSON.stringify(json.error || json)}`);
    out.push(...(json.records || []));
    offset = json.offset;
    await new Promise((r) => setTimeout(r, 120));
  } while (offset);
  return out;
}

/**
 * Brand / dirty-partner inventory metrics.
 * @param {object[]} censusRecords
 */
export function summarizeBrandInventory(censusRecords = []) {
  let unknown = 0;
  let dirty = 0;
  let humanReview = 0;
  const dirtyExamples = [];
  for (const rec of censusRecords) {
    const f = rec.fields || {};
    const brand = String(f[MAP_FIRST_PASS.currentBrand] || "").trim();
    const name = String(f[MAP_FIRST_PASS.propertyName] || "");
    const url = String(f[MAP_FIRST_PASS.officialUrl] || f[MAP_FIRST_PASS.sourceUrl] || "");
    if (f[MAP_FIRST_PASS.humanReview] === true) humanReview += 1;
    const d = classifyDirtyPartnerLabel(brand, name, url);
    if (d.dirty) {
      dirty += 1;
      if (dirtyExamples.length < 20) {
        dirtyExamples.push({
          record_id: rec.id,
          brand,
          reason: d.reason,
          property_name: name,
        });
      }
      continue;
    }
    if (brand && !isCensusOfficialBrand(brand)) unknown += 1;
  }
  return {
    unknown_brands: unknown,
    dirty_partner_labels: dirty,
    human_review: humanReview,
    dirty_partner_examples: dirtyExamples,
  };
}

/**
 * Load read-only Brand Setup promotion decision pack (never writes Brand Setup).
 */
export function loadBrandSetupPromotionPack() {
  try {
    if (!fs.existsSync(PROMOTION_PACK_JSON)) {
      return {
        brand_setup_writes: false,
        brand_explorer_writes: false,
        candidates: [],
        note: "promotion pack file missing — run brand-registry-resolution-v1 first",
      };
    }
    const raw = JSON.parse(fs.readFileSync(PROMOTION_PACK_JSON, "utf8"));
    return {
      brand_setup_writes: false,
      brand_explorer_writes: false,
      candidates: raw.candidates || [],
      source: "reports/research-engine-v2/production-census-brand-setup-promotion-candidates.json",
    };
  } catch (err) {
    return {
      brand_setup_writes: false,
      brand_explorer_writes: false,
      candidates: [],
      error: err?.message || String(err),
    };
  }
}

/**
 * Ensure dirty partner labels stay stewarded (HR + Radar Hold). Idempotent.
 * @param {object[]} censusRecords
 * @param {{ enableWrites?: boolean, baseId?: string, token?: string, tableId?: string, log?: Function }} opts
 */
export async function parkDirtyPartnerLabels(censusRecords = [], opts = {}) {
  const log = opts.log || (() => {});
  const proposals = [];
  for (const rec of censusRecords) {
    const f = rec.fields || {};
    const brand = String(f[MAP_FIRST_PASS.currentBrand] || "").trim();
    const name = String(f[MAP_FIRST_PASS.propertyName] || "");
    const url = String(f[MAP_FIRST_PASS.officialUrl] || f[MAP_FIRST_PASS.sourceUrl] || "");
    const dirty = classifyDirtyPartnerLabel(brand, name, url);
    if (!dirty.dirty) continue;
    const hr = f[MAP_FIRST_PASS.humanReview] === true;
    const radarHold = String(f[MAP_FIRST_PASS.radarDisplayStatus] || "") === "Hold";
    const reasonOk = String(f[MAP_FIRST_PASS.radarDisplayReason] || "").includes(dirty.reason);
    if (hr && radarHold && reasonOk) continue;
    proposals.push({
      record_id: rec.id,
      brand,
      reason: dirty.reason,
      patch: {
        [MAP_FIRST_PASS.humanReview]: true,
        "Enrichment Priority": "High",
        "Last Reviewed Date": todayIsoDate(),
        [MAP_FIRST_PASS.publicDisplayReviewStatus]: "Needs Review",
        [MAP_FIRST_PASS.radarDisplayStatus]: "Hold",
        [MAP_FIRST_PASS.radarDisplayReason]: `Brand registry: ${dirty.reason}`,
      },
    });
  }

  let updatesApplied = 0;
  const writeErrors = [];
  if (opts.enableWrites && proposals.length && opts.baseId && opts.token) {
    const tableId = opts.tableId || CENSUS_TABLE_ID;
    log(`[cala-completion] parking ${proposals.length} dirty partner labels…`);
    for (let i = 0; i < proposals.length; i += 10) {
      const chunk = proposals.slice(i, i + 10).map((p) => ({
        id: p.record_id,
        fields: p.patch,
      }));
      try {
        const res = await fetch(
          `https://api.airtable.com/v0/${encodeURIComponent(opts.baseId)}/${encodeURIComponent(tableId)}`,
          {
            method: "PATCH",
            headers: {
              Authorization: `Bearer ${opts.token}`,
              "Content-Type": "application/json",
            },
            body: JSON.stringify({ records: chunk, typecast: true }),
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
  } else {
    log(
      `[cala-completion] dirty-partner park proposals=${proposals.length} (writes=${Boolean(opts.enableWrites)})`
    );
  }

  return {
    proposals_planned: proposals.length,
    updates_applied: updatesApplied,
    write_errors: writeErrors,
    classification: "dirty_partner_label",
    excluded_from_clean_core: true,
    steward_review_required: true,
    brand_setup_writes: false,
  };
}

function enrichMetrics(baseMetrics, brandInv) {
  return {
    ...(baseMetrics || {}),
    unknown_brands: brandInv.unknown_brands,
    dirty_partner_labels: brandInv.dirty_partner_labels,
    human_review: brandInv.human_review,
    excluded_from_clean_core:
      (baseMetrics?.total_records || 0) - (baseMetrics?.clean_core || 0),
  };
}

function renderCalaCompletionMd(report) {
  const b = report.before || {};
  const a = report.after || {};
  const promo = report.brand_setup_promotion_pack?.candidates || [];
  const lines = [
    `# CALA Census Completion Mission v1`,
    ``,
    `**Status:** \`${report.status}\``,
    `**Objective:** \`${CALA_CENSUS_COMPLETION_V1_OBJECTIVE}\``,
    `**Write target:** Hotel Property Census (\`${report.write_target?.table_id || CENSUS_TABLE_ID}\`)`,
    `**Airtable writes:** ${report.airtable_writes ? "yes" : "no"}`,
    `**Brand Setup writes:** false`,
    `**Brand Explorer writes:** false`,
    `**Inserts:** ${report.inserts_applied ?? 0}`,
    `**Updates:** ${report.updates_applied ?? 0}`,
    `**Runtime ms:** ${report.runtime_ms ?? "—"}`,
    ``,
    `## Inventory`,
    ``,
    `| Metric | Before | After |`,
    `| --- | ---: | ---: |`,
    `| Total records | ${b.total_records ?? "—"} | ${a.total_records ?? "—"} |`,
    `| Clean Core | ${b.clean_core ?? "—"} | ${a.clean_core ?? "—"} |`,
    `| Excluded from Clean Core | ${b.excluded_from_clean_core ?? "—"} | ${a.excluded_from_clean_core ?? "—"} |`,
    `| Unknown brands | ${b.unknown_brands ?? "—"} | ${a.unknown_brands ?? "—"} |`,
    `| Dirty partner labels | ${b.dirty_partner_labels ?? "—"} | ${a.dirty_partner_labels ?? "—"} |`,
    `| Human Review | ${b.human_review ?? "—"} | ${a.human_review ?? "—"} |`,
    `| Unknown City | ${b.unknown_city ?? "—"} | ${a.unknown_city ?? "—"} |`,
    `| Canonical blank | ${b.canonical_blank ?? "—"} | ${a.canonical_blank ?? "—"} |`,
    `| State / Region complete | ${b.state_region_complete ?? "—"} | ${a.state_region_complete ?? "—"} |`,
    ``,
    `## Geography`,
    ``,
    `| Metric | Before | After |`,
    `| --- | ---: | ---: |`,
    `| Continent complete | ${b.continent_complete ?? "—"} | ${a.continent_complete ?? "—"} |`,
    `| Sub-Continent complete | ${b.subcontinent_complete ?? "—"} | ${a.subcontinent_complete ?? "—"} |`,
    `| Market complete | ${b.market_complete ?? "—"} | ${a.market_complete ?? "—"} |`,
    `| Submarket complete | ${b.submarket_complete ?? "—"} | ${a.submarket_complete ?? "—"} |`,
    ``,
    `## Level 2`,
    ``,
    `| Metric | Before | After |`,
    `| --- | ---: | ---: |`,
    `| Address complete | ${b.address_complete ?? "—"} | ${a.address_complete ?? "—"} |`,
    `| Address Confidence High | ${b.address_confidence_high ?? "—"} | ${a.address_confidence_high ?? "—"} |`,
    `| Address Source URL complete | ${b.address_source_url_complete ?? "—"} | ${a.address_source_url_complete ?? "—"} |`,
    `| Lat/Long complete | ${b.lat_long_complete ?? "—"} | ${a.lat_long_complete ?? "—"} |`,
    `| Mapbox eligible | ${b.mapbox_eligible ?? "—"} | ${a.mapbox_eligible ?? "—"} |`,
    `| Est. Mapbox requests | ${b.estimated_mapbox_requests ?? "—"} | ${a.estimated_mapbox_requests ?? "—"} |`,
    `| Phone complete | ${b.phone_complete ?? "—"} | ${a.phone_complete ?? "—"} |`,
    `| Rooms complete | ${b.rooms_complete ?? "—"} | ${a.rooms_complete ?? "—"} |`,
    ``,
    `## Readiness`,
    ``,
    `| Metric | Before | After |`,
    `| --- | ---: | ---: |`,
    `| Map Ready | ${b.map_ready ?? "—"} | ${a.map_ready ?? "—"} |`,
    `| Contact Ready | ${b.contact_ready ?? "—"} | ${a.contact_ready ?? "—"} |`,
    `| Size Ready | ${b.size_ready ?? "—"} | ${a.size_ready ?? "—"} |`,
    `| Complete Census v1 | ${b.complete_census_v1 ?? "—"} | ${a.complete_census_v1 ?? "—"} |`,
    `| Needs Source Lookup | ${b.source_lookup_remaining ?? "—"} | ${a.source_lookup_remaining ?? "—"} |`,
    `| Needs Steward Review | ${b.steward_remaining ?? "—"} | ${a.steward_remaining ?? "—"} |`,
    `| Duplicate Risk | ${b.duplicate_risk_remaining ?? "—"} | ${a.duplicate_risk_remaining ?? "—"} |`,
    `| Not Usable Yet / below Clean Core | ${b.below_clean_core ?? "—"} | ${a.below_clean_core ?? "—"} |`,
    ``,
    `## Dirty partner labels (parked — not force-mapped)`,
    ``,
    `- Count: ${a.dirty_partner_labels ?? report.dirty_partner_park?.proposals_planned ?? 0}`,
    `- Classification: dirty_partner_label / excluded_from_clean_core / steward_review_required`,
    `- Brand Setup not modified`,
    ``,
    `## Brand Setup promotion pack (read-only)`,
    ``,
  ];
  for (const c of promo.slice(0, 20)) {
    lines.push(
      `- **${c.proposed_brand_name}** ×${c.census_records_affected} · ${c.parent_company || "—"} · ${(c.countries_affected || []).join(", ") || "—"} · \`${c.recommended_action}\``
    );
  }
  if (!promo.length) lines.push(`_None loaded_`);

  lines.push(
    ``,
    `## Operations`,
    ``,
    `- Records updated: ${report.updates_applied ?? 0}`,
    `- Records inserted: ${report.inserts_applied ?? 0}`,
    `- Dirty partners parked (writes): ${report.dirty_partner_park?.updates_applied ?? 0}`,
    `- Fields written: ${(report.fields_written || []).join(", ") || "(none)"}`,
    `- Phases completed: ${(report.phases || []).length}`,
    `- Passes / runtime: see run_dir`,
    `- Safety stops: ${(report.safety_stops || []).length}`,
    `- Run dir: \`${report.run_dir || "—"}\``,
    ``,
    `## Safety`,
    ``,
    `- Hotel Property Census only`,
    `- Brand Setup / Brand Explorer untouched`,
    `- No owner/operator/date / Recent Momentum / Company Validated / Brand Verified`,
    `- No weak brand inference; dirty labels parked`,
    `- No Mapbox on dirty identity; phone/rooms official-only`,
    ``,
    `## Next recommended action`,
    ``,
    `${report.next_recommended_action || "—"}`,
    ``
  );
  return lines.join("\n");
}

export function writeCalaCompletionReports(report) {
  const jsonPath = path.join(
    ROOT,
    "reports/research-engine-v2/production-census-cala-completion-v1.json"
  );
  const mdPath = path.join(
    ROOT,
    "reports/research-engine-v2/production-census-cala-completion-v1.md"
  );
  const docsPath = path.join(
    ROOT,
    "docs/data-intelligence/production-census-cala-completion-v1.md"
  );
  const md = renderCalaCompletionMd(report);
  writeJson(jsonPath, report);
  // Mission helper also writes the same basename — call it first, then overwrite with the
  // cala-completion inventory (dirty partners + promotion pack + Level 2 tables).
  writeMissionPublicReports(report, {
    objective: CALA_CENSUS_COMPLETION_V1_OBJECTIVE,
  });
  writeText(mdPath, md);
  writeText(docsPath, md);
  return { jsonPath, mdPath, docsPath };
}

/**
 * Mission entrypoint.
 */
export async function runCalaCensusCompletionV1Mission(opts = {}) {
  const argv = opts.argv || process.argv.slice(2);
  const args = opts.args || parseAutopilotArgs(argv);
  const env = opts.env || process.env;
  const log = opts.log || ((m) => console.log(m));
  const started = Date.now();

  args.objective = CALA_CENSUS_COMPLETION_V1_OBJECTIVE;

  const envCheck = checkAutopilotApplyEnv(env);
  const preflight = applyPreflight(args, envCheck);
  const enableWrites = Boolean(
    opts.enableProductionWrites &&
      argv.includes("--enable-production-writes") &&
      args.allApplyConfirms &&
      envCheck.allOk &&
      preflight.ok
  );

  // Phase 1 — Source-of-Truth Guard
  const writeTarget = assertProductionCensusWriteTarget({
    baseName: productionHotelPropertyCensus.baseName,
    tableName: productionHotelPropertyCensus.tableName,
    tableId: CENSUS_TABLE_ID,
  });
  if (!writeTarget.ok) {
    const blocked = {
      ok: false,
      status: CALA_CENSUS_COMPLETION_STATUS.BLOCKED,
      objective: CALA_CENSUS_COMPLETION_V1_OBJECTIVE,
      blocked_reason: writeTarget.reason || "wrong_census_target",
      airtable_writes: false,
      brand_setup_writes: false,
      brand_explorer_writes: false,
      source_of_truth_guard: { ok: false, reason: writeTarget.reason },
    };
    writeCalaCompletionReports(blocked);
    return blocked;
  }

  if (args.mode === "mission" && !preflight.ok) {
    const blocked = {
      ok: false,
      status: CALA_CENSUS_COMPLETION_STATUS.BLOCKED,
      objective: CALA_CENSUS_COMPLETION_V1_OBJECTIVE,
      blocked_reason: "confirmation_or_env",
      blockers: preflight.blockers,
      airtable_writes: false,
      brand_setup_writes: false,
      brand_explorer_writes: false,
      source_of_truth_guard: { ok: true, write_target: writeTarget },
    };
    writeCalaCompletionReports(blocked);
    return blocked;
  }

  const token = opts.token ?? resolvePat();
  const bases = opts.bases ?? resolveTargetBase();
  if (!token || !bases?.target_base_id) {
    const blocked = {
      ok: false,
      status: CALA_CENSUS_COMPLETION_STATUS.BLOCKED,
      objective: CALA_CENSUS_COMPLETION_V1_OBJECTIVE,
      blocked_reason: "missing_airtable_credentials",
      airtable_writes: false,
      brand_setup_writes: false,
      brand_explorer_writes: false,
    };
    writeCalaCompletionReports(blocked);
    return blocked;
  }

  log(`[cala-completion] SoT guard OK — Hotel Property Census ${CENSUS_TABLE_ID}`);
  log(`[cala-completion] Brand Setup / Brand Explorer read-only; promotion pack not applied`);

  const promotionPack = loadBrandSetupPromotionPack();
  log(
    `[cala-completion] promotion pack candidates=${(promotionPack.candidates || []).length} (read-only)`
  );

  let census = await listCensus(bases.target_base_id, token, CENSUS_TABLE_ID);
  const brandBefore = summarizeBrandInventory(census);
  const beforeBase = snapshotMissionCensusMetrics(census, { env });
  const before = enrichMetrics(beforeBase, brandBefore);

  // Park dirty partners (do not stop mission if they remain)
  const dirtyPark = await parkDirtyPartnerLabels(census, {
    enableWrites,
    baseId: bases.target_base_id,
    token,
    tableId: CENSUS_TABLE_ID,
    log,
  });

  if (dirtyPark.updates_applied > 0) {
    census = await listCensus(bases.target_base_id, token, CENSUS_TABLE_ID);
  }

  // Phases 2–9 via shared mission runner
  log(`[cala-completion] starting phased mission (brand/core → Level 2)…`);
  const missionReport = await runCleanCensusV1Mission({
    argv,
    args,
    env,
    enableProductionWrites: enableWrites,
    token,
    bases,
    log,
    beforeMetrics: before,
    skipBeforeSnapshot: true,
  });

  // Refresh after inventory
  let after = missionReport.after || before;
  try {
    const afterRecords = await listCensus(bases.target_base_id, token, CENSUS_TABLE_ID);
    const brandAfter = summarizeBrandInventory(afterRecords);
    const afterBase = snapshotMissionCensusMetrics(afterRecords, { env });
    after = enrichMetrics(afterBase, brandAfter);
  } catch (err) {
    log(`[cala-completion] after inventory refresh failed: ${err?.message || err}`);
  }

  const updatesApplied =
    (missionReport.updates_applied || 0) + (dirtyPark.updates_applied || 0);

  const report = {
    ...missionReport,
    ok: missionReport.status !== CALA_CENSUS_COMPLETION_STATUS.BLOCKED,
    status: missionReport.status,
    objective: CALA_CENSUS_COMPLETION_V1_OBJECTIVE,
    version: CALA_CENSUS_COMPLETION_V1_VERSION,
    write_target: {
      base: productionHotelPropertyCensus.baseName,
      table: productionHotelPropertyCensus.tableName,
      table_id: CENSUS_TABLE_ID,
    },
    brand_setup_writes: false,
    brand_explorer_writes: false,
    airtable_writes: enableWrites && updatesApplied > 0,
    updates_applied: updatesApplied,
    inserts_applied: 0,
    before,
    after,
    dirty_partner_park: dirtyPark,
    brand_setup_promotion_pack: promotionPack,
    source_of_truth_guard: {
      ok: true,
      hotel_property_census_only: true,
      brand_setup_read_only: true,
      brand_explorer_read_only: true,
      old_census_blocked: true,
      vic_read_only: true,
    },
    runtime_ms: Date.now() - started,
  };

  // Re-resolve status with dirty_partner_labels on after metrics
  if (report.status !== CALA_CENSUS_COMPLETION_STATUS.BLOCKED) {
    const clean = after.clean_core || 0;
    const complete = after.complete_census_v1 || 0;
    const sourceGaps =
      (after.blocked_missing_address || 0) +
      (after.blocked_source_access || 0) +
      (after.source_lookup_remaining || 0);
    const dirtyParked = after.dirty_partner_labels || 0;
    const stewardBeyondDirty = Math.max(0, (after.steward_remaining || 0) - dirtyParked);
    if (missionReport.safety_stops?.length) {
      report.status = CALA_CENSUS_COMPLETION_STATUS.BLOCKED;
    } else if (complete < clean || sourceGaps > 0 || stewardBeyondDirty > 0) {
      report.status = CALA_CENSUS_COMPLETION_STATUS.PARTIAL;
    } else {
      report.status = CALA_CENSUS_COMPLETION_STATUS.COMPLETE;
    }
    report.ok = report.status !== CALA_CENSUS_COMPLETION_STATUS.BLOCKED;
  }

  writeCalaCompletionReports(report);
  log(
    `[cala-completion] status=${report.status} updates=${updatesApplied} clean_core ${before.clean_core}→${after.clean_core} address ${before.address_complete}→${after.address_complete} coords ${before.lat_long_complete}→${after.lat_long_complete}`
  );
  return report;
}
