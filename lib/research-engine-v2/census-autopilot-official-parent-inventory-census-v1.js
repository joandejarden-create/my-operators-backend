/**
 * Official Parent Inventory Census Mission v1.
 *
 * Discovers official parent-company inventory (not Active/Live-only), reconciles
 * coverage, inserts High-confidence missing hotels, classifies Brand Governance
 * Status, applies Census Only / Hold for non-active evidence-backed brands,
 * normalizes Brand / Brand Family where safe, recomputes geography + Clean Core,
 * and maintains the Brand Setup promotion decision pack (read-only vs Brand Setup).
 *
 * Write target: Hotel Property Census (tbl9aY5ijiuIzzWam) only.
 * Never writes Brand Setup / Brand Explorer / old Census / VIC.
 * Never writes address / lat / long / phone / rooms in this mission.
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
import {
  runCoverageReconciliation,
  COVERAGE_STATUS,
} from "./census-autopilot-coverage-reconciliation.js";
import {
  classifyBrandGovernanceStatus,
  buildNonActiveCensusGovernanceFields,
  writeBrandSetupPromotionDecisionPack,
  BRAND_GOVERNANCE_STATUS,
  CENSUS_ONLY_PRODUCTION_USE_STATUS,
  buildActiveBrandIndex,
} from "./census-brand-governance.js";
import { buildBrandNormalizationProposals } from "./census-brand-normalization.js";
import { buildParentCompanyNormalizationProposals } from "./census-parent-company-normalization.js";
import { buildMarketGeographyProposals } from "./census-market-submarket-classifier.js";
import { CENSUS_GEO_FIELDS } from "./census-region-market-map.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, "../..");

export const OFFICIAL_PARENT_INVENTORY_CENSUS_V1_OBJECTIVE =
  "official-parent-inventory-census-v1";
export const OFFICIAL_PARENT_INVENTORY_CENSUS_V1_VERSION =
  "official-parent-inventory-census-v1";

export const OFFICIAL_PARENT_INVENTORY_STATUS = Object.freeze({
  COMPLETE: "production_census_official_parent_inventory_census_v1_complete",
  PARTIAL:
    "production_census_official_parent_inventory_census_v1_partial_steward_remaining",
  BLOCKED: "production_census_official_parent_inventory_census_v1_blocked_safety_stop",
});

export const OFFICIAL_PARENTS = Object.freeze([
  "Marriott",
  "Hilton",
  "IHG",
  "Choice",
  "Accor",
  "Wyndham",
  "Preferred",
]);

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
  "Source URL",
  "Official Property URL",
  "Family / Source Family",
  "Human Review Required",
  "Data Confidence Tier",
  "Enrichment Status",
  "Enrichment Priority",
  "Last Reviewed Date",
  "Production Use Status",
  "Public Display Review Status",
  "Radar Display Status",
  "Radar Display Reason",
  "Continent",
  "Sub-Continent",
  "Market",
  "Submarket",
];

const ALLOWED_PATCH_FIELDS = new Set([
  "Property Name",
  "Canonical Property Name",
  "Current Brand",
  "Brand Family",
  "City",
  "State / Region",
  "Country",
  "Continent",
  "Sub-Continent",
  "Market",
  "Submarket",
  "Source URL",
  "Family / Source Family",
  "Data Confidence Tier",
  "Production Use Status",
  "Human Review Required",
  "Public Display Review Status",
  "Radar Display Status",
  "Radar Display Reason",
  "Enrichment Status",
  "Enrichment Priority",
  "Last Reviewed Date",
]);

const FORBIDDEN_PATCH_FIELDS = new Set([
  "Address",
  "Latitude",
  "Longitude",
  "Phone",
  "Rooms / Keys",
  "Owner Name",
  "Developer",
  "Developer Name",
  "Operator / Management Company",
  "Opening Date",
  "Renovation Date",
  "Renovation / Conversion Date",
  "Affiliation Start Date",
  "Recent Momentum",
  "Company Validated",
  "Brand Verified",
  "Brand Status",
]);

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

function sanitizePatch(fields = {}) {
  /** @type {Record<string, unknown>} */
  const out = {};
  for (const [k, v] of Object.entries(fields || {})) {
    if (FORBIDDEN_PATCH_FIELDS.has(k)) continue;
    if (!ALLOWED_PATCH_FIELDS.has(k)) continue;
    if (v === undefined || v === null || v === "") continue;
    out[k] = v;
  }
  return out;
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
  const writtenIds = [];
  const writeErrors = [];
  const size = Math.min(100, Math.max(1, batchSize || 100));
  for (let i = 0; i < proposals.length; i += size) {
    const chunk = proposals.slice(i, i + size);
    const updates = chunk
      .map((p) => ({ id: p.record_id, fields: sanitizePatch(p.patch) }))
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
          writeErrors.push({
            status: res.status,
            error: json.error || json,
            ids: records.map((r) => r.id),
          });
        } else {
          for (const r of json.records || []) {
            writtenIds.push(r.id);
            updatesApplied += 1;
          }
        }
      } catch (err) {
        writeErrors.push({ error: err?.message || String(err) });
      }
      await new Promise((r) => setTimeout(r, 180));
    }
    log?.(
      `[official-parent-inventory] patch batch ${Math.floor(i / size) + 1}: written=${updatesApplied} errors=${writeErrors.length}`
    );
  }
  return { updatesApplied, writtenIds, writeErrors };
}

function countByGovernance(records, activeIndex) {
  const counts = {
    [BRAND_GOVERNANCE_STATUS.ACTIVE_BRAND_SETUP]: 0,
    [BRAND_GOVERNANCE_STATUS.EVIDENCE_BACKED_NON_ACTIVE]: 0,
    [BRAND_GOVERNANCE_STATUS.PROMOTION_CANDIDATE]: 0,
    [BRAND_GOVERNANCE_STATUS.DIRTY_PARTNER_LABEL]: 0,
    [BRAND_GOVERNANCE_STATUS.BRAND_CODE_UNRESOLVED]: 0,
    [BRAND_GOVERNANCE_STATUS.UNSUPPORTED_OR_AMBIGUOUS]: 0,
  };
  for (const r of records) {
    const gov = classifyBrandGovernanceStatus({ fields: r.fields || {} }, { activeIndex });
    counts[gov.status] = (counts[gov.status] || 0) + 1;
  }
  return counts;
}

function countCleanCore(records, dictionary, activeIndex) {
  let n = 0;
  for (const r of records) {
    try {
      const ev = evaluateCleanCorePass(r, {
        dictionary,
        activeIndex,
        skipBrandSourceOfTruth: false,
      });
      if (ev?.pass) n += 1;
    } catch {
      /* ignore */
    }
  }
  return n;
}

function countFieldEquals(records, field, value) {
  return records.filter((r) => String(r.fields?.[field] || "").trim() === value).length;
}

function countFieldTruthy(records, field) {
  return records.filter((r) => {
    const v = r.fields?.[field];
    return v === true || String(v || "").trim().toLowerCase() === "hold";
  }).length;
}

/**
 * Build governance patches for existing Census rows that need Census Only / Hold.
 */
export function buildGovernanceHoldPatches(censusRecords = [], opts = {}) {
  const activeIndex = opts.activeIndex || buildActiveBrandIndex(opts);
  const proposals = [];
  const promotionMap = new Map();

  for (const rec of censusRecords) {
    const fields = rec.fields || {};
    const gov = classifyBrandGovernanceStatus({ fields }, { activeIndex });

    if (
      gov.status === BRAND_GOVERNANCE_STATUS.EVIDENCE_BACKED_NON_ACTIVE ||
      gov.status === BRAND_GOVERNANCE_STATUS.PROMOTION_CANDIDATE
    ) {
      const hold = buildNonActiveCensusGovernanceFields(gov);
      /** @type {Record<string, unknown>} */
      const patch = {};
      for (const [k, v] of Object.entries(hold)) {
        const cur = fields[k];
        if (cur !== v) patch[k] = v;
      }
      // Ensure Census Only even if Hold fields already set
      if (fields["Production Use Status"] !== CENSUS_ONLY_PRODUCTION_USE_STATUS) {
        patch["Production Use Status"] = CENSUS_ONLY_PRODUCTION_USE_STATUS;
      }
      if (Object.keys(patch).length) {
        proposals.push({
          record_id: rec.id,
          reason: `governance_${gov.status}`,
          brand: gov.brand,
          patch: sanitizePatch({
            ...patch,
            "Last Reviewed Date": todayIsoDate(),
          }),
        });
      }

      const key = String(gov.brand || "").trim();
      if (key && !gov.in_active_brand_setup) {
        const prev = promotionMap.get(key) || {
          proposed_brand_name: key,
          parent_company: gov.parent_company,
          governance_status: gov.status,
          official_source_evidence: true,
          appears_in_official_parent_inventory: true,
          in_active_brand_setup: false,
          census_records_affected: 0,
          countries_affected: new Set(),
          source_url_examples: [],
          property_examples: [],
        };
        prev.census_records_affected += 1;
        const country = String(fields[MAP_FIRST_PASS.country] || "").trim();
        if (country) prev.countries_affected.add(country);
        const url = String(
          fields[MAP_FIRST_PASS.officialUrl] || fields["Official Property URL"] || ""
        ).trim();
        if (url && prev.source_url_examples.length < 3) prev.source_url_examples.push(url);
        const name = String(fields[MAP_FIRST_PASS.propertyName] || "").trim();
        if (name && prev.property_examples.length < 3) prev.property_examples.push(name);
        promotionMap.set(key, prev);
      }
    }

    if (
      gov.status === BRAND_GOVERNANCE_STATUS.DIRTY_PARTNER_LABEL ||
      gov.status === BRAND_GOVERNANCE_STATUS.BRAND_CODE_UNRESOLVED ||
      gov.status === BRAND_GOVERNANCE_STATUS.UNSUPPORTED_OR_AMBIGUOUS
    ) {
      if (fields[MAP_FIRST_PASS.humanReview] !== true) {
        proposals.push({
          record_id: rec.id,
          reason: `exclude_${gov.status}`,
          brand: gov.brand,
          patch: sanitizePatch({
            "Human Review Required": true,
            "Enrichment Priority": "High",
            "Last Reviewed Date": todayIsoDate(),
            "Radar Display Reason": `Brand governance: ${gov.status}`,
          }),
        });
      }
    }
  }

  const promotion_candidates = [...promotionMap.values()].map((c) => ({
    ...c,
    countries_affected: [...c.countries_affected],
  }));

  return { proposals, promotion_candidates };
}

function mergeProposals(...lists) {
  /** @type {Map<string, { record_id: string, patch: Record<string, unknown>, reasons: string[] }>} */
  const byId = new Map();
  for (const list of lists) {
    for (const p of list || []) {
      const id = p.record_id;
      if (!id) continue;
      const patch = sanitizePatch(p.patch || p.fields || {});
      if (!Object.keys(patch).length) continue;
      const prev = byId.get(id) || { record_id: id, patch: {}, reasons: [] };
      Object.assign(prev.patch, patch);
      prev.reasons.push(p.reason || p.method || "patch");
      byId.set(id, prev);
    }
  }
  return [...byId.values()];
}

function inventoryByParent(coverageByParent = {}) {
  /** @type {Record<string, number>} */
  const out = {};
  for (const [parent, report] of Object.entries(coverageByParent)) {
    out[parent] = report?.official_inventory_count || 0;
  }
  return out;
}

function insertsByParent(coverageByParent = {}) {
  /** @type {Record<string, number>} */
  const out = {};
  for (const [parent, report] of Object.entries(coverageByParent)) {
    out[parent] = report?.inserted_count || 0;
  }
  return out;
}

function renderMd(report) {
  const b = report.before || {};
  const a = report.after || {};
  const g = report.governance_after || {};
  const lines = [
    `# Official Parent Inventory Census Mission v1`,
    ``,
    `**Status:** \`${report.status}\``,
    `**Objective:** \`${OFFICIAL_PARENT_INVENTORY_CENSUS_V1_OBJECTIVE}\``,
    `**Scope:** \`official-parent-inventory\``,
    `**Write target:** Hotel Property Census (\`${report.write_target?.table_id}\`)`,
    `**Airtable writes:** ${report.airtable_writes ? "yes" : "no"}`,
    `**Brand Setup writes:** false`,
    `**Brand Explorer writes:** false`,
    ``,
    `## Before / After`,
    ``,
    `| Metric | Before | After |`,
    `| --- | ---: | ---: |`,
    `| Total Census records | ${b.total ?? "—"} | ${a.total ?? "—"} |`,
    `| Clean Core | ${b.clean_core ?? "—"} | ${a.clean_core ?? "—"} |`,
    `| Census Only / Not Owner-Facing | ${b.census_only ?? "—"} | ${a.census_only ?? "—"} |`,
    `| Public Display Hold | ${b.public_hold ?? "—"} | ${a.public_hold ?? "—"} |`,
    `| Radar Display Hold | ${b.radar_hold ?? "—"} | ${a.radar_hold ?? "—"} |`,
    `| Human Review Required | ${b.human_review ?? "—"} | ${a.human_review ?? "—"} |`,
    ``,
    `## Brand Governance Status (after)`,
    ``,
    `| Status | Count |`,
    `| --- | ---: |`,
    `| active_brand_setup | ${g.active_brand_setup ?? 0} |`,
    `| evidence_backed_non_active_brand | ${g.evidence_backed_non_active_brand ?? 0} |`,
    `| brand_setup_promotion_candidate | ${g.brand_setup_promotion_candidate ?? 0} |`,
    `| dirty_partner_label | ${g.dirty_partner_label ?? 0} |`,
    `| brand_code_unresolved | ${g.brand_code_unresolved ?? 0} |`,
    `| unsupported_or_ambiguous | ${g.unsupported_or_ambiguous ?? 0} |`,
    ``,
    `## Official inventory by parent`,
    ``,
    `| Parent | Official inventory | Inserts |`,
    `| --- | ---: | ---: |`,
  ];
  for (const parent of OFFICIAL_PARENTS) {
    lines.push(
      `| ${parent} | ${report.official_inventory_by_parent?.[parent] ?? 0} | ${report.inserts_by_parent?.[parent] ?? 0} |`
    );
  }
  lines.push(
    ``,
    `## Mission totals`,
    ``,
    `- Inserts applied: ${report.inserts_applied ?? 0}`,
    `- Governance / normalization / geo patches applied: ${report.updates_applied ?? 0}`,
    `- Promotion pack candidates: ${report.promotion_pack_candidates ?? 0}`,
    `- Steward remaining: ${report.steward_remaining ?? 0}`,
    `- Safety stops: ${(report.safety_stops || []).join("; ") || "none"}`,
    ``,
    `## Fields written`,
    ``,
    `${(report.fields_written || []).map((f) => `- ${f}`).join("\n") || "- (none)"}`,
    ``,
    `## Hard constraints`,
    ``,
    `- Brand Setup untouched`,
    `- Brand Explorer untouched`,
    `- No address / coordinates / phone / rooms in this mission`,
    `- No owner / operator / developer / date / Recent Momentum / validation fields`,
    `- Non-active brands remain Census Only / Hold (not owner-facing)`,
    ``
  );
  return lines.join("\n");
}

export function writeOfficialParentInventoryReports(report) {
  const jsonPath = path.join(
    ROOT,
    "reports/research-engine-v2/production-census-official-parent-inventory-census-v1.json"
  );
  const mdPath = path.join(
    ROOT,
    "reports/research-engine-v2/production-census-official-parent-inventory-census-v1.md"
  );
  const docsPath = path.join(
    ROOT,
    "docs/data-intelligence/production-census-official-parent-inventory-census-v1.md"
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
export async function runOfficialParentInventoryCensusV1Mission(opts = {}) {
  const argv = opts.argv || process.argv.slice(2);
  const args = opts.args || parseAutopilotArgs(argv);
  const env = opts.env || process.env;
  const log = opts.log || ((m) => console.log(m));
  const started = Date.now();

  const envCheck = checkAutopilotApplyEnv(env);
  const preflight = applyPreflight(args, envCheck);
  const enableWrites = Boolean(
    opts.enableProductionWrites &&
      argv.includes("--enable-production-writes") &&
      args.allApplyConfirms &&
      envCheck.allOk &&
      preflight.ok
  );

  const writeTarget = assertProductionCensusWriteTarget({
    baseName: productionHotelPropertyCensus.baseName,
    tableName: productionHotelPropertyCensus.tableName,
    tableId: CENSUS_TABLE_ID,
  });
  if (!writeTarget.ok) {
    const blocked = {
      ok: false,
      status: OFFICIAL_PARENT_INVENTORY_STATUS.BLOCKED,
      objective: OFFICIAL_PARENT_INVENTORY_CENSUS_V1_OBJECTIVE,
      blocked_reason: writeTarget.reason || "wrong_census_target",
      airtable_writes: false,
      brand_setup_writes: false,
      brand_explorer_writes: false,
      safety_stops: ["wrong_census_target"],
    };
    writeOfficialParentInventoryReports(blocked);
    return blocked;
  }

  if (args.mode === "mission" && !preflight.ok) {
    const blocked = {
      ok: false,
      status: OFFICIAL_PARENT_INVENTORY_STATUS.BLOCKED,
      objective: OFFICIAL_PARENT_INVENTORY_CENSUS_V1_OBJECTIVE,
      blocked_reason: "confirmation_or_env",
      blockers: preflight.blockers,
      airtable_writes: false,
      brand_setup_writes: false,
      brand_explorer_writes: false,
      safety_stops: preflight.blockers || ["confirmation_or_env"],
    };
    writeOfficialParentInventoryReports(blocked);
    return blocked;
  }

  const token = opts.token ?? resolvePat();
  const bases = opts.bases ?? resolveTargetBase();
  if (!token || !bases?.target_base_id) {
    const blocked = {
      ok: false,
      status: OFFICIAL_PARENT_INVENTORY_STATUS.BLOCKED,
      objective: OFFICIAL_PARENT_INVENTORY_CENSUS_V1_OBJECTIVE,
      blocked_reason: "missing_airtable_credentials",
      airtable_writes: false,
      brand_setup_writes: false,
      brand_explorer_writes: false,
      safety_stops: ["missing_airtable_credentials"],
    };
    writeOfficialParentInventoryReports(blocked);
    return blocked;
  }

  const region = args.region || "CALA";
  const runDir =
    opts.runDir ||
    path.join(
      ROOT,
      "reports/research-engine-v2/autopilot",
      `${new Date().toISOString().replace(/[:.]/g, "-").slice(0, 19)}_${region}-official-parent-inventory-census-v1`
    );
  fs.mkdirSync(runDir, { recursive: true });

  log(`[official-parent-inventory] listing Hotel Property Census (before)…`);
  let census = await listCensus(bases.target_base_id, token, CENSUS_TABLE_ID);
  const dictionary = buildCanonicalBrandDictionary({ region });
  const activeIndex = buildActiveBrandIndex({ region });

  const before = {
    total: census.length,
    clean_core: countCleanCore(census, dictionary, activeIndex),
    census_only: countFieldEquals(
      census,
      "Production Use Status",
      CENSUS_ONLY_PRODUCTION_USE_STATUS
    ),
    public_hold: countFieldEquals(census, "Public Display Review Status", "Hold"),
    radar_hold: countFieldEquals(census, "Radar Display Status", "Hold"),
    human_review: countFieldTruthy(census, MAP_FIRST_PASS.humanReview),
    governance: countByGovernance(census, activeIndex),
  };
  writeJson(path.join(runDir, "before-snapshot.json"), before);
  log(
    `[official-parent-inventory] before total=${before.total} clean_core≈${before.clean_core} census_only=${before.census_only}`
  );

  /** @type {Record<string, object>} */
  const coverageByParent = {};
  let insertsApplied = 0;
  const safetyStops = [];
  const parents = opts.parents || OFFICIAL_PARENTS;

  for (const parent of parents) {
    log(`[official-parent-inventory] coverage reconciliation parent=${parent}…`);
    try {
      const cov = await runCoverageReconciliation({
        region,
        parentCompany: parent,
        mode: args.mode || "mission",
        batchSize: args.batchSize || 100,
        enableProductionWrites: enableWrites,
        allApplyConfirms: Boolean(args.allApplyConfirms),
        confirms: args.confirms,
        discoverAllOfficialParents: true,
        requireBrandMatch: false,
        env,
        log,
        runDir: path.join(runDir, `coverage-${parent.toLowerCase()}`),
        censusRecords: census,
      });
      coverageByParent[parent] = cov;
      insertsApplied += cov?.inserted_count || 0;
      if (cov?.status === COVERAGE_STATUS.BLOCKED) {
        safetyStops.push(`coverage_blocked_${parent}:${cov.blocked_reason || "blocked"}`);
      }
      if ((cov?.inserted_count || 0) > 0) {
        census = await listCensus(bases.target_base_id, token, CENSUS_TABLE_ID);
      }
    } catch (err) {
      const msg = err?.message || String(err);
      log(`[official-parent-inventory] coverage error parent=${parent}: ${msg}`);
      coverageByParent[parent] = {
        ok: false,
        status: COVERAGE_STATUS.BLOCKED,
        blocked_reason: msg,
        inserted_count: 0,
        official_inventory_count: 0,
      };
      safetyStops.push(`coverage_error_${parent}:${msg}`);
    }
  }

  // Re-list after coverage wave
  log(`[official-parent-inventory] re-listing Census after coverage…`);
  census = await listCensus(bases.target_base_id, token, CENSUS_TABLE_ID);

  const govHold = buildGovernanceHoldPatches(census, { activeIndex });
  const brandNorm = buildBrandNormalizationProposals(census, { dictionary });
  const parentNorm = buildParentCompanyNormalizationProposals(census, { dictionary });
  const geoNorm = buildMarketGeographyProposals(census, {});

  const brandProposals = (brandNorm.proposals || []).map((p) => ({
    record_id: p.record_id,
    reason: "brand_normalization",
    patch: {
      ...(p.patch || {}),
      ...(p.canonical_brand ? { "Current Brand": p.canonical_brand } : {}),
    },
  }));
  const parentProposals = (parentNorm.proposals || []).map((p) => ({
    record_id: p.record_id,
    reason: "parent_company_normalization",
    patch: p.patch || (p.canonical_parent ? { "Brand Family": p.canonical_parent } : {}),
  }));
  const geoProposals = (geoNorm.proposals || []).map((p) => ({
    record_id: p.record_id,
    reason: "market_geography_completion",
    patch: p.patch || {},
  }));

  const merged = mergeProposals(
    brandProposals,
    parentProposals,
    geoProposals,
    govHold.proposals // governance Hold / Census Only wins last
  );
  writeJson(path.join(runDir, "merged-patch-proposals.json"), {
    count: merged.length,
    proposals: merged.slice(0, 500),
  });

  let updatesApplied = 0;
  let writeErrors = [];
  if (enableWrites && merged.length) {
    log(`[official-parent-inventory] applying ${merged.length} governance/normalization/geo patches…`);
    const applied = await applyPatches(merged, {
      baseId: bases.target_base_id,
      token,
      tableId: CENSUS_TABLE_ID,
      batchSize: args.batchSize || 100,
      log,
    });
    updatesApplied = applied.updatesApplied;
    writeErrors = applied.writeErrors;
    if (writeErrors.length) {
      safetyStops.push(`patch_write_errors:${writeErrors.length}`);
    }
    census = await listCensus(bases.target_base_id, token, CENSUS_TABLE_ID);
  } else if (!enableWrites) {
    log(
      `[official-parent-inventory] controlled/dry — ${merged.length} patches prepared, no writes`
    );
  }

  const promotion = writeBrandSetupPromotionDecisionPack(govHold.promotion_candidates, {
    source: OFFICIAL_PARENT_INVENTORY_CENSUS_V1_OBJECTIVE,
    region,
    inserts_applied: insertsApplied,
  });

  const governanceAfter = countByGovernance(census, activeIndex);
  const after = {
    total: census.length,
    clean_core: countCleanCore(census, dictionary, activeIndex),
    census_only: countFieldEquals(
      census,
      "Production Use Status",
      CENSUS_ONLY_PRODUCTION_USE_STATUS
    ),
    public_hold: countFieldEquals(census, "Public Display Review Status", "Hold"),
    radar_hold: countFieldEquals(census, "Radar Display Status", "Hold"),
    human_review: countFieldTruthy(census, MAP_FIRST_PASS.humanReview),
    governance: governanceAfter,
  };

  const stewardRemaining =
    (governanceAfter[BRAND_GOVERNANCE_STATUS.DIRTY_PARTNER_LABEL] || 0) +
    (governanceAfter[BRAND_GOVERNANCE_STATUS.BRAND_CODE_UNRESOLVED] || 0) +
    (governanceAfter[BRAND_GOVERNANCE_STATUS.UNSUPPORTED_OR_AMBIGUOUS] || 0) +
    (governanceAfter[BRAND_GOVERNANCE_STATUS.PROMOTION_CANDIDATE] || 0);

  const hardBlocked = safetyStops.some((s) => /wrong_census|missing_airtable|confirmation/.test(s));
  let status = OFFICIAL_PARENT_INVENTORY_STATUS.COMPLETE;
  if (hardBlocked) status = OFFICIAL_PARENT_INVENTORY_STATUS.BLOCKED;
  else if (stewardRemaining > 0 || writeErrors.length > 0 || safetyStops.length > 0) {
    status = OFFICIAL_PARENT_INVENTORY_STATUS.PARTIAL;
  }

  const fieldsWritten = [
    ...new Set(
      merged.flatMap((p) => Object.keys(p.patch || {})).concat(
        insertsApplied > 0
          ? [
              "Property Name",
              "Canonical Property Name",
              "Current Brand",
              "Brand Family",
              "City",
              "Country",
              "Continent",
              "Sub-Continent",
              "Market",
              "Submarket",
              "Source URL",
              "Production Use Status",
              "Public Display Review Status",
              "Radar Display Status",
              "Human Review Required",
            ]
          : []
      )
    ),
  ].sort();

  const report = {
    ok: status !== OFFICIAL_PARENT_INVENTORY_STATUS.BLOCKED,
    status,
    version: OFFICIAL_PARENT_INVENTORY_CENSUS_V1_VERSION,
    objective: OFFICIAL_PARENT_INVENTORY_CENSUS_V1_OBJECTIVE,
    scope: "official-parent-inventory",
    region,
    strategy: args.strategy || "fastest-safe",
    mode: args.mode || "mission",
    generated_at: new Date().toISOString(),
    elapsed_ms: Date.now() - started,
    write_target: {
      base: productionHotelPropertyCensus.baseName,
      table: productionHotelPropertyCensus.tableName,
      table_id: CENSUS_TABLE_ID,
    },
    airtable_writes: enableWrites && (insertsApplied > 0 || updatesApplied > 0),
    brand_setup_writes: false,
    brand_explorer_writes: false,
    level2_writes: false,
    before,
    after,
    governance_after: governanceAfter,
    official_inventory_by_parent: inventoryByParent(coverageByParent),
    inserts_by_parent: insertsByParent(coverageByParent),
    inserts_applied: insertsApplied,
    updates_applied: updatesApplied,
    patch_proposals_prepared: merged.length,
    promotion_pack_candidates: govHold.promotion_candidates.length,
    promotion_pack_paths: {
      json: promotion.jsonPath,
      md: promotion.mdPath,
    },
    steward_remaining: stewardRemaining,
    safety_stops: safetyStops,
    write_errors: writeErrors.slice(0, 20),
    fields_written: fieldsWritten,
    coverage_by_parent: Object.fromEntries(
      Object.entries(coverageByParent).map(([k, v]) => [
        k,
        {
          status: v.status,
          official_inventory_count: v.official_inventory_count,
          inserted_count: v.inserted_count,
          coverage_counts: v.coverage_counts,
          blocked_reason: v.blocked_reason || null,
        },
      ])
    ),
    run_dir: runDir,
    next_recommended_action:
      status === OFFICIAL_PARENT_INVENTORY_STATUS.COMPLETE
        ? "Review promotion pack; owner-facing remains Active/Live only"
        : status === OFFICIAL_PARENT_INVENTORY_STATUS.PARTIAL
          ? "Review steward remaining + promotion pack; continue governance holds"
          : "Resolve safety stop before re-run",
  };

  writeJson(path.join(runDir, "final-summary.json"), report);
  writeOfficialParentInventoryReports(report);
  log(
    `[official-parent-inventory] done status=${status} inserts=${insertsApplied} patches=${updatesApplied} clean_core ${before.clean_core}→${after.clean_core}`
  );
  return report;
}
