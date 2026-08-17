/**
 * Source-Confirmed Census v2 — brand steward resolution mission.
 *
 * Classifies unknown brands + Human Review conflicts internally.
 * Applies High-confidence remaps only. Stewards unresolved with reason codes.
 * Write target: Hotel Property Census only. No Brand Setup / Brand Explorer.
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
  canonicalizeParentCompany,
  buildParentCompanyNormalizationProposals,
  writeParentCompanyNormalizationReports,
  PARENT_COMPANY_NORMALIZATION_STATUS,
} from "./census-parent-company-normalization.js";
import {
  resolveCensusOfficialBrand,
  isCensusOfficialBrand,
  getCensusOfficialEntry,
  isOpaqueBrandCode,
  decodeBrandFromOfficialUrl,
  CENSUS_OFFICIAL_BRAND_REGISTRY_VERSION,
} from "./census-official-brand-registry.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, "../..");

export const SOURCE_CONFIRMED_CENSUS_V2_OBJECTIVE = "source-confirmed-census-v2";
export const SOURCE_CONFIRMED_CENSUS_V2_VERSION = "source-confirmed-census-v2-v1";

export const SOURCE_CONFIRMED_STATUS = Object.freeze({
  COMPLETE: "production_census_source_confirmed_census_v2_complete",
  PARTIAL: "production_census_source_confirmed_census_v2_partial_steward_remaining",
  BLOCKED: "production_census_source_confirmed_census_v2_blocked_safety_stop",
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
  "Source URL",
  "Official Property URL",
  "Family / Source Family",
  "Human Review Required",
  "Data Confidence Tier",
  "Enrichment Status",
  "Enrichment Priority",
  "Last Reviewed Date",
  "Address",
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

function familiesCompatible(a, b, listingFamilies = []) {
  if (!a || !b) return true;
  const na = String(a).toLowerCase();
  const nb = String(b).toLowerCase();
  if (na === nb) return true;
  if (na.includes(nb) || nb.includes(na)) return true;
  for (const f of listingFamilies || []) {
    if (String(f).toLowerCase() === nb || String(f).toLowerCase() === na) return true;
  }
  if (/marriott/i.test(a) && /marriott/i.test(b)) return true;
  if (/hilton/i.test(a) && /hilton/i.test(b)) return true;
  if (/ihg|intercontinental/i.test(a) && /ihg|intercontinental/i.test(b)) return true;
  if (/choice|radisson/i.test(a) && /choice|radisson/i.test(b)) return true;
  if (/accor/i.test(a) && /accor/i.test(b)) return true;
  if (/wyndham/i.test(a) && /wyndham/i.test(b)) return true;
  if (/slh|small luxury/i.test(a) && /slh|small luxury|hilton/i.test(b)) return true;
  if (/design hotels/i.test(a) && /marriott|accor/i.test(b)) return true;
  return false;
}

/**
 * Classify one census record for source-confirmed brand mission.
 */
export function classifySourceConfirmedBrandRow(record) {
  const fields = record?.fields || {};
  const brand = String(fields[MAP_FIRST_PASS.currentBrand] || "").trim();
  const propertyName = String(fields[MAP_FIRST_PASS.propertyName] || "").trim();
  const brandFamily = String(fields[MAP_FIRST_PASS.brandFamily] || "").trim();
  const sourceFamily = String(fields[MAP_FIRST_PASS.family] || "").trim();
  const sourceUrl = String(
    fields[MAP_FIRST_PASS.officialUrl] || fields[MAP_FIRST_PASS.sourceUrl] || ""
  ).trim();
  const humanReview = fields[MAP_FIRST_PASS.humanReview] === true;

  const base = {
    record_id: record.id,
    identity_key: fields[MAP_FIRST_PASS.identityKey] || null,
    property_name: propertyName,
    brand_before: brand,
    human_review_before: humanReview,
    source_family: sourceFamily,
    brand_family: brandFamily,
    source_url: sourceUrl,
  };

  const resolved = resolveCensusOfficialBrand(brand, {
    propertyName,
    sourceUrl,
    sourceFamily,
  });

  if (resolved.ok && resolved.already_canonical) {
    const entry = getCensusOfficialEntry(resolved.canonical);
    if (humanReview) {
      const urlDecoded = decodeBrandFromOfficialUrl(sourceUrl, propertyName, sourceFamily);
      if (
        urlDecoded.ok &&
        String(urlDecoded.canonical).toLowerCase() ===
          String(resolved.canonical).toLowerCase()
      ) {
        return {
          ...base,
          class: "soft_brand_listing_confirmed",
          action: "clear_human_review",
          high_patch: {
            [MAP_FIRST_PASS.humanReview]: false,
            [MAP_FIRST_PASS.brandFamily]: entry?.parent || brandFamily || resolved.parent,
            "Data Confidence Tier": "High",
            "Enrichment Status": "In Progress",
            "Enrichment Priority": "High",
            "Last Reviewed Date": todayIsoDate(),
          },
          reason_code: "soft_brand_official_listing_confirmed",
          brand_after: resolved.canonical,
        };
      }
      return {
        ...base,
        class: "source_conflict_steward",
        action: "none",
        reason_code: "source_conflict_unresolved",
        brand_after: brand,
      };
    }
    return {
      ...base,
      class: "brand_valid_source_confirmed",
      action: "none",
      reason_code: "already_official",
      brand_after: brand,
    };
  }

  if (resolved.ok && resolved.confidence === "High" && !resolved.already_canonical) {
    const patch = {
      [MAP_FIRST_PASS.currentBrand]: resolved.canonical,
      "Data Confidence Tier": "High",
      "Enrichment Status": "In Progress",
      "Enrichment Priority": "High",
      "Last Reviewed Date": todayIsoDate(),
    };
    if (resolved.parent) patch[MAP_FIRST_PASS.brandFamily] = resolved.parent;
    if (humanReview) patch[MAP_FIRST_PASS.humanReview] = false;
    return {
      ...base,
      class: "high_safe_remap",
      action: "update_brand",
      high_patch: patch,
      reason_code: resolved.method || "high_safe_remap",
      brand_after: resolved.canonical,
      method: resolved.method,
      brand_setup_promotion_candidate: Boolean(resolved.brand_setup_promotion_candidate),
    };
  }

  if (resolved.steward_code === "brand_code_unresolved" || isOpaqueBrandCode(brand)) {
    return {
      ...base,
      class: "brand_code_unresolved",
      action: humanReview ? "none" : "flag_human_review",
      high_patch: humanReview
        ? null
        : {
            [MAP_FIRST_PASS.humanReview]: true,
            "Enrichment Priority": "High",
            "Last Reviewed Date": todayIsoDate(),
          },
      reason_code: "brand_code_unresolved",
      brand_after: brand,
    };
  }

  if (resolved.steward_code === "brand_setup_promotion_candidate") {
    return {
      ...base,
      class: "brand_setup_promotion_candidate",
      action: "none",
      reason_code: "brand_setup_promotion_candidate",
      brand_after: brand,
      promotion_candidate: true,
    };
  }

  if (humanReview && isCensusOfficialBrand(brand)) {
    const urlDecoded = decodeBrandFromOfficialUrl(sourceUrl, propertyName, sourceFamily);
    if (
      urlDecoded.ok &&
      String(urlDecoded.canonical).toLowerCase() === String(brand).toLowerCase()
    ) {
      const entry = getCensusOfficialEntry(brand);
      return {
        ...base,
        class: "soft_brand_listing_confirmed",
        action: "clear_human_review",
        high_patch: {
          [MAP_FIRST_PASS.humanReview]: false,
          [MAP_FIRST_PASS.brandFamily]:
            canonicalizeParentCompany(entry?.parent || brandFamily) ||
            entry?.parent ||
            brandFamily,
          "Last Reviewed Date": todayIsoDate(),
        },
        reason_code: "human_review_cleared_source_confirmed",
        brand_after: brand,
      };
    }
    return {
      ...base,
      class: "source_conflict_steward",
      action: "none",
      reason_code: "source_conflict_unresolved",
      brand_after: brand,
    };
  }

  if (!isCensusOfficialBrand(brand)) {
    return {
      ...base,
      class: "brand_unknown_not_in_registry",
      action: "none",
      reason_code: resolved.steward_code || "brand_unknown_not_in_registry",
      brand_after: brand,
      promotion_candidate: Boolean(brand && brand.length >= 4 && !isOpaqueBrandCode(brand)),
    };
  }

  void familiesCompatible;
  return {
    ...base,
    class: "brand_valid_source_confirmed",
    action: "none",
    reason_code: "already_official",
    brand_after: brand,
  };
}

/**
 * Build proposals for entire census.
 */
export function buildSourceConfirmedBrandProposals(censusRecords = []) {
  const rows = [];
  const proposals = [];
  const steward = [];
  const promotionCandidates = new Map();
  const examples = [];
  const counters = {
    records_scanned: 0,
    brand_valid_source_confirmed: 0,
    high_safe_remap: 0,
    soft_brand_listing_confirmed: 0,
    brand_code_unresolved: 0,
    brand_setup_promotion_candidate: 0,
    brand_unknown_not_in_registry: 0,
    source_conflict_steward: 0,
    human_review_before: 0,
    unknown_before: 0,
  };

  for (const rec of censusRecords) {
    counters.records_scanned += 1;
    const brand = String(rec.fields?.[MAP_FIRST_PASS.currentBrand] || "").trim();
    if (rec.fields?.[MAP_FIRST_PASS.humanReview] === true) counters.human_review_before += 1;
    if (brand && !isCensusOfficialBrand(brand)) counters.unknown_before += 1;

    const row = classifySourceConfirmedBrandRow(rec);
    rows.push(row);
    if (counters[row.class] != null) counters[row.class] += 1;

    if (row.promotion_candidate || row.class === "brand_setup_promotion_candidate") {
      const b = row.brand_before || "";
      if (!promotionCandidates.has(b)) {
        promotionCandidates.set(b, {
          brand: b,
          count: 0,
          parent_hint: row.source_family,
          examples: [],
        });
      }
      const p = promotionCandidates.get(b);
      p.count += 1;
      if (p.examples.length < 3) p.examples.push(row.property_name);
    }

    if (row.high_patch && Object.keys(row.high_patch).length) {
      proposals.push({
        record_id: row.record_id,
        identity_key: row.identity_key,
        property_name: row.property_name,
        queue: "brand_normalization",
        confidence: "High",
        action: "update",
        patch: row.high_patch,
        fields: row.high_patch,
        brand_before: row.brand_before,
        brand_after: row.brand_after,
        classification: row.class,
        method: row.method || row.reason_code,
        allow_normalization_overwrite: true,
      });
      if (examples.length < 30 && row.brand_before !== row.brand_after) {
        examples.push({
          record_id: row.record_id,
          property_name: row.property_name,
          before: row.brand_before,
          after: row.brand_after,
          reason: row.reason_code,
        });
      } else if (
        examples.length < 30 &&
        row.class === "soft_brand_listing_confirmed"
      ) {
        examples.push({
          record_id: row.record_id,
          property_name: row.property_name,
          before: `${row.brand_before} (HR)`,
          after: `${row.brand_after} (HR cleared)`,
          reason: row.reason_code,
        });
      }
    }

    if (
      [
        "brand_code_unresolved",
        "brand_unknown_not_in_registry",
        "source_conflict_steward",
        "brand_setup_promotion_candidate",
      ].includes(row.class)
    ) {
      steward.push({
        record_id: row.record_id,
        identity_key: row.identity_key,
        property_name: row.property_name,
        brand: row.brand_before,
        reason_code: row.reason_code,
        class: row.class,
        source_family: row.source_family,
        source_url: row.source_url,
      });
    }
  }

  return {
    version: SOURCE_CONFIRMED_CENSUS_V2_VERSION,
    objective: SOURCE_CONFIRMED_CENSUS_V2_OBJECTIVE,
    registry_version: CENSUS_OFFICIAL_BRAND_REGISTRY_VERSION,
    counters,
    proposals,
    steward_cases: steward,
    promotion_candidates: [...promotionCandidates.values()].sort((a, b) => b.count - a.count),
    examples_before_after: examples,
    rows,
  };
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

function countCleanCore(records, dictionary) {
  let n = 0;
  for (const r of records) {
    try {
      const ev = evaluateCleanCorePass(r, { dictionary, skipBrandSourceOfTruth: false });
      if (ev?.pass) n += 1;
    } catch {
      /* ignore */
    }
  }
  return n;
}

function countUnknown(records) {
  return records.filter((r) => {
    const b = String(r.fields?.[MAP_FIRST_PASS.currentBrand] || "").trim();
    return b && !isCensusOfficialBrand(b);
  }).length;
}

function countHr(records) {
  return records.filter((r) => r.fields?.[MAP_FIRST_PASS.humanReview] === true).length;
}

function renderMd(report) {
  const c = report.counters || {};
  const lines = [
    `# Source-Confirmed Census v2 — Brand Steward Mission`,
    ``,
    `**Status:** \`${report.status}\``,
    `**Objective:** \`${SOURCE_CONFIRMED_CENSUS_V2_OBJECTIVE}\``,
    `**Write target:** Hotel Property Census (\`${report.write_target?.table_id}\`)`,
    `**Airtable writes:** ${report.airtable_writes ? "yes" : "no"}`,
    ``,
    `## Before / After`,
    ``,
    `| Metric | Before | After |`,
    `| --- | ---: | ---: |`,
    `| Unknown brands (not in official census registry) | ${report.before?.unknown ?? "—"} | ${report.after?.unknown ?? "—"} |`,
    `| Human Review Required | ${report.before?.human_review ?? "—"} | ${report.after?.human_review ?? "—"} |`,
    `| Clean Core pass (approx) | ${report.before?.clean_core ?? "—"} | ${report.after?.clean_core ?? "—"} |`,
    `| High remaps applied | — | ${report.updates_applied ?? 0} |`,
    ``,
    `## Classification counters`,
    ``,
    `| Class | Count |`,
    `| --- | ---: |`,
    `| Scanned | ${c.records_scanned ?? 0} |`,
    `| brand_valid_source_confirmed | ${c.brand_valid_source_confirmed ?? 0} |`,
    `| high_safe_remap | ${c.high_safe_remap ?? 0} |`,
    `| soft_brand_listing_confirmed | ${c.soft_brand_listing_confirmed ?? 0} |`,
    `| brand_code_unresolved | ${c.brand_code_unresolved ?? 0} |`,
    `| brand_setup_promotion_candidate | ${c.brand_setup_promotion_candidate ?? 0} |`,
    `| brand_unknown_not_in_registry | ${c.brand_unknown_not_in_registry ?? 0} |`,
    `| source_conflict_steward | ${c.source_conflict_steward ?? 0} |`,
    ``,
    `## High-safe remaps (examples)`,
    ``,
  ];
  for (const ex of report.examples_before_after || []) {
    lines.push(
      `- \`${ex.before}\` → \`${ex.after}\` (${ex.reason}) — ${ex.property_name || ex.record_id}`
    );
  }
  if (!(report.examples_before_after || []).length) lines.push(`_None_`);

  lines.push(``, `## Brand Setup promotion candidates (read-only; not written)`, ``);
  for (const p of (report.promotion_candidates || []).slice(0, 40)) {
    lines.push(`- **${p.brand}** ×${p.count} (family hint: ${p.parent_hint || "—"})`);
  }
  if (!(report.promotion_candidates || []).length) lines.push(`_None_`);

  lines.push(
    ``,
    `## Unresolved steward (reason codes)`,
    ``,
    `- Total steward cases: ${(report.steward_cases || []).length}`,
    `- Excluded from Clean Core: ${report.after?.excluded_from_clean_core ?? "—"}`,
    ``,
    `## Safety`,
    ``,
    `- Hotel Property Census only`,
    `- Brand Setup / Brand Explorer untouched`,
    `- No address / coords / phone / rooms`,
    `- No owner/operator/date writes`,
    `- No hotel-name-only brand guesses`,
    `- Opaque codes stewarded as \`brand_code_unresolved\` when undecodable`,
    ``
  );
  return lines.join("\n");
}

export function writeSourceConfirmedReports(report) {
  const jsonPath = path.join(
    ROOT,
    "reports/research-engine-v2/production-census-source-confirmed-census-v2.json"
  );
  const mdPath = path.join(
    ROOT,
    "reports/research-engine-v2/production-census-source-confirmed-census-v2.md"
  );
  const docsPath = path.join(
    ROOT,
    "docs/data-intelligence/production-census-source-confirmed-census-v2.md"
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
export async function runSourceConfirmedCensusV2Mission(opts = {}) {
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
      status: SOURCE_CONFIRMED_STATUS.BLOCKED,
      objective: SOURCE_CONFIRMED_CENSUS_V2_OBJECTIVE,
      blocked_reason: writeTarget.reason || "wrong_census_target",
      airtable_writes: false,
    };
    writeSourceConfirmedReports(blocked);
    return blocked;
  }

  if (args.mode === "mission" && !preflight.ok) {
    const blocked = {
      ok: false,
      status: SOURCE_CONFIRMED_STATUS.BLOCKED,
      objective: SOURCE_CONFIRMED_CENSUS_V2_OBJECTIVE,
      blocked_reason: "confirmation_or_env",
      blockers: preflight.blockers,
      airtable_writes: false,
    };
    writeSourceConfirmedReports(blocked);
    return blocked;
  }

  const token = opts.token ?? resolvePat();
  const bases = opts.bases ?? resolveTargetBase();
  if (!token || !bases?.target_base_id) {
    const blocked = {
      ok: false,
      status: SOURCE_CONFIRMED_STATUS.BLOCKED,
      objective: SOURCE_CONFIRMED_CENSUS_V2_OBJECTIVE,
      blocked_reason: "missing_airtable_credentials",
      airtable_writes: false,
    };
    writeSourceConfirmedReports(blocked);
    return blocked;
  }

  const region = args.region || "CALA";
  const runDir = path.join(
    ROOT,
    "reports/research-engine-v2/autopilot",
    `${new Date().toISOString().replace(/[:.]/g, "-").slice(0, 19)}_${region}-source-confirmed-census-v2`
  );
  fs.mkdirSync(runDir, { recursive: true });

  log(`[source-confirmed-v2] listing Hotel Property Census…`);
  let census = await listCensus(bases.target_base_id, token, CENSUS_TABLE_ID);
  const dictionary = buildCanonicalBrandDictionary({ region });

  const before = {
    records: census.length,
    unknown: countUnknown(census),
    human_review: countHr(census),
    clean_core: countCleanCore(census, dictionary),
  };
  log(
    `[source-confirmed-v2] before unknown=${before.unknown} hr=${before.human_review} clean_core≈${before.clean_core}`
  );

  const plan = buildSourceConfirmedBrandProposals(census);
  writeJson(path.join(runDir, "classification.json"), {
    counters: plan.counters,
    proposals: plan.proposals.length,
    steward: plan.steward_cases.length,
    promotion_candidates: plan.promotion_candidates,
    examples: plan.examples_before_after,
  });

  let updatesApplied = 0;
  const writtenIds = [];
  const writeErrors = [];

  if (enableWrites && plan.proposals.length) {
    log(`[source-confirmed-v2] applying ${plan.proposals.length} High patches…`);
    const batchSize = Math.min(100, Math.max(1, args.batchSize || 100));
    for (let i = 0; i < plan.proposals.length; i += batchSize) {
      const chunk = plan.proposals.slice(i, i + batchSize);
      const updates = chunk.map((p) => ({ id: p.record_id, fields: p.patch }));
      for (let j = 0; j < updates.length; j += 10) {
        const records = updates.slice(j, j + 10);
        try {
          const res = await fetch(
            `https://api.airtable.com/v0/${encodeURIComponent(bases.target_base_id)}/${encodeURIComponent(CENSUS_TABLE_ID)}`,
            {
              method: "PATCH",
              headers: {
                Authorization: `Bearer ${token}`,
                "Content-Type": "application/json",
              },
              // typecast: allow new official Brand select options on Census (not Brand Setup)
              body: JSON.stringify({ records, typecast: true }),
            }
          );
          const json = await res.json().catch(() => ({}));
          if (!res.ok) {
            writeErrors.push({ status: res.status, error: json.error || json, ids: records.map((r) => r.id) });
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
      log(
        `[source-confirmed-v2] batch ${Math.floor(i / batchSize) + 1}: written=${updatesApplied} errors=${writeErrors.length}`
      );
    }
  } else if (!enableWrites) {
    log(`[source-confirmed-v2] dry — ${plan.proposals.length} High proposals not written`);
  }

  // Parent / Brand Family normalization (canonical parents) — after brand remaps
  log(`[source-confirmed-v2] parent_company_normalization…`);
  if (enableWrites && updatesApplied > 0) {
    census = await listCensus(bases.target_base_id, token, CENSUS_TABLE_ID);
  }
  const parentPlan = buildParentCompanyNormalizationProposals(census, { dictionary });
  const parentHigh = (parentPlan.proposals || []).filter((p) => p.action === "update");
  let parentUpdates = 0;
  if (enableWrites && parentHigh.length) {
    log(`[source-confirmed-v2] applying ${parentHigh.length} High Brand Family patches…`);
    for (let i = 0; i < parentHigh.length; i += 10) {
      const records = parentHigh.slice(i, i + 10).map((p) => ({
        id: p.record_id,
        fields: p.patch,
      }));
      try {
        const res = await fetch(
          `https://api.airtable.com/v0/${encodeURIComponent(bases.target_base_id)}/${encodeURIComponent(CENSUS_TABLE_ID)}`,
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
            phase: "parent_company_normalization",
          });
        } else {
          parentUpdates += (json.records || []).length;
          updatesApplied += (json.records || []).length;
          for (const r of json.records || []) writtenIds.push(r.id);
        }
      } catch (err) {
        writeErrors.push({
          error: err?.message || String(err),
          phase: "parent_company_normalization",
        });
      }
      await new Promise((r) => setTimeout(r, 180));
    }
    log(`[source-confirmed-v2] parent Brand Family updates=${parentUpdates}`);
  } else {
    log(
      `[source-confirmed-v2] parent dry — ${parentHigh.length} High Brand Family proposals (steward=${(parentPlan.steward_cases || []).length})`
    );
  }

  if (enableWrites && (updatesApplied > 0 || parentUpdates > 0)) {
    census = await listCensus(bases.target_base_id, token, CENSUS_TABLE_ID);
  }
  const afterPlan = buildSourceConfirmedBrandProposals(census);
  const afterParent = buildParentCompanyNormalizationProposals(census, { dictionary });
  const after = {
    records: census.length,
    unknown: countUnknown(census),
    human_review: countHr(census),
    clean_core: countCleanCore(census, dictionary),
    excluded_from_clean_core: census.length - countCleanCore(census, dictionary),
    parent_valid: afterParent.counters?.parent_valid ?? 0,
    parent_blank: afterParent.counters?.parent_blank ?? 0,
    parent_alias_remaining: afterParent.counters?.parent_alias_normalizable ?? 0,
  };

  let parentStatus = PARENT_COMPANY_NORMALIZATION_STATUS.READY_NEEDS_MISSION;
  if (parentUpdates > 0 && (afterParent.counters?.parent_alias_normalizable || 0) === 0) {
    parentStatus =
      (afterParent.steward_cases || []).length > 0
        ? PARENT_COMPANY_NORMALIZATION_STATUS.PARTIAL
        : PARENT_COMPANY_NORMALIZATION_STATUS.APPLIED_CLEAN;
  } else if (parentUpdates > 0) {
    parentStatus = PARENT_COMPANY_NORMALIZATION_STATUS.PARTIAL;
  } else if ((parentPlan.counters?.high_proposals || 0) > 0 && !enableWrites) {
    parentStatus = PARENT_COMPANY_NORMALIZATION_STATUS.READY_NEEDS_MISSION;
  } else if ((parentPlan.counters?.high_proposals || 0) === 0) {
    parentStatus =
      (parentPlan.steward_cases || []).length > 0
        ? PARENT_COMPANY_NORMALIZATION_STATUS.PARTIAL
        : PARENT_COMPANY_NORMALIZATION_STATUS.APPLIED_CLEAN;
  }

  writeParentCompanyNormalizationReports(
    {
      ...afterParent,
      status: parentStatus,
      before: {
        parent_valid: parentPlan.counters?.parent_valid,
        parent_blank: parentPlan.counters?.parent_blank,
        clean_core: before.clean_core,
      },
      after: {
        parent_valid: after.parent_valid,
        parent_blank: after.parent_blank,
        clean_core: after.clean_core,
      },
      records_written: parentUpdates,
      fields_written: parentUpdates
        ? ["Brand Family", "Data Confidence Tier", "Enrichment Status", "Enrichment Priority", "Last Reviewed Date"]
        : [],
      airtable_writes: enableWrites && parentUpdates > 0,
      examples_before_after: parentPlan.examples_before_after,
      steward_cases: afterParent.steward_cases,
      counters: {
        ...parentPlan.counters,
        high_applied: parentUpdates,
      },
    },
    { mode: args.mode, objective: SOURCE_CONFIRMED_CENSUS_V2_OBJECTIVE }
  );

  let status = SOURCE_CONFIRMED_STATUS.PARTIAL;
  if (writeErrors.length && updatesApplied === 0 && enableWrites && plan.proposals.length) {
    status = SOURCE_CONFIRMED_STATUS.BLOCKED;
  } else if (
    after.unknown === 0 &&
    after.human_review === 0 &&
    afterPlan.steward_cases.filter((s) => s.class !== "brand_setup_promotion_candidate")
      .length === 0
  ) {
    status = SOURCE_CONFIRMED_STATUS.COMPLETE;
  }

  const report = {
    ok: status !== SOURCE_CONFIRMED_STATUS.BLOCKED,
    status,
    objective: SOURCE_CONFIRMED_CENSUS_V2_OBJECTIVE,
    version: SOURCE_CONFIRMED_CENSUS_V2_VERSION,
    write_target: {
      base: productionHotelPropertyCensus.baseName,
      table: productionHotelPropertyCensus.tableName,
      table_id: CENSUS_TABLE_ID,
    },
    brand_setup_writes: false,
    brand_explorer_writes: false,
    airtable_writes: enableWrites && updatesApplied > 0,
    before,
    after,
    counters: plan.counters,
    updates_applied: updatesApplied,
    parent_company_normalization: {
      status: parentStatus,
      high_proposed: parentPlan.counters?.high_proposals ?? 0,
      updates_applied: parentUpdates,
      steward_cases: (afterParent.steward_cases || []).length,
    },
    proposals_planned: plan.proposals.length,
    steward_cases: afterPlan.steward_cases,
    promotion_candidates: afterPlan.promotion_candidates,
    examples_before_after: plan.examples_before_after,
    write_errors: writeErrors.slice(0, 20),
    written_ids: writtenIds,
    run_dir: runDir,
    runtime_ms: Date.now() - started,
    next_recommended_action:
      status === SOURCE_CONFIRMED_STATUS.COMPLETE
        ? "Proceed to core identity / Clean Core mission"
        : "Steward remaining brand_code_unresolved + Brand Setup promotion candidates; do not guess opaque codes",
  };

  writeJson(path.join(runDir, "final-summary.json"), report);
  writeText(path.join(runDir, "final-summary.md"), renderMd(report));
  writeSourceConfirmedReports(report);
  log(
    `[source-confirmed-v2] status=${status} updates=${updatesApplied} unknown ${before.unknown}→${after.unknown} hr ${before.human_review}→${after.human_review}`
  );
  return report;
}
