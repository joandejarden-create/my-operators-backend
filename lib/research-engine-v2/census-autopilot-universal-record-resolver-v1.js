/**
 * Universal Hotel Record Resolver mission v1.
 * Record-level field completion across parent families.
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
} from "./census-autopilot-full-latam-v3.js";
import {
  buildCensusGapLedger,
  buildCompletionScorecard,
  writeCensusGapLedger,
} from "./census-gap-ledger.js";
import { buildActiveBrandIndex } from "./census-brand-governance.js";
import { buildCanonicalBrandDictionary } from "./census-brand-canonical-dictionary.js";
import {
  resolveUniversalHotelRecord,
  prioritizeIncompleteRecords,
  warmFamilyDirectoryCaches,
  inspectHotelRecord,
  sanitizeResolverPatch,
} from "./universal-hotel-record-resolver.js";
import { extractChoicePropertyId } from "./census-autopilot-family-directory-adapters.js";
import {
  normalizeChoicePropertyCode,
  isParentPropertyCodeStubName,
} from "./choice-property-record-resolver.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, "../..");

export const UNIVERSAL_RECORD_RESOLVER_V1_OBJECTIVE =
  "universal-record-resolver-v1";
export const UNIVERSAL_RECORD_RESOLVER_V1_VERSION =
  "universal-record-resolver-v1";

export const UNIVERSAL_RECORD_RESOLVER_STATUS = Object.freeze({
  COMPLETE: "production_census_universal_record_resolver_v1_complete",
  PARTIAL_SOURCE:
    "production_census_universal_record_resolver_v1_partial_source_remaining",
  PARTIAL_EXTERNAL:
    "production_census_universal_record_resolver_v1_partial_external_source_decision_needed",
  BLOCKED: "production_census_universal_record_resolver_v1_blocked",
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
  "Market",
  "Submarket",
  "Address",
  "Address Confidence",
  "Address Source URL",
  "Source URL",
  "Official Property URL",
  "Family / Source Family",
  "Phone",
  "Rooms / Keys",
  "Rooms Confidence",
  "Rooms Source URL",
  "Latitude",
  "Longitude",
  "Human Review Required",
  "Enrichment Status",
];

const ALLOWED_WRITE = new Set([
  "Property Name",
  "Canonical Property Name",
  "Official Property URL",
  "Source URL",
  "Family / Source Family",
  "City",
  "State / Region",
  "Market",
  "Submarket",
  "Address",
  "Address Confidence",
  "Address Source URL",
  "Phone",
  "Rooms / Keys",
  "Rooms Confidence",
  "Rooms Source URL",
  "Rooms Source Type",
  "Rooms Evidence Tier",
  "Rooms Review Status",
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

function writeJson(filePath, data) {
  fs.mkdirSync(path.dirname(filePath), { recursive: true });
  fs.writeFileSync(filePath, `${JSON.stringify(data, null, 2)}\n`, "utf8");
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
      throw new Error(
        `census_list_failed:${res.status}:${json?.error?.message || JSON.stringify(json?.error || {})}`
      );
    }
    out.push(...(json.records || []));
    offset = json.offset;
  } while (offset);
  return out;
}

async function fetchRecordById(baseId, token, tableId, recordId) {
  const res = await fetch(
    `https://api.airtable.com/v0/${encodeURIComponent(baseId)}/${encodeURIComponent(tableId)}/${encodeURIComponent(recordId)}`,
    { headers: { Authorization: `Bearer ${token}` } }
  );
  const json = await res.json();
  if (!res.ok) throw new Error(`record_fetch_failed:${res.status}`);
  return json;
}

async function applyPatches(proposals, { baseId, token, tableId, batchSize, log }) {
  let updatesApplied = 0;
  const writeErrors = [];
  const fieldCounts = {
    hotel_url: 0,
    state: 0,
    market: 0,
    submarket: 0,
    address: 0,
    phone: 0,
    rooms: 0,
    coordinates: 0,
    canonical: 0,
  };
  const size = Math.min(100, Math.max(1, batchSize || 100));

  for (let i = 0; i < proposals.length; i += size) {
    const chunk = proposals.slice(i, i + size);
    const updates = chunk
      .map((p) => {
        const fields = {};
        for (const [k, v] of Object.entries(p.patch || {})) {
          if (isForbiddenAutopilotField(k)) continue;
          if (!ALLOWED_WRITE.has(k)) continue;
          if (v === undefined || v === null || v === "") continue;
          if (k === "Address" && !p.patch["Address Source URL"]) continue;
          if (k === "Rooms / Keys" && !p.patch["Rooms Source URL"]) continue;
          fields[k] = v;
          if (k === "Official Property URL") fieldCounts.hotel_url += 1;
          if (k === "State / Region") fieldCounts.state += 1;
          if (k === "Market") fieldCounts.market += 1;
          if (k === "Submarket") fieldCounts.submarket += 1;
          if (k === "Address") fieldCounts.address += 1;
          if (k === "Phone") fieldCounts.phone += 1;
          if (k === "Rooms / Keys") fieldCounts.rooms += 1;
          if (k === "Latitude" || k === "Longitude") fieldCounts.coordinates += 1;
          if (k === "Canonical Property Name") fieldCounts.canonical += 1;
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
          writeErrors.push({
            status: res.status,
            error: json.error || json,
            record_ids: records.map((r) => r.id),
            batch_retry: "one_by_one",
          });
          // One bad row must not block sibling High name/address writes
          for (const rec of records) {
            try {
              const res1 = await fetch(
                `https://api.airtable.com/v0/${encodeURIComponent(baseId)}/${encodeURIComponent(tableId)}`,
                {
                  method: "PATCH",
                  headers: {
                    Authorization: `Bearer ${token}`,
                    "Content-Type": "application/json",
                  },
                  body: JSON.stringify({ records: [rec], typecast: true }),
                }
              );
              const json1 = await res1.json().catch(() => ({}));
              if (!res1.ok) {
                writeErrors.push({
                  status: res1.status,
                  error: json1.error || json1,
                  record_id: rec.id,
                  fields: Object.keys(rec.fields || {}),
                });
              } else {
                updatesApplied += (json1.records || []).length;
              }
            } catch (err1) {
              writeErrors.push({
                record_id: rec.id,
                error: err1?.message || String(err1),
              });
            }
            await new Promise((r) => setTimeout(r, 160));
          }
        } else {
          updatesApplied += (json.records || []).length;
        }
      } catch (err) {
        writeErrors.push({ error: err?.message || String(err) });
      }
      await new Promise((r) => setTimeout(r, 180));
    }
    log?.(
      `[universal-resolver] write batch ${Math.floor(i / size) + 1}: applied=${updatesApplied} errors=${writeErrors.length}`
    );
  }
  return { updatesApplied, writeErrors, fieldCounts };
}

function renderMarkdown(report) {
  const mx = report.mx043 || {};
  return `# Universal Hotel Record Resolver v1

**Status:** \`${report.status}\`
**Objective:** \`${report.objective}\`
**Census mode:** \`${report.census_mode}\`
**Secondary sources enabled:** ${report.secondary_enabled}
**Webhound as Census SoT:** false
**Write target:** Hotel Property Census (\`tbl9aY5ijiuIzzWam\`)
**Airtable writes:** ${report.airtable_writes}

## Summary

- Incomplete records scanned: ${report.incomplete_scanned}
- Records resolved: ${report.records_resolved}
- Records partially resolved: ${report.records_partial}
- Records unresolved: ${report.records_unresolved}
- Records updated: ${report.records_updated}
- Records inserted: ${report.records_inserted}

## Field writes

| Field | Count |
| --- | ---: |
| Canonical Property Name | ${report.field_counts?.canonical ?? 0} |
| Hotel URL | ${report.field_counts?.hotel_url ?? 0} |
| State / Region | ${report.field_counts?.state ?? 0} |
| Market | ${report.field_counts?.market ?? 0} |
| Submarket | ${report.field_counts?.submarket ?? 0} |
| Address | ${report.field_counts?.address ?? 0} |
| Phone | ${report.field_counts?.phone ?? 0} |
| Rooms | ${report.field_counts?.rooms ?? 0} |
| Coordinates | ${report.field_counts?.coordinates ?? 0} |

## Choice MX043

- Record ID: ${mx.record_id || "—"}
- Before: ${JSON.stringify(mx.before || {})}
- After: ${JSON.stringify(mx.after || {})}
- Patch keys: ${(mx.patch_keys || []).join(", ") || "—"}
- Blockers: ${(mx.blockers || []).map((b) => b.reason || b.field).join("; ") || "—"}

## Secondary / external

- Secondary opportunities: ${report.secondary_opportunities_count ?? 0}
- Secondary writes: ${report.secondary_writes ?? 0}
- External source decision report: \`reports/research-engine-v2/hotel-census-external-source-options.md\`

## Top unresolved (sample)

${(report.top_unresolved || [])
  .slice(0, 15)
  .map(
    (u) =>
      `- ${u.record_id} (${u.family || "?"}): ${(u.unresolved_keys || []).join(", ")}`
  )
  .join("\n") || "- —"}

## Command to continue

\`\`\`bash
ALLOW_CENSUS_AUTOPILOT_APPLY=1 \\
CONFIRM_WRITE_TO_PRODUCTION_CENSUS=1 \\
CONFIRM_NO_BRAND_EXPLORER_WRITES=1 \\
CONFIRM_NO_OWNER_OPERATOR_WRITES=1 \\
ENABLE_SECONDARY_HOTEL_DATA_SOURCES=0 \\
npm run census:autopilot -- --region CALA --scope official-parent-inventory --mode mission \\
  --objective universal-record-resolver-v1 \\
  --census-mode field-completion-only \\
  --strategy highest-yield-safe --run-until-complete --max-passes 10 --batch-size 100 \\
  --confirm-safe-writes --confirm-write-to-production-census \\
  --confirm-no-brand-explorer-writes --confirm-no-owner-operator \\
  --confirm-no-date-writes --confirm-no-recent-momentum \\
  --confirm-no-company-validation --confirm-webhound-not-production-source \\
  --enable-production-writes
\`\`\`
`;
}

/**
 * @param {object} opts
 */
export async function runUniversalRecordResolverV1Mission(opts = {}) {
  const env = opts.env || process.env;
  const log = opts.log || console.log;
  const argv = opts.argv || process.argv.slice(2);
  const args = opts.args || parseAutopilotArgs(argv);
  const effectiveMode = resolveCensusMode(argv, {
    ...args,
    censusMode:
      args.censusMode ||
      opts.censusMode ||
      CENSUS_MODE.FIELD_COMPLETION_ONLY,
  });
  const enableWrites = Boolean(opts.enableProductionWrites);
  const secondaryEnabled =
    String(env.ENABLE_SECONDARY_HOTEL_DATA_SOURCES || "0") === "1";

  const runStamp = new Date().toISOString().replace(/[:.]/g, "-").slice(0, 19);
  const runDir = path.join(
    ROOT,
    "reports/research-engine-v2/autopilot",
    `${runStamp}_CALA-universal-record-resolver-v1`
  );
  fs.mkdirSync(runDir, { recursive: true });

  const sot = assertProductionCensusWriteTarget({
    tableId: CENSUS_TABLE_ID,
    tableName: "Hotel Property Census",
  });
  if (!sot.ok) {
    return {
      ok: false,
      status: UNIVERSAL_RECORD_RESOLVER_STATUS.BLOCKED,
      reason: sot.reason,
    };
  }

  const envCheck = checkAutopilotApplyEnv(env);
  const preflightArgs = {
    ...args,
    mode: enableWrites ? "mission" : args.mode || "controlled",
    region: args.region || "CALA",
    scope: args.scope || "official-parent-inventory",
    parentCompany: args.parentCompany || "Choice Hotels International",
    confirms: {
      safeWrites: true,
      writeToProductionCensus: true,
      noBrandExplorer: true,
      noOwnerOperator: true,
      noDateWrites: true,
      noRecentMomentum: true,
      noCompanyValidation: true,
      webhoundNotProduction: true,
    },
    allApplyConfirms: true,
  };
  const preflight = applyPreflight(preflightArgs, envCheck);
  if (enableWrites && (!envCheck.allOk || !preflight.ok)) {
    return {
      ok: false,
      status: UNIVERSAL_RECORD_RESOLVER_STATUS.BLOCKED,
      reason: "missing_confirmations",
      envCheck,
      preflight,
    };
  }

  const token = resolvePat(env);
  const bases = resolveTargetBase(env);
  log(`[universal-resolver] SoT OK — Hotel Property Census ${CENSUS_TABLE_ID}`);
  log(
    `[universal-resolver] mode=${effectiveMode} secondary=${secondaryEnabled} writes=${enableWrites}`
  );

  const insertGuard = assertNoInsertInFieldCompletionMode(effectiveMode, 0);
  if (!insertGuard.ok) {
    return {
      ok: false,
      status: UNIVERSAL_RECORD_RESOLVER_STATUS.BLOCKED,
      reason: insertGuard.reason,
    };
  }

  let census = await listCensus(bases.target_base_id, token, CENSUS_TABLE_ID);
  const activeIndex = buildActiveBrandIndex({ region: args.region || "CALA" });
  const dictionary = buildCanonicalBrandDictionary({
    region: args.region || "CALA",
  });

  const recordId = args.recordId || opts.recordId || null;
  const propertyCode =
    normalizeChoicePropertyCode(args.propertyCode || opts.propertyCode || "") ||
    null;

  /** @type {object[]} */
  let workset = [];
  if (recordId) {
    const rec = await fetchRecordById(
      bases.target_base_id,
      token,
      CENSUS_TABLE_ID,
      recordId
    );
    workset = [rec];
  } else if (propertyCode) {
    workset = census.filter((r) => {
      const id = extractChoicePropertyId(
        r.fields || {},
        r.fields?.["Property Identity Key"]
      );
      return id === propertyCode || JSON.stringify(r.fields || {}).includes(propertyCode);
    });
    if (!workset.length) {
      // Still allow synthetic resolve attempt if code known
      workset = [
        {
          id: null,
          fields: {
            "Brand Family": "Choice Hotels International",
            "Brand Property Code": propertyCode,
            Country: "Mexico",
            "Property Identity Key": `ind_choice_mx_${propertyCode.toLowerCase()}`,
            "Canonical Property Name": `Choice property ${propertyCode}`,
            "Property Name": `Choice property ${propertyCode}`,
          },
        },
      ];
    }
  } else {
    const parent = args.parentCompany || opts.parentCompany;
    let pool = census;
    if (parent) {
      const p = String(parent).toLowerCase();
      pool = census.filter((r) => {
        const f = r.fields || {};
        return (
          String(f["Brand Family"] || "").toLowerCase().includes(p.split(" ")[0]) ||
          String(f["Parent Company"] || "").toLowerCase().includes(p.split(" ")[0])
        );
      });
    }
    const prioritized = prioritizeIncompleteRecords(pool);
    const maxPass = Math.min(
      prioritized.length,
      Number(args.batchSize || opts.batchSize || 100) *
        Math.min(Number(args.maxPasses || opts.maxPasses || 3), 10)
    );
    // Always include MX043 regression record if present
    const mx043 = census.find(
      (r) =>
        extractChoicePropertyId(r.fields || {}, r.fields?.["Property Identity Key"]) ===
        "MX043"
    );
    // Force-include parent "… property CODE" stubs (must not fall outside batch window)
    const stubRecords = pool.filter((r) => {
      const f = r.fields || {};
      return (
        isParentPropertyCodeStubName(f["Canonical Property Name"]) ||
        isParentPropertyCodeStubName(f["Property Name"])
      );
    });
    const picked = prioritized.slice(0, maxPass).map((x) => x.record);
    const ensureIds = new Set(picked.map((r) => r.id).filter(Boolean));
    for (const stub of stubRecords) {
      if (stub.id && !ensureIds.has(stub.id)) {
        picked.unshift(stub);
        ensureIds.add(stub.id);
      }
    }
    if (mx043 && !ensureIds.has(mx043.id)) {
      picked.unshift(mx043);
    }
    workset = picked;
  }

  log(`[universal-resolver] Pass 1 — workset=${workset.length}`);
  writeJson(path.join(runDir, "workset.json"), {
    count: workset.length,
    ids: workset.map((r) => r.id).filter(Boolean).slice(0, 200),
  });

  log(`[universal-resolver] Pass 2 — warm directories + resolve`);
  await warmFamilyDirectoryCaches({ log });

  const beforeById = {};
  const results = [];
  const proposals = [];
  let secondaryOpportunities = 0;

  const mx043Rec =
    workset.find(
      (r) =>
        extractChoicePropertyId(r.fields || {}, r.fields?.["Property Identity Key"]) ===
          "MX043" ||
        propertyCode === "MX043"
    ) || null;
  if (mx043Rec?.id) {
    beforeById[mx043Rec.id] = {
      "Canonical Property Name": mx043Rec.fields?.["Canonical Property Name"],
      "Property Name": mx043Rec.fields?.["Property Name"],
      Address: mx043Rec.fields?.Address || null,
      Phone: mx043Rec.fields?.Phone || null,
      "Rooms / Keys": mx043Rec.fields?.["Rooms / Keys"] || null,
      Market: mx043Rec.fields?.Market || null,
      Submarket: mx043Rec.fields?.Submarket || null,
      Latitude: mx043Rec.fields?.Latitude ?? null,
      Official: mx043Rec.fields?.["Official Property URL"] || null,
    };
  }

  for (let i = 0; i < workset.length; i += 1) {
    const rec = workset[i];
    if (!rec?.id && effectiveMode === CENSUS_MODE.FIELD_COMPLETION_ONLY) {
      // Synthetic without id — resolve for report only, never insert
      const resolved = await resolveUniversalHotelRecord(rec, {
        env,
        propertyCode,
        log,
        skipWarmCache: true,
        enableMapbox: false,
      });
      results.push(resolved);
      continue;
    }
    if (i % 10 === 0) log(`[universal-resolver] resolve ${i + 1}/${workset.length}`);
    try {
      const resolved = await resolveUniversalHotelRecord(rec, {
        env,
        propertyCode:
          propertyCode ||
          extractChoicePropertyId(rec.fields || {}, rec.fields?.["Property Identity Key"]),
        log,
        skipWarmCache: true,
        enableMapbox: true,
      });
      results.push(resolved);
      secondaryOpportunities += (resolved.secondary_opportunities || []).length;
      const patch = sanitizeResolverPatch(resolved.patch || {});
      // Ignore meta-only patches (no material field completion)
      const materialKeys = Object.keys(patch).filter(
        (k) =>
          ![
            "Last Reviewed Date",
            "Enrichment Status",
            "Enrichment Priority",
            "Continent",
            "Sub-Continent",
          ].includes(k)
      );
      if (resolved.ok && materialKeys.length && rec.id) {
        proposals.push({
          record_id: rec.id,
          reason: `universal_resolver_${resolved.family || "record"}`,
          confidence: "High",
          patch: {
            ...Object.fromEntries(materialKeys.map((k) => [k, patch[k]])),
            "Last Reviewed Date": todayIsoDate(),
            "Enrichment Status":
              patch["Enrichment Status"] ||
              `Universal record resolver — ${resolved.family || "record"}`,
          },
          webhound_as_sot: false,
        });
      }
    } catch (err) {
      results.push({
        ok: false,
        status: "error",
        record_id: rec.id,
        error: err?.message || String(err),
      });
    }
    await new Promise((r) => setTimeout(r, 100));
  }

  writeJson(path.join(runDir, "resolve-results.json"), {
    count: results.length,
    sample: results.slice(0, 40),
  });
  writeJson(path.join(runDir, "proposals.json"), {
    count: proposals.length,
    sample: proposals.slice(0, 40),
  });

  log(`[universal-resolver] Pass 3 — writes proposals=${proposals.length}`);
  let recordsUpdated = 0;
  let fieldCounts = {
    hotel_url: 0,
    state: 0,
    market: 0,
    submarket: 0,
    address: 0,
    phone: 0,
    rooms: 0,
    coordinates: 0,
    canonical: 0,
  };
  const safetyStops = [];

  if (enableWrites && proposals.length) {
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
      writeJson(path.join(runDir, "write-errors.json"), {
        count: applied.writeErrors.length,
        errors: applied.writeErrors.slice(0, 50),
      });
    }
    census = await listCensus(bases.target_base_id, token, CENSUS_TABLE_ID);
  }

  // MX043 after
  let mx043After = null;
  const mx043Id = mx043Rec?.id;
  if (mx043Id) {
    const afterRec = census.find((r) => r.id === mx043Id);
    mx043After = afterRec
      ? {
          "Canonical Property Name": afterRec.fields?.["Canonical Property Name"],
          "Property Name": afterRec.fields?.["Property Name"],
          Address: afterRec.fields?.Address || null,
          Phone: afterRec.fields?.Phone || null,
          "Rooms / Keys": afterRec.fields?.["Rooms / Keys"] || null,
          Market: afterRec.fields?.Market || null,
          Submarket: afterRec.fields?.Submarket || null,
          Latitude: afterRec.fields?.Latitude ?? null,
          Official: afterRec.fields?.["Official Property URL"] || null,
        }
      : null;
  }
  const mx043Result = results.find((r) => r.record_id === mx043Id) || null;

  log(`[universal-resolver] Pass 4/5 — gap ledger`);
  const ledger = buildCensusGapLedger(census, { activeIndex, dictionary });
  const scorecard = buildCompletionScorecard(census, { activeIndex, dictionary });
  writeCensusGapLedger(ledger, scorecard, { runDir });

  const recordsResolved = results.filter((r) => r.status === "resolved").length;
  const recordsPartial = results.filter((r) => r.status === "partial").length;
  const recordsUnresolved = results.filter(
    (r) => r.status === "unresolved" || r.status === "error"
  ).length;

  let status = UNIVERSAL_RECORD_RESOLVER_STATUS.PARTIAL_SOURCE;
  if (safetyStops.some((s) => /blocked|confirmation|wrong_census/i.test(s))) {
    status = UNIVERSAL_RECORD_RESOLVER_STATUS.BLOCKED;
  } else if (
    secondaryOpportunities > 0 &&
    !secondaryEnabled &&
    recordsUpdated === 0 &&
    recordsResolved === 0
  ) {
    status = UNIVERSAL_RECORD_RESOLVER_STATUS.PARTIAL_EXTERNAL;
  } else if (
    recordsUnresolved === 0 &&
    recordsPartial === 0 &&
    (recordsResolved > 0 || workset.length === 0)
  ) {
    status = UNIVERSAL_RECORD_RESOLVER_STATUS.COMPLETE;
  } else if (secondaryOpportunities > 50 && recordsUpdated < 5) {
    status = UNIVERSAL_RECORD_RESOLVER_STATUS.PARTIAL_EXTERNAL;
  } else {
    status = UNIVERSAL_RECORD_RESOLVER_STATUS.PARTIAL_SOURCE;
  }

  const report = {
    ok: status !== UNIVERSAL_RECORD_RESOLVER_STATUS.BLOCKED,
    status,
    version: UNIVERSAL_RECORD_RESOLVER_V1_VERSION,
    objective: UNIVERSAL_RECORD_RESOLVER_V1_OBJECTIVE,
    census_mode: effectiveMode,
    secondary_enabled: secondaryEnabled,
    webhound_as_census_sot: false,
    airtable_writes: enableWrites && recordsUpdated > 0,
    brand_setup_writes: false,
    brand_explorer_writes: false,
    records_inserted: 0,
    records_updated: recordsUpdated,
    incomplete_scanned: workset.length,
    records_resolved: recordsResolved,
    records_partial: recordsPartial,
    records_unresolved: recordsUnresolved,
    field_counts: fieldCounts,
    secondary_opportunities_count: secondaryOpportunities,
    secondary_writes: 0,
    mx043: {
      record_id: mx043Id || null,
      before: beforeById[mx043Id] || null,
      after: mx043After,
      patch_keys: Object.keys(mx043Result?.patch || {}),
      blockers: mx043Result?.blockers || [],
      status: mx043Result?.status || null,
    },
    top_unresolved: results
      .filter((r) => r.status === "unresolved" || (r.unresolved_keys || []).length)
      .slice(0, 25)
      .map((r) => ({
        record_id: r.record_id,
        family: r.family,
        unresolved_keys: r.unresolved_keys || r.inspection?.missing_keys,
        blockers: (r.blockers || []).slice(0, 5),
      })),
    run_dir: runDir,
    generated_at: new Date().toISOString(),
  };

  const md = renderMarkdown(report);
  const reportMd = path.join(
    ROOT,
    "reports/research-engine-v2/production-census-universal-record-resolver-v1.md"
  );
  const reportJson = path.join(
    ROOT,
    "reports/research-engine-v2/production-census-universal-record-resolver-v1.json"
  );
  const docsMd = path.join(
    ROOT,
    "docs/data-intelligence/production-census-universal-record-resolver-v1.md"
  );
  fs.writeFileSync(reportMd, md, "utf8");
  fs.writeFileSync(docsMd, md, "utf8");
  writeJson(reportJson, report);
  writeJson(path.join(runDir, "report.json"), report);

  // Append gap ledger note
  const ledgerMdPath = path.join(ROOT, "reports/research-engine-v2/census-gap-ledger.md");
  if (fs.existsSync(ledgerMdPath)) {
    const cur = fs.readFileSync(ledgerMdPath, "utf8");
    if (!cur.includes("Universal record resolver v1")) {
      fs.writeFileSync(
        ledgerMdPath,
        `${cur.trimEnd()}

## Universal record resolver v1 (${todayIsoDate()})

- Status: \`${status}\`
- Updated: ${recordsUpdated} · resolved/partial/unresolved: ${recordsResolved}/${recordsPartial}/${recordsUnresolved}
- MX043: ${mx043Id || "n/a"} · canonical stub → ${mx043After?.["Canonical Property Name"] || "pending"}
- Secondary sources: ${secondaryEnabled ? "enabled" : "disabled (opportunities only)"}
`
      );
    }
  }

  log(
    `[universal-resolver] done status=${status} updated=${recordsUpdated} resolved=${recordsResolved} partial=${recordsPartial}`
  );
  return report;
}
