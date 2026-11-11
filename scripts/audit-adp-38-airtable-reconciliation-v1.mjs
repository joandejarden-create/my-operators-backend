#!/usr/bin/env node
/**
 * ADP_38_AIRTABLE_RECONCILIATION_V1 — read-only.
 *
 * Reconciles the 38 certified hotels against Airtable ADP tables.
 * Does NOT write Airtable, remesure, republish, or deploy.
 *
 * Authority for current customer metrics: durable published registry /
 * PRODUCTION_DURABLE_PUBLISHED_VS_MEASURED.json (filesystem SoT).
 * Airtable = index / Live overlay / history tables.
 */
import "dotenv/config";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  HISTORY_TABLES,
  EXISTING_LIVE_PUBLISH,
  isAdpHistoryWritesEnabled,
} from "../lib/ai-demand-positioning/longitudinal/airtable-history-schema-final-v1.js";
import {
  ADP_AIRTABLE_ECOSYSTEM_CONTRACT_V1,
  ADP_ECOSYSTEM_TABLES,
  listFullE2eMeasurementHistoryTables,
  PERSISTENCE_AUTH_SCOPE,
} from "../lib/ai-demand-positioning/longitudinal/adp-airtable-ecosystem-contract-v1.js";
import {
  ADP_GOVERNED_BASE_ID,
  resolveProductionHistoryDestination,
} from "../lib/ai-demand-positioning/longitudinal/resolve-adp-history-destination-v1.js";
import { ADP_PUBLISHED_REPORTS_TABLE } from "../lib/ai-demand-positioning/airtable-field-map.js";
import { CORRECTED_INDEPENDENT_APPEARANCE_METHODOLOGY_ID } from "../lib/ai-demand-positioning/certification/adp-unprompted-independent-discovery-standard-v1.js";
import { getAdpPersistencePolicy } from "../lib/ai-demand-positioning/adp-persistence-policy.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const OUT = path.join(
  ROOT,
  "reports/dealality-adp-airtable-persistence-historical-traceability-v1"
);
const DURABLE_AUDIT = path.join(
  ROOT,
  "reports/dealality-adp-single-version-publication-contract-v1/admin-published-vs-measured-dates/PRODUCTION_DURABLE_PUBLISHED_VS_MEASURED.json"
);

const CLASS = Object.freeze({
  MATCH: "MATCH",
  STALE: "STALE",
  MISSING: "MISSING",
  INCOMPLETE: "INCOMPLETE",
  HISTORICAL_ONLY: "HISTORICAL_ONLY",
  LOOKUP_UNAVAILABLE: "LOOKUP_UNAVAILABLE",
});

fs.mkdirSync(OUT, { recursive: true });

function token() {
  return (
    process.env.AIRTABLE_API_KEY ||
    process.env.AIRTABLE_PAT ||
    process.env.AIRTABLE_TOKEN ||
    ""
  ).trim();
}

function baseId() {
  const dest = resolveProductionHistoryDestination({
    confirmGovernedAdpBase: true,
  });
  return (
    process.env.ADP_AIRTABLE_BASE_ID ||
    dest.baseId ||
    process.env.AIRTABLE_INTELLIGENCE_BASE_ID ||
    ADP_GOVERNED_BASE_ID
  );
}

async function airtableList(table, formula = null, fields = null) {
  const t = token();
  const b = baseId();
  if (!t || !b) return { ok: false, error: "AIRTABLE_CONFIG_MISSING", records: [] };
  const out = [];
  let offset = null;
  do {
    const u = new URL(`https://api.airtable.com/v0/${b}/${encodeURIComponent(table)}`);
    u.searchParams.set("pageSize", "100");
    if (formula) u.searchParams.set("filterByFormula", formula);
    if (fields) for (const f of fields) u.searchParams.append("fields[]", f);
    if (offset) u.searchParams.set("offset", offset);
    const res = await fetch(u, {
      headers: { Authorization: `Bearer ${t}` },
    });
    if (!res.ok) {
      const body = await res.text().catch(() => "");
      return {
        ok: false,
        error: `HTTP_${res.status}`,
        detail: body.slice(0, 240),
        records: out,
      };
    }
    const json = await res.json();
    out.push(...(json.records || []));
    offset = json.offset || null;
  } while (offset);
  return { ok: true, records: out };
}

async function discoverTables() {
  const t = token();
  const b = baseId();
  if (!t || !b) return { ok: false, tables: [], error: "AIRTABLE_CONFIG_MISSING" };
  const res = await fetch(`https://api.airtable.com/v0/meta/bases/${b}/tables`, {
    headers: { Authorization: `Bearer ${t}` },
  });
  if (!res.ok) {
    return {
      ok: false,
      tables: [],
      error: `META_HTTP_${res.status}`,
      detail: (await res.text().catch(() => "")).slice(0, 200),
    };
  }
  const json = await res.json();
  return {
    ok: true,
    tables: (json.tables || []).map((tb) => ({
      id: tb.id,
      name: tb.name,
      fieldCount: (tb.fields || []).length,
      fields: (tb.fields || []).map((f) => ({ name: f.name, type: f.type })),
    })),
  };
}

function loadDurableUniverse() {
  if (!fs.existsSync(DURABLE_AUDIT)) {
    throw new Error(`missing_durable_audit:${DURABLE_AUDIT}`);
  }
  const doc = JSON.parse(fs.readFileSync(DURABLE_AUDIT, "utf8"));
  return (doc.rows || []).map((r) => ({
    propertyId: r.propertyId,
    measurementPeriodId: r.periodId,
    considerationRate: r.considerationRate,
    publishedAt: r.publishedAt,
    measuredMonthYear: r.measuredMonthYear,
    publishedMonthYear: r.publishedMonthYear,
    methodologyId: CORRECTED_INDEPENDENT_APPEARANCE_METHODOLOGY_ID,
    methodologyFingerprintSha256:
      "c097b23588c365dc468957846005c5e104bc705cd813d5e62e49bbaf742960e1",
  }));
}

function classifyPublishedOverlay(hotel, liveRows) {
  const matches = liveRows.filter(
    (r) =>
      String(r.fields?.["ADP Property ID"] || r.fields?.["Property ID"] || "") ===
      hotel.propertyId
  );
  if (!matches.length) {
    return {
      class: CLASS.MISSING,
      detail: "no_live_published_reports_row",
      rows: 0,
    };
  }
  const live = matches.filter(
    (r) => String(r.fields?.["Publish Status"] || "") === "Live"
  );
  const current =
    live[0] ||
    matches.sort((a, b) =>
      String(b.fields?.["Published At"] || "").localeCompare(
        String(a.fields?.["Published At"] || "")
      )
    )[0];
  const period = String(
    current.fields?.["Period ID"] || current.fields?.["Period Id"] || ""
  );
  const cons = Number(
    current.fields?.["Demand Capture Rate"] ??
      current.fields?.["Consideration Rate"] ??
      NaN
  );
  const issues = [];
  if (period && period !== hotel.measurementPeriodId) {
    issues.push(`period_mismatch:airtable=${period}`);
  }
  if (
    Number.isFinite(cons) &&
    hotel.considerationRate != null &&
    Math.abs(cons - Number(hotel.considerationRate)) > 0.05
  ) {
    // Demand Capture ≠ Consideration — flag only if field is Consideration
    if (current.fields?.["Consideration Rate"] != null) {
      issues.push(`consideration_mismatch:airtable=${cons}`);
    }
  }
  if (!period) issues.push("period_missing_on_live_row");
  if (!current.fields?.["Payload JSON"] && !current.fields?.["Payload Store Ref"]) {
    issues.push("payload_missing");
  }

  let klass = CLASS.MATCH;
  if (issues.some((i) => i.startsWith("period_mismatch"))) klass = CLASS.STALE;
  else if (issues.length) klass = CLASS.INCOMPLETE;
  if (matches.length > 1 && live.length === 0) {
    // older rows only
    if (klass === CLASS.MATCH) klass = CLASS.HISTORICAL_ONLY;
  }

  return {
    class: klass,
    detail: issues.join(";") || null,
    rows: matches.length,
    liveRows: live.length,
    airtablePeriodId: period || null,
    airtableConsideration: Number.isFinite(cons) ? cons : null,
    publishedAt: current.fields?.["Published At"] || null,
    publishStatus: current.fields?.["Publish Status"] || null,
    recordId: current.id,
  };
}

function classifyHistoryTable(hotel, tableName, records) {
  const forHotel = records.filter(
    (r) =>
      String(r.fields?.["Property ID"] || "") === hotel.propertyId ||
      String(r.fields?.["ADP Property ID"] || "") === hotel.propertyId
  );
  const forPeriod = forHotel.filter(
    (r) =>
      String(r.fields?.["Period ID"] || "") === hotel.measurementPeriodId
  );
  if (!forHotel.length) {
    return { class: CLASS.MISSING, hotelRows: 0, periodRows: 0 };
  }
  if (!forPeriod.length) {
    return {
      class: CLASS.HISTORICAL_ONLY,
      hotelRows: forHotel.length,
      periodRows: 0,
      detail: "hotel_has_history_but_not_current_certified_period",
    };
  }
  // Presence of current period rows = at least INDEX present
  const currentFlags = forPeriod.filter(
    (r) =>
      r.fields?.["Is Current Historical Version"] === true ||
      r.fields?.["Is Current Historical Version"] === "true"
  );
  return {
    class: CLASS.MATCH,
    hotelRows: forHotel.length,
    periodRows: forPeriod.length,
    currentFlagRows: currentFlags.length,
  };
}

async function main() {
  const universe = loadDurableUniverse();
  const meta = await discoverTables();
  const tableNames = new Set((meta.tables || []).map((t) => t.name));

  const expectedHistory = listFullE2eMeasurementHistoryTables();
  const tablePresence = {
    publishedReports: tableNames.has(ADP_PUBLISHED_REPORTS_TABLE),
    history: {},
  };
  for (const t of expectedHistory) {
    const name = t.liveNameAliases?.[0] || t.tableName;
    tablePresence.history[name] = tableNames.has(name) || tableNames.has(t.tableName);
  }

  // Load Published Reports (all pages)
  const published = await airtableList(ADP_PUBLISHED_REPORTS_TABLE);
  const historyLoads = {};
  for (const name of Object.values(HISTORY_TABLES)) {
    if (!tableNames.has(name)) {
      historyLoads[name] = {
        ok: false,
        error: "TABLE_NOT_IN_BASE",
        records: [],
      };
      continue;
    }
    historyLoads[name] = await airtableList(name);
  }

  const hotels = [];
  for (const hotel of universe) {
    const pub = published.ok
      ? classifyPublishedOverlay(hotel, published.records)
      : {
          class: CLASS.LOOKUP_UNAVAILABLE,
          detail: published.error,
        };
    const history = {};
    for (const [name, load] of Object.entries(historyLoads)) {
      if (!load.ok) {
        history[name] = {
          class:
            load.error === "TABLE_NOT_IN_BASE"
              ? CLASS.MISSING
              : CLASS.LOOKUP_UNAVAILABLE,
          detail: load.error,
        };
        continue;
      }
      history[name] = classifyHistoryTable(hotel, name, load.records);
    }

    const historyClasses = Object.values(history).map((h) => h.class);
    const historyAllMissing = historyClasses.every(
      (c) => c === CLASS.MISSING || c === CLASS.LOOKUP_UNAVAILABLE
    );
    hotels.push({
      ...hotel,
      publishedReports: pub,
      history,
      overall:
        pub.class === CLASS.MATCH && !historyAllMissing
          ? CLASS.INCOMPLETE // Live ok but history incomplete is typical today
          : pub.class === CLASS.MATCH && historyAllMissing
            ? CLASS.INCOMPLETE
            : pub.class,
      historyCompleteness: historyAllMissing
        ? "NONE"
        : historyClasses.every((c) => c === CLASS.MATCH)
          ? "COMPLETE_FOR_CURRENT_PERIOD"
          : "PARTIAL",
    });
  }

  const summary = {
    contract: "ADP_38_AIRTABLE_RECONCILIATION_V1",
    ecosystemContract: ADP_AIRTABLE_ECOSYSTEM_CONTRACT_V1,
    generatedAt: new Date().toISOString(),
    baseId: baseId(),
    governedBaseId: ADP_GOVERNED_BASE_ID,
    persistencePolicy: getAdpPersistencePolicy(),
    historyWritesEnabled: isAdpHistoryWritesEnabled(),
    publishedReportsRole: EXISTING_LIVE_PUBLISH.role,
    metaOk: meta.ok,
    metaError: meta.error || null,
    discoveredTableCount: meta.tables?.length || 0,
    discoveredAdpNamedTables: (meta.tables || [])
      .filter((t) => /ADP|AI Demand Positioning/i.test(t.name))
      .map((t) => t.name),
    tablePresence,
    hotelCount: hotels.length,
    publishedReports: {
      loadOk: published.ok,
      error: published.error || null,
      totalRows: published.records?.length || 0,
      byClass: hotels.reduce((a, h) => {
        const c = h.publishedReports.class;
        a[c] = (a[c] || 0) + 1;
        return a;
      }, {}),
    },
    historyByTable: Object.fromEntries(
      Object.keys(HISTORY_TABLES).map((k) => {
        const name = HISTORY_TABLES[k];
        const load = historyLoads[name];
        return [
          name,
          {
            tablePresent: tablePresence.history[name] === true,
            loadOk: load?.ok === true,
            error: load?.error || null,
            totalRows: load?.records?.length || 0,
            hotelsMatch: hotels.filter((h) => h.history[name]?.class === CLASS.MATCH)
              .length,
            hotelsMissing: hotels.filter(
              (h) => h.history[name]?.class === CLASS.MISSING
            ).length,
            hotelsHistoricalOnly: hotels.filter(
              (h) => h.history[name]?.class === CLASS.HISTORICAL_ONLY
            ).length,
          },
        ];
      })
    ),
    authScopes: PERSISTENCE_AUTH_SCOPE,
    stopBeforeAirtableMutation: true,
    stopBeforeProductionDeploy: true,
  };

  fs.writeFileSync(
    path.join(OUT, "AIRTABLE_BASE_META.json"),
    `${JSON.stringify({ ok: meta.ok, baseId: baseId(), tables: meta.tables }, null, 2)}\n`
  );
  fs.writeFileSync(
    path.join(OUT, "RECONCILIATION_38.json"),
    `${JSON.stringify({ summary, hotels }, null, 2)}\n`
  );
  fs.writeFileSync(
    path.join(OUT, "RECONCILIATION_SUMMARY.json"),
    `${JSON.stringify(summary, null, 2)}\n`
  );

  console.log(
    JSON.stringify(
      {
        ok: meta.ok && published.ok !== false,
        hotelCount: hotels.length,
        publishedByClass: summary.publishedReports.byClass,
        historyTablesPresent: Object.values(tablePresence.history).filter(Boolean)
          .length,
        out: OUT,
      },
      null,
      2
    )
  );
}

main().catch((err) => {
  console.error(String(err?.stack || err));
  process.exit(1);
});
