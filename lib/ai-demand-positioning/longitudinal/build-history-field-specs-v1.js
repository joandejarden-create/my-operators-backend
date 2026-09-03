/**
 * Merged ADP history field specs = Core HISTORY_FIELD_SPECS + BPP extensions.
 * Used by ensure-adp-history-schema (sandbox activation).
 */

import {
  HISTORY_TABLES,
  HISTORY_FIELD_SPECS,
  PERSISTENCE_STATES,
} from "./airtable-history-schema-final-v1.js";
import {
  MEASUREMENT_FAMILY,
  BPP_HISTORY_FIELD_EXTENSIONS,
} from "../brand-portfolio/bpp-history-schema-v1.js";

function choices(names) {
  return (names || []).map((name) => ({ name: String(name) }));
}

function withAirtableOptions(spec) {
  const out = { name: spec.name, type: spec.type, primary: Boolean(spec.primary) };
  if (spec.description) out.description = spec.description;

  if (spec.type === "singleSelect") {
    const choiceNames = spec.choices || spec.options?.choices?.map((c) => c.name || c) || [];
    out.options = { choices: choices(choiceNames) };
  } else if (spec.type === "number") {
    out.options = { precision: spec.precision ?? 2 };
  } else if (spec.type === "checkbox") {
    out.options = { icon: "check", color: "greenBright" };
  } else if (spec.type === "date") {
    out.options = { dateFormat: { name: "iso" } };
  } else if (spec.type === "dateTime") {
    out.options = {
      dateFormat: { name: "iso" },
      timeFormat: { name: "24hour" },
      timeZone: "utc",
    };
  }
  return out;
}

function mergeByName(baseSpecs, extensions) {
  const map = new Map();
  for (const s of baseSpecs || []) map.set(s.name, { ...s });
  for (const s of extensions || []) {
    if (map.has(s.name)) {
      const existing = map.get(s.name);
      // Merge singleSelect choices if both define them
      if (existing.type === "singleSelect" && s.type === "singleSelect") {
        const a = new Set([
          ...(existing.choices || []),
          ...(existing.options?.choices?.map((c) => c.name || c) || []),
          ...(s.choices || []),
          ...(s.options?.choices?.map((c) => c.name || c) || []),
        ]);
        map.set(s.name, { ...existing, ...s, choices: [...a] });
      } else {
        map.set(s.name, { ...existing, ...s });
      }
    } else {
      map.set(s.name, { ...s });
    }
  }
  return [...map.values()];
}

/** Extra fields required by founder sandbox activation but not yet on base Core specs. */
const EXTRA_EXTENSIONS = Object.freeze({
  [HISTORY_TABLES.PERIOD_METRICS]: [
    { name: "Metrics Version", type: "singleLineText" },
    { name: "Customer Publication Version", type: "singleLineText" },
  ],
  [HISTORY_TABLES.REPORT_SNAPSHOTS]: [
    { name: "Metrics Version", type: "singleLineText" },
  ],
  [HISTORY_TABLES.PROMPT_LEDGER]: [
    {
      name: "Scenario Class",
      type: "singleSelect",
      choices: [
        "NEUTRAL_DEMAND",
        "PROPERTY_SPECIFIC",
        "BRAND_SPECIFIC",
        "BRAND_PORTFOLIO_DEMAND",
        "COMPETITOR_SPECIFIC",
        "OTHER_GOVERNED_SPECIAL",
      ],
    },
  ],
});

export function buildMergedHistoryFieldSpecs() {
  const result = {};
  for (const tableName of Object.values(HISTORY_TABLES)) {
    const merged = mergeByName(
      HISTORY_FIELD_SPECS[tableName] || [],
      [
        ...(BPP_HISTORY_FIELD_EXTENSIONS[tableName] || []),
        ...(EXTRA_EXTENSIONS[tableName] || []),
      ]
    );
    // Ensure Persistence State choices on monitoring periods
    result[tableName] = merged.map((s) => {
      if (s.name === "Persistence State" && !s.choices) {
        return withAirtableOptions({ ...s, choices: Object.values(PERSISTENCE_STATES) });
      }
      if (s.name === "Measurement Family" && !s.choices) {
        return withAirtableOptions({ ...s, choices: Object.values(MEASUREMENT_FAMILY) });
      }
      if (s.name === "Ranking Universe Type" && !s.choices) {
        return withAirtableOptions({
          ...s,
          choices: ["MARKET_CORE", "BRAND_PORTFOLIO"],
        });
      }
      return withAirtableOptions(s);
    });
  }
  return result;
}

export function getPrimaryFieldName(specs) {
  const primary = (specs || []).find((s) => s.primary);
  return primary?.name || specs?.[0]?.name || "Name";
}

export { HISTORY_TABLES, MEASUREMENT_FAMILY };
