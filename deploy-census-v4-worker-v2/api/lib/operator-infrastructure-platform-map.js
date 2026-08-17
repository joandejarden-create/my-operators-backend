/**
 * Operator Setup — Infrastructure Platform child table (Explorer Infrastructure & Data tab).
 * One linked child table per operator; `section` + optional `row_key` discriminate row shape.
 *
 * Table: Operator Setup - Infrastructure Platform
 * Override: AIRTABLE_OPERATOR_SETUP_INFRASTRUCTURE_PLATFORM_TABLE
 */

import { properCaseBody, properCaseExtra } from "./operator-leadership-platform-map.js";

export const INFRASTRUCTURE_PLATFORM_TABLE =
  process.env.AIRTABLE_OPERATOR_SETUP_INFRASTRUCTURE_PLATFORM_TABLE ||
  "Operator Setup - Infrastructure Platform";

/** Airtable single-select values — must match base options exactly. */
export const INFRASTRUCTURE_PLATFORM_SECTIONS = {
  decisionSignal: "Decision Signal",
  technologyStack: "Technology Stack",
  infrastructureService: "Infrastructure Service",
  dataDomain: "Data Domain",
  dataGovernance: "Data Governance",
  analyticsCapability: "Analytics Capability",
  technologyMaturity: "Technology Maturity",
  portfolioMetric: "Portfolio Metric",
};

export const MATURITY_LEVELS = ["Basic", "Structured", "Integrated", "Advanced"];

export const MATURITY_ROW_KEY = "technology_maturity";

/**
 * Allowed single-select values per signal — must match Setup form + bindings JSON.
 * Source: api/lib/third-party-operator-new-two-field-bindings.json
 */
export const DECISION_SIGNAL_SELECT_OPTIONS = {
  infra_signal_uptime: [
    "Not Measured / N/A",
    "99.99%+",
    "99.9%+",
    "99.5–99.89%",
    "99.0–99.49%",
    "98.0–98.9%",
    "Below 98%",
    "Varies by system",
  ],
  infra_signal_incident: [
    "Not Measured / N/A",
    "<15 min",
    "<30 min",
    "<1 hour",
    "<2 hours",
    "<4 hours",
    "<24 hours",
    "Tiered / Varies",
  ],
  infra_signal_adoption: [
    "Not Measured / N/A",
    "95%+",
    "90–94%",
    "85–89%",
    "80–84%",
    "70–79%",
    "Below 70%",
  ],
  infra_signal_refresh: [
    "Not Measured / N/A",
    "Real-time",
    "Hourly",
    "Daily",
    "Daily + Weekly",
    "Weekly",
    "Monthly",
    "Varies",
  ],
  risk_signal_audit: [
    "Not Measured / N/A",
    "Below 85%",
    "85–89%",
    "90–94%",
    "95%+",
    "99%+",
  ],
  risk_signal_bcp: [
    "Not Measured / N/A",
    "Monthly",
    "Quarterly",
    "Semi-Annual",
    "Annual",
    "Every 2+ Years",
    "Ad hoc / event-driven",
  ],
  risk_signal_control: [
    "Not Measured / N/A",
    "95%+ closed on time",
    "90–94% closed on time",
    "80–89% closed on time",
    "70–79% closed on time",
    "Below 70%",
  ],
  risk_signal_insurance: [
    "Not Measured / N/A",
    "Monthly",
    "Quarterly",
    "Semi-Annual",
    "Annual",
    "Biennial",
    "As needed / event-driven",
  ],
};

/** Stable keys for Explorer KPI snapshot + migration. */
export const DECISION_SIGNAL_SPECS = [
  {
    rowKey: "infra_signal_uptime",
    title: "Platform Uptime",
    legacyKeys: ["Platform Uptime Sla"],
  },
  {
    rowKey: "infra_signal_incident",
    title: "Critical Incident Response",
    legacyKeys: ["Critical Incident Response"],
  },
  {
    rowKey: "infra_signal_adoption",
    title: "System Adoption (Portfolio)",
    legacyKeys: ["System Adoption (Portfolio)"],
  },
  {
    rowKey: "infra_signal_refresh",
    title: "Data Refresh Cadence",
    legacyKeys: ["Data Refresh Cadence"],
  },
  {
    rowKey: "risk_signal_audit",
    title: "Audit Pass Consistency",
    legacyKeys: ["Audit Pass Consistency"],
  },
  {
    rowKey: "risk_signal_bcp",
    title: "BCP Test Frequency",
    legacyKeys: ["Bcp Test Frequency", "BCP Test Frequency"],
  },
  {
    rowKey: "risk_signal_control",
    title: "Control Closure Rate",
    legacyKeys: ["Control Closure Rate"],
  },
  {
    rowKey: "risk_signal_insurance",
    title: "Insurance Adequacy Review",
    legacyKeys: ["Insurance Adequacy Review"],
  },
];

export const PORTFOLIO_METRIC_SPECS = [
  {
    rowKey: "infra_kpi_reporting",
    title: "Reporting Systems",
    legacyKeys: ["Reporting Systems"],
  },
  {
    rowKey: "infra_kpi_revenue",
    title: "Revenue Systems",
    legacyKeys: ["Revenue Systems [infra_kpi_revenue]", "Revenue Systems"],
  },
  {
    rowKey: "infra_kpi_exec",
    title: "Execution Platform",
    legacyKeys: ["Execution Platform"],
  },
  {
    rowKey: "infra_kpi_tools",
    title: "Owner Tools",
    legacyKeys: ["Owner Tools"],
  },
];

const JSON_SECTION_SPECS = [
  {
    formKey: "infra_technology_stack_json",
    sectionKey: "technologyStack",
    mapItem: mapStackJsonItem,
  },
  {
    formKey: "infra_services_offered_json",
    sectionKey: "infrastructureService",
    mapItem: mapServiceJsonItem,
  },
  {
    formKey: "infra_data_domains_json",
    sectionKey: "dataDomain",
    mapItem: mapDomainJsonItem,
  },
  {
    formKey: "infra_data_governance_json",
    sectionKey: "dataGovernance",
    mapItem: mapGovernanceJsonItem,
  },
  {
    formKey: "infra_analytics_support_json",
    sectionKey: "analyticsCapability",
    mapItem: mapAnalyticsJsonItem,
  },
];

export const MAP_INFRASTRUCTURE_PLATFORM = {
  section: "section",
  rowKey: "row_key",
  displayOrder: "display_order",
  title: "title",
  subtitle: "subtitle",
  body: "body",
  extra: "extra",
};

const SECTION_BY_API_KEY = Object.fromEntries(
  Object.entries(INFRASTRUCTURE_PLATFORM_SECTIONS).map(([k, v]) => [k, v])
);

const SECTION_KEY_FROM_LABEL = Object.fromEntries(
  Object.entries(INFRASTRUCTURE_PLATFORM_SECTIONS).map(([k, v]) => [v, k])
);

const NOT_MEASURED = /^not measured\s*\/\s*n\/?a$/i;

function nz(v) {
  return v != null && String(v).trim() !== "" ? String(v).trim() : "";
}

/** Decision signals, portfolio KPIs, and maturity level must match form/Airtable options exactly. */
function preservesExactExtraValue(sectionKey) {
  return (
    sectionKey === "decisionSignal" ||
    sectionKey === "portfolioMetric" ||
    sectionKey === "technologyMaturity"
  );
}

function formatBodyForSection(sectionKey, raw) {
  const s = nz(raw);
  if (!s) return "";
  if (preservesExactExtraValue(sectionKey)) return s;
  if (sectionKey === "dataDomain" && s.includes("\n")) {
    return s
      .split(/\n+/)
      .map((line) => properCaseBody(line))
      .filter(Boolean)
      .join("\n");
  }
  return properCaseBody(s);
}

function formatExtraForSection(sectionKey, raw) {
  const s = nz(raw);
  if (!s) return "";
  if (preservesExactExtraValue(sectionKey)) return s;
  return properCaseExtra(s);
}

function meaningfulValue(v) {
  const s = nz(v);
  if (!s) return false;
  if (NOT_MEASURED.test(s)) return false;
  return true;
}

function parseJsonArray(raw) {
  if (raw == null || raw === "") return null;
  if (Array.isArray(raw)) return raw;
  try {
    const parsed = JSON.parse(String(raw));
    return Array.isArray(parsed) ? parsed : null;
  } catch {
    return null;
  }
}

/** Read legacy Governance field by form key and known Airtable column titles. */
export function pickLegacyGovernanceField(fields, formKey, legacyKeys = []) {
  const f = fields || {};
  const keys = [formKey, ...(legacyKeys || [])];
  for (const k of keys) {
    const v = f[k];
    if (v == null) continue;
    if (Array.isArray(v)) {
      if (v.length) return v;
      continue;
    }
    const s = nz(v);
    if (s) return s;
  }
  return "";
}

function sectionLabel(sectionKey) {
  return SECTION_BY_API_KEY[sectionKey] || "";
}

function sectionKeyFromRowFields(f) {
  const raw = nz(f[MAP_INFRASTRUCTURE_PLATFORM.section]);
  return SECTION_KEY_FROM_LABEL[raw] || "";
}

function baseRow(sectionKey, displayOrder, patch = {}) {
  return {
    [MAP_INFRASTRUCTURE_PLATFORM.section]: sectionLabel(sectionKey),
    [MAP_INFRASTRUCTURE_PLATFORM.displayOrder]: displayOrder,
    ...patch,
  };
}

function mapStackJsonItem(item, displayOrder) {
  if (!item || !nz(item.title)) return null;
  return baseRow("technologyStack", displayOrder, {
    [MAP_INFRASTRUCTURE_PLATFORM.title]: nz(item.title),
    [MAP_INFRASTRUCTURE_PLATFORM.body]: formatBodyForSection("technologyStack", item.description),
    [MAP_INFRASTRUCTURE_PLATFORM.extra]: formatExtraForSection("technologyStack", item.examples),
  });
}

function mapServiceJsonItem(item, displayOrder) {
  if (!item || !nz(item.title)) return null;
  return baseRow("infrastructureService", displayOrder, {
    [MAP_INFRASTRUCTURE_PLATFORM.title]: nz(item.title),
    [MAP_INFRASTRUCTURE_PLATFORM.body]: formatBodyForSection("infrastructureService", item.description),
  });
}

function mapDomainJsonItem(item, displayOrder) {
  if (!item || !nz(item.title)) return null;
  const items = Array.isArray(item.items)
    ? item.items.map((x) => formatBodyForSection("dataDomain", x)).filter(Boolean)
    : nz(item.description)
        .split(/\n+/)
        .map((line) => formatBodyForSection("dataDomain", line))
        .filter(Boolean);
  return baseRow("dataDomain", displayOrder, {
    [MAP_INFRASTRUCTURE_PLATFORM.title]: nz(item.title),
    [MAP_INFRASTRUCTURE_PLATFORM.body]: items.join("\n"),
  });
}

function mapGovernanceJsonItem(item, displayOrder) {
  if (!item || !nz(item.title)) return null;
  return baseRow("dataGovernance", displayOrder, {
    [MAP_INFRASTRUCTURE_PLATFORM.title]: nz(item.title),
    [MAP_INFRASTRUCTURE_PLATFORM.body]: formatBodyForSection("dataGovernance", item.description),
  });
}

function mapAnalyticsJsonItem(item, displayOrder) {
  if (!item || !nz(item.title)) return null;
  return baseRow("analyticsCapability", displayOrder, {
    [MAP_INFRASTRUCTURE_PLATFORM.title]: nz(item.title),
    [MAP_INFRASTRUCTURE_PLATFORM.body]: formatBodyForSection("analyticsCapability", item.description),
  });
}

function parseSystemsTechnologyLines(text, startOrder) {
  const lines = String(text || "")
    .split(/\n+/)
    .map((l) => l.trim())
    .filter(Boolean);
  const out = [];
  let order = startOrder;
  lines.forEach((line, i) => {
    const m = line.match(/^([^:]+):\s*(.+)$/);
    if (m) {
      out.push(
        baseRow("technologyStack", order++, {
          [MAP_INFRASTRUCTURE_PLATFORM.rowKey]: `stack_systems_${i + 1}`,
          [MAP_INFRASTRUCTURE_PLATFORM.title]: m[1].trim(),
          [MAP_INFRASTRUCTURE_PLATFORM.extra]: formatExtraForSection("technologyStack", m[2].trim()),
        })
      );
      return;
    }
    out.push(
      baseRow("technologyStack", order++, {
        [MAP_INFRASTRUCTURE_PLATFORM.rowKey]: `stack_systems_${i + 1}`,
        [MAP_INFRASTRUCTURE_PLATFORM.title]: line,
      })
    );
  });
  return out;
}

function parseInventoryLines(text, startOrder) {
  const lines = String(text || "")
    .split(/\n+/)
    .map((l) => l.trim())
    .filter(Boolean);
  const out = [];
  let order = startOrder;
  lines.forEach((line, i) => {
    out.push(
      baseRow("technologyStack", order++, {
        [MAP_INFRASTRUCTURE_PLATFORM.rowKey]: `stack_inventory_${i + 1}`,
        [MAP_INFRASTRUCTURE_PLATFORM.title]: line,
      })
    );
  });
  return out;
}

function parseReportingNarrativeLines(text, startOrder) {
  const lines = String(text || "")
    .split(/\n+/)
    .map((l) => l.trim())
    .filter(Boolean);
  if (!lines.length) return [];
  return [
    baseRow("dataDomain", startOrder, {
      [MAP_INFRASTRUCTURE_PLATFORM.rowKey]: "owner_reporting_rhythm",
      [MAP_INFRASTRUCTURE_PLATFORM.title]: "Owner Reporting & Asset Management",
      [MAP_INFRASTRUCTURE_PLATFORM.body]: formatBodyForSection(
        "dataDomain",
        lines.join("\n")
      ),
    }),
  ];
}

/**
 * Build Airtable child-row field objects from a Governance row (migration + legacy import).
 * @param {Record<string, unknown>} governanceFields
 * @returns {{ rows: object[], sources: string[], countsBySection: Record<string, number> }}
 */
export function buildInfrastructurePlatformAirtableRowsFromLegacyGovernance(governanceFields) {
  const fields = governanceFields || {};
  const rows = [];
  const sources = [];
  let order = 1;

  for (const spec of DECISION_SIGNAL_SPECS) {
    const value = pickLegacyGovernanceField(fields, spec.rowKey, spec.legacyKeys);
    if (!meaningfulValue(value)) continue;
    rows.push(
      baseRow("decisionSignal", order++, {
        [MAP_INFRASTRUCTURE_PLATFORM.rowKey]: spec.rowKey,
        [MAP_INFRASTRUCTURE_PLATFORM.title]: spec.title,
        [MAP_INFRASTRUCTURE_PLATFORM.extra]: value,
      })
    );
    sources.push(spec.rowKey);
  }

  for (const spec of PORTFOLIO_METRIC_SPECS) {
    const value = pickLegacyGovernanceField(fields, spec.rowKey, spec.legacyKeys);
    if (!meaningfulValue(String(value))) continue;
    rows.push(
      baseRow("portfolioMetric", order++, {
        [MAP_INFRASTRUCTURE_PLATFORM.rowKey]: spec.rowKey,
        [MAP_INFRASTRUCTURE_PLATFORM.title]: spec.title,
        [MAP_INFRASTRUCTURE_PLATFORM.extra]: String(value),
      })
    );
    sources.push(spec.rowKey);
  }

  const maturity = pickLegacyGovernanceField(fields, "infra_technology_maturity_level", [
    "infra_technology_maturity_level",
  ]);
  if (meaningfulValue(maturity) && MATURITY_LEVELS.includes(maturity)) {
    rows.push(
      baseRow("technologyMaturity", order++, {
        [MAP_INFRASTRUCTURE_PLATFORM.rowKey]: MATURITY_ROW_KEY,
        [MAP_INFRASTRUCTURE_PLATFORM.title]: "Current Maturity Level",
        [MAP_INFRASTRUCTURE_PLATFORM.extra]: maturity,
      })
    );
    sources.push("infra_technology_maturity_level");
  }

  let hasStackJson = false;
  for (const spec of JSON_SECTION_SPECS) {
    const raw = pickLegacyGovernanceField(fields, spec.formKey, [spec.formKey]);
    const parsed = parseJsonArray(raw);
    if (!parsed || !parsed.length) continue;
    if (spec.sectionKey === "technologyStack") hasStackJson = true;
    parsed.forEach((item) => {
      const mapped = spec.mapItem(item, order++);
      if (mapped) rows.push(mapped);
    });
    sources.push(spec.formKey);
  }

  if (!hasStackJson) {
    const systemsText = pickLegacyGovernanceField(fields, "infra_systems_technology", [
      "Systems & Technology",
    ]);
    if (systemsText) {
      const stackRows = parseSystemsTechnologyLines(systemsText, order);
      if (stackRows.length) {
        order += stackRows.length;
        rows.push(...stackRows);
        sources.push("infra_systems_technology");
      }
    }
  }

  const inventoryText = pickLegacyGovernanceField(fields, "systems_inventory_lines", [
    "Line-Item Systems & Integrations",
  ]);
  if (inventoryText) {
    const invRows = parseInventoryLines(inventoryText, order);
    if (invRows.length) {
      order += invRows.length;
      rows.push(...invRows);
      sources.push("systems_inventory_lines");
    }
  }

  const reportingText = pickLegacyGovernanceField(fields, "infra_asset_management_reporting", [
    "Asset Management & Reporting",
  ]);
  if (reportingText) {
    const repRows = parseReportingNarrativeLines(reportingText, order);
    if (repRows.length) {
      order += repRows.length;
      rows.push(...repRows);
      sources.push("infra_asset_management_reporting");
    }
  }

  const countsBySection = {};
  rows.forEach((row) => {
    const label = row[MAP_INFRASTRUCTURE_PLATFORM.section] || "unknown";
    countsBySection[label] = (countsBySection[label] || 0) + 1;
  });

  return { rows, sources, countsBySection };
}

/** Airtable child row → grouped Explorer / Setup shape. */
export function mapInfrastructurePlatformRowFromAirtable(record) {
  const f = (record && record.fields) || record || {};
  const sectionKey = sectionKeyFromRowFields(f);
  if (!sectionKey) return null;

  const displayOrder = Number(f[MAP_INFRASTRUCTURE_PLATFORM.displayOrder]) || 0;
  const rowKey = nz(f[MAP_INFRASTRUCTURE_PLATFORM.rowKey]);
  const title = nz(f[MAP_INFRASTRUCTURE_PLATFORM.title]);
  const subtitle = nz(f[MAP_INFRASTRUCTURE_PLATFORM.subtitle]);
  const body = formatBodyForSection(sectionKey, f[MAP_INFRASTRUCTURE_PLATFORM.body]);
  const extra = formatExtraForSection(sectionKey, f[MAP_INFRASTRUCTURE_PLATFORM.extra]);

  if (sectionKey === "decisionSignal") {
    return {
      sectionKey,
      displayOrder,
      rowKey,
      explorer: { rowKey, title, value: extra, note: body },
    };
  }
  if (sectionKey === "portfolioMetric") {
    return {
      sectionKey,
      displayOrder,
      rowKey,
      explorer: { rowKey, title, value: extra },
    };
  }
  if (sectionKey === "technologyStack") {
    return {
      sectionKey,
      displayOrder,
      rowKey,
      explorer: { title, description: body, examples: extra },
    };
  }
  if (sectionKey === "infrastructureService") {
    return {
      sectionKey,
      displayOrder,
      rowKey,
      explorer: { title, description: body },
    };
  }
  if (sectionKey === "dataDomain") {
    const items = body
      .split(/\n+/)
      .map(nz)
      .filter(Boolean);
    return {
      sectionKey,
      displayOrder,
      rowKey,
      explorer: { title, items, description: body },
    };
  }
  if (sectionKey === "dataGovernance" || sectionKey === "analyticsCapability") {
    return {
      sectionKey,
      displayOrder,
      rowKey,
      explorer: { title, description: body },
    };
  }
  if (sectionKey === "technologyMaturity") {
    return {
      sectionKey,
      displayOrder,
      rowKey,
      explorer: { level: extra, summary: body, title },
    };
  }
  return null;
}

/**
 * @param {import("airtable").Records<any>} rows
 */
export function mapInfrastructurePlatformRowsForDetail(rows) {
  const buckets = {
    decisionSignals: [],
    technologyStack: [],
    infrastructureServices: [],
    dataDomains: [],
    dataGovernance: [],
    analyticsCapabilities: [],
    technologyMaturity: null,
    portfolioMetrics: [],
  };

  const bucketKey = {
    decisionSignal: "decisionSignals",
    technologyStack: "technologyStack",
    infrastructureService: "infrastructureServices",
    dataDomain: "dataDomains",
    dataGovernance: "dataGovernance",
    analyticsCapability: "analyticsCapabilities",
    portfolioMetric: "portfolioMetrics",
  };

  (rows || [])
    .map(mapInfrastructurePlatformRowFromAirtable)
    .filter(Boolean)
    .sort((a, b) => a.displayOrder - b.displayOrder)
    .forEach((mapped) => {
      if (mapped.sectionKey === "technologyMaturity") {
        buckets.technologyMaturity = mapped.explorer;
        return;
      }
      const key = bucketKey[mapped.sectionKey];
      if (key && mapped.explorer) buckets[key].push(mapped.explorer);
    });

  return buckets;
}

/** Form/API payload → Airtable child fields (Setup save — Phase 4+). */
export function mapInfrastructurePlatformRowToAirtable(row, displayOrder) {
  const sectionKey = nz(row.sectionKey || row.section);
  const section = sectionLabel(sectionKey);
  if (!section) return null;

  const out = {
    [MAP_INFRASTRUCTURE_PLATFORM.section]: section,
    [MAP_INFRASTRUCTURE_PLATFORM.displayOrder]: displayOrder,
  };

  if (nz(row.rowKey)) out[MAP_INFRASTRUCTURE_PLATFORM.rowKey] = nz(row.rowKey);
  if (nz(row.title)) out[MAP_INFRASTRUCTURE_PLATFORM.title] = nz(row.title);
  if (nz(row.subtitle)) out[MAP_INFRASTRUCTURE_PLATFORM.subtitle] = nz(row.subtitle);
  if (nz(row.body)) {
    out[MAP_INFRASTRUCTURE_PLATFORM.body] = formatBodyForSection(sectionKey, row.body);
  }
  if (nz(row.extra)) {
    out[MAP_INFRASTRUCTURE_PLATFORM.extra] = formatExtraForSection(sectionKey, row.extra);
  }
  if (nz(row.description)) {
    out[MAP_INFRASTRUCTURE_PLATFORM.body] = formatBodyForSection(sectionKey, row.description);
  }
  if (nz(row.examples)) {
    out[MAP_INFRASTRUCTURE_PLATFORM.extra] = formatExtraForSection(sectionKey, row.examples);
  }
  if (nz(row.value)) {
    out[MAP_INFRASTRUCTURE_PLATFORM.extra] = formatExtraForSection(sectionKey, row.value);
  }
  if (nz(row.level)) {
    out[MAP_INFRASTRUCTURE_PLATFORM.extra] = formatExtraForSection(sectionKey, row.level);
  }

  if (sectionKey === "dataDomain" && Array.isArray(row.items) && row.items.length) {
    out[MAP_INFRASTRUCTURE_PLATFORM.body] = row.items
      .map((item) => formatBodyForSection("dataDomain", item))
      .filter(Boolean)
      .join("\n");
  }

  return out;
}

/**
 * @param {object} body - intake body with infrastructurePlatform object
 */
export function buildInfrastructurePlatformAirtableRows(body) {
  const ip = body && body.infrastructurePlatform;
  if (!ip || typeof ip !== "object") return [];

  const sections = [
    ["decisionSignal", ip.decisionSignals],
    ["technologyStack", ip.technologyStack],
    ["infrastructureService", ip.infrastructureServices],
    ["dataDomain", ip.dataDomains],
    ["dataGovernance", ip.dataGovernance],
    ["analyticsCapability", ip.analyticsCapabilities],
    ["portfolioMetric", ip.portfolioMetrics],
  ];

  const out = [];
  let order = 1;

  sections.forEach(([sectionKey, list]) => {
    if (!Array.isArray(list)) return;
    list.forEach((row) => {
      const mapped = mapInfrastructurePlatformRowToAirtable({ ...row, sectionKey }, order++);
      if (mapped) out.push(mapped);
    });
  });

  if (ip.technologyMaturity && typeof ip.technologyMaturity === "object") {
    const tm = ip.technologyMaturity;
    const mapped = mapInfrastructurePlatformRowToAirtable(
      {
        sectionKey: "technologyMaturity",
        rowKey: MATURITY_ROW_KEY,
        title: "Current Maturity Level",
        level: tm.level,
        body: tm.summary,
        description: tm.summary,
      },
      order++
    );
    if (mapped) out.push(mapped);
  }

  return out;
}

export class InfrastructurePlatformValidationError extends Error {
  /** @param {{ field: string, message: string }[]} errors */
  constructor(errors) {
    super("Infrastructure platform validation failed");
    this.name = "InfrastructurePlatformValidationError";
    this.errors = errors;
    this.statusCode = 400;
  }
}

/**
 * Validate structured Infrastructure Platform payload before child-table write.
 * @param {object} ip
 * @param {{ strictSignals?: boolean }} [opts]
 */
export function validateInfrastructurePlatformPayload(ip, opts = {}) {
  const strictSignals = opts.strictSignals !== false;
  const errors = [];
  const platform = ip && typeof ip === "object" ? ip : {};

  if (strictSignals) {
    (platform.decisionSignals || []).forEach((sig) => {
      const rowKey = nz(sig?.rowKey);
      const value = nz(sig?.value);
      if (!rowKey || !value || NOT_MEASURED.test(value)) return;
      const allowed = DECISION_SIGNAL_SELECT_OPTIONS[rowKey];
      if (allowed && !allowed.includes(value)) {
        errors.push({
          field: rowKey,
          message: `Invalid option "${value}" for ${rowKey}`,
        });
      }
    });
  }

  const maturity = platform.technologyMaturity;
  if (maturity && nz(maturity.level) && !MATURITY_LEVELS.includes(nz(maturity.level))) {
    errors.push({
      field: "infra_technology_maturity_level",
      message: `Invalid maturity level "${maturity.level}"`,
    });
  }

  return { valid: errors.length === 0, errors };
}

function linesToExplorerTechnologyStack(text) {
  return String(text || "")
    .split(/\n+/)
    .map((l) => l.trim())
    .filter(Boolean)
    .map((line) => {
      const m = line.match(/^([^:]+):\s*(.+)$/);
      if (m) {
        return { title: m[1].trim(), description: "", examples: m[2].trim() };
      }
      return { title: line, description: "", examples: "" };
    });
}

function linesToExplorerStackInventory(text) {
  return String(text || "")
    .split(/\n+/)
    .map((l) => l.trim())
    .filter(Boolean)
    .map((line) => ({ title: line, description: "", examples: "" }));
}

function reportingLinesToDataDomains(text) {
  const lines = String(text || "")
    .split(/\n+/)
    .map((l) => l.trim())
    .filter(Boolean);
  if (!lines.length) return [];
  return [
    {
      title: "Owner Reporting & Asset Management",
      items: lines,
      description: lines.join("\n"),
    },
  ];
}

function jsonItemsOrEmpty(raw) {
  const parsed = parseJsonArray(raw);
  return parsed && parsed.length ? parsed : [];
}

function nonEmptyRepeaterRows(list, titleKey = "title") {
  return (Array.isArray(list) ? list : []).filter((row) => row && nz(row[titleKey]));
}

/**
 * Build structured `infrastructurePlatform` from Setup intake body (flat fields + optional repeater object).
 * @param {object} body
 */
export function buildInfrastructurePlatformPayloadFromIntakeBody(body) {
  const b = body || {};
  const incoming =
    b.infrastructurePlatform && typeof b.infrastructurePlatform === "object" ? b.infrastructurePlatform : {};

  const decisionSignals = [];
  for (const spec of DECISION_SIGNAL_SPECS) {
    const fromIncoming = (incoming.decisionSignals || []).find((s) => nz(s?.rowKey) === spec.rowKey);
    const value = nz(fromIncoming?.value ?? pickLegacyGovernanceField(b, spec.rowKey, spec.legacyKeys));
    if (!meaningfulValue(value)) continue;
    decisionSignals.push({ rowKey: spec.rowKey, title: spec.title, value });
  }

  const portfolioMetrics = [];
  for (const spec of PORTFOLIO_METRIC_SPECS) {
    const fromIncoming = (incoming.portfolioMetrics || []).find((s) => nz(s?.rowKey) === spec.rowKey);
    const value = nz(fromIncoming?.value ?? pickLegacyGovernanceField(b, spec.rowKey, spec.legacyKeys));
    if (!meaningfulValue(String(value))) continue;
    portfolioMetrics.push({ rowKey: spec.rowKey, title: spec.title, value: String(value) });
  }

  let technologyStack = nonEmptyRepeaterRows(incoming.technologyStack);
  if (!technologyStack.length) {
    const jsonStack = jsonItemsOrEmpty(b.infra_technology_stack_json);
    technologyStack = jsonStack.length
      ? jsonStack
      : [
          ...linesToExplorerTechnologyStack(b.infra_systems_technology),
          ...linesToExplorerStackInventory(b.systems_inventory_lines),
        ];
  }

  let infrastructureServices = nonEmptyRepeaterRows(incoming.infrastructureServices);
  if (!infrastructureServices.length) {
    infrastructureServices = jsonItemsOrEmpty(b.infra_services_offered_json);
  }

  let dataDomains = nonEmptyRepeaterRows(incoming.dataDomains);
  if (!dataDomains.length) {
    const jsonDomains = jsonItemsOrEmpty(b.infra_data_domains_json);
    dataDomains = jsonDomains.length
      ? jsonDomains.map((d) => ({
          title: nz(d.title),
          items: Array.isArray(d.items) ? d.items.map(nz).filter(Boolean) : [],
          description: nz(d.description),
        }))
      : reportingLinesToDataDomains(b.infra_asset_management_reporting);
  }

  let dataGovernance = nonEmptyRepeaterRows(incoming.dataGovernance);
  if (!dataGovernance.length) {
    dataGovernance = jsonItemsOrEmpty(b.infra_data_governance_json);
  }

  let analyticsCapabilities = nonEmptyRepeaterRows(incoming.analyticsCapabilities);
  if (!analyticsCapabilities.length) {
    analyticsCapabilities = jsonItemsOrEmpty(b.infra_analytics_support_json);
  }

  let technologyMaturity = null;
  const incomingMaturity =
    incoming.technologyMaturity && typeof incoming.technologyMaturity === "object"
      ? incoming.technologyMaturity
      : null;
  const maturityLevel = nz(b.infra_technology_maturity_level || incomingMaturity?.level);
  if (meaningfulValue(maturityLevel) && MATURITY_LEVELS.includes(maturityLevel)) {
    technologyMaturity = {
      level: maturityLevel,
      summary: nz(incomingMaturity?.summary),
    };
  }

  return {
    decisionSignals,
    portfolioMetrics,
    technologyStack,
    infrastructureServices,
    dataDomains,
    dataGovernance,
    analyticsCapabilities,
    technologyMaturity,
  };
}

/** Mirror child-table data onto legacy Governance prefill keys (read-path compat — Phase 2). */
export function applyInfrastructurePlatformToLegacyPrefill(prefill, platform) {
  if (!prefill || !platform) return prefill;

  (platform.decisionSignals || []).forEach((sig) => {
    const key = nz(sig.rowKey);
    const val = nz(sig.value);
    if (key && val) prefill[key] = val;
  });

  (platform.portfolioMetrics || []).forEach((metric) => {
    const key = nz(metric.rowKey);
    const val = nz(metric.value);
    if (key && val) prefill[key] = val;
  });

  if (platform.technologyStack && platform.technologyStack.length) {
    prefill.infra_technology_stack_json = JSON.stringify(platform.technologyStack);
  }
  if (platform.infrastructureServices && platform.infrastructureServices.length) {
    prefill.infra_services_offered_json = JSON.stringify(platform.infrastructureServices);
  }
  if (platform.dataDomains && platform.dataDomains.length) {
    prefill.infra_data_domains_json = JSON.stringify(
      platform.dataDomains.map((d) => ({
        title: d.title,
        items: d.items || [],
      }))
    );
  }
  if (platform.dataGovernance && platform.dataGovernance.length) {
    prefill.infra_data_governance_json = JSON.stringify(platform.dataGovernance);
  }
  if (platform.analyticsCapabilities && platform.analyticsCapabilities.length) {
    prefill.infra_analytics_support_json = JSON.stringify(platform.analyticsCapabilities);
  }
  if (platform.technologyMaturity && platform.technologyMaturity.level) {
    prefill.infra_technology_maturity_level = platform.technologyMaturity.level;
  }

  prefill.infrastructurePlatform = platform;
  return prefill;
}
