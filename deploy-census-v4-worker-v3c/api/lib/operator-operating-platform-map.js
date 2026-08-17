/**
 * Operator Setup — Operating Platform child table (Explorer Operating Platform tab).
 * Capability tiles per subsection — no JSON blobs.
 *
 * Table: Operator Setup - Operating Platform
 */

import { properCaseBody, properCaseExtra } from "./operator-leadership-platform-map.js";

export const OPERATING_PLATFORM_TABLE =
  process.env.AIRTABLE_OPERATOR_SETUP_OPERATING_PLATFORM_TABLE ||
  "Operator Setup - Operating Platform";

export const OPERATING_PLATFORM_ROW_TYPES = {
  pillarIntro: "Pillar Intro",
  capability: "Capability",
  kpiLevel: "KPI Level",
  performanceSignal: "Performance Signal",
  positioningCard: "Positioning Card",
};

export const OPERATING_PLATFORM_SECTIONS = {
  platformSnapshot: "Platform Snapshot",
  operatingSignals: "Operating Signals",
  platformPositioning: "Platform Positioning",
  commercialEngine: "Commercial Engine",
  ownerReporting: "Owner Reporting & Communication",
  preOpeningTransition: "Pre-Opening & Transition Support",
  conversionRepositioning: "Conversion & Repositioning",
  fbLifestyleResort: "F&B, Lifestyle & Resort",
  operationalExecutionLabor: "Operational Execution & Labor",
  procurementCostControl: "Procurement & Cost Control",
  salesMarketingActivation: "Sales & Marketing Activation",
  engineeringPropertyCare: "Engineering & Property Care",
  portfolioMultiProperty: "Portfolio & Multi-Property Management",
  technologyEnabledOperations: "Technology-Enabled Operations",
};

/** Explorer pillar id (operator-operating-platform-sections.js) → API section key */
export const PILLAR_UI_ID_TO_SECTION_KEY = {
  commercial: "commercialEngine",
  reporting: "ownerReporting",
  preopening: "preOpeningTransition",
  conversion: "conversionRepositioning",
  fb: "fbLifestyleResort",
  operational: "operationalExecutionLabor",
  procurement: "procurementCostControl",
  sales: "salesMarketingActivation",
  engineering: "engineeringPropertyCare",
  portfolio: "portfolioMultiProperty",
  technology: "technologyEnabledOperations",
};

export const MAP_OPERATING_PLATFORM = {
  section: "section",
  rowType: "row_type",
  rowKey: "row_key",
  displayOrder: "display_order",
  title: "title",
  subtitle: "subtitle",
  body: "body",
  extra: "extra",
};

const SECTION_BY_API_KEY = Object.fromEntries(
  Object.entries(OPERATING_PLATFORM_SECTIONS).map(([k, v]) => [k, v])
);
const SECTION_KEY_BY_LABEL = Object.fromEntries(
  Object.entries(OPERATING_PLATFORM_SECTIONS).map(([k, v]) => [v, k])
);
const ROW_TYPE_KEY_BY_LABEL = Object.fromEntries(
  Object.entries(OPERATING_PLATFORM_ROW_TYPES).map(([k, v]) => [v, k])
);

function nz(v) {
  return v != null && String(v).trim() !== "" ? String(v).trim() : "";
}

const NOT_MEASURED = /^not\s+measured/i;

/** KPI / signal `extra` must match Setup select options exactly (e.g. Not Measured / N/A, Within ±3.5%). */
function preservesExactOperatingValue(rowTypeKey) {
  return rowTypeKey === "kpiLevel" || rowTypeKey === "performanceSignal";
}

function formatOperatingPlatformBody(rowTypeKey, raw) {
  const s = nz(raw);
  if (!s) return "";
  if (preservesExactOperatingValue(rowTypeKey) || NOT_MEASURED.test(s)) return s;
  if (s.includes("\n")) {
    return s
      .split(/\n+/)
      .map((line) => properCaseBody(line))
      .filter(Boolean)
      .join("\n");
  }
  return properCaseBody(s);
}

function formatOperatingPlatformExtra(rowTypeKey, raw) {
  const s = nz(raw);
  if (!s) return "";
  if (preservesExactOperatingValue(rowTypeKey) || NOT_MEASURED.test(s)) return s;
  return properCaseExtra(s);
}

function parseJsonArray(raw) {
  if (raw == null || raw === "") return [];
  if (Array.isArray(raw)) return raw;
  try {
    const parsed = JSON.parse(String(raw));
    return Array.isArray(parsed) ? parsed : [];
  } catch {
    return [];
  }
}

function parseJsonObject(raw) {
  if (raw == null || raw === "") return null;
  if (typeof raw === "object" && !Array.isArray(raw)) return raw;
  try {
    const parsed = JSON.parse(String(raw));
    return parsed && typeof parsed === "object" && !Array.isArray(parsed) ? parsed : null;
  } catch {
    return null;
  }
}

function sectionLabel(sectionKey) {
  return SECTION_BY_API_KEY[nz(sectionKey)] || "";
}

function rowTypeLabel(rowTypeKey) {
  return OPERATING_PLATFORM_ROW_TYPES[nz(rowTypeKey)] || "";
}

function sectionKeyFromRowFields(fields) {
  return SECTION_KEY_BY_LABEL[nz(fields?.[MAP_OPERATING_PLATFORM.section])] || "";
}

function rowTypeKeyFromRowFields(fields) {
  return ROW_TYPE_KEY_BY_LABEL[nz(fields?.[MAP_OPERATING_PLATFORM.rowType])] || "";
}

function baseRow(sectionKey, rowTypeKey, displayOrder, patch = {}) {
  const section = sectionLabel(sectionKey);
  const rowType = rowTypeLabel(rowTypeKey);
  if (!section || !rowType) return null;
  return {
    [MAP_OPERATING_PLATFORM.section]: section,
    [MAP_OPERATING_PLATFORM.rowType]: rowType,
    [MAP_OPERATING_PLATFORM.displayOrder]: displayOrder,
    ...patch,
  };
}

export const SNAPSHOT_KPI_SPECS = [
  {
    rowKey: "revenue_management_capability",
    title: "Commercial Engine",
    platformKeys: ["cap_kpi_operating_model"],
    governanceKeys: ["revenueManagementCapability", "Revenue Management Capability"],
  },
  {
    rowKey: "owner_reporting_level",
    title: "Owner Reporting",
    platformKeys: ["cap_kpi_reporting"],
    governanceKeys: ["ownerReportingLevel", "Owner Reporting Level"],
  },
  {
    rowKey: "pre_opening_support",
    title: "Pre-Opening Support",
    platformKeys: ["cap_kpi_transition"],
    governanceKeys: ["preOpeningSupportCapability", "Pre-Opening Support Capability"],
    commercialKeys: ["newBuildOpeningExperience", "New-Build Opening Experience"],
  },
  {
    rowKey: "conversion_reflag",
    title: "Conversion Capability",
    platformKeys: [],
    governanceKeys: ["conversionReflagExperience", "Conversion / Reflag Experience"],
    commercialKeys: ["conversionReflagExperience"],
  },
  {
    rowKey: "fb_capability",
    title: "F&B & Resort",
    platformKeys: ["cap_kpi_execution_strength"],
    governanceKeys: ["fbCapabilityLevel", "F&B Capability Level", "fBCapabilityLevel"],
  },
];

export const PERFORMANCE_SIGNAL_SPECS = [
  { rowKey: "cap_signal_budget", title: "Budget Accuracy" },
  { rowKey: "cap_signal_lift", title: "Time to First Performance Lift" },
  { rowKey: "cap_signal_trans", title: "Transitions Delivered on Schedule" },
];

export const POSITIONING_CARD_SPECS = [
  { rowKey: "cap_card_asset_positioning", title: "Asset Positioning" },
  { rowKey: "cap_card_service_diff", title: "Service Differentiation" },
  { rowKey: "cap_card_execution_rel", title: "Execution Reliability" },
];

const PILLAR_JSON_SPECS = [
  { sectionKey: "commercialEngine", formKey: "op_commercial_engine_json" },
  { sectionKey: "ownerReporting", formKey: "op_owner_reporting_json" },
  { sectionKey: "preOpeningTransition", formKey: "op_preopening_transition_json" },
  { sectionKey: "conversionRepositioning", formKey: "op_conversion_repositioning_json" },
  { sectionKey: "fbLifestyleResort", formKey: "op_fb_lifestyle_resort_json" },
];

const PILLAR_INTRO_SPECS = [
  { sectionKey: "operationalExecutionLabor", formKeys: ["cap_profile_operational"] },
  { sectionKey: "commercialEngine", formKeys: ["cap_profile_commercial"] },
  { sectionKey: "preOpeningTransition", formKeys: ["cap_profile_transition"] },
  { sectionKey: "conversionRepositioning", formKeys: ["cap_deep_revenue_systems"] },
  { sectionKey: "technologyEnabledOperations", formKeys: ["cap_deep_execution_infra"] },
  { sectionKey: "ownerReporting", formKeys: ["cap_card_governance"] },
  { sectionKey: "fbLifestyleResort", formKeys: ["cap_card_service_diff"] },
];

const PILLAR_MULTILINE_CAPABILITY_SPECS = [
  { sectionKey: "commercialEngine", formKeys: ["cap_profile_commercial"], skipIfIntro: true },
  { sectionKey: "preOpeningTransition", formKeys: ["cap_profile_transition"], skipIfIntro: true },
];

function pickField(sources, keys) {
  const keyList = Array.isArray(keys) ? keys : keys != null ? [keys] : [];
  for (const src of sources) {
    if (!src) continue;
    for (const k of keyList) {
      const v = nz(src[k]);
      if (v) return v;
    }
  }
  return "";
}

function linesToCapabilities(text) {
  return String(text || "")
    .split(/\n+/)
    .map((l) => l.trim())
    .filter(Boolean)
    .map((line) => {
      const m = line.match(/^([^:]+):\s*(.+)$/);
      if (m) {
        return { title: m[1].trim(), description: m[2].trim() };
      }
      return { title: line, description: "" };
    });
}

function itemsFromJsonPayload(obj) {
  const items = Array.isArray(obj?.items) ? obj.items : parseJsonArray(obj);
  return items
    .map((item) => ({
      title: nz(item?.title),
      description: nz(item?.description),
    }))
    .filter((item) => item.title);
}

function emptyPillarsShape() {
  const pillars = {};
  Object.keys(PILLAR_UI_ID_TO_SECTION_KEY).forEach((uiId) => {
    const key = PILLAR_UI_ID_TO_SECTION_KEY[uiId];
    pillars[key] = { title: "", description: "", items: [] };
  });
  return pillars;
}

export function mapOperatingPlatformRowFromAirtable(record) {
  const f = record?.fields || record || {};
  const sectionKey = sectionKeyFromRowFields(f);
  const rowTypeKey = rowTypeKeyFromRowFields(f);
  if (!sectionKey || !rowTypeKey) return null;

  const displayOrder = Number(f[MAP_OPERATING_PLATFORM.displayOrder]) || 0;
  const rowKey = nz(f[MAP_OPERATING_PLATFORM.rowKey]);
  const title = nz(f[MAP_OPERATING_PLATFORM.title]);
  const body = formatOperatingPlatformBody(rowTypeKey, f[MAP_OPERATING_PLATFORM.body]);
  const extra = formatOperatingPlatformExtra(rowTypeKey, f[MAP_OPERATING_PLATFORM.extra]);

  return {
    sectionKey,
    rowTypeKey,
    displayOrder,
    rowKey,
    title,
    body,
    extra,
  };
}

export function mapOperatingPlatformRowsForDetail(rows) {
  const pillars = emptyPillarsShape();
  const snapshotKpis = [];
  const performanceSignals = [];
  const positioningCards = [];

  const sectionTitles = {};
  const introBySection = {};

  (rows || [])
    .map(mapOperatingPlatformRowFromAirtable)
    .filter(Boolean)
    .sort((a, b) => a.displayOrder - b.displayOrder)
    .forEach((row) => {
      const { sectionKey, rowTypeKey, title, body, extra, rowKey } = row;

      if (rowTypeKey === "pillarIntro") {
        introBySection[sectionKey] = body || title;
        if (title) sectionTitles[sectionKey] = title;
        return;
      }

      if (rowTypeKey === "capability" && pillars[sectionKey]) {
        if (title) {
          pillars[sectionKey].items.push({ title, description: body });
        }
        return;
      }

      if (rowTypeKey === "kpiLevel") {
        snapshotKpis.push({ rowKey, title, value: extra || body });
        return;
      }

      if (rowTypeKey === "performanceSignal") {
        performanceSignals.push({ rowKey, title, value: extra || body });
        return;
      }

      if (rowTypeKey === "positioningCard") {
        positioningCards.push({ rowKey, title, description: body });
      }
    });

  Object.keys(pillars).forEach((key) => {
    const defaultTitle = sectionLabel(key);
    pillars[key].title = sectionTitles[key] || defaultTitle;
    pillars[key].description = introBySection[key] || "";
  });

  return { pillars, snapshotKpis, performanceSignals, positioningCards };
}

export function mapOperatingPlatformRowToAirtable(row, displayOrder) {
  const sectionKey = nz(row.sectionKey || row.section);
  const rowTypeKey = nz(row.rowType || row.rowTypeKey);
  const mapped = baseRow(sectionKey, rowTypeKey, displayOrder, {});
  if (!mapped) return null;

  if (nz(row.rowKey)) mapped[MAP_OPERATING_PLATFORM.rowKey] = nz(row.rowKey);
  if (nz(row.title)) mapped[MAP_OPERATING_PLATFORM.title] = nz(row.title);
  if (nz(row.subtitle)) mapped[MAP_OPERATING_PLATFORM.subtitle] = nz(row.subtitle);
  if (nz(row.body)) {
    mapped[MAP_OPERATING_PLATFORM.body] = formatOperatingPlatformBody(rowTypeKey, row.body);
  }
  if (nz(row.description)) {
    mapped[MAP_OPERATING_PLATFORM.body] = formatOperatingPlatformBody(rowTypeKey, row.description);
  }
  if (nz(row.extra)) {
    mapped[MAP_OPERATING_PLATFORM.extra] = formatOperatingPlatformExtra(rowTypeKey, row.extra);
  }
  if (nz(row.value)) {
    mapped[MAP_OPERATING_PLATFORM.extra] = formatOperatingPlatformExtra(rowTypeKey, row.value);
  }

  return mapped;
}

export function buildOperatingPlatformAirtableRows(body) {
  const op =
    body?.operatingPlatform && typeof body.operatingPlatform === "object"
      ? body.operatingPlatform
      : null;
  if (!op) return [];

  const out = [];
  let order = 1;

  (op.snapshotKpis || []).forEach((row) => {
    const m = mapOperatingPlatformRowToAirtable(
      { sectionKey: "platformSnapshot", rowType: "kpiLevel", ...row },
      order++
    );
    if (m) out.push(m);
  });

  (op.performanceSignals || []).forEach((row) => {
    const m = mapOperatingPlatformRowToAirtable(
      { sectionKey: "operatingSignals", rowType: "performanceSignal", ...row },
      order++
    );
    if (m) out.push(m);
  });

  (op.positioningCards || []).forEach((row) => {
    const m = mapOperatingPlatformRowToAirtable(
      { sectionKey: "platformPositioning", rowType: "positioningCard", ...row },
      order++
    );
    if (m) out.push(m);
  });

  Object.entries(op.pillars || {}).forEach(([sectionKey, pillar]) => {
    if (!pillar || typeof pillar !== "object") return;
    const intro = nz(pillar.description);
    if (intro) {
      const introRow = mapOperatingPlatformRowToAirtable(
        {
          sectionKey,
          rowType: "pillarIntro",
          title: nz(pillar.title) || sectionLabel(sectionKey),
          body: intro,
        },
        order++
      );
      if (introRow) out.push(introRow);
    }
    (pillar.items || []).forEach((item) => {
      const m = mapOperatingPlatformRowToAirtable(
        {
          sectionKey,
          rowType: "capability",
          title: nz(item.title),
          body: nz(item.description),
        },
        order++
      );
      if (m) out.push(m);
    });
  });

  return out;
}

function offeredServicesToPillars(offeredRaw) {
  const offered = Array.isArray(offeredRaw)
    ? offeredRaw
    : String(offeredRaw || "")
        .split(/[,;\n|]+/)
        .map((s) => nz(s))
        .filter(Boolean);

  const patterns = {
    commercialEngine: /revenue|sales|marketing|commercial|distribution|pricing|forecast/i,
    ownerReporting: /report|governance|owner|budget|capex|dashboard|review/i,
    preOpeningTransition: /pre-?opening|transition|opening|recruit|procurement|systems|training/i,
    conversionRepositioning: /conversion|reflag|reposition|turnaround|pip|renovation|stabiliz/i,
    fbLifestyleResort: /f&b|food|beverage|resort|lifestyle|spa|restaurant|pool|programming|wellness/i,
    salesMarketingActivation: /sales|marketing|group|corporate|leisure/i,
    procurementCostControl: /procurement|purchasing|cost|supply/i,
    operationalExecutionLabor: /labor|staffing|hr|housekeeping|engineering/i,
  };

  const out = {};
  for (const [sectionKey, re] of Object.entries(patterns)) {
    const hits = offered
      .filter((s) => re.test(s))
      .slice(0, 6)
      .map((s) => ({ title: s, description: "" }));
    if (hits.length) out[sectionKey] = hits;
  }
  return out;
}

export function buildOperatingPlatformPayloadFromIntakeBody(body) {
  const b = body || {};
  const incoming =
    b.operatingPlatform && typeof b.operatingPlatform === "object" ? b.operatingPlatform : {};

  const pillars = emptyPillarsShape();
  if (incoming.pillars) {
    Object.keys(pillars).forEach((key) => {
      const p = incoming.pillars[key];
      if (!p) return;
      pillars[key] = {
        title: nz(p.title),
        description: nz(p.description),
        items: Array.isArray(p.items)
          ? p.items.map((i) => ({ title: nz(i.title), description: nz(i.description) })).filter((i) => i.title)
          : [],
      };
    });
  }

  for (const spec of PILLAR_JSON_SPECS) {
    const key = spec.sectionKey;
    if (pillars[key].items.length) continue;
    const raw = b[spec.formKey];
    const obj = parseJsonObject(raw);
    if (obj) {
      pillars[key].description = pillars[key].description || nz(obj.intro) || nz(obj.description);
      pillars[key].items = itemsFromJsonPayload(obj);
    }
  }

  for (const spec of PILLAR_INTRO_SPECS) {
    const key = spec.sectionKey;
    if (pillars[key].description) continue;
    const text = pickField([b], spec.formKeys);
    if (text && !pillars[key].items.length) {
      pillars[key].description = text;
    }
  }

  for (const spec of PILLAR_MULTILINE_CAPABILITY_SPECS) {
    const key = spec.sectionKey;
    if (pillars[key].items.length) continue;
    const text = pickField([b], spec.formKeys);
    if (!text) continue;
    if (spec.skipIfIntro && pillars[key].description === text) continue;
    const caps = linesToCapabilities(text);
    if (caps.length) {
      if (!pillars[key].description && caps.length > 1) {
        pillars[key].items = caps;
      } else if (!pillars[key].description) {
        pillars[key].description = text;
      }
    }
  }

  const offeredCaps = offeredServicesToPillars(b.offeredServices);
  Object.keys(offeredCaps).forEach((key) => {
    if (!pillars[key].items.length) pillars[key].items = offeredCaps[key];
  });

  const snapshotKpis = [];
  for (const spec of SNAPSHOT_KPI_SPECS) {
    const fromIncoming = (incoming.snapshotKpis || []).find((s) => nz(s.rowKey) === spec.rowKey);
    const value =
      nz(fromIncoming?.value) ||
      pickField([b], spec.platformKeys) ||
      pickField([b], spec.governanceKeys) ||
      pickField([b], spec.commercialKeys);
    if (!value || /^not measured/i.test(value)) continue;
    snapshotKpis.push({ rowKey: spec.rowKey, title: spec.title, value });
  }

  const performanceSignals = [];
  for (const spec of PERFORMANCE_SIGNAL_SPECS) {
    const fromIncoming = (incoming.performanceSignals || []).find(
      (s) => nz(s.rowKey) === spec.rowKey
    );
    const value = nz(fromIncoming?.value) || pickField([b], [spec.rowKey]);
    if (!value || /^not measured/i.test(value)) continue;
    performanceSignals.push({ rowKey: spec.rowKey, title: spec.title, value });
  }

  const positioningCards = [];
  for (const spec of POSITIONING_CARD_SPECS) {
    const fromIncoming = (incoming.positioningCards || []).find(
      (s) => nz(s.rowKey) === spec.rowKey
    );
    const text = nz(fromIncoming?.description) || pickField([b], [spec.rowKey]);
    if (!text) continue;
    positioningCards.push({ rowKey: spec.rowKey, title: spec.title, description: text });
  }

  return { pillars, snapshotKpis, performanceSignals, positioningCards };
}

export function buildOperatingPlatformAirtableRowsFromLegacy(platformFields, governanceFields, commercialFields) {
  const platform = platformFields || {};
  const governance = governanceFields || {};
  const commercial = commercialFields || {};
  const combined = { ...platform, ...governance, ...commercial };

  const payload = buildOperatingPlatformPayloadFromIntakeBody(combined);
  const rows = [];
  let order = 1;

  const push = (row) => {
    const m = mapOperatingPlatformRowToAirtable(row, order++);
    if (m) rows.push(m);
  };

  payload.snapshotKpis.forEach((s) =>
    push({ sectionKey: "platformSnapshot", rowType: "kpiLevel", ...s })
  );
  payload.performanceSignals.forEach((s) =>
    push({ sectionKey: "operatingSignals", rowType: "performanceSignal", ...s })
  );
  payload.positioningCards.forEach((s) =>
    push({ sectionKey: "platformPositioning", rowType: "positioningCard", ...s })
  );

  Object.entries(payload.pillars).forEach(([sectionKey, pillar]) => {
    if (nz(pillar.description)) {
      push({
        sectionKey,
        rowType: "pillarIntro",
        title: nz(pillar.title) || sectionLabel(sectionKey),
        body: pillar.description,
      });
    }
    (pillar.items || []).forEach((item) => {
      push({
        sectionKey,
        rowType: "capability",
        title: item.title,
        body: item.description,
      });
    });
  });

  return { rows, payload };
}

export function applyOperatingPlatformToLegacyPrefill(prefill, platform) {
  if (!prefill || !platform) return prefill;

  const jsonMirror = {
    commercialEngine: "op_commercial_engine_json",
    ownerReporting: "op_owner_reporting_json",
    preOpeningTransition: "op_preopening_transition_json",
    conversionRepositioning: "op_conversion_repositioning_json",
    fbLifestyleResort: "op_fb_lifestyle_resort_json",
  };

  Object.entries(jsonMirror).forEach(([sectionKey, formKey]) => {
    const pillar = platform.pillars?.[sectionKey];
    if (!pillar) return;
    const payload = {
      intro: pillar.description,
      items: (pillar.items || []).map((i) => ({
        title: i.title,
        description: i.description,
      })),
    };
    if (payload.intro || payload.items.length) {
      prefill[formKey] = JSON.stringify(payload);
    }
  });

  const introMirror = {
    operationalExecutionLabor: "cap_profile_operational",
    commercialEngine: "cap_profile_commercial",
    preOpeningTransition: "cap_profile_transition",
    conversionRepositioning: "cap_deep_revenue_systems",
    technologyEnabledOperations: "cap_deep_execution_infra",
  };
  Object.entries(introMirror).forEach(([sectionKey, formKey]) => {
    const desc = nz(platform.pillars?.[sectionKey]?.description);
    if (desc) prefill[formKey] = desc;
  });

  (platform.positioningCards || []).forEach((card) => {
    if (nz(card.rowKey) && nz(card.description)) {
      prefill[card.rowKey] = card.description;
    }
  });

  (platform.snapshotKpis || []).forEach((sig) => {
    const key = nz(sig.rowKey);
    const val = nz(sig.value);
    if (!key || !val) return;
    if (key === "revenue_management_capability") {
      prefill.revenueManagementCapability = val;
      prefill.cap_kpi_operating_model = val;
    }
    if (key === "owner_reporting_level") {
      prefill.ownerReportingLevel = val;
      prefill.cap_kpi_reporting = val;
    }
    if (key === "pre_opening_support") {
      prefill.preOpeningSupportCapability = val;
      prefill.cap_kpi_transition = val;
    }
    if (key === "conversion_reflag") prefill.conversionReflagExperience = val;
    if (key === "fb_capability") {
      prefill.fbCapabilityLevel = val;
      prefill.cap_kpi_execution_strength = val;
    }
  });

  (platform.performanceSignals || []).forEach((sig) => {
    const key = nz(sig.rowKey);
    const val = nz(sig.value);
    if (key && val) prefill[key] = val;
  });

  prefill.operatingPlatform = platform;
  return prefill;
}

export class OperatingPlatformValidationError extends Error {
  constructor(errors) {
    super("Operating platform validation failed");
    this.name = "OperatingPlatformValidationError";
    this.errors = errors;
    this.statusCode = 400;
  }
}

export function validateOperatingPlatformPayload(platform) {
  const errors = [];
  const p = platform && typeof platform === "object" ? platform : {};
  Object.entries(p.pillars || {}).forEach(([sectionKey, pillar]) => {
    (pillar?.items || []).forEach((item, i) => {
      if (!nz(item?.title)) {
        errors.push({
          field: `${sectionKey}.items[${i}].title`,
          message: "Capability title required",
        });
      }
    });
  });
  return { valid: errors.length === 0, errors };
}
