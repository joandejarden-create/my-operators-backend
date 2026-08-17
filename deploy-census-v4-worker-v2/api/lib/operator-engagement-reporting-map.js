/**
 * Operator Setup — Engagement & Reporting child table (Explorer Owner Engagement tab).
 * One linked child table per operator; `section` discriminates row shape.
 *
 * Table: Operator Setup - Engagement & Reporting
 * Override: AIRTABLE_OPERATOR_SETUP_ENGAGEMENT_REPORTING_TABLE
 */

import { properCaseBody } from "./operator-leadership-platform-map.js";

export const ENGAGEMENT_REPORTING_TABLE =
  process.env.AIRTABLE_OPERATOR_SETUP_ENGAGEMENT_REPORTING_TABLE ||
  "Operator Setup - Engagement & Reporting";

export const ENGAGEMENT_REPORTING_SECTIONS = {
  strategicOwnerValue: "Strategic Owner Value",
  engagementCadence: "Engagement Cadence",
  controlsGovernance: "Controls & Governance",
  reportsReceived: "Reports Received",
  ownerTools: "Owner Tools",
  lifecycleSupport: "Lifecycle Support",
  ownerValueCard: "Owner Value Card",
  optionalCluster: "Optional Cluster",
  engagementSignal: "Engagement Signal",
};

export const MAP_ENGAGEMENT_REPORTING = {
  section: "section",
  rowKey: "row_key",
  displayOrder: "display_order",
  title: "title",
  subtitle: "subtitle",
  body: "body",
  extra: "extra",
};

const SECTION_BY_API_KEY = Object.fromEntries(
  Object.entries(ENGAGEMENT_REPORTING_SECTIONS).map(([k, v]) => [k, v])
);
const SECTION_KEY_BY_LABEL = Object.fromEntries(
  Object.entries(ENGAGEMENT_REPORTING_SECTIONS).map(([k, v]) => [v, k])
);

function nz(v) {
  return v != null && String(v).trim() !== "" ? String(v).trim() : "";
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

function sectionLabel(sectionKey) {
  return SECTION_BY_API_KEY[nz(sectionKey)] || "";
}

function sectionKeyFromRowFields(fields) {
  const label = nz(fields && fields[MAP_ENGAGEMENT_REPORTING.section]);
  return SECTION_KEY_BY_LABEL[label] || "";
}

function baseRow(sectionKey, displayOrder, patch = {}) {
  return {
    [MAP_ENGAGEMENT_REPORTING.section]: sectionLabel(sectionKey),
    [MAP_ENGAGEMENT_REPORTING.displayOrder]: displayOrder,
    ...patch,
  };
}

export const OWNER_VALUE_CARD_SPECS = [
  { rowKey: "ov_card_discipline", title: "Discipline & Controls", formKey: "ov_card_discipline" },
  { rowKey: "ov_card_commercial", title: "Commercial Engine", formKey: "ov_card_commercial" },
  { rowKey: "ov_card_communication", title: "Insight to Action", formKey: "ov_card_communication" },
  { rowKey: "ov_card_flexibility", title: "Flexibility & Tradeoffs", formKey: "ov_card_flexibility" },
  { rowKey: "ov_card_risk", title: "Continuity & Escalation", formKey: "ov_card_risk" },
];

export const OPTIONAL_CLUSTER_SPECS = [
  { rowKey: "ov_cluster_interaction", title: "Interaction Rhythm", formKey: "ov_cluster_interaction" },
  { rowKey: "ov_cluster_deliverables", title: "Deliverable Labels", formKey: "ov_cluster_deliverables" },
  {
    rowKey: "owner_engagement_narrative",
    title: "Decision Support",
    formKey: "ownerEngagementNarrative",
  },
];

export const ENGAGEMENT_SIGNAL_SPECS = [
  {
    rowKey: "owner_reporting_level",
    title: "Owner Reporting Level",
    formKeys: ["ownerReportingLevel", "Owner Reporting Level"],
    governanceKeys: ["ownerReportingLevel", "Owner Reporting Level"],
  },
  {
    rowKey: "governance_cadence",
    title: "Governance Cadence",
    formKeys: ["governanceCadence", "Governance Cadence", "reportingFrequency"],
    governanceKeys: ["governanceCadence", "Governance Cadence"],
  },
];

const JSON_SECTION_SPECS = [
  {
    sectionKey: "strategicOwnerValue",
    formKey: "ov_strategic_owner_value_json",
    mapItem(item, order) {
      const title = nz(item.title);
      if (!title) return null;
      return baseRow("strategicOwnerValue", order, {
        [MAP_ENGAGEMENT_REPORTING.title]: title,
        [MAP_ENGAGEMENT_REPORTING.body]: formatEngagementBody("strategicOwnerValue", nz(item.description)),
      });
    },
  },
  {
    sectionKey: "engagementCadence",
    formKey: "ov_engagement_cadence_json",
    mapItem(item, order) {
      const cadence = nz(item.cadence || item.label);
      const engagementType = nz(item.engagementType || item.type || item.title);
      const focus = nz(item.focus || item.description);
      if (!cadence && !engagementType && !focus) return null;
      return baseRow("engagementCadence", order, {
        [MAP_ENGAGEMENT_REPORTING.subtitle]: cadence,
        [MAP_ENGAGEMENT_REPORTING.title]: engagementType || cadence,
        [MAP_ENGAGEMENT_REPORTING.body]: formatEngagementBody("engagementCadence", focus),
      });
    },
  },
  {
    sectionKey: "controlsGovernance",
    formKey: "ov_controls_governance_json",
    mapItem(item, order) {
      const title = nz(item.title);
      if (!title) return null;
      return baseRow("controlsGovernance", order, {
        [MAP_ENGAGEMENT_REPORTING.title]: title,
        [MAP_ENGAGEMENT_REPORTING.body]: formatEngagementBody("controlsGovernance", nz(item.description)),
      });
    },
  },
  {
    sectionKey: "reportsReceived",
    formKey: "ov_reports_received_json",
    mapItem(item, order) {
      const title = nz(item.title);
      if (!title) return null;
      return baseRow("reportsReceived", order, {
        [MAP_ENGAGEMENT_REPORTING.title]: title,
        [MAP_ENGAGEMENT_REPORTING.body]: formatEngagementBody("reportsReceived", nz(item.description)),
      });
    },
  },
  {
    sectionKey: "ownerTools",
    formKey: "ov_owner_tools_json",
    mapItem(item, order) {
      const title = nz(item.title);
      if (!title) return null;
      return baseRow("ownerTools", order, {
        [MAP_ENGAGEMENT_REPORTING.title]: title,
        [MAP_ENGAGEMENT_REPORTING.body]: formatEngagementBody("ownerTools", nz(item.description)),
      });
    },
  },
  {
    sectionKey: "lifecycleSupport",
    formKey: "ov_lifecycle_support_json",
    mapItem(item, order) {
      const stage = nz(item.stage || item.title);
      const support = nz(item.support || item.description);
      if (!stage && !support) return null;
      return baseRow("lifecycleSupport", order, {
        [MAP_ENGAGEMENT_REPORTING.title]: stage,
        [MAP_ENGAGEMENT_REPORTING.body]: formatEngagementBody("lifecycleSupport", support),
      });
    },
  },
];

function pickField(fields, keys) {
  const keyList = Array.isArray(keys) ? keys : keys != null ? [keys] : [];
  for (const k of keyList) {
    const v = nz(fields[k]);
    if (v) return v;
  }
  return "";
}

/** Preserve author sentence case in Explorer narratives (avoid title-casing full paragraphs). */
function formatEngagementBody(_sectionKey, raw) {
  return nz(raw);
}

/** Airtable child row → grouped Explorer / Setup shape. */
export function mapEngagementReportingRowFromAirtable(record) {
  const f = (record && record.fields) || record || {};
  const sectionKey = sectionKeyFromRowFields(f);
  if (!sectionKey) return null;

  const displayOrder = Number(f[MAP_ENGAGEMENT_REPORTING.displayOrder]) || 0;
  const rowKey = nz(f[MAP_ENGAGEMENT_REPORTING.rowKey]);
  const title = nz(f[MAP_ENGAGEMENT_REPORTING.title]);
  const subtitle = nz(f[MAP_ENGAGEMENT_REPORTING.subtitle]);
  const body = nz(f[MAP_ENGAGEMENT_REPORTING.body]);
  const extra = nz(f[MAP_ENGAGEMENT_REPORTING.extra]);

  if (sectionKey === "engagementCadence") {
    return {
      sectionKey,
      displayOrder,
      rowKey,
      explorer: {
        cadence: subtitle || title,
        engagementType: title,
        focus: body,
        description: body,
      },
    };
  }
  if (sectionKey === "lifecycleSupport") {
    return {
      sectionKey,
      displayOrder,
      rowKey,
      explorer: { stage: title, support: body, title, description: body },
    };
  }
  if (sectionKey === "ownerValueCard" || sectionKey === "optionalCluster") {
    return {
      sectionKey,
      displayOrder,
      rowKey,
      explorer: { rowKey, title, description: body, body },
      formKey: rowKey,
    };
  }
  if (sectionKey === "engagementSignal") {
    return {
      sectionKey,
      displayOrder,
      rowKey,
      explorer: { rowKey, title, value: extra || body },
    };
  }
  return {
    sectionKey,
    displayOrder,
    rowKey,
    explorer: { title, description: body, rowKey },
  };
}

/**
 * @param {import("airtable").Records<any>} rows
 */
export function mapEngagementReportingRowsForDetail(rows) {
  const buckets = {
    strategicOwnerValue: [],
    engagementCadence: [],
    controlsGovernance: [],
    reportsReceived: [],
    ownerTools: [],
    lifecycleSupport: [],
    ownerValueCards: [],
    optionalClusters: [],
    engagementSignals: [],
  };

  const bucketKey = {
    strategicOwnerValue: "strategicOwnerValue",
    engagementCadence: "engagementCadence",
    controlsGovernance: "controlsGovernance",
    reportsReceived: "reportsReceived",
    ownerTools: "ownerTools",
    lifecycleSupport: "lifecycleSupport",
    ownerValueCard: "ownerValueCards",
    optionalCluster: "optionalClusters",
    engagementSignal: "engagementSignals",
  };

  (rows || [])
    .map(mapEngagementReportingRowFromAirtable)
    .filter(Boolean)
    .sort((a, b) => a.displayOrder - b.displayOrder)
    .forEach((mapped) => {
      const key = bucketKey[mapped.sectionKey];
      if (key && mapped.explorer) buckets[key].push(mapped.explorer);
    });

  return buckets;
}

export function mapEngagementReportingRowToAirtable(row, displayOrder) {
  const sectionKey = nz(row.sectionKey || row.section);
  const section = sectionLabel(sectionKey);
  if (!section) return null;

  const out = {
    [MAP_ENGAGEMENT_REPORTING.section]: section,
    [MAP_ENGAGEMENT_REPORTING.displayOrder]: displayOrder,
  };

  if (nz(row.rowKey)) out[MAP_ENGAGEMENT_REPORTING.rowKey] = nz(row.rowKey);
  if (nz(row.title)) out[MAP_ENGAGEMENT_REPORTING.title] = nz(row.title);
  if (nz(row.subtitle)) out[MAP_ENGAGEMENT_REPORTING.subtitle] = nz(row.subtitle);
  if (nz(row.body)) {
    out[MAP_ENGAGEMENT_REPORTING.body] = formatEngagementBody(sectionKey, nz(row.body));
  }
  if (nz(row.extra)) out[MAP_ENGAGEMENT_REPORTING.extra] = nz(row.extra);
  if (nz(row.description)) {
    out[MAP_ENGAGEMENT_REPORTING.body] = formatEngagementBody(sectionKey, nz(row.description));
  }
  if (nz(row.value)) out[MAP_ENGAGEMENT_REPORTING.extra] = nz(row.value);
  if (nz(row.cadence)) out[MAP_ENGAGEMENT_REPORTING.subtitle] = nz(row.cadence);
  if (nz(row.engagementType)) out[MAP_ENGAGEMENT_REPORTING.title] = nz(row.engagementType);
  if (nz(row.focus)) out[MAP_ENGAGEMENT_REPORTING.body] = formatEngagementBody(sectionKey, nz(row.focus));
  if (nz(row.stage)) out[MAP_ENGAGEMENT_REPORTING.title] = nz(row.stage);
  if (nz(row.support)) out[MAP_ENGAGEMENT_REPORTING.body] = formatEngagementBody(sectionKey, nz(row.support));

  return out;
}

export function buildEngagementReportingAirtableRows(body) {
  const er =
    body && body.engagementReporting && typeof body.engagementReporting === "object"
      ? body.engagementReporting
      : body && body.engagementPlatform && typeof body.engagementPlatform === "object"
        ? body.engagementPlatform
        : null;
  if (!er) return [];

  const sections = [
    ["strategicOwnerValue", er.strategicOwnerValue],
    ["engagementCadence", er.engagementCadence],
    ["controlsGovernance", er.controlsGovernance],
    ["reportsReceived", er.reportsReceived],
    ["ownerTools", er.ownerTools],
    ["lifecycleSupport", er.lifecycleSupport],
    ["ownerValueCard", er.ownerValueCards],
    ["optionalCluster", er.optionalClusters],
    ["engagementSignal", er.engagementSignals],
  ];

  const out = [];
  let order = 1;

  sections.forEach(([sectionKey, list]) => {
    if (!Array.isArray(list)) return;
    list.forEach((row) => {
      const mapped = mapEngagementReportingRowToAirtable({ ...row, sectionKey }, order++);
      if (mapped) out.push(mapped);
    });
  });

  return out;
}

function nonEmptyTitleRows(list) {
  return (Array.isArray(list) ? list : []).filter((row) => row && nz(row.title || row.stage || row.cadence));
}

/**
 * Build structured payload from Setup intake (repeaters + legacy Commercial/Governance fields).
 * @param {object} body
 */
export function buildEngagementReportingPayloadFromIntakeBody(body) {
  const b = body || {};
  const incoming =
    b.engagementReporting && typeof b.engagementReporting === "object"
      ? b.engagementReporting
      : b.engagementPlatform && typeof b.engagementPlatform === "object"
        ? b.engagementPlatform
        : {};

  let strategicOwnerValue = nonEmptyTitleRows(incoming.strategicOwnerValue);
  if (!strategicOwnerValue.length) {
    strategicOwnerValue = parseJsonArray(b.ov_strategic_owner_value_json).map((item) => ({
      title: nz(item.title),
      description: nz(item.description),
    }));
  }

  let engagementCadence = (incoming.engagementCadence || []).filter(
    (r) => r && (nz(r.cadence) || nz(r.engagementType) || nz(r.focus))
  );
  if (!engagementCadence.length) {
    engagementCadence = parseJsonArray(b.ov_engagement_cadence_json).map((item) => ({
      cadence: nz(item.cadence || item.label),
      engagementType: nz(item.engagementType || item.type || item.title),
      focus: nz(item.focus || item.description),
    }));
  }

  let controlsGovernance = nonEmptyTitleRows(incoming.controlsGovernance);
  if (!controlsGovernance.length) {
    controlsGovernance = parseJsonArray(b.ov_controls_governance_json).map((item) => ({
      title: nz(item.title),
      description: nz(item.description),
    }));
  }

  let reportsReceived = nonEmptyTitleRows(incoming.reportsReceived);
  if (!reportsReceived.length) {
    reportsReceived = parseJsonArray(b.ov_reports_received_json).map((item) => ({
      title: nz(item.title),
      description: nz(item.description),
    }));
  }

  let ownerTools = nonEmptyTitleRows(incoming.ownerTools);
  if (!ownerTools.length) {
    ownerTools = parseJsonArray(b.ov_owner_tools_json).map((item) => ({
      title: nz(item.title),
      description: nz(item.description),
    }));
  }

  let lifecycleSupport = (incoming.lifecycleSupport || []).filter(
    (r) => r && (nz(r.stage) || nz(r.support))
  );
  if (!lifecycleSupport.length) {
    lifecycleSupport = parseJsonArray(b.ov_lifecycle_support_json).map((item) => ({
      stage: nz(item.stage || item.title),
      support: nz(item.support || item.description),
    }));
  }

  const ownerValueCards = [];
  for (const spec of OWNER_VALUE_CARD_SPECS) {
    const fromIncoming = (incoming.ownerValueCards || []).find(
      (c) => nz(c.rowKey) === spec.rowKey
    );
    const text = nz(fromIncoming?.description ?? fromIncoming?.body ?? b[spec.formKey]);
    if (!text) continue;
    ownerValueCards.push({
      rowKey: spec.rowKey,
      title: spec.title,
      description: text,
      body: text,
    });
  }

  const optionalClusters = [];
  for (const spec of OPTIONAL_CLUSTER_SPECS) {
    const fromIncoming = (incoming.optionalClusters || []).find(
      (c) => nz(c.rowKey) === spec.rowKey
    );
    const text = nz(fromIncoming?.description ?? fromIncoming?.body ?? b[spec.formKey]);
    if (!text) continue;
    optionalClusters.push({
      rowKey: spec.rowKey,
      title: spec.title,
      description: text,
      body: text,
    });
  }

  const engagementSignals = [];
  for (const spec of ENGAGEMENT_SIGNAL_SPECS) {
    const fromIncoming = (incoming.engagementSignals || []).find(
      (s) => nz(s.rowKey) === spec.rowKey
    );
    const value = nz(fromIncoming?.value ?? pickField(b, spec.formKeys));
    if (!value) continue;
    engagementSignals.push({ rowKey: spec.rowKey, title: spec.title, value });
  }

  return {
    strategicOwnerValue,
    engagementCadence,
    controlsGovernance,
    reportsReceived,
    ownerTools,
    lifecycleSupport,
    ownerValueCards,
    optionalClusters,
    engagementSignals,
  };
}

/**
 * Legacy Commercial (+ optional Governance) → child rows for migration.
 * @param {Record<string, unknown>} commercialFields
 * @param {Record<string, unknown>} [governanceFields]
 */
export function buildEngagementReportingAirtableRowsFromLegacy(
  commercialFields,
  governanceFields = {}
) {
  const commercial = commercialFields || {};
  const governance = governanceFields || {};
  const rows = [];
  const sources = [];
  let order = 1;

  for (const spec of JSON_SECTION_SPECS) {
    const raw = pickField(commercial, [spec.formKey]);
    const parsed = parseJsonArray(raw);
    if (!parsed.length) continue;
    parsed.forEach((item) => {
      const mapped = spec.mapItem(item, order++);
      if (mapped) rows.push(mapped);
    });
    sources.push(spec.formKey);
  }

  for (const spec of OWNER_VALUE_CARD_SPECS) {
    const text = pickField(commercial, [spec.formKey]);
    if (!text) continue;
    rows.push(
      baseRow("ownerValueCard", order++, {
        [MAP_ENGAGEMENT_REPORTING.rowKey]: spec.rowKey,
        [MAP_ENGAGEMENT_REPORTING.title]: spec.title,
        [MAP_ENGAGEMENT_REPORTING.body]: formatEngagementBody("ownerValueCard", text),
      })
    );
    sources.push(spec.formKey);
  }

  for (const spec of OPTIONAL_CLUSTER_SPECS) {
    const text = pickField(commercial, [spec.formKey]);
    if (!text) continue;
    rows.push(
      baseRow("optionalCluster", order++, {
        [MAP_ENGAGEMENT_REPORTING.rowKey]: spec.rowKey,
        [MAP_ENGAGEMENT_REPORTING.title]: spec.title,
        [MAP_ENGAGEMENT_REPORTING.body]: formatEngagementBody("optionalCluster", text),
      })
    );
    sources.push(spec.formKey);
  }

  for (const spec of ENGAGEMENT_SIGNAL_SPECS) {
    const value = pickField(governance, spec.governanceKeys) || pickField(commercial, spec.formKeys);
    if (!value) continue;
    rows.push(
      baseRow("engagementSignal", order++, {
        [MAP_ENGAGEMENT_REPORTING.rowKey]: spec.rowKey,
        [MAP_ENGAGEMENT_REPORTING.title]: spec.title,
        [MAP_ENGAGEMENT_REPORTING.extra]: value,
      })
    );
    sources.push(spec.rowKey);
  }

  const countsBySection = {};
  rows.forEach((row) => {
    const label = row[MAP_ENGAGEMENT_REPORTING.section] || "unknown";
    countsBySection[label] = (countsBySection[label] || 0) + 1;
  });

  return { rows, sources, countsBySection };
}

/** Mirror child-table data onto legacy Commercial prefill keys (read-path compat). */
export function applyEngagementReportingToLegacyPrefill(prefill, platform) {
  if (!prefill || !platform) return prefill;

  if (platform.strategicOwnerValue?.length) {
    prefill.ov_strategic_owner_value_json = JSON.stringify(
      platform.strategicOwnerValue.map((r) => ({
        title: r.title,
        description: r.description,
      }))
    );
  }
  if (platform.engagementCadence?.length) {
    prefill.ov_engagement_cadence_json = JSON.stringify(platform.engagementCadence);
  }
  if (platform.controlsGovernance?.length) {
    prefill.ov_controls_governance_json = JSON.stringify(
      platform.controlsGovernance.map((r) => ({
        title: r.title,
        description: r.description,
      }))
    );
  }
  if (platform.reportsReceived?.length) {
    prefill.ov_reports_received_json = JSON.stringify(
      platform.reportsReceived.map((r) => ({
        title: r.title,
        description: r.description,
      }))
    );
  }
  if (platform.ownerTools?.length) {
    prefill.ov_owner_tools_json = JSON.stringify(
      platform.ownerTools.map((r) => ({
        title: r.title,
        description: r.description,
      }))
    );
  }
  if (platform.lifecycleSupport?.length) {
    prefill.ov_lifecycle_support_json = JSON.stringify(platform.lifecycleSupport);
  }

  (platform.ownerValueCards || []).forEach((card) => {
    const key = nz(card.rowKey);
    const text = nz(card.description || card.body);
    if (key && text) prefill[key] = text;
  });

  (platform.optionalClusters || []).forEach((row) => {
    const key = nz(row.rowKey);
    const text = nz(row.description || row.body);
    if (!key || !text) return;
    if (key === "owner_engagement_narrative") prefill.ownerEngagementNarrative = text;
    else prefill[key] = text;
  });

  (platform.engagementSignals || []).forEach((sig) => {
    const key = nz(sig.rowKey);
    const val = nz(sig.value);
    if (key === "owner_reporting_level" && val) prefill.ownerReportingLevel = val;
    if (key === "governance_cadence" && val) prefill.governanceCadence = val;
  });

  prefill.engagementReporting = platform;
  prefill.engagementPlatform = platform;
  return prefill;
}

export class EngagementReportingValidationError extends Error {
  constructor(errors) {
    super("Engagement & Reporting validation failed");
    this.name = "EngagementReportingValidationError";
    this.errors = errors;
    this.statusCode = 400;
  }
}

export function validateEngagementReportingPayload(platform) {
  const errors = [];
  const p = platform && typeof platform === "object" ? platform : {};
  (p.engagementCadence || []).forEach((row, i) => {
    if (!nz(row.cadence) && !nz(row.engagementType) && !nz(row.focus)) {
      errors.push({
        field: `engagementCadence[${i}]`,
        message: "Cadence row needs cadence, engagement type, or focus",
      });
    }
  });
  return { valid: errors.length === 0, errors };
}
