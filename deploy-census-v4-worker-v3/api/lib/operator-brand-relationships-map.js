/**
 * Operator Setup — Brand & Relationships child table (Explorer Brand tab).
 * Portfolio mix, depth tables, capability tiles, narratives, signals — no JSON blobs on write path.
 *
 * Table: Operator Setup - Brand Relationships
 * Override: AIRTABLE_OPERATOR_SETUP_BRAND_RELATIONSHIPS_TABLE
 */

import { properCaseBody, properCaseExtra } from "./operator-leadership-platform-map.js";

export const BRAND_RELATIONSHIPS_TABLE =
  process.env.AIRTABLE_OPERATOR_SETUP_BRAND_RELATIONSHIPS_TABLE ||
  "Operator Setup - Brand Relationships";

export const BRAND_RELATIONSHIPS_SECTIONS = {
  brandSnapshot: "Brand Snapshot",
  portfolioMix: "Portfolio Mix",
  relationshipDepth: "Relationship Depth",
  executionCapabilities: "Execution Capabilities",
  governanceCompliance: "Governance Compliance",
  softIndependent: "Soft Independent",
  brandNarrative: "Brand Narrative",
  brandSignal: "Brand Signal",
};

export const MAP_BRAND_RELATIONSHIPS = {
  section: "section",
  rowKey: "row_key",
  displayOrder: "display_order",
  title: "title",
  subtitle: "subtitle",
  body: "body",
  extra: "extra",
};

const SECTION_BY_API_KEY = Object.fromEntries(
  Object.entries(BRAND_RELATIONSHIPS_SECTIONS).map(([k, v]) => [k, v])
);
const SECTION_KEY_BY_LABEL = Object.fromEntries(
  Object.entries(BRAND_RELATIONSHIPS_SECTIONS).map(([k, v]) => [v, k])
);

export const BRAND_SIGNAL_SPECS = [
  { rowKey: "brand_signal_audit", title: "Brand Audit Pass Rate", formKeys: ["brand_signal_audit"] },
  {
    rowKey: "brand_signal_reflag",
    title: "Reflag Readiness Lead Time",
    formKeys: ["brand_signal_reflag"],
  },
  {
    rowKey: "brand_signal_franchise_align",
    title: "Franchise Alignment",
    formKeys: ["brand_signal_franchise_align"],
  },
  {
    rowKey: "brand_signal_soft_retention",
    title: "Soft Brand Retention",
    formKeys: ["brand_signal_soft_retention"],
  },
];

export const BRAND_NARRATIVE_SPECS = [
  { rowKey: "brand_narrative_compliance", title: "Compliance + Commercial Balance", formKeys: ["brand_narrative_compliance"] },
  { rowKey: "brand_narrative_relationship", title: "Brand Relationship Model", formKeys: ["brand_narrative_relationship"] },
];

const JSON_SECTION_SPECS = [
  {
    sectionKey: "portfolioMix",
    formKey: "brand_portfolio_mix_json",
    mapItem(item, order) {
      const title = nz(item.brandFlagType || item.brand || item.name);
      if (!title) return null;
      return baseRow("portfolioMix", order, {
        [MAP_BRAND_RELATIONSHIPS.title]: title,
        [MAP_BRAND_RELATIONSHIPS.subtitle]: nz(item.portfolioMix || item.mix || item.share),
        [MAP_BRAND_RELATIONSHIPS.body]: formatBrandBody("portfolioMix", nz(item.assetContext || item.context)),
        [MAP_BRAND_RELATIONSHIPS.extra]: formatBrandExtra(
          "portfolioMix",
          nz(item.relationshipStatus || item.status)
        ),
      });
    },
    mapExplorer(row) {
      return {
        brandFlagType: row.title,
        portfolioMix: row.subtitle,
        assetContext: row.body,
        relationshipStatus: row.extra,
      };
    },
  },
  {
    sectionKey: "relationshipDepth",
    formKey: "brand_relationship_depth_json",
    mapItem(item, order) {
      const title = nz(item.brandSegment || item.segment);
      if (!title) return null;
      return baseRow("relationshipDepth", order, {
        [MAP_BRAND_RELATIONSHIPS.title]: title,
        [MAP_BRAND_RELATIONSHIPS.subtitle]: nz(item.relationshipType || item.type),
        [MAP_BRAND_RELATIONSHIPS.body]: formatBrandBody("relationshipDepth", nz(item.ownerContext || item.context)),
        [MAP_BRAND_RELATIONSHIPS.extra]: formatBrandExtra("relationshipDepth", nz(item.depth)),
      });
    },
    mapExplorer(row) {
      return {
        brandSegment: row.title,
        relationshipType: row.subtitle,
        ownerContext: row.body,
        depth: row.extra,
      };
    },
  },
  {
    sectionKey: "executionCapabilities",
    formKey: "brand_execution_capabilities_json",
    mapItem(item, order) {
      const title = nz(item.title);
      if (!title) return null;
      return baseRow("executionCapabilities", order, {
        [MAP_BRAND_RELATIONSHIPS.title]: title,
        [MAP_BRAND_RELATIONSHIPS.body]: formatBrandBody(
          "executionCapabilities",
          nz(item.description)
        ),
      });
    },
    mapExplorer(row) {
      return { title: row.title, description: row.body };
    },
  },
  {
    sectionKey: "governanceCompliance",
    formKey: "brand_governance_compliance_json",
    mapItem(item, order) {
      const title = nz(item.title);
      if (!title) return null;
      return baseRow("governanceCompliance", order, {
        [MAP_BRAND_RELATIONSHIPS.title]: title,
        [MAP_BRAND_RELATIONSHIPS.body]: formatBrandBody(
          "governanceCompliance",
          nz(item.description)
        ),
      });
    },
    mapExplorer(row) {
      return { title: row.title, description: row.body };
    },
  },
];

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
  const label = nz(fields && fields[MAP_BRAND_RELATIONSHIPS.section]);
  return SECTION_KEY_BY_LABEL[label] || "";
}

function baseRow(sectionKey, displayOrder, patch = {}) {
  return {
    [MAP_BRAND_RELATIONSHIPS.section]: sectionLabel(sectionKey),
    [MAP_BRAND_RELATIONSHIPS.displayOrder]: displayOrder,
    ...patch,
  };
}

function pickField(fields, keys) {
  const keyList = Array.isArray(keys) ? keys : keys != null ? [keys] : [];
  for (const k of keyList) {
    const v = nz(fields[k]);
    if (v) return v;
  }
  return "";
}

/** Preserve signal select values and percent tokens exactly. */
function preservesExactBrandValue(sectionKey, raw) {
  if (sectionKey === "brandSignal") return true;
  const s = nz(raw);
  if (/%/.test(s)) return true;
  if (/^not\s+measured/i.test(s)) return true;
  return false;
}

function formatBrandBody(sectionKey, raw) {
  const s = nz(raw);
  if (!s) return "";
  if (sectionKey === "brandNarrative" || sectionKey === "softIndependent") return s;
  if (preservesExactBrandValue(sectionKey, s)) return s;
  if (s.includes("\n")) {
    return s
      .split(/\n+/)
      .map((line) => properCaseBody(line))
      .filter(Boolean)
      .join("\n");
  }
  return properCaseBody(s);
}

function formatBrandExtra(sectionKey, raw) {
  const s = nz(raw);
  if (!s) return "";
  if (preservesExactBrandValue(sectionKey, s)) return s;
  return properCaseExtra(s);
}

export function mapBrandRelationshipsRowFromAirtable(record) {
  const f = (record && record.fields) || record || {};
  const sectionKey = sectionKeyFromRowFields(f);
  if (!sectionKey) return null;

  const displayOrder = Number(f[MAP_BRAND_RELATIONSHIPS.displayOrder]) || 0;
  const rowKey = nz(f[MAP_BRAND_RELATIONSHIPS.rowKey]);
  const title = nz(f[MAP_BRAND_RELATIONSHIPS.title]);
  const subtitle = nz(f[MAP_BRAND_RELATIONSHIPS.subtitle]);
  const body = nz(f[MAP_BRAND_RELATIONSHIPS.body]);
  const extra = nz(f[MAP_BRAND_RELATIONSHIPS.extra]);

  const spec = JSON_SECTION_SPECS.find((s) => s.sectionKey === sectionKey);
  if (spec) {
    return {
      sectionKey,
      displayOrder,
      rowKey,
      explorer: spec.mapExplorer({ title, subtitle, body, extra }),
    };
  }

  if (sectionKey === "brandSnapshot") {
    return {
      sectionKey,
      displayOrder,
      rowKey,
      explorer: { rowKey, title, value: extra || subtitle || body },
    };
  }
  if (sectionKey === "softIndependent") {
    return {
      sectionKey,
      displayOrder,
      rowKey,
      explorer: { narrative: body },
    };
  }
  if (sectionKey === "brandNarrative") {
    return {
      sectionKey,
      displayOrder,
      rowKey,
      explorer: { rowKey, title, body },
    };
  }
  if (sectionKey === "brandSignal") {
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
    explorer: { title, subtitle, body, extra },
  };
}

export function mapBrandRelationshipsRowsForDetail(rows) {
  const buckets = {
    snapshotMetrics: [],
    portfolioMix: [],
    relationshipDepth: [],
    executionCapabilities: [],
    governanceCompliance: [],
    softIndependentNarrative: "",
    narratives: {},
    brandSignals: [],
  };

  const bucketKey = {
    brandSnapshot: "snapshotMetrics",
    portfolioMix: "portfolioMix",
    relationshipDepth: "relationshipDepth",
    executionCapabilities: "executionCapabilities",
    governanceCompliance: "governanceCompliance",
    softIndependent: "softIndependentNarrative",
    brandNarrative: "narratives",
    brandSignal: "brandSignals",
  };

  (rows || [])
    .map(mapBrandRelationshipsRowFromAirtable)
    .filter(Boolean)
    .sort((a, b) => a.displayOrder - b.displayOrder)
    .forEach((mapped) => {
      const key = bucketKey[mapped.sectionKey];
      if (!key || !mapped.explorer) return;
      if (key === "softIndependentNarrative") {
        buckets.softIndependentNarrative = nz(mapped.explorer.narrative);
        return;
      }
      if (key === "narratives") {
        const rk = nz(mapped.explorer.rowKey);
        if (rk) buckets.narratives[rk] = nz(mapped.explorer.body);
        return;
      }
      buckets[key].push(mapped.explorer);
    });

  return buckets;
}

export function mapBrandRelationshipsRowToAirtable(row, displayOrder) {
  const sectionKey = nz(row.sectionKey || row.section);
  const section = sectionLabel(sectionKey);
  if (!section) return null;

  const out = {
    [MAP_BRAND_RELATIONSHIPS.section]: section,
    [MAP_BRAND_RELATIONSHIPS.displayOrder]: displayOrder,
  };

  if (nz(row.rowKey)) out[MAP_BRAND_RELATIONSHIPS.rowKey] = nz(row.rowKey);
  if (nz(row.title)) out[MAP_BRAND_RELATIONSHIPS.title] = nz(row.title);
  if (nz(row.subtitle)) out[MAP_BRAND_RELATIONSHIPS.subtitle] = nz(row.subtitle);
  if (nz(row.body)) out[MAP_BRAND_RELATIONSHIPS.body] = formatBrandBody(sectionKey, nz(row.body));
  if (nz(row.extra)) out[MAP_BRAND_RELATIONSHIPS.extra] = formatBrandExtra(sectionKey, nz(row.extra));
  if (nz(row.description)) {
    out[MAP_BRAND_RELATIONSHIPS.body] = formatBrandBody(sectionKey, nz(row.description));
  }
  if (nz(row.value)) out[MAP_BRAND_RELATIONSHIPS.extra] = formatBrandExtra(sectionKey, nz(row.value));

  if (sectionKey === "portfolioMix") {
    const title = nz(row.brandFlagType || row.title);
    if (!title) return null;
    out[MAP_BRAND_RELATIONSHIPS.title] = title;
    out[MAP_BRAND_RELATIONSHIPS.subtitle] = nz(row.portfolioMix || row.subtitle);
    out[MAP_BRAND_RELATIONSHIPS.body] = formatBrandBody(
      sectionKey,
      nz(row.assetContext || row.body)
    );
    out[MAP_BRAND_RELATIONSHIPS.extra] = formatBrandExtra(
      sectionKey,
      nz(row.relationshipStatus || row.extra)
    );
  }
  if (sectionKey === "relationshipDepth") {
    const title = nz(row.brandSegment || row.title);
    if (!title) return null;
    out[MAP_BRAND_RELATIONSHIPS.title] = title;
    out[MAP_BRAND_RELATIONSHIPS.subtitle] = nz(row.relationshipType || row.subtitle);
    out[MAP_BRAND_RELATIONSHIPS.body] = formatBrandBody(
      sectionKey,
      nz(row.ownerContext || row.body)
    );
    out[MAP_BRAND_RELATIONSHIPS.extra] = formatBrandExtra(sectionKey, nz(row.depth || row.extra));
  }
  if (sectionKey === "softIndependent") {
    const text = nz(row.narrative || row.body || row.description);
    if (!text) return null;
    out[MAP_BRAND_RELATIONSHIPS.rowKey] = "brand_soft_independent_narrative";
    out[MAP_BRAND_RELATIONSHIPS.body] = formatBrandBody(sectionKey, text);
  }

  return out;
}

export function buildBrandRelationshipsAirtableRows(body) {
  const br =
    body && body.brandRelationships && typeof body.brandRelationships === "object"
      ? body.brandRelationships
      : null;
  if (!br) return [];

  const out = [];
  let order = 1;

  const listSections = [
    ["portfolioMix", br.portfolioMix],
    ["relationshipDepth", br.relationshipDepth],
    ["executionCapabilities", br.executionCapabilities],
    ["governanceCompliance", br.governanceCompliance],
  ];

  listSections.forEach(([sectionKey, list]) => {
    if (!Array.isArray(list)) return;
    list.forEach((row) => {
      const mapped = mapBrandRelationshipsRowToAirtable({ ...row, sectionKey }, order++);
      if (mapped) out.push(mapped);
    });
  });

  const narrative = nz(br.softIndependentNarrative);
  if (narrative) {
    const mapped = mapBrandRelationshipsRowToAirtable(
      { sectionKey: "softIndependent", narrative },
      order++
    );
    if (mapped) out.push(mapped);
  }

  const narratives = br.narratives && typeof br.narratives === "object" ? br.narratives : {};
  for (const spec of BRAND_NARRATIVE_SPECS) {
    const text = nz(narratives[spec.rowKey]);
    if (!text) continue;
    out.push(
      baseRow("brandNarrative", order++, {
        [MAP_BRAND_RELATIONSHIPS.rowKey]: spec.rowKey,
        [MAP_BRAND_RELATIONSHIPS.title]: spec.title,
        [MAP_BRAND_RELATIONSHIPS.body]: formatBrandBody("brandNarrative", text),
      })
    );
  }

  (br.brandSignals || []).forEach((sig) => {
    const rowKey = nz(sig.rowKey);
    const value = nz(sig.value);
    if (!rowKey || !value) return;
    const spec = BRAND_SIGNAL_SPECS.find((s) => s.rowKey === rowKey);
    out.push(
      baseRow("brandSignal", order++, {
        [MAP_BRAND_RELATIONSHIPS.rowKey]: rowKey,
        [MAP_BRAND_RELATIONSHIPS.title]: nz(sig.title) || spec?.title || rowKey,
        [MAP_BRAND_RELATIONSHIPS.extra]: formatBrandExtra("brandSignal", value),
      })
    );
  });

  (br.snapshotMetrics || []).forEach((kpi) => {
    const rowKey = nz(kpi.rowKey);
    const value = nz(kpi.value);
    if (!rowKey || !value) return;
    out.push(
      baseRow("brandSnapshot", order++, {
        [MAP_BRAND_RELATIONSHIPS.rowKey]: rowKey,
        [MAP_BRAND_RELATIONSHIPS.title]: nz(kpi.title) || rowKey,
        [MAP_BRAND_RELATIONSHIPS.extra]: formatBrandExtra("brandSnapshot", value),
      })
    );
  });

  return out;
}

function nonEmptyTitleRows(list) {
  return (Array.isArray(list) ? list : []).filter(
    (row) =>
      row &&
      (nz(row.title) ||
        nz(row.brandFlagType) ||
        nz(row.brandSegment) ||
        nz(row.brand || row.name))
  );
}

export function buildBrandRelationshipsPayloadFromIntakeBody(body) {
  const b = body || {};
  const incoming =
    b.brandRelationships && typeof b.brandRelationships === "object"
      ? b.brandRelationships
      : {};

  let portfolioMix = nonEmptyTitleRows(incoming.portfolioMix);
  if (!portfolioMix.length) {
    portfolioMix = parseJsonArray(b.brand_portfolio_mix_json);
  }

  let relationshipDepth = (incoming.relationshipDepth || []).filter(
    (r) => r && (nz(r.brandSegment) || nz(r.segment))
  );
  if (!relationshipDepth.length) {
    relationshipDepth = parseJsonArray(b.brand_relationship_depth_json);
  }

  let executionCapabilities = nonEmptyTitleRows(incoming.executionCapabilities);
  if (!executionCapabilities.length) {
    executionCapabilities = parseJsonArray(b.brand_execution_capabilities_json);
  }

  let governanceCompliance = nonEmptyTitleRows(incoming.governanceCompliance);
  if (!governanceCompliance.length) {
    governanceCompliance = parseJsonArray(b.brand_governance_compliance_json);
  }

  let softIndependentNarrative = nz(incoming.softIndependentNarrative);
  if (!softIndependentNarrative) {
    softIndependentNarrative = nz(b.brand_soft_independent_narrative);
  }

  const narratives = { ...(incoming.narratives || {}) };
  for (const spec of BRAND_NARRATIVE_SPECS) {
    if (nz(narratives[spec.rowKey])) continue;
    const text = pickField(b, spec.formKeys);
    if (text) narratives[spec.rowKey] = text;
  }

  const brandSignals = [];
  for (const spec of BRAND_SIGNAL_SPECS) {
    const fromIncoming = (incoming.brandSignals || []).find(
      (s) => nz(s.rowKey) === spec.rowKey
    );
    const value = nz(fromIncoming?.value ?? pickField(b, spec.formKeys));
    if (!value) continue;
    brandSignals.push({
      rowKey: spec.rowKey,
      title: spec.title,
      value,
    });
  }

  const snapshotMetrics = Array.isArray(incoming.snapshotMetrics)
    ? incoming.snapshotMetrics.filter((k) => k && nz(k.rowKey) && nz(k.value))
    : [];

  return {
    portfolioMix,
    relationshipDepth,
    executionCapabilities,
    governanceCompliance,
    softIndependentNarrative,
    narratives,
    brandSignals,
    snapshotMetrics,
  };
}

/**
 * Profile JSON → child rows for migration.
 * @param {Record<string, unknown>} profileFields
 */
export function buildBrandRelationshipsAirtableRowsFromLegacy(profileFields) {
  const profile = profileFields || {};
  const rows = [];
  const sources = [];
  let order = 1;

  for (const spec of JSON_SECTION_SPECS) {
    const raw = pickField(profile, [spec.formKey]);
    const parsed = parseJsonArray(raw);
    if (!parsed.length) continue;
    parsed.forEach((item) => {
      const mapped = spec.mapItem(item, order++);
      if (mapped) rows.push(mapped);
    });
    sources.push(spec.formKey);
  }

  const softText = pickField(profile, ["brand_soft_independent_narrative"]);
  if (softText) {
    rows.push(
      baseRow("softIndependent", order++, {
        [MAP_BRAND_RELATIONSHIPS.rowKey]: "brand_soft_independent_narrative",
        [MAP_BRAND_RELATIONSHIPS.body]: formatBrandBody("softIndependent", softText),
      })
    );
    sources.push("brand_soft_independent_narrative");
  }

  for (const spec of BRAND_NARRATIVE_SPECS) {
    const text = pickField(profile, spec.formKeys);
    if (!text) continue;
    rows.push(
      baseRow("brandNarrative", order++, {
        [MAP_BRAND_RELATIONSHIPS.rowKey]: spec.rowKey,
        [MAP_BRAND_RELATIONSHIPS.title]: spec.title,
        [MAP_BRAND_RELATIONSHIPS.body]: formatBrandBody("brandNarrative", text),
      })
    );
    sources.push(spec.rowKey);
  }

  for (const spec of BRAND_SIGNAL_SPECS) {
    const value = pickField(profile, spec.formKeys);
    if (!value) continue;
    rows.push(
      baseRow("brandSignal", order++, {
        [MAP_BRAND_RELATIONSHIPS.rowKey]: spec.rowKey,
        [MAP_BRAND_RELATIONSHIPS.title]: spec.title,
        [MAP_BRAND_RELATIONSHIPS.extra]: formatBrandExtra("brandSignal", value),
      })
    );
    sources.push(spec.rowKey);
  }

  const countsBySection = {};
  rows.forEach((row) => {
    const label = row[MAP_BRAND_RELATIONSHIPS.section] || "unknown";
    countsBySection[label] = (countsBySection[label] || 0) + 1;
  });

  return { rows, sources, countsBySection };
}

/** Mirror child-table data onto legacy Profile prefill keys (read-path compat). */
export function applyBrandRelationshipsToLegacyPrefill(prefill, platform) {
  if (!prefill || !platform) return prefill;

  if (platform.portfolioMix?.length) {
    prefill.brand_portfolio_mix_json = JSON.stringify(platform.portfolioMix);
  }
  if (platform.relationshipDepth?.length) {
    prefill.brand_relationship_depth_json = JSON.stringify(platform.relationshipDepth);
  }
  if (platform.executionCapabilities?.length) {
    prefill.brand_execution_capabilities_json = JSON.stringify(
      platform.executionCapabilities.map((r) => ({
        title: r.title,
        description: r.description,
      }))
    );
  }
  if (platform.governanceCompliance?.length) {
    prefill.brand_governance_compliance_json = JSON.stringify(
      platform.governanceCompliance.map((r) => ({
        title: r.title,
        description: r.description,
      }))
    );
  }
  if (nz(platform.softIndependentNarrative)) {
    prefill.brand_soft_independent_narrative = platform.softIndependentNarrative;
  }

  const narratives = platform.narratives || {};
  for (const spec of BRAND_NARRATIVE_SPECS) {
    const text = nz(narratives[spec.rowKey]);
    if (text) prefill[spec.rowKey] = text;
  }

  (platform.brandSignals || []).forEach((sig) => {
    const key = nz(sig.rowKey);
    const val = nz(sig.value);
    if (key && val) prefill[key] = val;
  });

  prefill.brandRelationships = platform;
  return prefill;
}

export class BrandRelationshipsValidationError extends Error {
  constructor(errors) {
    super("Brand & Relationships validation failed");
    this.name = "BrandRelationshipsValidationError";
    this.errors = errors;
    this.statusCode = 400;
  }
}

export function validateBrandRelationshipsPayload(platform) {
  const errors = [];
  const p = platform && typeof platform === "object" ? platform : {};
  (p.portfolioMix || []).forEach((row, i) => {
    if (!nz(row.brandFlagType) && !nz(row.brand) && !nz(row.title)) {
      errors.push({
        field: `portfolioMix[${i}]`,
        message: "Portfolio mix row needs brand / flag type",
      });
    }
  });
  return { valid: errors.length === 0, errors };
}
