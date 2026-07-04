/**

 * Operator Materials — Airtable field map.
 * Canonical: Operator Setup - Explorer Materials (presentation rows).
 * Legacy fallback: Governance JSON fields.
 * UI: public/js/operator-dna-materials.js
 */

import { OPERATOR_EXPLORER_MATERIALS_TABLE } from "./operator-materials-explorer-presentation-map.js";

export const OPERATOR_MATERIALS_PRESENTATION_TABLE = OPERATOR_EXPLORER_MATERIALS_TABLE;
export const OPERATOR_MATERIALS_TABLE = "Operator Setup - Governance, Delivery & Diligence";



export const OPERATOR_MATERIALS_JSON_KEY = "operator_materials_json";

export const OPERATOR_MATERIALS_GALLERY_JSON_KEY = "operator_materials_gallery_json";



/** @type {Record<string, { airtableField: string, table: string, type: string, uiUse: string }>} */

export const MAP_OPERATOR_MATERIALS_FIELDS = {

  [OPERATOR_MATERIALS_JSON_KEY]: {

    airtableField: OPERATOR_MATERIALS_JSON_KEY,

    table: OPERATOR_MATERIALS_TABLE,

    type: "multilineText",

    uiUse: "Operator DNA — file cards (files[] + optional gallery[])",

  },

  [OPERATOR_MATERIALS_GALLERY_JSON_KEY]: {

    airtableField: OPERATOR_MATERIALS_GALLERY_JSON_KEY,

    table: OPERATOR_MATERIALS_TABLE,

    type: "multilineText",

    uiUse: "Operator DNA — Image Gallery (6 slots); overrides gallery[] in materials JSON when set",

  },

  diligenceDocumentLinks: {

    airtableField: "diligenceDocumentLinks",

    table: OPERATOR_MATERIALS_TABLE,

    type: "multilineText",

    uiUse: "Operator DNA — file card fallback URLs (one per line)",

  },

};



/** @type {{ name: string, type: string }[]} */

export const OPERATOR_MATERIALS_EXPLORER_AIRTABLE_FIELD_SPECS = [
  { name: OPERATOR_MATERIALS_JSON_KEY, type: "multilineText" },
  { name: OPERATOR_MATERIALS_GALLERY_JSON_KEY, type: "multilineText" },
  { name: "diligenceDocumentLinks", type: "multilineText" },
];



/**

 * @param {unknown} raw

 * @returns {Array<{ href: string, title: string, kind: string, body: string, badge: string }>}

 */

export function parseOperatorMaterialsFiles(raw) {

  var s = raw == null ? "" : String(raw).trim();

  if (!s) return [];

  var data;

  try {

    data = JSON.parse(s);

  } catch (e) {

    return [];

  }

  var rows = Array.isArray(data) ? data : data && Array.isArray(data.files) ? data.files : [];

  var out = [];

  for (var i = 0; i < rows.length; i++) {

    var row = rows[i];

    if (!row || typeof row !== "object") continue;

    var href = String(row.href || row.url || "").trim();

    if (!href) continue;

    var title = String(row.title || row.name || "").trim() || href;

    var kind = String(row.kind || row.type || "").trim().toUpperCase() || "FILE";

    var body = String(row.body || row.description || row.meta || row.subtitle || "").trim();

    var badge = String(row.badge || "").trim() || "Operator provided";

    out.push({ href, title, kind, body, badge });

  }

  return out;

}


