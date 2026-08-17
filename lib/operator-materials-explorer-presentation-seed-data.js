/**
 * Seed rows for Operator Setup - Explorer Materials (Brand Explorer Presentation parity).
 */

import {
  OPERATOR_MATERIALS_GALLERY_SLOT_PREFIX,
  OPERATOR_MATERIALS_SLOT_FILE,
} from "../api/lib/operator-materials-explorer-presentation-map.js";
import { OPERATOR_MATERIALS_GALLERY_SLOTS } from "./operator-materials-explorer-seed-data.js";

const SAMPLE_PDF = "https://www.w3.org/WAI/ER/tests/xhtml/testfiles/resources/pdf/dummy.pdf";
const SAMPLE_PDF_2 = "https://pdfobject.com/pdf/sample.pdf";

/**
 * @param {{ companyName?: string, index?: number }} opts
 * @returns {Array<{ slotKey: string, title: string, body: string, sort: number, imageUrl?: string }>}
 */
export function buildOperatorMaterialsPresentationRows(opts) {
  opts = opts || {};
  const company = String(opts.companyName || "this operator").trim() || "this operator";
  const idx = Number(opts.index) || 0;
  const pdfUrl = idx % 2 === 0 ? SAMPLE_PDF : SAMPLE_PDF_2;

  const files = [
    {
      title: "Company Overview & Platform",
      body:
        company +
        " overview for owners: CALA footprint, management platform, reporting rhythm, and transition support for resort and urban assets.\n\n" +
        pdfUrl +
        "\n\nBadge: Operator provided",
      sort: 0,
      imageUrl: pdfUrl,
    },
    {
      title: "Owner Reporting & Governance Pack",
      body:
        "Sample owner reporting calendar, committee cadence, and KPI pack structure used on institutional and single-asset assignments.\n\n" +
        SAMPLE_PDF +
        "\n\nBadge: Operator provided",
      sort: 1,
      imageUrl: SAMPLE_PDF,
    },
    {
      title: "CALA Portfolio & Market Snapshot",
      body:
        "Illustrative markets, chain-scale mix, and operating situations " +
        company +
        " pursues across Mexico, Central America, and the Caribbean.\n\n" +
        SAMPLE_PDF_2 +
        "\n\nBadge: Operator provided",
      sort: 2,
      imageUrl: SAMPLE_PDF_2,
    },
  ];

  const rows = files.map((f) => ({
    slotKey: OPERATOR_MATERIALS_SLOT_FILE,
    title: f.title,
    body: f.body,
    sort: f.sort,
    imageUrl: f.imageUrl,
  }));

  OPERATOR_MATERIALS_GALLERY_SLOTS.forEach((slot, i) => {
    rows.push({
      slotKey: OPERATOR_MATERIALS_GALLERY_SLOT_PREFIX + (i + 1),
      title: slot.title,
      body: "",
      sort: i + 1,
      imageUrl: slot.imageUrl,
    });
  });

  return rows;
}
