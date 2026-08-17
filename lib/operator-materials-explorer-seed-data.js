/**
 * Operator Materials Explorer seed payloads (Governance table).
 * UI parity: Brand Explorer Official Brand Materials + Image Gallery.
 *
 * Gallery URLs verified HTTP 200; paired with captions (see brand-education-atelier-north.html).
 */

const SAMPLE_PDF = "https://www.w3.org/WAI/ER/tests/xhtml/testfiles/resources/pdf/dummy.pdf";

const U = "https://images.unsplash.com/photo-";
const Q = "?auto=format&fit=crop&w=1200&q=80";

/** Title + imageUrl kept in sync (Proper Case captions). */
export const OPERATOR_MATERIALS_GALLERY_SLOTS = [
  {
    title: "Resort Exterior",
    imageUrl: `${U}1566073771259-6a8506099945${Q}`,
  },
  {
    title: "Hotel Lobby & Lounge",
    imageUrl: `${U}1775866914943-ba1415a35afc${Q}`,
  },
  {
    title: "Guest Room",
    imageUrl: `${U}1631049307264-da0ec9d70304${Q}`,
  },
  {
    title: "Resort Pool & Terrace",
    imageUrl: `${U}1571896349842-33c89424de2d${Q}`,
  },
  {
    title: "Restaurant & Dining Room",
    imageUrl: `${U}1414235077428-338989a2e8c0${Q}`,
  },
  {
    title: "Front Desk & Reception",
    imageUrl: `${U}1775447665921-87fb172bf115${Q}`,
  },
];

/**
 * @param {{ companyName?: string }} opts
 * @returns {{ operator_materials_json: string, operator_materials_gallery_json: string, diligenceDocumentLinks: string }}
 */
export function buildOperatorMaterialsSeedFields(opts) {
  opts = opts || {};
  const company = String(opts.companyName || "this operator").trim() || "this operator";

  const files = [
    {
      title: "Company Overview & Platform",
      body:
        company +
        " overview for owners: CALA footprint, management platform, reporting rhythm, and typical transition support for resort and urban assets.",
      href: SAMPLE_PDF,
      kind: "PDF",
      badge: "Operator provided",
    },
    {
      title: "Owner Reporting & Governance Pack",
      body:
        "Sample owner reporting calendar, committee cadence, and KPI pack structure used on institutional and single-asset assignments.",
      href: SAMPLE_PDF,
      kind: "PDF",
      badge: "Operator provided",
    },
    {
      title: "CALA Portfolio & Market Snapshot",
      body:
        "Illustrative markets, chain-scale mix, and operating situations " +
        company +
        " pursues across Mexico, Central America, and the Caribbean.",
      href: SAMPLE_PDF,
      kind: "PDF",
      badge: "Operator provided",
    },
  ];

  const gallery = OPERATOR_MATERIALS_GALLERY_SLOTS.map(function (slot) {
    return { title: slot.title, imageUrl: slot.imageUrl };
  });

  const payload = { files, gallery };

  return {
    operator_materials_json: JSON.stringify(payload, null, 2),
    operator_materials_gallery_json: JSON.stringify(gallery, null, 2),
    diligenceDocumentLinks: [SAMPLE_PDF, "https://pdfobject.com/pdf/sample.pdf"].join("\n"),
  };
}
