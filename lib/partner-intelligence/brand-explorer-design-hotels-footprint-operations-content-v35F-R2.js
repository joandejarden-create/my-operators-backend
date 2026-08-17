/**
 * Design Hotels footprint + operations backfill content packages v35F-R2.
 * Affiliation / curation language — no source footnotes in owner-facing body.
 */
import {
  DESIGN_HOTELS_SOURCE_IDS,
} from "./brand-explorer-design-hotels-content-packages-v35F.js";

export const V35F_R2_CONTENT_VERSION = "v35F-R2";

/** Slots targeted by R2 (creates + region format normalization). */
export const V35F_R2_SLOT_KEYS = Object.freeze([
  "footprint.region.am",
  "footprint.region.cala",
  "footprint.region.eu",
  "footprint.region.mea",
  "footprint.region.apac",
  "footprint.growth_themes",
  "footprint.growth_editorial",
  "footprint.growth_fit",
  "operations.flexibility.design",
  "operations.flexibility.conversion",
  "operations.flexibility.localization",
  "operations.flexibility.operational_rigidity",
  "operations.flexibility.pip",
  "operations.flexibility.prototype",
  "operations.compliance.qa_cadence",
  "operations.compliance.training_rigor",
  "operations.compliance.reporting",
  "operations.compliance.brand_interaction",
  "operations.model.staffing_intensity",
  "operations.model.fb_complexity",
  "operations.model.training",
  "operations.model.reporting_discipline",
  "operations.model.qa_rhythm",
  "operations.model.technology",
  "operations.operator_compat.summary",
  "operations.operator_compat.fit",
  "operations.operator_compat.tags",
]);

function pkg(slotKey, title, body, meta = {}) {
  return {
    slotKey,
    title: title || "",
    body,
    tab: meta.tab || tabForSlot(slotKey),
    sort: meta.sort ?? 0,
    sourceIds: meta.sourceIds || [],
  };
}

function tabForSlot(slotKey) {
  if (slotKey.startsWith("footprint.")) return "Footprint & Growth";
  if (slotKey.startsWith("operations.")) return "Operating Model";
  return "Footprint & Growth";
}

function regionBody(narrative) {
  return `Directional presence\n\n${narrative}`;
}

/**
 * @param {Map<string, { id: string, sourceTitle?: string, sourceUrl?: string }>} [sourcesById]
 */
export function buildDesignHotelsFootprintOperationsContentPackagesV35FR2(sourcesById = new Map()) {
  const S = DESIGN_HOTELS_SOURCE_IDS;
  const directorySources = [S.directory, S.about].filter((id) => sourcesById.has(id) || true);
  const opsSources = [S.about, S.member, S.consumer].filter((id) => sourcesById.has(id) || true);

  return [
    pkg(
      "footprint.region.am",
      "",
      regionBody(
        "Americas member hotels span urban and resort contexts in the public directory—illustrative regional presence only, not property-level counts."
      ),
      { sourceIds: [S.directory] }
    ),
    pkg(
      "footprint.region.cala",
      "",
      regionBody(
        "CALA member hotels appear in the global directory—including curated CALA property examples on this Explorer profile for owner reference."
      ),
      { sourceIds: [S.directory] }
    ),
    pkg(
      "footprint.region.eu",
      "",
      regionBody(
        "European presence reflects Design Hotels' founding context and ongoing member base—directory reference only."
      ),
      { sourceIds: [S.directory, S.about] }
    ),
    pkg(
      "footprint.region.mea",
      "",
      regionBody(
        "Middle East and Africa member hotels appear selectively in the global directory—confirm current membership and market priorities directly with Design Hotels."
      ),
      { sourceIds: directorySources }
    ),
    pkg(
      "footprint.region.apac",
      "",
      regionBody(
        "Asia Pacific member hotels reflect Design Hotels' global curation footprint in the public directory—directory reference only, not a development pipeline disclosure."
      ),
      { sourceIds: directorySources }
    ),
    pkg(
      "footprint.growth_themes",
      "",
      [
        "Design-led independent conversions",
        "Urban cultural destinations",
        "Resort and leisure experiential markets",
        "Global curation expansion",
        "Marriott Bonvoy ecosystem alignment",
      ].join("\n"),
      { sourceIds: [S.about, S.directory] }
    ),
    pkg(
      "footprint.growth_editorial",
      "",
      "Design Hotels growth follows curation and member-hotel quality—not standardized prototype rollout. Owners should confirm collection priorities and market interest directly with the brand platform.",
      { sourceIds: [S.about, S.member] }
    ),
    pkg(
      "footprint.growth_fit",
      "",
      [
        "Distinctive boutique assets with established guest experience",
        "Design-led conversions preserving local identity",
        "Resort and urban lifestyle hotels seeking global collection recognition",
      ].join("\n"),
      { sourceIds: [S.about, S.member] }
    ),
    pkg("operations.flexibility.design", "", "Very high", { sourceIds: opsSources }),
    pkg("operations.flexibility.conversion", "", "High", { sourceIds: opsSources }),
    pkg("operations.flexibility.localization", "", "Very high", { sourceIds: opsSources }),
    pkg("operations.flexibility.operational_rigidity", "", "Low", { sourceIds: opsSources }),
    pkg("operations.flexibility.pip", "", "Moderate", { sourceIds: opsSources }),
    pkg("operations.flexibility.prototype", "", "Low", { sourceIds: opsSources }),
    pkg(
      "operations.compliance.qa_cadence",
      "",
      "Periodic design and experience review cycles—not uniform chain inspection calendars.",
      { sourceIds: opsSources }
    ),
    pkg(
      "operations.compliance.training_rigor",
      "",
      "Collection orientation plus property-specific onboarding for operating teams.",
      { sourceIds: opsSources }
    ),
    pkg(
      "operations.compliance.reporting",
      "",
      "Property-level reporting aligned with affiliation agreement and Marriott ecosystem requirements where applicable.",
      { sourceIds: opsSources }
    ),
    pkg(
      "operations.compliance.brand_interaction",
      "",
      "Curation and design review touchpoints—frequency varies by membership stage and repositioning scope.",
      { sourceIds: opsSources }
    ),
    pkg("operations.model.staffing_intensity", "", "Moderate to high", { sourceIds: opsSources }),
    pkg("operations.model.fb_complexity", "", "Variable by property", { sourceIds: opsSources }),
    pkg("operations.model.training", "", "Moderate", { sourceIds: opsSources }),
    pkg("operations.model.reporting_discipline", "", "Moderate", { sourceIds: opsSources }),
    pkg(
      "operations.model.qa_rhythm",
      "",
      "Periodic design and experience review aligned with collection standards.",
      { sourceIds: opsSources }
    ),
    pkg(
      "operations.model.technology",
      "",
      "Moderate—distribution and systems integration where Bonvoy participation applies.",
      { sourceIds: opsSources }
    ),
    pkg(
      "operations.operator_compat.summary",
      "",
      "Best with owners and operators who preserve independent hotel character, design narrative, and localized F&B—full-service or boutique lifestyle capability preferred over limited-service prototypes.",
      { sourceIds: opsSources }
    ),
    pkg(
      "operations.operator_compat.fit",
      "",
      "Strong fit when distinctive architecture, cultural programming, and guest experience already differentiate the asset before affiliation.",
      { sourceIds: opsSources }
    ),
    pkg(
      "operations.operator_compat.tags",
      "",
      [
        "Design-led",
        "Independent character",
        "Boutique lifestyle",
        "Urban cultural",
        "Resort experiential",
        "Conversion-minded",
      ].join("\n"),
      { sourceIds: opsSources }
    ),
  ];
}

export function regionBodyHasDirectionalFormat(body) {
  const blob = body == null ? "" : String(body).trim();
  if (!blob) return false;
  const paras = blob.split(/\n\s*\n/).map((p) => p.trim()).filter(Boolean);
  if (paras.length < 2) return false;
  return /^directional presence$/i.test(paras[0]) || paras[0].length <= 40;
}
