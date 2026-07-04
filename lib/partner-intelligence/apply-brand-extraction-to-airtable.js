/**
 * Apply source-extracted brand facts → Brand Setup + Brand Explorer presentation.
 * Only writes values with dataGap !== "Yes". Source paths stay in PI facts / Explorer Hero Data Source — not in display copy.
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { BRAND_EXPLORER_FIELDS } from "../../api/lib/partner-intelligence-explorer-field-registry.js";
import { applyKimptonGlobalReplacements } from "../kimpton-brand-explorer-presentation-overrides.js";
import {
  FLEXIBILITY_SLOT_KEYS,
  flexLevelForSlot,
  normalizeFlexSlotBody,
} from "../brand-explorer-flexibility-levels.mjs";
import { applyIhgLoyaltyPresentationSlots } from "./build-ihg-loyalty-presentation-slots.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..", "..");

function nz(v) {
  if (v == null) return "";
  return String(v).trim();
}

/**
 * @param {Array<{ fieldKey: string, extractedValue: string, evidenceText?: string, dataGap?: string, pageSectionAnchor?: string, _sourceTitle?: string }>} mergedFacts
 */
export function buildBrandSetupPatchFromFacts(mergedFacts, pilot = {}) {
  const byKey = new Map(mergedFacts.map((f) => [f.fieldKey, f]));
  const basics = {};
  const loyalty = {};
  const footprint = {};
  const fee = {};

  for (const field of BRAND_EXPLORER_FIELDS) {
    const fact = byKey.get(field.fieldKey);
    if (!fact || fact.dataGap === "Yes") continue;
    const val = nz(fact.extractedValue);
    if (!val) continue;

    if (field.brandSetupBasicsField) {
      basics[field.brandSetupBasicsField] = val;
    }
    if (field.brandSetupLoyaltyField) {
      const num = Number(val);
      loyalty[field.brandSetupLoyaltyField] = Number.isFinite(num) ? num : val;
    }
    if (field.brandSetupFootprintField) {
      footprint[field.brandSetupFootprintField] = val;
    }
    if (field.brandSetupFeeField) {
      const num = Number(val);
      fee[field.brandSetupFeeField] = Number.isFinite(num) ? num : val;
      if (field.fieldKey === "be.economics.royaltyPct") {
        fee["Max - Typical Royalty Fee Range"] = Number.isFinite(num) ? num : val;
        fee["Additional Notes - Typical Royalty Fee Range"] =
          `Per extracted FDD/reference material. Evidence: ${nz(fact.evidenceText).slice(0, 200)}`;
      }
    }
  }

  if (pilot.brandName) basics["Brand Name"] = pilot.brandName;
  if (pilot.parentCompany) basics["Parent Company"] = pilot.parentCompany;
  basics["Explorer Hero Data Source"] =
    "IHG Brand Reference Material folder — Partner Intelligence extraction (rules). Human review required.";
  basics["Explorer Hero Verification"] = "Verified";

  const childTables = {};
  if (Object.keys(basics).length) childTables["Brand Setup - Brand Basics"] = basics;
  if (Object.keys(loyalty).length) childTables["Brand Setup - Loyalty & Commercial"] = loyalty;
  if (Object.keys(footprint).length) childTables["Brand Setup - Brand Footprint"] = footprint;
  if (Object.keys(fee).length) childTables["Brand Setup - Fee Structure"] = fee;

  return { basics, childTables };
}

/**
 * Overlay extracted facts onto presentation rows (by slotKey).
 */
/** Slot keys that need formatted presentation copy, not raw extracted values. */
const LOYALTY_SLOT_KEYS = new Set([
  "be.loyalty.programName",
  "be.loyalty.memberCount",
  "be.loyalty.roomContributionPct",
  "be.loyalty.enterpriseBookingPct",
]);

export function buildPresentationRowsFromFacts(templateRows, mergedFacts, pilot = {}) {
  const bySlot = new Map();
  for (const field of BRAND_EXPLORER_FIELDS) {
    if (!field.slotKey || LOYALTY_SLOT_KEYS.has(field.fieldKey)) continue;
    const fact = mergedFacts.find((f) => f.fieldKey === field.fieldKey);
    if (!fact || fact.dataGap === "Yes") continue;
    const body = nz(fact.extractedValue);
    bySlot.set(field.slotKey, applyKimptonGlobalReplacements(body));
  }

  const overlaid = templateRows.map((row) => {
    const hit = bySlot.get(row.slotKey);
    if (!hit) return { ...row };
    return { ...row, body: hit };
  });

  const withLoyalty = applyIhgLoyaltyPresentationSlots(overlaid, mergedFacts, {
    brandName: pilot.brandName,
  });

  // Flexibility indicators: canonical levels only (Radisson Blu parity); narrative stays in standards_philosophy.
  const segment = "softCollection";
  return withLoyalty.map((row) => {
    const slotKey = nz(row.slotKey);
    if (!FLEXIBILITY_SLOT_KEYS.includes(slotKey)) return row;
    const { level } = normalizeFlexSlotBody(slotKey, row.body, segment);
    const canonical = flexLevelForSlot(segment, slotKey);
    const body = level && level.length <= 24 ? level : canonical;
    return { ...row, body };
  });
}

export function writeKimptonExtractionArtifacts(mergedFacts, pilot, templateFixturePath) {
  const patch = buildBrandSetupPatchFromFacts(mergedFacts, pilot);
  const template = JSON.parse(fs.readFileSync(templateFixturePath, "utf8"));
  const rows = buildPresentationRowsFromFacts(template.rows, mergedFacts, pilot);

  const setupOut = {
    brandName: pilot.brandName,
    basicsRecordId: pilot.recordId,
    parentCompany: pilot.parentCompany,
    sourceUrls: mergedFacts
      .filter((f) => f.dataGap !== "Yes")
      .map((f) => f.pageSectionAnchor)
      .filter(Boolean),
    extractionRun: new Date().toISOString(),
    basics: patch.basics,
    childTables: patch.childTables,
  };

  const presentationOut = {
    targetBrandBasicsName: pilot.brandName,
    brandNameFallback: pilot.brandName,
    instructions:
      'Source-grounded fixture. Apply: node scripts/apply-brand-explorer-presentation-fixture.mjs --brand-name "Kimpton Hotels" --fixture fixtures/brand-explorer-presentation-kimpton-from-sources.json --replace',
    rows,
  };

  const setupPath = path.join(ROOT, "fixtures", "kimpton-brand-setup-from-sources.json");
  const presentationPath = path.join(
    ROOT,
    "fixtures",
    "brand-explorer-presentation-kimpton-from-sources.json"
  );
  fs.writeFileSync(setupPath, JSON.stringify(setupOut, null, 2));
  fs.writeFileSync(presentationPath, JSON.stringify(presentationOut, null, 2));

  return { setupPath, presentationPath, patch };
}
