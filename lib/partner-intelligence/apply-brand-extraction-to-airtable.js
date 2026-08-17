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
import { applyHiltonLoyaltyPresentationSlots } from "./build-hilton-loyalty-presentation-slots.js";
import { overlayCurioCalaMaterials } from "../curio-brand-explorer-cala-materials.js";
import { applyBrandedResidencesFromFacts } from "./brand-residences-status-setup.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..", "..");

function nz(v) {
  if (v == null) return "";
  return String(v).trim();
}

function isHiltonPilot(pilot = {}) {
  const parent = nz(pilot.parentCompany).toLowerCase();
  const folder = nz(pilot.referenceFolder).toLowerCase();
  const program = nz(pilot.loyaltyProgram).toLowerCase();
  return program === "hiltonhonors" || parent.includes("hilton") || folder === "hilton";
}

function isCurioPilot(pilot = {}) {
  return pilot.key === "curioCollection" || nz(pilot.brandSlug) === "curio";
}

function applyLoyaltyPresentationSlots(templateRows, mergedFacts, pilot = {}) {
  if (isHiltonPilot(pilot)) {
    return applyHiltonLoyaltyPresentationSlots(templateRows, mergedFacts, { brandName: pilot.brandName });
  }
  return applyIhgLoyaltyPresentationSlots(templateRows, mergedFacts, { brandName: pilot.brandName });
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
      const loyaltyVal = Number.isFinite(num) ? num : val;
      if (
        field.brandSetupLoyaltyField === "Typical Loyalty Program Name" &&
        isHiltonPilot(pilot) &&
        /ihg/i.test(String(loyaltyVal))
      ) {
        continue;
      }
      loyalty[field.brandSetupLoyaltyField] = loyaltyVal;
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
  Object.assign(basics, applyBrandedResidencesFromFacts(byKey));
  if (isHiltonPilot(pilot)) {
    loyalty["Typical Loyalty Program Name"] = "Hilton Honors";
  }
  const folderLabel = pilot.referenceFolder || pilot.parentCompany || "Brand Reference Material";
  basics["Explorer Hero Data Source"] =
    `${folderLabel} folder — Partner Intelligence extraction (rules). Human review required.`;
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

  const withLoyalty = applyLoyaltyPresentationSlots(overlaid, mergedFacts, pilot);

  // Flexibility indicators: canonical levels only (Radisson Blu parity); narrative stays in standards_philosophy.
  const segment = "softCollection";
  const withFlex = withLoyalty.map((row) => {
    const slotKey = nz(row.slotKey);
    if (!FLEXIBILITY_SLOT_KEYS.includes(slotKey)) return row;
    const { level } = normalizeFlexSlotBody(slotKey, row.body, segment);
    const canonical = flexLevelForSlot(segment, slotKey);
    const body = level && level.length <= 24 ? level : canonical;
    return { ...row, body };
  });

  return isCurioPilot(pilot) ? overlayCurioCalaMaterials(withFlex) : withFlex;
}

export function writeKimptonExtractionArtifacts(mergedFacts, pilot, templateFixturePath, artifactBasename = "kimpton") {
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
    instructions: `Source-grounded fixture. Apply: node scripts/apply-brand-explorer-presentation-fixture.mjs --brand-name "${pilot.brandName}" --fixture fixtures/brand-explorer-presentation-${artifactBasename}-from-sources.json --replace`,
    rows,
  };

  const setupPath = path.join(ROOT, "fixtures", `${artifactBasename}-brand-setup-from-sources.json`);
  const presentationPath = path.join(
    ROOT,
    "fixtures",
    `brand-explorer-presentation-${artifactBasename}-from-sources.json`
  );
  fs.writeFileSync(setupPath, JSON.stringify(setupOut, null, 2));
  fs.writeFileSync(presentationPath, JSON.stringify(presentationOut, null, 2));

  return { setupPath, presentationPath, patch };
}
