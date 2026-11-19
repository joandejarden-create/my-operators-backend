/**
 * Phase 2D — production template population inventory.
 * Reconciles Presentation row counts, unique slot keys, renderer references,
 * and collection cardinality without shrinking the denominator to game scores.
 */
import { readFileSync, existsSync } from "fs";
import { join } from "path";
import { repoRoot, CONTENT_CONTRACT, COLLECTION_CONTRACT } from "./canonical/product-shell.js";
import { ALL_TAB_CONTRACT_FIELDS, TAB_CONTRACTS } from "../partner-intelligence/brand-explorer-tab-contracts.js";

export const BE_PHASE2D_TEMPLATE_INVENTORY_V1 = "BE_PHASE2D_TEMPLATE_INVENTORY_V1";

/** Slot families that are multi-row collections in production (same Slot Key, many titles). */
export const MULTI_ROW_SLOT_KEYS = Object.freeze([
  "standards.requirement",
  "commercial.demand",
  "commercial.theme",
  "economics.fee",
  "economics.risk",
  "economics.legal",
  "loyalty.proof",
  "loyalty.elite",
  "footprint.momentum",
  "footprint.openings",
  "footprint.portfolio_mix",
  "materials.caseStudy",
  "materials.file",
  "insight.similar",
]);

/** Legacy / freeform keys that are not part of the canonical atelier contract. */
export const LEGACY_OR_FREEFORM_SLOT_KEYS = Object.freeze([
  "Guest Psychographics Description",
]);

/** Supporting materials — still applicable for full visual quality. */
export const SUPPORTING_MATERIAL_SLOT_PREFIXES = Object.freeze([
  "materials.gallery.",
  "materials.file",
  "materials.caseStudy",
]);

function nz(v) {
  return v == null ? "" : String(v).trim();
}

function collectionMinFor(slotKey) {
  const key = nz(slotKey);
  for (const col of COLLECTION_CONTRACT.collections || []) {
    if (col.slot === key) return col.count || 1;
    if (col.slotPattern && key.startsWith(col.slotPattern)) return 1;
  }
  return MULTI_ROW_SLOT_KEYS.includes(key) ? 2 : 1;
}

/** Trailing-dot patterns / plural aliases are extract artifacts, not renderable slots. */
function isParseArtifactSlot(key) {
  if (/\.$/.test(key)) return true;
  if (/^(overview\.scenarios|valueOwners\.scenarios|overview\.proof_operator)$/.test(key)) return true;
  if (key === "footprint.portfolio_mix.archived") return true;
  return false;
}

function classifySlot(slotKey, histCount = 1) {
  const key = nz(slotKey);
  if (LEGACY_OR_FREEFORM_SLOT_KEYS.includes(key)) {
    return { role: "legacy_freeform", applicableRequired: false, notes: "Legacy freeform label; not a canonical atelier slot" };
  }
  if (isParseArtifactSlot(key)) {
    return {
      role: "parse_artifact_or_alias",
      applicableRequired: false,
      notes: "Trailing-dot pattern or plural alias from extractor — not a distinct applicable field",
    };
  }
  if (MULTI_ROW_SLOT_KEYS.includes(key) || histCount > 1) {
    return {
      role: "repeated_collection_item",
      applicableRequired: true,
      minItems: Math.max(histCount || 1, collectionMinFor(key)),
      notes: "Multiple Presentation rows share this Slot Key (collection)",
    };
  }
  if (SUPPORTING_MATERIAL_SLOT_PREFIXES.some((p) => key === p || key.startsWith(p))) {
    return { role: "supporting_material", applicableRequired: true, notes: "Imagery / files / case studies — required for full profile quality" };
  }
  if (/^economics\.fee|^economics\.kpi|^commercial\.kpi|^commercial\.lever/.test(key)) {
    return {
      role: "nested_content_element",
      applicableRequired: true,
      notes: "Nested economics/commercial element; may be diligence guidance when confidential figures unavailable",
    };
  }
  if (/^branded_residences\.|^hero\./.test(key)) {
    return { role: "optional_or_conditional", applicableRequired: false, notes: "Optional/conditional surface" };
  }
  return { role: "unique_required_field", applicableRequired: true, notes: "Unique required presentation field" };
}

/**
 * Build inventory from Phase 2C baseline snapshot + atelier extraction + contracts.
 */
export function buildProductionTemplateInventory({
  baselinePath = null,
  atelierSlotsPath = null,
} = {}) {
  const root = repoRoot();
  const baselineFile =
    baselinePath || join(root, "reports/brand-explorer/phase2c-curio-reconstruction/01-baseline-snapshot.json");
  const atelierFile =
    atelierSlotsPath ||
    join(root, "reports/brand-explorer/phase2d-curio-quality-recovery/atelier-referenced-slots.json");

  const baseline = existsSync(baselineFile) ? JSON.parse(readFileSync(baselineFile, "utf8")) : null;
  const atelier = existsSync(atelierFile) ? JSON.parse(readFileSync(atelierFile, "utf8")) : { slots: [] };
  const hist = baseline?.presentation?.slotHistogram || {};
  const baselineSlots = Object.keys(hist);
  const atelierSlots = atelier.slots || [];

  const union = new Set([...baselineSlots, ...atelierSlots]);
  for (const item of CONTENT_CONTRACT.staticRequired) union.add(item.itemId);
  for (const f of ALL_TAB_CONTRACT_FIELDS) union.add(f.slotKey || f.fieldId);

  const rows = [...union].sort().map((slotKey) => {
    const count = hist[slotKey] || 0;
    const cls = classifySlot(slotKey, count);
    const inBaseline = count > 0;
    const inAtelier = atelierSlots.includes(slotKey);
    const inContentContract = CONTENT_CONTRACT.staticRequired.some((i) => i.itemId === slotKey);
    const inTabContract = ALL_TAB_CONTRACT_FIELDS.some((f) => (f.slotKey || f.fieldId) === slotKey);
    return {
      slotKey,
      baselineRowCount: count,
      inBaseline,
      inAtelierDocs: inAtelier,
      inContentContract,
      inTabContract,
      ...cls,
      tabHint:
        TAB_CONTRACTS.find((t) =>
          t.sections.some((s) => s.fields.some((f) => (f.slotKey || f.fieldId) === slotKey)),
        )?.tabId || null,
    };
  });

  const applicable = rows.filter((r) => r.applicableRequired);
  const multiRowBaselineExtras = Object.entries(hist)
    .filter(([, n]) => n > 1)
    .reduce((sum, [, n]) => sum + (n - 1), 0);

  return {
    contract: BE_PHASE2D_TEMPLATE_INVENTORY_V1,
    generatedAt: new Date().toISOString(),
    reconciliation: {
      baselinePresentationRows: baseline?.presentation?.rowCount || null,
      uniqueBaselineSlotKeys: baselineSlots.length,
      multiRowExtraRows: multiRowBaselineExtras,
      atelierReferencedSlots: atelierSlots.length,
      unionSlotKeys: rows.length,
      applicableRequiredSlots: applicable.length,
      nonApplicableSlots: rows.length - applicable.length,
      note: "229 rows = unique slots + repeated collection children. Completeness denominator = applicableRequiredSlots (unique keys), not raw row count. Do not shrink denominator.",
    },
    roleCounts: rows.reduce((acc, r) => {
      acc[r.role] = (acc[r.role] || 0) + 1;
      return acc;
    }, {}),
    slots: rows,
    applicableSlotKeys: applicable.map((r) => r.slotKey),
  };
}
