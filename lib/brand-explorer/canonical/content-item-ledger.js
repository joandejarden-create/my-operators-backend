/**
 * Item ledger. The denominator is base canonical items plus instantiated collection children.
 * Every new item starts as RESEARCH_REQUIRED until evidence sets a disposition.
 */
import { COLLECTION_CONTRACT, CONTENT_CONTRACT } from "./product-shell.js";

const CHILD_COLLECTIONS = [
  { id: "representativeProperties", itemPrefix: "property" },
  { id: "recentMomentum", itemPrefix: "momentum" },
  { id: "similarBrands", itemPrefix: "similar" },
  { id: "loyaltyTiers", itemPrefix: "loyalty.tier" },
  { id: "loyaltyBenefits", itemPrefix: "loyalty.benefit" },
  { id: "commercialThemes", itemPrefix: "commercial.theme" },
];

function childCount(spec, input) {
  if (spec.mode === "CONDITIONAL") {
    const enabled = input?.conditionals?.[spec.when] === true;
    if (!enabled) return 0;
    if (spec.countMode === "ALL_SOURCE_MEMBERS") return input?.sourceMemberCounts?.[spec.id] ?? 0;
    return spec.count || 0;
  }
  if (spec.mode === "EXACT" || spec.mode === "MINIMUM") return spec.count || 0;
  if (spec.mode === "ALL_SOURCE_MEMBERS") return input?.sourceMemberCounts?.[spec.id] ?? 0;
  return 0;
}

export function instantiateContentLedger(input = {}) {
  const items = CONTENT_CONTRACT.staticRequired.map((item) => ({
    ...item,
    kind: "static",
    disposition: "RESEARCH_REQUIRED",
    body: "",
    sourceIds: [],
  }));
  const children = [];
  for (const spec of CHILD_COLLECTIONS) {
    const contract = COLLECTION_CONTRACT.collections.find((c) => c.id === spec.id);
    if (!contract) continue;
    const n = childCount(contract, input);
    for (let i = 1; i <= n; i += 1) {
      children.push({
        itemId: `${spec.itemPrefix}.${i}`,
        collectionId: spec.id,
        kind: "collection_child",
        tabId: contract.id.startsWith("loyalty") ? "atelier-loyalty" : "atelier-footprint",
        ownerQuestion: `${contract.label} child ${i}`,
        purpose: contract.childRule || contract.label,
        disposition: "RESEARCH_REQUIRED",
        body: "",
        sourceIds: [],
      });
    }
  }
  const all = items.concat(children);
  return {
    baseCanonicalItems: items.length,
    instantiatedCollectionChildren: children.length,
    denominator: all.length,
    items: all,
  };
}
