/**
 * Design Hotels visual correction content v35F-R3.
 * Momentum rows moved to v35F-R4 (Tribute-parity property listings).
 */
import { DESIGN_HOTELS_SOURCE_IDS } from "./brand-explorer-design-hotels-content-packages-v35F.js";

export const V35F_R3_SLOT_KEYS = Object.freeze([
  "loyalty.kpi.members",
  "loyalty.kpi.hotels",
  "loyalty.kpi.mix",
  "valueOwners.watchouts",
  "footprint.editorial",
  "footprint.editorial_bullets",
]);

function pkg(slotKey, title, body, meta = {}) {
  return {
    slotKey,
    title: title || "",
    body,
    tab: meta.tab || "Loyalty Program",
    sort: meta.sort ?? 0,
    sourceIds: meta.sourceIds || [],
    patchOnly: Boolean(meta.patchOnly),
    matchTitle: meta.matchTitle || null,
  };
}

export function buildDesignHotelsVisualCorrectionPackagesV35FR3() {
  const S = DESIGN_HOTELS_SOURCE_IDS;
  const bonvoySources = [S.bonvoy, S.member];
  const memberSources = [S.member, S.about, S.directory];

  return [
    pkg(
      "loyalty.kpi.members",
      "",
      "200M+ members (Bonvoy program scale · not property-specific)",
      { tab: "Loyalty Program", sourceIds: bonvoySources }
    ),
    pkg(
      "loyalty.kpi.hotels",
      "",
      "Global member directory · Bonvoy participation varies by property",
      { tab: "Loyalty Program", sourceIds: memberSources }
    ),
    pkg(
      "loyalty.kpi.mix",
      "",
      "Varies by property · confirm during affiliation diligence",
      { tab: "Loyalty Program", sourceIds: bonvoySources }
    ),
    pkg(
      "valueOwners.watchouts",
      "",
      [
        "Do not assume uniform Bonvoy benefits or distribution participation across all member hotels.",
        "Curation and design review expectations may limit rapid prototype-style conversions.",
        "Public collection materials are not property-level performance representations.",
        "Agreement structure, participation costs, and standards should be confirmed directly—not inferred from this Explorer view.",
        "Loyalty contribution and channel mix may differ materially property to property—underwrite only after property-level confirmation.",
      ].join("\n"),
      { tab: "Value to Owners", sourceIds: memberSources, patchOnly: true }
    ),
    pkg(
      "footprint.editorial",
      "",
      "Design Hotels is best understood as a global design-led member collection with selective Marriott Bonvoy context—not a uniform chain rollout. Footprint on this Explorer profile combines directional regional directory presence with CALA census-backed open and pipeline counts where verified. Owners should evaluate curation fit, operating autonomy, and property-level participation terms against other Marriott affiliation paths.",
      { tab: "Footprint & Growth", sourceIds: [S.directory, S.about, S.member] }
    ),
    pkg(
      "footprint.editorial_bullets",
      "",
      [
        "Global directory breadth does not imply uniform commercial or loyalty participation",
        "CALA census metrics are region-specific—not global open/pipeline totals",
        "Regional cards are directional directory context only",
        "Design-led assets benefit most when local identity is already a guest-facing strength",
        "Confirm membership scope and Bonvoy participation directly with brand representatives",
      ].join("\n"),
      { tab: "Footprint & Growth", sourceIds: [S.directory, S.about, S.member] }
    ),
  ];
}
