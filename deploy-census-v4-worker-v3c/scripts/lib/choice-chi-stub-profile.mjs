/**
 * Minimal Tier1Profile for CHI brands not in choice-tier1-explorer-profiles.mjs.
 */
import { FDD_ITEM19 } from "./choice-fdd-item19.mjs";
import { CHOICE_DEAL_TERMS_FDD_FILE } from "./choice-deal-terms-profiles.mjs";
import { buildProjectFitFormForBrand } from "./choice-project-fit-profiles.mjs";
import { stubScenarioBodiesForProfile } from "./choice-chi-stub-scenarios.mjs";

const SCALE_BY_SEGMENT = {
  economy: "economy",
  midscale: "midscale",
  upperMidscale: "upper-midscale",
  upscale: "upscale",
  extendedStay: "extended-stay",
  softCollection: "soft collection",
  luxuryCollection: "luxury collection",
};

const ROYALTY_DEFAULT = {
  economy: "5.0% royalty on gross room revenues (confirm marketing, technology, and reservation fees in FDD)",
  midscale: "5.0% royalty on gross room revenues (confirm marketing, technology, and reservation fees in FDD)",
  upperMidscale: "5.5% royalty on gross room revenues (confirm fees in FDD)",
  upscale: "6.0% royalty on gross room revenues (confirm marketing, technology, and reservation fees in FDD)",
  extendedStay: "6.0% royalty on room revenue for duration of agreement (confirm in FDD)",
  softCollection: "5.0% membership fee on gross room revenues (confirm marketing and reservation fees in FDD)",
  luxuryCollection: "6.0% royalty on gross room revenues (confirm fees in FDD)",
};

function slugify(name) {
  return String(name)
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-|-$/g, "");
}

/**
 * @param {string} profileName
 * @param {string} airtableName
 * @returns {import('./choice-tier1-explorer-profiles.mjs').Tier1Profile}
 */
export function buildStubProfile(profileName, airtableName = profileName) {
  const fit = buildProjectFitFormForBrand(profileName);
  const segment = fit?.segment || "midscale";
  const scaleLabel = SCALE_BY_SEGMENT[segment] || "midscale";
  const item19 = FDD_ITEM19[profileName] || FDD_ITEM19[airtableName] || {};
  const fddFile = CHOICE_DEAL_TERMS_FDD_FILE[profileName] || "35771-202604-09.txt";

  const loyaltyNote = item19.loyaltyPct
    ? `Item 19 FY ${item19.performanceYear || "2025"}: ~${item19.loyaltyPct}% loyalty`
    : "Confirm Item 19 and Item 20 in your FDD";

  return {
    name: profileName,
    slug: slugify(profileName),
    segment,
    scaleLabel,
    tagline: `${profileName} — Choice Hotels International portfolio brand.`,
    royaltyLabel: ROYALTY_DEFAULT[segment] || ROYALTY_DEFAULT.midscale,
    fddFile,
    pressKitFile: null,
    positioning: `${profileName} is a ${scaleLabel} flag under Choice Hotels International—confirm prototype, fees, and geography in your franchise disclosure and LOI.`,
    developmentModel: "Conversion and new construction where market supports; confirm PIP and prototype in disclosure.",
    typicalUseCase: `Owners seeking ${scaleLabel} Choice distribution, Choice Privileges participation, and recognizable retail in Americas markets—including selective CALA growth corridors.`,
    scenarios: stubScenarioBodiesForProfile(profileName) || [
      `Independent or tired flag reflag seeking ${scaleLabel} Choice systems and loyalty.`,
      "CALA or U.S. corridor where Choice enterprise distribution lifts direct and member mix.",
      "Portfolio owners standardizing on CHI tier appropriate to asset class.",
    ],
    bestAt: [
      `Markets that support ${scaleLabel} ADR and required amenity stack.`,
      "Owners who model net contribution after fees, loyalty, and channel mix.",
      "Operators with tier-appropriate QA and opening discipline.",
    ],
    growthThemes: [
      `${scaleLabel} growth`,
      "Americas conversion and NC",
      "Choice Privileges distribution",
      "CALA selective expansion",
    ],
    footprintEditorial: `${profileName} participates in the Choice Hotels International portfolio in the Americas. ${loyaltyNote}. Use FDD Item 20 and local market study for open-hotel counts—do not rely on generic tier assumptions.`,
    pipelineStats: loyaltyNote,
    heroPurpose: `Deliver ${scaleLabel} guest experience with Choice systems—operators who meet prototype, loyalty fulfillment, and QA expectations.`,
    similarBrands: ["Comfort Inn & Suites", "Quality Inn", "Cambria Hotels"],
  };
}
