#!/usr/bin/env node
/**
 * Generates fixtures/ai-visibility/brand-decision-eligibility-v1.json
 * Deterministic rules from Brand Basics audit + founder specials. No LLM.
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const auditPath = path.join(
  __dirname,
  "..",
  "data",
  "ai-visibility",
  "phase3a7-showcase-brand-basics-audit.json"
);
const outPath = path.join(
  __dirname,
  "..",
  "fixtures",
  "ai-visibility",
  "brand-decision-eligibility-v1.json"
);

const audit = JSON.parse(fs.readFileSync(auditPath, "utf8"));
const mg = audit.mgalleryAndKimptonNameHits.find((b) => b.brandName === "MGallery Collection");

const brands = [
  ...audit.rows.map((r) => ({
    brandId: r.BRAND_ID,
    brandName: r.BRAND_NAME_LIVE || r.BRAND,
    brandModel: r.BRAND_MODEL,
    chainScale: r.CHAIN_SCALE,
    parent: r.CANONICAL_PARENT,
  })),
  {
    brandId: mg.brandId,
    brandName: mg.brandName,
    brandModel: mg.brandModel,
    chainScale: mg.chainScale,
    parent: "Accor",
  },
].filter((b) => b.brandId !== "recmKqo7M7mLZgRqQ"); // RED portfolio-only, not cohort

const LIFESTYLE_COLLECTION_IDS = new Set([
  "recCvV0PuZOi8c3hC", // Tribute
  "rec02zPClpWUTCyXM", // Design Hotels
  "recEJCTDj1zrsjPM6", // Autograph soft/lifestyle-adjacent collection
]);

const TERRITORIES = [
  "Conversion",
  "Collection / Soft Brand",
  "Lifestyle Positioning",
  "Upper-Upscale Positioning",
  "New Build",
  "Branded Residences / Mixed Use",
  "Owner Economics / Flexibility",
];

function cell(brand, territory) {
  const model = brand.brandModel;
  const scale = brand.chainScale;
  const isCollection = model === "Collection Brand";
  const isHard = model === "Hard Brand";
  const isLifestyle = model === "Lifestyle Brand";

  switch (territory) {
    case "Conversion":
      return {
        eligibility: "ELIGIBLE",
        source: "Brand Basics Brand Model + Active/Live cohort membership",
        reason: "Active development brand in owner-decision cohort; conversion is a core owner ask for this set.",
      };
    case "Collection / Soft Brand":
      if (isHard) {
        return {
          eligibility: "NOT_ELIGIBLE",
          source: "Brand Basics Brand Model=Hard Brand + founder showcase rule",
          reason: "Hard brand — must not enter Collection / Soft Brand analysis.",
        };
      }
      if (isLifestyle) {
        return {
          eligibility: "NOT_ELIGIBLE",
          source: "Brand Basics Brand Model=Lifestyle Brand",
          reason: "Lifestyle brand, not a collection / soft-brand model.",
        };
      }
      if (isCollection) {
        return {
          eligibility: "ELIGIBLE",
          source: "Brand Basics Brand Model=Collection Brand",
          reason: "Collection brand — addressable for collection / soft-brand owner decisions.",
        };
      }
      return {
        eligibility: "UNKNOWN",
        source: "Brand Basics Brand Model",
        reason: "Brand Model not mapped to collection/soft eligibility.",
      };
    case "Lifestyle Positioning":
      if (isHard) {
        return {
          eligibility: "NOT_ELIGIBLE",
          source: "Brand Basics Brand Model=Hard Brand",
          reason: "Hard brand — not a lifestyle-positioning subject.",
        };
      }
      if (isLifestyle || LIFESTYLE_COLLECTION_IDS.has(brand.brandId)) {
        return {
          eligibility: "ELIGIBLE",
          source: isLifestyle
            ? "Brand Basics Brand Model=Lifestyle Brand"
            : "Brand Basics Collection Brand + showcase lifestyle/design role",
          reason: isLifestyle
            ? "Lifestyle brand — addressable for lifestyle positioning."
            : "Collection brand with governed lifestyle/design showcase role.",
        };
      }
      return {
        eligibility: "UNKNOWN",
        source: "Brand Basics Brand Model=Collection Brand",
        reason: "Collection brand without governed lifestyle showcase role — lifestyle intent unclear.",
      };
    case "Upper-Upscale Positioning":
      if (scale === "Upper Upscale") {
        return {
          eligibility: "ELIGIBLE",
          source: "Brand Basics Hotel Chain Scale=Upper Upscale",
          reason: "Upper Upscale chain scale — addressable for UU positioning.",
        };
      }
      if (scale === "Upscale") {
        return {
          eligibility: "NOT_ELIGIBLE",
          source: "Brand Basics Hotel Chain Scale=Upscale",
          reason: "Upscale (not Upper Upscale) — excluded from UU positioning analysis.",
        };
      }
      return {
        eligibility: "UNKNOWN",
        source: "Brand Basics Hotel Chain Scale",
        reason: "Chain scale missing or not UU/Upscale.",
      };
    case "New Build":
      return {
        eligibility: "UNKNOWN",
        source:   "new-build eligibility field in AI Visibility loader)",
        reason: "No governed new-build eligibility field — UNKNOWN ≠ NOT_ELIGIBLE.",
      };
    case "Branded Residences / Mixed Use":
      return {
        eligibility: "UNKNOWN",
        source: "Brand Basics (no residences/mixed-use field in AI Visibility loader)",
        reason: "No governed residences/mixed-use eligibility field.",
      };
    case "Owner Economics / Flexibility":
      if (isCollection) {
        return {
          eligibility: "ELIGIBLE",
          source: "Brand Basics Brand Model=Collection Brand",
          reason: "Collection / soft brands are primary subjects for owner flexibility economics asks.",
        };
      }
      return {
        eligibility: "UNKNOWN",
        source: "Brand Basics Brand Model",
        reason: "No governed owner-flexibility field for hard/lifestyle brands.",
      };
    default:
      return {
        eligibility: "UNKNOWN",
        source: "brand_decision_eligibility_v1",
        reason: "Unknown territory.",
      };
  }
}

const entries = [];
for (const brand of brands) {
  for (const territory of TERRITORIES) {
    const r = cell(brand, territory);
    entries.push({
      brandId: brand.brandId,
      brandName: brand.brandName,
      decisionTerritory: territory,
      eligibility: r.eligibility,
      source: r.source,
      reason: r.reason,
      version: "1",
    });
  }
}

const doc = {
  id: "brand_decision_eligibility_v1",
  version: "1",
  configVersion: "brand_decision_eligibility_v1",
  LANGUAGE_NEUTRAL: true,
  UNKNOWN_BEHAVIOR: "UNKNOWN ≠ NOT_ELIGIBLE; never coerce unknown to zero or ineligible",
  peerSetId: "peers_uu_collection_lifestyle_owner_decision_v2",
  decisionTerritories: TERRITORIES,
  notes: [
    "Deterministic governance only — no AI inference / eligibility scores.",
    "Westin + Radisson Blu: Hard Brand → NOT_ELIGIBLE for Collection / Soft Brand.",
    "Language does not change structural eligibility.",
  ],
  entries,
};

fs.writeFileSync(outPath, JSON.stringify(doc, null, 2), "utf8");
console.log(`Wrote ${entries.length} entries → ${outPath}`);
