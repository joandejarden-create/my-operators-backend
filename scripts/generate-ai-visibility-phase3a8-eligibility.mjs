#!/usr/bin/env node
/**
 * Phase 3A.8 — rebuild eligibility + geography fixtures from discovery audits.
 * Deterministic. No LLM. No Airtable writes.
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { normalizeResidencesStatus } from "../lib/brand-explorer/brand-residences-api-shape.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const root = path.join(__dirname, "..");
const discovery = JSON.parse(
  fs.readFileSync(path.join(root, "data/ai-visibility/phase3a8-eligibility-data-discovery.json"), "utf8")
);
const footprint = JSON.parse(
  fs.readFileSync(path.join(root, "data/ai-visibility/phase3a8-footprint-operating-presence.json"), "utf8")
);
const prevElig = JSON.parse(
  fs.readFileSync(path.join(root, "fixtures/ai-visibility/brand-decision-eligibility-v1.json"), "utf8")
);

const fpByBrand = Object.fromEntries(footprint.rows.map((r) => [r.brandId, r]));

/** Region Offered choice → AI Visibility geography keys */
const REGION_OFFERED_MAP = {
  "Caribbean & Latin America": ["CALA"],
  "North America": ["NORTH_AMERICA"],
  Europe: ["EUROPE"],
};

const LIFESTYLE_COLLECTION_IDS = new Set([
  "recCvV0PuZOi8c3hC",
  "rec02zPClpWUTCyXM",
  "recEJCTDj1zrsjPM6",
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

function regionOfferedToEligibility(regionOffered) {
  const set = new Set(regionOffered || []);
  const out = {
    GLOBAL: "UNKNOWN",
    CALA: "UNKNOWN",
    EUROPE: "UNKNOWN",
    NORTH_AMERICA: "UNKNOWN",
    MEXICO: "UNKNOWN",
  };
  if (!set.size) return out;

  for (const [label, keys] of Object.entries(REGION_OFFERED_MAP)) {
    const listed = set.has(label);
    for (const k of keys) {
      // Only mark NOT_ELIGIBLE when Region Offered is populated and omits the region.
      out[k] = listed ? "ELIGIBLE" : "NOT_ELIGIBLE";
    }
  }
  // Global: offered across multiple commercial regions → eligible for global monitoring consideration
  const major = ["CALA", "EUROPE", "NORTH_AMERICA"].filter((k) => out[k] === "ELIGIBLE");
  out.GLOBAL = major.length >= 2 ? "ELIGIBLE" : major.length === 1 ? "UNKNOWN" : "NOT_ELIGIBLE";
  // Mexico: no country-level Brand Basics field
  out.MEXICO = "UNKNOWN";
  return out;
}

function newBuildState(fp) {
  if (!fp?.footprintFound) {
    return {
      eligibility: "UNKNOWN",
      source: "Brand Setup - Brand Footprint missing",
      reason: "No footprint record linked — cannot confirm new-build development path.",
      evidenceQuality: "LOW",
    };
  }
  const n = fp.newBuildFootprint?.totalNewBuildHotel;
  if (n == null) {
    return {
      eligibility: "UNKNOWN",
      source: "Brand Setup - Brand Footprint.Total New Build Hotel",
      reason: "New-build count null — UNKNOWN ≠ NOT_ELIGIBLE.",
      evidenceQuality: "LOW",
    };
  }
  if (n > 0) {
    const estimated = String(fp.footprintDataStatus || "").toLowerCase() === "estimated";
    return {
      eligibility: "ELIGIBLE",
      source: "Brand Setup - Brand Footprint.Total New Build Hotel",
      reason:
        "Footprint records new-build hotels in system inventory/experience — brand has a governed new-build path.",
      evidenceQuality: estimated ? "MEDIUM" : "MEDIUM",
    };
  }
  // Zero can mean missing/incomplete footprint (Design Hotels / Ascend) — not proof of exclusion
  return {
    eligibility: "UNKNOWN",
    source: "Brand Setup - Brand Footprint.Total New Build Hotel=0",
    reason:
      "Zero new-build count may reflect incomplete footprint — not treated as NOT_ELIGIBLE.",
    evidenceQuality: "LOW",
  };
}

function residencesStates(statusRaw) {
  const status = normalizeResidencesStatus(statusRaw);
  if (status === "Yes" || status === "Case-by-Case") {
    return {
      brandedResidences: "ELIGIBLE",
      mixedUse: "UNKNOWN",
      combined: "ELIGIBLE",
      source: "Brand Basics Branded Residences Status",
      reason:
        status === "Yes"
          ? "Residences status Yes — residences-led combined territory interim ELIGIBLE; Mixed Use still ungoverned."
          : "Residences status Case-by-Case — addressable for residences questions; Mixed Use still ungoverned.",
      quality: "HIGH",
    };
  }
  if (status === "No") {
    return {
      brandedResidences: "NOT_ELIGIBLE",
      mixedUse: "UNKNOWN",
      combined: "NOT_ELIGIBLE",
      source: "Brand Basics Branded Residences Status=No",
      reason:
        "Residences=No. Interim combined territory is residences-led pending split — Mixed Use remains UNKNOWN separately.",
      quality: "HIGH",
    };
  }
  return {
    brandedResidences: "UNKNOWN",
    mixedUse: "UNKNOWN",
    combined: "UNKNOWN",
    source: "Brand Basics Branded Residences Status",
    reason: "Residences Not Confirmed / empty; Mixed Use has no Brand Basics field.",
    quality: "LOW",
  };
}

function cellFor(brand, territory, res, nb) {
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
        reason: "Active development brand in owner-decision cohort.",
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
        reason: "Brand Model not mapped.",
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
        reason:
          "Collection brand without governed lifestyle positioning claim — do not force into Lifestyle.",
      };
    case "Upper-Upscale Positioning":
      if (scale === "Upper Upscale") {
        return {
          eligibility: "ELIGIBLE",
          source: "Brand Basics Hotel Chain Scale=Upper Upscale",
          reason: "Upper Upscale chain scale.",
        };
      }
      if (scale === "Upscale") {
        return {
          eligibility: "NOT_ELIGIBLE",
          source: "Brand Basics Hotel Chain Scale=Upscale",
          reason: "Upscale — excluded from UU positioning analysis.",
        };
      }
      return {
        eligibility: "UNKNOWN",
        source: "Brand Basics Hotel Chain Scale",
        reason: "Chain scale missing.",
      };
    case "New Build":
      return {
        eligibility: nb.eligibility,
        source: nb.source,
        reason: nb.reason,
      };
    case "Branded Residences / Mixed Use":
      return {
        eligibility: res.combined,
        source: res.source,
        reason: res.reason,
      };
    case "Owner Economics / Flexibility":
      if (isCollection) {
        return {
          eligibility: "ELIGIBLE",
          source: "Brand Basics Brand Model=Collection Brand",
          reason:
            "Soft/collection affiliation flexibility only — not franchise fee/economic underwriting. Recommended rename: Soft-Brand Affiliation Flexibility.",
        };
      }
      return {
        eligibility: "UNKNOWN",
        source: "Brand Basics Brand Model",
        reason:
          "No governed owner-fee / franchise-flexibility field for hard/lifestyle brands.",
      };
    default:
      return {
        eligibility: "UNKNOWN",
        source: "brand_decision_eligibility_v1",
        reason: "Unknown territory.",
      };
  }
}

const geoBrands = [];
const entries = [];
const residencesMatrix = [];
const newBuildMatrix = [];

for (const b of discovery.rows) {
  const fp = fpByBrand[b.brandId];
  const geo = regionOfferedToEligibility(b.regionOffered);
  const op = fp?.operatingPresence || {};
  geoBrands.push({
    brandId: b.brandId,
    brandName: b.brandName,
    Global: geo.GLOBAL,
    CALA: geo.CALA,
    Europe: geo.EUROPE,
    "North America": geo.NORTH_AMERICA,
    Mexico: geo.MEXICO,
    OPERATING_PRESENCE: {
      GLOBAL: op.GLOBAL || "UNKNOWN",
      CALA: op.CALA || "UNKNOWN",
      EUROPE: op.EUROPE || "UNKNOWN",
      NORTH_AMERICA: op.NORTH_AMERICA || "UNKNOWN",
      MEXICO:
        fp?.mexicoCityHint === true
          ? "PRESENT_HINT_ONLY"
          : fp?.mexicoCityHint === false
            ? "NO_CITY_HINT"
            : "UNKNOWN",
    },
    DEVELOPMENT_ELIGIBILITY: {
      GLOBAL: geo.GLOBAL,
      CALA: geo.CALA,
      EUROPE: geo.EUROPE,
      NORTH_AMERICA: geo.NORTH_AMERICA,
      MEXICO: geo.MEXICO,
    },
    source: "Brand Basics Region Offered (+ Footprint Existing for operating presence)",
    quality: "HIGH",
    regionOffered: b.regionOffered,
  });

  const res = residencesStates(b.brandedResidencesStatus);
  const nb = newBuildState(fp);
  residencesMatrix.push({
    brandId: b.brandId,
    brandName: b.brandName,
    BRANDED_RESIDENCES_STATE: res.brandedResidences,
    MIXED_USE_STATE: res.mixedUse,
    CURRENT_COMBINED_INTENT_STATE: res.combined,
    SOURCE: res.source,
    QUALITY: res.quality,
    rawStatus: b.brandedResidencesStatus,
  });
  newBuildMatrix.push({
    brandId: b.brandId,
    brandName: b.brandName,
    NEW_BUILD_STATE: nb.eligibility,
    SOURCE: nb.source,
    EVIDENCE_QUALITY: nb.evidenceQuality,
    totalNewBuildHotel: fp?.newBuildFootprint?.totalNewBuildHotel ?? null,
  });

  for (const territory of TERRITORIES) {
    const r = cellFor(b, territory, res, nb);
    entries.push({
      brandId: b.brandId,
      brandName: b.brandName,
      decisionTerritory: territory,
      eligibility: r.eligibility,
      source: r.source,
      reason: r.reason,
      version: "1.1",
    });
  }
}

function countChanged(beforeEntries, afterEntries) {
  let changed = 0;
  for (const a of afterEntries) {
    const b = beforeEntries.find(
      (e) => e.brandId === a.brandId && e.decisionTerritory === a.decisionTerritory
    );
    if (!b || b.eligibility !== a.eligibility) changed += 1;
  }
  return changed;
}

const eligibilityDoc = {
  id: "brand_decision_eligibility_v1",
  version: "1.1",
  configVersion: "brand_decision_eligibility_v1",
  VERSION_BEFORE: String(prevElig.version || "1"),
  VERSION_AFTER: "1.1",
  LANGUAGE_NEUTRAL: true,
  UNKNOWN_BEHAVIOR: "UNKNOWN ≠ NOT_ELIGIBLE; never coerce unknown to zero or ineligible",
  peerSetId: "peers_uu_collection_lifestyle_owner_decision_v2",
  decisionTerritories: TERRITORIES,
  notes: [
    "Phase 3A.8 hardening: New Build from Brand Footprint Total New Build Hotel>0; Residences-led combined territory from Brand Basics Branded Residences Status; Mixed Use remains UNKNOWN.",
    "Interim combined Residences/Mixed Use is residences-led pending taxonomy SPLIT in prompt governance.",
    "Owner Economics / Flexibility remains Collection→ELIGIBLE as soft-brand affiliation flexibility only — recommend rename.",
    "Lifestyle Collection UNKNOWNs preserved (do not force Collection→Lifestyle).",
    "Language does not change structural eligibility.",
  ],
  taxonomyRecommendations: {
    Conversion: "KEEP",
    "Collection / Soft Brand": "KEEP",
    "Lifestyle Positioning": "KEEP",
    "Upper-Upscale Positioning": "KEEP",
    "New Build": "KEEP_WITH_UNKNOWN_OR_FOOTPRINT",
    "Branded Residences / Mixed Use": "SPLIT",
    "Owner Economics / Flexibility": "MODIFY",
  },
  entries,
};

const geoDoc = {
  id: "brand_ai_visibility_geography_eligibility_v1",
  version: "1.1",
  mexicoUnderCala: true,
  recommendedDefinition:
    "DEVELOPMENT_ELIGIBILITY = Brand Basics Region Offered lists the commercial region (owner consideration / development availability signal). OPERATING_PRESENCE = Brand Footprint Existing Hotel counts. These are not equivalent. Mexico has no country-level Brand Basics field → UNKNOWN. UNKNOWN ≠ NOT_ELIGIBLE for showcase monitoring.",
  defaultState: "UNKNOWN",
  defaultSource: "No Region Offered / incomplete",
  notes: [
    "Region Offered choices: North America, Caribbean & Latin America, Europe, MEA, APAC.",
    "Caribbean & Latin America maps to CALA.",
    "Do not infer Mexico eligibility from CALA Region Offered alone.",
    "Do not exclude brands from Mexico showcase solely for UNKNOWN country eligibility.",
  ],
  brands: geoBrands,
};

fs.writeFileSync(
  path.join(root, "fixtures/ai-visibility/brand-decision-eligibility-v1.json"),
  JSON.stringify(eligibilityDoc, null, 2),
  "utf8"
);
fs.writeFileSync(
  path.join(root, "fixtures/ai-visibility/brand-geography-eligibility-v1.json"),
  JSON.stringify(geoDoc, null, 2),
  "utf8"
);

const summary = {
  ENTRIES_CHANGED: countChanged(prevElig.entries || [], entries),
  VERSION_BEFORE: eligibilityDoc.VERSION_BEFORE,
  VERSION_AFTER: eligibilityDoc.VERSION_AFTER,
  newBuildMatrix,
  residencesMatrix,
  AIRTABLE_WRITES: 0,
};
fs.writeFileSync(
  path.join(root, "data/ai-visibility/phase3a8-eligibility-apply-summary.json"),
  JSON.stringify(summary, null, 2),
  "utf8"
);
console.log(JSON.stringify(summary, null, 2));
