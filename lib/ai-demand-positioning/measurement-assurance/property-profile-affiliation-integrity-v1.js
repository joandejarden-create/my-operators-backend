/**
 * PROPERTY_PROFILE_AFFILIATION_INTEGRITY + PROFILE_TO_PROMPT_PROVENANCE
 */

import crypto from "crypto";
import { MAJOR_BRAND_TOKENS } from "./prompt-integrity-contract-v1.js";
import { tokenPresent } from "./prompt-bias-detection-v1.js";

export const PROPERTY_PROFILE_AFFILIATION_INTEGRITY = "PROPERTY_PROFILE_AFFILIATION_INTEGRITY";
export const PROFILE_TO_PROMPT_PROVENANCE = "PROFILE_TO_PROMPT_PROVENANCE";

/** Gold: NOW NOW NOHO must never resolve as Hyatt. */
export const AFFILIATION_GOLD_CASES_V1 = Object.freeze([
  {
    caseId: "gold_noho_not_hyatt",
    propertyId: "adp_now_now_noho",
    mustNotContainBrandTokens: ["hyatt", "world of hyatt"],
    expectedBrand: "Independent",
    expectedAffiliation: "Independent",
    expectedOfficialBrandDomain: null,
    officialUrlMustNotInclude: ["hyatt.com"],
    founderAuthorizedOperatorCompany: "Dovetail + Co",
  },
]);

export function hashPropertyProfile(profile) {
  const body = {
    propertyId: profile?.propertyId,
    name: profile?.name,
    brand: profile?.brand,
    affiliation: profile?.affiliation,
    parentCompany: profile?.parentCompany ?? null,
    operatorCompany: profile?.operatorCompany ?? null,
    officialBrandDomain: profile?.officialBrandDomain ?? null,
    officialPropertyPageUrl: profile?.officialPropertyPageUrl ?? null,
    website: profile?.website ?? null,
    market: profile?.market,
    submarket: profile?.submarket,
    city: profile?.city,
  };
  return crypto.createHash("sha256").update(JSON.stringify(body), "utf8").digest("hex").slice(0, 16);
}

export function assertAffiliationGold(profile, gold = AFFILIATION_GOLD_CASES_V1[0]) {
  const defects = [];
  if (!profile || profile.propertyId !== gold.propertyId) {
    defects.push("profile_missing_or_wrong_id");
    return { pass: false, defects };
  }
  const brandBlob = [
    profile.brand,
    profile.affiliation,
    profile.parentCompany,
    profile.officialBrandDomain,
    profile.officialPropertyPageUrl,
  ]
    .filter(Boolean)
    .join(" | ");
  for (const tok of gold.mustNotContainBrandTokens) {
    if (tokenPresent(brandBlob, tok) || tokenPresent(String(profile.officialPropertyPageUrl || ""), tok)) {
      defects.push(`forbidden_token:${tok}`);
    }
  }
  if (String(profile.brand || "") !== gold.expectedBrand) defects.push(`brand!=${gold.expectedBrand}`);
  if (String(profile.affiliation || "") !== gold.expectedAffiliation) {
    defects.push(`affiliation!=${gold.expectedAffiliation}`);
  }
  if (profile.officialBrandDomain != null && profile.officialBrandDomain !== "") {
    defects.push("officialBrandDomain_must_be_null_for_independent");
  }
  for (const bad of gold.officialUrlMustNotInclude) {
    if (String(profile.officialPropertyPageUrl || "").toLowerCase().includes(bad)) {
      defects.push(`officialUrl_contains:${bad}`);
    }
  }
  return { pass: defects.length === 0, defects, profileHash: hashPropertyProfile(profile) };
}

/**
 * Block property/brand scenario generation when affiliation fails integrity.
 */
export function checkPropertyProfileAffiliationIntegrity(profile) {
  const defects = [];
  if (!profile?.propertyId) defects.push("missing_propertyId");
  if (!profile?.name) defects.push("missing_name");
  if (!profile?.market && !profile?.city) defects.push("missing_geography");

  const brand = String(profile?.brand || "");
  const affiliation = String(profile?.affiliation || "");
  const url = String(profile?.officialPropertyPageUrl || "");
  const domain = String(profile?.officialBrandDomain || "");

  const independent = /^independent$/i.test(brand) || /^independent$/i.test(affiliation);
  if (independent) {
    for (const tok of MAJOR_BRAND_TOKENS) {
      if (
        tokenPresent(brand, tok) ||
        tokenPresent(affiliation, tok) ||
        tokenPresent(domain, tok) ||
        (url && tokenPresent(url.replace(/https?:\/\//, ""), tok.split(" ")[0]))
      ) {
        // hyatt.com domain check
        if (/hyatt\.com/i.test(url) || /hilton\.com|marriott\.com/i.test(domain)) {
          defects.push(`independent_profile_has_chain_signal:${tok}`);
        }
      }
    }
    if (/hyatt\.com|hilton\.com|marriott\.com/i.test(url) && !/cambridgebeaches|nownow/i.test(url)) {
      defects.push("independent_official_url_points_at_chain");
    }
    if (/hyatt\.com/i.test(url)) defects.push("independent_official_url_is_hyatt");
  }

  const gold = AFFILIATION_GOLD_CASES_V1.find((g) => g.propertyId === profile?.propertyId);
  let goldResult = null;
  if (gold) {
    goldResult = assertAffiliationGold(profile, gold);
    if (!goldResult.pass) defects.push(...goldResult.defects.map((d) => `gold:${d}`));
  }

  return {
    gate: PROPERTY_PROFILE_AFFILIATION_INTEGRITY,
    pass: defects.length === 0,
    defects,
    profileHash: hashPropertyProfile(profile),
    goldResult,
    blocksPropertyBrandScenarioGeneration: defects.length > 0,
  };
}

export function buildProfileToPromptProvenance({ profile, scenario, exactRenderedPrompt }) {
  return {
    gate: PROFILE_TO_PROMPT_PROVENANCE,
    propertyId: profile?.propertyId,
    profileHash: hashPropertyProfile(profile),
    brand: profile?.brand ?? null,
    affiliation: profile?.affiliation ?? null,
    parentCompany: profile?.parentCompany ?? null,
    operatorCompany: profile?.operatorCompany ?? null,
    scenarioId: scenario?.scenarioId,
    scenarioSource: scenario?.source,
    exactRenderedPrompt,
    decisionNote:
      scenario?.source === "property_specific"
        ? "Scenario authored from property-specific catalog; profile hash bound at render time"
        : "Standard market pack; profile hash recorded for isolation audit",
  };
}
