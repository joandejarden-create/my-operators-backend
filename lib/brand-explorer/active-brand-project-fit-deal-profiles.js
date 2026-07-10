/**
 * Realistic Project Fit & Deal values for Brand Explorer brands.
 * Used by scripts/populate-active-brand-project-fit-deal.mjs — keep select values aligned with brand-setup.html.
 */
import { buildProjectFitFormForBrand } from "../../scripts/lib/choice-project-fit-profiles.mjs";
import { projectFitFormToAirtableFields } from "../../scripts/lib/choice-project-fit-builder.mjs";

/** @typedef {{
 *   brandDevelopmentStage?: string,
 *   brandedResidencesStatus?: string,
 *   acceptableProjectTypes?: string[],
 *   acceptableAgreementTypes?: string[],
 *   coBrandingAllowed?: string,
 *   mixedUseAllowed?: string,
 *   softCollectionBrand?: string,
 *   brandedResidencesAllowed?: string,
 * }} ProjectFitDealProfile */

const ALL_PROJECT_TYPES = [
  "New Build",
  "Conversion / Reflag",
  "Renovation / Repositioning",
  "Expansion / Add-on",
];

const FRANCHISE_AGREEMENTS = [
  "Franchise Only",
  "Flexible/Open",
  "Brand + Third-Party Mgmt. (Combined)",
];

const COLLECTION_AGREEMENTS = [
  "Franchise Only",
  "Flexible/Open",
  "Brand + Third-Party Mgmt. (Combined)",
];

const MANAGED_AGREEMENTS = ["Brand-Managed Only", "Flexible/Open"];

const MANAGED_PLUS_COMBINED = [
  "Brand-Managed Only",
  "Flexible/Open",
  "Brand + Third-Party Mgmt. (Combined)",
];

const LUXURY_PROJECT_TYPES = ["New Build", "Renovation / Repositioning"];

const COLLECTION_PROJECT_TYPES = [
  "New Build",
  "Conversion / Reflag",
  "Renovation / Repositioning",
  "Expansion / Add-on",
];

const CONVERSION_HEAVY_PROJECT_TYPES = [
  "Conversion / Reflag",
  "Renovation / Repositioning",
  "New Build",
  "Expansion / Add-on",
];

const EXTENDED_STAY_PROJECT_TYPES = [
  "New Build",
  "Conversion / Reflag",
  "Renovation / Repositioning",
];

const RESORT_PROJECT_TYPES = [
  "New Build",
  "Renovation / Repositioning",
  "Expansion / Add-on",
];

function norm(value) {
  return String(value || "").trim();
}

function parentIncludes(parent, needle) {
  return norm(parent).toLowerCase().includes(needle.toLowerCase());
}

function isChoiceParent(parent) {
  return parentIncludes(parent, "Choice Hotels");
}

function isFranchiseHeavyParent(parent) {
  const p = norm(parent).toLowerCase();
  return (
    isChoiceParent(parent) ||
    p.includes("wyndham") ||
    p.includes("bwh hotels") ||
    p.includes("red roof") ||
    p.includes("sonesta")
  );
}

function isSoftCollectionArchitecture(architecture, brandModel) {
  const arch = norm(architecture);
  const model = norm(brandModel);
  if (arch === "Soft/Collection Brand") return true;
  if (model === "Collection Brand") return true;
  if (arch === "Endorsed Brand") return true;
  return false;
}

function isLuxury(chainScale) {
  return norm(chainScale) === "Luxury";
}

function isEconomy(chainScale) {
  return norm(chainScale) === "Economy";
}

function isMidscaleFamily(chainScale) {
  const s = norm(chainScale);
  return s === "Midscale" || s === "Upper Midscale" || s === "Economy";
}

function isExtendedStay(serviceModel, brandModel) {
  const svc = norm(serviceModel);
  const model = norm(brandModel);
  return svc === "Extended Stay" || model === "Extended Stay Brand";
}

function isAllInclusive(brandModel, serviceModel) {
  const model = norm(brandModel);
  const svc = norm(serviceModel);
  return model === "All-Inclusive Brand" || svc.includes("All-Inclusive");
}

function isServicedApartments(brandModel) {
  return norm(brandModel) === "Serviced Apartments";
}

function isCoLiving(brandModel) {
  return norm(brandModel) === "Co-Living Brand";
}

function isConversionBrand(brandModel) {
  return norm(brandModel) === "Conversion Brand";
}

function isLifestyleBrand(brandModel) {
  return norm(brandModel) === "Lifestyle Brand";
}

function isCollectionBrand(brandModel, architecture) {
  return norm(brandModel) === "Collection Brand" || norm(architecture) === "Endorsed Brand";
}

function inferBrandModel(brandModel, architecture, serviceModel) {
  const model = norm(brandModel);
  if (model) return model;
  if (isSoftCollectionArchitecture(architecture, "")) return "Collection Brand";
  if (isExtendedStay(serviceModel, "")) return "Extended Stay Brand";
  return "Hard Brand";
}

function brandedResidencesStatusFromAllowed(allowed) {
  if (allowed === "Yes") return "Yes";
  if (allowed === "Case-by-case") return "Case-by-case";
  return "No";
}

function inferDevelopmentStage(basics, profileShape) {
  const existing = norm(basics.brandDevelopmentStage);
  if (existing) return existing;
  if (profileShape === "luxury-mature") return "Mature";
  if (profileShape === "growth") return "Growth";
  return "Mature";
}

/** @returns {Partial<ProjectFitDealProfile>|null} */
function choiceDealOverrides(brandName) {
  const form = buildProjectFitFormForBrand(brandName);
  if (!form) return null;
  const mapped = projectFitFormToAirtableFields(form);
  /** @type {Partial<ProjectFitDealProfile>} */
  const out = {};
  if (Array.isArray(mapped["Acceptable Project Type"]) && mapped["Acceptable Project Type"].length) {
    out.acceptableProjectTypes = mapped["Acceptable Project Type"];
  }
  if (Array.isArray(mapped["Acceptable Agreements Type"]) && mapped["Acceptable Agreements Type"].length) {
    out.acceptableAgreementTypes = mapped["Acceptable Agreements Type"];
  }
  if (mapped["Co-Branding Allowed"]) out.coBrandingAllowed = String(mapped["Co-Branding Allowed"]);
  if (mapped["Mixed-Use Development Allowed"]) {
    out.mixedUseAllowed = String(mapped["Mixed-Use Development Allowed"]);
  }
  if (mapped["Branded Residences Allowed"]) {
    out.brandedResidencesAllowed = String(mapped["Branded Residences Allowed"]);
    out.brandedResidencesStatus = brandedResidencesStatusFromAllowed(out.brandedResidencesAllowed);
  }
  return out;
}

/**
 * @param {{
 *   name: string,
 *   parentCompany?: string,
 *   chainScale?: string,
 *   brandModel?: string,
 *   architecture?: string,
 *   serviceModel?: string,
 *   brandDevelopmentStage?: string,
 *   brandedResidencesStatus?: string,
 * }} basics
 * @returns {ProjectFitDealProfile}
 */
export function buildBrandProjectFitDealProfile(basics) {
  const name = norm(basics.name);
  const parent = norm(basics.parentCompany);
  const chainScale = norm(basics.chainScale);
  const architecture = norm(basics.architecture);
  const serviceModel = norm(basics.serviceModel);
  const brandModel = inferBrandModel(basics.brandModel, architecture, serviceModel);
  const softCollection = isSoftCollectionArchitecture(architecture, brandModel) ? "Yes" : "No";

  /** @type {ProjectFitDealProfile} */
  let profile = { softCollectionBrand: softCollection };

  const choiceOverrides = isChoiceParent(parent) ? choiceDealOverrides(name) : null;

  if (isLuxury(chainScale)) {
    profile = {
      acceptableProjectTypes: LUXURY_PROJECT_TYPES.slice(),
      acceptableAgreementTypes: MANAGED_AGREEMENTS.slice(),
      coBrandingAllowed: "No",
      mixedUseAllowed: "Case-by-case",
      brandedResidencesAllowed: "Yes",
      brandedResidencesStatus: "Yes",
      softCollectionBrand: softCollection === "Yes" ? "Yes" : "No",
      brandDevelopmentStage: inferDevelopmentStage(basics, "luxury-mature"),
    };
  } else if (isAllInclusive(brandModel, serviceModel)) {
    profile = {
      acceptableProjectTypes: RESORT_PROJECT_TYPES.slice(),
      acceptableAgreementTypes: MANAGED_AGREEMENTS.slice(),
      coBrandingAllowed: "No",
      mixedUseAllowed: "Case-by-case",
      brandedResidencesAllowed: "No",
      brandedResidencesStatus: "No",
      softCollectionBrand: "No",
      brandDevelopmentStage: inferDevelopmentStage(basics, "growth"),
    };
  } else if (isExtendedStay(serviceModel, brandModel) || isServicedApartments(brandModel)) {
    profile = {
      acceptableProjectTypes: EXTENDED_STAY_PROJECT_TYPES.slice(),
      acceptableAgreementTypes: FRANCHISE_AGREEMENTS.slice(),
      coBrandingAllowed: "No",
      mixedUseAllowed: "No",
      brandedResidencesAllowed: "No",
      brandedResidencesStatus: "No",
      softCollectionBrand: "No",
      brandDevelopmentStage: inferDevelopmentStage(basics, "growth"),
    };
  } else if (isCoLiving(brandModel)) {
    profile = {
      acceptableProjectTypes: ["New Build", "Conversion / Reflag", "Renovation / Repositioning"],
      acceptableAgreementTypes: ["Flexible/Open", "Third-Party Management Only", "Brand + Third-Party Mgmt. (Combined)"],
      coBrandingAllowed: "Case-by-case",
      mixedUseAllowed: "Yes",
      brandedResidencesAllowed: "Case-by-case",
      brandedResidencesStatus: "Case-by-case",
      softCollectionBrand: "No",
      brandDevelopmentStage: inferDevelopmentStage(basics, "growth"),
    };
  } else if (isConversionBrand(brandModel)) {
    profile = {
      acceptableProjectTypes: CONVERSION_HEAVY_PROJECT_TYPES.slice(),
      acceptableAgreementTypes: FRANCHISE_AGREEMENTS.slice(),
      coBrandingAllowed: "Case-by-case",
      mixedUseAllowed: "Case-by-case",
      brandedResidencesAllowed: "No",
      brandedResidencesStatus: "No",
      softCollectionBrand: softCollection,
      brandDevelopmentStage: inferDevelopmentStage(basics, "mature"),
    };
  } else if (isLifestyleBrand(brandModel)) {
    profile = {
      acceptableProjectTypes: COLLECTION_PROJECT_TYPES.slice(),
      acceptableAgreementTypes: MANAGED_PLUS_COMBINED.slice(),
      coBrandingAllowed: "Yes",
      mixedUseAllowed: "Yes",
      brandedResidencesAllowed: chainScale === "Upper Upscale" || chainScale === "Luxury" ? "Case-by-case" : "No",
      brandedResidencesStatus:
        chainScale === "Upper Upscale" || chainScale === "Luxury" ? "Case-by-case" : "No",
      softCollectionBrand: softCollection === "Yes" ? "Yes" : "No",
      brandDevelopmentStage: inferDevelopmentStage(basics, "growth"),
    };
  } else if (isCollectionBrand(brandModel, architecture)) {
    profile = {
      acceptableProjectTypes: COLLECTION_PROJECT_TYPES.slice(),
      acceptableAgreementTypes: COLLECTION_AGREEMENTS.slice(),
      coBrandingAllowed: "Case-by-case",
      mixedUseAllowed: "Yes",
      brandedResidencesAllowed:
        chainScale === "Upscale" || chainScale === "Upper Upscale" ? "Case-by-case" : "No",
      brandedResidencesStatus:
        chainScale === "Upscale" || chainScale === "Upper Upscale" ? "Case-by-case" : "No",
      softCollectionBrand: "Yes",
      brandDevelopmentStage: inferDevelopmentStage(basics, "growth"),
    };
  } else if (isFranchiseHeavyParent(parent) || isMidscaleFamily(chainScale)) {
    profile = {
      acceptableProjectTypes: ALL_PROJECT_TYPES.slice(),
      acceptableAgreementTypes: FRANCHISE_AGREEMENTS.slice(),
      coBrandingAllowed: isEconomy(chainScale) ? "No" : "Case-by-case",
      mixedUseAllowed: isEconomy(chainScale) ? "No" : "Case-by-case",
      brandedResidencesAllowed:
        brandModel === "Collection Brand" ||
        (architecture === "Endorsed Brand" && (chainScale === "Upscale" || chainScale === "Upper Upscale"))
          ? "Case-by-case"
          : "No",
      brandedResidencesStatus: brandedResidencesStatusFromAllowed(
        brandModel === "Collection Brand" ||
          (architecture === "Endorsed Brand" && (chainScale === "Upscale" || chainScale === "Upper Upscale"))
          ? "Case-by-case"
          : "No"
      ),
      softCollectionBrand: softCollection,
      brandDevelopmentStage: inferDevelopmentStage(basics, "mature"),
    };
  } else if (chainScale === "Independent") {
    profile = {
      acceptableProjectTypes: COLLECTION_PROJECT_TYPES.slice(),
      acceptableAgreementTypes: ["Flexible/Open", "Third-Party Management Only", "Brand + Third-Party Mgmt. (Combined)"],
      coBrandingAllowed: "Case-by-case",
      mixedUseAllowed: "Case-by-case",
      brandedResidencesAllowed: "Case-by-case",
      brandedResidencesStatus: "Case-by-case",
      softCollectionBrand: softCollection,
      brandDevelopmentStage: inferDevelopmentStage(basics, "growth"),
    };
  } else {
    // Marriott, Hilton, IHG, Hyatt, Accor, and other full-service hard brands.
    profile = {
      acceptableProjectTypes: ALL_PROJECT_TYPES.slice(),
      acceptableAgreementTypes:
        chainScale === "Upper Upscale" || chainScale === "Upscale"
          ? MANAGED_PLUS_COMBINED.slice()
          : FRANCHISE_AGREEMENTS.slice(),
      coBrandingAllowed: chainScale === "Upper Upscale" ? "Case-by-case" : "Case-by-case",
      mixedUseAllowed: chainScale === "Economy" ? "No" : "Case-by-case",
      brandedResidencesAllowed:
        chainScale === "Upper Upscale" || chainScale === "Upscale" ? "Case-by-case" : "No",
      brandedResidencesStatus:
        chainScale === "Upper Upscale" || chainScale === "Upscale" ? "Case-by-case" : "No",
      softCollectionBrand: softCollection,
      brandDevelopmentStage: inferDevelopmentStage(basics, "mature"),
    };
  }

  if (choiceOverrides) {
    profile = { ...profile, ...choiceOverrides, softCollectionBrand: profile.softCollectionBrand };
    if (choiceOverrides.brandedResidencesAllowed) {
      profile.brandedResidencesStatus = brandedResidencesStatusFromAllowed(choiceOverrides.brandedResidencesAllowed);
    }
  }

  if (norm(basics.brandDevelopmentStage)) {
    profile.brandDevelopmentStage = norm(basics.brandDevelopmentStage);
  }
  if (norm(basics.brandedResidencesStatus) && !choiceOverrides?.brandedResidencesStatus) {
    profile.brandedResidencesStatus = norm(basics.brandedResidencesStatus);
  }

  return profile;
}

/** @deprecated Use buildBrandProjectFitDealProfile */
export const buildActiveBrandProjectFitDealProfile = buildBrandProjectFitDealProfile;

/**
 * @param {ProjectFitDealProfile} profile
 * @returns {{ basics: Record<string, unknown>, projectFit: Record<string, unknown> }}
 */
export function projectFitDealProfileToAirtableFields(profile) {
  /** @type {Record<string, unknown>} */
  const basics = {};
  /** @type {Record<string, unknown>} */
  const projectFit = {};

  if (profile.brandDevelopmentStage) {
    basics["Brand Development Stage"] = profile.brandDevelopmentStage;
  }
  if (profile.brandedResidencesStatus) {
    basics["Branded Residences Status"] = profile.brandedResidencesStatus;
  }

  if (profile.acceptableProjectTypes?.length) {
    projectFit["Acceptable Project Type"] = profile.acceptableProjectTypes;
  }
  if (profile.acceptableAgreementTypes?.length) {
    projectFit["Acceptable Agreements Type"] = profile.acceptableAgreementTypes;
  }
  if (profile.coBrandingAllowed) {
    projectFit["Co-Branding Allowed"] = profile.coBrandingAllowed;
  }
  if (profile.mixedUseAllowed) {
    projectFit["Mixed-Use Development Allowed"] = profile.mixedUseAllowed;
  }
  if (profile.softCollectionBrand) {
    projectFit["Soft/Collection Brand"] = profile.softCollectionBrand;
  }
  if (profile.brandedResidencesAllowed) {
    projectFit["Branded Residences Allowed"] = profile.brandedResidencesAllowed;
  }

  return { basics, projectFit };
}
