/**
 * Context-aware relevance layer for Deal Readiness (weighted-v2).
 * Infers project/deal/brand/operator context from merged fields and adjusts
 * which requirements count, gap severity, and foundational caps apply.
 */

import { isFieldFilled } from "./my-deals.js";
import {
  LEASE_STRUCTURE_FORM_FIELDS,
  isLeaseStructureDealApplicableFromMergedFields,
} from "./schemas/deal-setup-fields.js";

const LEASE_STRUCTURE_FIELD_SET = new Set(LEASE_STRUCTURE_FORM_FIELDS);

const READINESS_ALTERNATE_KEYS = {
  "Are you open to lesser-known or emerging brands with favorable terms?": [
    "Are you open to considering other brands with favorable terms?",
  ],
  "Regulatory or Permitting Issues Description": ["Regulatory or Permitting Issues Text"],
};

const OPERATOR_DETAIL_FIELDS = new Set([
  "Is the hotel currently managed by a third-party operator?",
  "Minimum Operator Experience (years)",
  "Preferred Third-Party Operators (names)",
  "Preferred Third-Party Operator Profile",
  "Services Required From Operator",
  "Other Operator Criteria or Notes",
  "Level of Involvement in Day-to-Day Ops",
  "Plan to Self-Manage or Hire Third Party?",
]);

const CONVERSION_PIP_FIELDS = new Set([
  "PIP / CapEx Status",
  "PIP Budget Range (if conversion)",
]);

const NEW_BUILD_SITE_FIELDS = new Set([
  "Current Form of Site Control",
  "Zoning Status",
  "Zoned for Hotel Development",
  "Total Site Size",
  "Total Site Size Unit",
]);

function normFieldValue(val) {
  if (val == null) return "";
  if (typeof val === "string") return val.trim().toLowerCase();
  if (typeof val === "object" && typeof val.name === "string") return val.name.trim().toLowerCase();
  if (Array.isArray(val)) {
    return val
      .map((x) => (typeof x === "string" ? x : x?.name || ""))
      .filter(Boolean)
      .join(" ")
      .toLowerCase();
  }
  return String(val).trim().toLowerCase();
}

export function getFieldValueForContext(fields, canonicalKey) {
  const extras = READINESS_ALTERNATE_KEYS[canonicalKey] || [];
  for (const k of [canonicalKey, ...extras]) {
    if (fields[k] !== undefined && fields[k] !== null) return fields[k];
  }
  return undefined;
}

export function isFilledForContext(fields, canonicalKey) {
  return isFieldFilled(getFieldValueForContext(fields, canonicalKey));
}

function primaryDemandDriversSelected(fields) {
  const raw = getFieldValueForContext(fields, "Primary Demand Drivers");
  if (Array.isArray(raw)) {
    return raw.map((x) => (typeof x === "string" ? x : (x && x.name) || "").trim()).filter(Boolean);
  }
  if (typeof raw === "string" && raw.trim() !== "") {
    return raw
      .split(/\s*,\s*/)
      .map((s) => s.trim())
      .filter(Boolean);
  }
  return [];
}

/** Conditional requirements aligned with Deal Setup UI logic. */
export function isContextualRequirementMet(fields, fname) {
  if (fname === "Regulatory or Permitting Issues Description") {
    const issue = normFieldValue(getFieldValueForContext(fields, "Regulatory or Permitting Issues?"));
    if (issue === "no") return true;
    return isFilledForContext(fields, fname);
  }
  if (fname === "Primary Demand Drivers Other") {
    const drivers = primaryDemandDriversSelected(fields);
    if (!drivers.some((d) => d === "Other")) return true;
    return isFilledForContext(fields, fname);
  }
  return isFilledForContext(fields, fname);
}

function isExistingAssetContext(ctx) {
  return (
    ctx.projectTypeContext === "conversion_reflag" ||
    ctx.projectTypeContext === "renovation_repositioning" ||
    ctx.projectTypeContext === "operating_asset"
  );
}

function isPreOperatingContext(ctx) {
  return ctx.projectTypeContext === "new_build" || ctx.projectTypeContext === "land_development_site";
}

function isOperatorInScope(ctx) {
  return (
    ctx.dealStructureContext === "brand_operator" ||
    ctx.dealStructureContext === "management_agreement" ||
    ctx.operatorContext === "operator_needed" ||
    ctx.operatorContext === "third_party_operator"
  );
}

function isLeaseDealContext(ctx) {
  return ctx.dealStructureContext === "lease" || ctx.dealStructureContext === "flexible_open";
}

function isFranchiseOnlyContext(ctx) {
  return ctx.dealStructureContext === "franchise_only";
}

function matchesAny(text, patterns) {
  const t = normFieldValue(text);
  return patterns.some((re) => re.test(t));
}

/**
 * @param {Record<string, unknown>} fields
 */
export function inferReadinessContext(fields) {
  const projectTypeRaw = getFieldValueForContext(fields, "Project Type");
  const stageRaw = getFieldValueForContext(fields, "Stage of Development");
  const dealStructRaw = getFieldValueForContext(fields, "Preferred Deal Structure");
  const brandedRaw = getFieldValueForContext(fields, "Is the hotel currently branded?");
  const managedRaw = getFieldValueForContext(fields, "Is the hotel currently managed by a third-party operator?");
  const planManageRaw = getFieldValueForContext(fields, "Plan to Self-Manage or Hire Third Party?");
  const hotelTypeRaw = getFieldValueForContext(fields, "Hotel Type");

  const propertyName = normFieldValue(getFieldValueForContext(fields, "Property Name"));
  const projectType = normFieldValue(projectTypeRaw);
  const stage = normFieldValue(stageRaw);
  const dealStruct = normFieldValue(dealStructRaw);
  const branded = normFieldValue(brandedRaw);
  const managed = normFieldValue(managedRaw);
  const planManage = normFieldValue(planManageRaw);
  const hotelType = normFieldValue(hotelTypeRaw);

  let projectTypeContext = "unknown";
  if (
    matchesAny(projectTypeRaw, [
      /conversion/,
      /reflag/,
      /re-flag/,
      /brand change/,
    ])
  ) {
    projectTypeContext = "conversion_reflag";
  } else if (
    matchesAny(projectTypeRaw, [/renovation/, /reposition/, /rebrand/, /upgrade/]) &&
    !/new build/i.test(projectType)
  ) {
    projectTypeContext = "renovation_repositioning";
  } else if (matchesAny(projectTypeRaw, [/new build/, /ground.?up/, /development hotel/])) {
    projectTypeContext = "new_build";
  } else if (matchesAny(projectTypeRaw, [/land\b/, /development site/, /site only/, /greenfield/])) {
    projectTypeContext = "land_development_site";
  } else if (
    matchesAny(projectTypeRaw, [/operating/, /existing hotel/, /stabilized/, /open hotel/]) ||
    matchesAny(stageRaw, [/operating/, /open/, /stabilized/, /in operation/])
  ) {
    projectTypeContext = "operating_asset";
  } else if (
    matchesAny(projectTypeRaw, [/reposition/, /rebrand/]) ||
    (matchesAny(stageRaw, [/renovation/, /reposition/]) && !/new build/i.test(projectType))
  ) {
    projectTypeContext = "renovation_repositioning";
  } else if (
    matchesAny(stageRaw, [
      /pre-?development/,
      /planning/,
      /entitlement/,
      /site control/,
      /under construction/,
      /construction/,
    ]) &&
    !isExistingAssetContext({ projectTypeContext: "operating_asset" })
  ) {
    projectTypeContext = /land|site/i.test(projectType) ? "land_development_site" : "new_build";
  }

  // Repositioning signals in property name or stage when Project Type is blank
  if (projectTypeContext === "unknown" && matchesAny(stageRaw, [/reposition/, /renovation/])) {
    projectTypeContext = "renovation_repositioning";
  }
  if (projectTypeContext === "unknown" && /reposition|rebrand/i.test(propertyName)) {
    projectTypeContext = "renovation_repositioning";
  }
  if (projectTypeContext === "unknown" && /conversion|reflag|re-flag/i.test(propertyName)) {
    projectTypeContext = "conversion_reflag";
  }
  if (projectTypeContext === "unknown" && /new build|ground.?up/i.test(propertyName)) {
    projectTypeContext = "new_build";
  }
  const strategicText = [
    propertyName,
    normFieldValue(getFieldValueForContext(fields, "Primary Goal for the Hotel")),
    normFieldValue(getFieldValueForContext(fields, "Top Priorities for Project")),
    normFieldValue(getFieldValueForContext(fields, "Top Concerns for this Project")),
  ].join(" ");
  if (projectTypeContext === "unknown" && /reposition|rebrand/i.test(strategicText)) {
    projectTypeContext = "renovation_repositioning";
  }
  if (projectTypeContext === "unknown" && /conversion|reflag|re-flag/i.test(strategicText)) {
    projectTypeContext = "conversion_reflag";
  }
  const hasOperatingAssetSignals =
    isFilledForContext(fields, "Total Number of Rooms/Keys") &&
    (isFilledForContext(fields, "Hotel Type") ||
      isFilledForContext(fields, "Estimated or Actual RevPAR") ||
      isFilledForContext(fields, "Full Address"));
  if (projectTypeContext === "unknown" && hasOperatingAssetSignals) {
    projectTypeContext = "renovation_repositioning";
  }
  if (
    projectTypeContext === "unknown" &&
    (isFilledForContext(fields, "Zoned for Hotel Development") ||
      isFilledForContext(fields, "Current Form of Site Control") ||
      matchesAny(stageRaw, [/pre-?development/, /planning/, /entitlement/, /under construction/]))
  ) {
    projectTypeContext = "new_build";
  }

  let dealStructureContext = "unknown";
  if (matchesAny(dealStructRaw, [/lease/, /ground lease/])) {
    dealStructureContext = "lease";
  } else if (matchesAny(dealStructRaw, [/flexible/, /open to options/])) {
    dealStructureContext = "flexible_open";
  } else if (
    matchesAny(dealStructRaw, [
      /franchise only/,
      /franchise-only/,
      /^franchise$/,
    ]) &&
    !/management|operator|lease/i.test(dealStruct)
  ) {
    dealStructureContext = "franchise_only";
  } else if (
    matchesAny(dealStructRaw, [
      /management agreement/,
      /third.party management/,
      /operator agreement/,
    ])
  ) {
    dealStructureContext = "management_agreement";
  } else if (
    matchesAny(dealStructRaw, [
      /brand.*operator/,
      /franchise.*management/,
      /brand and operator/,
      /both franchise/,
    ])
  ) {
    dealStructureContext = "brand_operator";
  } else if (/franchise/i.test(dealStruct) && /management|operator/i.test(dealStruct)) {
    dealStructureContext = "brand_operator";
  } else if (/franchise/i.test(dealStruct)) {
    dealStructureContext = "franchise_only";
  }

  let brandContext = "unknown";
  if (matchesAny(brandedRaw, [/^yes/, /branded/, /affiliated/, /flagged/, /currently brand/])) {
    brandContext = "currently_branded";
  } else if (matchesAny(brandedRaw, [/^no/, /independent/, /unbranded/, /not branded/])) {
    brandContext = "independent";
  }

  let operatorContext = "unknown";
  if (matchesAny(managedRaw, [/^yes/, /third.party/, /third party/, /managed by/])) {
    operatorContext = "third_party_operator";
  } else if (matchesAny(managedRaw, [/^no/, /self/, /owner.operat/, /not managed/])) {
    operatorContext = "owner_operated";
  }
  if (matchesAny(planManageRaw, [/hire third/, /third party/, /seeking operator/, /need an operator/])) {
    operatorContext = "operator_needed";
  }
  if (dealStructureContext === "brand_operator" || dealStructureContext === "management_agreement") {
    if (operatorContext === "unknown") operatorContext = "operator_needed";
  }
  if (dealStructureContext === "franchise_only" && operatorContext === "unknown") {
    if (matchesAny(planManageRaw, [/self.?manage/, /owner operate/])) operatorContext = "owner_operated";
  }

  const notes = [];
  if (projectTypeContext !== "unknown") notes.push(`projectType=${projectTypeContext}`);
  if (dealStructureContext !== "unknown") notes.push(`dealStructure=${dealStructureContext}`);
  if (brandContext !== "unknown") notes.push(`brand=${brandContext}`);
  if (operatorContext !== "unknown") notes.push(`operator=${operatorContext}`);
  if (hotelType) notes.push(`hotelType=${hotelType.slice(0, 40)}`);

  return {
    projectTypeContext,
    dealStructureContext,
    brandContext,
    operatorContext,
    readinessUseCase: "internal_review",
    contextSummary: notes.join("; ") || "limited signals",
  };
}

/** Human-readable label for narrative copy. */
export function projectTypeContextLabel(ctx) {
  switch (ctx.projectTypeContext) {
    case "new_build":
      return "new-build";
    case "conversion_reflag":
      return "conversion/reflag";
    case "renovation_repositioning":
      return "renovation/repositioning";
    case "operating_asset":
      return "existing operating asset";
    case "land_development_site":
      return "land/development site";
    default:
      return "hospitality";
  }
}

/**
 * @param {string} fieldKey
 * @param {Record<string, unknown>} fields
 * @param {ReturnType<inferReadinessContext>} context
 */
export function getFieldRelevanceForContext(fieldKey, fields, context) {
  const ctx = context || inferReadinessContext(fields);

  // Lease block — only when lease-oriented deal structure
  if (LEASE_STRUCTURE_FIELD_SET.has(fieldKey)) {
    if (isLeaseStructureDealApplicableFromMergedFields(fields) || isLeaseDealContext(ctx)) {
      const isLeaseType = fieldKey === "Lease Type";
      return {
        relevance: isLeaseType ? "foundational" : "important",
        severityIfMissing: isLeaseType ? "limiting" : "enhancement",
        capIfMissing: isLeaseType ? 86 : null,
        reason: "Lease-oriented deal structure",
      };
    }
    return {
      relevance: "not_applicable",
      severityIfMissing: "none",
      capIfMissing: null,
      reason: "Not a lease-oriented deal; lease fields excluded from readiness",
    };
  }

  if (fieldKey === "Lease Type" && !LEASE_STRUCTURE_FIELD_SET.has(fieldKey)) {
    if (isLeaseDealContext(ctx)) {
      return {
        relevance: "foundational",
        severityIfMissing: "limiting",
        capIfMissing: 86,
        reason: "Lease deal requires lease type",
      };
    }
    return {
      relevance: "not_applicable",
      severityIfMissing: "none",
      capIfMissing: null,
      reason: "Franchise or non-lease deal structure",
    };
  }

  // Current brand — high for conversion/reflag/operating; too early for new build/site
  if (fieldKey === "Is the hotel currently branded?") {
    if (isExistingAssetContext(ctx)) {
      return {
        relevance: "foundational",
        severityIfMissing: "limiting",
        capIfMissing: 84,
        reason: "Current brand status is foundational for conversion/reflag and operating assets",
      };
    }
    if (isPreOperatingContext(ctx)) {
      return {
        relevance: "too_early",
        severityIfMissing: "none",
        capIfMissing: null,
        reason: "Hotel may not exist yet; current brand status is not required at this stage",
      };
    }
    return {
      relevance: "important",
      severityIfMissing: "limiting",
      capIfMissing: 88,
      reason: "Brand starting point helps screening when project type is unclear",
    };
  }

  // Current operator — similar pattern
  if (fieldKey === "Is the hotel currently managed by a third-party operator?") {
    if (isPreOperatingContext(ctx) && !isOperatorInScope(ctx)) {
      return {
        relevance: "too_early",
        severityIfMissing: "none",
        capIfMissing: null,
        reason: "Operating management status may not apply before hotel exists",
      };
    }
    if (isExistingAssetContext(ctx)) {
      return {
        relevance: "foundational",
        severityIfMissing: "limiting",
        capIfMissing: 86,
        reason: "Current operator status matters for conversion and operating assets",
      };
    }
    if (isOperatorInScope(ctx)) {
      return {
        relevance: "foundational",
        severityIfMissing: "limiting",
        capIfMissing: 86,
        reason: "Operator path requires current management context",
      };
    }
    if (isFranchiseOnlyContext(ctx)) {
      return {
        relevance: "enhancement",
        severityIfMissing: "enhancement",
        capIfMissing: null,
        reason: "Franchise-only path; current operator detail is supplementary",
      };
    }
    return {
      relevance: "important",
      severityIfMissing: "limiting",
      capIfMissing: 88,
      reason: "",
    };
  }

  // PIP / CapEx — conversion/repositioning/operating; not for new build (use project cost)
  if (fieldKey === "PIP / CapEx Status") {
    if (isExistingAssetContext(ctx)) {
      return {
        relevance: "foundational",
        severityIfMissing: "limiting",
        capIfMissing: 86,
        reason: "PIP/CapEx clarity is foundational for conversion, repositioning, and operating assets",
      };
    }
    if (isPreOperatingContext(ctx)) {
      return {
        relevance: "not_applicable",
        severityIfMissing: "none",
        capIfMissing: null,
        reason: "New build/site uses total project cost rather than operating PIP status",
      };
    }
    return {
      relevance: "important",
      severityIfMissing: "limiting",
      capIfMissing: 88,
      reason: "",
    };
  }

  if (fieldKey === "PIP Budget Range (if conversion)") {
    if (ctx.projectTypeContext === "conversion_reflag" || ctx.projectTypeContext === "renovation_repositioning") {
      return {
        relevance: "important",
        severityIfMissing: "limiting",
        capIfMissing: null,
        reason: "Conversion/repositioning PIP budget supports screening",
      };
    }
    if (isPreOperatingContext(ctx)) {
      return {
        relevance: "not_applicable",
        severityIfMissing: "none",
        capIfMissing: null,
        reason: "PIP conversion budget does not apply to new build/site",
      };
    }
    return {
      relevance: "conditional",
      severityIfMissing: "enhancement",
      capIfMissing: null,
      reason: "Applies when project is a conversion",
    };
  }

  // New build — project cost over PIP
  if (fieldKey === "Total Project Cost Range") {
    if (isPreOperatingContext(ctx)) {
      return {
        relevance: "foundational",
        severityIfMissing: "blocking",
        capIfMissing: 86,
        reason: "Total project cost is foundational for new build and development site deals",
      };
    }
    return {
      relevance: "important",
      severityIfMissing: "limiting",
      capIfMissing: null,
      reason: "",
    };
  }

  // Site / zoning — new build & land
  if (NEW_BUILD_SITE_FIELDS.has(fieldKey)) {
    if (isPreOperatingContext(ctx)) {
      const foundational = fieldKey === "Current Form of Site Control" || fieldKey === "Zoning Status";
      return {
        relevance: foundational ? "foundational" : "important",
        severityIfMissing: foundational ? "limiting" : "enhancement",
        capIfMissing: foundational ? 82 : null,
        reason: "Site control and zoning matter for new build and development site context",
      };
    }
    if (isExistingAssetContext(ctx)) {
      return {
        relevance: "enhancement",
        severityIfMissing: "enhancement",
        capIfMissing: null,
        reason: "Less critical once hotel is operating or converting in place",
      };
    }
  }

  if (fieldKey === "Stage of Development") {
    if (isPreOperatingContext(ctx) || isExistingAssetContext(ctx) || ctx.projectTypeContext === "unknown") {
      return {
        relevance: "foundational",
        severityIfMissing: "limiting",
        capIfMissing: 78,
        reason: "Development stage anchors timeline and readiness expectations",
      };
    }
    return {
      relevance: "foundational",
      severityIfMissing: "limiting",
      capIfMissing: 78,
      reason: "",
    };
  }

  // Operator detail fields
  if (OPERATOR_DETAIL_FIELDS.has(fieldKey)) {
    if (fieldKey === "Plan to Self-Manage or Hire Third Party?") {
      if (isFranchiseOnlyContext(ctx) && !isOperatorInScope(ctx)) {
        return {
          relevance: "important",
          severityIfMissing: "enhancement",
          capIfMissing: null,
          reason: "Self-manage vs operator helps franchise screening",
        };
      }
    }
    if (isOperatorInScope(ctx)) {
      const deep =
        fieldKey === "Preferred Third-Party Operator Profile" ||
        fieldKey === "Services Required From Operator" ||
        fieldKey === "Other Operator Criteria or Notes";
      return {
        relevance: deep ? "important" : "foundational",
        severityIfMissing: deep ? "enhancement" : "limiting",
        capIfMissing: deep ? null : 88,
        reason: "Operator criteria matter for brand + operator or management paths",
      };
    }
    if (isFranchiseOnlyContext(ctx) && !isOperatorInScope(ctx)) {
      return {
        relevance: "not_applicable",
        severityIfMissing: "none",
        capIfMissing: null,
        reason: "Franchise-only deal; operator-specific fields are not in scope",
      };
    }
    if (isPreOperatingContext(ctx) && ctx.operatorContext === "owner_operated" && !isOperatorInScope(ctx)) {
      return {
        relevance: "not_applicable",
        severityIfMissing: "none",
        capIfMissing: null,
        reason: "Pre-operating owner-operated path; operator criteria not in scope",
      };
    }
    if (isPreOperatingContext(ctx) && !isOperatorInScope(ctx)) {
      return {
        relevance: "conditional",
        severityIfMissing: "enhancement",
        capIfMissing: null,
        reason: "Operator detail becomes important if third-party management is in scope",
      };
    }
  }

  // Amenities — more important for conversion/operating
  if (fieldKey === "Additional Amenities") {
    if (isExistingAssetContext(ctx)) {
      return {
        relevance: "important",
        severityIfMissing: "limiting",
        capIfMissing: null,
        reason: "Amenities and product definition matter for conversion and operating assets",
      };
    }
    return {
      relevance: "enhancement",
      severityIfMissing: "enhancement",
      capIfMissing: null,
      reason: "",
    };
  }

  // Universal anchors
  if (fieldKey === "Project Type") {
    return {
      relevance: "foundational",
      severityIfMissing: "blocking",
      capIfMissing: 74,
      reason: "Project type drives context-aware readiness rules",
    };
  }
  if (fieldKey === "Country") {
    return {
      relevance: "foundational",
      severityIfMissing: "blocking",
      capIfMissing: 59,
      reason: "Market/country anchor is required for screening",
    };
  }
  if (fieldKey === "Ownership Type" || fieldKey === "Ownership Structure") {
    return {
      relevance: "foundational",
      severityIfMissing: "blocking",
      capIfMissing: 79,
      reason: "Ownership and control context is required",
    };
  }
  if (fieldKey === "Total Number of Rooms/Keys") {
    if (ctx.projectTypeContext === "land_development_site") {
      return {
        relevance: "conditional",
        severityIfMissing: "limiting",
        capIfMissing: 82,
        reason: "Planned key count may be preliminary for raw land",
      };
    }
    return {
      relevance: "foundational",
      severityIfMissing: "blocking",
      capIfMissing: 79,
      reason: "Key count supports brand/operator screening",
    };
  }
  if (fieldKey === "Preferred Deal Structure") {
    return {
      relevance: "foundational",
      severityIfMissing: "blocking",
      capIfMissing: 84,
      reason: "Deal structure determines lease and operator relevance",
    };
  }
  if (fieldKey === "Primary Goal for the Hotel" || fieldKey === "Top Priorities for Project") {
    return {
      relevance: "foundational",
      severityIfMissing: "limiting",
      capIfMissing: 88,
      reason: "Owner objectives anchor strategic screening",
    };
  }

  // Default — important field, legacy severity applied upstream if needed
  return {
    relevance: "important",
    severityIfMissing: "enhancement",
    capIfMissing: null,
    reason: "",
  };
}

/**
 * Build scoring profile: which required fields count for weighted-v2 under this context.
 * @param {string[]} baseRequiredNames from requiredFieldNamesForReadiness
 */
export function buildContextScoringProfile(fields, context, baseRequiredNames) {
  const relevanceByField = {};
  const activeRequiredFields = [];
  const contextExcludedFields = [];
  const contextConditionalFields = [];
  const contextTooEarlyFields = [];
  const contextRelevanceNotes = [];

  for (const fname of baseRequiredNames) {
    const rel = getFieldRelevanceForContext(fname, fields, context);
    relevanceByField[fname] = rel;

    if (rel.relevance === "not_applicable") {
      contextExcludedFields.push({ field: fname, reason: rel.reason || "Not applicable in this context" });
      continue;
    }
    if (rel.relevance === "too_early") {
      contextTooEarlyFields.push({ field: fname, reason: rel.reason || "Too early to require in this context" });
      continue;
    }
    if (rel.relevance === "conditional") {
      const activated =
        isContextualRequirementMet(fields, fname) || isFilledForContext(fields, fname);
      contextConditionalFields.push({
        field: fname,
        reason: rel.reason || (activated ? "Conditional field activated" : "Conditional field not activated"),
        activated,
      });
      if (activated) activeRequiredFields.push(fname);
      continue;
    }
    if (rel.relevance === "foundational" || rel.relevance === "important" || rel.relevance === "enhancement") {
      activeRequiredFields.push(fname);
    }
    if (rel.reason) {
      contextRelevanceNotes.push({ field: fname, relevance: rel.relevance, note: rel.reason });
    }
  }

  return {
    relevanceByField,
    activeRequiredFields,
    contextAdjustedRequiredFieldCount: activeRequiredFields.length,
    contextExcludedFields,
    contextConditionalFields,
    contextTooEarlyFields,
    contextRelevanceNotes,
  };
}

function hasMarketCountryAnchor(fields) {
  if (isContextualRequirementMet(fields, "Country")) return true;
  return (
    isContextualRequirementMet(fields, "City & State") &&
    isContextualRequirementMet(fields, "Hotel Submarket & Location")
  );
}

function hasOwnershipControl(fields) {
  return (
    isContextualRequirementMet(fields, "Ownership Type") ||
    isContextualRequirementMet(fields, "Ownership Structure")
  );
}

function hasContactDecisionMaker(fields) {
  return (
    isContextualRequirementMet(fields, "Main Contact Name") &&
    isContextualRequirementMet(fields, "Email Address")
  );
}

function hasDocumentationPackage(fields) {
  return (
    isContextualRequirementMet(fields, "Financial Model Available?") ||
    isContextualRequirementMet(fields, "Working with Broker/Advisor?")
  );
}

function hasOwnerObjectives(fields) {
  return (
    isContextualRequirementMet(fields, "Primary Goal for the Hotel") ||
    isContextualRequirementMet(fields, "Top Priorities for Project")
  );
}

function pushCapIfMissing(caps, fields, context, { id, field, reason, defaultMax, isMet }) {
  const rel = getFieldRelevanceForContext(field, fields, context);
  if (rel.relevance === "not_applicable" || rel.relevance === "too_early") return;
  if (isMet()) return;
  const maxScore = rel.capIfMissing != null ? rel.capIfMissing : defaultMax;
  caps.push({
    id,
    maxScore,
    reason,
    contextNote: rel.reason || undefined,
  });
}

/**
 * Context-aware foundational caps (lowest wins). Skips N/A and too_early fields.
 */
export function computeContextAwareCaps(fields, context) {
  const caps = [];

  if (!hasMarketCountryAnchor(fields)) {
    const rel = getFieldRelevanceForContext("Country", fields, context);
    if (rel.relevance !== "not_applicable" && rel.relevance !== "too_early") {
      caps.push({
        id: "marketCountry",
        maxScore: rel.capIfMissing ?? 59,
        reason: "Missing market / country",
        contextNote: rel.reason || undefined,
      });
    }
  }

  pushCapIfMissing(caps, fields, context, {
    id: "projectType",
    field: "Project Type",
    reason: "Missing project type",
    defaultMax: 74,
    isMet: () => isContextualRequirementMet(fields, "Project Type"),
  });
  pushCapIfMissing(caps, fields, context, {
    id: "developmentStage",
    field: "Stage of Development",
    reason: "Missing stage of development",
    defaultMax: 78,
    isMet: () => isContextualRequirementMet(fields, "Stage of Development"),
  });
  pushCapIfMissing(caps, fields, context, {
    id: "keyCount",
    field: "Total Number of Rooms/Keys",
    reason: "Missing key count",
    defaultMax: 79,
    isMet: () => isContextualRequirementMet(fields, "Total Number of Rooms/Keys"),
  });

  if (!hasOwnershipControl(fields)) {
    const rel = getFieldRelevanceForContext("Ownership Type", fields, context);
    if (rel.relevance !== "not_applicable" && rel.relevance !== "too_early") {
      caps.push({
        id: "ownershipControl",
        maxScore: rel.capIfMissing ?? 79,
        reason: "Missing ownership / control status",
        contextNote: rel.reason || undefined,
      });
    }
  }

  pushCapIfMissing(caps, fields, context, {
    id: "brandStatus",
    field: "Is the hotel currently branded?",
    reason: "Missing current brand status",
    defaultMax: 84,
    isMet: () => isContextualRequirementMet(fields, "Is the hotel currently branded?"),
  });
  pushCapIfMissing(caps, fields, context, {
    id: "operatorStatus",
    field: "Is the hotel currently managed by a third-party operator?",
    reason: "Missing current operator status",
    defaultMax: 86,
    isMet: () =>
      isContextualRequirementMet(fields, "Is the hotel currently managed by a third-party operator?"),
  });
  pushCapIfMissing(caps, fields, context, {
    id: "dealStructure",
    field: "Preferred Deal Structure",
    reason: "Missing preferred deal structure",
    defaultMax: 84,
    isMet: () => isContextualRequirementMet(fields, "Preferred Deal Structure"),
  });
  pushCapIfMissing(caps, fields, context, {
    id: "capexPip",
    field: "PIP / CapEx Status",
    reason: "Missing capex / PIP status",
    defaultMax: 86,
    isMet: () => isContextualRequirementMet(fields, "PIP / CapEx Status"),
  });
  pushCapIfMissing(caps, fields, context, {
    id: "projectCost",
    field: "Total Project Cost Range",
    reason: "Missing total project cost range",
    defaultMax: 86,
    isMet: () => isContextualRequirementMet(fields, "Total Project Cost Range"),
  });

  if (!hasOwnerObjectives(fields)) {
    const rel = getFieldRelevanceForContext("Primary Goal for the Hotel", fields, context);
    if (rel.relevance !== "not_applicable" && rel.relevance !== "too_early") {
      caps.push({
        id: "ownerObjectives",
        maxScore: rel.capIfMissing ?? 88,
        reason: "Missing owner objectives / priorities",
        contextNote: rel.reason || undefined,
      });
    }
  }

  if (!hasContactDecisionMaker(fields)) {
    const rel = getFieldRelevanceForContext("Main Contact Name", fields, context);
    if (rel.relevance !== "not_applicable" && rel.relevance !== "too_early") {
      caps.push({
        id: "contactInfo",
        maxScore: rel.capIfMissing ?? 90,
        reason: "Missing contact / decision-maker info",
        contextNote: rel.reason || undefined,
      });
    }
  }

  if (!hasDocumentationPackage(fields)) {
    const rel = getFieldRelevanceForContext("Financial Model Available?", fields, context);
    if (rel.relevance !== "not_applicable" && rel.relevance !== "too_early") {
      caps.push({
        id: "documentation",
        maxScore: rel.capIfMissing ?? 92,
        reason: "Missing documentation package signals",
        contextNote: rel.reason || undefined,
      });
    }
  }

  pushCapIfMissing(caps, fields, context, {
    id: "leaseType",
    field: "Lease Type",
    reason: "Missing lease type",
    defaultMax: 86,
    isMet: () => isContextualRequirementMet(fields, "Lease Type"),
  });

  return caps;
}

export function gapSeverityFromRelevance(fieldKey, fields, context, relevanceByField) {
  const rel = relevanceByField?.[fieldKey] || getFieldRelevanceForContext(fieldKey, fields, context);
  if (rel.relevance === "not_applicable" || rel.relevance === "too_early") return "none";
  if (rel.severityIfMissing && rel.severityIfMissing !== "none") return rel.severityIfMissing;
  if (rel.relevance === "foundational") return "blocking";
  if (rel.relevance === "important") return "limiting";
  return "enhancement";
}

export function isFoundationalForStage(fieldKey, fields, context, relevanceByField) {
  const rel = relevanceByField?.[fieldKey] || getFieldRelevanceForContext(fieldKey, fields, context);
  if (rel.relevance === "not_applicable" || rel.relevance === "too_early") return false;
  return rel.relevance === "foundational";
}

export function listContextFoundationalGaps(fields, context, activeRequiredFields, relevanceByField) {
  const gaps = [];
  const seen = new Set();
  for (const fname of activeRequiredFields) {
    if (!isFoundationalForStage(fname, fields, context, relevanceByField)) continue;
    if (isContextualRequirementMet(fields, fname)) continue;
    if (seen.has(fname)) continue;
    seen.add(fname);
    gaps.push({
      field: fname,
      highlightField: fname,
      label: fname,
      section: "",
      relatedTab: "",
    });
  }
  if (!hasOwnershipControl(fields)) {
    const rel = getFieldRelevanceForContext("Ownership Type", fields, context);
    if (rel.relevance !== "not_applicable" && rel.relevance !== "too_early") {
      gaps.push({
        field: "Ownership / Control",
        highlightField: "Ownership Type",
        label: "Ownership / control status",
        section: "Location & Site Details",
        relatedTab: "Location & Site Details",
      });
    }
  }
  return gaps;
}
