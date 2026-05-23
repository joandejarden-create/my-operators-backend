/**
 * Backfill inference for Operator Capability P0 fields (no guessing).
 */

import {
  DEALS_FIELDS,
  LOCATION_FIELDS,
  SI_FIELDS,
  MP_FIELDS,
  LEGACY_DEAL_BRAND_FIELDS,
  NEEDS_REVIEW,
  SERVICES_TO_PRIORITIES,
  REPORTING_FREQUENCY_MAP,
  inferPrimaryMarketRegionFromCountry,
  strVal,
  listVal,
} from "./operator-capability-inputs.js";
import {
  mapLegacyLandGreenfieldProjectType,
  normalizeProjectTypeLabel,
  resolveProjectTypeKind,
  isTransitionProjectTypeKind,
} from "./project-type.js";

/**
 * @typedef {object} BackfillResult
 * @property {Record<string, unknown>} dealsPatch
 * @property {Record<string, unknown>} locationPatch
 * @property {Record<string, unknown>} siPatch
 * @property {string[]} notes
 * @property {boolean} uncertain
 */

/**
 * @param {Record<string, unknown>} merged — deal.fields merged with linked tables
 * @returns {BackfillResult}
 */
export function inferOperatorCapabilityBackfill(merged) {
  const dealsPatch = {};
  const locationPatch = {};
  const siPatch = {};
  const notes = [];
  let uncertain = false;

  const branded = strVal(merged[LEGACY_DEAL_BRAND_FIELDS.currentlyBranded]).toLowerCase();
  const managed = strVal(merged[LEGACY_DEAL_BRAND_FIELDS.currentlyManaged]).toLowerCase();
  const operatorName = strVal(merged[LEGACY_DEAL_BRAND_FIELDS.operatorNameCurrent]);
  const plan = strVal(merged[SI_FIELDS.planSelfManage]);
  const dealStruct = strVal(merged[MP_FIELDS.preferredDealStructure]);

  const current = inferCurrentOperatingModel(branded, managed, operatorName, dealStruct);
  if (current.value) {
    dealsPatch[DEALS_FIELDS.currentOperatingModel] = current.value;
    notes.push(...current.notes);
    if (current.uncertain) uncertain = true;
  }

  const preferred = inferPreferredFutureOperatingModel(plan, dealStruct);
  if (preferred.value) {
    siPatch[SI_FIELDS.preferredFutureOperatingModel] = preferred.value;
    notes.push(...preferred.notes);
    if (preferred.uncertain) uncertain = true;
  }

  const priorities = inferCapabilityPriorities(merged[SI_FIELDS.servicesRequired]);
  if (priorities.length) {
    siPatch[SI_FIELDS.operatorCapabilityPriorities] = priorities;
    notes.push(`Mapped Services Required → ${priorities.length} capability priorities`);
  }

  const freq = inferOwnerReportingFrequency(merged[SI_FIELDS.preferredReportingFrequency]);
  if (freq) {
    siPatch[SI_FIELDS.ownerReportingFrequency] = freq;
    notes.push("Copied Preferred Reporting Frequency → Owner Reporting Frequency");
  }

  const region = inferPrimaryMarketRegionFromCountry(strVal(merged[LOCATION_FIELDS.country]));
  if (region) {
    locationPatch[LOCATION_FIELDS.primaryMarketRegion] = region;
    if (region === NEEDS_REVIEW) {
      uncertain = true;
      notes.push("Primary Market Region: country not in known NA/CALA/Europe list");
    }
  }

  const rawProjectType = strVal(merged[DEALS_FIELDS.projectType]);
  const projectTypePatch = inferProjectTypeMigration(merged, rawProjectType);
  if (projectTypePatch.value) {
    dealsPatch[DEALS_FIELDS.projectType] = projectTypePatch.value;
    notes.push(projectTypePatch.note);
    if (projectTypePatch.uncertain) uncertain = true;
  }

  const opening = inferOpeningTransitionPhase(
    strVal(merged["Stage of Development"]),
    projectTypePatch.value || normalizeProjectTypeLabel(rawProjectType) || rawProjectType,
    strVal(merged["Expected Opening or Rebranding Date"])
  );
  if (opening.value) {
    dealsPatch[DEALS_FIELDS.openingTransitionPhase] = opening.value;
    notes.push(...opening.notes);
    if (opening.uncertain) uncertain = true;
  }

  return { dealsPatch, locationPatch, siPatch, notes, uncertain };
}

/**
 * Migrate deprecated Project Type values only (never infer acquisition as project type).
 * @param {Record<string, unknown>} merged
 * @param {string} rawProjectType
 */
function inferProjectTypeMigration(merged, rawProjectType) {
  if (!rawProjectType) return { value: "", note: "", uncertain: false };
  const canonical = normalizeProjectTypeLabel(rawProjectType);
  if (canonical && canonical !== rawProjectType && !/acquisition of operating/i.test(rawProjectType)) {
    return {
      value: canonical,
      note: `Project Type: ${rawProjectType} → ${canonical}`,
      uncertain: false,
    };
  }
  if (/^land\s*\/\s*greenfield/i.test(rawProjectType)) {
    const mapped = mapLegacyLandGreenfieldProjectType(merged);
    return { value: mapped.value, note: mapped.note, uncertain: mapped.value === "Other / To Be Confirmed" };
  }
  if (/acquisition of operating/i.test(rawProjectType)) {
    return {
      value: "",
      note: "Skip Project Type: legacy Acquisition value — use Deal Situation field when available",
      uncertain: false,
    };
  }
  return { value: "", note: "", uncertain: false };
}

/**
 * @param {string} branded
 * @param {string} managed
 * @param {string} operatorName
 * @param {string} dealStruct
 */
function inferCurrentOperatingModel(branded, managed, operatorName, dealStruct) {
  const notes = [];
  const isBranded = /^yes|branded|affiliated|flagged/.test(branded);
  const isUnbranded = /^no|independent|unbranded|not branded/.test(branded);
  const isManaged = /^yes|third.party|third party/.test(managed);
  const isNotManaged = /^no|self|owner.operat|not managed/.test(managed);

  if (!branded && !managed) {
    return { value: "", notes: ["Skip Current Operating Model: no branded/managed signals"], uncertain: false };
  }

  if (branded && managed && isBranded && isManaged) {
    return {
      value: "Third-party managed (branded)",
      notes: ["Current Operating Model: branded + third-party managed"],
      uncertain: false,
    };
  }
  if (isBranded && isManaged && operatorName && /independent|collection/i.test(operatorName)) {
    return {
      value: "Third-party managed (independent/collection)",
      notes: ["Current Operating Model: branded + operator + independent/collection hint"],
      uncertain: false,
    };
  }
  if (isBranded && isNotManaged) {
    return {
      value: "Owner-operated (branded/franchised)",
      notes: ["Current Operating Model: branded, not third-party managed"],
      uncertain: false,
    };
  }
  if (isUnbranded && isNotManaged) {
    return {
      value: "Owner-operated (unbranded)",
      notes: ["Current Operating Model: unbranded, not third-party managed"],
      uncertain: false,
    };
  }
  if (isUnbranded && isManaged) {
    return {
      value: "Third-party managed (independent/collection)",
      notes: ["Current Operating Model: unbranded + third-party managed"],
      uncertain: false,
    };
  }
  if (/brand.managed/i.test(dealStruct) && isBranded) {
    return { value: "Brand-managed", notes: ["Current Operating Model: deal structure brand-managed"], uncertain: false };
  }
  if (/lease/i.test(dealStruct) && isManaged) {
    return {
      value: "Lease/operator lease structure",
      notes: ["Current Operating Model: lease structure + managed"],
      uncertain: false,
    };
  }
  if ((isBranded && !isManaged && !isNotManaged) || (isManaged && !isBranded && !isUnbranded)) {
    return {
      value: NEEDS_REVIEW,
      notes: ["Current Operating Model: partial signals — Needs Review"],
      uncertain: true,
    };
  }
  return {
    value: NEEDS_REVIEW,
    notes: ["Current Operating Model: conflicting or unclear branded/managed combination"],
    uncertain: true,
  };
}

/**
 * @param {string} plan
 * @param {string} dealStruct
 */
function inferPreferredFutureOperatingModel(plan, dealStruct) {
  const notes = [];
  const p = plan.toLowerCase();
  const d = dealStruct.toLowerCase();

  if (p.includes("owner-operated") && !/third|brand-managed/i.test(p)) {
    return { value: "Owner-operated", notes: ["Preferred Future: Plan = Owner-Operated"], uncertain: false };
  }
  if (/third.party managed|third-party managed/i.test(p)) {
    if (/brand \+ third|combined/i.test(d)) {
      return {
        value: "Brand + third-party management",
        notes: ["Preferred Future: third-party plan + brand+operator deal structure"],
        uncertain: false,
      };
    }
    if (/franchise only/i.test(d) && !/management/i.test(d)) {
      return {
        value: NEEDS_REVIEW,
        notes: ["Preferred Future: third-party plan conflicts with franchise-only structure"],
        uncertain: true,
      };
    }
    return {
      value: "Third-party management only",
      notes: ["Preferred Future: Plan = Third-party Managed"],
      uncertain: false,
    };
  }
  if (/brand-managed/i.test(p)) {
    return { value: "Brand-managed", notes: ["Preferred Future: Plan = Brand-Managed"], uncertain: false };
  }
  if (/undecided/i.test(p)) {
    if (/franchise only/i.test(d)) {
      return {
        value: "Franchise/license only (owner or third-party operator)",
        notes: ["Preferred Future: undecided plan + franchise-only structure"],
        uncertain: false,
      };
    }
    if (/third.party management only/i.test(d)) {
      return {
        value: "Third-party management only",
        notes: ["Preferred Future: undecided plan + third-party management structure"],
        uncertain: false,
      };
    }
    if (/brand \+ third|combined/i.test(d)) {
      return {
        value: "Brand + third-party management",
        notes: ["Preferred Future: undecided plan + combined structure"],
        uncertain: false,
      };
    }
    return {
      value: "Undecided / exploring",
      notes: ["Preferred Future: Plan = Undecided"],
      uncertain: false,
    };
  }
  if (!plan && d) {
    if (/franchise only/i.test(d)) {
      return {
        value: "Franchise/license only (owner or third-party operator)",
        notes: ["Preferred Future: from deal structure only (franchise)"],
        uncertain: true,
      };
    }
    if (/third.party management only/i.test(d)) {
      return {
        value: "Third-party management only",
        notes: ["Preferred Future: from deal structure only (management)"],
        uncertain: true,
      };
    }
    if (/brand \+ third|combined/i.test(d)) {
      return {
        value: "Brand + third-party management",
        notes: ["Preferred Future: from deal structure only (combined)"],
        uncertain: true,
      };
    }
  }
  if (plan || dealStruct) {
    return {
      value: NEEDS_REVIEW,
      notes: ["Preferred Future: could not map plan + deal structure reliably"],
      uncertain: true,
    };
  }
  return { value: "", notes: [], uncertain: false };
}

/**
 * @param {unknown} servicesRaw
 * @returns {string[]}
 */
function inferCapabilityPriorities(servicesRaw) {
  const services = listVal(servicesRaw);
  const out = new Set();
  for (const s of services) {
    const mapped = SERVICES_TO_PRIORITIES[s];
    if (mapped) out.add(mapped);
  }
  return [...out];
}

/**
 * @param {unknown} legacyFreq
 * @returns {string}
 */
function inferOwnerReportingFrequency(legacyFreq) {
  const f = strVal(legacyFreq);
  if (!f) return "";
  return REPORTING_FREQUENCY_MAP[f] || f;
}

/**
 * @param {string} stage
 * @param {string} projectType
 * @param {string} openingDate
 */
function inferOpeningTransitionPhase(stage, projectType, openingDate) {
  const notes = [];
  const st = stage.toLowerCase();
  const kind = resolveProjectTypeKind(projectType);

  if (/stabilized|operating asset|in operation/.test(st)) {
    if (kind === "conversion_reflag") {
      return {
        value: "Rebranding in place",
        notes: ["Opening phase: stabilized + Conversion / Reflag"],
        uncertain: false,
      };
    }
    if (kind === "renovation_repositioning") {
      return {
        value: "Reopening after renovation",
        notes: ["Opening phase: stabilized + Renovation / Repositioning"],
        uncertain: false,
      };
    }
    if (kind === "existing_operating") {
      return { value: "N/A (stabilized operating)", notes: ["Opening phase: Existing Operating Hotel"], uncertain: false };
    }
    return { value: "N/A (stabilized operating)", notes: ["Opening phase: stabilized asset"], uncertain: false };
  }
  if (/under construction|construction/.test(st)) {
    return { value: "Construction", notes: ["Opening phase: under construction"], uncertain: false };
  }
  if (/fully entitled|entitlement/.test(st)) {
    return { value: "Planning / entitlement", notes: ["Opening phase: entitlement stage"], uncertain: false };
  }
  if (/land under control/.test(st)) {
    return { value: "Planning / entitlement", notes: ["Opening phase: land under control"], uncertain: false };
  }
  if (isTransitionProjectTypeKind(kind) && openingDate) {
    return {
      value: NEEDS_REVIEW,
      notes: ["Opening phase: transition project type with date but stage unclear — Needs Review"],
      uncertain: true,
    };
  }
  if (kind === "new_build" && !stage) {
    return {
      value: NEEDS_REVIEW,
      notes: ["Opening phase: New Build without stage — Needs Review"],
      uncertain: true,
    };
  }
  if (kind === "adaptive_reuse" && /entitlement|land under|planning/i.test(st)) {
    return { value: "Planning / entitlement", notes: ["Opening phase: Adaptive Reuse + early stage"], uncertain: false };
  }
  if (stage || projectType || openingDate) {
    return {
      value: NEEDS_REVIEW,
      notes: ["Opening phase: insufficient stage/type alignment"],
      uncertain: true,
    };
  }
  return { value: "", notes: [], uncertain: false };
}

/**
 * Detect conflicts between current and preferred operating models.
 * @param {Record<string, unknown>} merged
 * @returns {string[]}
 */
export function detectOperatingModelConflicts(merged) {
  const conflicts = [];
  const current = strVal(merged[DEALS_FIELDS.currentOperatingModel]);
  const preferred = strVal(merged[SI_FIELDS.preferredFutureOperatingModel]);
  const plan = strVal(merged[SI_FIELDS.planSelfManage]);

  if (!current || !preferred) return conflicts;

  const ownerCurrent = /owner-operated/i.test(current);
  const ownerPreferred = preferred === "Owner-operated";
  const tpCurrent = /third.party managed/i.test(current);
  const tpPreferred = /third.party management only|brand \+ third/i.test(preferred);

  if (ownerCurrent && tpPreferred && !/undecided|needs review/i.test(preferred)) {
    conflicts.push("Current model is owner-operated but preferred future targets third-party management");
  }
  if (tpCurrent && ownerPreferred && !/undecided|needs review/i.test(preferred)) {
    conflicts.push("Current model is third-party managed but preferred future is owner-operated");
  }
  if (plan === "Owner-Operated" && tpPreferred) {
    conflicts.push("Plan to Self-Manage is Owner-Operated but Preferred Future Operating Model expects third-party");
  }
  if (plan === "Third-party Managed" && ownerPreferred) {
    conflicts.push("Plan to Self-Manage is Third-party Managed but Preferred Future Operating Model is owner-operated");
  }
  return conflicts;
}
