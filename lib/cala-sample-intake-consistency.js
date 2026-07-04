/**
 * Cross-field consistency for CALA sample Deal Setup intake.
 * Aligns F&B program types with outlet concepts and branding with form logic.
 */

const UNBRANDED_BRAND_PATTERNS =
  /^(unbranded|unbranded\b|planned|component|independent heritage asset)/i;

/** @param {string} brand */
function isUnbrandedBrandLabel(brand) {
  const s = String(brand || "").trim();
  if (!s) return true;
  return UNBRANDED_BRAND_PATTERNS.test(s) || /\bunbranded\b/i.test(s) || /\bplanned\b/i.test(s);
}

/** @param {string} currentBrand @param {object} cfg */
function resolveCurrentBrandAffiliation(currentBrand, cfg) {
  const raw = String(currentBrand || cfg.currentBrand || "").trim();
  const aliases = {
    "legacy chain flag ending": "Four Points by Sheraton",
    "legacy flag expiring": "Holiday Inn",
    "independent resort flag": "Independent (local resort operator)",
    "independent boutique": "Independent (boutique collection)",
    "independent wellness resort": "Independent (wellness resort)",
    "independent / legacy flag": "Independent (legacy urban flag)",
    "independent": "Independent (legacy urban flag)",
    "legacy chain flag ending (sample)": "Four Points by Sheraton",
  };
  const key = raw.toLowerCase();
  if (aliases[key]) return aliases[key];
  for (const [k, v] of Object.entries(aliases)) {
    if (key === k) return v;
  }
  return raw;
}

/** @param {string} brand */
function parentCompanyFromBrand(brand) {
  const b = String(brand || "").toLowerCase();
  if (!b || b.includes("independent")) return "";
  if (b.includes("marriott") || b.includes("sheraton") || b.includes("courtyard") || b.includes("westin") || b.includes("w hotels") || b.includes("four points") || b.includes("ac hotel") || b.includes("delta hotel") || b.includes("renaissance") || b.includes("ritz") || b.includes("st. regis") || b.includes("autograph") || b.includes("fairfield") || b.includes("moxy") || b.includes("residence inn") || b.includes("city express")) {
    return "Marriott International";
  }
  if (b.includes("hilton") || b.includes("curio") || b.includes("tapestry") || b.includes("canopy") || b.includes("hampton")) {
    return "Hilton";
  }
  if (b.includes("hyatt") || b.includes("andaz")) return "Hyatt";
  if (b.includes("ihg") || b.includes("holiday inn") || b.includes("kimpton")) return "IHG Hotels & Resorts";
  if (b.includes("accor") || b.includes("sofitel") || b.includes("mgallery")) return "Accor";
  return "";
}

/**
 * @param {string} outletText
 * @param {object} cfg
 * @returns {string[]}
 */
export function inferFbProgramTypesFromOutlets(outletText, cfg = {}) {
  const t = String(outletText || "").toLowerCase();
  const types = new Set();

  if (/restaurant|dining|steakhouse|buffet|bistro|signature|all-day/.test(t)) {
    types.add("Full-Service Restaurant + Bar");
  }
  if (/\bbar\b|lounge|pool bar|rooftop|show kitchen/.test(t)) {
    types.add("Pool Bar / Rooftop Bar / Feature Bar");
  }
  if (/café|cafe|grab-and-go|grab and go|market|coffee/.test(t)) {
    types.add("Coffee Shop / Cafe");
  }
  if (/in-room|room service/.test(t)) {
    types.add("In-Room Dining / Room Service");
  }
  if (/grab-and-go|grab and go/.test(t) && !/restaurant|dining|steakhouse|buffet/.test(t)) {
    types.add("Minimal / Grab & Go");
  }

  if (cfg.serviceModel?.includes("All-Inclusive") || cfg.fbProgram?.includes("All-Inclusive")) {
    types.add("Full-Service Restaurant + Bar");
    types.add("Pool Bar / Rooftop Bar / Feature Bar");
    types.add("Coffee Shop / Cafe");
  }

  if (types.size === 0) {
    if (cfg.hotelType === "Airport" || String(cfg.submarket || "").toLowerCase().includes("airport")) {
      return ["Full-Service Restaurant + Bar", "Coffee Shop / Cafe", "Minimal / Grab & Go"];
    }
    return ["Full-Service Restaurant + Bar", "Coffee Shop / Cafe"];
  }

  return [...types];
}

/** @param {string} outletText */
export function countFbOutletsFromNames(outletText) {
  const parts = String(outletText || "")
    .split(/[;]/)
    .map((s) => s.trim())
    .filter(Boolean);
  return Math.max(1, parts.length);
}

/**
 * @param {Record<string, unknown>} fields
 * @param {object} cfg
 */
export function reconcileBrandingFields(fields, cfg = {}) {
  if (cfg.currentBrand && cfg.layer !== "reference") {
    fields["Current Brand Affiliation"] = resolveCurrentBrandAffiliation(cfg.currentBrand, cfg);
  }

  const brandedAnswer = fields["Is the hotel currently branded?"];
  const branded =
    brandedAnswer === "Yes" || (brandedAnswer !== "No" && cfg.currentlyBranded === "Yes");
  const notBranded = brandedAnswer === "No" || cfg.currentlyBranded === "No";
  const worked =
    fields["Have you worked with any of your preferred brands/operators before?"] === "Yes" ||
    cfg.workedWithPreferred === "Yes";

  if (notBranded || (!branded && !brandedAnswer)) {
    fields["Current Brand Affiliation"] = "";
    fields["Parent Company Name"] = "";
    if (fields["Existing flag staying or being replaced?"] !== "Not Applicable (Unbranded or New Build)") {
      const stage = String(fields["Stage of Development"] || cfg.stage || "");
      if (stage.includes("New") || String(cfg.projectType || "").includes("New Build")) {
        fields["Existing flag staying or being replaced?"] =
          "Not Applicable (Unbranded or New Build)";
      }
    }
    return fields;
  }

  const brand = resolveCurrentBrandAffiliation(
    String(fields["Current Brand Affiliation"] || ""),
    cfg
  );
  if (!isUnbrandedBrandLabel(brand)) {
    fields["Current Brand Affiliation"] = brand;
  } else if (cfg.currentBrand && !isUnbrandedBrandLabel(cfg.currentBrand)) {
    fields["Current Brand Affiliation"] = resolveCurrentBrandAffiliation(cfg.currentBrand, cfg);
  }

  if (worked) {
    if (!fields["Parent Company Name"]) {
      fields["Parent Company Name"] =
        cfg.parentCompanyName ||
        parentCompanyFromBrand(String(fields["Current Brand Affiliation"] || "")) ||
        "";
    }
  } else {
    fields["Parent Company Name"] = "";
  }

  return fields;
}

/**
 * @param {Record<string, unknown>} fields
 * @param {object} cfg
 */
export function reconcileFbFields(fields, cfg = {}) {
  const outletsYes = fields["F&B Outlets?"] !== "No";
  if (!outletsYes && cfg.fbOutlets === "No") return fields;

  const outletNames = String(
    fields["Outlet Names / Concepts"] || cfg.outletNames || ""
  ).trim();
  if (outletNames) {
    fields["Outlet Names / Concepts"] = outletNames;
    const types = inferFbProgramTypesFromOutlets(outletNames, cfg);
    fields["F&B Program Type"] = types;
    const count = countFbOutletsFromNames(outletNames);
    const cfgCount = Number(cfg.fbCount);
    fields["Number of F&B Outlets"] = String(
      Math.max(Number.isFinite(cfgCount) ? cfgCount : 0, count) || count
    );
    fields["F&B Outlets?"] = "Yes";
  }

  return fields;
}

const PLAN_ALIASES = {
  "hire third-party operator": "Third-party Managed",
  "third-party operator required": "Third-party Managed",
  "third-party luxury operator required": "Third-party Managed",
  "retain operator or re-bid": "Third-party Managed",
  "third-party operator": "Third-party Managed",
};

const MULTI_OTHER_DEFAULTS = {
  "Top Priorities for Project": [
    "Strong Financial Performance",
    "Brand Recognition",
    "Operational Expertise",
  ],
  "Top Concerns for this Project": ["High Costs", "Inflexibility", "Underperformance"],
  "Top 3 Deal Breakers": [
    "High Fees or Unfavorable Economics",
    "Insufficient Operator Experience (e.g. < 10 Years)",
    "Inflexible Contract Terms",
  ],
  "Must-haves From Brand or Operator": [
    "Strong Distribution and Marketing Support",
    "Experienced Operator (e.g. 10+ Years)",
    "Flexible Contract Terms",
  ],
  "Top 3 Success Metrics": ["RevPAR Growth", "NOI (Net Operating Income)", "Guest Satisfaction"],
};

/** @param {string} field */
function otherTextFieldName(field) {
  return (
    {
      "Top Priorities for Project": "Top Priorities for Project Other",
      "Top Concerns for this Project": "Top Concerns for this Project Other",
      "Top 3 Deal Breakers": "Top 3 Deal Breakers Other",
      "Must-haves From Brand or Operator": "Must-haves From Brand or Operator Other",
      "Top 3 Success Metrics": "Top 3 Success Metrics Other",
      "Target Guest Segment": "Target Guest Segment Other",
    }[field] || `${field} Other`
  );
}

/** @param {Record<string, unknown>} fields */
export function expectedOperatingModel(fields) {
  const managed =
    fields["Is the hotel currently managed by a third-party operator?"] === "Yes";
  const branded = fields["Is the hotel currently branded?"] === "Yes";
  if (managed && branded) return "Third-party managed (branded)";
  if (managed && !branded) return "Third-party managed (independent/collection)";
  if (!managed && branded) return "Owner-operated (branded/franchised)";
  return "Owner-operated (unbranded)";
}

/** @param {Record<string, unknown>} fields @param {object} cfg */
export function reconcileOperatingAndOperatorFields(fields, cfg = {}) {
  const managed =
    fields["Is the hotel currently managed by a third-party operator?"] === "Yes" ||
    cfg.currentlyManaged === "Yes";
  const branded =
    fields["Is the hotel currently branded?"] === "Yes" || cfg.currentlyBranded === "Yes";

  fields["Current Operating Model"] = expectedOperatingModel({
    ...fields,
    "Is the hotel currently managed by a third-party operator?": managed ? "Yes" : "No",
    "Is the hotel currently branded?": branded ? "Yes" : "No",
  });

  let plan = String(fields["Plan to Self-Manage or Hire Third Party?"] || cfg.operatorPlan || "").trim();
  const planKey = plan.toLowerCase();
  if (PLAN_ALIASES[planKey]) plan = PLAN_ALIASES[planKey];
  if (/third/i.test(plan) && plan !== "Third-party Managed") plan = "Third-party Managed";
  if (/owner/i.test(plan) && /operat/i.test(plan)) plan = "Owner-Operated";
  fields["Plan to Self-Manage or Hire Third Party?"] = plan || "Third-party Managed";

  const bids = String(fields["Who should receive bids for this project?"] || "");
  const wantsOperator = fields["Plan to Self-Manage or Hire Third Party?"] === "Third-party Managed";
  if (wantsOperator && /Brands Only/i.test(bids)) {
    fields["Who should receive bids for this project?"] = "Both Brands and Third-Party Operators";
  }

  if (managed) {
    if (!String(fields["Operator Name Current"] || "").trim()) {
      fields["Operator Name Current"] =
        cfg.operatorCurrent || cfg.operatorNameDefault || "Latin America Lodging Partners";
    }
  } else {
    fields["Operator Name Current"] = "";
  }
}

/** @param {Record<string, unknown>} fields */
function reconcileRoomCounts(fields) {
  const keys = parseInt(String(fields["Total Number of Rooms/Keys"] || "").replace(/\D/g, ""), 10);
  const std = parseInt(String(fields["Number of Standard Rooms"] || "").replace(/\D/g, ""), 10);
  const suites = parseInt(String(fields["Number of Suites"] || "").replace(/\D/g, ""), 10);
  if (!keys || !std) return;
  const sum = std + (suites || 0);
  if (Math.abs(keys - sum) <= 2) return;
  if (suites > 0 && std > 0) {
    fields["Number of Standard Rooms"] = String(Math.max(1, keys - suites));
  } else if (std > keys) {
    fields["Number of Standard Rooms"] = String(Math.max(1, keys - 1));
    fields["Number of Suites"] = "1";
  }
}

/** @param {Record<string, unknown>} fields @param {object} cfg */
function reconcileConditionalDescriptions(fields, cfg = {}) {
  if (fields["Site/Development Restrictions?"] === "Yes") {
    if (!String(fields["Site/Development Restrictions Description"] || "").trim()) {
      fields["Site/Development Restrictions Description"] =
        cfg.siteRestrictionsDescription ||
        "Entitlement package includes use, height, and loading constraints for the hotel component.";
    }
  } else {
    fields["Site/Development Restrictions Description"] = "";
  }

  if (fields["Regulatory or Permitting Issues?"] === "Yes") {
    if (!String(fields["Regulatory or Permitting Issues Description"] || "").trim()) {
      fields["Regulatory or Permitting Issues Description"] =
        cfg.regulatoryDescription ||
        "Permitting path defined; owner counsel tracking municipal and tourism authority approvals.";
    }
  } else {
    fields["Regulatory or Permitting Issues Description"] = "";
  }

  const franchiseField =
    "Has there ever been a franchise, branded management, affiliation or similar agreement pertaining to the proposed hotel or site?";
  if (fields[franchiseField] === "Yes") {
    if (!String(fields["Has there ever been a franchise description"] || "").trim()) {
      fields["Has there ever been a franchise description"] =
        cfg.franchiseHistoryDescription ||
        "Prior branded management and franchise affiliation on this site; agreement terms under review for transition.";
    }
  } else {
    fields["Has there ever been a franchise description"] = "";
  }
}

/** @param {Record<string, unknown>} fields @param {object} cfg */
function reconcileSiteAndProjectFields(fields, cfg = {}) {
  let unit = String(fields["Total Site Size Unit"] || cfg.siteSizeUnit || "").trim();
  const size = parseFloat(String(fields["Total Site Size"] || cfg.siteSize || ""));
  if ((unit === "Sq. M." || unit === "Sq. Ft.") && size > 0 && size < 10) {
    fields["Total Site Size Unit"] = "Acres";
    unit = "Acres";
  } else if (cfg.siteSizeUnit && (!unit || (unit === "Sq. Ft." && size < 10))) {
    fields["Total Site Size Unit"] = cfg.siteSizeUnit;
  }

  const projectType = String(fields["Project Type"] || cfg.projectType || "");
  const isConversion = /conversion|reflag|reposition|renovation/i.test(projectType);
  const gapFields = new Set((cfg.intentionalGaps || []).map((g) => g.field));

  if (isConversion && !gapFields.has("PIP / CapEx Status") && !String(fields["PIP / CapEx Status"] || "").trim()) {
    fields["PIP / CapEx Status"] = cfg.pipStatus || "Planned";
  }
  if (isConversion && !gapFields.has("PIP Budget Range (if conversion)") && !String(fields["PIP Budget Range (if conversion)"] || "").trim()) {
    fields["PIP Budget Range (if conversion)"] = cfg.pipBudget || cfg.pipBudgetDefault || "$3M – $5M";
  }
}

/** @param {Record<string, unknown>} fields */
function reconcileMultiSelectOtherFields(fields) {
  for (const [field, defaults] of Object.entries(MULTI_OTHER_DEFAULTS)) {
    const arr = Array.isArray(fields[field])
      ? [...fields[field]]
      : String(fields[field] || "")
          .split(",")
          .map((s) => s.trim())
          .filter(Boolean);
    if (!arr.includes("Other")) continue;
    const otherKey = otherTextFieldName(field);
    const otherText = String(fields[otherKey] || fields[`${field} Other Text`] || "").trim();
    if (otherText) {
      fields[field] = arr;
      continue;
    }
    const withoutOther = arr.filter((x) => x !== "Other");
    fields[field] = withoutOther.length >= 2 ? withoutOther : defaults;
    delete fields[otherKey];
    delete fields[`${field} Other Text`];
  }

  if (fields["Target Guest Segment"] === "Other") {
    const other = String(fields["Target Guest Segment Other"] || "").trim();
    if (!other) {
      fields["Target Guest Segment"] =
        String(fields["Hotel Type"] || "").includes("Airport") ||
        String(fields["Hotel Submarket & Location"] || "").toLowerCase().includes("airport")
          ? "Corporate / Business"
          : "Leisure";
      fields["Target Guest Segment Other"] = "";
    }
  } else {
    fields["Target Guest Segment Other"] = "";
  }
}

/** @param {Record<string, unknown>} fields @param {object} cfg */
export function reconcileFullIntakeFields(fields, cfg = {}) {
  reconcileFbFields(fields, cfg);
  reconcileBrandingFields(fields, cfg);
  reconcileOperatingAndOperatorFields(fields, cfg);
  reconcileRoomCounts(fields);
  reconcileConditionalDescriptions(fields, cfg);
  reconcileSiteAndProjectFields(fields, cfg);
  reconcileMultiSelectOtherFields(fields);
  return fields;
}

/**
 * Audit fictional intake fields; returns list of issue strings.
 * @param {Record<string, unknown>} fields
 * @param {string} [label]
 */
export function auditIntakeFieldConsistency(fields, label = "fictional") {
  const issues = [];
  const branded = fields["Is the hotel currently branded?"];
  const worked = fields["Have you worked with any of your preferred brands/operators before?"];
  const brand = String(fields["Current Brand Affiliation"] || "").trim();
  const parent = String(fields["Parent Company Name"] || "").trim();
  const outlets = String(fields["Outlet Names / Concepts"] || "");
  const programs = fields["F&B Program Type"];
  const programList = Array.isArray(programs) ? programs : programs ? [programs] : [];
  const expected = inferFbProgramTypesFromOutlets(outlets, {});

  if (branded === "No" && brand) {
    issues.push(`${label}: branded=No but Current Brand Affiliation is set (${brand})`);
  }
  if (branded === "Yes" && !brand) {
    issues.push(`${label}: branded=Yes but Current Brand Affiliation is empty`);
  }
  if (worked === "No" && parent) {
    issues.push(`${label}: workedWithPreferred=No but Parent Company is set (${parent})`);
  }
  if (worked === "Yes" && branded === "Yes" && !parent && brand && !/independent/i.test(brand)) {
    issues.push(`${label}: workedWithPreferred=Yes and chain brand but Parent Company empty`);
  }
  if (outlets && programList.length) {
    const missing = expected.filter((p) => !programList.includes(p));
    if (missing.length) {
      issues.push(`${label}: F&B Program Type missing [${missing.join(", ")}]`);
    }
  }

  const keys = parseInt(String(fields["Total Number of Rooms/Keys"] || "").replace(/\D/g, ""), 10);
  const std = parseInt(String(fields["Number of Standard Rooms"] || "").replace(/\D/g, ""), 10);
  const suites = parseInt(String(fields["Number of Suites"] || "").replace(/\D/g, ""), 10);
  if (keys && std + suites && Math.abs(keys - (std + suites)) > 2) {
    issues.push(`${label}: room keys ${keys} vs standard+suites ${std}+${suites}`);
  }

  const managed = fields["Is the hotel currently managed by a third-party operator?"];
  const op = String(fields["Operator Name Current"] || "").trim();
  if (managed === "Yes" && !op) issues.push(`${label}: managed Yes but no operator name`);
  if (managed === "No" && op) issues.push(`${label}: managed No but operator name set`);

  const model = String(fields["Current Operating Model"] || "");
  const expectedModel = expectedOperatingModel(fields);
  if (model && model !== expectedModel) {
    issues.push(`${label}: operating model "${model}" expected "${expectedModel}"`);
  }

  const plan = String(fields["Plan to Self-Manage or Hire Third Party?"] || "");
  const bids = String(fields["Who should receive bids for this project?"] || "");
  if (/third/i.test(plan) && /Brands Only/i.test(bids)) {
    issues.push(`${label}: third-party plan but bids brands-only`);
  }

  const fb = fields["F&B Outlets?"];
  if (fb === "No" && (fields["Number of F&B Outlets"] || outlets)) {
    issues.push(`${label}: F&B Outlets No but details present`);
  }
  if (fb === "Yes" && !outlets) issues.push(`${label}: F&B Outlets Yes but no outlet names`);

  if (fields["Site/Development Restrictions?"] === "Yes" && !String(fields["Site/Development Restrictions Description"] || "").trim()) {
    issues.push(`${label}: site restrictions Yes without description`);
  }
  if (fields["Regulatory or Permitting Issues?"] === "Yes" && !String(fields["Regulatory or Permitting Issues Description"] || "").trim()) {
    issues.push(`${label}: regulatory Yes without description`);
  }
  const franchiseField =
    "Has there ever been a franchise, branded management, affiliation or similar agreement pertaining to the proposed hotel or site?";
  if (fields[franchiseField] === "Yes" && !String(fields["Has there ever been a franchise description"] || "").trim()) {
    issues.push(`${label}: franchise history Yes without description`);
  }

  for (const field of Object.keys(MULTI_OTHER_DEFAULTS)) {
    const arr = Array.isArray(fields[field]) ? fields[field] : [];
    if (arr.includes("Other")) {
      const otherText = String(fields[otherTextFieldName(field)] || "").trim();
      if (!otherText) issues.push(`${label}: ${field} includes Other without detail`);
    }
  }
  if (fields["Target Guest Segment"] === "Other" && !String(fields["Target Guest Segment Other"] || "").trim()) {
    issues.push(`${label}: Target Guest Other without text`);
  }

  const unit = String(fields["Total Site Size Unit"] || "");
  const size = parseFloat(String(fields["Total Site Size"] || ""));
  if (unit === "Sq. M." && size > 0 && size < 10) {
    issues.push(`${label}: site size ${size} ${unit} likely wrong unit`);
  }

  return issues;
}

/**
 * @param {Record<string, unknown>} fields
 * @param {object} cfg
 */
export function applyCalaIntakeConsistency(fields, cfg = {}) {
  return reconcileFullIntakeFields(fields, cfg);
}
