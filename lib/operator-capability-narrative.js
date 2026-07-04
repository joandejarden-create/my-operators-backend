/**
 * Operator Capability Snapshot — neutral review-preparation copy (not consulting advice).
 * Enriches builder output without changing capability inference rules.
 */

import { strVal, listVal, SI_FIELDS, DEALS_FIELDS, LOCATION_FIELDS } from "./operator-capability-inputs.js";

/** Allowed only inside the standard disclaimer sentence. */
export const OCS_DISCLAIMER_ALLOWLIST =
  "does not recommend, rank, endorse, or select operators";

const FORBIDDEN_PHRASES = [
  /dealality recommends/i,
  /\bwe recommend\b/i,
  /recommended operator/i,
  /best operator/i,
  /best operating model/i,
  /preferred pathway/i,
  /preferred operator path/i,
  /\bshould select\b/i,
  /\bshould choose\b/i,
  /\bthe owner should\b/i,
  /\bowner should use\b/i,
  /\bendorse\b/i,
  /\branked\b/i,
  /\branking\b/i,
  /top operator/i,
  /rank(?:s|ed|ing)?\s+operators/i,
  /select(?:s|ed|ing)?\s+(?:the\s+)?(?:best|top)\s+operator/i,
  /shortlist of operators/i,
  /best-fit operator/i,
  /operating model conflict/i,
  /recommended pathway/i,
  /primary stated pathway/i,
  /is the preferred path/i,
  /is the best path/i,
  /must appoint\b/i,
  /\bnon-negotiable\b/i,
];

const TRANSITION_MESSAGE_MAP = {
  "Current model is owner-operated but preferred future targets third-party management":
    "Operating model transition to validate: Current model is owner-operated while the preferred future model is third-party management.",
  "Current model is third-party managed but preferred future is owner-operated":
    "Operating model transition to validate: Current model is third-party managed while the preferred future model is owner-operated.",
  "Plan to Self-Manage is Owner-Operated but Preferred Future Operating Model expects third-party":
    "Operating model transition to validate: Plan to Self-Manage is owner-operated while the preferred future model expects third-party management.",
  "Plan to Self-Manage is Third-party Managed but Preferred Future Operating Model is owner-operated":
    "Operating model transition to validate: Plan to Self-Manage is third-party managed while the preferred future model is owner-operated.",
};

/** Shown once in Operating model transition section — not repeated in gaps or status. */

const KIND_THEME_FALLBACK = {
  new_build:
    "pre-opening planning, brand standards coordination, operating budgets, staffing ramp-up, commercial launch, systems setup, and local permitting interfaces",
  conversion_reflag:
    "PIP execution, brand standards alignment, operator transition, owner reporting, and local execution",
  renovation_repositioning:
    "capex coordination, commercial repositioning, operating-while-renovating risk, and owner reporting",
  existing_operating: "full management, owner reporting, and revenue management",
  adaptive_reuse: "development complexity, conversion execution, and pre-opening ramp",
  mixed_use: "governance, amenity complexity, and multi-stakeholder coordination",
  other_tbc: "operating capabilities (pending confirmed project type)",
};

/** @param {string} text */
export function stripDisclaimerForCopyAudit(text) {
  return String(text || "").replace(
    /does not recommend,\s*rank,\s*endorse,\s*or select operators/gi,
    ""
  );
}

/** @param {string} text */
export function containsForbiddenOperatorLanguage(text) {
  return FORBIDDEN_PHRASES.some((re) => re.test(stripDisclaimerForCopyAudit(text)));
}

/**
 * Audit narrative fields on a built snapshot (excludes disclaimer allowlist phrase).
 * @param {Record<string, unknown>} snap
 */
export function auditOperatorCapabilitySnapshotCopy(snap) {
  const parts = [
    snap.executiveSummary,
    snap.ownerAdvisorReviewTakeaway,
    snap.whyOperatorStrategyMatters,
    snap.capabilityImplications,
    snap.decisionPointsBeforeOutreach,
    snap.diligenceQuestions,
    snap.knownGapsClarifications,
    snap.brandManagedGuidance,
    snap.operatingModelTransitionsToValidate,
    snap.operatingModelTransitionSummary,
    snap.reviewContext,
    snap.operatingPathways,
    snap.capabilityAreas,
    snap.newBuildGuidance,
  ];
  return containsForbiddenOperatorLanguage(JSON.stringify(parts));
}

/** @param {string} preferred */
export function isBrandManagedPreferred(preferred) {
  return /brand-managed/i.test(strVal(preferred));
}

/** @param {ReturnType<typeof import('./operator-capability-rules.js').buildOperatingContext>} ctx */
function operatingModelsDiffer(ctx) {
  const cur = strVal(ctx.currentOperatingModel);
  const pref = strVal(ctx.preferredFutureOperatingModel);
  if (!cur || cur === "—" || !pref || pref === "—") return false;
  if (/undecided|exploring|needs review/i.test(pref)) return false;
  return cur !== pref;
}

/**
 * @param {ReturnType<typeof import('./operator-capability-rules.js').buildOperatingContext>} ctx
 * @param {string[]} [rawConflicts]
 */
export function buildOperatingModelTransitionSummary(ctx, rawConflicts = []) {
  if (!operatingModelsDiffer(ctx) && !(rawConflicts || []).length) return "";
  const cur = sentenceCaseModelForSnapshotCopy(ctx.currentOperatingModel) || "—";
  const pref = sentenceCaseModelForSnapshotCopy(ctx.preferredFutureOperatingModel) || "—";
  return polishNarrativeText(
    `Current model: ${cur}. Preferred future model: ${pref}. ` +
      "This may be appropriate for the deal, but timing, approval requirements, economics, reporting package, and handover responsibilities should be validated before outreach."
  );
}

/** @param {string[]} rawConflicts @param {ReturnType<typeof import('./operator-capability-rules.js').buildOperatingContext>} ctx */
export function buildOperatingModelTransitions(rawConflicts, ctx) {
  const summary = buildOperatingModelTransitionSummary(ctx, rawConflicts);
  return summary ? [summary] : [];
}

/** @param {string[]} reasons */
export function neutralizeSnapshotAccessReasons(reasons) {
  return (reasons || []).map((r) => {
    let out = String(r);
    out = out.replace(/Operating model conflicts:/gi, "Operating model transitions to validate:");
    for (const [raw, neutral] of Object.entries(TRANSITION_MESSAGE_MAP)) {
      if (out.includes(raw)) {
        out = out.replace(raw, neutral.replace(/^Operating model transition to validate: /, ""));
      }
    }
    return out;
  });
}

/** @param {string} model */
function formatModelForSnapshotCopy(model) {
  let m = strVal(model);
  if (!m || m === "—") return m;
  return m
    .replace(/\s*\(unbranded\)/gi, "/unbranded")
    .replace(/\s*\(branded\)/gi, "/branded")
    .replace(/\s+only$/i, "")
    .trim();
}

/** Lowercase model labels when embedded in narrative sentences (tables keep raw field values). */
function sentenceCaseModelForSnapshotCopy(model) {
  const m = formatModelForSnapshotCopy(model);
  if (!m || m === "—") return m;
  return m.toLowerCase();
}

const HYPHENATED_TERMS_NB = [
  ["third-party", "third\u2011party"],
  ["pre-opening", "pre\u2011opening"],
  ["owner-operated", "owner\u2011operated"],
  ["brand-managed", "brand\u2011managed"],
];

/** Reduce awkward line breaks on common hospitality compounds in narrative prose. */
export function polishNarrativeText(text) {
  let s = String(text || "");
  for (const [plain, nb] of HYPHENATED_TERMS_NB) {
    s = s.replace(new RegExp(plain.replace(/-/g, "\\-"), "gi"), (match) => {
      if (match === match.toUpperCase()) return nb.toUpperCase();
      if (match[0] === match[0].toUpperCase()) {
        return nb.charAt(0).toUpperCase() + nb.slice(1);
      }
      return nb;
    });
  }
  return s;
}

/** @param {string[]} lines */
function polishNarrativeLines(lines) {
  return (lines || []).map((line) => polishNarrativeText(line));
}

/** @param {string[]} clarifications */
export function normalizeClarificationsForDisplay(clarifications) {
  return (clarifications || [])
    .map((item) => {
      let c = String(item);
      if (/Review operating model consistency:/i.test(c)) {
        return null;
      }
      if (/out of scope for this deal/i.test(c)) {
        return polishNarrativeText(
          "third-party operator review may be limited per current inputs; pre-opening and pathway accountability may still merit validation."
        );
      }
      if (isOperatingModelTransitionDuplicate(c) || /Operating model transition(s)? to validate/i.test(c)) {
        return null;
      }
      return c;
    })
    .filter(Boolean);
}

const PRIORITY_EXECUTIVE_LABEL = {
  "Full hotel management": "full hotel management",
  "Pre-opening / opening support": "pre-opening support",
  "Conversion & PIP execution": "conversion execution",
  "Revenue management & distribution": "revenue management",
  "Accounting & owner reporting": "owner reporting",
  "Procurement & cost control": "procurement and cost control",
  "F&B / culinary operations": "F&B operations",
  "Sales & marketing": "sales and marketing",
  "HR & training": "HR and training",
  "Technology & systems": "technology and systems",
  "Design / renovation PM": "development coordination",
  "Asset management / capex planning": "asset management",
  "Local market / CALA execution": "local execution",
  "Lifestyle / experience programming": "experience programming",
  "Crisis / business continuity": "business continuity",
};

const AREA_EXECUTIVE_LABEL = {
  pre_opening: "pre-opening support",
  revenue_distribution: "revenue management",
  owner_reporting: "owner reporting",
  development_complexity: "development coordination",
  cala_local: "local execution",
  brand_standards: "brand standards coordination",
  operator_transition: "operator transition",
  conversion: "conversion execution",
};

/** @param {Record<string, unknown>} fields @param {{ id: string }[]} areas */
function compactThemesForExecutiveSummary(fields, areas) {
  const priorities = listVal(fields[SI_FIELDS.operatorCapabilityPriorities]);
  const fromPriorities = priorities
    .map((p) => PRIORITY_EXECUTIVE_LABEL[p] || String(p).split("/")[0].trim().toLowerCase())
    .filter(Boolean);
  if (fromPriorities.length >= 2) {
    return [...new Set(fromPriorities)].slice(0, 5).join(", ");
  }
  const fromAreas = (areas || [])
    .map((a) => AREA_EXECUTIVE_LABEL[a.id] || "")
    .filter(Boolean);
  if (fromAreas.length >= 2) {
    return [...new Set(fromAreas)].slice(0, 5).join(", ");
  }
  const fallback = (KIND_THEME_FALLBACK.new_build || "").split(", ").slice(0, 5).join(", ");
  return fallback || "operating capability themes";
}

/** @param {string} text */
function isOperatingModelTransitionDuplicate(text) {
  return (
    /transition to validate|Operating model transitions to validate/i.test(String(text || "")) ||
    /Current model is owner-operated while the preferred future model is third-party/i.test(String(text || "")) ||
    /appropriate for the deal|Current model:|Preferred future model:|contemplated move|timing, approval requirements, economics, reporting package, and handover/i.test(
      String(text || "")
    )
  );
}

const CAPABILITY_NARRATIVE = {
  pre_opening: {
    relevance: "May be relevant for ground-up and transition timelines",
    whyItMayMatter:
      "Pre-opening planning may coordinate budgets, staffing ramp-up, systems cutover, and brand opening standards before the hotel accepts guests.",
    whatToValidate:
      "Who owns the pre-opening plan today, what budget exists, and when an operating party may be appointed.",
  },
  design_renovation: {
    relevance: "May be relevant during design and construction",
    whyItMayMatter:
      "Design and renovation PM may keep owner, brand, architect, and contractor aligned on standards, FF&E, and permitting assumptions.",
    whatToValidate:
      "Whether brand standards are reflected in current design packages and who approves changes during entitlement or construction.",
  },
  development_complexity: {
    relevance: "May be relevant when entitlement or construction risk is material",
    whyItMayMatter:
      "Development complexity and permitting interfaces may affect opening date, cost exposure, and what operating partners can commit to pre-opening.",
    whatToValidate:
      "Permitting status, critical path risks, and which party interfaces with authorities and brand technical teams.",
  },
  cala_local: {
    relevance: "May be elevated for CALA or cross-border execution",
    whyItMayMatter:
      "Local market execution may cover labor norms, distribution, owner–brand–operator governance, and on-the-ground commercial ramp.",
    whatToValidate:
      "Which local expertise may be required (brand, owner, third-party manager, or advisory) and how in-market decisions are made.",
  },
  brand_standards: {
    relevance: "May be relevant for branded conversions and new flags",
    whyItMayMatter:
      "Brand standards alignment may affect PIP scope, design approvals, and operating cost assumptions before management structure is final.",
    whatToValidate:
      "Whether a brand PIP or standards package exists and who signs off on deviations.",
  },
  full_management: {
    relevance: "May be relevant when third-party management is in scope",
    whyItMayMatter:
      "Full hotel management may shape economics, staffing, reporting, and owner governance for the stabilized asset.",
    whatToValidate:
      "Whether management is brand-direct, third-party, or owner-led—and what the brand may approve.",
  },
  conversion_pip: {
    relevance: "May be relevant for conversion / reflag paths",
    whyItMayMatter:
      "Conversion and PIP execution may sequence brand change, downtime, and operator transition.",
    whatToValidate:
      "PIP timing, funding, and who manages contractor and brand technical reviews.",
  },
  operator_transition: {
    relevance: "May be relevant when management model may change",
    whyItMayMatter:
      "Operator transition and handover may affect guest experience, staff continuity, and owner reporting during changeover.",
    whatToValidate:
      "Transition timeline, labor considerations, and data/systems handoff responsibilities.",
  },
  revenue_distribution: {
    relevance: "May be relevant for commercial performance",
    whyItMayMatter:
      "Revenue management and distribution may influence ramp-up and stabilized RevPAR performance.",
    whatToValidate:
      "Who may own revenue strategy pre-opening vs at stabilization and which systems the brand may require.",
  },
  accounting_reporting: {
    relevance: "May be relevant for owner oversight and lender reporting",
    whyItMayMatter:
      "Accounting and owner reporting packages may define transparency, covenant compliance, and decision rights.",
    whatToValidate:
      "Reporting frequency, package contents, and whether contemplated models meet lender requirements.",
  },
  commercial_repositioning: {
    relevance: "May be relevant for repositioning and renovation",
    whyItMayMatter:
      "Commercial repositioning may align product, pricing, and channel strategy with the new brand or segment promise.",
    whatToValidate:
      "Repositioning thesis, market comp set, and launch commercial resources before reopening.",
  },
  sales_marketing: {
    relevance: "May be relevant for new-build ramp and demand generation",
    whyItMayMatter:
      "Sales and marketing resources may shape pre-opening awareness, group pipeline, and early occupancy performance.",
    whatToValidate:
      "Who may lead commercial launch (owner, brand, third party, or hybrid) and which channels may be funded pre-opening.",
  },
  hr_training: {
    relevance: "May be relevant before opening and during ramp-up",
    whyItMayMatter:
      "HR and training may govern leadership hiring, hourly staffing curves, labor compliance, and brand training windows.",
    whatToValidate:
      "Whether hiring timelines align with construction completion and who may own payroll and training cutover.",
  },
  governance: {
    relevance: "May be relevant for mixed-use and multi-stakeholder deals",
    whyItMayMatter:
      "Governance may clarify decision rights across owner, brand, operator, and other uses within the project.",
    whatToValidate:
      "Decision committees, service-level expectations, and dispute resolution between parties.",
  },
};

const NEW_BUILD_THEMES = [
  {
    title: "Pre-opening planning",
    detail:
      "Document how owner, brand, and (if applicable) third-party roles may align on budgets, timelines, and opening milestones before keys-in-hand.",
  },
  {
    title: "Brand standards coordination",
    detail:
      "Clarify whether design, FF&E, and operating standards are reflected in permitting and construction assumptions—not only at soft opening.",
  },
  {
    title: "Operating budget development",
    detail:
      "Review whether stabilized and ramp-up operating budgets are available to test management economics and lender covenants.",
  },
  {
    title: "Staffing and training ramp-up",
    detail:
      "Clarify leadership hiring, hourly staffing curves, and brand training windows relative to construction completion.",
  },
  {
    title: "Commercial launch strategy",
    detail:
      "Identify distribution, sales, and marketing resources that may apply pre-opening and in the first 90 days of operation.",
  },
  {
    title: "Systems setup",
    detail:
      "Confirm how PMS, POS, HR/payroll, and reporting stack may align with brand mandates and owner reporting needs.",
  },
  {
    title: "Local execution / permitting interface",
    detail:
      "Clarify who may own authority relationships, inspections, and in-market vendor selection during development.",
  },
  {
    title: "Owner decision timing",
    detail:
      "Document decision gates for management structure, pre-opening advisor appointment, and brand vs third-party confirmation.",
  },
];

/**
 * @param {{ id: string, label: string, sources?: string[], strength?: string }[]} capabilityAreas
 * @param {ReturnType<typeof import('./operator-capability-rules.js').buildOperatingContext>} ctx
 * @param {import('./project-type.js').ProjectTypeKind} kind
 */
export function enrichCapabilityAreas(capabilityAreas, ctx, kind) {
  return (capabilityAreas || []).map((area) => {
    const base = CAPABILITY_NARRATIVE[area.id] || {
      relevance: "May merit review for this deal context",
      whyItMayMatter: `${area.label} may influence how operating support is structured before outreach.`,
      whatToValidate: `Confirm scope, timing, and accountable party for ${area.label.toLowerCase()}.`,
    };
    const source = (area.sources && area.sources[0]) || "Deal inputs";
    const ruleTrigger = source.startsWith("Project Type")
      ? `project_type_kind:${kind}`
      : source === "Operator Capability Priorities"
        ? "stated_priorities"
        : source === "Deal context inference"
          ? "generic_context_blob"
          : source;
    const strengthLabel =
      area.strength === "stated" ? "stated" : area.strength === "needs_validation" ? "needs validation" : "inferred";
    return {
      ...area,
      relevance: base.relevance,
      whyItMayMatter: base.whyItMayMatter,
      whyItMatters: base.whyItMayMatter,
      whatToValidate: base.whatToValidate,
      ruleTrigger,
      sourceLabel: source,
      strengthLabel,
    };
  });
}

/**
 * @param {ReturnType<typeof import('./operator-capability-rules.js').buildOperatingContext>} ctx
 * @param {import('./project-type.js').ProjectTypeKind} kind
 * @param {Record<string, unknown>} fields
 */
export function buildWhyOperatorStrategyMatters(ctx, kind, fields) {
  const lines = [
    "Operating structure can affect capex timing, brand approval, pre-opening spend, reporting cadence, and long-term owner oversight.",
  ];
  if (kind === "new_build") {
    lines.push(
      "For a New Build, development, brand standards, opening ramp-up, and management-path decisions often need to be aligned before outreach."
    );
  } else if (kind === "conversion_reflag") {
    lines.push(
      "Conversion and reflag paths often require coordination across PIP execution, standards implementation, transition timing, and reporting cutover—themes for review, not a specific operating model."
    );
  } else if (kind === "renovation_repositioning") {
    lines.push(
      "Renovation and repositioning may require coordination across capex, commercial strategy, and operating-while-renovating considerations."
    );
  }
  return lines.slice(0, 2);
}

/**
 * @param {ReturnType<typeof import('./operator-capability-rules.js').buildOperatingContext>} ctx
 * @param {import('./project-type.js').ProjectTypeKind} kind
 */
export function buildOperatingPathways(ctx, kind) {
  const brandManaged = isBrandManagedPreferred(ctx.preferredFutureOperatingModel);
  const thirdPartyInScope = ctx.operatorInScope;
  const preferred = strVal(ctx.preferredFutureOperatingModel);
  const undecidedPreferred = /undecided|exploring/i.test(preferred);

  const pathways = [];

  pathways.push({
    id: "brand_managed",
    label: "Brand-managed model",
    relevance: brandManaged
      ? "Listed in current inputs — pathway to validate"
      : "Market-dependent — pathway to validate",
    whyItMayMatter:
      "Direct brand management may affect economics, availability, and owner reporting versus third-party options.",
    validationQuestion: "Would the target brand manage the hotel directly, and on what fee and reporting terms?",
  });

  pathways.push({
    id: "third_party",
    label: "Third-party manager",
    relevance: thirdPartyInScope
      ? "In scope per current inputs — pathway to validate"
      : "Pathway to validate if brand approves third-party management",
    whyItMayMatter:
      "A third-party manager may provide management, transition support, and reporting, subject to brand approval and commercial terms.",
    validationQuestion:
      "Would the target brand approve a third-party manager, and what owner reporting and fee structure would apply?",
  });

  pathways.push({
    id: "owner_operated",
    label: "Owner-operated with upgraded support",
    relevance: undecidedPreferred
      ? "Pathway to validate"
      : /owner-operated/i.test(preferred)
        ? "Listed in current inputs — pathway to validate"
        : "Pathway to validate",
    whyItMayMatter:
      "Owner-operated structures may retain control while sourcing selective revenue, accounting, or pre-opening support.",
    validationQuestion: "Which capabilities would remain in-house versus sourced through brand or advisory support?",
  });

  pathways.push({
    id: "pre_opening_advisory",
    label: "Pre-opening / transition advisory support",
    relevance:
      kind === "new_build" || kind === "conversion_reflag" || /planning|construction|pre-opening|rebrand/i.test(ctx.openingTransitionPhase)
        ? "Often relevant before appointment — pathway to validate"
        : "Situational — pathway to validate",
    whyItMayMatter:
      "Advisory support may bridge design, permitting, staffing ramp-up, and systems before a long-term manager is confirmed.",
    validationQuestion: "Who owns pre-opening planning today, and when must an operating party be appointed?",
  });

  return pathways.map((p) => ({
    ...p,
    whyItMatters: p.whyItMayMatter,
  }));
}

/**
 * @param {ReturnType<typeof enrichCapabilityAreas>} enrichedAreas
 * @param {ReturnType<typeof import('./operator-capability-rules.js').buildOperatingContext>} ctx
 */
export function buildCapabilityImplications(enrichedAreas, ctx) {
  if (!enrichedAreas.length) {
    return [
      "No capability themes surfaced from current inputs; confirm project type and operating inputs before outreach.",
    ];
  }
  const lines = [
    `${enrichedAreas.length} capability area${enrichedAreas.length === 1 ? "" : "s"} may merit review from current inputs—not an operator or operating-model recommendation.`,
  ];
  const ids = new Set(enrichedAreas.map((a) => a.id));
  if (ids.has("pre_opening") && ids.has("development_complexity")) {
    lines.push(
      "Pre-opening and development complexity may be linked to permitting, design assumptions, and feasible opening timing."
    );
  }
  return lines.slice(0, 3);
}

/**
 * @param {ReturnType<typeof import('./operator-capability-rules.js').buildOperatingContext>} ctx
 * @param {import('./project-type.js').ProjectTypeKind} kind
 * @param {string[]} clarifications
 * @param {string[]} missing
 */
export function buildDecisionPointsBeforeOutreach(ctx, kind, clarifications, missing) {
  const points = [];
  if (isBrandManagedPreferred(ctx.preferredFutureOperatingModel)) {
    points.push(
      "Review consideration: validate brand direct-management availability and economics for this market."
    );
    points.push(
      "Review consideration: document criteria if brand management is unavailable or terms are not acceptable."
    );
  }
  if (!ctx.operatorInScope) {
    points.push(
      "Review consideration: clarify whether third-party operator dialogue is permitted before brand commitments."
    );
  } else {
    points.push(
      "Review consideration: align bid audience and management pathway inputs before issuing operator outreach."
    );
  }
  if (kind === "new_build") {
    points.push(
      "Review consideration: document decision gates for pre-opening appointment vs construction milestones."
    );
    points.push(
      "Review consideration: validate whether brand standards are reflected in current design and permitting packages."
    );
  }
  if (kind === "other_tbc") {
    points.push("Review consideration: confirm project type before using capability themes externally.");
  }
  if (!operatingModelsDiffer(ctx)) {
    for (const m of missing.slice(0, 2)) {
      points.push(`Review consideration: confirm ${m}.`);
    }
    for (const c of normalizeClarificationsForDisplay(clarifications).slice(0, 2)) {
      if (/out of scope/i.test(c) || isOperatingModelTransitionDuplicate(c)) continue;
      points.push(`Review consideration: ${c.replace(/\.$/, "")}.`);
    }
  }
  return [...new Set(points)].filter((p) => !isOperatingModelTransitionDuplicate(p)).slice(0, 6);
}

/** @param {import('./project-type.js').ProjectTypeKind} kind */
export function buildNewBuildGuidance(kind) {
  if (kind !== "new_build") return [];
  return NEW_BUILD_THEMES;
}

/**
 * @param {ReturnType<typeof import('./operator-capability-rules.js').buildOperatingContext>} ctx
 * @param {string[]} clarifications
 * @param {string[]} missing
 * @param {string[]} transitions
 */
export function buildKnownGapsClarifications(ctx, clarifications, missing) {
  const gaps = [];
  if (operatingModelsDiffer(ctx)) {
    gaps.push(
      "Confirm timing, approval requirements, economics, reporting package, and handover responsibilities for the contemplated operating model transition."
    );
  }
  for (const c of normalizeClarificationsForDisplay(clarifications)) {
    if (isOperatingModelTransitionDuplicate(c)) continue;
    if (/out of scope/i.test(c)) {
      gaps.push(
        polishNarrativeText(
          "third-party operator review may be limited per current inputs; pre-opening and pathway accountability may still merit validation."
        )
      );
      continue;
    }
    if (!gaps.includes(c)) gaps.push(c);
  }
  for (const m of missing || []) {
    gaps.push(`Confirm ${m}.`);
  }
  return [...new Set(gaps)].slice(0, 4);
}

/**
 * @param {ReturnType<typeof import('./operator-capability-rules.js').buildOperatingContext>} ctx
 * @param {import('./project-type.js').ProjectTypeKind} kind
 * @param {{ id: string, label: string }[]} capabilityAreas
 * @param {string[]} transitions
 */
export function buildOwnerAdvisorReviewTakeaway(ctx, kind) {
  if (kind === "conversion_reflag" || operatingModelsDiffer(ctx)) {
    return [
      "This may not be only a brand-selection question. Current inputs suggest that operating model, pre-opening planning, reporting, and local execution should be clarified before outreach so the owner/advisor can evaluate available pathways with better context.",
      "This snapshot is for owner/advisor review preparation only—not operator matching, scoring, or prescriptive operating-model advice.",
    ];
  }
  return [
    "Current inputs suggest operating capability themes may merit review before brand or operator outreach so pathways and diligence items can be evaluated with better context.",
    "This snapshot is for owner/advisor review preparation only—not operator matching, scoring, or prescriptive operating-model advice.",
  ];
}

/**
 * @param {ReturnType<typeof import('./operator-capability-rules.js').buildOperatingContext>} ctx
 * @param {import('./project-type.js').ProjectTypeKind} kind
 * @param {Record<string, unknown>} fields
 * @param {string[]} clarifications
 * @param {string[]} missing
 * @param {boolean} requiresManualReview
 * @param {{ id: string }[]} capabilityAreas
 */
export function buildExpandedDiligenceQuestions(
  ctx,
  kind,
  fields,
  clarifications,
  missing,
  requiresManualReview,
  capabilityAreas
) {
  const questions = [];
  const preferred = ctx.preferredFutureOperatingModel;
  const brandManaged = isBrandManagedPreferred(preferred);
  const region = ctx.primaryMarketRegion;
  const opening = ctx.openingTransitionPhase;

  if (requiresManualReview) {
    questions.push(
      "Are Current Operating Model and Opening / Transition Phase records confirmed before external outreach?"
    );
  }

  if (brandManaged) {
    questions.push("Would the target brand manage the hotel directly?");
    questions.push(
      "If brand management is unavailable, would the brand approve a third-party manager for this asset?"
    );
    questions.push(
      "What economics, reporting package, and term structure may apply under direct brand management?"
    );
  }

  if (kind === "new_build") {
    questions.push("Who owns pre-opening planning before a long-term management structure is confirmed?");
    questions.push("Has an opening budget and staffing ramp-up plan been developed?");
    questions.push(
      "Are brand standards reflected in design, FF&E, and permitting assumptions?"
    );
    questions.push(
      "What commercial launch resources (sales, marketing, distribution) may be available before opening?"
    );
    if (/planning|entitlement|pre-construction/i.test(opening)) {
      questions.push(
        "What is the critical path from entitlement to pre-opening ramp, and which party owns permitting interfaces?"
      );
    }
  }

  if (kind === "conversion_reflag" || kind === "renovation_repositioning") {
    questions.push("What is the expected opening or transition phase timing and downtime risk?");
    questions.push("Who manages PIP execution and brand technical approvals?");
    questions.push("How may systems, staffing, and reporting be transitioned?");
  }

  if (region === "CALA" || /cala/i.test(strVal(fields[LOCATION_FIELDS.primaryMarketRegion]))) {
    questions.push("What local market expertise may be needed for CALA execution (labor, vendors, distribution)?");
  } else if (region && region !== "—") {
    questions.push(`What in-market operating expertise may be required for ${region}?`);
  }

  if (ctx.operatorInScope) {
    questions.push("Which operator capability priorities are identified in inputs as most important for owner governance?");
    if (/third.party|brand \+ third/i.test(preferred)) {
      questions.push("What owner reporting frequency and package are expected from a third-party manager?");
    }
  }

  for (const m of missing) {
    questions.push(`What is the confirmed ${m}?`);
  }

  if (!ctx.operatorInScope && /undecided|exploring/i.test(preferred)) {
    questions.push(
      "What future operating model is being explored (owner-operated, brand-managed, or third-party management)?"
    );
  }

  for (const c of normalizeClarificationsForDisplay(clarifications)) {
    if (/transition to validate/i.test(c)) continue;
    if (/confirm project type/i.test(c)) {
      questions.push("What is the confirmed Project Type for this opportunity?");
    } else if (/opening|transition/i.test(c) && !questions.some((q) => /opening/i.test(q))) {
      questions.push("What is the expected opening or transition phase and timing?");
    } else if (/reporting frequency/i.test(c)) {
      questions.push("What owner reporting frequency is expected from the operating party?");
    } else if (/capability priorities/i.test(c)) {
      questions.push("Which operator capability priorities matter most for review preparation?");
    } else if (/market region/i.test(c)) {
      questions.push("What is the primary market region for operating execution?");
    }
  }

  const priorities = listVal(fields[SI_FIELDS.operatorCapabilityPriorities]);
  if (priorities.length && !questions.some((q) => /priorities/i.test(q))) {
    questions.push(
      `How may stated priorities (${priorities.slice(0, 3).join(", ")}) be staffed and measured pre-opening and at stabilization?`
    );
  }

  if (capabilityAreas.some((a) => a.id === "development_complexity") && !questions.some((q) => /permitting/i.test(q))) {
    questions.push("Which party interfaces with authorities and brand technical teams on permitting and inspections?");
  }

  const deduped = [...new Set(questions.map((q) => q.trim()).filter(Boolean))];
  if (deduped.length < 5 && kind === "new_build") {
    deduped.push("What systems (PMS, POS, HR) may need to be live before soft opening, and who implements them?");
  }
  return deduped.slice(0, 8);
}

/**
 * @param {ReturnType<typeof import('./operator-capability-rules.js').buildOperatingContext>} ctx
 * @param {import('./project-type.js').ProjectTypeKind} kind
 * @param {{ id: string, label: string }[]} capabilityAreas
 * @param {string[]} clarifications
 * @param {string} status
 */
export function buildExecutiveSummaryNarrative(ctx, kind, fields, capabilityAreas, status) {
  if (status === "blocked") {
    return [
      "Core inputs may be insufficient for reliable capability themes; confirm missing fields before external use of this draft.",
    ];
  }
  const region = ctx.primaryMarketRegion !== "—" ? ctx.primaryMarketRegion : "the stated market";
  const pt = ctx.projectType !== "—" ? ctx.projectType : "hotel";
  const themes = compactThemesForExecutiveSummary(fields, capabilityAreas);

  let line = `This ${pt} opportunity in ${region}`;
  if (operatingModelsDiffer(ctx)) {
    const cur = sentenceCaseModelForSnapshotCopy(ctx.currentOperatingModel);
    const pref = sentenceCaseModelForSnapshotCopy(ctx.preferredFutureOperatingModel);
    line += ` involves a potential shift from ${cur} to ${pref}`;
  }
  line += `. Based on current inputs, the snapshot highlights operating capabilities that may need review before brand or operator outreach, including ${themes}.`;
  return [polishNarrativeText(line)];
}

/** Neutral explainability line for PDF/UI (no rule-debug strings). */
export function buildCapabilitySignalsLine() {
  return "Signals used: stated operator capability priorities, project type, opening/transition phase, primary market region, and current/future operating model inputs.";
}

/**
 * Full narrative bundle for snapshot builder.
 * @param {object} params
 */
export function buildOperatorCapabilityNarrative(params) {
  const {
    fields,
    operatingContext: ctx,
    projectTypeKind: kind,
    capabilityAreas,
    clarifications,
    missingInputs,
    requiresManualReview,
    snapshotStatus,
    operatingModelConflicts = [],
  } = params;

  const operatingModelTransitionsToValidate = buildOperatingModelTransitions(
    operatingModelConflicts,
    ctx
  );
  const enrichedCapabilityAreas = enrichCapabilityAreas(capabilityAreas, ctx, kind);
  const operatingPathways = buildOperatingPathways(ctx, kind);
  const whyOperatorStrategyMatters = buildWhyOperatorStrategyMatters(ctx, kind, fields);
  const capabilityImplications = buildCapabilityImplications(enrichedCapabilityAreas, ctx);
  const decisionPointsBeforeOutreach = buildDecisionPointsBeforeOutreach(
    ctx,
    kind,
    clarifications,
    missingInputs || []
  );
  const diligenceQuestions = buildExpandedDiligenceQuestions(
    ctx,
    kind,
    fields,
    clarifications,
    missingInputs || [],
    requiresManualReview,
    capabilityAreas
  );
  const operatingModelTransitionSummary = buildOperatingModelTransitionSummary(
    ctx,
    operatingModelConflicts
  );
  const knownGapsClarifications = buildKnownGapsClarifications(ctx, clarifications, missingInputs || []);
  const newBuildGuidance = buildNewBuildGuidance(kind);
  const executiveSummary = buildExecutiveSummaryNarrative(
    ctx,
    kind,
    fields,
    capabilityAreas,
    snapshotStatus
  );
  const ownerAdvisorReviewTakeaway = buildOwnerAdvisorReviewTakeaway(ctx, kind);

  const brandManagedGuidance = [];

  const displayClarifications = normalizeClarificationsForDisplay(clarifications);

  const allText = [
    ...executiveSummary,
    ...ownerAdvisorReviewTakeaway,
    ...whyOperatorStrategyMatters,
    ...capabilityImplications,
    ...decisionPointsBeforeOutreach,
    ...diligenceQuestions,
    ...knownGapsClarifications,
    ...brandManagedGuidance,
    ...operatingModelTransitionsToValidate,
    ...enrichedCapabilityAreas.flatMap((a) => [a.whyItMayMatter, a.whatToValidate, a.relevance]),
    ...operatingPathways.flatMap((p) => [p.whyItMayMatter, p.validationQuestion]),
  ].join("\n");

  if (containsForbiddenOperatorLanguage(allText)) {
    throw new Error("Operator capability narrative contains forbidden operator recommendation language");
  }

  const capabilityAreasPolished = enrichedCapabilityAreas.map((area) => ({
    ...area,
    relevance: polishNarrativeText(area.relevance),
    whyItMayMatter: polishNarrativeText(area.whyItMayMatter),
    whyItMatters: polishNarrativeText(area.whyItMatters || area.whyItMayMatter),
    whatToValidate: polishNarrativeText(area.whatToValidate),
  }));
  const operatingPathwaysPolished = operatingPathways.map((p) => ({
    ...p,
    whyItMayMatter: polishNarrativeText(p.whyItMayMatter),
    whyItMatters: polishNarrativeText(p.whyItMatters),
    validationQuestion: polishNarrativeText(p.validationQuestion),
  }));
  const newBuildGuidancePolished = newBuildGuidance.map((row) => ({
    ...row,
    detail: polishNarrativeText(row.detail),
  }));

  return {
    executiveSummary: polishNarrativeLines(executiveSummary),
    ownerAdvisorReviewTakeaway: polishNarrativeLines(ownerAdvisorReviewTakeaway),
    whyOperatorStrategyMatters: polishNarrativeLines(whyOperatorStrategyMatters),
    operatingPathways: operatingPathwaysPolished,
    capabilityImplications: polishNarrativeLines(capabilityImplications),
    decisionPointsBeforeOutreach: polishNarrativeLines(decisionPointsBeforeOutreach),
    diligenceQuestions: polishNarrativeLines(diligenceQuestions),
    knownGapsClarifications: polishNarrativeLines(knownGapsClarifications),
    displayClarifications,
    newBuildGuidance: newBuildGuidancePolished,
    brandManagedGuidance,
    operatingModelTransitionsToValidate: polishNarrativeLines(operatingModelTransitionsToValidate),
    operatingModelTransitionSummary,
    capabilityAreas: capabilityAreasPolished,
  };
}
