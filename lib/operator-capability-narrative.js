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

const TRANSITION_SUPPORTING =
  "The current and preferred future operating models differ. This may be appropriate for the deal, but the transition should be validated before outreach or external sharing.";

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

/** @param {string[]} rawConflicts @param {ReturnType<typeof import('./operator-capability-rules.js').buildOperatingContext>} ctx */
export function buildOperatingModelTransitions(rawConflicts, ctx) {
  const lines = (rawConflicts || []).map(
    (c) => TRANSITION_MESSAGE_MAP[c] || `Operating model transition to validate: ${c}`
  );
  if (lines.length || operatingModelsDiffer(ctx)) {
    if (!lines.includes(TRANSITION_SUPPORTING)) {
      lines.push(TRANSITION_SUPPORTING);
    }
  }
  return [...new Set(lines)];
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

/** @param {string[]} clarifications */
export function normalizeClarificationsForDisplay(clarifications) {
  return (clarifications || []).map((item) => {
    let c = String(item);
    if (/Review operating model consistency:/i.test(c)) {
      const detail = c.replace(/^Review operating model consistency:\s*/i, "").trim();
      const mapped = TRANSITION_MESSAGE_MAP[detail];
      return mapped || `Operating model transition to validate: ${detail}`;
    }
    if (/out of scope for this deal/i.test(c)) {
      return (
        "Third-party operator capability review may be limited for this deal based on bid audience and operating model inputs. " +
        "Operating pathways and pre-opening accountability may still merit validation before external sharing."
      );
    }
    return c;
  });
}

/** @param {import('./project-type.js').ProjectTypeKind} kind @param {{ label: string }[]} areas */
function themeListForSummary(kind, areas) {
  const labels = (areas || []).map((a) => a.label).filter(Boolean);
  if (labels.length) {
    const shown = labels.slice(0, 6);
    const tail = labels.length > 6 ? ", and related themes" : "";
    return shown.join(", ") + tail;
  }
  return KIND_THEME_FALLBACK[kind] || KIND_THEME_FALLBACK.other_tbc;
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
  const lines = [];
  lines.push(
    "Operating structure can affect capex timing, brand approval, transition planning, pre-opening spend, reporting cadence, and long-term owner oversight."
  );

  if (kind === "new_build") {
    lines.push(
      "For a new build, development, brand standards, and operating ramp-up may need alignment before a long-term management structure is confirmed."
    );
  } else if (kind === "conversion_reflag") {
    lines.push(
      "Conversion and reflag opportunities often require coordination across PIP execution, standards implementation, systems transition, staffing continuity, and reporting cutover. The snapshot identifies these as diligence themes, not as a recommendation of a specific operating model."
    );
  } else if (kind === "renovation_repositioning") {
    lines.push(
      "Renovation and repositioning may require coordination across capex, commercial strategy, and operating-while-renovating considerations."
    );
  } else if (kind === "existing_operating") {
    lines.push(
      "For an existing operating hotel, review may focus on management model fit, reporting transparency, and performance levers."
    );
  }

  if (isBrandManagedPreferred(ctx.preferredFutureOperatingModel)) {
    lines.push(
      "Brand-managed appears in current inputs as a future model to validate— including whether the brand may manage directly, on what economics, and what alternatives may exist if brand management is unavailable in this market."
    );
    if (!ctx.operatorInScope) {
      lines.push(
        "Third-party management may remain a pathway to validate; pre-opening and transition support may still be relevant before a final structure is confirmed."
      );
    }
  } else if (!ctx.operatorInScope) {
    lines.push(
      "Current bid audience and operating model inputs may limit third-party operator outreach; operating pathways and pre-opening accountability may still merit validation."
    );
  } else {
    lines.push(
      "This snapshot organizes capability and pathway themes for review before brand or operator conversations—not to select an operator or operating model."
    );
  }

  const stage = strVal(fields["Stage of Development"]);
  if (stage && stage !== "—") {
    lines.push(`Development stage (${stage}) may inform timing for pre-opening and governance validation.`);
  }
  return lines;
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
      : "Conditional / market-dependent — pathway to validate",
    whyItMayMatter:
      "Some brands manage directly in select markets; economics, availability, and owner reporting may differ from third-party management.",
    validationQuestion:
      "Would the target brand manage the hotel directly, and on what fee structure and reporting package?",
  });

  pathways.push({
    id: "third_party",
    label: "Third-party manager",
    relevance: thirdPartyInScope
      ? "In scope for structured review based on current inputs"
      : brandManaged
        ? "Pathway to validate if brand management is unavailable"
        : "Pathway to validate if brand approves third-party management",
    whyItMayMatter:
      "A third-party manager may provide full management, transition support, local execution, and reporting infrastructure, subject to brand approval and commercial terms.",
    validationQuestion:
      "Would the target brand approve a third-party manager for this asset, and what owner reporting and fee structure would apply?",
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
      "An owner-operated structure may retain control while sourcing selective support (revenue, accounting, pre-opening advisory).",
    validationQuestion:
      "Which capabilities may remain in-house vs be sourced through brand or advisory support?",
  });

  pathways.push({
    id: "pre_opening_advisory",
    label: "Pre-opening / transition advisory support",
    relevance:
      kind === "new_build" ||
      kind === "conversion_reflag" ||
      kind === "adaptive_reuse" ||
      /planning|construction|pre-opening|rebrand/i.test(ctx.openingTransitionPhase)
        ? "Often relevant before long-term management is confirmed"
        : "Situational — pathway to validate",
    whyItMayMatter:
      "Pre-opening advisors may bridge design, permitting, staffing ramp-up, and systems setup before a long-term manager is confirmed.",
    validationQuestion:
      "Who owns pre-opening planning today, and when may an operating party need to be appointed?",
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
  const lines = [];
  if (!enrichedAreas.length) {
    lines.push(
      "No capability themes surfaced from current inputs; project type and operating model inputs may need confirmation before outreach."
    );
    return lines;
  }
  lines.push(
    `${enrichedAreas.length} capability area${enrichedAreas.length === 1 ? "" : "s"} may merit structured review based on project type and deal context. This is not an operator or operating-model recommendation.`
  );
  const stated = enrichedAreas.filter((a) => a.strength === "stated");
  if (stated.length) {
    lines.push(
      `Stated priorities (${stated.map((a) => a.label).join(", ")}) may anchor diligence and pathway validation conversations.`
    );
  }
  if (isBrandManagedPreferred(ctx.preferredFutureOperatingModel) && !ctx.operatorInScope) {
    lines.push(
      "Where brand-managed appears in inputs, pre-opening, standards coordination, and local execution may still merit review until brand management availability is confirmed."
    );
  }
  const ids = new Set(enrichedAreas.map((a) => a.id));
  if (ids.has("pre_opening") && ids.has("development_complexity")) {
    lines.push(
      "Pre-opening and development complexity may be linked: permitting and design inputs may influence feasible opening timing and staffing ramp."
    );
  }
  if (ids.has("cala_local")) {
    lines.push(
      "Local market execution may be validated against who could operate in-market (brand, owner, third party, or hybrid)."
    );
  }
  return lines;
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
      "Review consideration: validate brand direct-management availability and economics for this market and product."
    );
    points.push(
      "Review consideration: document criteria if brand management is not offered or terms are not acceptable."
    );
  }
  if (!ctx.operatorInScope) {
    points.push(
      "Review consideration: clarify whether any third-party operator dialogue is permitted before brand commitments are made."
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
    points.push("Review consideration: confirm project type before relying on capability themes in external materials.");
  }
  for (const m of missing.slice(0, 4)) {
    points.push(`Review consideration: confirm ${m} to reduce ambiguous operating assumptions in outreach.`);
  }
  for (const c of normalizeClarificationsForDisplay(clarifications).slice(0, 3)) {
    if (!/out of scope/i.test(c)) {
      points.push(c.replace(/\.$/, ""));
    }
  }
  return [...new Set(points)].slice(0, 8);
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
export function buildKnownGapsClarifications(ctx, clarifications, missing, transitions = []) {
  const gaps = normalizeClarificationsForDisplay(clarifications);
  for (const t of transitions) {
    if (!gaps.includes(t)) gaps.push(t);
  }
  for (const m of missing || []) {
    gaps.push(`Input to confirm: ${m}.`);
  }
  if (isBrandManagedPreferred(ctx.preferredFutureOperatingModel)) {
    gaps.push(
      "Brand-managed appears in current inputs. Validate direct brand management availability, economics, and reporting—not whether third-party operators are excluded from all pre-opening needs."
    );
    if (!ctx.operatorInScope) {
      gaps.push(
        "Third-party operator bids may be out of scope; fallback management and pre-opening support pathways may still merit documentation."
      );
    }
  }
  return [...new Set(gaps)].slice(0, 12);
}

/**
 * @param {ReturnType<typeof import('./operator-capability-rules.js').buildOperatingContext>} ctx
 * @param {import('./project-type.js').ProjectTypeKind} kind
 * @param {{ id: string, label: string }[]} capabilityAreas
 * @param {string[]} transitions
 */
export function buildOwnerAdvisorReviewTakeaway(ctx, kind, capabilityAreas, transitions) {
  const themes = themeListForSummary(kind, capabilityAreas);
  const lines = [];

  if (kind === "conversion_reflag" || operatingModelsDiffer(ctx) || transitions.length) {
    lines.push(
      "This may not be only a brand-selection question. Based on current inputs, operating model transition and capability themes may affect execution, reporting, and handover planning."
    );
  } else {
    lines.push(
      "Based on current inputs, this snapshot highlights operating capability themes that may merit review before brand or operator outreach."
    );
  }

  lines.push(
    `Themes that may merit validation include ${themes}. These areas may be clarified before outreach so the owner/advisor can evaluate operating pathways with better context.`
  );

  lines.push(
    "This snapshot is for owner/advisor review preparation only—not operator matching, scoring, or prescriptive operating-model advice."
  );
  return lines;
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
  return deduped.slice(0, 10);
}

/**
 * @param {ReturnType<typeof import('./operator-capability-rules.js').buildOperatingContext>} ctx
 * @param {import('./project-type.js').ProjectTypeKind} kind
 * @param {{ id: string, label: string }[]} capabilityAreas
 * @param {string[]} clarifications
 * @param {string} status
 */
export function buildExecutiveSummaryNarrative(ctx, kind, capabilityAreas, clarifications, status) {
  const paragraphs = [];
  const region = ctx.primaryMarketRegion !== "—" ? ctx.primaryMarketRegion : "the stated market";
  const pt = ctx.projectType !== "—" ? ctx.projectType : "hotel";
  const themes = themeListForSummary(kind, capabilityAreas);

  let opener = `This is a ${pt} opportunity in ${region}`;
  if (operatingModelsDiffer(ctx)) {
    opener += " where the current and preferred future operating models differ";
  }
  opener += ".";
  paragraphs.push(opener);

  if (status === "blocked") {
    paragraphs.push(
      "Core inputs may be insufficient for reliable capability themes; missing fields may be confirmed before using this draft in external discussions."
    );
    return paragraphs;
  }

  paragraphs.push(
    `Based on current deal inputs, the snapshot highlights operating capabilities that may need to be reviewed before brand or operator outreach, including ${themes}.`
  );

  if (isBrandManagedPreferred(ctx.preferredFutureOperatingModel)) {
    paragraphs.push(
      "Brand-managed appears in current inputs as a future model to validate—including direct management availability, economics, and whether third-party management or pre-opening advisory remains a practical pathway to confirm."
    );
  } else if (!ctx.operatorInScope) {
    paragraphs.push(
      "Third-party operator bids may be limited for this deal based on current inputs; operating pathways and pre-opening themes may still merit validation before brand commitments."
    );
  }

  if (clarifications.length > 0 || status === "limited") {
    paragraphs.push(
      `${clarifications.length} clarification${clarifications.length === 1 ? "" : "s"} may be resolved to strengthen this internal draft before external sharing.`
    );
  }

  return paragraphs;
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
  const knownGapsClarifications = buildKnownGapsClarifications(
    ctx,
    clarifications,
    missingInputs || [],
    operatingModelTransitionsToValidate
  );
  const newBuildGuidance = buildNewBuildGuidance(kind);
  const executiveSummary = buildExecutiveSummaryNarrative(
    ctx,
    kind,
    capabilityAreas,
    clarifications,
    snapshotStatus
  );
  const ownerAdvisorReviewTakeaway = buildOwnerAdvisorReviewTakeaway(
    ctx,
    kind,
    enrichedCapabilityAreas,
    operatingModelTransitionsToValidate
  );

  const brandManagedGuidance = isBrandManagedPreferred(ctx.preferredFutureOperatingModel)
    ? [
        "Brand-managed appears in current inputs—validate direct brand management availability and economics.",
        "Review consideration: reporting, incentive alignment, and capital responsibility under a brand-managed structure.",
        "Review consideration: if brand management is unavailable, whether the brand may approve third-party management.",
        "Pre-opening planning, standards coordination, and local execution may merit review before a final structure is confirmed.",
      ]
    : [];

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

  return {
    executiveSummary,
    ownerAdvisorReviewTakeaway,
    whyOperatorStrategyMatters,
    operatingPathways,
    capabilityImplications,
    decisionPointsBeforeOutreach,
    diligenceQuestions,
    knownGapsClarifications,
    displayClarifications,
    newBuildGuidance,
    brandManagedGuidance,
    operatingModelTransitionsToValidate,
    capabilityAreas: enrichedCapabilityAreas,
  };
}
