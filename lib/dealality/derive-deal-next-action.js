/**

 * Per-deal recommended next step for My Deals — pure rule engine (no API, no AI).

 */

import {

  deriveOwnerNextAction,

  enrichWorkspaceRow,

  isAwaitingBrand,

  isAwaitingOwner,

  isPassedArchived,

  isStalledRow,

} from "../deal-workspace-pipeline.js";

import { isOperatorInScopeFromFields } from "../operator-capability-inputs.js";



export const QUICK_ACTION_IDS = {

  COMPLETE_DEAL_INFO: "complete-deal-info",

  RUN_READINESS: "run-readiness",

  REVIEW_BRAND_ALIGNMENT: "review-brand-alignment",

  REVIEW_OPERATOR_STRATEGY: "review-operator-strategy",

  PREPARE_OUTREACH: "prepare-outreach",

  VIEW_CONTACTED: "view-contacted",

  VIEW_SHORTLIST: "view-shortlist",

};



const PHASE = {

  INTAKE_INCOMPLETE: "intake_incomplete",

  READINESS_NOT_RUN: "readiness_not_run",

  READINESS_GAPS: "readiness_gaps",

  BRAND_ALIGNMENT: "brand_alignment",

  OPERATOR_STRATEGY: "operator_strategy",

  OUTREACH_PREP: "outreach_prep",

  ACTIVE_OUTREACH_OWNER: "active_outreach_owner",

  ACTIVE_OUTREACH_BRAND: "active_outreach_brand",

  ADVANCED_DEAL: "advanced_deal",

  CLOSED_PASSED: "closed_passed",

  TRACK_PROGRESS: "track_progress",

};



function str(v) {

  return v == null ? "" : String(v).trim();

}



function normStage(stage) {

  return str(stage).toLowerCase();

}



function hasReadinessReview(deal) {

  if (!deal || typeof deal !== "object") return false;

  if (str(deal.dealReadinessStage)) return true;

  if (deal.dealReadinessScore != null && deal.dealReadinessScore !== "" && !Number.isNaN(Number(deal.dealReadinessScore))) {

    return true;

  }

  return Boolean(str(deal.dealReadinessLastReviewed));

}



function numOrNull(v) {

  if (v == null || v === "") return null;

  const n = Number(v);

  return Number.isFinite(n) ? n : null;

}



function isPlaceholder(s) {

  const t = str(s);

  return !t || t === "—" || t === "-";

}



function isIntakeIncomplete(deal) {

  const form = str(deal.formStatus).toLowerCase();

  if (/draft|incomplete|quick start/i.test(form)) return true;

  if (isPlaceholder(deal.projectName)) return true;

  if (isPlaceholder(deal.hotelLocation)) return true;

  return false;

}



function isBrandPath(deal) {

  const dt = str(deal.dealBidType);

  if (dt === "3rd party only") return false;

  return true;

}



function isOperatorPath(deal) {

  const dt = str(deal.dealBidType);

  if (dt === "3rd party only" || dt === "Both") return true;

  const fields = { ...(deal.strategicIntentForm || {}), ...deal };

  return isOperatorInScopeFromFields(fields);

}



function readinessHasGaps(deal) {

  const blocking = numOrNull(deal.dealReadinessBlockingCount);

  const missing = numOrNull(deal.dealReadinessMissingCount);

  const stage = normStage(deal.dealReadinessStage);

  if (blocking != null && blocking > 0) return true;

  if (stage === "discovery" || stage === "shaping") return true;

  if (missing != null && missing > 5) return true;

  return false;

}



function isOutreachReady(deal) {

  const stage = normStage(deal.dealReadinessStage);

  const score = numOrNull(deal.dealReadinessScore);

  if (stage.includes("ready for external") || stage === "ready") return true;

  if (stage === "advancing") return true;

  if (score != null && score >= 70) return true;

  return false;

}



function mapContactedRow(row) {

  const r = row && typeof row === "object" ? row : {};

  return {

    status: r.status ?? r._requestStatus,

    _requestStatus: r._requestStatus ?? r.status,

    ndaStatus: r.ndaStatus,

    dealRoomAccess: r.dealRoomAccess,

    proposalStatus: r.proposalStatus ?? (r.proposal && r.proposal.proposalStatus),

    proposal: r.proposal,

    requestSentAt: r.requestSentAt,

    lastUpdated: r.lastUpdated,

    lastActivity: r.lastActivity,

    nextFollowupDate: r.nextFollowupDate,

  };

}



function enrichContactedRows(rows) {

  return (Array.isArray(rows) ? rows : []).map((r) => enrichWorkspaceRow(mapContactedRow(r)));

}



function activeContactedRows(rows) {

  return enrichContactedRows(rows).filter((r) => r.workspaceBucket !== "archived");

}



function allContactedClosed(rows) {

  const enriched = enrichContactedRows(rows);

  if (!enriched.length) return false;

  return enriched.every((r) => r.workspaceBucket === "archived");

}



function hasAdvancedContacted(rows) {

  return activeContactedRows(rows).some((r) =>

    ["nda-room", "terms-proposal", "advanced"].includes(r.workspaceBucket)

  );

}



function ownerActionRow(rows) {

  return activeContactedRows(rows).find((r) => isAwaitingOwner(r)) || null;

}



function dealStatusPlain(deal) {

  const s = str(deal.dealStatus);

  if (/active|visible/i.test(s)) return "active";

  if (s) return s.toLowerCase();

  return "in progress";

}



function readinessPlain(deal) {

  const score = numOrNull(deal.dealReadinessScore);

  const stage = str(deal.dealReadinessStage);

  if (score != null && score >= 85) return `readiness is strong (${Math.round(score)}/100)`;

  if (score != null && score >= 70) return `readiness is solid (${Math.round(score)}/100)`;

  if (score != null) return `readiness is ${Math.round(score)}/100`;

  if (stage) return `readiness is at the ${stage} stage`;

  return "readiness has not been saved yet";

}



/** Deal-scoped BDR signals (not portfolio-wide). */

export function computeDealContactedSignals(contactedRows) {

  const active = activeContactedRows(contactedRows);

  const signals = [];

  if (!active.length) return signals;



  signals.push({ label: "Contacted Brands", value: String(active.length) });



  const awaitingOwner = active.filter(isAwaitingOwner).length;

  if (awaitingOwner > 0) {

    signals.push({ label: "Awaiting Owner Action", value: String(awaitingOwner) });

  }



  const awaitingBrand = active.filter(isAwaitingBrand).length;

  if (awaitingBrand > 0) {

    signals.push({ label: "Awaiting Brand Response", value: String(awaitingBrand) });

  }



  const stalled = active.filter(isStalledRow).length;

  if (stalled > 0) {

    signals.push({ label: "Stalled / At Risk", value: String(stalled) });

  }



  let latestMs = null;

  active.forEach((r) => {

    if (r.lastActivitySort != null && (latestMs == null || r.lastActivitySort > latestMs)) {

      latestMs = r.lastActivitySort;

    }

  });

  if (latestMs != null) {

    signals.push({ label: "Last Activity", value: new Date(latestMs).toISOString().slice(0, 10) });

  }



  return signals;

}



function buildQuickActions(flags) {

  const viewContactedLabel = flags.viewContactedLabel || "View Contacted Brands";

  const defs = [

    { id: QUICK_ACTION_IDS.COMPLETE_DEAL_INFO, label: "Complete Deal Info", show: flags.completeDealInfo },

    { id: QUICK_ACTION_IDS.RUN_READINESS, label: "Open Deal Readiness", show: flags.runReadiness },

    { id: QUICK_ACTION_IDS.REVIEW_BRAND_ALIGNMENT, label: "Review Brand Alignment", show: flags.reviewBrandAlignment },

    { id: QUICK_ACTION_IDS.REVIEW_OPERATOR_STRATEGY, label: "Review Operator Strategy", show: flags.reviewOperatorStrategy },

    { id: QUICK_ACTION_IDS.PREPARE_OUTREACH, label: "Prepare Outreach Setup", show: flags.prepareOutreach },

    { id: QUICK_ACTION_IDS.VIEW_CONTACTED, label: viewContactedLabel, show: flags.viewContacted },

    { id: QUICK_ACTION_IDS.VIEW_SHORTLIST, label: "View Brand Shortlist", show: flags.viewShortlist },

  ];

  return defs.filter((d) => d.show);

}



function resultBase(deal, extra) {

  const d = deal || {};

  return {

    dealId: str(d.id),

    projectName: isPlaceholder(d.projectName) ? "—" : str(d.projectName),

    location: isPlaceholder(d.hotelLocation) ? "—" : str(d.hotelLocation),

    dealStatus: isPlaceholder(d.dealStatus) ? "—" : str(d.dealStatus),

    formStatus: isPlaceholder(d.formStatus) ? "" : str(d.formStatus),

    readinessStage: str(d.dealReadinessStage) || "Not reviewed yet",

    readinessScore: numOrNull(d.dealReadinessScore),

    currentDealState: "",

    whatNeedsAttention: "",

    primaryAction: "",

    primaryActionLabel: "",

    primaryQuickActionId: "",

    whyThisMatters: "",

    dealSignals: [],

    showRecommendedNextStep: false,

    missingInformation: [],

    muted: false,

    quickActions: [],

    ...extra,

  };

}



function isGenericNavigationOnly(result) {

  const state = str(result.currentDealState);

  const attention = str(result.whatNeedsAttention);

  const why = str(result.whyThisMatters);

  if (!state || !attention || !why) return true;

  if (state === attention && attention === why) return true;

  const navOnly = /^(review contacted brands?|view contacted brands?)\.?$/i.test(attention);

  if (navOnly && (state === attention || state.toLowerCase().includes("contacted brands in progress"))) {

    return true;

  }

  return false;

}



function finalize(deal, contacted, extra) {

  const dealSignals = computeDealContactedSignals(contacted);

  const result = resultBase(deal, {

    dealSignals,

    ...extra,

  });

  if (!result.primaryActionLabel && result.primaryAction) {

    result.primaryActionLabel = result.primaryAction;

  }

  if (!result.whatNeedsAttention && result.primaryAction) {

    result.whatNeedsAttention = result.primaryAction;

  }

  result.showRecommendedNextStep = !isGenericNavigationOnly(result);

  return result;

}



/**

 * @param {object} context

 * @param {object} context.deal — My Deals list deal object

 * @param {object[]} [context.contactedRows] — BDR rows for this deal only

 * @param {number} [context.shortlistCount]

 * @param {number} [context.operatorStrategyRowCount]

 */

export function deriveDealNextAction(context) {

  const ctx = context && typeof context === "object" ? context : {};

  const deal = ctx.deal || {};

  const contacted = Array.isArray(ctx.contactedRows) ? ctx.contactedRows : [];

  const shortlistCount = numOrNull(ctx.shortlistCount) ?? 0;

  const contactedCount = activeContactedRows(contacted).length;

  const hasPreferredBrands = Boolean(str(deal.preferredBrandsChosen));



  const defaultFlags = {

    completeDealInfo: true,

    runReadiness: true,

    reviewBrandAlignment: isBrandPath(deal) && hasReadinessReview(deal),

    reviewOperatorStrategy: isOperatorPath(deal),

    prepareOutreach: false,

    viewContacted: contactedCount > 0,

    viewShortlist: shortlistCount > 0,

  };



  if (!str(deal.id).startsWith("rec")) {

    return finalize(deal, contacted, {

      phase: "none",

      muted: true,

      showRecommendedNextStep: false,

    });

  }



  const dealStatusLower = str(deal.dealStatus).toLowerCase();

  if (

    (contactedCount > 0 && allContactedClosed(contacted)) ||

    /signed|closed|archived|passed|declined/i.test(dealStatusLower)

  ) {

    return finalize(deal, contacted, {

      phase: PHASE.CLOSED_PASSED,

      currentDealState: "Prior brand outreach on this deal is closed or passed.",

      whatNeedsAttention: "Confirm whether the deal status still reflects reality and refresh shortlist or intake if you are reopening outreach.",

      primaryAction: "Update Deal Status",

      primaryActionLabel: "Update Deal Status",

      primaryQuickActionId: QUICK_ACTION_IDS.COMPLETE_DEAL_INFO,

      whyThisMatters: "Stale deal status can mislead reporting and hide when it is safe to restart outreach.",

      muted: true,

      quickActions: buildQuickActions({

        ...defaultFlags,

        completeDealInfo: true,

        runReadiness: hasReadinessReview(deal),

        prepareOutreach: false,

        viewContacted: contactedCount > 0,

        viewShortlist: shortlistCount > 0,

      }),

    });

  }



  const ownerRow = ownerActionRow(contacted);

  if (ownerRow) {

    const ownerCount = activeContactedRows(contacted).filter(isAwaitingOwner).length;

    const ownerLabel = deriveOwnerNextAction(ownerRow);

    const attention =

      ownerLabel && ownerLabel !== "—"

        ? `Respond to owner follow-up items (${ownerLabel.toLowerCase()}) on ${ownerCount} contacted brand${ownerCount === 1 ? "" : "s"}.`

        : `Respond to information requests on ${ownerCount} contacted brand${ownerCount === 1 ? "" : "s"} before outreach can advance.`;

    return finalize(deal, contacted, {

      phase: PHASE.ACTIVE_OUTREACH_OWNER,

      currentDealState: `This deal is ${dealStatusPlain(deal)}, ${readinessPlain(deal)}, and at least one contacted brand is waiting on owner input.`,

      whatNeedsAttention: attention,

      primaryAction: ownerLabel && ownerLabel !== "—" ? ownerLabel : "Provide Requested Information",

      primaryActionLabel: "Review Contacted Brand Status",

      primaryQuickActionId: QUICK_ACTION_IDS.VIEW_CONTACTED,

      whyThisMatters: "Outreach stalls when owner requests pile up; clearing them keeps brand momentum.",

      quickActions: buildQuickActions({

        ...defaultFlags,

        completeDealInfo: false,

        viewContacted: true,

        viewContactedLabel: "Review Contacted Brand Status",

        prepareOutreach: !deal.hasOutreachSetup,

      }),

    });

  }



  if (hasAdvancedContacted(contacted)) {

    const hot = activeContactedRows(contacted).find((r) =>

      ["nda-room", "terms-proposal", "advanced"].includes(r.workspaceBucket)

    );

    const label = hot ? deriveOwnerNextAction(hot) : "Track negotiation progress";

    return finalize(deal, contacted, {

      phase: PHASE.ADVANCED_DEAL,

      currentDealState: `This deal is ${dealStatusPlain(deal)} with active negotiation, NDA, or deal-room activity on contacted brands.`,

      whatNeedsAttention: label && label !== "—"

        ? `Track and act on negotiation items: ${label.toLowerCase()}.`

        : "Track negotiation progress and owner actions across contacted brands.",

      primaryAction: label && label !== "—" ? label : "Track Deal Progress",

      primaryActionLabel: "Review Contacted Brand Status",

      primaryQuickActionId: QUICK_ACTION_IDS.VIEW_CONTACTED,

      whyThisMatters: "Late follow-up during negotiation can slow term comparison and deal-room momentum.",

      quickActions: buildQuickActions({

        ...defaultFlags,

        completeDealInfo: false,

        viewContacted: true,

        viewContactedLabel: "Review Contacted Brand Status",

      }),

    });

  }



  if (contactedCount > 0) {

    return finalize(deal, contacted, {

      phase: PHASE.ACTIVE_OUTREACH_BRAND,

      currentDealState: `This deal is ${dealStatusPlain(deal)}, ${readinessPlain(deal)}, and brand outreach is already underway.`,

      whatNeedsAttention: "Review contacted brand activity before adding more brands or changing the shortlist.",

      primaryAction: "Review Contacted Brand Status",

      primaryActionLabel: "Review Contacted Brand Status",

      primaryQuickActionId: QUICK_ACTION_IDS.VIEW_CONTACTED,

      whyThisMatters: "The deal has moved beyond initial alignment. The next risk is losing momentum or missing owner follow-up items.",

      quickActions: buildQuickActions({

        ...defaultFlags,

        completeDealInfo: false,

        viewContacted: true,

        viewContactedLabel: "Review Contacted Brand Status",

        runReadiness: hasReadinessReview(deal),

      }),

    });

  }



  if (isIntakeIncomplete(deal)) {

    return finalize(deal, contacted, {

      phase: PHASE.INTAKE_INCOMPLETE,

      currentDealState: "Core project details are still incomplete for this deal.",

      whatNeedsAttention: "Finish intake fields (project name, location, and form status) before alignment or outreach.",

      primaryAction: "Complete Deal Information",

      primaryActionLabel: "Complete Deal Info",

      primaryQuickActionId: QUICK_ACTION_IDS.COMPLETE_DEAL_INFO,

      whyThisMatters: "Incomplete intake weakens readiness scoring and makes brand review harder to interpret.",

      quickActions: buildQuickActions({

        ...defaultFlags,

        runReadiness: false,

        reviewBrandAlignment: false,

        reviewOperatorStrategy: isOperatorPath(deal),

        prepareOutreach: false,

        viewContacted: false,

        viewShortlist: shortlistCount > 0,

      }),

    });

  }



  if (!hasReadinessReview(deal)) {

    return finalize(deal, contacted, {

      phase: PHASE.READINESS_NOT_RUN,

      currentDealState: `Intake is in place, but ${readinessPlain(deal)}.`,

      whatNeedsAttention: "Run Deal Readiness to surface gaps, stage, and whether the deal is ready for external review.",

      primaryAction: "Run Deal Readiness Review",

      primaryActionLabel: "Open Deal Readiness",

      primaryQuickActionId: QUICK_ACTION_IDS.RUN_READINESS,

      whyThisMatters: "Without a saved readiness review, alignment and outreach decisions lack a shared baseline.",

      quickActions: buildQuickActions({

        ...defaultFlags,

        reviewBrandAlignment: false,

        prepareOutreach: false,

        viewContacted: false,

      }),

    });

  }



  if (readinessHasGaps(deal)) {

    const missing = numOrNull(deal.dealReadinessMissingCount);

    const blocking = numOrNull(deal.dealReadinessBlockingCount);

    const bullets = [];

    if (blocking != null && blocking > 0) bullets.push(`${blocking} blocking gap(s) flagged in Deal Readiness`);

    if (missing != null && missing > 0) bullets.push(`${missing} missing field(s) in Deal Readiness`);

    if (!bullets.length) bullets.push("Readiness stage indicates more intake detail is needed");

    const gapDetail = bullets[0] || "readiness gaps remain";

    return finalize(deal, contacted, {

      phase: PHASE.READINESS_GAPS,

      currentDealState: `This deal is ${dealStatusPlain(deal)} with ${readinessPlain(deal)}, but readiness still flags issues.`,

      whatNeedsAttention: `Close readiness gaps before external review (${gapDetail.toLowerCase()}).`,

      primaryAction: "Complete Missing Information",

      primaryActionLabel: "Open Deal Readiness",

      primaryQuickActionId: QUICK_ACTION_IDS.RUN_READINESS,

      whyThisMatters: "Blocking readiness gaps can delay brand review and create rework during outreach.",

      missingInformation: bullets.slice(0, 3),

      quickActions: buildQuickActions({

        ...defaultFlags,

        prepareOutreach: false,

        viewContacted: false,

      }),

    });

  }



  if (isBrandPath(deal) && hasPreferredBrands && contactedCount === 0) {

    return finalize(deal, contacted, {

      phase: PHASE.BRAND_ALIGNMENT,

      currentDealState: `This deal is ${dealStatusPlain(deal)} with ${readinessPlain(deal)} and preferred brands selected.`,

      whatNeedsAttention: "Review brand alignment against your preferred brands before starting outreach.",

      primaryAction: "Review Brand Alignment",

      primaryActionLabel: "Review Brand Alignment",

      primaryQuickActionId: QUICK_ACTION_IDS.REVIEW_BRAND_ALIGNMENT,

      whyThisMatters: "Alignment review helps confirm fit and messaging before brands are contacted.",

      quickActions: buildQuickActions({

        ...defaultFlags,

        prepareOutreach: !deal.hasOutreachSetup,

        viewContacted: false,

      }),

    });

  }



  if (isOperatorPath(deal) && (ctx.operatorStrategyRowCount == null || ctx.operatorStrategyRowCount === 0)) {

    return finalize(deal, contacted, {

      phase: PHASE.OPERATOR_STRATEGY,

      currentDealState: `This deal is ${dealStatusPlain(deal)} with ${readinessPlain(deal)} and includes operator path considerations.`,

      whatNeedsAttention: "Review operator capability and alignment before outreach or shortlist changes.",

      primaryAction: "Review Operator Strategy",

      primaryActionLabel: "Review Operator Strategy",

      primaryQuickActionId: QUICK_ACTION_IDS.REVIEW_OPERATOR_STRATEGY,

      whyThisMatters: "Operator fit affects which brands and structures are realistic for this deal.",

      quickActions: buildQuickActions({

        ...defaultFlags,

        reviewBrandAlignment: isBrandPath(deal) && hasPreferredBrands,

        prepareOutreach: false,

        viewContacted: false,

      }),

    });

  }



  if (isOutreachReady(deal) && !deal.hasOutreachSetup) {

    return finalize(deal, contacted, {

      phase: PHASE.OUTREACH_PREP,

      currentDealState: `This deal is ${dealStatusPlain(deal)} with ${readinessPlain(deal)} and no outreach preferences saved yet.`,

      whatNeedsAttention: "Set outreach preferences (timing, messaging, attachments) before contacting brands.",

      primaryAction: "Prepare Outreach Preferences",

      primaryActionLabel: "Prepare Outreach Setup",

      primaryQuickActionId: QUICK_ACTION_IDS.PREPARE_OUTREACH,

      whyThisMatters: "Outreach without preferences can send the wrong timing, tone, or materials to brands.",

      quickActions: buildQuickActions({

        ...defaultFlags,

        prepareOutreach: true,

        viewContacted: false,

      }),

    });

  }



  if (isBrandPath(deal) && hasPreferredBrands) {

    return finalize(deal, contacted, {

      phase: PHASE.BRAND_ALIGNMENT,

      currentDealState: `This deal is ${dealStatusPlain(deal)} with ${readinessPlain(deal)} and preferred brands on file.`,

      whatNeedsAttention: "Revisit brand alignment before expanding outreach or changing the shortlist.",

      primaryAction: "Review Brand Alignment",

      primaryActionLabel: "Review Brand Alignment",

      primaryQuickActionId: QUICK_ACTION_IDS.REVIEW_BRAND_ALIGNMENT,

      whyThisMatters: "Fit signals can shift as intake improves; alignment review keeps outreach focused.",

      quickActions: buildQuickActions({

        ...defaultFlags,

        prepareOutreach: !deal.hasOutreachSetup,

      }),

    });

  }



  if (isOperatorPath(deal)) {

    return finalize(deal, contacted, {

      phase: PHASE.OPERATOR_STRATEGY,

      currentDealState: `This deal is ${dealStatusPlain(deal)} with ${readinessPlain(deal)} and operator scope in play.`,

      whatNeedsAttention: "Review operator strategy and capability fit for this deal.",

      primaryAction: "Review Operator Strategy",

      primaryActionLabel: "Review Operator Strategy",

      primaryQuickActionId: QUICK_ACTION_IDS.REVIEW_OPERATOR_STRATEGY,

      whyThisMatters: "Operator constraints should be clear before brand outreach accelerates.",

      quickActions: buildQuickActions({

        ...defaultFlags,

        reviewBrandAlignment: false,

      }),

    });

  }



  return finalize(deal, contacted, {

    phase: PHASE.TRACK_PROGRESS,

    currentDealState: `This deal is ${dealStatusPlain(deal)} with ${readinessPlain(deal)}.`,

    whatNeedsAttention: "Continue strengthening intake and readiness outputs for this deal.",

    primaryAction: "Complete Deal Information",

    primaryActionLabel: "Complete Deal Info",

    primaryQuickActionId: QUICK_ACTION_IDS.COMPLETE_DEAL_INFO,

    whyThisMatters: "More complete deal context improves every downstream review and outreach step.",

    quickActions: buildQuickActions(defaultFlags),

  });

}



export { PHASE };


