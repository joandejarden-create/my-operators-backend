/**
 * Deal Brief v2 — ownerDraft + recipientView modes (single document, mode-driven copy).
 * Reuses readiness payload when available; no per-deal overrides.
 */
(function (global) {
  "use strict";

  var MODES = { OWNER_DRAFT: "ownerDraft", RECIPIENT_VIEW: "recipientView" };
  var VALIDATION_MAX = 6;

  var EXECUTIVE_FIELD_LABELS = {
    "Project Type": "Project type",
    "Stage of Development": "Development stage",
    "Total Number of Rooms/Keys": "Key count / room program",
    "Current Form of Site Control": "Site control",
    "Total Project Cost Range": "Project cost range",
    "PIP Budget Range (if conversion)": "PIP budget range",
    "PIP / CapEx Status": "PIP / CapEx status",
    "Preferred Deal Structure": "Preferred deal structure",
    "Lease Type": "Lease type",
    "Primary Demand Drivers": "Demand drivers",
    "Is the hotel currently branded?": "Current brand status",
    "Is the hotel currently managed by a third-party operator?": "Current operator status",
    "Has there ever been a franchise, branded management, affiliation or similar agreeement pertaining to the proposed hotel or site?":
      "Prior brand / management agreement history",
    "Has there ever been a franchise, branded management, affiliation or similar agreement pertaining to the proposed hotel or site?":
      "Prior brand / management agreement history",
  };

  var LEASE_FIELD_KEYS = [
    "Lease Type",
    "Initial Lease Term (years)",
    "Lease Start Date (or Availability)",
    "Lease Expiration or End Date",
    "Base Rent (annual or structure)",
    "Percentage Rent (if applicable)",
    "CAM Insurance Tax Responsibility",
    "Key Money or TI Allowance",
    "Renewal Options",
    "Early Termination or Break Clause",
    "Security Deposit or Guarantees",
    "Lease Structure Notes",
  ];

  var OPERATOR_FIELD_KEYS = [
    "Is the hotel currently managed by a third-party operator?",
    "Minimum Operator Experience (years)",
    "Preferred Third-Party Operators (names)",
    "Preferred Third-Party Operator Profile",
    "Services Required From Operator",
    "Other Operator Criteria or Notes",
    "Level of Involvement in Day-to-Day Ops",
    "Plan to Self-Manage or Hire Third Party?",
  ];

  var PIP_FIELD_KEYS = ["PIP / CapEx Status", "PIP Budget Range (if conversion)"];

  function readinessStageKey(stage) {
    return String(stage || "").trim().toLowerCase();
  }

  function executiveFieldLabel(field) {
    var f = String(field || "").trim();
    if (!f) return "Additional item";
    if (EXECUTIVE_FIELD_LABELS[f]) return EXECUTIVE_FIELD_LABELS[f];
    var low = f.toLowerCase();
    if (
      (/agreeement/i.test(f) || /similar agreement pertaining/i.test(low)) &&
      /franchise|branded management|affiliation/i.test(low)
    ) {
      return "Prior brand / management agreement history";
    }
    if (/franchise|branded management|affiliation/i.test(low) && /proposed hotel/i.test(low)) {
      return "Prior brand / management agreement history";
    }
    if (/project type/i.test(low)) return "Project type";
    if (/stage of development/i.test(low)) return "Development stage";
    if (/currently branded/i.test(low)) return "Current brand status";
    if (/currently managed|third.party operator/i.test(low)) return "Current operator status";
    if (/pip\s*\/\s*capex|capex status/i.test(low)) return "PIP / CapEx status";
    if (/preferred deal structure/i.test(low)) return "Preferred deal structure";
    if (/site control/i.test(low)) return "Site control";
    return f.length > 48 ? f.slice(0, 45).trim() + "…" : f;
  }

  function inferBriefContext(fields) {
    fields = fields || {};
    function norm(v) {
      if (v == null) return "";
      if (typeof v === "string") return v.trim().toLowerCase();
      if (Array.isArray(v)) return v.map(norm).join(" ");
      if (typeof v === "object" && v.name) return String(v.name).trim().toLowerCase();
      return String(v).trim().toLowerCase();
    }
    function gv(key) {
      var v = fields[key];
      return v != null && v !== "" ? v : undefined;
    }
    var pt = norm(gv("Project Type"));
    var dealStruct = norm(gv("Preferred Deal Structure"));
    var planManage = norm(gv("Plan to Self-Manage or Hire Third Party?"));

    var projectTypeContext = "unknown";
    if (/conversion|reflag|re-flag/.test(pt)) projectTypeContext = "conversion_reflag";
    else if (/renovation|reposition|rebrand/.test(pt) && !/new build/.test(pt)) projectTypeContext = "renovation_repositioning";
    else if (/new build|ground.?up/.test(pt)) projectTypeContext = "new_build";
    else if (/land|development site|greenfield/.test(pt)) projectTypeContext = "land_development_site";
    else if (/operating|existing/.test(pt)) projectTypeContext = "operating_asset";

    var dealStructureContext = "unknown";
    if (/lease|ground lease/.test(dealStruct)) dealStructureContext = "lease";
    else if (/flexible|open to options/.test(dealStruct)) dealStructureContext = "flexible_open";
    else if (/franchise only|franchise-only|^franchise$/.test(dealStruct) && !/management|operator|lease/.test(dealStruct)) {
      dealStructureContext = "franchise_only";
    } else if (/management agreement|third.party management|operator agreement/.test(dealStruct)) {
      dealStructureContext = "management_agreement";
    } else if (/franchise.*management|brand.*operator|both franchise/.test(dealStruct)) {
      dealStructureContext = "brand_operator";
    } else if (/franchise/.test(dealStruct) && /management|operator/.test(dealStruct)) {
      dealStructureContext = "brand_operator";
    } else if (/franchise/.test(dealStruct)) dealStructureContext = "franchise_only";

    var operatorContext = "unknown";
    if (/hire third|third party|seeking operator|need an operator/.test(planManage)) operatorContext = "operator_needed";
    if (dealStructureContext === "brand_operator" || dealStructureContext === "management_agreement") {
      operatorContext = "operator_needed";
    }

    return { projectTypeContext: projectTypeContext, dealStructureContext: dealStructureContext, operatorContext: operatorContext };
  }

  function isPreOperating(ctx) {
    return ctx.projectTypeContext === "new_build" || ctx.projectTypeContext === "land_development_site";
  }

  function isExistingAsset(ctx) {
    return (
      ctx.projectTypeContext === "conversion_reflag" ||
      ctx.projectTypeContext === "renovation_repositioning" ||
      ctx.projectTypeContext === "operating_asset"
    );
  }

  function isOperatorInScope(ctx) {
    return (
      ctx.dealStructureContext === "brand_operator" ||
      ctx.dealStructureContext === "management_agreement" ||
      ctx.operatorContext === "operator_needed"
    );
  }

  function isLeaseDeal(fields, ctx) {
    ctx = ctx || inferBriefContext(fields);
    if (ctx.dealStructureContext === "lease" || ctx.dealStructureContext === "flexible_open") return true;
    var ds = String((fields && fields["Preferred Deal Structure"]) || "").toLowerCase();
    return ds.indexOf("lease") >= 0 || ds.indexOf("flexible") >= 0;
  }

  /** Whether an Airtable field key should appear in the brief (client fallback when no readiness payload). */
  function isFieldApplicableClient(fieldKey, fields) {
    var ctx = inferBriefContext(fields);
    if (LEASE_FIELD_KEYS.indexOf(fieldKey) >= 0) return isLeaseDeal(fields, ctx);
    if (OPERATOR_FIELD_KEYS.indexOf(fieldKey) >= 0) {
      if (isPreOperating(ctx) && !isOperatorInScope(ctx)) return false;
      if (ctx.dealStructureContext === "franchise_only" && fieldKey !== "Plan to Self-Manage or Hire Third Party?") {
        return fieldKey === "Plan to Self-Manage or Hire Third Party?" || !isPreOperating(ctx);
      }
      return isOperatorInScope(ctx) || isExistingAsset(ctx) || !isPreOperating(ctx);
    }
    if (PIP_FIELD_KEYS.indexOf(fieldKey) >= 0) return !isPreOperating(ctx) || isExistingAsset(ctx);
    if (fieldKey === "Is the hotel currently branded?" && isPreOperating(ctx)) return false;
    return true;
  }

  function isFieldShownInBrief(fieldKey, fields, readiness) {
    if (!fieldKey) return true;
    if (readiness && readiness.contextAwareScoring) {
      var excluded = readiness.contextExcludedFields || [];
      var tooEarly = readiness.contextTooEarlyFields || [];
      for (var i = 0; i < excluded.length; i++) {
        if (excluded[i] && excluded[i].field === fieldKey) return false;
      }
      for (var j = 0; j < tooEarly.length; j++) {
        if (tooEarly[j] && tooEarly[j].field === fieldKey) return false;
      }
      return true;
    }
    return isFieldApplicableClient(fieldKey, fields);
  }

  function mapBriefStatusFromStage(stage) {
    var sl = readinessStageKey(stage);
    if (sl === "discovery") {
      return {
        briefStatus: "Early draft",
        externalSharing: "Not recommended",
        reviewStatusLabel: "Core Intake Still Needed",
      };
    }
    if (sl === "shaping") {
      return {
        briefStatus: "Draft in progress",
        externalSharing: "Not recommended",
        reviewStatusLabel: "Needs Clarification Before Structured Review",
      };
    }
    if (sl === "advancing") {
      return {
        briefStatus: "Draftable with noted gaps",
        externalSharing: "Not yet recommended for broad circulation",
        reviewStatusLabel: "Eligible for Structured Review",
      };
    }
    if (sl.indexOf("ready for external") >= 0) {
      return {
        briefStatus: "Ready for controlled external review",
        externalSharing: "Selective outreach after owner/advisor validation",
        reviewStatusLabel: "Ready for Controlled External Review",
      };
    }
    if (sl === "ready") {
      return {
        briefStatus: "Ready for advanced review",
        externalSharing: "Available for selected recipients, subject to owner/advisor validation",
        reviewStatusLabel: "Ready for Advanced Review",
      };
    }
    return {
      briefStatus: "Draft in progress",
      externalSharing: "Not recommended",
      reviewStatusLabel: "Needs Clarification Before Structured Review",
    };
  }

  function buildValidationItems(readiness, maxCount) {
    var limit = maxCount == null ? VALIDATION_MAX : Math.min(VALIDATION_MAX, maxCount);
    if (!readiness) return [];
    var seen = {};
    var rows = [];
    function push(item) {
      if (!item || rows.length >= limit) return;
      var raw = item.field || item.label || item.highlightField || "";
      var label = executiveFieldLabel(raw);
      var key = label.toLowerCase();
      if (!raw || seen[key]) return;
      seen[key] = true;
      rows.push(label);
    }
    (readiness.blockingIssues || []).forEach(push);
    (readiness.limitingIssues || []).forEach(push);
    if (rows.length < limit) {
      (readiness.enhancementIssues || []).forEach(push);
    }
    return rows;
  }

  function readinessSummaryContextLabel(projectType) {
    var p = String(projectType || "").trim();
    if (!p) return "";
    if (/new build|ground.?up/i.test(p)) return "new-build";
    if (/conversion|reflag|re-flag/i.test(p)) return "conversion/reflag";
    if (/renovation|reposition|rebrand/i.test(p)) return "renovation/repositioning";
    if (/land|development site|greenfield/i.test(p)) return "land/development-site";
    if (/operating|existing/i.test(p)) return "existing operating-asset";
    return p.toLowerCase().replace(/\s+/g, " ");
  }

  var SCREENING = " that may be relevant for brand and operator screening";

  function buildOwnerOpportunityLead(meta) {
    var pt = meta && meta.projectType && meta.projectType !== "—" ? meta.projectType : "";
    var pos = (meta && meta.targetPositioning) || "";
    var label = readinessSummaryContextLabel(pt);
    var sentence;
    if (!label) {
      sentence = "The current inputs describe a hospitality opportunity" + SCREENING;
    } else {
      sentence = "The current inputs describe a " + label + " hospitality opportunity" + SCREENING;
    }
    if (pos) {
      sentence += ", with potential fit across " + pos + " and adjacent brand pathways";
    } else if (pt && /conversion|reposition|rebrand/i.test(pt)) {
      sentence += ", with potential fit across upscale, upper-upscale, lifestyle, or soft-brand pathways";
    }
    return sentence + ".";
  }

  function buildRecipientOpportunityLead(meta, normalized) {
    var keys = (meta && meta.keyCount) || (normalized && normalized.totalKeys) || "";
    var loc = (meta && meta.marketLine) || (normalized && normalized.hotelLocation) || "the identified market";
    var pt = (meta && meta.projectType) || "";
    var ptBit = pt && pt !== "—" ? String(pt).toLowerCase() + " " : "";
    return (
      "Based on current inputs, this " +
      (keys ? keys + "-key " : "") +
      ptBit +
      "opportunity in " +
      loc +
      " may be relevant for brand and operator review. Additional detail is provided in the sections below."
    );
  }

  /**
   * Resolve brief mode from URL + audience (no per-deal IDs).
   * @param {object} params - URLSearchParams-like { get(name) }
   * @param {object} opts - { fromBrandPortal, forceMode }
   */
  function resolveBriefMode(params, opts) {
    opts = opts || {};
    function get(name) {
      return params && typeof params.get === "function" ? (params.get(name) || "").trim() : "";
    }
    var forced = get("mode") || opts.forceMode || "";
    if (forced === MODES.RECIPIENT_VIEW || forced === "recipient") return MODES.RECIPIENT_VIEW;
    if (forced === MODES.OWNER_DRAFT || forced === "owner") return MODES.OWNER_DRAFT;
    if (get("preview") === "recipient" || get("preview") === "recipientView") return MODES.RECIPIENT_VIEW;
    var from = get("from").toLowerCase();
    if (opts.fromBrandPortal || from === "bdd" || from === "bwp") return MODES.RECIPIENT_VIEW;
    return MODES.OWNER_DRAFT;
  }

  function coverCopyForMode(mode) {
    if (mode === MODES.RECIPIENT_VIEW) {
      return {
        confidential: "Confidential · For recipient only",
        sub: "For Brands & Operators",
        disclaimer:
          "This brief was prepared by Dealality on behalf of the owner and is intended for informational review by selected recipients only.",
        dateSuffix: "Prepared for recipient review",
      };
    }
    return {
      confidential: "Draft for Owner Review",
      sub: "Internal owner/advisor review",
      disclaimer:
        "This brief organizes current deal inputs for internal review. It is not yet shared externally and does not constitute a recommendation, endorsement, or investment advice.",
      dateSuffix: "Generated from current deal inputs · Not yet shared externally",
    };
  }

  function contactCopyForMode(mode) {
    if (mode === MODES.RECIPIENT_VIEW) {
      return {
        cta: "Visit Dealality to review the opportunity and respond.",
        showProposalCta: true,
      };
    }
    return {
      cta: "Review and validate this brief before selecting recipients.",
      showProposalCta: false,
    };
  }

  global.DealBriefV2 = {
    MODES: MODES,
    VALIDATION_MAX: VALIDATION_MAX,
    resolveBriefMode: resolveBriefMode,
    coverCopyForMode: coverCopyForMode,
    contactCopyForMode: contactCopyForMode,
    mapBriefStatusFromStage: mapBriefStatusFromStage,
    buildValidationItems: buildValidationItems,
    buildOwnerOpportunityLead: buildOwnerOpportunityLead,
    buildRecipientOpportunityLead: buildRecipientOpportunityLead,
    executiveFieldLabel: executiveFieldLabel,
    isFieldShownInBrief: isFieldShownInBrief,
    inferBriefContext: inferBriefContext,
  };
})(typeof window !== "undefined" ? window : globalThis);
