/**
 * Injects Phase 5B Operator Alignment fields into Deal Intake and Operator Setup forms.
 * Options loaded from /fixtures/operator-alignment-field-options.json
 */
(function () {
  "use strict";

  function esc(s) {
    return String(s || "")
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/"/g, "&quot;");
  }

  function optionsHtml(list, includeEmpty) {
    var html = includeEmpty ? '<option value="">Select One…</option>' : "";
    (list || []).forEach(function (v) {
      html += '<option value="' + esc(v) + '">' + esc(v) + "</option>";
    });
    return html;
  }

  function fieldLabel(name, displayOverride) {
    return displayOverride != null ? displayOverride : name;
  }

  function multiSelect(name, label, help, list) {
    return (
      '<div class="form-group multi-select-with-count">' +
      '<label class="form-label">' +
      esc(label) +
      '<span class="help-text">' +
      esc(help) +
      ' <span class="selection-count"></span></span></label>' +
      '<select class="form-select" name="' +
      esc(name) +
      '" multiple size="6" style="min-width:260px;">' +
      optionsHtml(list, false) +
      "</select></div>"
    );
  }

  function singleSelect(name, label, help, list) {
    return (
      '<div class="form-group">' +
      '<label class="form-label">' +
      esc(label) +
      '<span class="help-text">' +
      esc(help) +
      "</span></label>" +
      '<select class="form-select" name="' +
      esc(name) +
      '">' +
      optionsHtml(list, true) +
      "</select></div>"
    );
  }

  function injectDealFields(o) {
    var mpIntent = document.getElementById("oasOperatorManagementMpInject");
    if (mpIntent) {
      mpIntent.innerHTML =
        '<div class="form-group multi-select-with-count">' +
        '<label class="form-label">Preferred Operator Management Structure' +
        '<span class="help-text">How would the owner ideally like the hotel to be managed? Owner preference for operating structure — separate from brand franchise economics above. <span class="selection-count"></span></span></label>' +
        '<select class="form-select" name="Preferred Operator Management Structure" multiple size="6" style="min-width:260px;">' +
        optionsHtml(o.preferredOperatorManagementStructure, false) +
        "</select></div>";
    }

    var strat = document.getElementById("oasOperatorStrategyInject");
    if (strat) {
      strat.innerHTML =
        '<h3 class="project-fit-subheader">Operator Strategy &amp; Requirements</h3>' +
        '<p class="subsection-description">Structured inputs for operator alignment review. Legacy fields above remain for compatibility.</p>' +
        '<div class="form-grid">' +
        singleSelect(
          "Operator Structure Intent",
          "Operator Structure Intent",
          "What role should the operator play in the opportunity? Optional owner intent for operator alignment review.",
          o.preferredOperatorManagementStructure
        ) +
        singleSelect(
          "Operator Review Status",
          fieldLabel("Operator Review Status"),
          "Where you are in operator review workflow (separate from strategy status).",
          o.operatorReviewStatus
        ) +
        multiSelect(
          "Preferred Management Structure",
          fieldLabel("Preferred Operator Management Structure"),
          "How you expect the hotel to be operated (not the same as franchise-only brand economics).",
          o.preferredManagementStructure
        ) +
        multiSelect(
          "Required Operator Services",
          fieldLabel("Required Operator Services"),
          "Services you expect from an operator (standardized list).",
          o.operatorServices
        ) +
        multiSelect(
          "Must-Have Operator Services",
          fieldLabel("Must-Have Operator Services"),
          "Non-negotiable operator services.",
          o.operatorServices
        ) +
        multiSelect(
          "Nice-to-Have Operator Services",
          fieldLabel("Nice-to-Have Operator Services"),
          "Optional operator services.",
          o.operatorServices
        ) +
        singleSelect(
          "Market Presence Requirement",
          fieldLabel("Market Presence Requirement"),
          "How local/country operator presence should be validated.",
          o.marketPresenceRequirement
        ) +
        singleSelect(
          "Pre-Opening Support Needed",
          fieldLabel("Pre-Opening Support Needed"),
          "Whether pre-opening operator support is in scope.",
          o.preOpeningSupportNeeded
        ) +
        singleSelect(
          "Owner Reporting Expectations",
          fieldLabel("Owner Reporting Expectations"),
          "Reporting depth you expect from an operator.",
          o.ownerReportingExpectations
        ) +
        singleSelect(
          "Brand / Operator Responsibility Split",
          fieldLabel("Brand / Operator Responsibility Split"),
          "How brand and operator roles may be divided.",
          o.brandOperatorSplit
        ) +
        singleSelect(
          "Owner Control Preference",
          fieldLabel("Owner Control Preference"),
          "How much control the owner expects to retain.",
          o.ownerControlPreference
        ) +
        "</div>";
    }

    var brand = document.getElementById("oasBrandAgreementInject");
    if (brand) {
      brand.innerHTML =
        '<h3 class="project-fit-subheader">Brand &amp; Agreement Structure</h3>' +
        '<p class="subsection-description">Clarifies brand affiliation vs operating model. Does not replace Preferred Deal Structure above.</p>' +
        '<div class="form-grid">' +
        singleSelect(
          "Brand Agreement Structure",
          fieldLabel("Brand Agreement Structure"),
          "Intended brand agreement type (franchise, management, etc.).",
          o.brandAgreementStructure
        ) +
        singleSelect(
          "Operating Model",
          fieldLabel("Operating Model", "Operating Model (Target)"),
          "Who is expected to run the hotel day-to-day.",
          o.dealOperatingModel
        ) +
        multiSelect(
          "Operator Scope",
          fieldLabel("Operator Scope"),
          "Which operator functions are in scope.",
          o.operatorScope
        ) +
        "</div>";
    }

    var ops = document.getElementById("oasOperationsCommercialInject");
    if (ops) {
      ops.innerHTML =
        '<h3 class="project-fit-subheader">Operations &amp; Commercial Requirements</h3>' +
        '<div class="form-grid">' +
        singleSelect("F&B Complexity", fieldLabel("F&B Complexity"), "Relative F&B scope for operator fit.", o.fbCapability) +
        multiSelect(
          "Commercial Priority",
          fieldLabel("Commercial Priority"),
          "Commercial areas that matter most on this deal.",
          o.commercialPriority
        ) +
        singleSelect(
          "Local Labor / HR Support Needed",
          fieldLabel("Local Labor / HR Support Needed"),
          "Whether operator should provide HR/labor support.",
          o.yesNoNa
        ) +
        singleSelect(
          "Procurement Support Needed",
          fieldLabel("Procurement Support Needed"),
          "Whether operator procurement support is needed.",
          o.yesNoNa
        ) +
        singleSelect(
          "Owner Internal Ops Capability",
          fieldLabel("Owner Internal Ops Capability"),
          "Strength of owner’s in-house hotel operations team.",
          o.ownerInternalOps
        ) +
        singleSelect(
          "Opening Timeline",
          fieldLabel("Opening Timeline"),
          "Expected opening horizon (complements Opening / Transition Phase).",
          o.openingTimeline
        ) +
        "</div>";
    }
  }

  function injectOperatorFields(o) {
    var el = document.getElementById("oasOperatorProfileInject");
    if (!el) return;
    el.innerHTML =
      '<h3 class="project-fit-subheader">Operator Alignment Profile</h3>' +
      '<p class="subsection-description">Structured geography, services, and experience fields for operator alignment snapshots. Long-text markets below remain for narrative context.</p>' +
      '<div class="form-grid">' +
      multiSelect(
        "activeCountries",
        fieldLabel("Active Countries"),
        "Countries where you have active or documented hotel operations.",
        o.activeCountries
      ) +
      multiSelect(
        "activeMarkets",
        fieldLabel("Active Markets / Cities"),
        "Primary cities or markets (structured).",
        o.activeMarkets
      ) +
      multiSelect(
        "marketPresenceType",
        fieldLabel("Market Presence Type"),
        "Whether presence is active ops, pipeline, or target market.",
        o.marketPresenceType
      ) +
      multiSelect(
        "serviceModelsSupported",
        fieldLabel("Service Models Supported"),
        "Hotel service models you operate.",
        o.serviceModelsSupported
      ) +
      multiSelect(
        "managementStructuresSupported",
        fieldLabel("Management Structures Supported"),
        "Deal structures / management models you support.",
        o.managementStructures
      ) +
      multiSelect(
        "offeredServices",
        fieldLabel("Offered Services"),
        "Services your platform offers owners.",
        o.operatorServices
      ) +
      singleSelect(
        "newBuildOpeningExperience",
        fieldLabel("New-Build Opening Experience"),
        "Documented new-build opening depth.",
        o.experienceLevel
      ) +
      singleSelect(
        "preOpeningSupportCapability",
        fieldLabel("Pre-Opening Support Capability"),
        "Pre-opening / transition support level.",
        o.preOpeningCapability
      ) +
      multiSelect(
        "brandFamiliesOperated",
        fieldLabel("Brand Families Operated"),
        "Major brand families (in addition to brand links).",
        o.brandFamilies
      ) +
      singleSelect(
        "conversionReflagExperience",
        fieldLabel("Conversion / Reflag Experience"),
        "Documented conversion or reflag depth.",
        o.experienceLevel
      ) +
      singleSelect(
        "softBrandLifestyleExperience",
        fieldLabel("Soft Brand / Lifestyle Experience"),
        "Soft brand or lifestyle operating depth.",
        o.experienceLevel
      ) +
      singleSelect(
        "fbCapabilityLevel",
        fieldLabel("F&B Capability Level"),
        "F&B operating capability.",
        o.fbCapability
      ) +
      singleSelect(
        "revenueManagementCapability",
        fieldLabel("Revenue Management Capability"),
        "RM platform depth.",
        o.revenueMgmt
      ) +
      multiSelect("salesPlatform", fieldLabel("Sales Platform"), "Sales / distribution platform.", o.salesPlatform) +
      singleSelect(
        "ownerReportingLevel",
        fieldLabel("Owner Reporting Level"),
        "Typical owner reporting depth.",
        o.ownerReportingLevel
      ) +
      singleSelect(
        "governanceCadence",
        fieldLabel("Governance Cadence"),
        "Typical owner governance cadence.",
        o.governanceCadence
      ) +
      '<div class="form-group"><label class="form-label">Minimum Key Count<span class="help-text">Smallest property size you typically accept.</span></label>' +
      '<input type="number" class="form-input" name="minimumKeyCount" min="0" step="1" placeholder="e.g. 80"></div>' +
      '<div class="form-group full-width"><label class="form-label">Similar Project Case Studies<span class="help-text">Short summary when case study records are not linked.</span></label>' +
      '<textarea class="form-textarea" name="similarProjectCaseStudies" rows="2" maxlength="2000" placeholder="Optional summary of relevant projects"></textarea></div>' +
      "</div>" +
      '<div class="form-grid oas-admin-fields" style="margin-top:16px;">' +
      '<p class="subsection-description full-width">Internal / admin (optional)</p>' +
      singleSelect(
        "dataConfidenceLevel",
        fieldLabel("Data Confidence Level"),
        "How this profile data was validated.",
        o.dataConfidence
      ) +
      multiSelect("sourceType", fieldLabel("Source Type"), "Where profile data came from.", o.sourceType) +
      '<div class="form-group"><label class="form-label">Last Updated Date</label>' +
      '<input type="date" class="form-input" name="lastUpdatedDate"></div>' +
      "</div>";
  }

  function run() {
    fetch("/fixtures/operator-alignment-field-options.json?v=proper-case-options-20260714")
      .then(function (r) {
        return r.json();
      })
      .then(function (o) {
        injectDealFields(o);
        injectOperatorFields(o);
      })
      .catch(function (e) {
        console.warn("[oas-inject-form-fields]", e);
      });
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", run);
  } else {
    run();
  }
})();
