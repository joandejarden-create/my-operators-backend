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
    var html = includeEmpty ? '<option value="">Select one…</option>' : "";
    (list || []).forEach(function (v) {
      html += '<option value="' + esc(v) + '">' + esc(v) + "</option>";
    });
    return html;
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
    var strat = document.getElementById("oasOperatorStrategyInject");
    if (strat) {
      strat.innerHTML =
        '<h3 class="project-fit-subheader">Operator strategy &amp; requirements</h3>' +
        '<p class="subsection-description">Structured inputs for operator alignment review. Legacy fields above remain for compatibility.</p>' +
        '<div class="form-grid">' +
        singleSelect(
          "Operator Review Status",
          "Operator review status",
          "Where you are in operator review workflow (separate from strategy status).",
          o.operatorReviewStatus
        ) +
        multiSelect(
          "Preferred Management Structure",
          "Preferred management structure",
          "How you expect the hotel to be operated (not the same as franchise-only brand economics).",
          o.preferredManagementStructure
        ) +
        multiSelect(
          "Required Operator Services",
          "Required operator services",
          "Services you expect from an operator (standardized list).",
          o.operatorServices
        ) +
        multiSelect(
          "Must-Have Operator Services",
          "Must-have operator services",
          "Non-negotiable operator services.",
          o.operatorServices
        ) +
        multiSelect(
          "Nice-to-Have Operator Services",
          "Nice-to-have operator services",
          "Optional operator services.",
          o.operatorServices
        ) +
        singleSelect(
          "Market Presence Requirement",
          "Market presence requirement",
          "How local/country operator presence should be validated.",
          o.marketPresenceRequirement
        ) +
        singleSelect(
          "Pre-Opening Support Needed",
          "Pre-opening support needed",
          "Whether pre-opening operator support is in scope.",
          o.preOpeningSupportNeeded
        ) +
        singleSelect(
          "Owner Reporting Expectations",
          "Owner reporting expectations",
          "Reporting depth you expect from an operator.",
          o.ownerReportingExpectations
        ) +
        singleSelect(
          "Brand / Operator Responsibility Split",
          "Brand / operator responsibility split",
          "How brand and operator roles may be divided.",
          o.brandOperatorSplit
        ) +
        singleSelect(
          "Owner Control Preference",
          "Owner control preference",
          "How much control the owner expects to retain.",
          o.ownerControlPreference
        ) +
        "</div>";
    }

    var brand = document.getElementById("oasBrandAgreementInject");
    if (brand) {
      brand.innerHTML =
        '<h3 class="project-fit-subheader">Brand &amp; agreement structure</h3>' +
        '<p class="subsection-description">Clarifies brand affiliation vs operating model. Does not replace Preferred Deal Structure above.</p>' +
        '<div class="form-grid">' +
        singleSelect(
          "Brand Agreement Structure",
          "Brand agreement structure",
          "Intended brand agreement type (franchise, management, etc.).",
          o.brandAgreementStructure
        ) +
        singleSelect(
          "Operating Model",
          "Operating model (target)",
          "Who is expected to run the hotel day-to-day.",
          o.dealOperatingModel
        ) +
        multiSelect(
          "Operator Scope",
          "Operator scope",
          "Which operator functions are in scope.",
          o.operatorScope
        ) +
        "</div>";
    }

    var ops = document.getElementById("oasOperationsCommercialInject");
    if (ops) {
      ops.innerHTML =
        '<h3 class="project-fit-subheader">Operations &amp; commercial requirements</h3>' +
        '<div class="form-grid">' +
        singleSelect("F&B Complexity", "F&B complexity", "Relative F&B scope for operator fit.", o.fbCapability) +
        multiSelect(
          "Commercial Priority",
          "Commercial priority",
          "Commercial areas that matter most on this deal.",
          o.commercialPriority
        ) +
        singleSelect(
          "Local Labor / HR Support Needed",
          "Local labor / HR support needed",
          "Whether operator should provide HR/labor support.",
          o.yesNoNa
        ) +
        singleSelect(
          "Procurement Support Needed",
          "Procurement support needed",
          "Whether operator procurement support is needed.",
          o.yesNoNa
        ) +
        singleSelect(
          "Owner Internal Ops Capability",
          "Owner internal ops capability",
          "Strength of owner’s in-house hotel operations team.",
          o.ownerInternalOps
        ) +
        singleSelect(
          "Opening Timeline",
          "Opening timeline",
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
      '<h3 class="project-fit-subheader">Operator alignment profile</h3>' +
      '<p class="subsection-description">Structured geography, services, and experience fields for operator alignment snapshots. Long-text markets below remain for narrative context.</p>' +
      '<div class="form-grid">' +
      multiSelect(
        "activeCountries",
        "Active countries",
        "Countries where you have active or documented hotel operations.",
        o.activeCountries
      ) +
      multiSelect(
        "activeMarkets",
        "Active markets / cities",
        "Primary cities or markets (structured).",
        o.activeMarkets
      ) +
      multiSelect(
        "marketPresenceType",
        "Market presence type",
        "Whether presence is active ops, pipeline, or target market.",
        o.marketPresenceType
      ) +
      multiSelect(
        "serviceModelsSupported",
        "Service models supported",
        "Hotel service models you operate.",
        o.serviceModelsSupported
      ) +
      multiSelect(
        "managementStructuresSupported",
        "Management structures supported",
        "Deal structures / management models you support.",
        o.managementStructures
      ) +
      multiSelect(
        "offeredServices",
        "Offered services",
        "Services your platform offers owners.",
        o.operatorServices
      ) +
      singleSelect(
        "newBuildOpeningExperience",
        "New-build opening experience",
        "Documented new-build opening depth.",
        o.experienceLevel
      ) +
      singleSelect(
        "preOpeningSupportCapability",
        "Pre-opening support capability",
        "Pre-opening / transition support level.",
        o.preOpeningCapability
      ) +
      multiSelect(
        "brandFamiliesOperated",
        "Brand families operated",
        "Major brand families (in addition to brand links).",
        o.brandFamilies
      ) +
      singleSelect(
        "conversionReflagExperience",
        "Conversion / reflag experience",
        "Documented conversion or reflag depth.",
        o.experienceLevel
      ) +
      singleSelect(
        "softBrandLifestyleExperience",
        "Soft brand / lifestyle experience",
        "Soft brand or lifestyle operating depth.",
        o.experienceLevel
      ) +
      singleSelect(
        "fbCapabilityLevel",
        "F&B capability level",
        "F&B operating capability.",
        o.fbCapability
      ) +
      singleSelect(
        "revenueManagementCapability",
        "Revenue management capability",
        "RM platform depth.",
        o.revenueMgmt
      ) +
      multiSelect("salesPlatform", "Sales platform", "Sales / distribution platform.", o.salesPlatform) +
      singleSelect(
        "ownerReportingLevel",
        "Owner reporting level",
        "Typical owner reporting depth.",
        o.ownerReportingLevel
      ) +
      singleSelect(
        "governanceCadence",
        "Governance cadence",
        "Typical owner governance cadence.",
        o.governanceCadence
      ) +
      '<div class="form-group"><label class="form-label">Minimum key count<span class="help-text">Smallest property size you typically accept.</span></label>' +
      '<input type="number" class="form-input" name="minimumKeyCount" min="0" step="1" placeholder="e.g. 80"></div>' +
      '<div class="form-group full-width"><label class="form-label">Similar project case studies<span class="help-text">Short summary when case study records are not linked.</span></label>' +
      '<textarea class="form-textarea" name="similarProjectCaseStudies" rows="2" maxlength="2000" placeholder="Optional summary of relevant projects"></textarea></div>' +
      "</div>" +
      '<div class="form-grid oas-admin-fields" style="margin-top:16px;">' +
      '<p class="subsection-description full-width">Internal / admin (optional)</p>' +
      singleSelect(
        "dataConfidenceLevel",
        "Data confidence level",
        "How this profile data was validated.",
        o.dataConfidence
      ) +
      multiSelect("sourceType", "Source type", "Where profile data came from.", o.sourceType) +
      '<div class="form-group"><label class="form-label">Last updated date</label>' +
      '<input type="date" class="form-input" name="lastUpdatedDate"></div>' +
      "</div>";
  }

  function run() {
    fetch("/fixtures/operator-alignment-field-options.json")
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
