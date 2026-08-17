/**
 * Conversion Financing Package — My Deals modal (Request Financing).
 */
(function (global) {
  "use strict";

  var OPTIONS = {
    capitalNeedType: [
      "Brand Conversion / Reflagging",
      "PIP / Renovation Financing",
      "Refinance Plus Capex",
      "Acquisition Plus Conversion",
      "Repositioning Capital",
      "Operator Transition Financing",
      "Development / Construction Financing",
      "Working Capital / Stabilization",
      "Other",
    ],
    useOfProceeds: [
      "PIP / Brand Standards",
      "Guestroom Renovation",
      "Public Area Renovation",
      "FF&E",
      "Signage / Exterior",
      "Systems / Technology",
      "Soft Costs",
      "Working Capital",
      "Interest Reserve",
      "Existing Debt Payoff",
      "Acquisition Closing",
      "Contingency",
      "Other",
    ],
    capitalUnlock: [
      "Brand Conversion",
      "Renovation Completion",
      "Higher Positioning",
      "Operator Transition",
      "Stabilization",
      "Acquisition Closing",
      "Refinance / Maturity Solution",
      "Sale Preparation",
      "Ownership Preservation",
      "Other",
    ],
    capitalAssetStatus: [
      "Operating Hotel",
      "Closed Hotel",
      "Under Renovation",
      "Under Contract / Acquisition Target",
      "Development Site",
      "Mixed-Use Project",
      "Distressed / Underperforming",
      "Other",
    ],
    capitalBrandStatus: [
      "Independent",
      "Currently Branded",
      "Brand Agreement Expiring",
      "Brand Conversion Under Review",
      "Brand Contacted",
      "LOI / Application Stage",
      "Franchise Approval Received",
      "Unknown / Not Applicable",
    ],
    pipCapexEstimateStatus: [
      "Not Estimated",
      "Owner Estimate",
      "Brand-Provided PIP",
      "Architect / Contractor Estimate",
      "Final Budget Available",
    ],
    capitalOperatorStatus: [
      "Existing Operator Staying",
      "Existing Operator Under Review",
      "New Operator Being Evaluated",
      "Operator Not Selected",
      "Owner-Operated",
      "Not Applicable",
    ],
    capitalFinancialsAvailability: [
      "Yes, TTM Available",
      "Yes, 2–3 Years Available",
      "Partial Financials",
      "Not Available",
      "Not Yet Uploaded",
    ],
    existingDebtStatus: [
      "No Existing Debt",
      "Existing Debt in Place",
      "Debt Maturity Approaching",
      "Refinance Required",
      "Unknown / Not Disclosed",
    ],
    ownerEquityContributionStatus: [
      "Available",
      "Partially Available",
      "Not Yet Determined",
      "Not Disclosed",
    ],
    capitalTiming: ["Immediate", "30–60 Days", "3–6 Months", "6–12 Months", "Early Planning"],
    capitalCurrency: ["USD", "EUR", "MXN", "DOP", "COP", "BRL", "CLP", "PEN", "Local Currency", "Other"],
    capitalSharingPreference: [
      "Save Internally Only",
      "Share Anonymized Summary With Matched Capital Providers",
      "Share Named Opportunity Only After Owner Approval",
      "Share Documents Only After NDA / Permission",
    ],
    capitalSharingStatus: [
      "Draft",
      "Internal Review",
      "Owner Approved Summary",
      "Shared Anonymously",
      "Intro Approved",
      "NDA / Confidential Review",
      "Archived",
    ],
    supportingDocumentsAvailable: [
      "TTM P&L",
      "2–3 Years P&L",
      "Capex / PIP Budget",
      "Existing Debt Summary",
      "Property Photos",
      "Brand Correspondence",
      "Operator Proposal",
      "Appraisal / Valuation",
      "STR / Market Data",
      "Purchase Agreement",
      "Construction Budget",
      "None Yet",
    ],
  };

  var overlay = null;
  var bodyEl = null;
  var state = { dealId: null, inputs: {}, snapshot: null, sharingStatus: "Draft" };

  function esc(v) {
    return String(v == null ? "" : v)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;");
  }

  function fetchApi(url, opts) {
    if (global.DealalityMemberstackAuth && global.DealalityMemberstackAuth.fetchMyDealsApi) {
      return global.DealalityMemberstackAuth.fetchMyDealsApi(url, opts);
    }
    return fetch(url, opts);
  }

  function selectField(key, label, required) {
    var opts = OPTIONS[key] || [];
    var val = state.inputs[key] || "";
    return (
      '<div class="cfp-field">' +
      "<label>" +
      esc(label) +
      (required ? " *" : "") +
      "</label>" +
      '<select data-field="' +
      esc(key) +
      '">' +
      '<option value="">Select…</option>' +
      opts
        .map(function (o) {
          return (
            '<option value="' + esc(o) + '"' + (val === o ? " selected" : "") + ">" + esc(o) + "</option>"
          );
        })
        .join("") +
      "</select></div>"
    );
  }

  function textField(key, label, placeholder) {
    var val = state.inputs[key] || "";
    return (
      '<div class="cfp-field">' +
      "<label>" + esc(label) + "</label>" +
      '<input type="text" data-field="' + esc(key) + '" value="' + esc(val) + '" placeholder="' + esc(placeholder || "") + '">' +
      "</div>"
    );
  }

  function checklistField(key, label) {
    var selected = state.inputs[key] || [];
    if (!Array.isArray(selected)) selected = [];
    var items = OPTIONS[key] || [];
    return (
      '<div class="cfp-field full">' +
      "<label>" + esc(label) + "</label>" +
      '<div class="cfp-checklist" data-checklist="' + esc(key) + '">' +
      items
        .map(function (o) {
          var checked = selected.indexOf(o) >= 0 ? " checked" : "";
          return (
            "<label><input type=\"checkbox\" data-check=\"" +
            esc(key) +
            '" value="' +
            esc(o) +
            '"' +
            checked +
            "> " +
            esc(o) +
            "</label>"
          );
        })
        .join("") +
      "</div></div>"
    );
  }

  function renderForm() {
    return (
      '<p class="helper" style="margin:0 0 14px;color:var(--neutral--400);font-size:13px;">Structure a financing request for this hotel opportunity. Dealality helps organize what the capital would unlock, how it connects to the asset strategy, and what a capital provider may need to evaluate.</p>' +
      '<div class="cfp-form-grid">' +
      selectField("capitalNeedType", "Financing Need Type", true) +
      textField("capitalAmount", "Capital Amount Requested", "e.g. 10000000") +
      textField("capitalAmountRange", "Or Amount Range", "e.g. $8M – $12M") +
      selectField("capitalCurrency", "Currency") +
      checklistField("useOfProceeds", "Use of Proceeds") +
      '<div class="cfp-field full"><label>What Would This Capital Help Unlock?</label><p class="helper">Examples include a brand conversion, PIP completion, renovation, operator transition, acquisition closing, refinance, or repositioning.</p>' +
      (function () {
        var selected = state.inputs.capitalUnlock || [];
        return (
          '<div class="cfp-checklist" data-checklist="capitalUnlock">' +
          OPTIONS.capitalUnlock
            .map(function (o) {
              return (
                "<label><input type=\"checkbox\" data-check=\"capitalUnlock\" value=\"" +
                esc(o) +
                '"' +
                (selected.indexOf(o) >= 0 ? " checked" : "") +
                "> " +
                esc(o) +
                "</label>"
              );
            })
            .join("") +
          "</div></div>"
        );
      })() +
      selectField("capitalAssetStatus", "Current Asset Status") +
      selectField("capitalBrandStatus", "Current Brand Status") +
      textField("targetBrandPath", "Target Brand Path", "Brand or conversion path") +
      textField("pipCapexEstimate", "PIP / Renovation Estimate", "Amount or range") +
      selectField("pipCapexEstimateStatus", "PIP / Renovation Status") +
      selectField("capitalOperatorStatus", "Operator Status") +
      selectField("capitalFinancialsAvailability", "Historical Financials Available?") +
      selectField("existingDebtStatus", "Existing Debt Status") +
      selectField("ownerEquityContributionStatus", "Owner Equity Contribution") +
      selectField("capitalTiming", "Desired Timing") +
      selectField("capitalSharingPreference", "Confidentiality / Sharing Preference") +
      checklistField("supportingDocumentsAvailable", "Supporting Documents Available") +
      "</div>"
    );
  }

  function renderPackage() {
    if (!state.snapshot) return "";
    var sections = [
      state.snapshot.opportunitySummary,
      state.snapshot.capitalRequest,
      state.snapshot.conversionRepositioningThesis,
      state.snapshot.brandOperatorContext,
      state.snapshot.pipCapexScope,
      state.snapshot.financialBaseline,
      state.snapshot.capitalStackContext,
      state.snapshot.executionDependencies,
      state.snapshot.capitalProviderReviewLens,
      state.snapshot.riskFlagsAndOpenQuestions,
      state.snapshot.ownerSharingControls,
    ];
    var html =
      '<div class="cfp-package"><div class="cfp-chip-row">' +
      '<span class="cfp-chip">Conversion Financing Package</span>' +
      '<span class="cfp-chip">Platform-Derived</span>' +
      '<span class="cfp-chip">Sharing: ' + esc(state.sharingStatus || "Draft") + "</span></div>";
    sections.forEach(function (sec) {
      if (!sec) return;
      html += '<div class="cfp-section"><h3>' + esc(sec.headline) + "</h3>";
      if (sec.summary) html += "<p>" + esc(sec.summary) + "</p>";
      if (sec.items && sec.items.length) {
        html += "<ul>";
        sec.items.forEach(function (item) {
          if (typeof item === "string") html += "<li>" + esc(item) + "</li>";
          else if (item && item.question) html += "<li>" + esc(item.item) + ": " + esc(item.question) + "</li>";
          else if (item && item.label) html += "<li><strong>" + esc(item.label) + "</strong> — " + esc(item.rationale) + "</li>";
        });
        html += "</ul>";
      }
      if (sec.disclaimer) html += '<p class="cfp-disclaimer">' + esc(sec.disclaimer) + "</p>";
      html += "</div>";
    });
    html +=
      '<p class="cfp-disclaimer">Your financing request is private by default. You control whether an anonymized summary or named opportunity is shared with capital providers.</p>' +
      '<div class="cfp-actions">' +
      '<label style="font-size:13px;color:var(--neutral--300);display:flex;align-items:center;gap:8px;">Sharing Status <select id="cfpSharingStatusSelect">' +
      OPTIONS.capitalSharingStatus
        .map(function (o) {
          return '<option value="' + esc(o) + '"' + (state.sharingStatus === o ? " selected" : "") + ">" + esc(o) + "</option>";
        })
        .join("") +
      "</select></label>" +
      '<button type="button" class="cfp-btn cfp-btn-secondary" id="cfpSaveSharingBtn">Update Sharing</button>' +
      "</div></div>";
    return html;
  }

  function collectInputsFromDom() {
    var inputs = Object.assign({}, state.inputs);
    bodyEl.querySelectorAll("[data-field]").forEach(function (el) {
      inputs[el.getAttribute("data-field")] = el.value;
    });
    Object.keys(OPTIONS).forEach(function (key) {
      if (!Array.isArray(OPTIONS[key]) || key.indexOf("capitalUnlock") === 0 && key === "capitalUnlock") {
        /* checklist keys below */
      }
    });
    ["useOfProceeds", "capitalUnlock", "supportingDocumentsAvailable"].forEach(function (key) {
      var vals = [];
      bodyEl.querySelectorAll('input[data-check="' + key + '"]:checked').forEach(function (cb) {
        vals.push(cb.value);
      });
      inputs[key] = vals;
    });
    inputs.capitalSharingStatus = state.sharingStatus || "Draft";
    return inputs;
  }

  function setLoading(message) {
    bodyEl.innerHTML = '<div class="cfp-state" role="status">' + esc(message || "Loading…") + "</div>";
  }

  function setError(message) {
    bodyEl.innerHTML = '<div class="cfp-state cfp-state-error">' + esc(message) + "</div>";
  }

  function bindFormActions() {
    var saveBtn = bodyEl.querySelector("#cfpSaveBtn");
    var genBtn = bodyEl.querySelector("#cfpGenerateBtn");
    if (saveBtn) {
      saveBtn.addEventListener("click", function () {
        saveBtn.disabled = true;
        var inputs = collectInputsFromDom();
        fetchApi("/api/deals/" + encodeURIComponent(state.dealId) + "/conversion-financing-package/save-inputs", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ inputs: inputs }),
        })
          .then(function (r) {
            return r.json().then(function (d) {
              return { ok: r.ok, data: d };
            });
          })
          .then(function (res) {
            if (!res.ok || !res.data.ok) throw new Error((res.data && res.data.error) || "Save failed");
            state.inputs = res.data.inputs || inputs;
            state.sharingStatus = res.data.sharingStatus || "Draft";
            renderBody();
          })
          .catch(function (err) {
            setError(err.message || "Could not save financing request.");
          })
          .finally(function () {
            saveBtn.disabled = false;
          });
      });
    }
    if (genBtn) {
      genBtn.addEventListener("click", function () {
        genBtn.disabled = true;
        setLoading("Generating Conversion Financing Package…");
        var inputs = collectInputsFromDom();
        fetchApi("/api/deals/" + encodeURIComponent(state.dealId) + "/conversion-financing-package/generate", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ inputs: inputs }),
        })
          .then(function (r) {
            return r.json().then(function (d) {
              return { ok: r.ok, data: d };
            });
          })
          .then(function (res) {
            if (!res.ok || !res.data.ok) throw new Error((res.data && res.data.error) || "Generate failed");
            state.inputs = res.data.inputs || inputs;
            state.snapshot = res.data.snapshot;
            state.sharingStatus = res.data.sharingStatus || "Draft";
            renderBody();
          })
          .catch(function (err) {
            setError(err.message || "Could not generate package.");
          })
          .finally(function () {
            genBtn.disabled = false;
          });
      });
    }
    var sharingBtn = bodyEl.querySelector("#cfpSaveSharingBtn");
    if (sharingBtn) {
      sharingBtn.addEventListener("click", function () {
        var sel = bodyEl.querySelector("#cfpSharingStatusSelect");
        var sharingStatus = sel ? sel.value : state.sharingStatus;
        fetchApi("/api/deals/" + encodeURIComponent(state.dealId) + "/conversion-financing-package/sharing", {
          method: "PATCH",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ sharingStatus: sharingStatus }),
        })
          .then(function (r) {
            return r.json().then(function (d) {
              return { ok: r.ok, data: d };
            });
          })
          .then(function (res) {
            if (!res.ok || !res.data.ok) throw new Error((res.data && res.data.error) || "Update failed");
            state.sharingStatus = res.data.sharingStatus;
            renderBody();
          })
          .catch(function (err) {
            alert(err.message || "Could not update sharing status.");
          });
      });
    }
  }

  function renderBody() {
    bodyEl.innerHTML =
      renderForm() +
      '<div class="cfp-actions">' +
      '<button type="button" class="cfp-btn cfp-btn-secondary" id="cfpSaveBtn">Save Request</button>' +
      '<button type="button" class="cfp-btn cfp-btn-primary" id="cfpGenerateBtn">Generate Package</button>' +
      "</div>" +
      renderPackage();
    bindFormActions();
  }

  function openForDeal(dealId) {
    if (!overlay) return;
    state = { dealId: dealId, inputs: {}, snapshot: null, sharingStatus: "Draft" };
    overlay.classList.add("is-open");
    overlay.setAttribute("aria-hidden", "false");
    setLoading("Loading financing request…");
    fetchApi("/api/deals/" + encodeURIComponent(dealId) + "/conversion-financing-package", { method: "GET" })
      .then(function (r) {
        return r.json().then(function (d) {
          return { ok: r.ok, data: d };
        });
      })
      .then(function (res) {
        if (!res.ok || !res.data.ok) throw new Error((res.data && res.data.error) || "Load failed");
        if (res.data.package) {
          state.inputs = res.data.package.inputs || {};
          state.snapshot = res.data.package.snapshot;
          state.sharingStatus = res.data.package.sharingStatus || "Draft";
        }
        renderBody();
      })
      .catch(function (err) {
        setError(err.message || "Could not load financing request.");
      });
  }

  function closeModal() {
    if (!overlay) return;
    overlay.classList.remove("is-open");
    overlay.setAttribute("aria-hidden", "true");
  }

  function init() {
    overlay = document.getElementById("cfpModalOverlay");
    bodyEl = document.getElementById("cfpModalBody");
    if (!overlay || !bodyEl) return;
    var closeBtn = document.getElementById("cfpModalClose");
    if (closeBtn) closeBtn.addEventListener("click", closeModal);
    overlay.addEventListener("click", function (e) {
      if (e.target === overlay) closeModal();
    });
  }

  global.DealalityConversionFinancingPackage = {
    init: init,
    openForDeal: openForDeal,
    close: closeModal,
  };

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }
})(window);
