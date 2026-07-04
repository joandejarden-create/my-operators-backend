/**
 * Deal Setup / New Deal Setup — operator capability P0 visibility (client-side).
 */
(function (global) {
  function strVal(v) {
    if (v == null || v === "") return "";
    if (typeof v === "string") return v.trim();
    if (Array.isArray(v)) return v.map(String).filter(Boolean).join(", ");
    return String(v).trim();
  }

  function isOperatorInScope(values) {
    var f = values || {};
    var bids = strVal(f["Who should receive bids for this project?"] || f["Who Should Receive Bids for This Project?"]).toLowerCase();
    if (bids.indexOf("third-party operators only") >= 0 || bids.indexOf("both brands") >= 0) return true;
    var preferred = strVal(f["Preferred Future Operating Model"]);
    if (/third.party|brand \+ third/i.test(preferred) && !/franchise\/license only/i.test(preferred)) return true;
    var plan = strVal(f["Plan to Self-Manage or Hire Third Party?"]);
    if (/third.party|third-party managed/i.test(plan)) return true;
    var dealStruct = strVal(f["Preferred Deal Structure"]);
    if (/third.party management|brand \+ third/i.test(dealStruct)) return true;
    return false;
  }

  function setBlockVisible(block, visible) {
    if (!block) return;
    block.classList.toggle("hidden", !visible);
    block.querySelectorAll(".ocs-required-when-visible").forEach(function (label) {
      var req = label.querySelector(".required");
      if (!req) return;
      if (visible) req.classList.remove("ocs-required-suppressed");
      else req.classList.add("ocs-required-suppressed");
    });
  }

  function updateVisibility(form) {
    if (!form) return;
    try {
      var values = {};
      form.querySelectorAll("[name]").forEach(function (el) {
        var n = el.getAttribute("name");
        if (!n) return;
        if (el.type === "file") return;
        if (el.type === "checkbox") {
          if (!values[n]) values[n] = [];
          if (el.checked) values[n].push(el.value);
        } else if (el.tagName === "SELECT" && el.multiple) {
          values[n] = Array.from(el.selectedOptions || []).map(function (o) {
            return o.value;
          });
        } else {
          values[n] = el.value;
        }
      });
      var inScope = isOperatorInScope(values);
      setBlockVisible(form.querySelector("#operatorCapabilityP0Block"), inScope);
      setBlockVisible(form.querySelector("#operatorReportingP0Block"), inScope);
      var freqLabel = form.querySelector('[data-ocs-field="Owner Reporting Frequency"] .form-label .required');
      if (freqLabel) freqLabel.classList.toggle("ocs-required-suppressed", !inScope);
    } catch (err) {
      console.error("[operator-capability-intake] updateVisibility failed:", err);
    }
  }

  var LEGACY_PROJECT_TYPE_VALUES = [
    "Renovation / repositioning (open hotel)",
    "Land / greenfield only",
    "Acquisition of operating hotel",
    "Conversion",
  ];

  function ensureProjectTypeSelectLegacyOption(form) {
    if (!form) return;
    var sel = form.querySelector('[name="Project Type"]');
    if (!sel || !sel.value) return;
    var v = sel.value.trim();
    var found = false;
    for (var i = 0; i < sel.options.length; i++) {
      if (sel.options[i].value === v) {
        found = true;
        break;
      }
    }
    if (!found) {
      var opt = document.createElement("option");
      opt.value = v;
      opt.textContent = v + " (saved value)";
      opt.setAttribute("data-legacy-project-type", "1");
      sel.appendChild(opt);
      sel.value = v;
    }
  }

  global.OperatorCapabilityIntake = {
    isOperatorInScope: isOperatorInScope,
    updateVisibility: updateVisibility,
    ensureProjectTypeSelectLegacyOption: ensureProjectTypeSelectLegacyOption,
    LEGACY_PROJECT_TYPE_VALUES: LEGACY_PROJECT_TYPE_VALUES,
  };
})(typeof window !== "undefined" ? window : globalThis);
