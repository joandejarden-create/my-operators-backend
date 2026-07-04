/**
 * Partner Directory filter option lists — keep in sync with
 * lib/company-profile-owner-operator-fields.js (Operating Model, Third-Party Mgmt).
 */
(function () {
  const AIRTABLE_OPERATING_MODEL = "Operating Model";
  const AIRTABLE_THIRD_PARTY_MGMT = "Third-Party Management Availability";

  const REGION_FILTER_OPTIONS = [
    { value: "", label: "All Regions" },
    { value: "GLOBAL", label: "Global" },
    { value: "AMERICAS", label: "Americas" },
    { value: "CALA", label: "Caribbean & Latin America" },
    { value: "EUROPE", label: "Europe" },
    { value: "MEA", label: "Middle East & Africa" },
    { value: "AP", label: "Asia Pacific" },
  ];

  const OPERATING_MODEL_OPTIONS = [
    { value: "", label: "All Models" },
    { value: "Own-and-Operate Only", label: "Own-and-Operate Only" },
    { value: "Affiliated-Owned Hotels Only", label: "Affiliated-Owned Hotels Only" },
    { value: "Third-Party Management", label: "Third-Party Management" },
    { value: "Mixed Owner/Operator Model", label: "Mixed Owner/Operator Model" },
    { value: "Asset-Light Management Platform", label: "Asset-Light Management Platform" },
    { value: "Franchisee/Operator Model", label: "Franchisee/Operator Model" },
    { value: "Unknown / To Confirm", label: "Unknown / To Confirm" },
  ];

  const THIRD_PARTY_MGMT_OPTIONS = [
    { value: "", label: "All Availability" },
    { value: "Yes", label: "Yes" },
    { value: "No", label: "No" },
    { value: "Selectively", label: "Selectively" },
    { value: "Case-by-Case", label: "Case-by-Case" },
    { value: "Unknown / To Confirm", label: "Unknown / To Confirm" },
  ];

  const RESPONSIVENESS_SPEED_OPTIONS = [
    { value: "", label: "All Speeds" },
    { value: "Lightning Fast", label: "Lightning Fast" },
    { value: "Very Fast", label: "Very Fast" },
    { value: "Responsive", label: "Responsive" },
    { value: "Slow", label: "Slow" },
    { value: "Stalled", label: "Stalled" },
    { value: "Unresponsive", label: "Unresponsive" },
  ];

  const RESPONSIVENESS_FREQUENCY_OPTIONS = [
    { value: "", label: "All Frequencies" },
    { value: "Frequently", label: "Frequently" },
    { value: "Occasionally", label: "Occasionally" },
    { value: "Rarely", label: "Rarely" },
  ];

  const INSIGHTS_REGION_OPTIONS = REGION_FILTER_OPTIONS.map((o) => ({
    value: o.value === "" ? "all" : o.value,
    label: o.label,
  }));

  function normalizeFieldKey(key) {
    return String(key)
      .toLowerCase()
      .replace(/[\u2018\u2019\u2032]/g, "'");
  }

  function fieldFromRaw(rawFields, fieldName) {
    if (!rawFields || typeof rawFields !== "object") return "";
    if (rawFields[fieldName] != null && String(rawFields[fieldName]).trim() !== "") {
      return String(rawFields[fieldName]).trim();
    }
    const target = normalizeFieldKey(fieldName);
    for (const k of Object.keys(rawFields)) {
      if (normalizeFieldKey(k) !== target) continue;
      const v = rawFields[k];
      if (v != null && String(v).trim() !== "") return String(v).trim();
    }
    return "";
  }

  function operatingModelFromEntity(entity) {
    if (!entity) return "";
    if (entity.operatingModel) return String(entity.operatingModel).trim();
    return fieldFromRaw(entity.rawFields, AIRTABLE_OPERATING_MODEL);
  }

  function thirdPartyMgmtFromEntity(entity) {
    if (!entity) return "";
    if (entity.thirdPartyManagementAvailability) {
      return String(entity.thirdPartyManagementAvailability).trim();
    }
    return fieldFromRaw(entity.rawFields, AIRTABLE_THIRD_PARTY_MGMT);
  }

  function normalizeOperatingModelForFilter(value) {
    const v = String(value || "").trim();
    if (!v) return "";
    const opt = OPERATING_MODEL_OPTIONS.find((o) => o.value && o.value === v);
    if (opt) return opt.value;
    const lower = v.toLowerCase();
    for (const o of OPERATING_MODEL_OPTIONS) {
      if (!o.value) continue;
      if (o.value.toLowerCase() === lower) return o.value;
    }
    return v;
  }

  function normalizeThirdPartyMgmtForFilter(value) {
    const v = String(value || "").trim();
    if (!v) return "";
    if (v === "Case-by-case") return "Case-by-Case";
    const opt = THIRD_PARTY_MGMT_OPTIONS.find((o) => o.value && o.value === v);
    if (opt) return opt.value;
    const lower = v.toLowerCase();
    for (const o of THIRD_PARTY_MGMT_OPTIONS) {
      if (!o.value) continue;
      if (o.value.toLowerCase() === lower) return o.value;
    }
    return v;
  }

  function populateSelect(selectId, options) {
    const el = document.getElementById(selectId);
    if (!el) return;
    el.innerHTML = "";
    for (const opt of options) {
      const node = document.createElement("option");
      node.value = opt.value;
      node.textContent = opt.label;
      el.appendChild(node);
    }
  }

  function populateStaticFilterSelects() {
    populateSelect("regionFilter", REGION_FILTER_OPTIONS);
    populateSelect("operatingModelFilter", OPERATING_MODEL_OPTIONS);
    populateSelect("thirdPartyMgmtFilter", THIRD_PARTY_MGMT_OPTIONS);
    populateSelect("responsivenessSpeedFilter", RESPONSIVENESS_SPEED_OPTIONS);
    populateSelect("responsivenessFrequencyFilter", RESPONSIVENESS_FREQUENCY_OPTIONS);
  }

  function populateInsightsFilterSelects() {
    populateSelect("insightsRegion", INSIGHTS_REGION_OPTIONS);
  }

  window.DEALALITY_PARTNER_DIRECTORY_FILTERS = {
    REGION_FILTER_OPTIONS,
    OPERATING_MODEL_OPTIONS,
    THIRD_PARTY_MGMT_OPTIONS,
    RESPONSIVENESS_SPEED_OPTIONS,
    RESPONSIVENESS_FREQUENCY_OPTIONS,
    INSIGHTS_REGION_OPTIONS,
    operatingModelFromEntity,
    thirdPartyMgmtFromEntity,
    normalizeOperatingModelForFilter,
    normalizeThirdPartyMgmtForFilter,
    populateStaticFilterSelects,
    populateInsightsFilterSelects,
  };
})();
