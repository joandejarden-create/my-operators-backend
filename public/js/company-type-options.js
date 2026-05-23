/**
 * Browser mirror of lib/company-type-normalize.js — keep filter list in sync.
 */
(function () {
  const AIRTABLE_FIELD = "Company Type";

  const FILTER_TO_AIRTABLE = {
    "HOTEL MGMT. COMPANY": "Hotel Management Company",
    "HOTEL BRANDS (FRANCHISE)": "Hotel Brands (Franchise)",
    "HOTEL OWNERS": "Hotel Owner",
    "HOSPITALITY CONSULTANTS": "Hospitality Consultants",
    OTHER: "Other",
  };

  const AIRTABLE_TO_FILTER = Object.fromEntries(
    Object.entries(FILTER_TO_AIRTABLE).map(([k, v]) => [v, k])
  );

  const FILTER_OPTIONS = [
    { value: "", label: "All Types" },
    { value: "HOTEL MGMT. COMPANY", label: "3rd Party Operator" },
    { value: "HOTEL BRANDS (FRANCHISE)", label: "Hotel Brands (Franchise)" },
    { value: "HOTEL OWNERS", label: "Hotel Owners" },
    { value: "HOSPITALITY CONSULTANTS", label: "Advisor / Consultant" },
    { value: "OTHER", label: "Other" },
  ];

  const LABEL_BY_VALUE = Object.fromEntries(
    FILTER_OPTIONS.map((o) => [o.value, o.label])
  );

  function normalizeFieldKey(key) {
    return String(key)
      .toLowerCase()
      .replace(/[\u2018\u2019\u2032]/g, "'");
  }

  function getFieldValue(fields, fieldName) {
    if (!fields || typeof fields !== "object") return undefined;
    if (fields[fieldName] != null && String(fields[fieldName]).trim() !== "") {
      return fields[fieldName];
    }
    const target = normalizeFieldKey(fieldName);
    for (const k of Object.keys(fields)) {
      if (normalizeFieldKey(k) !== target) continue;
      const v = fields[k];
      if (v != null && String(v).trim() !== "") return v;
    }
    return undefined;
  }

  function normalizeToFilterKey(rawValue) {
    const raw = rawValue == null ? "" : String(rawValue).trim();
    if (!raw) return "";
    if (AIRTABLE_TO_FILTER[raw]) return AIRTABLE_TO_FILTER[raw];

    const upper = raw.toUpperCase();
    if (
      upper === "HOTEL OWNERS" ||
      upper === "HOTEL OWNER" ||
      upper === "OWNER" ||
      upper === "OWNERS"
    ) {
      return "HOTEL OWNERS";
    }
    if (
      upper.includes("BRAND") ||
      upper.includes("FRANCHISE") ||
      upper === "HOTEL BRANDS (FRANCHISE)"
    ) {
      return "HOTEL BRANDS (FRANCHISE)";
    }
    if (
      upper.includes("MGMT") ||
      upper.includes("MANAGEMENT") ||
      upper.includes("OPERATOR") ||
      upper === "3RD PARTY OPERATOR"
    ) {
      return "HOTEL MGMT. COMPANY";
    }
    if (
      upper.includes("CONSULTANT") ||
      upper.includes("ADVISOR") ||
      upper.includes("BROKER") ||
      upper.includes("HOSPITALITY")
    ) {
      return "HOSPITALITY CONSULTANTS";
    }
    if (upper.includes("LENDER") || upper.includes("LEGAL")) {
      return "HOSPITALITY CONSULTANTS";
    }
    if (upper === "OTHER") return "OTHER";
    if (upper.includes("OWNER")) return "HOTEL OWNERS";
    return "";
  }

  window.DEALALITY_COMPANY_TYPE = {
    airtableField: AIRTABLE_FIELD,
    filterOptions: FILTER_OPTIONS,
    normalizeToFilterKey,
    typeFromFields(fields) {
      const companyType = getFieldValue(fields, AIRTABLE_FIELD);
      const userType = getFieldValue(fields, "User Type");
      return normalizeToFilterKey(companyType || userType || "");
    },
    filterLabel(value) {
      const key = value == null ? "" : String(value).trim();
      return LABEL_BY_VALUE[key] ?? key;
    },
    populateFilterSelect(selectId) {
      const select = document.getElementById(selectId || "userTypeFilter");
      if (!select) return;
      const current = select.value;
      select.innerHTML = "";
      FILTER_OPTIONS.forEach((opt) => {
        const el = document.createElement("option");
        el.value = opt.value;
        el.textContent = opt.label;
        if (opt.value && FILTER_TO_AIRTABLE[opt.value]) {
          el.dataset.airtableValue = FILTER_TO_AIRTABLE[opt.value];
        }
        select.appendChild(el);
      });
      if (current && LABEL_BY_VALUE[current] !== undefined) {
        select.value = current;
      }
    },
    populateInsightsTypeSelect(selectId) {
      const select = document.getElementById(selectId || "insightsType");
      if (!select) return;
      const current = select.value;
      select.innerHTML = "";
      const allOpt = document.createElement("option");
      allOpt.value = "all";
      allOpt.textContent = "All Types";
      select.appendChild(allOpt);
      FILTER_OPTIONS.filter((o) => o.value).forEach((opt) => {
        const el = document.createElement("option");
        el.value = opt.value;
        el.textContent = opt.label;
        if (FILTER_TO_AIRTABLE[opt.value]) {
          el.dataset.airtableValue = FILTER_TO_AIRTABLE[opt.value];
        }
        select.appendChild(el);
      });
      if (current && (current === "all" || LABEL_BY_VALUE[current] !== undefined)) {
        select.value = current;
      }
    },
  };
})();
