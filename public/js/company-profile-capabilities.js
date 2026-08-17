/**
 * Company Settings — capability selection and Owner-Operator derivation (browser).
 * Keep in sync with lib/company-profile-owner-operator-fields.js
 */
(function () {
  const OWNER_OPERATOR_AIRTABLE = "Hotel Owner - Operator";

  const CAPABILITY_OPTIONS = [
    { id: "owns_assets", label: "We own or control hotel assets", tag: "Owns Hotels" },
    { id: "develops", label: "We develop hotels", tag: "Develops Hotels" },
    { id: "operates_own", label: "We operate hotels for our own portfolio", tag: "Operates Own Portfolio" },
    {
      id: "operates_affiliated",
      label: "We operate hotels for affiliated-owned hotels",
      tag: "Operates Affiliated-Owned Hotels",
    },
    {
      id: "operates_third_party",
      label: "We operate hotels for third-party owners",
      tag: "Operates Third-Party Hotels",
    },
    { id: "brand", label: "We are a hotel brand / franchisor", tag: "Brand / Franchisor" },
    { id: "investor", label: "We invest in hotel assets", tag: "Capital Provider" },
    {
      id: "advisor",
      label: "We advise, broker, finance, or support hotel transactions",
      tag: "Consultant / Advisor",
    },
  ];

  const FORM_TO_AIRTABLE = {
    Brand: "Hotel Brands (Franchise)",
    Operator: "Hotel Management Company",
    Owner: "Hotel Owner",
    owner_operator: OWNER_OPERATOR_AIRTABLE,
    Advisor: "Hospitality Consultants",
    Other: "Other",
  };

  const OPERATING_MODEL_OPTIONS = [
    "Own-and-Operate Only",
    "Affiliated-Owned Hotels Only",
    "Third-Party Management",
    "Mixed Owner/Operator Model",
    "Asset-Light Management Platform",
    "Franchisee/Operator Model",
    "Unknown / To Confirm",
  ];

  const THIRD_PARTY_OPTIONS = ["Yes", "No", "Selectively", "Case-by-Case", "Unknown / To Confirm"];

  const OWN_TAGS = { "Owns Hotels": true, "Develops Hotels": true };
  const OPERATE_TAGS = {
    "Operates Own Portfolio": true,
    "Operates Affiliated-Owned Hotels": true,
    "Operates Third-Party Hotels": true,
  };

  function capabilitiesToTags(ids) {
    const set = new Set((ids || []).map(String));
    const tags = [];
    CAPABILITY_OPTIONS.forEach(function (opt) {
      if (set.has(opt.id)) tags.push(opt.tag);
    });
    return tags;
  }

  function deriveFromTags(tags) {
    const list = tags || [];
    const owns = list.some(function (t) { return OWN_TAGS[t]; });
    const operates = list.some(function (t) { return OPERATE_TAGS[t]; });
    const isBrand = list.indexOf("Brand / Franchisor") !== -1;
    const isAdvisorOnly =
      list.indexOf("Consultant / Advisor") !== -1 &&
      !owns &&
      !operates &&
      !isBrand &&
      list.indexOf("Capital Provider") === -1;

    if (owns && operates) {
      return {
        companyTypeForm: "owner_operator",
        companyTypeDisplay: OWNER_OPERATOR_AIRTABLE,
        workspaceAccess: ["Owner", "Operator"],
      };
    }
    if (isBrand) {
      return {
        companyTypeForm: "Brand",
        companyTypeDisplay: FORM_TO_AIRTABLE.Brand,
        workspaceAccess: ["Brand"],
      };
    }
    if (operates && !owns) {
      return {
        companyTypeForm: "Operator",
        companyTypeDisplay: FORM_TO_AIRTABLE.Operator,
        workspaceAccess: ["Operator"],
      };
    }
    if (owns && !operates) {
      return {
        companyTypeForm: "Owner",
        companyTypeDisplay: FORM_TO_AIRTABLE.Owner,
        workspaceAccess: ["Owner"],
      };
    }
    if (isAdvisorOnly) {
      return {
        companyTypeForm: "Advisor",
        companyTypeDisplay: FORM_TO_AIRTABLE.Advisor,
        workspaceAccess: [],
      };
    }
    return {
      companyTypeForm: "Other",
      companyTypeDisplay: FORM_TO_AIRTABLE.Other,
      workspaceAccess: [],
    };
  }

  function suggestThirdParty(tags, existing, operatingModel) {
    if (existing) return existing;
    if (
      operatingModel === "Mixed Owner/Operator Model" ||
      operatingModel === "Mixed owner/operator model"
    ) {
      return "Case-by-Case";
    }
    if (tags.indexOf("Operates Third-Party Hotels") !== -1) return "Yes";
    if (
      tags.indexOf("Operates Own Portfolio") !== -1 ||
      tags.indexOf("Operates Affiliated-Owned Hotels") !== -1
    ) {
      if (tags.indexOf("Operates Third-Party Hotels") === -1) return "No";
    }
    return "";
  }

  function companyTypeDisplayFromFormKey(key) {
    return FORM_TO_AIRTABLE[key] || key || "";
  }

  window.DEALALITY_COMPANY_CAPABILITIES = {
    CAPABILITY_OPTIONS: CAPABILITY_OPTIONS,
    OPERATING_MODEL_OPTIONS: OPERATING_MODEL_OPTIONS,
    THIRD_PARTY_OPTIONS: THIRD_PARTY_OPTIONS,
    capabilitiesToTags: capabilitiesToTags,
    deriveFromTags: deriveFromTags,
    suggestThirdParty: suggestThirdParty,
    companyTypeDisplayFromFormKey: companyTypeDisplayFromFormKey,
    populateOperatingModelSelect: function (selectId) {
      var sel = document.getElementById(selectId || "operatingModel");
      if (!sel || sel.options.length > 1) return;
      OPERATING_MODEL_OPTIONS.forEach(function (v) {
        var o = document.createElement("option");
        o.value = v;
        o.textContent = v;
        sel.appendChild(o);
      });
    },
    populateThirdPartySelect: function (selectId) {
      var sel = document.getElementById(selectId || "thirdPartyManagementAvailability");
      if (!sel || sel.options.length > 1) return;
      THIRD_PARTY_OPTIONS.forEach(function (v) {
        var o = document.createElement("option");
        o.value = v;
        o.textContent = v;
        sel.appendChild(o);
      });
    },
    renderCapabilityCheckboxes: function (containerId) {
      var root = document.getElementById(containerId || "companyCapabilitiesGroup");
      if (!root) return;
      root.innerHTML = "";
      CAPABILITY_OPTIONS.forEach(function (opt) {
        var wrap = document.createElement("div");
        wrap.className = "checkbox-item";
        var id = "cap-" + opt.id;
        var input = document.createElement("input");
        input.type = "checkbox";
        input.id = id;
        input.name = "companyCapabilities";
        input.value = opt.id;
        var label = document.createElement("label");
        label.setAttribute("for", id);
        label.textContent = opt.label;
        wrap.appendChild(input);
        wrap.appendChild(label);
        root.appendChild(wrap);
      });
    },
    getSelectedCapabilityIds: function () {
      return Array.from(
        document.querySelectorAll('input[name="companyCapabilities"]:checked')
      ).map(function (el) { return el.value; });
    },
    setSelectedCapabilityIds: function (ids) {
      var set = new Set((ids || []).map(String));
      document.querySelectorAll('input[name="companyCapabilities"]').forEach(function (el) {
        el.checked = set.has(el.value);
      });
    },
  };
})();
