/**

 * Browser mirror of lib/company-role-normalize.js (Partner Directory + Company Settings).

 * Keep in sync with PARTNER_DIRECTORY_COMPANY_ROLE_FILTERS and Airtable choices there.

 */

(function () {

  const AIRTABLE_FIELD = "Company's role in the hotel ecosystem";



  const FORM_DISPLAY_LABELS = {

    Brand: "We represent a hotel brand (franchise/licensing platform)",

    Operator: "We operate hotels under third-party brands (operator only)",

    Both: "We both represent a brand and operate hotels",

    Owner: "We are an owner, developer, or investor",

    Advisor: "We are a broker, consultant, or service provider",

    Lender: "We are a lender or legal/advisory firm",

  };



  const AIRTABLE_CHOICES = {

    Brand: "Brand - " + FORM_DISPLAY_LABELS.Brand,

    Operator: "Operator - " + FORM_DISPLAY_LABELS.Operator,

    Both: "Both - " + FORM_DISPLAY_LABELS.Both,

    Owner: "Owner - " + FORM_DISPLAY_LABELS.Owner,

    Advisor: "Advisor - " + FORM_DISPLAY_LABELS.Advisor,

    Lender: "Lender - " + FORM_DISPLAY_LABELS.Lender,

  };



  const TITLE_CASE_CHOICES = {

    Brand: "Brand - We Represent A Hotel Brand (Franchise/Licensing Platform)",

    Operator: "Operator - We Operate Hotels Under Third-Party Brands (Operator Only)",

    Both: "Both - We Both Represent A Brand And Operate Hotels",

    Owner: "Owner - We Are An Owner, Developer, Or Investor",

    Advisor: "Advisor - We Are A Broker, Consultant, Or Service Provider",

    Lender: "Lender - We Are A Lender Or Legal/Advisory Firm",

  };



  const AIRTABLE_TO_FILTER = {};

  for (const [key, val] of Object.entries(AIRTABLE_CHOICES)) {

    AIRTABLE_TO_FILTER[val] = key;

  }

  for (const [key, val] of Object.entries(FORM_DISPLAY_LABELS)) {

    AIRTABLE_TO_FILTER[val] = key;

  }

  for (const [key, val] of Object.entries(TITLE_CASE_CHOICES)) {

    AIRTABLE_TO_FILTER[val] = key;

  }



  const PREFIX_TO_FILTER = {

    brand: "Brand",

    operator: "Operator",

    both: "Both",

    owner: "Owner",

    advisor: "Advisor",

    lender: "Lender",

  };



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



  function normalizeAirtableToFilterKey(rawValue) {

    const raw = rawValue == null ? "" : String(rawValue).trim();

    if (!raw) return "";

    if (AIRTABLE_TO_FILTER[raw]) return AIRTABLE_TO_FILTER[raw];



    const prefixMatch = raw.match(/^([A-Za-z]+)\s*[-–—]\s*/);

    if (prefixMatch) {

      const key = PREFIX_TO_FILTER[prefixMatch[1].toLowerCase()];

      if (key) return key;

    }



    const lower = raw.toLowerCase();

    if (lower.startsWith("brand") || lower.includes("franchise/licensing platform")) return "Brand";

    if (lower.startsWith("operator") || lower.includes("operator only")) return "Operator";

    if (lower.startsWith("both")) return "Both";

    if (lower.startsWith("owner") || lower.includes("developer, or investor")) return "Owner";

    if (lower.startsWith("advisor") || lower.includes("broker, consultant")) return "Advisor";

    if (lower.startsWith("lender") || lower.includes("legal/advisory")) return "Lender";

    return "";

  }



  function displayLabel(rawValue, formKey) {

    const raw = rawValue == null ? "" : String(rawValue).trim();

    if (raw) return raw;

    const key = formKey == null ? "" : String(formKey).trim();

    return AIRTABLE_CHOICES[key] || FORM_DISPLAY_LABELS[key] || "";

  }



  const FILTER_OPTIONS = [

    { value: "", label: "All Roles" },

    { value: "Brand", label: "Brand (Franchise / Licensing)", airtable: AIRTABLE_CHOICES.Brand },

    { value: "Operator", label: "Operator (Third-Party Brands)", airtable: AIRTABLE_CHOICES.Operator },

    { value: "Both", label: "Brand & Operator (Both)", airtable: AIRTABLE_CHOICES.Both },

    { value: "Owner", label: "Owner / Developer / Investor", airtable: AIRTABLE_CHOICES.Owner },

    { value: "Advisor", label: "Advisor / Consultant", airtable: AIRTABLE_CHOICES.Advisor },

    { value: "Lender", label: "Lender / Legal-Advisory", airtable: AIRTABLE_CHOICES.Lender },

  ];



  const LABEL_BY_VALUE = Object.fromEntries(

    FILTER_OPTIONS.map((o) => [o.value, o.label])

  );



  window.DEALALITY_COMPANY_ROLE = {

    airtableField: AIRTABLE_FIELD,

    airtableChoices: AIRTABLE_CHOICES,

    formDisplayLabels: FORM_DISPLAY_LABELS,

    filterOptions: FILTER_OPTIONS,

    normalizeAirtableToFilterKey,

    displayLabel,

    roleFromFields(fields) {

      return normalizeAirtableToFilterKey(getFieldValue(fields, AIRTABLE_FIELD));

    },

    filterLabel(value) {

      const key = value == null ? "" : String(value).trim();

      return LABEL_BY_VALUE[key] ?? key;

    },

    populateFilterSelect(selectId) {

      const select = document.getElementById(selectId || "companyRoleFilter");

      if (!select) return;

      const current = select.value;

      select.innerHTML = "";

      FILTER_OPTIONS.forEach((opt) => {

        const el = document.createElement("option");

        el.value = opt.value;

        el.textContent = opt.label;

        if (opt.airtable) el.dataset.airtableValue = opt.airtable;

        select.appendChild(el);

      });

      if (current && LABEL_BY_VALUE[current] !== undefined) {

        select.value = current;

      }

    },

  };

})();

