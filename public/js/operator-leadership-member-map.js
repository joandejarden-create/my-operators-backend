/**
 * Browser copy — Leadership Team Members field map + select options.
 * Keep in sync with api/lib/operator-leadership-member-map.js
 */
(function (global) {
  "use strict";

  var MAP_LEADERSHIP_MEMBER = {
    displayOrder: "display_order",
    name: "name",
    title: "title",
    role: "role",
    summary: "summary",
    bio: "bio",
    headshot: "headshot",
    hospitalityExperienceYears: "hospitality_experience_years",
    companyTenureYears: "company_tenure_years",
    priorBackground: "prior_background",
    languages: "languages",
    marketExperience: "market_experience",
    coreExpertise: "core_expertise",
    relevantAssetTypes: "relevant_asset_types",
  };

  var LEADERSHIP_MEMBER_SELECT_OPTIONS = {
    languages: [
      "English",
      "Spanish",
      "Portuguese",
      "French",
      "Italian",
      "German",
      "Mandarin",
      "Japanese",
      "Arabic",
      "Other",
    ],
    marketExperience: [
      "United States",
      "Mexico",
      "Dominican Republic",
      "Puerto Rico",
      "Costa Rica",
      "Panama",
      "Colombia",
      "Brazil",
      "Chile",
      "Peru",
      "Argentina",
      "Caribbean",
      "CALA — Regional",
      "Europe",
      "Middle East",
      "Central America",
      "South America",
    ],
    coreExpertise: [
      "Revenue Management",
      "Direct Booking",
      "Distribution",
      "Operations",
      "Pre-Opening / Transitions",
      "Development",
      "Finance & Owner Reporting",
      "F&B / Lifestyle",
      "Brand Compliance",
      "Sales & Marketing",
      "Technology",
      "HR / Talent",
      "Legal / Compliance",
      "Owner Relations",
    ],
    relevantAssetTypes: [
      "Resort",
      "Lifestyle",
      "Independent",
      "Full-Service",
      "Select-Service",
      "Extended-Stay",
      "Urban",
      "Airport",
      "Convention",
      "Mixed-Use",
      "Branded",
      "Soft Brand",
      "All-Inclusive",
    ],
  };

  function nz(v) {
    return v != null && String(v).trim() !== "" ? String(v).trim() : "";
  }

  function parseExecMultiSelectValue(value) {
    if (value == null || value === "") return [];
    if (Array.isArray(value)) {
      return value.map(function (v) {
        return nz(v);
      }).filter(Boolean);
    }
    var t = String(value).trim();
    if (!t) return [];
    if (t.charAt(0) === "[") {
      try {
        var p = JSON.parse(t);
        return Array.isArray(p) ? parseExecMultiSelectValue(p) : [];
      } catch (e) {
        return [];
      }
    }
    return t
      .split(/[,;\n|]+/)
      .map(function (x) {
        return nz(x);
      })
      .filter(Boolean);
  }

  function filterToAllowedOptions(values, optionKey) {
    var allowed = LEADERSHIP_MEMBER_SELECT_OPTIONS[optionKey] || [];
    var set = {};
    allowed.forEach(function (a) {
      set[a] = true;
    });
    return parseExecMultiSelectValue(values).filter(function (v) {
      return set[v];
    });
  }

  function parseExperienceYears(value) {
    if (value == null || value === "") return null;
    var n = Number(String(value).replace(/[^\d.]/g, ""));
    if (!Number.isFinite(n) || n < 0 || n > 80) return null;
    return Math.round(n * 10) / 10;
  }

  function formatLeaderExperienceLine(hospitalityYears, companyTenureYears) {
    var parts = [];
    var h = parseExperienceYears(hospitalityYears);
    var c = parseExperienceYears(companyTenureYears);
    if (h != null) parts.push(h + " yrs hospitality");
    if (c != null) parts.push(c + " yrs with company");
    return parts.join(" | ");
  }

  function optionsHtml(optionKey, selected) {
    var opts = LEADERSHIP_MEMBER_SELECT_OPTIONS[optionKey] || [];
    var sel = {};
    parseExecMultiSelectValue(selected).forEach(function (v) {
      sel[v] = true;
    });
    return opts
      .map(function (label) {
        return (
          '<option value="' +
          label.replace(/"/g, "&quot;") +
          '"' +
          (sel[label] ? " selected" : "") +
          ">" +
          label +
          "</option>"
        );
      })
      .join("");
  }

  function buildExecProfileFieldsHtml(execIndex) {
    var n = execIndex;
    var prefix = "exec_" + n + "_";
    return (
      '<div class="exec-profile-detail" data-exec-profile-detail="1">' +
      '<h3 class="project-fit-subheader case-study-row-subheader exec-profile-detail__title">Profile Detail</h3>' +
      '<div class="exec-profile-detail__grid">' +
      '<div class="field-wrap">' +
      '<label class="form-label label-spacing" for="' +
      prefix +
      'hospitality_experience_years">Hospitality Experience (Years)</label>' +
      '<input class="form-input" type="number" min="0" max="80" step="0.5" name="' +
      prefix +
      'hospitality_experience_years" id="' +
      prefix +
      'hospitality_experience_years" placeholder="e.g. 15" data-explorer-payload="1" />' +
      "</div>" +
      '<div class="field-wrap">' +
      '<label class="form-label label-spacing" for="' +
      prefix +
      'company_tenure_years">Company Tenure (Years)</label>' +
      '<input class="form-input" type="number" min="0" max="80" step="0.5" name="' +
      prefix +
      'company_tenure_years" id="' +
      prefix +
      'company_tenure_years" placeholder="e.g. 5" data-explorer-payload="1" />' +
      "</div>" +
      '<div class="field-wrap exec-profile-detail__span-2">' +
      '<label class="form-label label-spacing" for="' +
      prefix +
      'prior_background">Prior Background</label>' +
      '<input class="form-input" type="text" name="' +
      prefix +
      'prior_background" id="' +
      prefix +
      'prior_background" placeholder="e.g. Global OTA, resort revenue management" data-explorer-payload="1" />' +
      "</div>" +
      '<div class="field-wrap">' +
      '<label class="form-label label-spacing" for="' +
      prefix +
      'languages">Languages</label>' +
      '<select class="form-select exec-profile-multiselect" name="' +
      prefix +
      'languages" id="' +
      prefix +
      'languages" multiple size="3" data-explorer-payload="1">' +
      optionsHtml("languages") +
      "</select></div>" +
      '<div class="field-wrap">' +
      '<label class="form-label label-spacing" for="' +
      prefix +
      'market_experience">Market Experience</label>' +
      '<select class="form-select exec-profile-multiselect" name="' +
      prefix +
      'market_experience" id="' +
      prefix +
      'market_experience" multiple size="3" data-explorer-payload="1">' +
      optionsHtml("marketExperience") +
      "</select></div>" +
      '<div class="field-wrap exec-profile-detail__full">' +
      '<label class="form-label label-spacing" for="' +
      prefix +
      'core_expertise">Core Expertise</label>' +
      '<select class="form-select exec-profile-multiselect" name="' +
      prefix +
      'core_expertise" id="' +
      prefix +
      'core_expertise" multiple size="3" data-explorer-payload="1">' +
      optionsHtml("coreExpertise") +
      "</select></div>" +
      '<div class="field-wrap exec-profile-detail__full">' +
      '<label class="form-label label-spacing" for="' +
      prefix +
      'relevant_asset_types">Relevant Asset Types</label>' +
      '<select class="form-select exec-profile-multiselect" name="' +
      prefix +
      'relevant_asset_types" id="' +
      prefix +
      'relevant_asset_types" multiple size="3" data-explorer-payload="1">' +
      optionsHtml("relevantAssetTypes") +
      "</select></div>" +
      "</div></div>"
    );
  }

  global.OperatorLeadershipMemberMap = {
    MAP_LEADERSHIP_MEMBER: MAP_LEADERSHIP_MEMBER,
    LEADERSHIP_MEMBER_SELECT_OPTIONS: LEADERSHIP_MEMBER_SELECT_OPTIONS,
    parseExecMultiSelectValue: parseExecMultiSelectValue,
    filterToAllowedOptions: filterToAllowedOptions,
    parseExperienceYears: parseExperienceYears,
    formatLeaderExperienceLine: formatLeaderExperienceLine,
    buildExecProfileFieldsHtml: buildExecProfileFieldsHtml,
    optionsHtml: optionsHtml,
  };
})(typeof globalThis !== "undefined" ? globalThis : typeof window !== "undefined" ? window : self);
