/**
 * Leadership profile cards — structured detail blocks (Explorer / DNA).
 */
(function (global) {
  "use strict";

  function nz(v) {
    return v != null && String(v).trim() !== "" ? String(v).trim() : "";
  }

  function escapeHtml(s) {
    return String(s == null ? "" : s)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;");
  }

  function arrayish(v) {
    if (Array.isArray(v)) return v.map(nz).filter(Boolean);
    if (v == null || v === "") return [];
    return String(v)
      .split(/[,;|\n]+/)
      .map(function (x) {
        return nz(x);
      })
      .filter(Boolean);
  }

  var WORD_YEAR_MAP = {
    "twenty-five": 25,
    "twenty five": 25,
    twenty: 20,
    "twenty-four": 24,
    "twenty four": 24,
    "twenty-three": 23,
    "twenty three": 23,
    "twenty-two": 22,
    "twenty two": 22,
    "twenty-one": 21,
    "twenty one": 21,
    thirty: 30,
    "thirty-five": 35,
    "thirty five": 35,
    fifteen: 15,
    eighteen: 18,
    "twenty-plus": 20,
    "20+": 20,
  };

  function parseYearsFromNarrative(text) {
    var years = [];
    var s = String(text || "");
    var re = /(\d+(?:\.\d+)?)\s*\+?\s*years?/gi;
    var m;
    while ((m = re.exec(s)) !== null) {
      var n = parseFloat(m[1]);
      if (!isNaN(n) && n > 0 && n < 80) years.push(n);
    }
    var wordRe =
      /(twenty[- ]?five|twenty[- ]?four|twenty[- ]?three|twenty[- ]?two|twenty[- ]?one|twenty|thirty[- ]?five|thirty|fifteen|eighteen)\s*\+?\s*years?/gi;
    while ((m = wordRe.exec(s)) !== null) {
      var token = m[1].toLowerCase().replace(/-/g, " ");
      var wn = WORD_YEAR_MAP[token] || WORD_YEAR_MAP[m[1].toLowerCase()];
      if (wn) years.push(wn);
    }
    return years;
  }

  function inferLanguages(L) {
    var fromStructured = arrayish(L.languages);
    if (fromStructured.length) return fromStructured;
    var fromFluency = arrayish(L.languageFluencyLevel);
    if (fromFluency.length) return fromFluency;
    var raw = nz(L.languages);
    if (raw) return arrayish(raw);
    return [];
  }

  function inferMarkets(L) {
    var fromStructured = arrayish(L.marketExperience);
    if (fromStructured.length) return fromStructured;
    var region = nz(L.region || L.function);
    var hits = [];
    if (region) hits.push(region);
    var narrative = [L.summary, L.bio, L.experienceSummary, L.calaExperienceSummary].join(" ");
    var marketHints = [
      "Dominican Republic",
      "Puerto Rico",
      "Mexico",
      "Costa Rica",
      "Brazil",
      "Caribbean",
      "CALA",
      "Florida",
      "United States",
    ];
    marketHints.forEach(function (label) {
      if (narrative.toLowerCase().indexOf(label.toLowerCase()) >= 0) hits.push(label);
    });
    if (/\bDR\b/.test(narrative) && hits.indexOf("Dominican Republic") < 0) hits.push("Dominican Republic");
    if (/\bPR\b/.test(narrative) && hits.indexOf("Puerto Rico") < 0) hits.push("Puerto Rico");
    return hits.filter(function (v, i, a) {
      return a.indexOf(v) === i;
    });
  }

  function inferExpertise(L) {
    var fromStructured = arrayish(L.coreExpertise);
    if (fromStructured.length) return fromStructured;
    var narrative = [L.summary, L.bio, L.title, L.function].join(" ").toLowerCase();
    var hints = [];
    if (/revenue|commercial|pricing/.test(narrative)) hints.push("Revenue Management");
    if (/operat|beach ops|general manager|gm\b/.test(narrative)) hints.push("Operations");
    if (/pre-opening|transition|opening/.test(narrative)) hints.push("Pre-Opening / Transitions");
    if (/develop/.test(narrative)) hints.push("Development");
    if (/finance|cfo|reporting/.test(narrative)) hints.push("Finance & Owner Reporting");
    if (/f&b|food|beverage|lifestyle/.test(narrative)) hints.push("F&B / Lifestyle");
    if (/brand|qa|compliance/.test(narrative)) hints.push("Brand Compliance");
    if (/security|life safety/.test(narrative)) hints.push("Operations");
    return hints;
  }

  function inferAssetTypes(L) {
    var fromStructured = arrayish(L.relevantAssetTypes);
    if (fromStructured.length) return fromStructured;
    var narrative = [L.summary, L.bio].join(" ").toLowerCase();
    var hints = [];
    if (/resort|beach/.test(narrative)) hints.push("Resort");
    if (/full-service|full service/.test(narrative)) hints.push("Full-Service");
    if (/lifestyle/.test(narrative)) hints.push("Lifestyle");
    if (/independent/.test(narrative)) hints.push("Independent");
    if (/select[- ]?service/.test(narrative)) hints.push("Select-Service");
    return hints;
  }

  /** Fill structured profile fields from Airtable or narrative fallbacks. */
  function enrichLeaderProfile(leader) {
    var L = leader || {};
    var narrative = [L.summary, L.bio, L.experienceSummary, L.shortBio, L.calaExperienceSummary].join(
      " "
    );
    var structuredAssets = uniqueList(arrayish(L.relevantAssetTypes));
    var hosp =
      L.hospitalityExperienceYears != null && L.hospitalityExperienceYears !== ""
        ? L.hospitalityExperienceYears
        : null;
    var co =
      L.companyTenureYears != null && L.companyTenureYears !== ""
        ? L.companyTenureYears
        : null;
    if (hosp == null) {
      var yrs = parseYearsFromNarrative(narrative);
      if (yrs.length) hosp = Math.max.apply(null, yrs);
    }
    var mapApi = global.OperatorLeadershipMemberMap;
    var experienceLine =
      nz(L.experienceLine) ||
      (mapApi && mapApi.formatLeaderExperienceLine
        ? mapApi.formatLeaderExperienceLine(hosp, co)
        : hosp != null
          ? hosp + " yrs hospitality"
          : "");
    return {
      hospitalityExperienceYears: hosp,
      companyTenureYears: co,
      priorBackground: nz(L.priorBackground),
      languages: inferLanguages(L),
      marketExperience: inferMarkets(L),
      coreExpertise: inferExpertise(L),
      relevantAssetTypes: structuredAssets.length ? structuredAssets : inferAssetTypes(L),
      experienceLine: experienceLine,
    };
  }

  function uniqueList(items) {
    var list = arrayish(items);
    var seen = {};
    return list.filter(function (t) {
      var k = String(t).toLowerCase();
      if (seen[k]) return false;
      seen[k] = true;
      return true;
    });
  }

  /** Compact inline tags (dot-separated) — less vertical space than pill rows. */
  function inlineTagsHtml(items) {
    var list = uniqueList(items);
    if (!list.length) return "";
    return (
      '<span class="leader-profile-fact__tags">' +
      list
        .map(function (t) {
          return '<span class="oe-lead-tag">' + escapeHtml(t) + "</span>";
        })
        .join("") +
      "</span>"
    );
  }

  function factRow(label, valueHtml) {
    return (
      '<div class="leader-profile-fact">' +
      '<span class="leader-profile-fact__label">' +
      escapeHtml(label) +
      "</span>" +
      '<span class="leader-profile-fact__value">' +
      (valueHtml ||
        '<span class="leader-profile-fact__empty" aria-hidden="true">—</span>') +
      "</span></div>"
    );
  }

  /**
   * @param {object} leader - detail API / vm leadership row
   * @returns {string} HTML or "" if no structured fields
   */
  function buildLeaderProfileDetailHtml(leader) {
    var base = leader || {};
    var enriched = enrichLeaderProfile(base);
    var L = Object.assign({}, base, enriched);
    var prior = nz(L.priorBackground);
    var experienceLine = nz(L.experienceLine);
    var credLine = [experienceLine, prior].filter(Boolean).join(" · ");

    var rows =
      factRow("Credentials", credLine ? escapeHtml(credLine) : "") +
      factRow("Languages", inlineTagsHtml(L.languages)) +
      factRow("Markets", inlineTagsHtml(L.marketExperience)) +
      factRow("Expertise", inlineTagsHtml(L.coreExpertise)) +
      factRow("Asset types", inlineTagsHtml(L.relevantAssetTypes));

    return (
      '<div class="leader-profile-detail leader-profile-detail--compact">' +
      '<div class="leader-profile-detail__facts">' +
      rows +
      "</div></div>"
    );
  }

  var LEADERSHIP_PROFILE_GRID_MAX_COLS = 4;

  /**
   * Even columns per row (max 4): 6→3, 4→2, 8→4, 5→3+2, etc.
   * @param {number} totalCount
   * @param {number} [maxPerRow]
   * @returns {number}
   */
  function computeLeadershipProfileGridColumns(totalCount, maxPerRow) {
    var maxCols = maxPerRow > 0 ? maxPerRow : LEADERSHIP_PROFILE_GRID_MAX_COLS;
    var n = Math.max(0, Math.floor(Number(totalCount) || 0));
    if (n <= 0) return 1;
    if (n <= maxCols) return n;
    var rowCount = Math.ceil(n / maxCols);
    var colsPerRow = Math.ceil(n / rowCount);
    return Math.min(maxCols, Math.max(1, colsPerRow));
  }

  function leadershipProfileGridAttrs(profileCount, maxPerRow) {
    var cols = computeLeadershipProfileGridColumns(profileCount, maxPerRow);
    return (
      'class="oe-leader-profile-grid oe-leader-profile-grid--aligned" ' +
      'style="--oe-leader-profile-cols:' +
      cols +
      '" ' +
      'data-oe-leader-profile-cols="' +
      cols +
      '"'
    );
  }

  global.OperatorLeadershipProfileDetail = {
    buildLeaderProfileDetailHtml: buildLeaderProfileDetailHtml,
    enrichLeaderProfile: enrichLeaderProfile,
    computeLeadershipProfileGridColumns: computeLeadershipProfileGridColumns,
    leadershipProfileGridAttrs: leadershipProfileGridAttrs,
    LEADERSHIP_PROFILE_GRID_MAX_COLS: LEADERSHIP_PROFILE_GRID_MAX_COLS,
  };
})(typeof globalThis !== "undefined" ? globalThis : typeof window !== "undefined" ? window : self);
