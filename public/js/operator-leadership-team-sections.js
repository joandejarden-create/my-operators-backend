/**
 * Leadership & Team — owner-facing Explorer subsections (JSON from Operator Setup).
 */
(function (global) {
  "use strict";

  var FIELD = {
    org: "lead_org_structure_json",
    depth: "lead_team_depth_json",
    languages: "lead_language_capability_json",
    cadence: "lead_governance_cadence_json",
    markets: "lead_team_markets_json",
    ownerModel: "lead_owner_relationship_json",
  };

  /** Snapshot KPI scalars when Operator Setup fields are empty (matches form defaults / fixture). */
  var SNAPSHOT_SCALAR_DEFAULTS = {
    lead_avg_hospitality_experience: "18 yrs",
    lead_signal_tenure: "6.2 yrs",
  };

  var DEFAULTS = {
    lead_org_structure_json: [
      {
        title: "Corporate Leadership",
        description:
          "Executive oversight, enterprise governance, owner escalation, portfolio strategy.",
        tags: ["CEO / President", "COO", "Finance", "Legal / Compliance"],
      },
      {
        title: "Regional Leadership",
        description:
          "Market-specific support, owner relationships, local partner coordination, growth strategy.",
        tags: ["Regional MD", "Market Leads", "Development", "Owner Relations"],
      },
      {
        title: "Property Operations",
        description:
          "Hotel-level execution, operational standards, staffing, service delivery, transition support.",
        tags: ["Regional Ops", "GMs", "Task Force", "Quality Support"],
      },
      {
        title: "Commercial Platform",
        description:
          "Revenue management, sales, marketing, distribution, forecasting, and direct-booking strategy.",
        tags: ["Revenue", "Sales", "Digital", "Distribution"],
      },
      {
        title: "Owner Reporting & Finance",
        description:
          "Monthly reporting, business reviews, budgeting, capex tracking, and financial visibility.",
        tags: ["Finance", "Accounting", "Asset Mgmt", "Owner Reporting"],
      },
      {
        title: "Specialty Support",
        description:
          "Pre-opening, brand compliance, F&B, lifestyle programming, procurement, and technical transitions.",
        tags: ["Pre-Opening", "F&B", "Brand Compliance", "Procurement"],
      },
    ],
    lead_team_depth_json: [
      {
        function: "Operations",
        leadRole: "COO / Regional Ops",
        depth: "Strong",
        relevance:
          "Day-to-day execution, service delivery, staffing, and operational performance.",
      },
      {
        function: "Revenue Management",
        leadRole: "VP Commercial Strategy",
        depth: "Strong",
        relevance: "Pricing, forecasting, channel strategy, segmentation, and ramp-up planning.",
      },
      {
        function: "Owner Reporting",
        leadRole: "VP Finance & Owner Reporting",
        depth: "Strong",
        relevance: "Monthly reporting, budget process, KPI visibility, and owner communication.",
      },
      {
        function: "Pre-Opening / Transitions",
        leadRole: "Director, Pre-Opening",
        depth: "Very Strong",
        relevance: "Opening readiness, systems, task force coordination, and takeover planning.",
      },
      {
        function: "F&B / Lifestyle",
        leadRole: "F&B / Experience Lead",
        depth: "Strong",
        relevance:
          "Restaurant activation, programming, lifestyle positioning, and guest experience.",
      },
      {
        function: "Brand Compliance",
        leadRole: "Brand Transition Lead",
        depth: "Moderate / Strong",
        relevance:
          "Brand onboarding, compliance coordination, PIP tracking, and technical services support.",
      },
      {
        function: "CALA / Regional Growth",
        leadRole: "Regional Director, CALA",
        depth: "Emerging / Strong",
        relevance:
          "Local introductions, regional owner support, market entry, and partner coordination.",
      },
    ],
    lead_language_capability_json: [
      {
        language: "English",
        proficiency: "Native / Fluent",
        support: "Corporate leadership, reporting, owner communication",
      },
      {
        language: "Spanish",
        proficiency: "Fluent",
        support: "CALA growth, regional owner communication, market entry support",
      },
      {
        language: "Portuguese",
        proficiency: "Working / Fluent",
        support: "Commercial strategy and Brazil-related market experience",
      },
      {
        language: "French",
        proficiency: "Working",
        support: "Island market support, pre-opening / transition support",
      },
    ],
    lead_governance_cadence_json: [
      {
        title: "Weekly: Transition / Pre-Opening Calls",
        description: "During onboarding, opening, takeover, or major repositioning.",
      },
      {
        title: "Monthly: Business Review",
        description:
          "Performance, owner reporting, KPI review, action plan, and issue escalation.",
      },
      {
        title: "Quarterly: Strategic Review",
        description:
          "Market outlook, commercial priorities, capex planning, and asset value initiatives.",
      },
      {
        title: "Annually: Budget and Business Plan",
        description:
          "Operating budget, capital priorities, owner objectives, and annual performance plan.",
      },
    ],
    lead_team_markets_json: [
      {
        market: "Mexico",
        experience:
          "Prior openings, brand transitions, owner relationships, business development",
        leaders: "Regional Director, Business Development, Revenue Lead",
      },
      {
        market: "Dominican Republic",
        experience:
          "Resort operations, F&B programming, labor model familiarity, owner communication",
        leaders: "COO, Regional Director, F&B Lead",
      },
      {
        market: "Puerto Rico",
        experience: "U.S. regulatory overlay, island operations, finance/reporting complexity",
        leaders: "Finance Lead, COO, Pre-Opening Lead",
      },
      {
        market: "Costa Rica",
        experience:
          "Resort positioning, sustainability-oriented owner priorities, leisure demand",
        leaders: "Regional Director, Commercial Strategy",
      },
      {
        market: "Brazil",
        experience: "Commercial strategy, Portuguese-language support, owner reporting context",
        leaders: "Commercial Strategy, Finance",
      },
    ],
    lead_owner_relationship_json: [
      {
        title: "Primary Owner Contact",
        value: "SVP Business Development / Regional Director",
        description: "Main point of contact through onboarding and active evaluation.",
      },
      {
        title: "Executive Sponsor",
        value: "COO",
        description: "Senior escalation and strategic oversight for complex owner situations.",
      },
      {
        title: "Monthly Business Reviews",
        value: "Operations + Finance + Commercial",
        description: "Performance review, owner priorities, forecast, and action plan alignment.",
      },
      {
        title: "Budget Review Lead",
        value: "Finance & Owner Reporting",
        description: "Annual budget, capex review, and financial performance visibility.",
      },
      {
        title: "Pre-Opening Lead",
        value: "Pre-Opening & Transitions",
        description:
          "Critical path, systems, staffing, vendor coordination, and opening readiness.",
      },
      {
        title: "Brand Coordination Lead",
        value: "Development / Brand Compliance",
        description:
          "Coordinates with brand teams, technical services, standards, and PIP issues.",
      },
    ],
  };

  function nz(v) {
    if (v == null) return "";
    return String(v).trim();
  }

  function escapeHtml(s) {
    return String(s == null ? "" : s)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;");
  }

  function pick(ex, p, key) {
    if (ex && nz(ex[key])) return ex[key];
    if (p && nz(p[key])) return p[key];
    return "";
  }

  /** Internal Operator Setup guidance — must not render on owner-facing Explorer. */
  function isInternalSetupHintCopy(value) {
    var s = nz(value);
    if (!s) return false;
    if (/^(Mirror CALA leadership|mirror your top three leaders|\[Internal fill guidance)/i.test(s)) {
      return true;
    }
    if (/Leadership Team Members:/i.test(s) && s.length > 72) return true;
    return false;
  }

  function pickOwnerFacing(ex, p, key) {
    var v = pick(ex, p, key);
    return isInternalSetupHintCopy(v) ? "" : v;
  }

  function parseJsonArray(raw) {
    if (raw == null || raw === "") return null;
    if (Array.isArray(raw)) return raw;
    try {
      var parsed = JSON.parse(String(raw));
      return Array.isArray(parsed) ? parsed : null;
    } catch (e) {
      return null;
    }
  }

  var PLATFORM_KEY_BY_FIELD = {
    lead_org_structure_json: "orgStructure",
    lead_team_depth_json: "teamDepth",
    lead_language_capability_json: "languages",
    lead_governance_cadence_json: "governanceCadence",
    lead_team_markets_json: "teamMarkets",
    lead_owner_relationship_json: "ownerRelationship",
  };

  function sectionData(vm, fieldKey) {
    var pk = PLATFORM_KEY_BY_FIELD[fieldKey];
    var lp =
      (vm && vm.leadershipPlatform) ||
      (vm && vm.prefill && vm.prefill.leadershipPlatform) ||
      null;
    if (pk && lp && Array.isArray(lp[pk]) && lp[pk].length) return lp[pk];

    var p = (vm && vm.prefill) || {};
    var ex = (vm && vm.ex) || {};
    var fromRecord = parseJsonArray(pick(ex, p, fieldKey));
    if (fromRecord && fromRecord.length) return fromRecord;
    return DEFAULTS[fieldKey] || [];
  }

  function scalarSnapshotField(vm, fieldKey) {
    var p = (vm && vm.prefill) || {};
    var ex = (vm && vm.ex) || {};
    var v = pickOwnerFacing(ex, p, fieldKey);
    if (nz(v)) return v;
    return SNAPSHOT_SCALAR_DEFAULTS[fieldKey] || "";
  }

  function uniqueLanguageNames(names) {
    var seen = {};
    var out = [];
    (names || []).forEach(function (name) {
      var n = nz(name);
      if (!n) return;
      var key = n.toLowerCase();
      if (seen[key]) return;
      seen[key] = true;
      out.push(n);
    });
    return out;
  }

  /**
   * Languages for Leadership Snapshot KPIs — same source as Language & Regional Capability table.
   * @param {object} vm
   * @returns {{ count: number, names: string[] }}
   */
  function languageSnapshot(vm) {
    var rows = sectionData(vm, FIELD.languages);
    var names = uniqueLanguageNames(
      rows.map(function (row) {
        return row && row.language;
      })
    );
    return { count: names.length, names: names };
  }

  function depthBadgeClass(depth) {
    var d = nz(depth).toLowerCase();
    if (d.indexOf("very strong") >= 0) return "oe-lead-depth oe-lead-depth--very-strong";
    if (d.indexOf("emerging") >= 0) return "oe-lead-depth oe-lead-depth--emerging";
    if (d.indexOf("moderate") >= 0) return "oe-lead-depth oe-lead-depth--moderate";
    return "oe-lead-depth oe-lead-depth--strong";
  }

  function renderOrganizationStructure(rows) {
    if (!rows.length) return "";
    var cards = rows
      .map(function (row) {
        var tags = (row.tags || [])
          .map(function (t) {
            return '<span class="oe-lead-chip">' + escapeHtml(t) + "</span>";
          })
          .join("");
        return (
          '<div class="card oe-lead-org-card">' +
          "<h3>" +
          escapeHtml(row.title) +
          "</h3>" +
          "<p>" +
          escapeHtml(row.description) +
          "</p>" +
          (tags ? '<div class="oe-lead-chip-row">' + tags + "</div>" : "") +
          "</div>"
        );
      })
      .join("");
    return (
      '<section class="section oe-lead-section">' +
      '<h2 class="section-title">Organization Structure</h2>' +
      '<p class="gold-mock-tab-empty odna-subsection-intro">How the operator is organized to support owners from corporate leadership through property execution.</p>' +
      '<div class="grid-3 oe-lead-org-grid">' +
      cards +
      "</div></section>"
    );
  }

  function renderTeamDepthTable(rows) {
    if (!rows.length) return "";
    var body = rows
      .map(function (row) {
        return (
          "<tr>" +
          "<th scope=\"row\">" +
          escapeHtml(row.function) +
          "</th>" +
          "<td>" +
          escapeHtml(row.leadRole) +
          "</td>" +
          '<td><span class="' +
          depthBadgeClass(row.depth) +
          '">' +
          escapeHtml(row.depth) +
          "</span></td>" +
          "<td>" +
          escapeHtml(row.relevance) +
          "</td></tr>"
        );
      })
      .join("");
    return (
      '<section class="section oe-lead-section">' +
      '<h2 class="section-title">Team Depth by Function</h2>' +
      '<p class="gold-mock-tab-empty odna-subsection-intro">A practical view of the support bench behind key owner priorities.</p>' +
      '<div class="gold-footprint-table-wrap oe-lead-table-wrap">' +
      '<table class="gold-footprint-table oe-lead-data-table">' +
      "<thead><tr><th scope=\"col\">Function</th><th scope=\"col\">Lead Role</th><th scope=\"col\">Team Depth</th><th scope=\"col\">Owner Relevance</th></tr></thead>" +
      "<tbody>" +
      body +
      "</tbody></table></div></section>"
    );
  }

  function renderLanguageTable(rows) {
    if (!rows.length) return "";
    var body = rows
      .map(function (row) {
        return (
          "<tr>" +
          "<th scope=\"row\">" +
          escapeHtml(row.language) +
          "</th>" +
          '<td><span class="oe-lead-proficiency">' +
          escapeHtml(row.proficiency) +
          "</span></td>" +
          "<td>" +
          escapeHtml(row.support) +
          "</td></tr>"
        );
      })
      .join("");
    return (
      '<section class="section oe-lead-section">' +
      '<h2 class="section-title">Language &amp; Regional Capability</h2>' +
      '<p class="gold-mock-tab-empty odna-subsection-intro">Language coverage and owner communication support by function.</p>' +
      '<div class="gold-footprint-table-wrap oe-lead-table-wrap">' +
      '<table class="gold-footprint-table oe-lead-data-table oe-lead-lang-table">' +
      "<thead><tr><th scope=\"col\">Language</th><th scope=\"col\">Proficiency</th><th scope=\"col\">Owner Support</th></tr></thead>" +
      "<tbody>" +
      body +
      "</tbody></table></div></section>"
    );
  }

  function titleCaseWord(word) {
    var w = nz(word);
    if (!w) return "";
    if (w === "and" || w === "or" || w === "of") return w;
    return w
      .split("-")
      .map(function (part) {
        if (!part) return part;
        return part.charAt(0).toUpperCase() + part.slice(1).toLowerCase();
      })
      .join("-");
  }

  /** Cadence card headers: "Weekly: transition / pre-opening calls" → Proper Case after the colon. */
  function formatGovernanceCadenceTitle(title) {
    var s = nz(title);
    if (!s) return "";
    var colon = s.indexOf(":");
    if (colon < 0) return s;
    var prefix = s.slice(0, colon + 1) + " ";
    var body = s.slice(colon + 1).trim();
    var formatted = body
      .split(/\s*\/\s*/)
      .map(function (segment) {
        return segment
          .split(/\s+/)
          .map(titleCaseWord)
          .join(" ");
      })
      .join(" / ");
    return prefix + formatted;
  }

  function renderGovernanceCadence(rows) {
    if (!rows.length) return "";
    var items = rows
      .map(function (row) {
        return (
          '<div class="card oe-lead-cadence-card">' +
          '<span class="oe-lead-cadence-card__icon" aria-hidden="true"></span>' +
          "<div>" +
          "<h3>" +
          escapeHtml(formatGovernanceCadenceTitle(row.title)) +
          "</h3>" +
          "<p>" +
          escapeHtml(row.description) +
          "</p></div></div>"
        );
      })
      .join("");
    return (
      '<section class="section oe-lead-section">' +
      '<h2 class="section-title">Governance &amp; Communication Cadence</h2>' +
      '<p class="gold-mock-tab-empty odna-subsection-intro">How the team typically communicates with ownership.</p>' +
      '<div class="oe-lead-cadence-list">' +
      items +
      "</div></section>"
    );
  }

  function renderTeamMarketsTable(rows) {
    if (!rows.length) return "";
    var body = rows
      .map(function (row) {
        return (
          "<tr>" +
          "<th scope=\"row\">" +
          escapeHtml(row.market) +
          "</th>" +
          "<td>" +
          escapeHtml(row.experience) +
          "</td>" +
          "<td>" +
          escapeHtml(row.leaders) +
          "</td></tr>"
        );
      })
      .join("");
    return (
      '<section class="section oe-lead-section">' +
      '<h2 class="section-title">Team Experience Markets</h2>' +
      '<p class="gold-mock-tab-empty odna-subsection-intro">Shows which leaders or functions support the operator&apos;s team-level market credibility.</p>' +
      '<div class="gold-footprint-table-wrap oe-lead-table-wrap">' +
      '<table class="gold-footprint-table oe-lead-data-table">' +
      "<thead><tr><th scope=\"col\">Market</th><th scope=\"col\">Team Experience</th><th scope=\"col\">Relevant Leaders / Functions</th></tr></thead>" +
      "<tbody>" +
      body +
      "</tbody></table></div></section>"
    );
  }

  function renderOwnerRelationship(rows) {
    if (!rows.length) return "";
    var cards = rows
      .map(function (row) {
        return (
          '<div class="card oe-lead-owner-card">' +
          '<span class="oe-lead-cadence-card__icon" aria-hidden="true"></span>' +
          "<div>" +
          "<h3>" +
          escapeHtml(row.title) +
          "</h3>" +
          '<p class="oe-lead-owner-card__value">' +
          escapeHtml(row.value) +
          "</p>" +
          "<p>" +
          escapeHtml(row.description) +
          "</p></div></div>"
        );
      })
      .join("");
    return (
      '<section class="section oe-lead-section">' +
      '<h2 class="section-title">Owner Relationship Model</h2>' +
      '<p class="gold-mock-tab-empty odna-subsection-intro">How an owner would interact with the operator during evaluation, onboarding, and ongoing management.</p>' +
      '<div class="grid-3 oe-lead-owner-grid">' +
      cards +
      "</div></section>"
    );
  }

  /**
   * @param {object} vm - gold-mock view model
   * @returns {string} HTML for all leadership team subsections
   */
  function buildLeadershipTeamSectionsHtml(vm) {
    var langCadence =
      '<div class="grid-2 oe-lead-lang-gov-grid">' +
      renderLanguageTable(sectionData(vm, FIELD.languages)) +
      renderGovernanceCadence(sectionData(vm, FIELD.cadence)) +
      "</div>";

    return (
      renderOrganizationStructure(sectionData(vm, FIELD.org)) +
      renderTeamDepthTable(sectionData(vm, FIELD.depth)) +
      langCadence +
      renderTeamMarketsTable(sectionData(vm, FIELD.markets)) +
      renderOwnerRelationship(sectionData(vm, FIELD.ownerModel))
    );
  }

  global.OperatorLeadershipTeamSections = {
    FIELD: FIELD,
    DEFAULTS: DEFAULTS,
    SNAPSHOT_SCALAR_DEFAULTS: SNAPSHOT_SCALAR_DEFAULTS,
    buildLeadershipTeamSectionsHtml: buildLeadershipTeamSectionsHtml,
    sectionData: sectionData,
    scalarSnapshotField: scalarSnapshotField,
    languageSnapshot: languageSnapshot,
    isInternalSetupHintCopy: isInternalSetupHintCopy,
    pickOwnerFacing: pickOwnerFacing,
  };
})(typeof window !== "undefined" ? window : global);
