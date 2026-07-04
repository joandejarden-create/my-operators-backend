/**
 * Engagement & Reporting — owner-facing subsections (Explorer / DNA).
 * JSON via prefill / explorerProfileJson (ov_* keys) with defaults until Setup fields exist.
 */
(function (global) {
  "use strict";

  var FIELD = {
    strategicOwnerValue: "ov_strategic_owner_value_json",
    engagementCadence: "ov_engagement_cadence_json",
    controlsGovernance: "ov_controls_governance_json",
    reportsReceived: "ov_reports_received_json",
    ownerTools: "ov_owner_tools_json",
    lifecycleSupport: "ov_lifecycle_support_json",
  };

  var DEFAULTS = {
    ov_strategic_owner_value_json: [
      {
        title: "Owner Strategy Translation",
        description:
          "Converts owner objectives into an operating plan, reporting cadence, and measurable priorities.",
      },
      {
        title: "Asset Performance Visibility",
        description:
          "Creates a clearer view of revenue, expenses, GOP, guest feedback, capex, and market position.",
      },
      {
        title: "Decision Support",
        description:
          "Helps owners understand trade-offs around pricing, staffing, brand requirements, capex timing, and repositioning.",
      },
      {
        title: "Governance Discipline",
        description:
          "Provides a structured rhythm for reviews, approvals, escalations, and accountability.",
      },
      {
        title: "Value Protection",
        description:
          "Supports long-term value by connecting operating decisions to asset condition, guest experience, and exit readiness.",
      },
      {
        title: "Owner Education",
        description:
          "Helps owners understand hotel metrics, brand obligations, distribution dynamics, and operating levers.",
      },
    ],
    ov_engagement_cadence_json: [
      {
        cadence: "Weekly",
        engagementType: "Transition / ramp-up call",
        focus:
          "Pre-opening, takeover, conversion, or major repositioning periods.",
      },
      {
        cadence: "Monthly",
        engagementType: "Owner performance review",
        focus: "P&L, forecast, KPIs, action plan, guest feedback, capex status, and open issues.",
      },
      {
        cadence: "Quarterly",
        engagementType: "Strategic business review",
        focus:
          "Market performance, competitive position, owner priorities, investment needs, and brand strategy.",
      },
      {
        cadence: "Annually",
        engagementType: "Budget and business plan",
        focus:
          "Operating budget, capex plan, staffing model, commercial plan, and asset strategy.",
      },
    ],
    ov_controls_governance_json: [
      {
        title: "Budget Process",
        description:
          "Annual budget planning, owner review, approval workflow, forecast updates, and variance tracking.",
      },
      {
        title: "CapEx Planning",
        description:
          "Capital prioritization, PIP planning, ROI framing, approval records, and project status tracking.",
      },
      {
        title: "Performance Reviews",
        description:
          "Structured operating reviews tied to owner priorities, KPI trends, and corrective actions.",
      },
      {
        title: "Approval Controls",
        description:
          "Documented approval thresholds for spend, staffing, contract commitments, and capital projects.",
      },
      {
        title: "Issue Escalation",
        description:
          "Defined escalation path for service, financial, brand, legal, compliance, and owner-sensitive issues.",
      },
      {
        title: "Audit Trail",
        description:
          "Records major decisions, reporting packages, approval history, and follow-up actions.",
      },
    ],
    ov_reports_received_json: [
      {
        title: "Monthly Owner Report",
        description:
          "P&L summary, KPI trends, forecast, variance commentary, and action items.",
      },
      {
        title: "Commercial Performance Pack",
        description:
          "ADR, RevPAR, channel mix, segmentation, pace, pickup, and market intelligence.",
      },
      {
        title: "CapEx / PIP Tracker",
        description:
          "Capital projects, timing, approvals, spend status, risk items, and brand requirements.",
      },
      {
        title: "Guest Experience Report",
        description:
          "Reputation, guest satisfaction, service recovery, review trends, and operational responses.",
      },
      {
        title: "Labor & Productivity Report",
        description:
          "Staffing levels, productivity, payroll trends, turnover, and key operating constraints.",
      },
      {
        title: "Asset Value Narrative",
        description:
          "Quarterly summary connecting operating performance to asset value, positioning, and owner objectives.",
      },
    ],
    ov_owner_tools_json: [
      {
        title: "Owner Portal",
        description:
          "Secure owner access to reports, dashboards, documents, meeting notes, and approvals.",
      },
      {
        title: "Dashboard Views",
        description:
          "KPI summary, revenue trends, expense controls, capex status, guest experience, and action items.",
      },
      {
        title: "Document Library",
        description:
          "Budgets, contracts, brand documents, reports, meeting packs, capex files, and project records.",
      },
      {
        title: "Meeting Center",
        description:
          "Agenda, notes, decisions, action owners, due dates, and follow-up status.",
      },
      {
        title: "Owner Advisory Boards / Councils",
        description:
          "Structured owner feedback forums, portfolio insights, education sessions, and strategic input.",
      },
      {
        title: "Owner Education Library",
        description:
          "Plain-language explainers on hotel metrics, brand terms, distribution, capex, and operating models.",
      },
    ],
    ov_lifecycle_support_json: [
      {
        stage: "Evaluation",
        support:
          "Deal review, owner goals, asset needs, operating assumptions, and management fit.",
      },
      {
        stage: "Onboarding",
        support:
          "Kickoff, transition plan, document collection, reporting setup, and governance rhythm.",
      },
      {
        stage: "Pre-Opening / Transition",
        support:
          "Critical path, staffing, systems, vendor setup, brand coordination, and owner updates.",
      },
      {
        stage: "Stabilized Operations",
        support:
          "Monthly reviews, forecast updates, performance management, capex tracking, and strategy check-ins.",
      },
      {
        stage: "Repositioning / Exit",
        support:
          "Asset narrative, capex decisions, performance proof, buyer readiness, and value enhancement.",
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
    ov_strategic_owner_value_json: "strategicOwnerValue",
    ov_engagement_cadence_json: "engagementCadence",
    ov_controls_governance_json: "controlsGovernance",
    ov_reports_received_json: "reportsReceived",
    ov_owner_tools_json: "ownerTools",
    ov_lifecycle_support_json: "lifecycleSupport",
  };

  function engagementPlatform(vm) {
    if (vm && vm.engagementReporting && typeof vm.engagementReporting === "object") {
      return vm.engagementReporting;
    }
    if (vm && vm.engagementPlatform && typeof vm.engagementPlatform === "object") {
      return vm.engagementPlatform;
    }
    if (vm && vm.prefill && vm.prefill.engagementReporting && typeof vm.prefill.engagementReporting === "object") {
      return vm.prefill.engagementReporting;
    }
    return null;
  }

  function sectionData(vm, fieldKey) {
    var platformKey = PLATFORM_KEY_BY_FIELD[fieldKey];
    var ep = engagementPlatform(vm);
    if (platformKey && ep && Array.isArray(ep[platformKey]) && ep[platformKey].length) {
      return ep[platformKey];
    }
    var p = (vm && vm.prefill) || {};
    var ex = (vm && vm.ex) || {};
    var fromRecord = parseJsonArray(pick(ex, p, fieldKey));
    if (fromRecord && fromRecord.length) return fromRecord;
    return DEFAULTS[fieldKey] || [];
  }

  function wrapSection(title, intro, bodyHtml, extraClass) {
    if (!bodyHtml) return "";
    return (
      '<section class="section oe-eng-section' +
      (extraClass ? " " + extraClass : "") +
      '">' +
      '<h2 class="section-title">' +
      escapeHtml(title) +
      "</h2>" +
      (intro
        ? '<p class="gold-mock-tab-empty odna-subsection-intro">' + escapeHtml(intro) + "</p>"
        : "") +
      bodyHtml +
      "</section>"
    );
  }

  function iconCard(row, cardClass, iconClass) {
    if (!row || !nz(row.title)) return "";
    var title =
      global.OperatorExplorerCardTitle && typeof global.OperatorExplorerCardTitle.formatCardTitle === "function"
        ? global.OperatorExplorerCardTitle.formatCardTitle(row.title)
        : row.title;
    return (
      '<div class="card ' +
      cardClass +
      '">' +
      '<span class="' +
      iconClass +
      '" aria-hidden="true"></span>' +
      '<div class="oe-eng-card__body">' +
      "<h3>" +
      escapeHtml(title) +
      "</h3>" +
      "<p>" +
      escapeHtml(row.description || "") +
      "</p></div></div>"
    );
  }

  function buildStrategicOwnerValueSection(vm) {
    var rows = sectionData(vm, FIELD.strategicOwnerValue);
    if (!rows.length) return "";
    return wrapSection(
      "Strategic Owner Value",
      "What this operator may add for you as an owner beyond day-to-day hotel operations.",
      '<div class="grid-3 oe-eng-value-grid">' +
        rows
          .map(function (row) {
            return iconCard(row, "oe-eng-value-card", "oe-eng-value-card__icon");
          })
          .join("") +
        "</div>",
      "oe-eng-section--value"
    );
  }

  function buildCadenceTableSection(vm) {
    var rows = sectionData(vm, FIELD.engagementCadence);
    if (!rows.length) return "";
    var body = rows
      .map(function (row) {
        return (
          "<tr><th scope=\"row\">" +
          escapeHtml(row.cadence || row.label || "") +
          "</th><td>" +
          escapeHtml(row.engagementType || row.type || "") +
          "</td><td>" +
          escapeHtml(row.focus || row.description || "") +
          "</td></tr>"
        );
      })
      .join("");
    return wrapSection(
      "Owner Engagement Cadence",
      "When you can expect touchpoints, formal reviews, and owner decision cycles through the year.",
      '<div class="oe-eng-table-wrap">' +
        '<table class="oe-eng-table">' +
        "<thead><tr><th scope=\"col\">Cadence</th><th scope=\"col\">Engagement type</th><th scope=\"col\">Typical focus</th></tr></thead>" +
        "<tbody>" +
        body +
        "</tbody></table></div>",
      "oe-eng-section--cadence"
    );
  }

  function buildControlsGovernanceSection(vm) {
    var rows = sectionData(vm, FIELD.controlsGovernance);
    if (!rows.length) return "";
    return wrapSection(
      "Controls & Governance",
      "How this operator helps you stay on top of budgets, capex, performance, and escalations.",
      '<div class="grid-3 oe-eng-controls-grid">' +
        rows
          .map(function (row) {
            return iconCard(row, "oe-eng-controls-card", "oe-eng-controls-card__icon");
          })
          .join("") +
        "</div>",
      "oe-eng-section--controls"
    );
  }

  function buildReportsSection(vm) {
    var rows = sectionData(vm, FIELD.reportsReceived);
    if (!rows.length) return "";
    return wrapSection(
      "Reports Owners Receive",
      "Examples of the reporting packages and decision-support materials that may be provided.",
      '<div class="grid-3 oe-eng-reports-grid">' +
        rows
          .map(function (row) {
            return iconCard(row, "oe-eng-reports-card", "oe-eng-reports-card__icon");
          })
          .join("") +
        "</div>",
      "oe-eng-section--reports"
    );
  }

  function buildOwnerToolsSection(vm) {
    var rows = sectionData(vm, FIELD.ownerTools);
    if (!rows.length) return "";
    return wrapSection(
      "Owner Tools & Support Channels",
      "Where you can access reports, documents, meetings, and owner education between formal reviews.",
      '<div class="grid-3 oe-eng-tools-grid">' +
        rows
          .map(function (row) {
            return iconCard(row, "oe-eng-tools-card", "oe-eng-tools-card__icon");
          })
          .join("") +
        "</div>",
      "oe-eng-section--tools"
    );
  }

  function buildLifecycleSection(vm) {
    var rows = sectionData(vm, FIELD.lifecycleSupport);
    if (!rows.length) return "";
    var body = rows
      .map(function (row) {
        return (
          "<tr><th scope=\"row\">" +
          escapeHtml(row.stage || row.label || "") +
          "</th><td>" +
          escapeHtml(row.support || row.description || "") +
          "</td></tr>"
        );
      })
      .join("");
    return wrapSection(
      "Owner Support Across the Asset Lifecycle",
      "How you can expect communication and support to change from first evaluation through stabilization or exit.",
      '<div class="oe-eng-table-wrap">' +
        '<table class="oe-eng-table oe-eng-table--lifecycle">' +
        "<thead><tr><th scope=\"col\">Lifecycle stage</th><th scope=\"col\">Owner support provided</th></tr></thead>" +
        "<tbody>" +
        body +
        "</tbody></table></div>",
      "oe-eng-section--lifecycle"
    );
  }

  function buildAllSectionsHtml(vm) {
    return (
      buildStrategicOwnerValueSection(vm) +
      buildCadenceTableSection(vm) +
      buildControlsGovernanceSection(vm) +
      buildReportsSection(vm) +
      buildOwnerToolsSection(vm) +
      buildLifecycleSection(vm)
    );
  }

  global.OperatorEngagementReportingSections = {
    buildAllSectionsHtml: buildAllSectionsHtml,
    FIELD: FIELD,
    DEFAULTS: DEFAULTS,
  };
})(typeof globalThis !== "undefined" ? globalThis : global);
