/**
 * Infrastructure & Data — owner-facing subsections (Explorer / DNA).
 * JSON via explorerProfileJson (infra_* keys) with defaults until Operator Setup fields exist.
 */
(function (global) {
  "use strict";

  var FIELD = {
    technologyStack: "infra_technology_stack_json",
    servicesOffered: "infra_services_offered_json",
    dataDomains: "infra_data_domains_json",
    dataGovernance: "infra_data_governance_json",
    analyticsSupport: "infra_analytics_support_json",
    technologyMaturity: "infra_technology_maturity_json",
    /** TODO: confirm Airtable field + single-select options on Operator Setup */
    technologyMaturityLevel: "infra_technology_maturity_level",
  };

  /** Shared rubric — descriptions are consistent; only currentLevel varies per operator. */
  var MATURITY_RUBRIC = [
    {
      level: "Basic",
      description:
        "Core systems in place; reports largely manual or PDF-based; limited integration.",
    },
    {
      level: "Structured",
      description:
        "Standard reporting cadence, common KPI definitions, owner dashboards, and document organization.",
    },
    {
      level: "Integrated",
      description:
        "Connected systems, automated feeds, owner portal, workflow tracking, and role-based data visibility.",
    },
    {
      level: "Advanced",
      description:
        "Predictive insights, benchmarking, market signals, automated alerts, and portfolio-level decision support.",
    },
  ];

  var MATURITY_LEVEL_KEYS = MATURITY_RUBRIC.map(function (row) {
    return row.level.toLowerCase();
  });

  var DEFAULTS = {
    infra_technology_stack_json: [
      {
        title: "PMS / Property Operations",
        description:
          "Property management, reservations, guest profile, folios, and operational workflows.",
        examples: "Opera, Cloudbeds, Mews, Infor, or equivalent",
      },
      {
        title: "RMS / Revenue Management",
        description: "Pricing, demand forecasting, yielding, restrictions, pace, and optimization.",
        examples: "Duetto, IDeaS, Atomize, or equivalent",
      },
      {
        title: "CRS / Distribution",
        description:
          "Central reservations, connectivity, brand distribution, channel management, and inventory controls.",
        examples: "Brand CRS, SynXis, SiteMinder, DerbySoft, or equivalent",
      },
      {
        title: "CRM / Guest Data",
        description:
          "Guest profiles, marketing segmentation, loyalty capture, lifecycle marketing, and direct booking support.",
        examples: "Salesforce, Revinate, Cendyn, HubSpot, or equivalent",
      },
      {
        title: "BI / Dashboards",
        description:
          "Owner dashboards, operating KPIs, commercial analytics, financial reporting, and portfolio views.",
        examples: "Power BI, Tableau, Looker, Domo, or equivalent",
      },
      {
        title: "Accounting / ERP",
        description:
          "Financial reporting, AP/AR, budget tracking, owner statements, and controls.",
        examples: "Sage, NetSuite, M3, Oracle, or equivalent",
      },
    ],
    infra_services_offered_json: [
      {
        title: "Systems Selection Support",
        description: "Can help owners evaluate, select, and transition key hotel systems.",
      },
      {
        title: "Implementation Coordination",
        description:
          "Coordinates PMS, RMS, CRS, POS, finance, labor, and reporting setup during opening or transition.",
      },
      {
        title: "Data Room / Document Setup",
        description:
          "Creates organized repositories for contracts, reports, capex files, brand documents, and transition materials.",
      },
      {
        title: "Owner Dashboard Configuration",
        description:
          "Configures dashboards around owner priorities, asset KPIs, portfolio reporting, and action items.",
      },
      {
        title: "Data Quality Review",
        description:
          "Reviews source data completeness, reporting consistency, coding, mappings, and KPI definitions.",
      },
      {
        title: "Integration Planning",
        description:
          "Identifies required system integrations and data flows before go-live or takeover.",
      },
    ],
    infra_data_domains_json: [
      {
        title: "Commercial Data",
        items: [
          "ADR, occupancy, RevPAR, pace, pickup",
          "Segmentation, channel mix, booking window, forecast",
        ],
      },
      {
        title: "Financial Data",
        items: [
          "P&L, GOP, departmental expenses, payroll",
          "Forecast, budget, variance, owner distributions",
        ],
      },
      {
        title: "Guest Experience Data",
        items: [
          "Reviews, satisfaction, service recovery, reputation",
          "Survey results, complaint themes",
        ],
      },
      {
        title: "Labor Data",
        items: [
          "Staffing levels, productivity, scheduling, turnover",
          "Wage pressure, open roles",
        ],
      },
      {
        title: "Asset / CapEx Data",
        items: [
          "PIP items, project status, spend, approvals, timelines",
          "Asset condition, maintenance priorities",
        ],
      },
      {
        title: "Brand / Compliance Data",
        items: [
          "Brand standards, audits, QA results",
          "Compliance tasks, technical services feedback",
        ],
      },
    ],
    infra_data_governance_json: [
      {
        title: "Role-Based Access",
        description:
          "Owners, operators, asset managers, and property teams see the information relevant to their role.",
      },
      {
        title: "Permissioned Documents",
        description:
          "Sensitive reports, contracts, brand materials, and owner files can be access-controlled.",
      },
      {
        title: "Approval Workflows",
        description:
          "Capital items, major spend, budget approvals, and key decisions can follow documented workflows.",
      },
      {
        title: "Audit History",
        description:
          "Tracks report versions, approvals, document updates, meeting actions, and key decisions.",
      },
      {
        title: "Data Source Labeling",
        description:
          "Distinguishes operator-provided, system-generated, owner-provided, and third-party data.",
      },
      {
        title: "Confidentiality Controls",
        description:
          "Supports confidential owner/operator communication and controlled sharing of asset information.",
      },
    ],
    infra_analytics_support_json: [
      {
        title: "Performance Benchmarking",
        description:
          "Compares current performance against budget, forecast, prior year, competitive set, or portfolio averages.",
      },
      {
        title: "Market Intelligence",
        description:
          "Tracks supply, pipeline, demand drivers, competitor movement, and brand/operator presence.",
      },
      {
        title: "Owner Priority Tracking",
        description:
          "Connects KPIs and action plans to owner priorities such as exit strategy, cash flow, capex discipline, or ramp-up.",
      },
      {
        title: "Risk Signals",
        description:
          "Flags reporting gaps, underperformance, delayed capex, declining reviews, labor pressure, or missed follow-ups.",
      },
      {
        title: "Action Item Tracking",
        description:
          "Links insights to accountable owners, due dates, meeting notes, and follow-up status.",
      },
      {
        title: "Portfolio View",
        description:
          "Allows multi-asset owners to compare asset performance, risks, capex, and operating themes.",
      },
    ],
  };

  function nz(v) {
    if (v == null) return "";
    return String(v).trim();
  }

  function formatCardHeading(raw) {
    if (
      global.OperatorExplorerCardTitle &&
      typeof global.OperatorExplorerCardTitle.formatCardTitle === "function"
    ) {
      return global.OperatorExplorerCardTitle.formatCardTitle(raw);
    }
    return nz(raw);
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

  function parseJsonValue(raw) {
    if (raw == null || raw === "") return null;
    if (typeof raw === "object") return raw;
    try {
      return JSON.parse(String(raw));
    } catch (e) {
      return null;
    }
  }

  function normalizeMaturityKey(level) {
    return nz(level).toLowerCase();
  }

  function findRubricLevel(level) {
    var key = normalizeMaturityKey(level);
    if (!key) return null;
    for (var i = 0; i < MATURITY_RUBRIC.length; i++) {
      if (normalizeMaturityKey(MATURITY_RUBRIC[i].level) === key) {
        return MATURITY_RUBRIC[i];
      }
    }
    return null;
  }

  /**
   * One maturity assessment per operator (level + description + optional evidence).
   * @returns {{ level: string, description: string, evidence: string[] } | null}
   */
  function resolveMaturityAssessment(vm) {
    var ip = infraPlatform(vm);
    if (ip && ip.technologyMaturity && nz(ip.technologyMaturity.level)) {
      var rubricFromPlatform = findRubricLevel(ip.technologyMaturity.level);
      if (rubricFromPlatform) {
        return {
          level: rubricFromPlatform.level,
          description: nz(ip.technologyMaturity.summary) || rubricFromPlatform.description,
          evidence: [],
        };
      }
    }

    var p = (vm && vm.prefill) || {};
    var ex = (vm && vm.ex) || {};
    var level = nz(pick(ex, p, FIELD.technologyMaturityLevel));
    var rawJson = pick(ex, p, FIELD.technologyMaturity);
    var parsed = parseJsonValue(rawJson);
    var summary = "";
    var evidence = [];

    if (!level && parsed && typeof parsed === "object" && !Array.isArray(parsed)) {
      level = nz(parsed.currentLevel || parsed.level);
      summary = nz(parsed.summary || parsed.note);
      evidence = arrayish(parsed.evidence);
    }

    if (!level && Array.isArray(parsed)) {
      var marked = null;
      for (var i = 0; i < parsed.length; i++) {
        var row = parsed[i] || {};
        if (row.current || row.isCurrent || row.selected) {
          marked = row;
          break;
        }
      }
      if (marked) {
        level = nz(marked.level || marked.label || marked.title);
        summary = nz(marked.summary || marked.note);
        evidence = arrayish(marked.evidence);
      } else if (parsed.length === 1) {
        level = nz(parsed[0].level || parsed[0].label || parsed[0].title);
        summary = nz(parsed[0].summary || parsed[0].note);
        evidence = arrayish(parsed[0].evidence);
      }
    }

    var rubric = findRubricLevel(level);
    if (!rubric) return null;

    return {
      level: rubric.level,
      description: summary || rubric.description,
      evidence: evidence,
    };
  }

  function infraPlatform(vm) {
    if (vm && vm.infrastructurePlatform && typeof vm.infrastructurePlatform === "object") {
      return vm.infrastructurePlatform;
    }
    if (vm && vm.prefill && vm.prefill.infrastructurePlatform && typeof vm.prefill.infrastructurePlatform === "object") {
      return vm.prefill.infrastructurePlatform;
    }
    return null;
  }

  function sectionRows(vm, platformKey, fieldKey) {
    var ip = infraPlatform(vm);
    if (ip && Array.isArray(ip[platformKey]) && ip[platformKey].length) {
      return ip[platformKey];
    }
    return sectionData(vm, fieldKey);
  }

  function sectionData(vm, fieldKey) {
    var p = (vm && vm.prefill) || {};
    var ex = (vm && vm.ex) || {};
    var fromRecord = parseJsonArray(pick(ex, p, fieldKey));
    if (fromRecord && fromRecord.length) return fromRecord;
    return DEFAULTS[fieldKey] || [];
  }

  function arrayish(v) {
    if (Array.isArray(v)) return v.map(nz).filter(Boolean);
    if (v == null || v === "") return [];
    return String(v)
      .split(/[,;|\n]+/)
      .map(function (s) {
        return nz(s);
      })
      .filter(Boolean);
  }

  function wrapSection(title, intro, bodyHtml, extraClass) {
    if (!bodyHtml) return "";
    return (
      '<section class="section oe-infra-section' +
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

  function stackCard(row) {
    if (!row || !nz(row.title)) return "";
    var title = formatCardHeading(row.title);
    var examples = nz(row.examples);
    return (
      '<div class="card oe-infra-stack-card">' +
      "<h3>" +
      escapeHtml(title) +
      "</h3>" +
      "<p>" +
      escapeHtml(row.description || "") +
      "</p>" +
      (examples
        ? '<p class="oe-infra-examples"><span class="oe-lead-chip">' +
          escapeHtml(examples) +
          "</span></p>"
        : "") +
      "</div>"
    );
  }

  function domainTagFromTitle(title) {
    var words = nz(title).split(/\s+/).filter(Boolean);
    if (!words.length) return "";
    if (words.length === 1) return words[0].slice(0, 3).toUpperCase();
    return (words[0].charAt(0) + words[1].charAt(0)).toUpperCase();
  }

  function serviceCard(row) {
    if (!row || !nz(row.title)) return "";
    var title = formatCardHeading(row.title);
    return (
      '<div class="card oe-infra-service-card">' +
      '<span class="oe-infra-service-card__icon" aria-hidden="true"></span>' +
      "<div class=\"oe-infra-service-card__body\">" +
      "<h3>" +
      escapeHtml(title) +
      "</h3>" +
      "<p>" +
      escapeHtml(row.description || "") +
      "</p></div></div>"
    );
  }

  function domainCard(row) {
    if (!row || !nz(row.title)) return "";
    var title = formatCardHeading(row.title);
    var items = arrayish(row.items);
    if (!items.length && nz(row.description)) {
      items = arrayish(row.description);
    }
    var listHtml = items.length
      ? "<ul class=\"oe-infra-domain-card__list\">" +
        items
          .map(function (item) {
            return "<li>" + escapeHtml(item) + "</li>";
          })
          .join("") +
        "</ul>"
      : "";
    return (
      '<div class="card oe-infra-domain-card">' +
      '<span class="oe-infra-domain-card__tag">' +
      escapeHtml(domainTagFromTitle(row.title)) +
      "</span>" +
      "<div class=\"oe-infra-domain-card__body\">" +
      "<h3>" +
      escapeHtml(title) +
      "</h3>" +
      listHtml +
      "</div></div>"
    );
  }

  function governanceCard(row) {
    if (!row || !nz(row.title)) return "";
    var title = formatCardHeading(row.title);
    return (
      '<div class="card oe-infra-governance-card">' +
      '<span class="oe-infra-governance-card__icon" aria-hidden="true"></span>' +
      "<div class=\"oe-infra-governance-card__body\">" +
      "<h3>" +
      escapeHtml(title) +
      "</h3>" +
      "<p>" +
      escapeHtml(row.description || "") +
      "</p></div></div>"
    );
  }

  function analyticsCard(row) {
    if (!row || !nz(row.title)) return "";
    var title = formatCardHeading(row.title);
    return (
      '<div class="card oe-lead-cadence-card oe-infra-icon-card">' +
      '<span class="oe-lead-cadence-card__icon oe-infra-cadence-icon" aria-hidden="true"></span>' +
      "<div>" +
      "<h3>" +
      escapeHtml(title) +
      "</h3>" +
      "<p>" +
      escapeHtml(row.description || "") +
      "</p></div></div>"
    );
  }

  function buildTechnologyStackSection(vm) {
    var rows = sectionRows(vm, "technologyStack", FIELD.technologyStack);
    if (!rows.length) return "";
    return wrapSection(
      "Technology Platform Stack",
      "The core operating, commercial, financial, and reporting systems that support the owner relationship.",
      '<div class="grid-3 oe-infra-stack-grid">' + rows.map(stackCard).join("") + "</div>"
    );
  }

  function buildServicesOfferedSection(vm) {
    var rows = sectionRows(vm, "infrastructureServices", FIELD.servicesOffered);
    if (!rows.length) return "";
    return wrapSection(
      "Infrastructure Services Offered",
      "Technical and operational support services the operator may provide during onboarding, transition, and ongoing management.",
      '<div class="grid-3 oe-infra-service-grid">' + rows.map(serviceCard).join("") + "</div>",
      "oe-infra-section--services"
    );
  }

  function buildDataDomainsSection(vm) {
    var rows = sectionRows(vm, "dataDomains", FIELD.dataDomains);
    if (!rows.length) return "";
    return wrapSection(
      "Data Domains Captured",
      "The main categories of information owners may receive through reports, dashboards, portals, or review meetings.",
      '<div class="grid-3 oe-infra-domain-grid">' + rows.map(domainCard).join("") + "</div>",
      "oe-infra-section--domains"
    );
  }

  function buildDataGovernanceSection(vm) {
    var rows = sectionRows(vm, "dataGovernance", FIELD.dataGovernance);
    if (!rows.length) return "";
    return wrapSection(
      "Data Governance, Security & Controls",
      "How the operator should manage sensitive owner, property, brand, and financial information.",
      '<div class="grid-3 oe-infra-governance-grid">' +
        rows.map(governanceCard).join("") +
        "</div>",
      "oe-infra-section--governance"
    );
  }

  function buildAnalyticsSection(vm) {
    var rows = sectionRows(vm, "analyticsCapabilities", FIELD.analyticsSupport);
    if (!rows.length) return "";
    return wrapSection(
      "Analytics & Decision Support",
      "The insights layer that helps owners move from reporting to better decisions.",
      '<div class="grid-3 oe-infra-icon-grid">' + rows.map(analyticsCard).join("") + "</div>"
    );
  }

  function buildMaturitySection(vm) {
    var intro =
      "A practical way for owners to understand how advanced the operator's infrastructure and data capability is.";
    var assessment = resolveMaturityAssessment(vm);
    if (!assessment) {
      return wrapSection(
        "Technology Maturity View",
        intro,
        '<p class="gold-mock-tab-empty">Technology maturity assessment not yet available from Operator Setup.</p>',
        "oe-infra-section--maturity"
      );
    }
    var evidenceHtml = assessment.evidence.length
      ? '<ul class="oe-infra-maturity-card__evidence">' +
        assessment.evidence
          .map(function (item) {
            return "<li>" + escapeHtml(item) + "</li>";
          })
          .join("") +
        "</ul>"
      : "";
    var body =
      '<div class="card oe-value-kpi oe-infra-maturity-card">' +
      '<div class="oe-value-kpi__value">' +
      escapeHtml(assessment.level) +
      "</div>" +
      '<h3 class="oe-value-kpi__label">Current<br>Maturity Level</h3>' +
      "<p>" +
      escapeHtml(assessment.description) +
      "</p>" +
      evidenceHtml +
      "</div>";
    return wrapSection(
      "Technology Maturity View",
      intro,
      body,
      "oe-infra-section--maturity"
    );
  }

  function buildAllSectionsHtml(vm) {
    return (
      buildTechnologyStackSection(vm) +
      buildServicesOfferedSection(vm) +
      buildDataDomainsSection(vm) +
      buildDataGovernanceSection(vm) +
      buildAnalyticsSection(vm) +
      buildMaturitySection(vm)
    );
  }

  global.OperatorInfrastructureSections = {
    buildAllSectionsHtml: buildAllSectionsHtml,
    FIELD: FIELD,
    DEFAULTS: DEFAULTS,
    MATURITY_RUBRIC: MATURITY_RUBRIC,
    MATURITY_LEVEL_KEYS: MATURITY_LEVEL_KEYS,
    resolveMaturityAssessment: resolveMaturityAssessment,
  };
})(typeof globalThis !== "undefined" ? globalThis : global);
