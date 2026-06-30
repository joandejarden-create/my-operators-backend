/**
 * Operator Setup — Infrastructure Platform repeaters (child table: Operator Setup - Infrastructure Platform).
 * Decision signals + portfolio KPIs stay on existing form fields; this module collects structured repeater rows.
 */
(function (global) {
  "use strict";

  var DECISION_SIGNAL_SPECS = [
    { rowKey: "infra_signal_uptime", title: "Platform Uptime" },
    { rowKey: "infra_signal_incident", title: "Critical Incident Response" },
    { rowKey: "infra_signal_adoption", title: "System Adoption (Portfolio)" },
    { rowKey: "infra_signal_refresh", title: "Data Refresh Cadence" },
    { rowKey: "risk_signal_audit", title: "Audit Pass Consistency" },
    { rowKey: "risk_signal_bcp", title: "BCP Test Frequency" },
    { rowKey: "risk_signal_control", title: "Control Closure Rate" },
    { rowKey: "risk_signal_insurance", title: "Insurance Adequacy Review" },
  ];

  var PORTFOLIO_METRIC_SPECS = [
    { rowKey: "infra_kpi_reporting", title: "Reporting Systems" },
    { rowKey: "infra_kpi_revenue", title: "Revenue Systems" },
    { rowKey: "infra_kpi_exec", title: "Execution Platform" },
    { rowKey: "infra_kpi_tools", title: "Owner Tools" },
  ];

  var MATURITY_LEVELS = ["Basic", "Structured", "Integrated", "Advanced"];

  var REPEATER_SECTIONS = [
    {
      key: "technologyStack",
      title: "Technology Stack",
      hint: "Product layers and integrations—title, description, examples.",
      addLabel: "+ Add stack row",
      payloadKey: "technologyStack",
      fields: [
        { name: "title", label: "System / layer", type: "text", placeholder: "e.g. PMS", span: 1 },
        { name: "examples", label: "Product / vendor", type: "text", placeholder: "e.g. Oracle OPERA Cloud", span: 1 },
        { name: "description", label: "Notes", type: "textarea", rows: 2, span: 2 },
      ],
      emptyRow: function () {
        return { title: "", examples: "", description: "" };
      },
    },
    {
      key: "infrastructureServices",
      title: "Infrastructure Services",
      hint: "Services delivered with your platform stack.",
      addLabel: "+ Add service row",
      payloadKey: "infrastructureServices",
      fields: [
        { name: "title", label: "Service", type: "text", placeholder: "e.g. Revenue management", span: 2 },
        { name: "description", label: "Description", type: "textarea", rows: 2, span: 2 },
      ],
      emptyRow: function () {
        return { title: "", description: "" };
      },
    },
    {
      key: "dataDomains",
      title: "Data Domains",
      hint: "Data areas owners care about—one domain per row.",
      addLabel: "+ Add data domain",
      payloadKey: "dataDomains",
      fields: [
        { name: "title", label: "Domain title", type: "text", placeholder: "e.g. Owner Reporting", span: 2 },
        {
          name: "items",
          label: "Items (comma-separated)",
          type: "text",
          placeholder: "Weekly flash, Monthly pack, Dashboards",
          span: 2,
        },
        { name: "description", label: "Notes", type: "textarea", rows: 2, span: 2 },
      ],
      emptyRow: function () {
        return { title: "", items: "", description: "" };
      },
    },
    {
      key: "dataGovernance",
      title: "Data Governance",
      hint: "Policies, controls, and stewardship practices.",
      addLabel: "+ Add governance row",
      payloadKey: "dataGovernance",
      fields: [
        { name: "title", label: "Topic", type: "text", placeholder: "e.g. Data retention", span: 2 },
        { name: "description", label: "Description", type: "textarea", rows: 2, span: 2 },
      ],
      emptyRow: function () {
        return { title: "", description: "" };
      },
    },
    {
      key: "analyticsCapabilities",
      title: "Analytics Capabilities",
      hint: "What analytics and reporting you deliver to owners.",
      addLabel: "+ Add analytics row",
      payloadKey: "analyticsCapabilities",
      fields: [
        { name: "title", label: "Capability", type: "text", placeholder: "e.g. Portfolio benchmarking", span: 2 },
        { name: "description", label: "Description", type: "textarea", rows: 2, span: 2 },
      ],
      emptyRow: function () {
        return { title: "", description: "" };
      },
    },
  ];

  function nz(v) {
    return v != null && String(v).trim() !== "" ? String(v).trim() : "";
  }

  function esc(s) {
    return String(s == null ? "" : s)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/"/g, "&quot;");
  }

  function fieldHtml(sectionKey, rowIndex, field, value) {
    var id = "ip_" + sectionKey + "_" + rowIndex + "_" + field.name;
    var val = value != null ? value : "";
    if (field.type === "select") {
      var opts = (field.options || [])
        .map(function (o) {
          return (
            '<option value="' +
            esc(o) +
            '"' +
            (String(val) === String(o) ? " selected" : "") +
            ">" +
            esc(o) +
            "</option>"
          );
        })
        .join("");
      return (
        '<label class="form-label" for="' +
        id +
        '">' +
        esc(field.label) +
        '</label><select class="form-select" id="' +
        id +
        '" data-ip-field="' +
        esc(field.name) +
        '">' +
        opts +
        "</select>"
      );
    }
    if (field.type === "textarea") {
      return (
        '<label class="form-label" for="' +
        id +
        '">' +
        esc(field.label) +
        '</label><textarea class="form-textarea" id="' +
        id +
        '" rows="' +
        (field.rows || 2) +
        '" data-ip-field="' +
        esc(field.name) +
        '" placeholder="' +
        esc(field.placeholder || "") +
        '">' +
        esc(val) +
        "</textarea>"
      );
    }
    return (
      '<label class="form-label" for="' +
      id +
      '">' +
      esc(field.label) +
      '</label><input class="form-input" type="text" id="' +
      id +
      '" data-ip-field="' +
      esc(field.name) +
      '" value="' +
      esc(val) +
      '" placeholder="' +
      esc(field.placeholder || "") +
      '" />'
    );
  }

  function rowHtml(section, rowIndex, data) {
    var fields = section.fields
      .map(function (f) {
        var span = f.span === 2 ? " ip-field-span-2" : "";
        return (
          '<div class="ip-field' +
          span +
          '">' +
          fieldHtml(section.key, rowIndex, f, data && data[f.name]) +
          "</div>"
        );
      })
      .join("");
    return (
      '<div class="ip-row" data-ip-row="' +
      rowIndex +
      '"><button type="button" class="filter-reset-btn btn-remove-row ip-remove-row" aria-label="Remove row">Remove</button><div class="ip-field-grid">' +
      fields +
      "</div></div>"
    );
  }

  function sectionShell(section) {
    return (
      '<section class="ip-section" data-ip-section-block="' +
      esc(section.key) +
      '">' +
      '<h4 class="project-fit-subheader">' +
      esc(section.title) +
      "</h4>" +
      '<p class="subsection-hint">' +
      esc(section.hint) +
      '</p><div class="ip-rows"></div>' +
      '<button type="button" class="btn btn-secondary btn-sm ip-add-row" data-ip-add="' +
      esc(section.key) +
      '">' +
      esc(section.addLabel) +
      "</button></section>"
    );
  }

  var JSON_GOVERNANCE_KEYS = [
    "infra_technology_stack_json",
    "infra_services_offered_json",
    "infra_data_domains_json",
    "infra_data_governance_json",
    "infra_analytics_support_json",
  ];

  function applyScalarFields(governanceFields) {
    var g = governanceFields || {};
    Object.keys(g).forEach(function (key) {
      if (JSON_GOVERNANCE_KEYS.indexOf(key) !== -1) return;
      var el = document.querySelector('[name="' + key + '"]');
      if (!el || g[key] == null || g[key] === "") return;
      el.value = String(g[key]);
      try {
        el.dispatchEvent(new Event("input", { bubbles: true }));
        el.dispatchEvent(new Event("change", { bubbles: true }));
      } catch (e) { /* ignore */ }
    });
  }

  function mount(root) {
    if (!root) return;
    root.innerHTML =
      '<div class="infrastructure-platform-setup">' +
      REPEATER_SECTIONS.map(sectionShell).join("") +
      '<p class="subsection-hint ip-actions">' +
      '<button type="button" class="btn btn-secondary btn-sm" id="infraPlatformLoadHeCalaBtn">Load HE CALA draft rows</button>' +
      "</p></div>";

    REPEATER_SECTIONS.forEach(function (section) {
      var block = root.querySelector('[data-ip-section-block="' + section.key + '"]');
      if (!block) return;
      var rowsEl = block.querySelector(".ip-rows");
      var addBtn = block.querySelector(".ip-add-row");
      function addRow(data) {
        var idx = rowsEl.querySelectorAll(".ip-row").length;
        rowsEl.insertAdjacentHTML("beforeend", rowHtml(section, idx, data || section.emptyRow()));
      }
      if (addBtn) addBtn.addEventListener("click", function () { addRow(); });
      rowsEl.addEventListener("click", function (e) {
        var btn = e.target && e.target.closest && e.target.closest(".ip-remove-row");
        if (!btn) return;
        var row = btn.closest(".ip-row");
        if (row && rowsEl.querySelectorAll(".ip-row").length > 1) row.remove();
      });
      addRow();
    });

    var heBtn = root.querySelector("#infraPlatformLoadHeCalaBtn");
    if (heBtn) {
      heBtn.addEventListener("click", function () {
        fetch("/fixtures/operator-infrastructure-explorer-he-cala.json")
          .then(function (r) { return r.json(); })
          .then(function (d) {
            if (d.infrastructurePlatform) applyPlatformData(d.infrastructurePlatform);
            if (d.governanceFields) applyScalarFields(d.governanceFields);
          })
          .catch(function (e) {
            console.warn("[infrastructure-platform-setup] HE CALA load failed", e);
          });
      });
    }
  }

  function readFieldValue(form, name) {
    var el = form.querySelector('[name="' + name + '"]');
    if (!el) return "";
    return nz(el.value);
  }

  function meaningfulSignal(value) {
    if (!value) return false;
    return !/^not measured\s*\/\s*n\/?a$/i.test(value);
  }

  function collectDecisionSignals(form) {
    var out = [];
    DECISION_SIGNAL_SPECS.forEach(function (spec) {
      var value = readFieldValue(form, spec.rowKey);
      if (!meaningfulSignal(value)) return;
      out.push({ rowKey: spec.rowKey, title: spec.title, value: value });
    });
    return out;
  }

  function collectPortfolioMetrics(form) {
    var out = [];
    PORTFOLIO_METRIC_SPECS.forEach(function (spec) {
      var value = readFieldValue(form, spec.rowKey);
      if (!value) return;
      out.push({ rowKey: spec.rowKey, title: spec.title, value: value });
    });
    return out;
  }

  function collectTechnologyMaturity(form) {
    var level = readFieldValue(form, "infra_technology_maturity_level");
    if (!level || MATURITY_LEVELS.indexOf(level) === -1) return null;
    return { level: level, summary: "" };
  }

  function readRow(rowEl, section) {
    var out = {};
    section.fields.forEach(function (f) {
      var el = rowEl.querySelector('[data-ip-field="' + f.name + '"]');
      out[f.name] = el ? el.value : "";
    });
    if (section.key === "dataDomains" && out.items) {
      out.items = String(out.items)
        .split(/[,;\n|]+/)
        .map(function (t) { return nz(t); })
        .filter(Boolean);
    }
    return out;
  }

  function isRowEmpty(row, section) {
    return section.fields.every(function (f) {
      if (f.name === "items" && Array.isArray(row.items)) return !row.items.length;
      return !nz(row[f.name]);
    });
  }

  function collectRepeaterSection(sectionKey) {
    var section = REPEATER_SECTIONS.find(function (s) { return s.key === sectionKey; });
    if (!section) return [];
    var block = document.querySelector('[data-ip-section-block="' + sectionKey + '"]');
    if (!block) return [];
    var rows = [];
    block.querySelectorAll(".ip-row").forEach(function (rowEl) {
      var row = readRow(rowEl, section);
      if (!isRowEmpty(row, section)) rows.push(row);
    });
    return rows;
  }

  function collectInfrastructurePlatformFromForm(form) {
    form = form || document.querySelector("form") || document;
    var out = {
      decisionSignals: collectDecisionSignals(form),
      portfolioMetrics: collectPortfolioMetrics(form),
      technologyMaturity: collectTechnologyMaturity(form),
    };
    REPEATER_SECTIONS.forEach(function (section) {
      out[section.payloadKey] = collectRepeaterSection(section.key);
    });
    return out;
  }

  function applyPlatformData(platform) {
    if (!platform || typeof platform !== "object") return;
    REPEATER_SECTIONS.forEach(function (section) {
      var list = platform[section.payloadKey];
      if (!Array.isArray(list)) return;
      var block = document.querySelector('[data-ip-section-block="' + section.key + '"]');
      if (!block) return;
      var rowsEl = block.querySelector(".ip-rows");
      if (!rowsEl) return;
      rowsEl.innerHTML = "";
      var rows = list.length ? list : [section.emptyRow()];
      rows.forEach(function (row, idx) {
        var copy = Object.assign({}, row);
        if (section.key === "dataDomains" && Array.isArray(copy.items)) {
          copy.items = copy.items.join(", ");
        }
        rowsEl.insertAdjacentHTML("beforeend", rowHtml(section, idx, copy));
      });
    });
  }

  function applyInfrastructurePlatformPrefill(platform) {
    applyPlatformData(platform);
  }

  global.collectInfrastructurePlatformFromForm = collectInfrastructurePlatformFromForm;
  global.applyInfrastructurePlatformPrefill = applyInfrastructurePlatformPrefill;

  document.addEventListener("DOMContentLoaded", function () {
    var root = document.getElementById("infrastructure-platform-setup-root");
    if (root) mount(root);
  });
})(typeof window !== "undefined" ? window : global);
