/**
 * Operator Setup — Engagement & Reporting repeaters (child table).
 * Owner value cards stay on main form fields; repeaters collect Explorer subsection rows.
 */
(function (global) {
  "use strict";

  var CADENCE_OPTIONS = ["Weekly", "Monthly", "Quarterly", "Annually", "Ad hoc", ""];

  var REPEATER_SECTIONS = [
    {
      key: "strategicOwnerValue",
      title: "Strategic Owner Value",
      hint: "What owners gain beyond day-to-day operations—one pillar per row.",
      addLabel: "+ Add value pillar",
      payloadKey: "strategicOwnerValue",
      fields: [
        { name: "title", label: "Title", type: "text", span: 2 },
        { name: "description", label: "Description", type: "textarea", rows: 2, span: 2 },
      ],
      emptyRow: function () {
        return { title: "", description: "" };
      },
    },
    {
      key: "engagementCadence",
      title: "Owner Engagement Cadence",
      hint: "Touchpoints and review rhythm—cadence, type, and focus.",
      addLabel: "+ Add cadence row",
      payloadKey: "engagementCadence",
      fields: [
        {
          name: "cadence",
          label: "Cadence",
          type: "select",
          options: CADENCE_OPTIONS.filter(Boolean),
          span: 1,
        },
        { name: "engagementType", label: "Engagement type", type: "text", span: 1 },
        { name: "focus", label: "Typical focus", type: "textarea", rows: 2, span: 2 },
      ],
      emptyRow: function () {
        return { cadence: "", engagementType: "", focus: "" };
      },
    },
    {
      key: "controlsGovernance",
      title: "Controls & Governance",
      hint: "Budget, capex, approvals, and escalation practices.",
      addLabel: "+ Add control row",
      payloadKey: "controlsGovernance",
      fields: [
        { name: "title", label: "Topic", type: "text", span: 2 },
        { name: "description", label: "Description", type: "textarea", rows: 2, span: 2 },
      ],
      emptyRow: function () {
        return { title: "", description: "" };
      },
    },
    {
      key: "reportsReceived",
      title: "Reports Owners Receive",
      hint: "Reporting packages and decision-support materials.",
      addLabel: "+ Add report row",
      payloadKey: "reportsReceived",
      fields: [
        { name: "title", label: "Report / pack", type: "text", span: 2 },
        { name: "description", label: "Description", type: "textarea", rows: 2, span: 2 },
      ],
      emptyRow: function () {
        return { title: "", description: "" };
      },
    },
    {
      key: "ownerTools",
      title: "Owner Tools & Support Channels",
      hint: "Portal, dashboards, libraries, and owner education.",
      addLabel: "+ Add tool row",
      payloadKey: "ownerTools",
      fields: [
        { name: "title", label: "Tool / channel", type: "text", span: 2 },
        { name: "description", label: "Description", type: "textarea", rows: 2, span: 2 },
      ],
      emptyRow: function () {
        return { title: "", description: "" };
      },
    },
    {
      key: "lifecycleSupport",
      title: "Lifecycle Support",
      hint: "Support across evaluation through exit.",
      addLabel: "+ Add lifecycle row",
      payloadKey: "lifecycleSupport",
      fields: [
        { name: "stage", label: "Stage", type: "text", span: 1 },
        { name: "support", label: "Support provided", type: "textarea", rows: 2, span: 2 },
      ],
      emptyRow: function () {
        return { stage: "", support: "" };
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
    var name = "er_" + sectionKey + "_" + rowIndex + "_" + field.name;
    var spanClass = field.span === 2 ? " er-field--span2" : "";
    var val = esc(value == null ? "" : value);
    if (field.type === "textarea") {
      return (
        '<div class="er-field' +
        spanClass +
        '"><label>' +
        esc(field.label) +
        '</label><textarea data-er-field="' +
        esc(field.name) +
        '" rows="' +
        (field.rows || 2) +
        '">' +
        val +
        "</textarea></div>"
      );
    }
    if (field.type === "select") {
      var opts = (field.options || [])
        .map(function (o) {
          var sel = val === esc(o) ? " selected" : "";
          return '<option value="' + esc(o) + '"' + sel + ">" + esc(o) + "</option>";
        })
        .join("");
      return (
        '<div class="er-field' +
        spanClass +
        '"><label>' +
        esc(field.label) +
        '</label><select data-er-field="' +
        esc(field.name) +
        '"><option value=""></option>' +
        opts +
        "</select></div>"
      );
    }
    return (
      '<div class="er-field' +
      spanClass +
      '"><label>' +
      esc(field.label) +
      '</label><input type="text" data-er-field="' +
      esc(field.name) +
      '" value="' +
      val +
      '" /></div>'
    );
  }

  function rowHtml(section, rowIndex, data) {
    data = data || section.emptyRow();
    var fieldsHtml = section.fields.map(function (f) {
      return fieldHtml(section.key, rowIndex, f, data[f.name]);
    }).join("");
    return (
      '<div class="er-row" data-er-section="' +
      esc(section.key) +
      '">' +
      fieldsHtml +
      '<div class="er-row-actions"><button type="button" class="btn btn-secondary btn-sm er-remove-row">Remove</button></div></div>'
    );
  }

  function sectionShell(section) {
    return (
      '<div class="er-section-block" data-er-section-block="' +
      esc(section.key) +
      '">' +
      "<h4>" +
      esc(section.title) +
      "</h4>" +
      '<p class="er-section-hint">' +
      esc(section.hint) +
      "</p>" +
      '<div class="er-rows"></div>' +
      '<button type="button" class="btn btn-secondary btn-sm er-add-row">' +
      esc(section.addLabel) +
      "</button></div>"
    );
  }

  function mount(root) {
    if (!root) return;
    root.innerHTML =
      '<div class="engagement-reporting-setup">' +
      REPEATER_SECTIONS.map(sectionShell).join("") +
      '<p class="subsection-hint er-actions">' +
      '<button type="button" class="btn btn-secondary btn-sm" id="erLoadHeCalaBtn">Load HE CALA draft rows</button>' +
      "</p></div>";

    REPEATER_SECTIONS.forEach(function (section) {
      var block = root.querySelector('[data-er-section-block="' + section.key + '"]');
      if (!block) return;
      var rowsEl = block.querySelector(".er-rows");
      var addBtn = block.querySelector(".er-add-row");
      function addRow(data) {
        var idx = rowsEl.querySelectorAll(".er-row").length;
        rowsEl.insertAdjacentHTML("beforeend", rowHtml(section, idx, data || section.emptyRow()));
      }
      if (addBtn) addBtn.addEventListener("click", function () { addRow(); });
      rowsEl.addEventListener("click", function (e) {
        var btn = e.target && e.target.closest && e.target.closest(".er-remove-row");
        if (!btn) return;
        var row = btn.closest(".er-row");
        if (row && rowsEl.querySelectorAll(".er-row").length > 1) row.remove();
      });
      addRow();
    });

    var heBtn = root.querySelector("#erLoadHeCalaBtn");
    if (heBtn) {
      heBtn.addEventListener("click", function () {
        fetch("/fixtures/operator-engagement-explorer-he-cala.json")
          .then(function (r) { return r.json(); })
          .then(function (d) {
            if (d.engagementReporting) applyPlatformData(d.engagementReporting);
            if (d.commercialFields) applyCommercialScalarFields(d.commercialFields);
          })
          .catch(function (e) {
            console.warn("[engagement-reporting-setup] HE CALA load failed", e);
          });
      });
    }
  }

  function applyCommercialScalarFields(scalars) {
    if (!scalars || typeof scalars !== "object") return;
    Object.keys(scalars).forEach(function (key) {
      var el = document.querySelector('[name="' + key + '"]');
      if (el && scalars[key] != null) el.value = String(scalars[key]);
    });
  }

  function readRow(rowEl, section) {
    var out = {};
    section.fields.forEach(function (f) {
      var el = rowEl.querySelector('[data-er-field="' + f.name + '"]');
      out[f.name] = el ? el.value : "";
    });
    return out;
  }

  function isRowEmpty(row, section) {
    return section.fields.every(function (f) {
      return !nz(row[f.name]);
    });
  }

  function collectRepeaterSection(sectionKey) {
    var section = REPEATER_SECTIONS.find(function (s) { return s.key === sectionKey; });
    if (!section) return [];
    var block = document.querySelector('[data-er-section-block="' + sectionKey + '"]');
    if (!block) return [];
    var rows = [];
    block.querySelectorAll(".er-row").forEach(function (rowEl) {
      var row = readRow(rowEl, section);
      if (!isRowEmpty(row, section)) rows.push(row);
    });
    return rows;
  }

  function collectEngagementReportingFromForm(form) {
    var out = {};
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
      var block = document.querySelector('[data-er-section-block="' + section.key + '"]');
      if (!block) return;
      var rowsEl = block.querySelector(".er-rows");
      if (!rowsEl) return;
      rowsEl.innerHTML = "";
      var rows = list.length ? list : [section.emptyRow()];
      rows.forEach(function (row, idx) {
        var copy = Object.assign({}, row);
        if (section.key === "lifecycleSupport") {
          copy.stage = copy.stage || copy.title || "";
          copy.support = copy.support || copy.description || "";
        }
        rowsEl.insertAdjacentHTML("beforeend", rowHtml(section, idx, copy));
      });
    });
  }

  function applyEngagementReportingPrefill(platform) {
    applyPlatformData(platform);
  }

  global.collectEngagementReportingFromForm = collectEngagementReportingFromForm;
  global.applyEngagementReportingPrefill = applyEngagementReportingPrefill;

  document.addEventListener("DOMContentLoaded", function () {
    var root = document.getElementById("engagement-reporting-setup-root");
    if (root) mount(root);
  });
})(typeof window !== "undefined" ? window : global);
