/**
 * Operator Setup — Leadership Platform form repeaters (child table: Operator Setup - Leadership Platform).
 * Replaces raw JSON textareas with labeled rows; collects to body.leadershipPlatform on submit.
 */
(function (global) {
  "use strict";

  var DEPTH_OPTIONS = ["Strong", "Very Strong", "Moderate / Strong", "Emerging / Strong"];

  var SECTIONS = [
    {
      key: "orgStructure",
      title: "Organization Structure",
      hint: "How the operator is organized from division leadership through property execution.",
      addLabel: "+ Add org layer",
      fields: [
        { name: "title", label: "Layer title", type: "text", placeholder: "e.g. CALA Division Leadership", span: 2 },
        { name: "description", label: "Description", type: "textarea", rows: 2, span: 2 },
        { name: "tags", label: "Tags (comma-separated)", type: "text", placeholder: "Miami hub, Owner escalation", span: 2 },
      ],
      emptyRow: function () {
        return { title: "", description: "", tags: "" };
      },
    },
    {
      key: "teamDepth",
      title: "Team Depth by Function",
      hint: "Support bench behind key owner priorities — function, lead, depth, relevance.",
      addLabel: "+ Add function row",
      fields: [
        { name: "function", label: "Function", type: "text", placeholder: "Operations", span: 1 },
        { name: "leadRole", label: "Lead / bench", type: "text", placeholder: "Name or role label", span: 1 },
        {
          name: "depth",
          label: "Team depth",
          type: "select",
          options: DEPTH_OPTIONS,
          span: 1,
        },
        { name: "relevance", label: "Owner relevance", type: "textarea", rows: 2, span: 2 },
      ],
      emptyRow: function () {
        return { function: "", leadRole: "", depth: "Strong", relevance: "" };
      },
    },
    {
      key: "languages",
      title: "Language & Regional Capability",
      hint: "Language coverage and how it supports owner communication.",
      addLabel: "+ Add language",
      fields: [
        { name: "language", label: "Language", type: "text", placeholder: "Spanish", span: 1 },
        { name: "proficiency", label: "Proficiency", type: "text", placeholder: "Fluent", span: 1 },
        { name: "support", label: "Owner support", type: "textarea", rows: 2, span: 2 },
      ],
      emptyRow: function () {
        return { language: "", proficiency: "", support: "" };
      },
    },
    {
      key: "governanceCadence",
      title: "Governance & Communication Cadence",
      hint: "How the team communicates with ownership.",
      addLabel: "+ Add cadence item",
      fields: [
        { name: "title", label: "Cadence title", type: "text", placeholder: "Monthly: Business Review", span: 2 },
        { name: "description", label: "Description", type: "textarea", rows: 2, span: 2 },
      ],
      emptyRow: function () {
        return { title: "", description: "" };
      },
    },
    {
      key: "teamMarkets",
      title: "Team Experience Markets",
      hint: "Markets where the leadership bench has credible experience.",
      addLabel: "+ Add market",
      fields: [
        { name: "market", label: "Market", type: "text", placeholder: "Mexico", span: 1 },
        { name: "leaders", label: "Relevant leaders (comma-separated)", type: "text", span: 1 },
        { name: "experience", label: "Team experience", type: "textarea", rows: 2, span: 2 },
      ],
      emptyRow: function () {
        return { market: "", experience: "", leaders: "" };
      },
    },
    {
      key: "ownerRelationship",
      title: "Owner Relationship Model",
      hint: "How an owner interacts during evaluation, onboarding, and ongoing management.",
      addLabel: "+ Add relationship row",
      fields: [
        { name: "title", label: "Touchpoint", type: "text", placeholder: "Primary Owner Contact", span: 1 },
        { name: "value", label: "Lead / function", type: "text", placeholder: "Name or role", span: 1 },
        { name: "description", label: "Description", type: "textarea", rows: 2, span: 2 },
      ],
      emptyRow: function () {
        return { title: "", value: "", description: "" };
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
    var id = "lp_" + sectionKey + "_" + rowIndex + "_" + field.name;
    var val = value != null ? value : "";
    if (field.type === "select") {
      var opts = (field.options || [])
        .map(function (o) {
          return (
            '<option value="' +
            esc(o) +
            '"' +
            (String(val) === o ? " selected" : "") +
            ">" +
            esc(o) +
            "</option>"
          );
        })
        .join("");
      return (
        '<div class="field-wrap lp-field lp-span-' +
        (field.span || 1) +
        '"><label class="form-label label-spacing" for="' +
        id +
        '">' +
        esc(field.label) +
        '</label><select class="form-select" data-lp-field="' +
        esc(field.name) +
        '" id="' +
        id +
        '">' +
        opts +
        "</select></div>"
      );
    }
    if (field.type === "textarea") {
      return (
        '<div class="field-wrap lp-field lp-span-' +
        (field.span || 1) +
        '"><label class="form-label label-spacing" for="' +
        id +
        '">' +
        esc(field.label) +
        '</label><textarea class="form-textarea explorer-story-field" rows="' +
        (field.rows || 2) +
        '" data-lp-field="' +
        esc(field.name) +
        '" id="' +
        id +
        '">' +
        esc(val) +
        "</textarea></div>"
      );
    }
    return (
      '<div class="field-wrap lp-field lp-span-' +
      (field.span || 1) +
      '"><label class="form-label label-spacing" for="' +
      id +
      '">' +
      esc(field.label) +
      '</label><input class="form-input" type="text" data-lp-field="' +
      esc(field.name) +
      '" id="' +
      id +
      '" value="' +
      esc(val) +
      '" placeholder="' +
      esc(field.placeholder || "") +
      '" /></div>'
    );
  }

  function rowHtml(section, rowIndex, rowData) {
    var fields = section.fields
      .map(function (f) {
        return fieldHtml(section.key, rowIndex, f, rowData && rowData[f.name]);
      })
      .join("");
    return (
      '<div class="lp-row" data-lp-section="' +
      esc(section.key) +
      '" data-lp-row="' +
      rowIndex +
      '">' +
      '<button type="button" class="filter-reset-btn btn-remove-row lp-remove-row">Remove</button>' +
      '<div class="lp-row-grid">' +
      fields +
      "</div></div>"
    );
  }

  function sectionShell(section) {
    return (
      '<section class="lp-section" data-lp-section-block="' +
      esc(section.key) +
      '">' +
      "<h4 class=\"project-fit-subheader\">" +
      esc(section.title) +
      "</h4>" +
      '<p class="subsection-hint">' +
      esc(section.hint) +
      '</p><div class="lp-rows"></div>' +
      '<button type="button" class="btn btn-secondary btn-sm lp-add-row" data-lp-add="' +
      esc(section.key) +
      '">' +
      esc(section.addLabel) +
      "</button></section>"
    );
  }

  function mount(root) {
    if (!root) return;
    root.innerHTML =
      '<div class="leadership-platform-setup">' +
      SECTIONS.map(sectionShell).join("") +
      '<p class="subsection-hint lp-actions">' +
      '<button type="button" class="btn btn-secondary btn-sm" id="leadPlatformLoadHeCalaBtn">Load HE CALA draft rows</button>' +
      "</p></div>";

    SECTIONS.forEach(function (section) {
      var block = root.querySelector('[data-lp-section-block="' + section.key + '"]');
      if (!block) return;
      var rowsEl = block.querySelector(".lp-rows");
      var addBtn = block.querySelector(".lp-add-row");
      function addRow(data) {
        var idx = rowsEl.querySelectorAll(".lp-row").length;
        rowsEl.insertAdjacentHTML("beforeend", rowHtml(section, idx, data || section.emptyRow()));
      }
      if (addBtn) addBtn.addEventListener("click", function () { addRow(); });
      rowsEl.addEventListener("click", function (e) {
        var btn = e.target && e.target.closest && e.target.closest(".lp-remove-row");
        if (!btn) return;
        var row = btn.closest(".lp-row");
        if (row && rowsEl.querySelectorAll(".lp-row").length > 1) row.remove();
      });
      addRow();
    });

    var heBtn = root.querySelector("#leadPlatformLoadHeCalaBtn");
    if (heBtn) {
      heBtn.addEventListener("click", function () {
        fetch("/fixtures/operator-leadership-explorer-he-cala.json")
          .then(function (r) { return r.json(); })
          .then(function (d) {
            applyPlatformData({
              orgStructure: d.lead_org_structure_json || [],
              teamDepth: d.lead_team_depth_json || [],
              languages: d.lead_language_capability_json || [],
              governanceCadence: d.lead_governance_cadence_json || [],
              teamMarkets: d.lead_team_markets_json || [],
              ownerRelationship: d.lead_owner_relationship_json || [],
            });
            var avg = document.getElementById("lead_avg_hospitality_experience");
            if (avg && d.lead_avg_hospitality_experience) avg.value = d.lead_avg_hospitality_experience;
          })
          .catch(function (e) {
            console.warn("[leadership-platform-setup] HE CALA load failed", e);
          });
      });
    }
  }

  function readRow(rowEl, section) {
    var out = {};
    section.fields.forEach(function (f) {
      var el = rowEl.querySelector('[data-lp-field="' + f.name + '"]');
      out[f.name] = el ? el.value : "";
    });
    if (section.key === "orgStructure" && out.tags) {
      out.tags = String(out.tags)
        .split(/[,;\n|]+/)
        .map(function (t) { return nz(t); })
        .filter(Boolean);
    }
    return out;
  }

  function isRowEmpty(row, section) {
    return section.fields.every(function (f) {
      return !nz(row[f.name]);
    });
  }

  function collectSection(sectionKey) {
    var section = SECTIONS.find(function (s) { return s.key === sectionKey; });
    if (!section) return [];
    var block = document.querySelector('[data-lp-section-block="' + sectionKey + '"]');
    if (!block) return [];
    var rows = [];
    block.querySelectorAll(".lp-row").forEach(function (rowEl) {
      var row = readRow(rowEl, section);
      if (!isRowEmpty(row, section)) rows.push(row);
    });
    return rows;
  }

  function collectLeadershipPlatformFromForm() {
    var out = {};
    SECTIONS.forEach(function (s) {
      out[s.key] = collectSection(s.key);
    });
    return out;
  }

  function applyPlatformData(platform) {
    if (!platform || typeof platform !== "object") return;
    SECTIONS.forEach(function (section) {
      var list = platform[section.key];
      if (!Array.isArray(list)) return;
      var block = document.querySelector('[data-lp-section-block="' + section.key + '"]');
      if (!block) return;
      var rowsEl = block.querySelector(".lp-rows");
      if (!rowsEl) return;
      rowsEl.innerHTML = "";
      var rows = list.length ? list : [section.emptyRow()];
      rows.forEach(function (row, idx) {
        var copy = Object.assign({}, row);
        if (section.key === "orgStructure" && Array.isArray(copy.tags)) {
          copy.tags = copy.tags.join(", ");
        }
        rowsEl.insertAdjacentHTML("beforeend", rowHtml(section, idx, copy));
      });
    });
  }

  function ensureLeadershipPlatformMounted() {
    var root = document.getElementById("leadership-platform-setup-root");
    if (!root) return false;
    if (!root.querySelector(".leadership-platform-setup")) {
      mount(root);
    }
    return true;
  }

  function applyLeadershipPlatformPrefill(platform) {
    if (!platform || typeof platform !== "object") return;
    if (!ensureLeadershipPlatformMounted()) {
      console.warn("[operator-setup] leadership-platform-setup-root not found — skipping platform prefill");
      return;
    }
    applyPlatformData(platform);
  }

  global.collectLeadershipPlatformFromForm = collectLeadershipPlatformFromForm;
  global.applyLeadershipPlatformPrefill = applyLeadershipPlatformPrefill;
  global.ensureLeadershipPlatformMounted = ensureLeadershipPlatformMounted;

  document.addEventListener("DOMContentLoaded", function () {
    var root = document.getElementById("leadership-platform-setup-root");
    if (root) mount(root);
  });
})(typeof window !== "undefined" ? window : global);
