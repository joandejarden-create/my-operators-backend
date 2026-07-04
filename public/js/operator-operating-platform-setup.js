/**
 * Operator Setup — Operating Platform pillar repeaters (child table).
 * KPI / signal selects stay on the main form; repeaters collect capability tiles per pillar.
 */
(function (global) {
  "use strict";

  var MAX_ITEMS = 6;

  var PILLAR_SECTIONS = [
    {
      key: "commercialEngine",
      title: "Commercial Engine",
      hint: "Up to six capability tiles. Intro can mirror the Commercial Engine bullet field above.",
    },
    {
      key: "ownerReporting",
      title: "Owner Reporting & Communication",
      hint: "Reporting cadence, MBRs, dashboards, capex visibility—owner-facing operating rhythm.",
    },
    {
      key: "preOpeningTransition",
      title: "Pre-Opening & Transition Support",
      hint: "Conversions, openings, systems cutover, ramp-up—one tile per proof point.",
    },
    {
      key: "conversionRepositioning",
      title: "Conversion & Repositioning",
      hint: "Turnarounds, PIP execution, revenue systems during change.",
    },
    {
      key: "fbLifestyleResort",
      title: "F&B, Lifestyle & Resort",
      hint: "Resort programming, F&B, spa, pool—leisure-oriented differentiation.",
    },
    {
      key: "operationalExecutionLabor",
      title: "Operational Execution & Labor",
      hint: "SOPs, labor productivity, QA, guest experience—optional Explorer subsection.",
    },
    {
      key: "technologyEnabledOperations",
      title: "Technology-Enabled Operations",
      hint: "Regional pods, playbooks, escalation—people and process, not software lists.",
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

  function itemRowHtml(sectionKey, rowIndex, data) {
    data = data || { title: "", description: "" };
    return (
      '<div class="op-row" data-op-section="' +
      esc(sectionKey) +
      '">' +
      '<div class="op-field op-field--span2"><label>Capability title</label>' +
      '<input type="text" data-op-field="title" value="' +
      esc(data.title) +
      '" /></div>' +
      '<div class="op-field op-field--span2"><label>Description</label>' +
      '<textarea data-op-field="description" rows="2">' +
      esc(data.description) +
      "</textarea></div>" +
      '<div class="op-row-actions"><button type="button" class="btn btn-secondary btn-sm op-remove-row">Remove</button></div></div>'
    );
  }

  function pillarBlockHtml(section) {
    return (
      '<div class="op-section-block" data-op-section-block="' +
      esc(section.key) +
      '">' +
      "<h4>" +
      esc(section.title) +
      "</h4>" +
      '<p class="op-section-hint">' +
      esc(section.hint) +
      "</p>" +
      '<div class="op-field op-field--span2"><label>Pillar intro (optional)</label>' +
      '<textarea class="op-pillar-intro" rows="3" placeholder="Short paragraph shown above capability tiles in Explorer."></textarea></div>' +
      '<div class="op-items"></div>' +
      '<button type="button" class="btn btn-secondary btn-sm op-add-item">+ Add capability tile</button></div>'
    );
  }

  function mount(root) {
    if (!root) return;
    root.innerHTML =
      '<div class="operating-platform-setup">' +
      PILLAR_SECTIONS.map(pillarBlockHtml).join("") +
      '<p class="subsection-hint op-actions">' +
      '<button type="button" class="btn btn-secondary btn-sm" id="opLoadHeCalaBtn">Load HE CALA draft rows</button>' +
      "</p></div>";

    PILLAR_SECTIONS.forEach(function (section) {
      var block = root.querySelector('[data-op-section-block="' + section.key + '"]');
      if (!block) return;
      var itemsEl = block.querySelector(".op-items");
      var addBtn = block.querySelector(".op-add-item");

      function addItem(data) {
        var count = itemsEl.querySelectorAll(".op-row").length;
        if (count >= MAX_ITEMS) return;
        itemsEl.insertAdjacentHTML("beforeend", itemRowHtml(section.key, count, data));
      }

      if (addBtn) {
        addBtn.addEventListener("click", function () {
          addItem();
        });
      }
      itemsEl.addEventListener("click", function (e) {
        var btn = e.target && e.target.closest && e.target.closest(".op-remove-row");
        if (!btn) return;
        var row = btn.closest(".op-row");
        if (row) row.remove();
      });
      addItem();
    });

    var heBtn = root.querySelector("#opLoadHeCalaBtn");
    if (heBtn) {
      heBtn.addEventListener("click", function () {
        fetch("/fixtures/operator-operating-explorer-he-cala.json")
          .then(function (r) {
            return r.json();
          })
          .then(function (d) {
            if (d.operatingPlatform) applyOperatingPlatformPrefill(d.operatingPlatform);
            if (d.platformFields) applyPlatformScalarFields(d.platformFields);
          })
          .catch(function (e) {
            console.warn("[operating-platform-setup] HE CALA load failed", e);
          });
      });
    }
  }

  function applyPlatformScalarFields(scalars) {
    if (!scalars || typeof scalars !== "object") return;
    Object.keys(scalars).forEach(function (key) {
      var el = document.querySelector('[name="' + key + '"]');
      if (el && scalars[key] != null) el.value = String(scalars[key]);
    });
  }

  function readPillarBlock(sectionKey) {
    var block = document.querySelector('[data-op-section-block="' + sectionKey + '"]');
    if (!block) return { title: "", description: "", items: [] };
    var introEl = block.querySelector(".op-pillar-intro");
    var items = [];
    block.querySelectorAll(".op-row").forEach(function (rowEl) {
      var titleEl = rowEl.querySelector('[data-op-field="title"]');
      var descEl = rowEl.querySelector('[data-op-field="description"]');
      var title = titleEl ? titleEl.value : "";
      var description = descEl ? descEl.value : "";
      if (!nz(title) && !nz(description)) return;
      items.push({ title: nz(title), description: nz(description) });
    });
    return {
      title: "",
      description: introEl ? nz(introEl.value) : "",
      items: items.slice(0, MAX_ITEMS),
    };
  }

  function collectOperatingPlatformFromForm(form) {
    var pillars = {};
    PILLAR_SECTIONS.forEach(function (section) {
      pillars[section.key] = readPillarBlock(section.key);
    });
    return { pillars: pillars };
  }

  function applyOperatingPlatformPrefill(platform) {
    if (!platform || typeof platform !== "object") return;
    var pillars = platform.pillars || {};
    PILLAR_SECTIONS.forEach(function (section) {
      var pillar = pillars[section.key];
      if (!pillar) return;
      var block = document.querySelector('[data-op-section-block="' + section.key + '"]');
      if (!block) return;
      var introEl = block.querySelector(".op-pillar-intro");
      if (introEl) introEl.value = nz(pillar.description);
      var itemsEl = block.querySelector(".op-items");
      if (!itemsEl) return;
      itemsEl.innerHTML = "";
      var list = Array.isArray(pillar.items) && pillar.items.length ? pillar.items : [{ title: "", description: "" }];
      list.slice(0, MAX_ITEMS).forEach(function (item, idx) {
        itemsEl.insertAdjacentHTML(
          "beforeend",
          itemRowHtml(section.key, idx, {
            title: item.title || "",
            description: item.description || "",
          })
        );
      });
    });
  }

  global.collectOperatingPlatformFromForm = collectOperatingPlatformFromForm;
  global.applyOperatingPlatformPrefill = applyOperatingPlatformPrefill;

  document.addEventListener("DOMContentLoaded", function () {
    var root = document.getElementById("operating-platform-setup-root");
    if (root) mount(root);
  });
})(typeof window !== "undefined" ? window : global);
