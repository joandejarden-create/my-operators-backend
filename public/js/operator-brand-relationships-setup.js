/**
 * Operator Setup — Brand & Relationships structured repeaters (Explorer DNA).
 * Syncs to hidden JSON fields for save; optional footprint-derived portfolio mix.
 */
(function (global) {
  "use strict";

  var MAX_TABLE_ROWS = 12;
  var MAX_TILE_ROWS = 6;

  var JSON_KEYS = {
    portfolioMix: "brand_portfolio_mix_json",
    relationshipDepth: "brand_relationship_depth_json",
    executionCapabilities: "brand_execution_capabilities_json",
    governanceCompliance: "brand_governance_compliance_json",
  };

  var PARENT_TO_FLAG = {
    marriott: "Marriott Family",
    "marriott international": "Marriott Family",
    hilton: "Hilton Family",
    "hilton worldwide": "Hilton Family",
    hyatt: "Hyatt Family",
    "hyatt hotels": "Hyatt Family",
    ihg: "IHG Family",
    "intercontinental hotels group": "IHG Family",
    choice: "Wyndham / Choice / Other",
    wyndham: "Wyndham / Choice / Other",
    accor: "Wyndham / Choice / Other",
    sonesta: "Wyndham / Choice / Other",
    radisson: "Wyndham / Choice / Other",
  };

  var TABLE_SECTIONS = [
    {
      key: "portfolioMix",
      title: "Portfolio mix by brand / flag type",
      hint: "Share of portfolio by brand family. Mix % can be suggested from Company Profile brand units (rooms).",
      jsonKey: JSON_KEYS.portfolioMix,
      addLabel: "+ Add mix row",
      rowClass: "br-row--table",
      fields: [
        { name: "brandFlagType", label: "Brand / flag type", type: "text", span: 1 },
        { name: "portfolioMix", label: "Portfolio mix %", type: "text", span: 1 },
        { name: "assetContext", label: "Asset context", type: "text", span: 1 },
        { name: "relationshipStatus", label: "Relationship status", type: "text", span: 1 },
      ],
      emptyRow: function () {
        return {
          brandFlagType: "",
          portfolioMix: "",
          assetContext: "",
          relationshipStatus: "",
        };
      },
      maxRows: MAX_TABLE_ROWS,
    },
    {
      key: "relationshipDepth",
      title: "Brands & relationship depth",
      hint: "Segment-level relationship depth—qualitative; not auto-filled from footprint.",
      jsonKey: JSON_KEYS.relationshipDepth,
      addLabel: "+ Add depth row",
      fields: [
        { name: "brandSegment", label: "Brand segment", type: "text", span: 1 },
        { name: "relationshipType", label: "Relationship type", type: "text", span: 1 },
        { name: "depth", label: "Depth", type: "text", span: 1 },
        { name: "ownerContext", label: "Owner context", type: "textarea", rows: 2, span: 2 },
      ],
      emptyRow: function () {
        return {
          brandSegment: "",
          relationshipType: "",
          depth: "",
          ownerContext: "",
        };
      },
      maxRows: MAX_TABLE_ROWS,
    },
  ];

  var TILE_SECTIONS = [
    {
      key: "executionCapabilities",
      title: "Brand execution capabilities",
      hint: "Proof points for onboarding, conversion, and brand-owner coordination.",
      jsonKey: JSON_KEYS.executionCapabilities,
      addLabel: "+ Add capability",
      fields: [
        { name: "title", label: "Title", type: "text", span: 2 },
        { name: "description", label: "Description", type: "textarea", rows: 2, span: 2 },
      ],
      emptyRow: function () {
        return { title: "", description: "" };
      },
      maxRows: MAX_TILE_ROWS,
    },
    {
      key: "governanceCompliance",
      title: "Brand governance & compliance support",
      hint: "QA, technical services, reporting, and owner decision support.",
      jsonKey: JSON_KEYS.governanceCompliance,
      addLabel: "+ Add governance row",
      fields: [
        { name: "title", label: "Topic", type: "text", span: 2 },
        { name: "description", label: "Description", type: "textarea", rows: 2, span: 2 },
      ],
      emptyRow: function () {
        return { title: "", description: "" };
      },
      maxRows: MAX_TILE_ROWS,
    },
  ];

  var ALL_SECTIONS = TABLE_SECTIONS.concat(TILE_SECTIONS);

  function nz(v) {
    return v != null && String(v).trim() !== "" ? String(v).trim() : "";
  }

  function esc(s) {
    return String(s == null ? "" : s)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/"/g, "&quot;");
  }

  function parseJsonArray(raw) {
    if (raw == null || raw === "") return [];
    if (Array.isArray(raw)) return raw;
    try {
      var parsed = JSON.parse(String(raw));
      return Array.isArray(parsed) ? parsed : [];
    } catch (e) {
      return [];
    }
  }

  function fieldHtml(sectionKey, rowIndex, field, value) {
    var spanClass = field.span === 2 ? " br-field--span2" : "";
    var val = esc(value == null ? "" : value);
    if (field.type === "textarea") {
      return (
        '<div class="br-field' +
        spanClass +
        '"><label>' +
        esc(field.label) +
        '</label><textarea data-br-field="' +
        esc(field.name) +
        '" rows="' +
        (field.rows || 2) +
        '">' +
        val +
        "</textarea></div>"
      );
    }
    return (
      '<div class="br-field' +
      spanClass +
      '"><label>' +
      esc(field.label) +
      '</label><input type="text" data-br-field="' +
      esc(field.name) +
      '" value="' +
      val +
      '" /></div>'
    );
  }

  function rowHtml(section, rowIndex, data) {
    data = data || section.emptyRow();
    var rowClass = section.rowClass ? " " + section.rowClass : "";
    var fieldsHtml = section.fields
      .map(function (f) {
        return fieldHtml(section.key, rowIndex, f, data[f.name]);
      })
      .join("");
    return (
      '<div class="br-row' +
      rowClass +
      '" data-br-section="' +
      esc(section.key) +
      '">' +
      fieldsHtml +
      '<div class="br-row-actions"><button type="button" class="btn btn-secondary btn-sm br-remove-row">Remove</button></div></div>'
    );
  }

  function sectionShell(section) {
    return (
      '<div class="br-section-block" data-br-section-block="' +
      esc(section.key) +
      '">' +
      "<h4>" +
      esc(section.title) +
      "</h4>" +
      '<p class="br-section-hint">' +
      esc(section.hint) +
      "</p>" +
      '<div class="br-rows"></div>' +
      '<button type="button" class="btn btn-secondary btn-sm br-add-row">' +
      esc(section.addLabel) +
      "</button></div>"
    );
  }

  function collectBrandsPortfolioDetail(formEl) {
    var byBrand = {};
    if (!formEl) return [];
    formEl.querySelectorAll("input[data-brand][data-kind]").forEach(function (input) {
      var brand = input.getAttribute("data-brand");
      var kind = input.getAttribute("data-kind");
      if (!brand || !kind) return;
      if (!byBrand[brand]) byBrand[brand] = { brand_key: brand };
      var brandName = nz(input.getAttribute("data-brand-name"));
      if (brandName && !byBrand[brand].brand_name) byBrand[brand].brand_name = brandName;
      var raw = nz(input.value);
      if (raw === "") return;
      if (kind === "avg_staff") {
        var nf = parseFloat(String(raw).replace(/[^\d.]/g, ""));
        if (!Number.isNaN(nf)) byBrand[brand][kind] = nf;
      } else {
        var ni = parseInt(String(raw).replace(/\D/g, ""), 10);
        if (!Number.isNaN(ni)) byBrand[brand][kind] = ni;
      }
    });
    return Object.keys(byBrand)
      .map(function (k) {
        return byBrand[k];
      })
      .filter(function (row) {
        return Object.keys(row).length > 1;
      });
  }

  function parentForBrandName(brandName) {
    var osm = global.OperatorSetupBrandsManaged;
    if (osm && typeof osm.getParentCompanyForBrand === "function") {
      return nz(osm.getParentCompanyForBrand(brandName));
    }
    return "";
  }

  function flagTypeFromParent(parent, brandName) {
    var bn = nz(brandName).toLowerCase();
    if (bn === "independent" || bn.indexOf("independent") >= 0) {
      return "Independent / Soft Brand";
    }
    var p = nz(parent).toLowerCase();
    if (!p) return nz(brandName) || "Other";
    if (PARENT_TO_FLAG[p]) return PARENT_TO_FLAG[p];
    var first = p.split(/[;,/]/)[0].trim();
    if (PARENT_TO_FLAG[first]) return PARENT_TO_FLAG[first];
    if (p.indexOf("marriott") >= 0) return "Marriott Family";
    if (p.indexOf("hilton") >= 0) return "Hilton Family";
    if (p.indexOf("hyatt") >= 0) return "Hyatt Family";
    if (p.indexOf("ihg") >= 0 || p.indexOf("intercontinental") >= 0) return "IHG Family";
    if (p.indexOf("choice") >= 0 || p.indexOf("wyndham") >= 0 || p.indexOf("accor") >= 0) {
      return "Wyndham / Choice / Other";
    }
    return titleCaseWords(parent) + " Family";
  }

  function titleCaseWords(s) {
    return nz(s)
      .split(/\s+/)
      .map(function (w) {
        return w.charAt(0).toUpperCase() + w.slice(1).toLowerCase();
      })
      .join(" ");
  }

  function formatPercent(n) {
    if (n == null || isNaN(n)) return "";
    return String(Math.round(n)) + "%";
  }

  function derivePortfolioMixFromFootprint(formEl) {
    var rows = collectBrandsPortfolioDetail(formEl);
    var groups = {};
    var grandRooms = 0;

    rows.forEach(function (r) {
      var name = nz(r.brand_name) || nz(r.brand_key);
      var ep = Number(r.existing_properties) || 0;
      var er = Number(r.existing_rooms) || 0;
      var pp = Number(r.pipeline_properties) || 0;
      var pr = Number(r.pipeline_rooms) || 0;
      var rooms = er + pr;
      var hotels = ep + pp;
      var weight = rooms > 0 ? rooms : hotels;
      if (weight <= 0) return;
      var parent = parentForBrandName(name);
      var flag = flagTypeFromParent(parent, name);
      if (!groups[flag]) groups[flag] = { weight: 0, brands: [] };
      groups[flag].weight += weight;
      if (name && groups[flag].brands.indexOf(name) === -1) groups[flag].brands.push(name);
      grandRooms += weight;
    });

    if (!grandRooms) return { rows: [], summary: null };

    var list = Object.keys(groups)
      .map(function (flag) {
        return {
          brandFlagType: flag,
          portfolioMix: formatPercent((groups[flag].weight / grandRooms) * 100),
          assetContext: groups[flag].brands.slice(0, 4).join(", "),
          relationshipStatus: "Active / Prior",
          _weight: groups[flag].weight,
        };
      })
      .sort(function (a, b) {
        return b._weight - a._weight;
      });

    var brandedRooms = 0;
    var indepRooms = 0;
    list.forEach(function (row) {
      var pct = parseFloat(String(row.portfolioMix).replace("%", ""), 10);
      if (isNaN(pct)) return;
      var w = (pct / 100) * grandRooms;
      if (row.brandFlagType.indexOf("Independent") >= 0) indepRooms += w;
      else brandedRooms += w;
    });

    return {
      rows: list.map(function (r) {
        return {
          brandFlagType: r.brandFlagType,
          portfolioMix: r.portfolioMix,
          assetContext: r.assetContext,
          relationshipStatus: r.relationshipStatus,
        };
      }),
      summary: {
        totalBrands: rows.length,
        grandRooms: grandRooms,
        brandedPct: formatPercent((brandedRooms / grandRooms) * 100),
        independentPct: formatPercent((indepRooms / grandRooms) * 100),
      },
    };
  }

  function updateDerivedHintPanel(formEl, summary) {
    var panel = document.getElementById("brandFootprintDerivedHint");
    if (!panel) return;
    if (!summary || !summary.grandRooms) {
      panel.classList.add("is-hidden");
      panel.innerHTML = "";
      return;
    }
    panel.classList.remove("is-hidden");
    panel.innerHTML =
      "<strong>Suggested from Company Profile brand units</strong>" +
      "<ul>" +
      "<li>" +
      summary.totalBrands +
      " brand row(s) with room or hotel counts</li>" +
      "<li>Approx. branded " +
      esc(summary.brandedPct) +
      " · independent / soft " +
      esc(summary.independentPct) +
      " (by rooms)</li>" +
      "<li>Decision signal KPIs (audit pass rate, reflag lead time, etc.) are <em>not</em> auto-calculated—enter those manually.</li>" +
      "</ul>";
  }

  function readRow(rowEl, section) {
    var out = {};
    section.fields.forEach(function (f) {
      var el = rowEl.querySelector('[data-br-field="' + f.name + '"]');
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
    var section = ALL_SECTIONS.find(function (s) {
      return s.key === sectionKey;
    });
    if (!section) return [];
    var block = document.querySelector('[data-br-section-block="' + sectionKey + '"]');
    if (!block) return [];
    var rows = [];
    block.querySelectorAll(".br-row").forEach(function (rowEl) {
      var row = readRow(rowEl, section);
      if (!isRowEmpty(row, section)) rows.push(row);
    });
    return rows;
  }

  function syncHiddenJsonFields(form) {
    if (!form) form = document.getElementById("operatorIntakeForm");
    ALL_SECTIONS.forEach(function (section) {
      var ta = form && form.querySelector('[name="' + section.jsonKey + '"]');
      if (!ta) return;
      var rows = collectRepeaterSection(section.key);
      ta.value = rows.length ? JSON.stringify(rows) : "";
    });
  }

  function collectBrandRelationshipsPlatformFromForm(form) {
    syncHiddenJsonFields(form);
    var softTa =
      form && form.querySelector('[name="brand_soft_independent_narrative"]');
    return {
      portfolioMix: collectRepeaterSection("portfolioMix"),
      relationshipDepth: collectRepeaterSection("relationshipDepth"),
      executionCapabilities: collectRepeaterSection("executionCapabilities"),
      governanceCompliance: collectRepeaterSection("governanceCompliance"),
      softIndependentNarrative: softTa ? nz(softTa.value) : "",
      snapshotMetrics: [],
      narratives: {},
      brandSignals: [],
    };
  }

  function collectBrandRelationshipsFromForm(form) {
    syncHiddenJsonFields(form);
    var out = {};
    ALL_SECTIONS.forEach(function (section) {
      var rows = collectRepeaterSection(section.key);
      out[section.jsonKey] = rows.length ? JSON.stringify(rows) : "";
    });
    return out;
  }

  function applySectionRows(section, list) {
    var block = document.querySelector('[data-br-section-block="' + section.key + '"]');
    if (!block) return;
    var rowsEl = block.querySelector(".br-rows");
    if (!rowsEl) return;
    rowsEl.innerHTML = "";
    var rows = Array.isArray(list) && list.length ? list : [section.emptyRow()];
    rows.slice(0, section.maxRows).forEach(function (row, idx) {
      rowsEl.insertAdjacentHTML("beforeend", rowHtml(section, idx, row));
    });
  }

  function applyBrandRelationshipsPlatformPrefill(platform) {
    if (!platform || typeof platform !== "object") return;
    applySectionRows(TABLE_SECTIONS[0], platform.portfolioMix);
    applySectionRows(TABLE_SECTIONS[1], platform.relationshipDepth);
    applySectionRows(TILE_SECTIONS[0], platform.executionCapabilities);
    applySectionRows(TILE_SECTIONS[1], platform.governanceCompliance);
    var form = document.getElementById("operatorIntakeForm");
    var softTa =
      form && form.querySelector('[name="brand_soft_independent_narrative"]');
    if (softTa && nz(platform.softIndependentNarrative)) {
      softTa.value = platform.softIndependentNarrative;
    }
    syncHiddenJsonFields(form);
  }

  function applyBrandRelationshipsPrefill(payload) {
    if (!payload || typeof payload !== "object") return;
    if (payload.portfolioMix || payload.relationshipDepth) {
      applyBrandRelationshipsPlatformPrefill(payload);
      return;
    }
    ALL_SECTIONS.forEach(function (section) {
      var raw = payload[section.jsonKey];
      if (raw == null) return;
      applySectionRows(section, parseJsonArray(raw));
    });
  }

  function applyFromHiddenTextareas(form) {
    if (!form) form = document.getElementById("operatorIntakeForm");
    if (!form) return;
    var payload = {};
    ALL_SECTIONS.forEach(function (section) {
      var ta = form.querySelector('[name="' + section.jsonKey + '"]');
      if (ta && nz(ta.value)) payload[section.jsonKey] = ta.value;
    });
    applyBrandRelationshipsPrefill(payload);
  }

  function suggestPortfolioMixFromFootprint(formEl, replaceExisting) {
    if (!formEl) formEl = document.getElementById("operatorIntakeForm");
    var derived = derivePortfolioMixFromFootprint(formEl);
    updateDerivedHintPanel(formEl, derived.summary);
    if (!derived.rows.length) {
      if (typeof console !== "undefined" && console.warn) {
        console.warn("[brand-relationships-setup] No brand unit counts to derive mix from.");
      }
      return false;
    }
    var section = TABLE_SECTIONS[0];
    var block = document.querySelector('[data-br-section-block="' + section.key + '"]');
    if (!block) return false;
    var hasData = collectRepeaterSection(section.key).length > 0;
    if (hasData && !replaceExisting) {
      var ok = global.confirm(
        "Replace existing portfolio mix rows with values calculated from Company Profile brand units?"
      );
      if (!ok) return false;
    }
    applySectionRows(section, derived.rows);
    syncHiddenJsonFields(formEl);
    return true;
  }

  function mount(root) {
    if (!root) return;
    root.innerHTML =
      '<div class="brand-relationships-setup">' +
      TABLE_SECTIONS.map(sectionShell).join("") +
      TILE_SECTIONS.map(sectionShell).join("") +
      '<p class="subsection-hint br-actions">' +
      '<button type="button" class="btn btn-secondary btn-sm" id="brSuggestMixBtn">Suggest portfolio mix from footprint</button> ' +
      '<button type="button" class="btn btn-secondary btn-sm" id="brLoadHeCalaBtn">Load HE CALA draft rows</button>' +
      "</p></div>";

    ALL_SECTIONS.forEach(function (section) {
      var block = root.querySelector('[data-br-section-block="' + section.key + '"]');
      if (!block) return;
      var rowsEl = block.querySelector(".br-rows");
      var addBtn = block.querySelector(".br-add-row");

      function addRow(data) {
        var count = rowsEl.querySelectorAll(".br-row").length;
        if (count >= section.maxRows) return;
        rowsEl.insertAdjacentHTML("beforeend", rowHtml(section, count, data || section.emptyRow()));
      }

      if (addBtn) addBtn.addEventListener("click", function () { addRow(); });
      rowsEl.addEventListener("click", function (e) {
        var btn = e.target && e.target.closest && e.target.closest(".br-remove-row");
        if (!btn) return;
        var row = btn.closest(".br-row");
        if (row && rowsEl.querySelectorAll(".br-row").length > 1) row.remove();
      });
      addRow();
    });

    var mixBtn = root.querySelector("#brSuggestMixBtn");
    if (mixBtn) {
      mixBtn.addEventListener("click", function () {
        suggestPortfolioMixFromFootprint(document.getElementById("operatorIntakeForm"), false);
      });
    }

    var heBtn = root.querySelector("#brLoadHeCalaBtn");
    if (heBtn) {
      heBtn.addEventListener("click", function () {
        fetch("/fixtures/operator-brand-explorer-he-cala.json")
          .then(function (r) {
            return r.json();
          })
          .then(function (d) {
            if (d.brandRelationships) applyBrandRelationshipsPlatformPrefill(d.brandRelationships);
            if (d.profileFields) {
              Object.keys(d.profileFields).forEach(function (key) {
                var el = document.querySelector('[name="' + key + '"]');
                if (el && d.profileFields[key] != null) el.value = String(d.profileFields[key]);
              });
            }
          })
          .catch(function (e) {
            console.warn("[brand-relationships-setup] HE CALA load failed", e);
          });
      });
    }

    var form = document.getElementById("operatorIntakeForm");
    if (form) {
      applyFromHiddenTextareas(form);
      bindFootprintHintRefresh(form);
      var derived = derivePortfolioMixFromFootprint(form);
      updateDerivedHintPanel(form, derived.summary);
    }
  }

  function bindFootprintHintRefresh(form) {
    if (!form || form.dataset.brFootprintHintBound === "1") return;
    form.dataset.brFootprintHintBound = "1";
    form.addEventListener(
      "input",
      function (e) {
        var t = e.target;
        if (!t || !t.matches || !t.matches("input[data-brand][data-kind]")) return;
        var derived = derivePortfolioMixFromFootprint(form);
        updateDerivedHintPanel(form, derived.summary);
      },
      true
    );
  }

  global.collectBrandRelationshipsFromForm = collectBrandRelationshipsFromForm;
  global.collectBrandRelationshipsPlatformFromForm = collectBrandRelationshipsPlatformFromForm;
  global.syncBrandRelationshipsToHiddenFields = syncHiddenJsonFields;
  global.applyBrandRelationshipsPrefill = applyBrandRelationshipsPrefill;
  global.applyBrandRelationshipsPlatformPrefill = applyBrandRelationshipsPlatformPrefill;
  global.applyBrandRelationshipsFromHiddenFields = applyFromHiddenTextareas;
  global.deriveBrandPortfolioMixFromFootprint = derivePortfolioMixFromFootprint;
  global.suggestBrandPortfolioMixFromFootprint = suggestPortfolioMixFromFootprint;

  document.addEventListener("DOMContentLoaded", function () {
    var root = document.getElementById("brand-relationships-setup-root");
    if (root) mount(root);
  });
})(typeof window !== "undefined" ? window : global);
