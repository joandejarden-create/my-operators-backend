/**
 * Operator Setup (New / New Two) — SIGNAL+STORY UX: conditionals, char counters,
 * Explorer JSON payload, Key Leadership sync from executive repeater.
 * Loaded after the intake form on consolidated setup pages.
 */
(function () {
  function isExplorerFieldName(k) {
    if (!k || typeof k !== "string") return false;
    if (k === "marketDepthOptIn" || k === "displayLeadershipOnExplorer") return true;
    if (/^exec_\d+_/.test(k)) return true;
    if (k === "ownerEngagementNarrative") return true;
    const prefixes = [
      "overview_",
      "cap_",
      "brand_",
      "mkt_",
      "ov_",
      "infra_",
      "risk_",
      "lead_",
      "bf_",
      "tr_",
      "systems_",
    ];
    return prefixes.some(function (p) {
      return k.indexOf(p) === 0;
    });
  }

  function escRe(s) {
    return s.replace(/[-/\\^$*+?.()|[\]{}]/g, "\\$&");
  }

  function reindexExecutiveRows(rowsEl) {
    var prefix = "exec_";
    var rows = rowsEl.querySelectorAll(".repeater-row");
    rows.forEach(function (row, idx) {
      var n = idx + 1;
      row.querySelectorAll("input, textarea, select").forEach(function (el) {
        if (!el.name) return;
        el.name = el.name.replace(new RegExp(escRe(prefix) + "\\d+", "g"), prefix + n);
        if (el.id) el.id = el.name;
      });
      var h = row.querySelector("h4");
      if (h) h.textContent = "Executive " + n;
    });
  }

  function bindRemoveExecutiveRow(row, rowsEl) {
    var btn = row.querySelector(".btn-remove-row");
    if (!btn) return;
    btn.addEventListener("click", function () {
      if (rowsEl.querySelectorAll(".repeater-row").length <= 1) return;
      row.remove();
      reindexExecutiveRows(rowsEl);
    });
  }

  function bindExecutiveRepeater(form) {
    if (form.dataset.executiveRepeaterBound === "1") return;
    form.dataset.executiveRepeaterBound = "1";
    var rowsEl = document.getElementById("repeater-executives");
    if (!rowsEl) return;
    var rid = "executives";
    rowsEl.querySelectorAll(".repeater-row").forEach(function (row) {
      bindRemoveExecutiveRow(row, rowsEl);
    });
    form.querySelectorAll('[data-repeater-add="executives"]').forEach(function (btn) {
      btn.addEventListener("click", function () {
        var last = rowsEl.querySelector(".repeater-row:last-of-type");
        if (!last) return;
        var clone = last.cloneNode(true);
        clone.querySelectorAll("input, textarea, select").forEach(function (el) {
          el.value = "";
        });
        rowsEl.appendChild(clone);
        reindexExecutiveRows(rowsEl);
        bindRemoveExecutiveRow(clone, rowsEl);
      });
    });
  }

  function maxExecIndexFromPayload(obj) {
    var max = 0;
    Object.keys(obj || {}).forEach(function (k) {
      var m = /^exec_(\d+)_/.exec(k);
      if (m) max = Math.max(max, parseInt(m[1], 10));
    });
    return max;
  }

  function ensureExecutiveRowCount(form, needed) {
    var rowsEl = document.getElementById("repeater-executives");
    var addBtn = form.querySelector('[data-repeater-add="executives"]');
    if (!rowsEl || !addBtn) return;
    var rep = rowsEl.closest && rowsEl.closest("[data-repeater]");
    var maxAllowed = parseInt((rep && rep.getAttribute("data-repeater-max")) || "24", 10) || 24;
    var target = Math.min(Math.max(needed, 1), maxAllowed);
    while (rowsEl.querySelectorAll(".repeater-row").length < target) {
      addBtn.click();
    }
  }

  /**
   * Map detail API `leadershipTeam` onto executive repeater fields (exec_*).
   * Called after Explorer JSON + prefill loop so Airtable-linked rows win over static demo HTML.
   */
  function applyLeadershipTeamPrefill(form, team) {
    if (!form || !Array.isArray(team) || team.length === 0) return;
    var rowsEl = document.getElementById("repeater-executives");
    var addBtn = form.querySelector('[data-repeater-add="executives"]');
    if (!rowsEl || !addBtn) return;
    var rep = rowsEl.closest && rowsEl.closest("[data-repeater]");
    var maxAllowed = parseInt((rep && rep.getAttribute("data-repeater-max")) || "24", 10) || 24;
    var minRows = parseInt((rep && rep.getAttribute("data-repeater-min")) || "1", 10) || 1;
    var needed = Math.min(Math.max(team.length, minRows), maxAllowed);
    while (rowsEl.querySelectorAll(".repeater-row").length > needed) {
      if (rowsEl.querySelectorAll(".repeater-row").length <= minRows) break;
      var list = rowsEl.querySelectorAll(".repeater-row");
      list[list.length - 1].remove();
      reindexExecutiveRows(rowsEl);
    }
    while (rowsEl.querySelectorAll(".repeater-row").length < needed) {
      addBtn.click();
    }
    function nz(v) {
      return v != null && String(v).trim() !== "" ? String(v).trim() : "";
    }
    team.slice(0, needed).forEach(function (row, idx) {
      var n = idx + 1;
      function set(suffix, val) {
        var el = form.querySelector('[name="exec_' + n + "_" + suffix + '"]');
        if (el) el.value = val == null ? "" : String(val);
      }
      set("name", row.name);
      set("title", row.title);
      var roleLine = nz(row.function) || nz(row.role);
      set("role", roleLine);
      var sum = nz(row.summary) || nz(row.experienceSummary) || nz(row.shortBio);
      var bioText = nz(row.bio) || nz(row.shortBio) || nz(row.experienceSummary);
      set("summary", sum);
      set("bio", bioText);
      set("headshot", row.headshotUrl || "");
      ["summary", "bio"].forEach(function (suf) {
        var el = form.querySelector('[name="exec_' + n + "_" + suf + '"]');
        if (el) el.dispatchEvent(new Event("input", { bubbles: true }));
      });
    });
    syncKeyLeadershipFromExecutives(form);
  }

  function syncKeyLeadershipFromExecutives(form) {
    var hid = form.querySelector("#keyLeadershipHidden");
    if (!hid) return;
    var rows = form.querySelectorAll('.repeater[data-repeater="executives"] .repeater-row');
    var lines = [];
    rows.forEach(function (row) {
      var nameInp = row.querySelector('input[name$="_name"]');
      var titleInp = row.querySelector('input[name$="_title"]');
      var n = nameInp && nameInp.value ? String(nameInp.value).trim() : "";
      var t = titleInp && titleInp.value ? String(titleInp.value).trim() : "";
      if (n) lines.push(t ? n + " — " + t : n);
    });
    hid.value = lines.join("\n");
  }

  function attachStoryCharCounters(root) {
    root.querySelectorAll("textarea.explorer-story-field, textarea[maxlength]").forEach(function (ta) {
      if (ta.dataset.explorerCounterBound === "1") return;
      var wrap = ta.closest(".field-wrap");
      if (!wrap) return;
      var counter = wrap.querySelector(".explorer-char-count-num");
      if (!counter) return;
      ta.dataset.explorerCounterBound = "1";
      function sync() {
        counter.textContent = String(ta.value || "").length;
      }
      ta.addEventListener("input", sync);
      sync();
    });
  }

  function updateSoftBrandVisibility(form) {
    var wrap = form.querySelector("#softBrandSignalsWrap");
    if (!wrap) return;
    var brandsSel = form.querySelector("#brandsManagedSelect");
    var addl = form.querySelector("#additionalBrands");
    var text = "";
    if (addl && addl.value) text += addl.value;
    if (brandsSel) {
      Array.from(brandsSel.selectedOptions || []).forEach(function (o) {
        text += " " + (o.text || o.value);
      });
    }
    var on = /soft|collection|independent flag|curio|tapestry|autograph/i.test(text);
    wrap.classList.toggle("hidden", !on);
    wrap.setAttribute("aria-hidden", on ? "false" : "true");
    if (typeof window.updateOperatorSetupRequiredCounts === "function") window.updateOperatorSetupRequiredCounts();
  }

  function syncMarketDepthFromCheckbox(form) {
    var cb = form.querySelector("#marketDepthOptIn");
    var box = form.querySelector("#marketDepthFields");
    if (!cb || !box) return;
    var on = cb.checked;
    box.hidden = !on;
    box.classList.toggle("hidden", !on);
    var ta = box.querySelector("textarea");
    if (ta) ta.required = !!on;
    if (typeof window.updateOperatorSetupRequiredCounts === "function") window.updateOperatorSetupRequiredCounts();
  }

  function bindMarketDepthVisibility(form) {
    if (form.dataset.marketDepthVisibilityBound === "1") return;
    form.dataset.marketDepthVisibilityBound = "1";
    var cb = form.querySelector("#marketDepthOptIn");
    if (!cb) return;
    cb.addEventListener("change", function () {
      syncMarketDepthFromCheckbox(form);
    });
    syncMarketDepthFromCheckbox(form);
  }

  function syncLeadershipDetailFromCheckbox(form) {
    var cb = form.querySelector("#displayLeadershipOnExplorer");
    var box = form.querySelector("#leadershipExplorerDetail");
    if (!cb || !box) return;
    var on = cb.checked;
    box.classList.toggle("hidden", !on);
    box.setAttribute("aria-hidden", on ? "false" : "true");
  }

  function bindLeadershipDetailVisibility(form) {
    if (form.dataset.leadershipDetailVisibilityBound === "1") return;
    form.dataset.leadershipDetailVisibilityBound = "1";
    var cb = form.querySelector("#displayLeadershipOnExplorer");
    if (!cb) return;
    cb.addEventListener("change", function () {
      syncLeadershipDetailFromCheckbox(form);
    });
    syncLeadershipDetailFromCheckbox(form);
  }

  function coerceBool(v) {
    if (v === true || v === "true" || v === "yes" || v === "on") return true;
    if (v === false || v === "false" || v === "no" || v === "") return false;
    if (v === 1 || v === "1") return true;
    if (v === 0 || v === "0") return false;
    return null;
  }

  function setExplorerControl(form, name, rawVal) {
    var escName = name.replace(/\\/g, "\\\\").replace(/"/g, '\\"');
    var els = form.querySelectorAll('[name="' + escName + '"]');
    if (!els.length) return;
    var el = els[0];
    var tag = el.tagName;
    if (tag === "INPUT" && el.type === "checkbox") {
      var co = coerceBool(rawVal);
      el.checked = co !== null ? co : !!rawVal;
      return;
    }
    if (tag === "SELECT" && el.multiple && Array.isArray(rawVal)) {
      var vals = rawVal.map(String);
      Array.from(el.options).forEach(function (opt) {
        opt.selected = vals.indexOf(String(opt.value).trim()) !== -1;
      });
      return;
    }
    /** Single-select: do not clear on null/empty — Explorer JSON often includes `cap_*: ""` placeholders; column prefill runs after and would otherwise be wiped. */
    if (tag === "SELECT" && !el.multiple && (rawVal == null || rawVal === "")) {
      return;
    }
    if (rawVal == null || typeof rawVal === "undefined") {
      el.value = "";
      return;
    }
    if (tag === "SELECT" && !el.multiple && typeof rawVal === "object" && !Array.isArray(rawVal) && typeof rawVal.name === "string") {
      el.value = rawVal.name.trim();
      return;
    }
    el.value = typeof rawVal === "object" && !Array.isArray(rawVal) ? JSON.stringify(rawVal) : String(rawVal);
  }

  var BF_QUANT_TRIPLE = [
    { sel: "bf_selected_asset_types", hid: "bf_q_assets", qKey: "bf_q_assets", ideal: "idealBuildingTypes" },
    { sel: "bf_selected_situation_types", hid: "bf_q_situations", qKey: "bf_q_situations", ideal: "projectStage" },
    { sel: "bf_selected_deal_structures", hid: "bf_q_deals", qKey: "bf_q_deals", ideal: "idealAgreementTypes" },
  ];

  /** Leadership: multi-select → hidden `lead_kpi_functions` count (same sync pattern as Best Fit quant) */
  var LEAD_FUNCTIONS_QUANT_PAIR = { sel: "lead_functions_selected", hid: "lead_kpi_functions", qKey: "lead_kpi_functions" };

  function allQuantMultiselectPairs() {
    return BF_QUANT_TRIPLE.concat([LEAD_FUNCTIONS_QUANT_PAIR]);
  }

  function countBfMultiselectSelected(sel) {
    if (!sel) return 0;
    return Array.from(sel.selectedOptions || []).filter(function (o) {
      return o.value;
    }).length;
  }

  function refreshBfQuantCountsFromMultiselects(form) {
    allQuantMultiselectPairs().forEach(function (t) {
      var s = form.querySelector('[name="' + t.sel + '"]');
      var hid = form.querySelector("#" + t.hid);
      var ro = form.querySelector("#" + t.hid + "_readout");
      if (!s || !hid) return;
      var n = countBfMultiselectSelected(s);
      hid.value = String(n);
      if (ro) ro.textContent = hid.value;
    });
  }

  function reconcileBfQuantAfterPrefill(form, explorerObj) {
    allQuantMultiselectPairs().forEach(function (t) {
      var s = form.querySelector('[name="' + t.sel + '"]');
      var hid = form.querySelector("#" + t.hid);
      var ro = form.querySelector("#" + t.hid + "_readout");
      if (!s || !hid) return;
      var n = countBfMultiselectSelected(s);
      if (n > 0) {
        hid.value = String(n);
      } else if (explorerObj && explorerObj[t.qKey] != null && String(explorerObj[t.qKey]).trim() !== "") {
        hid.value = String(explorerObj[t.qKey]);
      } else {
        hid.value = "0";
      }
      if (ro) ro.textContent = hid.value;
    });
  }

  function bindBestFitQuantMultiselects(form) {
    if (form.dataset.bfQuantBound === "1") return;
    form.dataset.bfQuantBound = "1";
    allQuantMultiselectPairs().forEach(function (t) {
      var s = form.querySelector('[name="' + t.sel + '"]');
      if (!s) return;
      s.addEventListener("change", function () {
        refreshBfQuantCountsFromMultiselects(form);
      });
    });
  }

  /**
   * Parse saved Explorer Profile JSON and populate Explorer/Gold fields (edit load).
   * @param {HTMLFormElement} form
   * @param {string} jsonStr
   */
  function applyExplorerProfileJsonPrefill(form, jsonStr) {
    if (!form || !jsonStr || typeof jsonStr !== "string") return;
    var obj;
    try {
      obj = JSON.parse(jsonStr);
    } catch (e) {
      console.warn("applyExplorerProfileJsonPrefill: invalid JSON", e);
      return;
    }
    if (!obj || typeof obj !== "object") return;

    var hasExplorerKeys = Object.keys(obj).some(function (k) {
      return isExplorerFieldName(k);
    });
    if (!hasExplorerKeys) return;

    form.dataset.explorerProfilePrefillApplied = "1";

    var neededExec = maxExecIndexFromPayload(obj);
    if (neededExec > 0) ensureExecutiveRowCount(form, neededExec);

    Object.keys(obj).forEach(function (k) {
      if (!isExplorerFieldName(k)) return;
      setExplorerControl(form, k, obj[k]);
    });

    syncMarketDepthFromCheckbox(form);
    syncLeadershipDetailFromCheckbox(form);
    syncDiligenceQaOptionalVisibility(form);
    updateSoftBrandVisibility(form);
    syncKeyLeadershipFromExecutives(form);

    reconcileBfQuantAfterPrefill(form, obj);

    attachStoryCharCounters(form);
    form.querySelectorAll("textarea.explorer-story-field, textarea[maxlength]").forEach(function (ta) {
      ta.dispatchEvent(new Event("input", { bubbles: true }));
    });
    form.querySelectorAll("[name^='overview_'], [name^='bf_'], [name^='ov_'], [name^='mkt_'], [name^='brand_']").forEach(function (el) {
      if (el.dispatchEvent) el.dispatchEvent(new Event("change", { bubbles: true }));
    });
  }

  window.applyExplorerProfileJsonPrefill = applyExplorerProfileJsonPrefill;

  function prefillBestFitCountsFromIdeal(form) {
    var ran = false;
    function run() {
      if (ran) return;
      if (form.dataset.explorerProfilePrefillApplied === "1") return;
      var hasBf = BF_QUANT_TRIPLE.some(function (t) {
        var s = form.querySelector('[name="' + t.sel + '"]');
        return s && countBfMultiselectSelected(s) > 0;
      });
      if (hasBf) {
        ran = true;
        return;
      }
      function copyIdealToBf(idealName, bfSelName) {
        var src = form.querySelector('[name="' + idealName + '"]');
        var dst = form.querySelector('[name="' + bfSelName + '"]');
        if (!src || !dst || !src.multiple) return;
        var vals = Array.from(src.selectedOptions || [])
          .map(function (o) {
            return o.value;
          })
          .filter(Boolean);
        Array.from(dst.options).forEach(function (opt) {
          opt.selected = vals.indexOf(opt.value) !== -1;
        });
      }
      BF_QUANT_TRIPLE.forEach(function (t) {
        copyIdealToBf(t.ideal, t.sel);
      });
      refreshBfQuantCountsFromMultiselects(form);
      ran = true;
    }
    form.addEventListener(
      "focusin",
      function (e) {
        if (e.target && e.target.closest && e.target.closest('[data-explorer-tab="Best Fit"]')) run();
      },
      true
    );
  }

  window.enrichOperatorSetupSubmitData = function (data, form) {
    if (!form || !data) return;
    refreshBfQuantCountsFromMultiselects(form);
    syncKeyLeadershipFromExecutives(form);
    if (data.keyLeadership == null || String(data.keyLeadership).trim() === "") {
      var hid = form.querySelector("#keyLeadershipHidden");
      if (hid) data.keyLeadership = hid.value || "";
    }
    var explorer = {};
    Object.keys(data).forEach(function (k) {
      if (!isExplorerFieldName(k)) return;
      explorer[k] = data[k];
      delete data[k];
    });
    data.explorerProfileJson = JSON.stringify(explorer);
    // Intake + Operator Setup — New Base writer read top-level form keys (e.g. exec_* for Leadership Team
    // Members, overview_/cap_/… for one-to-one tables). Keep the same fields on the body alongside
    // explorerProfileJson so Save Section / Full Submit both persist structured + child rows.
    Object.keys(explorer).forEach(function (k) {
      data[k] = explorer[k];
    });
  };

  function init(form) {
    if (!form || form.id !== "operatorIntakeForm") return;
    bindExecutiveRepeater(form);
    attachStoryCharCounters(form);
    updateSoftBrandVisibility(form);
    bindMarketDepthVisibility(form);
    bindLeadershipDetailVisibility(form);
    bindBestFitQuantMultiselects(form);
    refreshBfQuantCountsFromMultiselects(form);
    prefillBestFitCountsFromIdeal(form);
    var brandsSel = form.querySelector("#brandsManagedSelect");
    var addl = form.querySelector("#additionalBrands");
    if (brandsSel)
      brandsSel.addEventListener("change", function () {
        updateSoftBrandVisibility(form);
      });
    if (addl)
      addl.addEventListener("input", function () {
        updateSoftBrandVisibility(form);
      });
  }

  window.applyLeadershipTeamPrefill = applyLeadershipTeamPrefill;

  document.addEventListener("DOMContentLoaded", function () {
    var form = document.getElementById("operatorIntakeForm");
    init(form);
  });
})();
