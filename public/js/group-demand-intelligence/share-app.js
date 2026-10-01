(function () {
  "use strict";

  var root = document.getElementById("gdiShareRoot");
  var loading = document.getElementById("gdiShareLoading");
  var drawer = document.getElementById("gdiShareDrawer");
  var drawerTitle = document.getElementById("gdiShareDrawerTitle");
  var drawerBody = document.getElementById("gdiShareDrawerBody");
  var drawerClose = document.getElementById("gdiShareDrawerClose");
  var params = new URLSearchParams(window.location.search || "");
  var share = params.get("share") || "";

  var UI = window.DealalityGdiUi;
  if (!UI) throw new Error("DealalityGdiUi missing");
  var esc = UI.esc;
  var fmtVal = UI.fmtVal;
  var trunc = UI.trunc;

  var STORAGE_PREFIX = "gdi-share-";
  var state = {
    hotelId: null,
    resolve: null,
    opportunities: [],
    validationEnums: null,
    tab: "report",
    sortKey: "priority",
    sortDir: 1,
    viewMode: "tiles",
    filters: { priority: "", segment: "", booking: "", territory: "", weekly: "" },
    reportData: null,
    reportError: null,
  };

  function readPersistedBrowse() {
    try {
      var v = sessionStorage.getItem(STORAGE_PREFIX + "viewMode");
      if (v === "list" || v === "tiles") state.viewMode = v;
      var s = sessionStorage.getItem(STORAGE_PREFIX + "sortKey");
      if (s) state.sortKey = s;
    } catch (e) {
      /* ignore */
    }
  }

  function persistBrowse() {
    try {
      sessionStorage.setItem(STORAGE_PREFIX + "viewMode", state.viewMode);
      sessionStorage.setItem(STORAGE_PREFIX + "sortKey", state.sortKey);
    } catch (e) {
      /* ignore */
    }
  }

  function resetBrowseView() {
    state.filters = { priority: "", segment: "", booking: "", territory: "" };
    state.sortKey = "priority";
    state.sortDir = 1;
    state.viewMode = "tiles";
    try {
      sessionStorage.removeItem(STORAGE_PREFIX + "viewMode");
      sessionStorage.removeItem(STORAGE_PREFIX + "sortKey");
    } catch (e) {
      /* ignore */
    }
    render();
  }

  readPersistedBrowse();

  function setLoading(on, message) {
    UI.setLoadingToast(loading, on, message);
  }

  function showError(msg, code) {
    setLoading(false);
    var customerMsg =
      msg ||
      "This Dealality access link is no longer active. Please request an updated link from your Dealality contact.";
    if (UI.inactiveShareLinkHtml) {
      root.innerHTML = UI.inactiveShareLinkHtml(customerMsg);
      return;
    }
    root.innerHTML = '<div class="gdi-error" role="alert"><p>' + esc(customerMsg) + "</p></div>";
  }

  function formatDate(iso) {
    if (!iso) return "—";
    var d = new Date(iso);
    if (isNaN(d.getTime())) return String(iso);
    return d.toLocaleDateString(undefined, {
      year: "numeric",
      month: "short",
      day: "numeric",
    });
  }

  function api(path) {
    var sep = path.indexOf("?") >= 0 ? "&" : "?";
    return fetch(path + sep + "share=" + encodeURIComponent(share)).then(function (r) {
      return r.json().then(function (data) {
        if (!r.ok || data.ok === false) {
          var err = new Error(
            data.message ||
              "This Dealality access link is no longer active. Please request an updated link from your Dealality contact."
          );
          err.code = data.code || data.error || "INTERNAL_ERROR";
          throw err;
        }
        return data;
      });
    });
  }

  function apiPost(path, body) {
    var sep = path.indexOf("?") >= 0 ? "&" : "?";
    return fetch(path + sep + "share=" + encodeURIComponent(share), {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body || {}),
    }).then(function (r) {
      return r.json().then(function (data) {
        if (!r.ok || data.ok === false) {
          throw new Error(data.message || data.error || "Request failed");
        }
        return data;
      });
    });
  }

  function enumOptions(keys, labels, selected) {
    return (
      '<option value="">—</option>' +
      (keys || [])
        .map(function (k) {
          return (
            '<option value="' +
            esc(k) +
            '"' +
            (selected === k ? " selected" : "") +
            ">" +
            esc((labels && labels[k]) || k) +
            "</option>"
          );
        })
        .join("")
    );
  }

  function nextUnvalidatedId(currentId) {
    var rows = state.opportunities || [];
    var start = -1;
    for (var i = 0; i < rows.length; i++) {
      if (rows[i].id === currentId) {
        start = i;
        break;
      }
    }
    for (var j = start + 1; j < rows.length; j++) {
      if (!rows[j].shareValidation) return rows[j].id;
    }
    for (var k = 0; k < start; k++) {
      if (!rows[k].shareValidation) return rows[k].id;
    }
    return null;
  }

  function validationFormHtml(o) {
    var caps = (state.resolve && state.resolve.capabilities) || [];
    var canValidate =
      state.resolve &&
      (state.resolve.canValidate === true ||
        caps.indexOf("CAN_VALIDATE") >= 0 ||
        (!state.resolve.capabilities &&
          Array.isArray(state.resolve.surfaces) &&
          state.resolve.surfaces.indexOf("opportunity_detail") >= 0));
    var canRecordAction =
      state.resolve &&
      (state.resolve.canRecordAction === true ||
        caps.indexOf("CAN_RECORD_ACTION") >= 0);
    var canRecordOutcome =
      state.resolve &&
      (state.resolve.canRecordOutcome === true ||
        caps.indexOf("CAN_RECORD_OUTCOME") >= 0);
    if (UI.customerFeedbackLifecycleHtml) {
      return UI.customerFeedbackLifecycleHtml(
        o,
        state.validationEnums,
        {
          canValidate: !!canValidate,
          canRecordAction: !!canRecordAction,
          canRecordOutcome: !!canRecordOutcome,
        },
        o.commercialProgression || null
      );
    }
    if (UI.shareCustomerValidationFormHtml) {
      return UI.shareCustomerValidationFormHtml(o, state.validationEnums, {
        canValidate: canValidate,
      });
    }
    return "";
  }

  function wireLifecycleControls(opportunityId) {
    wireValidationForm(opportunityId);

    var outcomeEl = document.getElementById("gdiDoOutcome");
    var lossWrap = document.getElementById("gdiDoLossWrap");
    function syncLossVisibility() {
      if (!lossWrap) return;
      var show = outcomeEl && outcomeEl.value === "LOST";
      lossWrap.hidden = !show;
      if (!show) {
        var lossEl = document.getElementById("gdiDoLoss");
        if (lossEl) lossEl.value = "";
      }
    }
    if (outcomeEl) {
      outcomeEl.addEventListener("change", syncLossVisibility);
      syncLossVisibility();
    }

    function showStatus(id, text) {
      var el = document.getElementById(id);
      if (!el) return;
      el.hidden = false;
      el.textContent = text;
    }

    var actionBtn = document.getElementById("gdiDoActionSave");
    if (actionBtn) {
      actionBtn.addEventListener("click", function () {
        var actionType = (document.getElementById("gdiDoAction") || {}).value || "";
        if (!actionType) {
          showStatus("gdiDoActionSaveStatus", "Select action...");
          return;
        }
        showStatus("gdiDoActionSaveStatus", "Saving…");
        apiPost(
          "/api/group-demand-intelligence/share/hotels/" +
            encodeURIComponent(state.hotelId) +
            "/opportunities/" +
            encodeURIComponent(opportunityId) +
            "/actions",
          { actionType: actionType, validator: "SHARE_REVIEWER" }
        )
          .then(function (data) {
            if (data.commercialProgression) {
              for (var i = 0; i < state.opportunities.length; i++) {
                if (state.opportunities[i].id === opportunityId) {
                  state.opportunities[i].commercialProgression =
                    data.commercialProgression;
                  break;
                }
              }
            }
            showStatus("gdiDoActionSaveStatus", "Saved");
          })
          .catch(function (err) {
            showStatus("gdiDoActionSaveStatus", err.message || "Save failed");
          });
      });
    }

    var outcomeBtn = document.getElementById("gdiDoOutcomeSave");
    if (outcomeBtn) {
      outcomeBtn.addEventListener("click", function () {
        var outcomeType =
          (document.getElementById("gdiDoOutcome") || {}).value || "";
        if (!outcomeType) {
          showStatus("gdiDoOutcomeSaveStatus", "Select outcome...");
          return;
        }
        var lossReason =
          (document.getElementById("gdiDoLoss") || {}).value || null;
        showStatus("gdiDoOutcomeSaveStatus", "Saving…");
        apiPost(
          "/api/group-demand-intelligence/share/hotels/" +
            encodeURIComponent(state.hotelId) +
            "/opportunities/" +
            encodeURIComponent(opportunityId) +
            "/outcomes",
          {
            outcomeType: outcomeType,
            lossReason: lossReason,
            validator: "SHARE_REVIEWER",
          }
        )
          .then(function (data) {
            if (data.commercialProgression) {
              for (var j = 0; j < state.opportunities.length; j++) {
                if (state.opportunities[j].id === opportunityId) {
                  state.opportunities[j].commercialProgression =
                    data.commercialProgression;
                  break;
                }
              }
            }
            showStatus("gdiDoOutcomeSaveStatus", "Saved");
          })
          .catch(function (err) {
            showStatus("gdiDoOutcomeSaveStatus", err.message || "Save failed");
          });
      });
    }
  }

  function wireValidationForm(opportunityId) {
    function collect() {
      return {
        familiarityStatus: (document.getElementById("gdiSvFamiliarity") || {}).value || null,
        commercialValue: (document.getElementById("gdiSvCommercial") || {}).value || null,
        contactPersonAssessment: (document.getElementById("gdiSvPerson") || {}).value || null,
        emailAssessment: (document.getElementById("gdiSvEmail") || {}).value || null,
        phoneAssessment: (document.getElementById("gdiSvPhone") || {}).value || null,
        note: (document.getElementById("gdiSvNote") || {}).value || "",
        validator: "SHARE_REVIEWER",
      };
    }
    function save(thenNext) {
      var statusEl = document.getElementById("gdiSvStatus");
      if (statusEl) statusEl.textContent = "Saving…";
      var payload = collect();
      if (!payload.familiarityStatus && !payload.commercialValue) {
        if (statusEl) statusEl.textContent = "Pick familiarity or commercial value.";
        return;
      }
      apiPost(
        "/api/group-demand-intelligence/share/hotels/" +
          encodeURIComponent(state.hotelId) +
          "/opportunities/" +
          encodeURIComponent(opportunityId) +
          "/validation",
        payload
      )
        .then(function (data) {
          for (var i = 0; i < state.opportunities.length; i++) {
            if (state.opportunities[i].id === opportunityId) {
              state.opportunities[i].shareValidation = data.validation;
              break;
            }
          }
          if (state.resolve && data.summary) {
            state.resolve.summary = Object.assign({}, state.resolve.summary, {
              validated: data.summary.validated,
              pendingValidation: data.summary.pendingValidation,
              confirmedNew: data.summary.confirmedNew,
              alreadyKnown: data.summary.alreadyKnown,
              worthPursuingNow: data.summary.worthPursuingNow,
              notRelevant: data.summary.notRelevant,
            });
            state.resolve.validationSummary = data.summary;
          }
          if (statusEl) statusEl.textContent = "Saved";
          if (thenNext) {
            var nid = nextUnvalidatedId(opportunityId);
            if (nid) openDetail(nid);
            else if (statusEl) statusEl.textContent = "Saved — all reviewed";
          }
        })
        .catch(function (err) {
          if (statusEl) statusEl.textContent = err.message || "Save failed";
        });
    }
    var saveBtn = document.getElementById("gdiSvSave");
    var nextBtn = document.getElementById("gdiSvSaveNext");
    if (saveBtn) saveBtn.addEventListener("click", function () { save(false); });
    if (nextBtn) nextBtn.addEventListener("click", function () { save(true); });
  }

  function priorityPill(p) {
    return UI.priorityPill(p);
  }

  function contactLabel(contact) {
    if (!contact) return "—";
    if (contact.name && contact.name !== "UNKNOWN") {
      return contact.name + (contact.role ? " (" + contact.role + ")" : "");
    }
    if (contact.email) return contact.email;
    return "—";
  }

  function whoShouldSalesContactHtml(o) {
    if (UI && typeof UI.whoShouldSalesContactHtml === "function") {
      return UI.whoShouldSalesContactHtml(o);
    }
    var c = o.primaryContact;
    if (!c) {
      if (o.commercialContactPathLabel || o.commercialContactPath) {
        return (
          "<p><strong>Contact path:</strong> " +
          esc(o.commercialContactPathLabel || o.commercialContactPath) +
          "</p>"
        );
      }
      return "<p>No usable contact resolved yet.</p>";
    }
    var backups = (o.backupContacts || [])
      .map(function (b) {
        return (
          "<li><strong>" +
          esc(b.name || "—") +
          "</strong> · " +
          esc(b.role || "") +
          " · " +
          esc(b.email || "") +
          (b.phone ? " · " + esc(b.phone) : "") +
          (b.whyThisContact ? "<br/><em>" + esc(b.whyThisContact) + "</em>" : "") +
          "</li>"
        );
      })
      .join("");
    return (
      "<p><strong>" +
      esc(c.name || "—") +
      "</strong></p>" +
      "<p>Title: " +
      esc(c.title || c.role || "—") +
      "</p>" +
      "<p>Relationship to event: " +
      esc(c.relationshipToEvent || o.relationshipToEvent || "—") +
      "</p>" +
      "<p><strong>Why this contact:</strong> " +
      esc(c.whyThisContact || o.whyThisContact || "—") +
      "</p>" +
      "<p>Email: " +
      esc(c.email || "—") +
      " · " +
      esc(c.emailVerificationStatusLabel || c.emailVerificationStatus || "") +
      "</p>" +
      "<p>Phone: " +
      esc(c.phone || "—") +
      (c.phoneTypeLabel || c.phoneType
        ? " (" + esc(c.phoneTypeLabel || c.phoneType) + ")"
        : "") +
      "</p>" +
      "<p>Contact Quality: <strong>" +
      esc(o.contactGradeLabel || c.contactGradeLabel || o.contactQualityLabel || "—") +
      "</strong> · Contact Confidence: <strong>" +
      esc(o.contactConfidence ?? c.contactConfidence ?? "—") +
      "</strong></p>" +
      "<p>Last verified: " +
      esc(c.lastVerifiedAt || "—") +
      "</p>" +
      (backups ? "<h4>Backup contacts</h4><ul>" + backups + "</ul>" : "")
    );
  }

  function oppById(id) {
    for (var i = 0; i < state.opportunities.length; i++) {
      if (state.opportunities[i].id === id) return state.opportunities[i];
    }
    return null;
  }

  function entityName() {
    return (state.resolve && state.resolve.hotelName) || "Hotel";
  }

  function entityMeta() {
    if (!state.resolve) return "";
    var parts = [];
    if (state.resolve.city) parts.push(state.resolve.city);
    if (state.resolve.state) parts.push(state.resolve.state);
    return parts.join(", ");
  }

  function qualifiedCount() {
    var s = (state.resolve && state.resolve.summary) || {};
    if (s.qualifiedCount != null) return s.qualifiedCount;
    return state.opportunities.length;
  }

  function fitLabel(key) {
    var map = {
      physicalFit: "Physical Fit",
      geographyFit: "Demand Territory Fit",
      timing: "Timing / Winnability",
      commercialValue: "Commercial Potential",
      historicalFit: "Historical Hotel / Brand Fit",
      competitiveAccessibility: "Competitive Accessibility",
      contactability: "Contactability",
    };
    return map[key] || key;
  }

  function openDetail(id) {
    var listRow = (state.opportunities || []).find(function (o) {
      return o.id === id;
    });
    // Immediate visual acknowledgement (<100ms target)
    drawerTitle.textContent = (listRow && listRow.title) || "Opportunity";
    drawerBody.innerHTML = UI.detailLoadingHtml();
    if (typeof drawer.showModal === "function") drawer.showModal();
    else drawer.setAttribute("open", "open");

    api(
      "/api/group-demand-intelligence/share/hotels/" +
        encodeURIComponent(state.hotelId) +
        "/opportunities/" +
        encodeURIComponent(id)
    )
      .then(function (data) {
        var o = data.opportunity;
        var audit = data.scoreAudit || {};
        drawerTitle.textContent = o.title;
        drawerBody.innerHTML = UI.intelligenceDetailHtml(o, audit, {
          validationHtml: validationFormHtml(o),
          whoContactHtml: whoShouldSalesContactHtml,
        });
        wireLifecycleControls(o.id);
      })
      .catch(function (err) {
        drawerBody.innerHTML = UI.detailErrorHtml(
          err.message || "Unable to load opportunity detail.",
          { retryOpportunityId: id }
        );
      });
  }

  function filteredSorted() {
    return UI.sortOpportunities(
      UI.filterOpportunities(state.opportunities, state.filters),
      state.sortKey,
      state.sortDir
    );
  }

  function applyTilePairFilter(priority, booking, e) {
    if (e) {
      e.preventDefault();
      e.stopPropagation();
    }
    state.filters.priority = priority || "";
    state.filters.booking = booking || "";
    render();
  }

  function wireBrowseControls() {
    root.querySelectorAll("[data-gdi-priority]").forEach(function (btn) {
      btn.addEventListener("click", function () {
        state.filters.priority = btn.getAttribute("data-gdi-priority") || "";
        state.filters = UI.reconcileBrowseFilters(
          state.opportunities,
          state.filters,
          "priority"
        );
        render();
      });
    });
    root.querySelectorAll("[data-gdi-booking]").forEach(function (btn) {
      btn.addEventListener("click", function () {
        state.filters.booking = btn.getAttribute("data-gdi-booking") || "";
        state.filters = UI.reconcileBrowseFilters(
          state.opportunities,
          state.filters,
          "booking"
        );
        render();
      });
    });
    root.querySelectorAll("[data-gdi-tile-priority][data-gdi-tile-booking]").forEach(
      function (btn) {
        btn.addEventListener("click", function (e) {
          applyTilePairFilter(
            btn.getAttribute("data-gdi-tile-priority"),
            btn.getAttribute("data-gdi-tile-booking"),
            e
          );
        });
      }
    );
    var sortSel = document.getElementById("gdiSortSelect");
    if (sortSel) {
      sortSel.addEventListener("change", function () {
        state.sortKey = sortSel.value || "priority";
        state.sortDir = 1;
        persistBrowse();
        render();
      });
    }
    var sortDirBtn = document.getElementById("gdiSortDirBtn");
    if (sortDirBtn) {
      sortDirBtn.addEventListener("click", function () {
        state.sortDir = state.sortDir === 1 ? -1 : 1;
        render();
      });
    }
    root.querySelectorAll("[data-gdi-view]").forEach(function (btn) {
      btn.addEventListener("click", function () {
        state.viewMode = btn.getAttribute("data-gdi-view") === "tiles" ? "tiles" : "list";
        persistBrowse();
        render();
      });
    });
    var exportBtn = document.getElementById("gdiExportCsvBtn");
    if (exportBtn) {
      exportBtn.addEventListener("click", downloadCustomerCsv);
    }
  }

  function buildExportHref() {
    if (!state.hotelId || !share) return "";
    var rows = filteredSorted();
    var q = ["share=" + encodeURIComponent(share)];
    var ids = rows
      .map(function (o) {
        return o.id || o.opportunityId;
      })
      .filter(Boolean);
    if (ids.length && ids.length <= 200) {
      q.push("ids=" + encodeURIComponent(ids.join(",")));
    } else {
      if (state.filters.weekly) q.push("weekly=" + encodeURIComponent(state.filters.weekly));
      if (state.filters.priority) {
        q.push("priority=" + encodeURIComponent(state.filters.priority));
      }
      if (state.filters.booking) {
        q.push("booking=" + encodeURIComponent(state.filters.booking));
      }
      if (state.filters.segment) {
        q.push("segment=" + encodeURIComponent(state.filters.segment));
      }
      if (state.filters.territory) {
        q.push("territory=" + encodeURIComponent(state.filters.territory));
      }
    }
    return (
      "/api/group-demand-intelligence/share/hotels/" +
      encodeURIComponent(state.hotelId) +
      "/opportunities/export.csv?" +
      q.join("&")
    );
  }

  function downloadCustomerCsv(e) {
    if (e) e.preventDefault();
    var href = buildExportHref();
    if (!href) return;
    fetch(href)
      .then(function (r) {
        var ct = String(r.headers.get("content-type") || "").toLowerCase();
        if (!r.ok || ct.indexOf("text/csv") < 0) {
          return r.text().then(function (body) {
            throw new Error(
              "CSV export failed. Got " + (ct || r.status) + " " + String(body || "").slice(0, 80)
            );
          });
        }
        var disp = r.headers.get("content-disposition") || "";
        var match = /filename\*?=(?:UTF-8''|")?([^\";]+)/i.exec(disp);
        var filename = match
          ? decodeURIComponent(match[1].replace(/"/g, ""))
          : "GDI Opportunities.csv";
        return r.blob().then(function (blob) {
          return { blob: blob, filename: filename };
        });
      })
      .then(function (pack) {
        var url = URL.createObjectURL(pack.blob);
        var a = document.createElement("a");
        a.href = url;
        a.download = pack.filename;
        document.body.appendChild(a);
        a.click();
        a.remove();
        setTimeout(function () {
          URL.revokeObjectURL(url);
        }, 2000);
      })
      .catch(function (err) {
        console.error("gdi_share_csv_export_failed", err);
        alert("Could not download CSV. Please try again.");
      });
  }

  function money(n) {
    if (n == null || !isFinite(Number(n))) return "—";
    return "$" + Math.round(Number(n)).toLocaleString("en-US");
  }

  function whoLine(who) {
    if (!who) return "Conference / Meetings Team";
    if (who.kind === "NAMED" && who.name) {
      return who.name + (who.title ? ", " + who.title : "");
    }
    return who.title || "Conference / Meetings Team";
  }

  function renderReport() {
    if (state.reportError) {
      return (
        '<div class="gdi-empty">' +
        esc(state.reportError) +
        "</div>"
      );
    }
    if (!state.reportData) {
      return '<div class="gdi-empty">Loading demand report…</div>';
    }
    var d = state.reportData;
    var ex = d.executiveSummary || {};
    var rev = ex.revenueScenario;
    var html = "";
    html +=
      '<section class="gdi-report-client">' +
      '<h2 class="gdi-h2">Commercial Demand Snapshot</h2>' +
      '<div class="gdi-kpi-row">' +
      kpi("Customer-ready", ex.customerReadyCount) +
      kpi("Action set", ex.actionSetCount) +
      kpi("High", ex.highPriority) +
      kpi("Medium", ex.mediumPriority) +
      kpi("Future watch", ex.futureWatchCount) +
      kpi("Named contacts", (ex.namedContactCoveragePct != null ? ex.namedContactCoveragePct + "%" : "—")) +
      "</div>";
    if (rev) {
      html +=
        "<h3 class=\"gdi-h3\">Estimated room-revenue scenarios</h3>" +
        '<div class="gdi-kpi-row">' +
        kpi("Low", money(rev.low)) +
        kpi("Base", money(rev.base)) +
        kpi("High", money(rev.high)) +
        "</div>" +
        '<p class="gdi-disclaimer-inline">' +
        esc(rev.disclaimer || "") +
        "</p>" +
        (rev.scopeNote
          ? '<p class="gdi-muted">' + esc(rev.scopeNote) + "</p>"
          : "");
    }
    html +=
      '<p class="gdi-muted">Public contact path coverage: <strong>' +
      esc(ex.publicContactPathCoveragePct != null ? ex.publicContactPathCoveragePct + "%" : "—") +
      "</strong></p>";

    html += '<h2 class="gdi-h2">Top Immediate Pursuits</h2>';
    var imm = d.immediatePursuits || [];
    if (!imm.length) {
      html += '<div class="gdi-empty">No immediate pursuits in this snapshot.</div>';
    } else {
      imm.forEach(function (c, i) {
        html += pursuitCard(c, i + 1);
      });
    }

    html +=
      '<h2 class="gdi-h2">Top Opportunities</h2>' +
      '<p class="gdi-muted">Prioritized action set (' +
      (d.topOpportunities || []).length +
      ").</p>";
    (d.topOpportunities || []).forEach(function (c) {
      html += oppCard(c);
    });

    var plan = d.actionPlan || {};
    html += '<h2 class="gdi-h2">30-Day Action Plan</h2>';
    html += planTable("Act now", plan.actNow);
    html += planTable("Develop", plan.develop);
    html += '<h3 class="gdi-h3">Watch</h3>';
    var watch = d.futureWatch || [];
    if (!watch.length) {
      html += '<div class="gdi-empty">No future-watch items in this snapshot.</div>';
    } else {
      watch.forEach(function (c) {
        html +=
          '<article class="gdi-card gdi-card--watch">' +
          "<h4>" +
          esc(c.opportunity) +
          "</h4>" +
          "<p><strong>Why it matters:</strong> " +
          esc(c.whyNow || "—") +
          "</p>" +
          "<p><strong>Current status:</strong> " +
          esc(c.currentBlocker || "Monitoring") +
          "</p>" +
          "<p><strong>Trigger:</strong> " +
          esc(c.trigger || "Next public update") +
          "</p></article>";
      });
    }

    var hi = d.supportingIntelligence || {};
    html +=
      '<h2 class="gdi-h2">Supporting Hotel Intelligence</h2>' +
      '<p class="gdi-muted">Why this hotel can compete for these groups.</p><ul class="gdi-list">';
    if (hi.rooms != null) html += "<li><strong>Guestrooms:</strong> ~" + esc(hi.rooms) + "</li>";
    if (hi.meetingSqFt != null)
      html +=
        "<li><strong>Total meeting space:</strong> ~" +
        esc(Number(hi.meetingSqFt).toLocaleString("en-US")) +
        " sq ft</li>";
    if (hi.largestMeetingSqFt != null)
      html +=
        "<li><strong>Largest meeting room:</strong> ~" +
        esc(Number(hi.largestMeetingSqFt).toLocaleString("en-US")) +
        " sq ft</li>";
    if (hi.territoryLabel)
      html += "<li><strong>Demand territory:</strong> " + esc(hi.territoryLabel) + "</li>";
    html += "</ul>";
    if ((hi.demandAnchors || []).length) {
      html +=
        "<p><strong>Demand anchors:</strong> " +
        esc((hi.demandAnchors || []).join("; ")) +
        "</p>";
    }

    html +=
      '<h2 class="gdi-h2">Notes</h2><ul class="gdi-list">' +
      (d.methodology || [])
        .map(function (m) {
          return "<li>" + esc(m) + "</li>";
        })
        .join("") +
      "</ul></section>";
    return html;
  }

  function kpi(label, value) {
    return (
      '<div class="gdi-kpi"><div class="gdi-kpi__label">' +
      esc(label) +
      '</div><div class="gdi-kpi__value">' +
      esc(value != null ? value : "—") +
      "</div></div>"
    );
  }

  function pursuitCard(c, n) {
    return (
      '<article class="gdi-card">' +
      '<div class="gdi-card__head"><span class="gdi-pri">' +
      esc(c.priority || "") +
      '</span><span class="gdi-muted">#' +
      n +
      "</span></div>" +
      "<h3>" +
      esc(c.opportunity) +
      "</h3>" +
      '<p class="gdi-muted">' +
      esc(c.organization || "") +
      " · " +
      esc(c.segment || "") +
      " · " +
      esc(c.datesCycle || "Dates TBD") +
      "</p>" +
      "<p><strong>Why it matters:</strong> " +
      esc(c.whyNow || "—") +
      "</p>" +
      "<p><strong>Why this hotel:</strong> " +
      esc(c.whyHotel || "—") +
      "</p>" +
      "<p><strong>Who:</strong> " +
      esc(whoLine(c.who)) +
      (c.contactPath && c.contactPath.display
        ? " · " + esc(c.contactPath.display)
        : "") +
      "</p>" +
      "<p><strong>Next action:</strong> " +
      esc(c.recommendedAction || "—") +
      "</p></article>"
    );
  }

  function oppCard(c) {
    return (
      '<article class="gdi-card gdi-card--compact">' +
      '<div class="gdi-card__head"><span class="gdi-pri">' +
      esc(c.priority || "") +
      '</span><span class="gdi-muted">' +
      esc(c.segment || "") +
      "</span></div>" +
      "<h3>" +
      esc(c.opportunity) +
      "</h3>" +
      '<p class="gdi-muted">' +
      esc(c.organization || "") +
      " · " +
      esc(c.datesCycle || "Dates TBD") +
      "</p>" +
      "<p><strong>Why this hotel:</strong> " +
      esc(c.whyHotel || "—") +
      "</p>" +
      "<p><strong>Who:</strong> " +
      esc(whoLine(c.who)) +
      " · " +
      esc((c.contactPath && c.contactPath.display) || "Not yet supported") +
      "</p>" +
      "<p><strong>Action:</strong> " +
      esc(c.recommendedAction || "—") +
      "</p></article>"
    );
  }

  function planTable(title, rows) {
    rows = rows || [];
    if (!rows.length) return "";
    return (
      "<h3 class=\"gdi-h3\">" +
      esc(title) +
      "</h3>" +
      '<div class="gdi-table-wrap"><table class="gdi-table"><thead><tr>' +
      "<th>Opportunity</th><th>Owner</th><th>Action</th><th>Outcome</th><th>Timing</th>" +
      "</tr></thead><tbody>" +
      rows
        .map(function (r) {
          return (
            "<tr><td>" +
            esc(r.opportunity) +
            "</td><td>" +
            esc(r.ownerRole) +
            "</td><td>" +
            esc(r.action) +
            "</td><td>" +
            esc(r.desiredOutcome) +
            "</td><td>" +
            esc(r.timing) +
            "</td></tr>"
          );
        })
        .join("") +
      "</tbody></table></div>"
    );
  }

  function shareTabs() {
    return [
      UI.GDI_REPORT_TAB || { id: "report", label: "Demand\nReport" },
      UI.GDI_MAIN_TAB,
    ];
  }

  function renderTabBody() {
    if (state.tab === "report") return renderReport();
    return renderOpps();
  }

  function render() {
    setLoading(false);
    var s = (state.resolve && state.resolve.summary) || {};
    var hotel = {
      hotelName: entityName(),
      city: state.resolve && state.resolve.city,
      state: state.resolve && state.resolve.state,
    };
    root.className = "gdi-shell dashboard-container aiv-dashboard";
    root.innerHTML =
      UI.hotelShellHtml({
        mode: "share",
        badges: { readOnly: true },
      }) +
      UI.contentTabsHtml(state.tab, shareTabs()) +
      UI.propertyBarHtml({
        mode: "share",
        hotel: hotel,
        activeWeekly: state.filters.weekly,
        weeklyCounts: UI.countByWeeklyDelta(state.opportunities),
        lastResearch: formatDate(s.lastResearchAt),
        runStatus: s.runStatus || "—",
        actionHtml:
          (state.resolve && state.resolve.pdfAvailable
            ? '<button type="button" class="btn-clear" id="gdiShareViewPdf">View PDF</button>' +
              '<button type="button" class="btn-clear" id="gdiShareDownloadPdf">Download PDF</button>'
            : "") +
          '<button type="button" class="btn-clear" id="gdiResetViewBtn">Reset View</button>',
      }) +
      '<div id="gdiTabBody">' +
      renderTabBody() +
      "</div>" +
      '<div class="gdi-disclaimer">Client view for review only. Findings are research-assisted and should be validated by the hotel sales team before outreach.</div>';

    wireBrowseControls();

    root.querySelectorAll("[data-tab]").forEach(function (btn) {
      btn.addEventListener("click", function () {
        var next = btn.getAttribute("data-tab");
        if (!next || next === state.tab) return;
        state.tab = next;
        render();
      });
    });

    function openSharePdf(download) {
      if (!state.hotelId || !share) return;
      var url =
        "/api/group-demand-intelligence/share/hotels/" +
        encodeURIComponent(state.hotelId) +
        "/report-pdf?share=" +
        encodeURIComponent(share) +
        (download ? "&download=1" : "");
      window.open(url, "_blank");
    }
    var viewPdf = document.getElementById("gdiShareViewPdf");
    if (viewPdf) viewPdf.addEventListener("click", function () { openSharePdf(false); });
    var dlPdf = document.getElementById("gdiShareDownloadPdf");
    if (dlPdf) dlPdf.addEventListener("click", function () { openSharePdf(true); });

    var weeklySelect = document.getElementById("gdiWeeklyFilter");
    if (weeklySelect) {
      weeklySelect.addEventListener("change", function () {
        state.filters.weekly = weeklySelect.value || "";
        state.filters = UI.reconcileBrowseFilters(
          state.opportunities,
          state.filters,
          "weekly"
        );
        render();
      });
    }

    var resetBtn = document.getElementById("gdiResetViewBtn");
    if (resetBtn) resetBtn.addEventListener("click", resetBrowseView);

    root.querySelectorAll("[data-open]").forEach(function (el) {
      el.addEventListener("click", function (e) {
        var innermost = e.target.closest("[data-open]");
        if (innermost !== el) return;
        openDetail(el.getAttribute("data-open"));
      });
      if (el.tagName === "ARTICLE") {
        el.addEventListener("keydown", function (e) {
          if (e.key === "Enter" || e.key === " ") {
            e.preventDefault();
            openDetail(el.getAttribute("data-open"));
          }
        });
      }
    });
  }

  function loadReportData(hotelId) {
    return api(
      "/api/group-demand-intelligence/share/hotels/" +
        encodeURIComponent(hotelId) +
        "/pdf-report"
    )
      .then(function (json) {
        if (!json || !json.ok || !json.available) {
          state.reportData = null;
          state.reportError =
            (json && json.message) ||
            "Group & Demand Intelligence is not currently available for this hotel.";
          return;
        }
        state.reportData = json.data;
        state.reportError = null;
      })
      .catch(function (err) {
        state.reportData = null;
        state.reportError =
          (err && err.message) ||
          "Group & Demand Intelligence is not currently available for this hotel.";
      });
  }

  function renderOpps() {
    if (!state.opportunities.length) {
      return '<div class="gdi-empty">No opportunities.</div>';
    }
    var rows = filteredSorted();
    var chrome = UI.opportunityBrowseChromeHtml({
      activePriority: state.filters.priority,
      priorityCounts: UI.facetCountByPriority(state.opportunities, state.filters),
      activeBooking: state.filters.booking,
      actionCounts: UI.facetCountByActionStatus(state.opportunities, state.filters),
      weeklyHeaderCounts: UI.weeklyDeltaHeaderCounts(state.opportunities),
      shown: rows.length,
      total: state.opportunities.length,
      sort: state.sortKey,
      viewMode: state.viewMode,
      noun: "Opportunities",
      exportHref: buildExportHref(),
    });
    if (!rows.length) {
      return (
        chrome +
        '<div class="gdi-empty">No opportunities match the current filters.</div>'
      );
    }
    return chrome + UI.opportunityCardsGridHtml(rows, state.viewMode, state.filters);
  }

  if (!share) {
    showError("A valid share link is required.");
    return;
  }

  function peekShareHotelId(token) {
    try {
      var raw = String(token || "");
      var marker = "gdishare.v1.";
      var body = raw.indexOf(marker) === 0 ? raw.slice(marker.length) : raw;
      var dot = body.lastIndexOf(".");
      if (dot <= 0) return null;
      var b64 = body.slice(0, dot).replace(/-/g, "+").replace(/_/g, "/");
      while (b64.length % 4) b64 += "=";
      var json = JSON.parse(atob(b64));
      var hid = json && (json.hotelId || json.hid);
      return hid ? String(hid) : null;
    } catch (e) {
      return null;
    }
  }

  if (drawerClose) {
    drawerClose.addEventListener("click", function () {
      if (typeof drawer.close === "function") drawer.close();
      else drawer.removeAttribute("open");
    });
  }

  if (drawerBody) {
    drawerBody.addEventListener("click", function (e) {
      var t = e.target;
      var retryBtn =
        t && t.closest ? t.closest("[data-gdi-detail-retry]") : null;
      if (retryBtn) {
        var retryId = retryBtn.getAttribute("data-gdi-detail-retry");
        if (retryId) openDetail(retryId);
      }
    });
  }

  // Parallel resolve + opportunities when hotelId is present in signed payload
  // (auth still enforced server-side on both calls).
  var peekedHotelId = peekShareHotelId(share);
  var resolvePromise = api("/api/group-demand-intelligence/share/resolve");
  var oppsPromise = peekedHotelId
    ? api(
        "/api/group-demand-intelligence/share/hotels/" +
          encodeURIComponent(peekedHotelId) +
          "/opportunities"
      ).catch(function () {
        return null;
      })
    : Promise.resolve(null);

  Promise.all([resolvePromise, oppsPromise])
    .then(function (pair) {
      var resolve = pair[0];
      var data = pair[1];
      state.resolve = resolve;
      state.hotelId = resolve.hotelId;
      if (data && data.opportunities) {
        state.opportunities = data.opportunities || [];
        state.validationEnums = data.validationEnums || null;
      }
      return loadReportData(resolve.hotelId).then(function () {
        if (data && data.opportunities) {
          render();
          return null;
        }
        return api(
          "/api/group-demand-intelligence/share/hotels/" +
            encodeURIComponent(resolve.hotelId) +
            "/opportunities"
        );
      });
    })
    .then(function (data) {
      if (!data) return;
      state.opportunities = data.opportunities || [];
      state.validationEnums = data.validationEnums || null;
      render();
    })
    .catch(function (err) {
      showError(err.message || "Unable to open share link.");
    });
})();
