/**
 * Packet 2.6C — Deep Research Center drawer (Market Alerts / radar-filter-drawer family).
 * Packet 2.6C-R2 UX: Dealality confirmation modal + shared bdd-toast (no browser confirm/alert).
 */
(function () {
  "use strict";

  var state = {
    open: false,
    hotelId: null,
    hotelName: null,
    payload: null,
    view: "center", // center | detail
    detailTemplateId: null,
    loading: false,
    error: null,
    pollTimer: null,
    confirmOpen: false,
    confirmTemplateId: null,
    confirmSubmitting: false,
    confirmReturnFocusEl: null,
    knownActiveIds: {},
    toastedStartIds: {},
    toastedTerminalIds: {},
    seedLifecycleOnNextPayload: false,
  };

  var toastHideTimer = null;

  function isShareMode() {
    try {
      if (window.__HOTEL_EXPLORER_SHARE_MODE === true) return true;
      var q = new URLSearchParams(window.location.search || "");
      return q.get("share") === "1" || q.get("share") === "true";
    } catch (err) {
      return false;
    }
  }

  function esc(s) {
    return String(s == null ? "" : s)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;");
  }

  function ensureDom() {
    if (document.getElementById("hiResearchOverlay")) return;
    var overlay = document.createElement("div");
    overlay.id = "hiResearchOverlay";
    overlay.className = "hi-research-overlay";
    overlay.setAttribute("aria-hidden", "true");
    var drawer = document.createElement("aside");
    drawer.id = "hiResearchDrawer";
    drawer.className = "hi-research-drawer";
    drawer.setAttribute("role", "dialog");
    drawer.setAttribute("aria-modal", "true");
    drawer.setAttribute("aria-labelledby", "hiResearchTitle");
    drawer.setAttribute("aria-hidden", "true");
    drawer.innerHTML =
      '<div class="hi-research-drawer__header">' +
      '<div class="hi-research-drawer__header-text">' +
      '<p class="hi-research-drawer__kicker" id="hiResearchKicker">Deep Research</p>' +
      '<h2 class="hi-research-drawer__title" id="hiResearchTitle">Research Center</h2>' +
      '<p class="hi-research-drawer__hotel" id="hiResearchHotel"></p>' +
      '<p class="hi-research-drawer__status" id="hiResearchStatus"></p>' +
      "</div>" +
      '<button type="button" class="hi-research-drawer__close" id="hiResearchClose" aria-label="Close Research Center">&times;</button>' +
      "</div>" +
      '<div class="hi-research-drawer__body" id="hiResearchBody"></div>';

    var confirm = document.createElement("div");
    confirm.id = "hiResearchConfirm";
    confirm.className = "hi-rc-confirm";
    confirm.setAttribute("hidden", "");
    confirm.setAttribute("aria-hidden", "true");
    confirm.innerHTML =
      '<div class="hi-rc-confirm__backdrop" data-rc-confirm-cancel tabindex="-1"></div>' +
      '<div class="hi-rc-confirm__panel" role="dialog" aria-modal="true" aria-labelledby="hiRcConfirmTitle" aria-describedby="hiRcConfirmDesc">' +
      '<button type="button" class="hi-rc-confirm__close" data-rc-confirm-cancel aria-label="Close">&times;</button>' +
      '<div class="hi-rc-confirm__body" id="hiRcConfirmBody"></div>' +
      '<div class="hi-rc-confirm__actions" id="hiRcConfirmActions"></div>' +
      "</div>";

    document.body.appendChild(overlay);
    document.body.appendChild(drawer);
    document.body.appendChild(confirm);

    overlay.addEventListener("click", function () {
      if (state.confirmOpen) return;
      close();
    });
    document.getElementById("hiResearchClose").addEventListener("click", function () {
      if (state.confirmOpen) {
        closeConfirmModal();
        return;
      }
      close();
    });
    document.addEventListener("keydown", onGlobalKeydown);
    document.getElementById("hiResearchBody").addEventListener("click", onBodyClick);
    confirm.addEventListener("click", onConfirmClick);
  }

  function onGlobalKeydown(e) {
    if (e.key === "Escape") {
      if (state.confirmOpen) {
        e.preventDefault();
        closeConfirmModal();
        return;
      }
      if (state.open) close();
      return;
    }
    if (state.confirmOpen && e.key === "Tab") {
      trapConfirmFocus(e);
    }
  }

  function trapConfirmFocus(e) {
    var panel = document.querySelector("#hiResearchConfirm .hi-rc-confirm__panel");
    if (!panel) return;
    var focusable = panel.querySelectorAll(
      'button:not([disabled]), [href], input:not([disabled]), select:not([disabled]), textarea:not([disabled]), [tabindex]:not([tabindex="-1"])'
    );
    if (!focusable.length) return;
    var first = focusable[0];
    var last = focusable[focusable.length - 1];
    if (e.shiftKey && document.activeElement === first) {
      e.preventDefault();
      last.focus();
    } else if (!e.shiftKey && document.activeElement === last) {
      e.preventDefault();
      first.focus();
    }
  }

  /**
   * Shared Dealality toast pattern (.bdd-toast from deal-workspace-shell.css).
   * Injects host if missing so Hotel Explorer golden demo can use the same system.
   */
  function showDealalityToast(opts) {
    var title = (opts && opts.title) || "";
    var body = (opts && opts.body) || "";
    var kind = (opts && opts.kind) || "info"; // info | success | error
    var el = document.getElementById("hiResearchBddToast");
    if (!el) {
      el = document.createElement("div");
      el.id = "hiResearchBddToast";
      el.className = "bdd-toast";
      el.setAttribute("role", "status");
      el.setAttribute("aria-live", "polite");
      el.innerHTML =
        '<div class="toast-wave-container" aria-hidden="true">' +
        '<div class="wave-container"><div class="wave wave-1"></div><div class="wave wave-2"></div><div class="wave wave-3"></div></div>' +
        "</div>" +
        '<div class="toast-copy"><div class="toast-title"></div><div class="toast-message"></div></div>';
      document.body.appendChild(el);
    }
    el.classList.remove("toast-success", "toast-error", "show");
    if (kind === "success") el.classList.add("toast-success");
    if (kind === "error") el.classList.add("toast-error");
    var titleEl = el.querySelector(".toast-title");
    var msgEl = el.querySelector(".toast-message");
    if (titleEl) titleEl.textContent = title;
    if (msgEl) msgEl.textContent = body;
    el.style.display = "flex";
    // Force reflow for enter transition (same pattern as My Deals / BDD toast).
    void el.offsetWidth;
    el.classList.add("show");
    if (toastHideTimer) clearTimeout(toastHideTimer);
    toastHideTimer = setTimeout(function () {
      el.classList.remove("show");
      setTimeout(function () {
        el.style.display = "none";
      }, 300);
    }, 4200);
  }

  function open(opts) {
    ensureDom();
    state.hotelId = opts && opts.hotelId;
    state.hotelName = (opts && opts.hotelName) || "";
    state.view = "center";
    state.detailTemplateId = null;
    state.error = null;
    state.open = true;
    state.seedLifecycleOnNextPayload = true;
    closeConfirmModal({ silent: true });
    var overlay = document.getElementById("hiResearchOverlay");
    var drawer = document.getElementById("hiResearchDrawer");
    overlay.classList.add("is-open");
    drawer.classList.add("is-open");
    overlay.setAttribute("aria-hidden", "false");
    drawer.setAttribute("aria-hidden", "false");
    if ("inert" in overlay) overlay.inert = false;
    if ("inert" in drawer) drawer.inert = false;
    document.body.style.overflow = "hidden";
    document.getElementById("hiResearchHotel").textContent = state.hotelName || "Hotel";
    var kicker = document.getElementById("hiResearchKicker");
    var title = document.getElementById("hiResearchTitle");
    if (kicker) kicker.textContent = isShareMode() ? "Research Reports" : "Deep Research";
    if (title) title.textContent = isShareMode() ? "Completed Reports" : "Research Center";
    document.getElementById("hiResearchClose").focus();
    if (window.HotelExplorer && typeof window.HotelExplorer.applyChainScaleTheme === "function") {
      window.HotelExplorer.applyChainScaleTheme();
    }
    refresh();
  }

  function close(opts) {
    opts = opts || {};
    if (state.confirmOpen) closeConfirmModal({ silent: true });
    state.open = false;
    stopPoll();
    var overlay = document.getElementById("hiResearchOverlay");
    var drawer = document.getElementById("hiResearchDrawer");
    // Move focus out before aria-hiding the drawer (prevents aria-hidden + focused descendant).
    var restoreFocus =
      opts.restoreFocus ||
      document.querySelector("[data-hex-deep-research], [data-action='deep-research'], .hex-action--deep-research") ||
      document.getElementById("hexDeepResearchBtn") ||
      document.body;
    try {
      if (document.activeElement && drawer && drawer.contains(document.activeElement)) {
        if (restoreFocus && typeof restoreFocus.focus === "function") {
          restoreFocus.focus({ preventScroll: true });
        } else if (document.activeElement.blur) {
          document.activeElement.blur();
        }
      }
    } catch (_) {}
    if (overlay) {
      overlay.classList.remove("is-open");
      overlay.setAttribute("aria-hidden", "true");
      if ("inert" in overlay) overlay.inert = true;
    }
    if (drawer) {
      drawer.classList.remove("is-open");
      drawer.setAttribute("aria-hidden", "true");
      if ("inert" in drawer) drawer.inert = true;
    }
    document.body.style.overflow = "";
  }

  function stopPoll() {
    if (state.pollTimer) {
      clearInterval(state.pollTimer);
      state.pollTimer = null;
    }
  }

  function startPollIfNeeded() {
    stopPoll();
    if (isShareMode()) return;
    var active = state.payload && state.payload.active_requests && state.payload.active_requests.length;
    if (!active || !state.open) return;
    var live = state.payload.execution_mode && state.payload.execution_mode.live;
    state.pollTimer = setInterval(function () {
      if (!state.open) {
        stopPoll();
        return;
      }
      refresh({ silent: true });
    }, live ? 8000 : 1500);
  }

  function seedLifecycleFromPayload(payload) {
    state.knownActiveIds = {};
    var active = (payload && payload.active_requests) || [];
    active.forEach(function (r) {
      if (r && r.request_id) {
        state.knownActiveIds[r.request_id] = true;
        state.toastedStartIds[r.request_id] = true;
      }
    });
    var archive = (payload && payload.archive) || [];
    archive.forEach(function (a) {
      if (a && a.request_id) state.toastedTerminalIds[a.request_id] = true;
    });
  }

  function emitLifecycleToasts(prevActiveIds, payload) {
    if (!payload || state.seedLifecycleOnNextPayload) return;
    var active = payload.active_requests || [];
    var archive = payload.archive || [];
    active.forEach(function (r) {
      if (!r || !r.request_id) return;
      if (!state.toastedStartIds[r.request_id] && !prevActiveIds[r.request_id]) {
        // Started via this session's accept path — toast already fired on POST accept.
        state.toastedStartIds[r.request_id] = true;
      }
      state.knownActiveIds[r.request_id] = true;
    });
    archive.forEach(function (a) {
      if (!a || !a.request_id) return;
      if (state.toastedTerminalIds[a.request_id]) return;
      if (!prevActiveIds[a.request_id] && !state.knownActiveIds[a.request_id]) {
        state.toastedTerminalIds[a.request_id] = true;
        return;
      }
      state.toastedTerminalIds[a.request_id] = true;
      var name = a.display_name || "Investigation";
      var status = String(a.status || "").toUpperCase();
      if (status.indexOf("FAIL") >= 0) {
        showDealalityToast({
          kind: "error",
          title: "Research couldn't finish",
          body: name + " did not complete. You can retry when available.",
        });
      } else if (status.indexOf("OPEN_QUESTION") >= 0 || status === "COMPLETED_WITH_OPEN_QUESTIONS") {
        showDealalityToast({
          kind: "success",
          title: "Research complete",
          body: "The investigation is ready, with some questions still unresolved.",
        });
      } else {
        showDealalityToast({
          kind: "success",
          title: "Research complete",
          body: "Your " + name + " is ready to review.",
        });
      }
    });
    // Drop known-active ids that are no longer active
    state.knownActiveIds = {};
    active.forEach(function (r) {
      if (r && r.request_id) state.knownActiveIds[r.request_id] = true;
    });
  }

  function refresh(opts) {
    if (!state.hotelId) return;
    if (!(opts && opts.silent)) {
      state.loading = true;
      render();
    }
    var qs = [];
    if (state.hotelName) qs.push("hotel_name=" + encodeURIComponent(state.hotelName));
    if (isShareMode()) qs.push("share=1");
    var url =
      "/api/hotel-intelligence/hotels/" +
      encodeURIComponent(state.hotelId) +
      "/research" +
      (qs.length ? "?" + qs.join("&") : "");
    fetch(url)
      .then(function (r) {
        return r.json();
      })
      .then(function (payload) {
        state.loading = false;
        if (!payload || payload.ok === false) {
          state.error = "Could not load Research Center.";
          render();
          return;
        }
        var prevActive = Object.assign({}, state.knownActiveIds);
        if (state.seedLifecycleOnNextPayload) {
          seedLifecycleFromPayload(payload);
          state.seedLifecycleOnNextPayload = false;
        } else {
          emitLifecycleToasts(prevActive, payload);
        }
        state.payload = payload;
        state.error = null;
        if (payload.hotel && payload.hotel.hotel_name) {
          state.hotelName = payload.hotel.hotel_name;
          document.getElementById("hiResearchHotel").textContent = state.hotelName;
        }
        var st = payload.header && payload.header.status_line;
        if (st) {
          if (isShareMode() || (payload.mode_flags && payload.mode_flags.share_readonly)) {
            document.getElementById("hiResearchStatus").textContent =
              st.completed_investigations +
              " Completed Report" +
              (st.completed_investigations === 1 ? "" : "s") +
              (st.last_researched_display
                ? " · Last Researched: " + st.last_researched_display
                : "");
          } else {
            document.getElementById("hiResearchStatus").textContent =
              st.completed_investigations +
              " Completed Investigation" +
              (st.completed_investigations === 1 ? "" : "s") +
              " · " +
              st.active +
              " Active · Last Researched: " +
              (st.last_researched_display || "—");
          }
        }
        render();
        startPollIfNeeded();
      })
      .catch(function (err) {
        console.warn("[ResearchCenter] load failed", err);
        state.loading = false;
        state.error = "Could not load Research Center.";
        render();
      });
  }

  function render() {
    var body = document.getElementById("hiResearchBody");
    if (!body) return;
    if (state.loading && !state.payload) {
      body.innerHTML = '<p class="hi-research-empty">Loading Research Center…</p>';
      return;
    }
    if (state.error && !state.payload) {
      body.innerHTML =
        '<div class="hi-research-error" role="alert"><p>' +
        esc(state.error) +
        '</p><button type="button" class="hi-research-btn hi-research-btn--secondary" data-rc-refresh>Retry</button></div>';
      return;
    }
    if (state.view === "detail" && state.detailTemplateId) {
      body.innerHTML = renderDetail(state.detailTemplateId);
      return;
    }
    body.innerHTML = renderCenter(state.payload);
  }

  function shareScopedDossierUrl(dossierId, kind) {
    var base =
      "/api/hotel-intelligence/dossiers/" +
      encodeURIComponent(dossierId) +
      (kind === "pdf" ? "/pdf" : "");
    if (!isShareMode() || !state.hotelId) return base;
    return (
      base +
      "?share=1&share_hotel=" +
      encodeURIComponent(state.hotelId)
    );
  }

  function renderCenter(p) {
    if (!p) return '<p class="hi-research-empty">No research data.</p>';
    var html = "";
    var shareReadonly =
      isShareMode() ||
      (p.mode_flags && p.mode_flags.share_readonly === true);
    if (p.execution_mode && !shareReadonly) {
      html +=
        '<p class="hi-research-mode-badge" data-live="' +
        (p.execution_mode.live ? "1" : "0") +
        '">' +
        esc(p.execution_mode.label || "") +
        (p.execution_mode.internal_budget_hint
          ? " · " + esc(p.execution_mode.internal_budget_hint)
          : "") +
        "</p>";
    }

    if (!shareReadonly && p.active_requests && p.active_requests.length) {
      html += '<section class="hi-research-section"><h3 class="hi-research-section__title">Research In Progress</h3>';
      p.active_requests.forEach(function (r) {
        html +=
          '<article class="hi-research-card hi-research-card--progress">' +
          '<p class="hi-research-card__eyebrow"><span class="hi-research-activity" aria-hidden="true"></span> Research In Progress</p>' +
          "<h4>" +
          esc(r.display_name) +
          "</h4>" +
          '<p class="hi-research-meta">Started ' +
          esc(
            r.started_at
              ? new Date(r.started_at).toLocaleString()
              : r.created_at
                ? new Date(r.created_at).toLocaleString()
                : "just now"
          ) +
          "</p>" +
          '<p class="hi-research-stage">Current stage: ' +
          esc(r.stage_label || r.status_label || "Starting Research") +
          "</p></article>";
      });
      html += "</section>";
    }

    if (!shareReadonly && p.mode === "FIRST_USE_FULL_INVESTIGATION" && p.first_use) {
      html +=
        '<section class="hi-research-section"><h3 class="hi-research-section__title">Latest Investigation</h3>' +
        '<article class="hi-research-card hi-research-card--cta">' +
        "<h4>" +
        esc(p.first_use.title) +
        "</h4><p>" +
        esc(p.first_use.description) +
        "</p>" +
        '<button type="button" class="hi-research-btn hi-research-btn--primary" data-rc-run="FULL_HOTEL_INTELLIGENCE">Run Full Investigation</button>' +
        "</article></section>";
    } else if (p.latest_investigation) {
      html +=
        '<section class="hi-research-section"><h3 class="hi-research-section__title">' +
        (shareReadonly ? "Latest Report" : "Latest Investigation") +
        "</h3>" +
        renderLatestCard(p.latest_investigation) +
        "</section>";
    }

    if (!shareReadonly && p.recommended_follow_up && p.recommended_follow_up.length) {
      html += '<section class="hi-research-section"><h3 class="hi-research-section__title">Recommended Follow-up</h3>';
      p.recommended_follow_up.forEach(function (rec) {
        var tid = rec.template && rec.template.template_id;
        html +=
          '<article class="hi-research-card">' +
          "<h4>" +
          esc(rec.card_title) +
          '</h4><p class="hi-research-meta">Recommended investigation: ' +
          esc(rec.template && rec.template.display_name) +
          "</p><p>" +
          esc(rec.reason) +
          '</p><div class="hi-research-actions">' +
          '<button type="button" class="hi-research-btn hi-research-btn--secondary" data-rc-detail="' +
          esc(tid) +
          '">Review Scope</button>' +
          '<button type="button" class="hi-research-btn hi-research-btn--primary" data-rc-run="' +
          esc(tid) +
          '">Run Research</button>' +
          "</div></article>";
      });
      html += "</section>";
    }

    if (
      !shareReadonly &&
      p.research_more &&
      p.research_more.length &&
      p.mode === "AFTER_FULL_INVESTIGATION"
    ) {
      html += '<section class="hi-research-section"><h3 class="hi-research-section__title">Research More</h3><div class="hi-research-list">';
      p.research_more.forEach(function (t) {
        html +=
          '<button type="button" class="hi-research-row" data-rc-detail="' +
          esc(t.template_id) +
          '"><span class="hi-research-row__icon" aria-hidden="true">' +
          iconFor(t.icon) +
          '</span><span class="hi-research-row__text"><strong>' +
          esc(t.display_name) +
          '</strong><span class="hi-research-row__q">' +
          esc(t.short_question || t.customer_question) +
          '</span><span class="hi-research-row__d">' +
          esc(t.description) +
          '</span></span><span class="hi-research-row__chev" aria-hidden="true">›</span></button>';
      });
      html += "</div></section>";
    }

    html +=
      '<section class="hi-research-section"><h3 class="hi-research-section__title">' +
      (shareReadonly ? "Completed Reports" : "Research Archive") +
      "</h3>";
    if (!p.archive || !p.archive.length) {
      html += shareReadonly
        ? '<p class="hi-research-empty">No completed research reports are available for this hotel.</p>'
        : '<p class="hi-research-empty">No completed investigations yet.</p>';
    } else {
      p.archive.forEach(function (a) {
        html += renderArchiveRow(a);
      });
    }
    html += "</section>";
    return html;
  }

  function renderLatestCard(inv) {
    var m = inv.metrics || {};
    var scope = (inv.scope || []).map(function (s) {
      return "<li>" + esc(s) + "</li>";
    }).join("");
    return (
      '<article class="hi-research-card hi-research-card--latest" data-rc-report="' +
      esc(inv.report_id || "") +
      '">' +
      '<p class="hi-research-card__eyebrow">' +
      esc((inv.report_labels && inv.report_labels.report_type_label) || "Investigation") +
      "</p><h4>" +
      esc(inv.display_name) +
      '</h4><p class="hi-research-meta">' +
      esc(inv.status_label || inv.status) +
      (inv.completed_display ? " · " + esc(inv.completed_display) : "") +
      "</p>" +
      (scope ? '<p class="hi-research-label">Scope</p><ul class="hi-research-scope">' + scope + "</ul>" : "") +
      '<div class="hi-research-metrics"><span>' +
      esc(m.source_count != null ? m.source_count : inv.source_count) +
      " Sources</span><span>" +
      esc(m.finding_count != null ? m.finding_count : inv.finding_count) +
      " Findings</span><span>" +
      esc(m.open_question_count != null ? m.open_question_count : inv.open_question_count) +
      " Open Questions</span></div>" +
      '<div class="hi-research-actions">' +
      (inv.actions && inv.actions.view_report && inv.report_id
        ? '<button type="button" class="hi-research-btn hi-research-btn--primary" data-rc-view="' +
          esc(inv.report_id) +
          '">View Report</button>'
        : "") +
      (inv.actions && inv.actions.download_pdf && (inv.pdf_id || inv.report_id)
        ? '<button type="button" class="hi-research-btn hi-research-btn--secondary" data-rc-pdf="' +
          esc(inv.pdf_id || inv.report_id) +
          '">Download PDF</button>'
        : "") +
      "</div></article>"
    );
  }

  function renderArchiveRow(a) {
    var actions = "";
    if (a.actions && a.actions.view_report && a.report_id) {
      actions +=
        '<button type="button" class="hi-research-btn hi-research-btn--secondary" data-rc-view="' +
        esc(a.report_id) +
        '">View Report</button>';
    }
    if (a.actions && a.actions.download_pdf && (a.pdf_id || a.report_id)) {
      actions +=
        '<button type="button" class="hi-research-btn hi-research-btn--secondary" data-rc-pdf="' +
        esc(a.pdf_id || a.report_id) +
        '">Download PDF</button>';
    }
    var lineage = "";
    if (a.parent_report_label || a.parent_report_id) {
      lineage =
        '<p class="hi-research-lineage">Follow-up to: ' +
        esc(a.parent_report_label || "Full Hotel Intelligence Investigation") +
        (a.parent_completed_display ? " · " + esc(a.parent_completed_display) : "") +
        "</p>";
    }
    return (
      '<article class="hi-research-card hi-research-card--archive" data-rc-report="' +
      esc(a.report_id || "") +
      '">' +
      '<p class="hi-research-meta">' +
      esc(a.completed_display || "—") +
      "</p><h4>" +
      esc(a.display_name) +
      '</h4><p class="hi-research-meta">' +
      esc((a.report_labels && a.report_labels.report_type_label) || "") +
      " · " +
      esc(a.status_label || a.status) +
      "</p>" +
      lineage +
      '<p class="hi-research-meta">' +
      esc(a.source_count) +
      " sources · " +
      esc(a.finding_count) +
      " findings · " +
      esc(a.open_question_count) +
      " open questions</p>" +
      (actions ? '<div class="hi-research-actions">' + actions + "</div>" : "") +
      "</article>"
    );
  }

  function findTemplate(templateId) {
    var t = null;
    var list = (state.payload && state.payload.templates) || [];
    list.forEach(function (x) {
      if (x.template_id === templateId) t = x;
    });
    if (!t && state.payload && state.payload.research_more) {
      state.payload.research_more.forEach(function (x) {
        if (x.template_id === templateId) t = x;
      });
    }
    if (!t && state.payload && state.payload.recommended_follow_up) {
      state.payload.recommended_follow_up.forEach(function (rec) {
        if (rec.template && rec.template.template_id === templateId) t = rec.template;
      });
    }
    return t;
  }

  function renderDetail(templateId) {
    var t = findTemplate(templateId);
    if (!t) {
      return (
        '<button type="button" class="hi-research-back" data-rc-back>← Back to Research Center</button>' +
        '<p class="hi-research-empty">Investigation not found.</p>'
      );
    }
    var scopeHtml = "";
    if (t.scope_groups && t.scope_groups.length) {
      scopeHtml = t.scope_groups
        .map(function (g) {
          return (
            '<div class="hi-research-scope-group"><p class="hi-research-label">' +
            esc(g.label) +
            "</p><ul class=\"hi-research-scope\">" +
            (g.items || [])
              .map(function (s) {
                return "<li>" + esc(s) + "</li>";
              })
              .join("") +
            "</ul></div>"
          );
        })
        .join("");
    } else {
      scopeHtml =
        '<ul class="hi-research-scope">' +
        (t.scope_labels || [])
          .map(function (s) {
            return "<li>" + esc(s) + "</li>";
          })
          .join("") +
        "</ul>";
    }
    return (
      '<button type="button" class="hi-research-back" data-rc-back>← Back to Research Center</button>' +
      '<article class="hi-research-card hi-research-card--detail">' +
      "<h3>" +
      esc(t.display_name) +
      "</h3><p>" +
      esc(t.description) +
      '</p><p class="hi-research-label">Scope</p>' +
      scopeHtml +
      '<div class="hi-research-actions">' +
      (isShareMode()
        ? '<p class="hi-research-meta">Research runs are disabled on shared previews.</p>'
        : '<button type="button" class="hi-research-btn hi-research-btn--primary" data-rc-run="' +
          esc(t.template_id) +
          '">Run Research</button>') +
      "</div></article>"
    );
  }

  function iconFor(name) {
    var map = {
      people: "◎",
      brand: "◈",
      capital: "⬡",
      development: "▣",
      portfolio: "▦",
      opportunity: "◉",
      dossier: "▤",
      research: "◆",
    };
    return map[name] || map.research;
  }

  function onBodyClick(e) {
    var back = e.target.closest("[data-rc-back]");
    if (back) {
      state.view = "center";
      state.detailTemplateId = null;
      render();
      return;
    }
    var refreshBtn = e.target.closest("[data-rc-refresh]");
    if (refreshBtn) {
      refresh();
      return;
    }
    var detail = e.target.closest("[data-rc-detail]");
    if (detail) {
      if (isShareMode()) return;
      state.view = "detail";
      state.detailTemplateId = detail.getAttribute("data-rc-detail");
      render();
      return;
    }
    var view = e.target.closest("[data-rc-view]");
    if (view) {
      var reportId = view.getAttribute("data-rc-view");
      close({ restoreFocus: document.getElementById("hexDossierLayer") || document.body });
      if (window.HotelExplorer && typeof window.HotelExplorer.openDossierById === "function") {
        window.HotelExplorer.openDossierById(reportId, {
          shareHotel: isShareMode() ? state.hotelId : null,
        });
      } else if (typeof window.__hexOpenDossierById === "function") {
        window.__hexOpenDossierById(reportId, {
          shareHotel: isShareMode() ? state.hotelId : null,
        });
      }
      return;
    }
    var pdf = e.target.closest("[data-rc-pdf]");
    if (pdf) {
      var pdfId = pdf.getAttribute("data-rc-pdf");
      var viewId =
        (pdf.closest("[data-rc-report]") &&
          pdf.closest("[data-rc-report]").getAttribute("data-rc-report")) ||
        pdfId;
      // Packet 2.7-R6: fetch+blob so Content-Disposition filename is honored.
      // window.location navigation often ignores CD and reuses a generic name.
      var url = shareScopedDossierUrl(pdfId, "pdf");
      pdf.disabled = true;
      fetch(url, { credentials: "same-origin" })
        .then(function (res) {
          if (!res.ok) {
            return res.json().catch(function () {
              return { error: "pdf_failed" };
            }).then(function (j) {
              throw new Error((j && j.message) || (j && j.error) || "pdf_failed");
            });
          }
          var reportHdr = res.headers.get("X-Dealality-Report-Id") || "";
          if (viewId && reportHdr && String(reportHdr) !== String(viewId) && String(reportHdr) !== String(pdfId)) {
            throw new Error("download_report_id_mismatch");
          }
          var cd = res.headers.get("Content-Disposition") || "";
          var m =
            cd.match(/filename\*=UTF-8''([^;]+)/i) ||
            cd.match(/filename=\"([^\"]+)\"/i) ||
            cd.match(/filename=([^;]+)/i);
          var filename = (m && m[1] && decodeURIComponent(m[1].trim())) || "hotel-intelligence-report.pdf";
          return res.blob().then(function (blob) {
            return { blob: blob, filename: filename };
          });
        })
        .then(function (payload) {
          var a = document.createElement("a");
          var href = URL.createObjectURL(payload.blob);
          a.href = href;
          a.download = payload.filename;
          document.body.appendChild(a);
          a.click();
          a.remove();
          setTimeout(function () {
            URL.revokeObjectURL(href);
          }, 2000);
        })
        .catch(function (err) {
          console.error("[research-center] pdf download", err);
          window.alert(
            "Could not download PDF. " + (err && err.message ? err.message : "Please try again.")
          );
        })
        .finally(function () {
          pdf.disabled = false;
        });
      return;
    }
    var run = e.target.closest("[data-rc-run]");
    if (run) {
      openConfirmModal(run.getAttribute("data-rc-run"), run);
    }
  }

  function confirmationFromTemplate(t) {
    if (!t) {
      return {
        confirmation_eyebrow: "Deep Research",
        confirmation_title: "Start research?",
        confirmation_summary:
          "Dealality will conduct a new evidence-backed investigation of this hotel and add the completed research to its Research Archive.",
        confirmation_scope: [],
        confirmation_deliverables: [
          "Research Addendum",
          "Evidence-backed findings",
          "Sources & open questions",
          "Downloadable PDF",
          "Archived research history",
        ],
        confirmation_pricing: null,
      };
    }
    return {
      confirmation_eyebrow: t.confirmation_eyebrow || "Deep Research",
      confirmation_title: t.confirmation_title || "Start " + (t.display_name || "research") + "?",
      confirmation_summary:
        t.confirmation_summary ||
        "Dealality will conduct a new evidence-backed investigation of this hotel and add the completed research to its Research Archive.",
      confirmation_scope: t.confirmation_scope || (t.scope_labels || []).map(function (label) {
        return { title: label, detail: "" };
      }),
      confirmation_deliverables: t.confirmation_deliverables || [
        "Research Addendum",
        "Evidence-backed findings",
        "Sources & open questions",
        "Downloadable PDF",
        "Archived research history",
      ],
      confirmation_pricing: t.confirmation_pricing || null,
      display_name: t.display_name,
    };
  }

  function renderConfirmBody(templateId) {
    var t = findTemplate(templateId);
    var c = confirmationFromTemplate(t);
    var scopeHtml = (c.confirmation_scope || [])
      .map(function (item) {
        var title = typeof item === "string" ? item : item.title;
        var detail = typeof item === "string" ? "" : item.detail || "";
        return (
          '<li class="hi-rc-confirm__scope-item"><strong>' +
          esc(title) +
          "</strong>" +
          (detail ? '<span>' + esc(detail) + "</span>" : "") +
          "</li>"
        );
      })
      .join("");
    var deliverables = (c.confirmation_deliverables || [])
      .map(function (d) {
        return "<li>" + esc(d) + "</li>";
      })
      .join("");

    var pricingSlot = "";
    if (c.confirmation_pricing) {
      pricingSlot =
        '<div class="hi-rc-confirm__pricing" data-future-pricing="1">' +
        esc(JSON.stringify(c.confirmation_pricing)) +
        "</div>";
    }

    var internal = "";
    var controls =
      state.payload &&
      state.payload.internal_research_controls &&
      state.payload.internal_research_controls.visible
        ? state.payload.internal_research_controls
        : null;
    if (controls) {
      internal =
        '<details class="hi-rc-confirm__internal">' +
        "<summary>Internal research controls</summary>" +
        '<div class="hi-rc-confirm__internal-body">' +
        "<p>Provider maximum: $" +
        Number(controls.provider_maximum_usd || 5).toFixed(2) +
        "</p>" +
        "<p>Daily external budget: $" +
        Number(controls.daily_external_budget_usd || 25).toFixed(2) +
        "</p>" +
        "<p>Automatic paid retries: Off</p>" +
        "</div></details>";
    }

    return {
      body:
        '<p class="hi-rc-confirm__eyebrow">' +
        esc(c.confirmation_eyebrow) +
        "</p>" +
        '<h3 class="hi-rc-confirm__title" id="hiRcConfirmTitle">' +
        esc(c.confirmation_title) +
        "</h3>" +
        '<p class="hi-rc-confirm__summary" id="hiRcConfirmDesc">' +
        esc(c.confirmation_summary) +
        "</p>" +
        '<div class="hi-rc-confirm__section">' +
        '<p class="hi-rc-confirm__section-label">What we\'ll investigate</p>' +
        '<ul class="hi-rc-confirm__scope">' +
        scopeHtml +
        "</ul></div>" +
        '<div class="hi-rc-confirm__section">' +
        '<p class="hi-rc-confirm__section-label">What you\'ll receive</p>' +
        '<ul class="hi-rc-confirm__deliverables">' +
        deliverables +
        "</ul></div>" +
        pricingSlot +
        internal +
        '<div class="hi-rc-confirm__error" id="hiRcConfirmError" hidden></div>',
      actions:
        '<button type="button" class="hi-research-btn hi-research-btn--secondary" data-rc-confirm-cancel>Cancel</button>' +
        '<button type="button" class="hi-research-btn hi-research-btn--primary hi-rc-confirm__start" data-rc-confirm-start' +
        (state.confirmSubmitting ? " disabled" : "") +
        ">" +
        (state.confirmSubmitting
          ? '<span class="hi-rc-confirm__spinner" aria-hidden="true"></span> Starting Research…'
          : "Start Research") +
        "</button>",
    };
  }

  function paintConfirmModal(templateId) {
    var parts = renderConfirmBody(templateId);
    var body = document.getElementById("hiRcConfirmBody");
    var actions = document.getElementById("hiRcConfirmActions");
    if (body) body.innerHTML = parts.body;
    if (actions) actions.innerHTML = parts.actions;
  }

  function openConfirmModal(templateId, returnFocusEl) {
    if (isShareMode()) {
      showDealalityToast({
        kind: "error",
        title: "Shared preview",
        body: "Research runs are disabled on shared previews.",
      });
      return;
    }
    if (!state.hotelId || !templateId || state.confirmSubmitting) return;
    ensureDom();
    state.confirmTemplateId = templateId;
    state.confirmOpen = true;
    state.confirmSubmitting = false;
    state.confirmReturnFocusEl = returnFocusEl || document.activeElement;
    var root = document.getElementById("hiResearchConfirm");
    paintConfirmModal(templateId);
    root.removeAttribute("hidden");
    root.setAttribute("aria-hidden", "false");
    requestAnimationFrame(function () {
      root.classList.add("hi-rc-confirm--open");
      var startBtn = root.querySelector("[data-rc-confirm-start]");
      if (startBtn) startBtn.focus();
    });
  }

  function closeConfirmModal(opts) {
    var root = document.getElementById("hiResearchConfirm");
    if (!root) {
      state.confirmOpen = false;
      state.confirmTemplateId = null;
      state.confirmSubmitting = false;
      return;
    }
    root.classList.remove("hi-rc-confirm--open");
    state.confirmOpen = false;
    var returnEl = state.confirmReturnFocusEl;
    state.confirmReturnFocusEl = null;
    state.confirmTemplateId = null;
    state.confirmSubmitting = false;
    setTimeout(function () {
      root.setAttribute("hidden", "");
      root.setAttribute("aria-hidden", "true");
      if (!(opts && opts.silent) && returnEl && typeof returnEl.focus === "function") {
        try {
          returnEl.focus();
        } catch (e) {
          /* ignore */
        }
      }
    }, 200);
  }

  function setConfirmError(message) {
    var err = document.getElementById("hiRcConfirmError");
    if (!err) return;
    err.hidden = !message;
    err.textContent = message || "";
  }

  function setConfirmSubmitting(on) {
    state.confirmSubmitting = Boolean(on);
    if (state.confirmTemplateId) paintConfirmModal(state.confirmTemplateId);
  }

  function onConfirmClick(e) {
    if (e.target.closest("[data-rc-confirm-cancel]")) {
      if (state.confirmSubmitting) return;
      closeConfirmModal();
      return;
    }
    if (e.target.closest("[data-rc-confirm-start]")) {
      submitResearch(state.confirmTemplateId);
    }
  }

  function customerErrorMessage(res) {
    var code = res && res.json && res.json.error;
    var msg = (res && res.json && res.json.message) || "";
    if (code === "max_concurrent_webhound_runs" || code === "active_request_exists") {
      return (
        msg ||
        "Another Deep Research investigation is currently running. Try again when it completes."
      );
    }
    if (code === "daily_external_research_budget_exhausted" || code === "external_research_disabled") {
      return "Deep Research is currently unavailable. Try again shortly.";
    }
    return msg || "Research couldn't be started. Try again shortly.";
  }

  function submitResearch(templateId) {
    if (isShareMode()) {
      showDealalityToast({
        kind: "error",
        title: "Shared preview",
        body: "Research runs are disabled on shared previews.",
      });
      return;
    }
    if (!state.hotelId || !templateId || state.confirmSubmitting) return;
    var live =
      state.payload &&
      state.payload.governance &&
      state.payload.governance.external_research_enabled === true;
    setConfirmError("");
    setConfirmSubmitting(true);

    var token =
      "ui_" +
      state.hotelId +
      "_" +
      templateId +
      "_" +
      String(Date.now()).slice(0, -3);
    var payload = {
      template_id: templateId,
      hotel_name: state.hotelName,
      to_completion: false,
    };
    if (live) {
      payload.run_live = true;
      payload.confirm_spend = true;
      payload.authorized = true;
      payload.provider_strategy = "WEBHOUND";
    } else {
      payload.provider_strategy = "SIMULATION";
    }

    fetch(
      "/api/hotel-intelligence/hotels/" + encodeURIComponent(state.hotelId) + "/research/requests",
      {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "Idempotency-Key": token,
        },
        body: JSON.stringify(payload),
      }
    )
      .then(function (r) {
        return r.json().then(function (j) {
          return { status: r.status, json: j };
        });
      })
      .then(function (res) {
        if (res.status === 409 || (res.json && res.json.ok === false)) {
          var errMsg = customerErrorMessage(res);
          setConfirmSubmitting(false);
          setConfirmError(errMsg);
          showDealalityToast({
            kind: "error",
            title: "Research couldn't be started",
            body: errMsg,
          });
          return;
        }
        if (!res.json || !res.json.ok) {
          var failMsg = customerErrorMessage(res);
          setConfirmSubmitting(false);
          setConfirmError(failMsg);
          showDealalityToast({
            kind: "error",
            title: "Research couldn't be started",
            body: failMsg,
          });
          return;
        }

        var reqId = res.json.request && res.json.request.request_id;
        var displayName =
          (findTemplate(templateId) && findTemplate(templateId).display_name) ||
          "Investigation";
        if (reqId) {
          state.toastedStartIds[reqId] = true;
          state.knownActiveIds[reqId] = true;
        }
        closeConfirmModal({ silent: true });
        state.view = "center";
        state.detailTemplateId = null;
        showDealalityToast({
          kind: "info",
          title: "Research started",
          body:
            displayName + " is now running. You can close this panel and return at any time.",
        });
        refresh();
      })
      .catch(function (err) {
        console.warn("[ResearchCenter] run failed", err);
        setConfirmSubmitting(false);
        var failMsg = "Research couldn't be started. Try again shortly.";
        setConfirmError(failMsg);
        showDealalityToast({
          kind: "error",
          title: "Research couldn't be started",
          body: failMsg,
        });
      });
  }

  window.HotelIntelligenceResearchCenter = {
    open: open,
    close: close,
    refresh: refresh,
    isOpen: function () {
      return state.open;
    },
  };
})();
