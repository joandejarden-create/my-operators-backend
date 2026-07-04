/**
 * Client-side admin gate for internal Support runbook pages.
 * Nav + route roles are enforced in app.js; this blocks direct URL access for non-admins.
 */
(function (global) {
  "use strict";

  function getAuth() {
    return global.DealalityMemberstackAuth || null;
  }

  async function fetchMe() {
    var auth = getAuth();
    if (!auth || typeof auth.authFetch !== "function") {
      return { ok: false, reason: "auth_unavailable" };
    }
    try {
      var res = await auth.authFetch("/api/me", { maxWaitMs: 20000 });
      if (!res.ok) {
        return { ok: false, reason: "http_" + res.status };
      }
      return { ok: true, data: await res.json() };
    } catch (err) {
      return { ok: false, reason: err && err.message ? err.message : "network_error" };
    }
  }

  function isAdminMe(data) {
    return !!(data && data.dealality && data.dealality.isAdmin);
  }

  function isInternalRunbookMe(data) {
    return isAdminMe(data);
  }

  function resolveEl(idOrEl) {
    if (!idOrEl) return null;
    return typeof idOrEl === "string" ? document.getElementById(idOrEl) : idOrEl;
  }

  /**
   * Full-page wave loader (DealalityWaveLoader) for admin runbook pages.
   * @param {string|HTMLElement} loadingIdOrEl
   * @param {string} [message]
   */
  function showPageLoading(loadingIdOrEl, message) {
    var loadingEl = resolveEl(loadingIdOrEl);
    if (!loadingEl) return;

    var wave = global.DealalityWaveLoader;
    if (wave && typeof wave.html === "function") {
      loadingEl.className = "support-page-loading";
      loadingEl.innerHTML = wave.html(message || "Loading…", { fixed: true });
    } else if (typeof console !== "undefined" && console.warn) {
      console.warn("[SupportAdminGate] DealalityWaveLoader unavailable; using text fallback.");
      loadingEl.className = "support-gate-state";
      loadingEl.innerHTML = "<p>" + (message || "Loading…") + "</p>";
    }

    loadingEl.hidden = false;
    loadingEl.setAttribute("aria-busy", "true");
  }

  /**
   * @param {string|HTMLElement} loadingIdOrEl
   * @param {string} message
   */
  function setPageLoadingMessage(loadingIdOrEl, message) {
    var loadingEl = resolveEl(loadingIdOrEl);
    if (!loadingEl) return;

    var main = loadingEl.querySelector(".loading-text-main");
    if (main) {
      main.textContent = message;
      return;
    }

    showPageLoading(loadingEl, message);
  }

  /**
   * @param {string|HTMLElement} loadingIdOrEl
   */
  function hidePageLoading(loadingIdOrEl) {
    var loadingEl = resolveEl(loadingIdOrEl);
    if (!loadingEl) return;
    loadingEl.hidden = true;
    loadingEl.removeAttribute("aria-busy");
  }

  async function requireInternalRunbook(options) {
    options = options || {};
    var loadingEl = document.getElementById(options.loadingId || "supportGateLoading");
    var deniedEl = document.getElementById(options.deniedId || "supportGateDenied");
    var contentEl = document.getElementById(options.contentId || "supportGateContent");

    var result = await fetchMe();

    if (!result.ok) {
      if (loadingEl) hidePageLoading(loadingEl);
      if (deniedEl) {
        deniedEl.hidden = false;
        var msg = deniedEl.querySelector("[data-gate-message]");
        if (msg) {
          msg.textContent =
            result.reason === "auth_unavailable"
              ? "Sign in through the Dealality app to view this page."
              : "Authentication required. Sign in and try again.";
        }
      }
      if (contentEl) contentEl.hidden = true;
      return false;
    }

    if (!isInternalRunbookMe(result.data)) {
      if (loadingEl) hidePageLoading(loadingEl);
      if (deniedEl) {
        deniedEl.hidden = false;
        var deniedMsg = deniedEl.querySelector("[data-gate-message]");
        if (deniedMsg) {
          deniedMsg.textContent =
            "Internal runbook access required. This page is for Dealality platform administrators only.";
        }
      }
      if (contentEl) contentEl.hidden = true;
      return false;
    }

    if (deniedEl) deniedEl.hidden = true;
    if (contentEl) contentEl.hidden = true;
    if (typeof options.onAllowed === "function") {
      options.onAllowed(result.data);
    }
    return true;
  }

  /**
   * @param {{
   *   loadingId?: string,
   *   deniedId?: string,
   *   contentId?: string,
   *   onAllowed?: (data: object) => void,
   * }} options
   */
  async function requireAdmin(options) {
    options = options || {};
    var loadingEl = document.getElementById(options.loadingId || "supportGateLoading");
    var deniedEl = document.getElementById(options.deniedId || "supportGateDenied");
    var contentEl = document.getElementById(options.contentId || "supportGateContent");

    var result = await fetchMe();

    if (loadingEl) hidePageLoading(loadingEl);

    if (!result.ok) {
      if (deniedEl) {
        deniedEl.hidden = false;
        var msg = deniedEl.querySelector("[data-gate-message]");
        if (msg) {
          msg.textContent =
            result.reason === "auth_unavailable"
              ? "Sign in through the Dealality app to view this page."
              : "Authentication required. Sign in and try again.";
        }
      }
      if (contentEl) contentEl.hidden = true;
      return false;
    }

    if (!isAdminMe(result.data)) {
      if (deniedEl) deniedEl.hidden = false;
      if (contentEl) contentEl.hidden = true;
      return false;
    }

    if (deniedEl) deniedEl.hidden = true;
    if (contentEl) contentEl.hidden = false;
    if (typeof options.onAllowed === "function") {
      options.onAllowed(result.data);
    }
    return true;
  }

  global.SupportAdminGate = {
    fetchMe: fetchMe,
    isAdminMe: isAdminMe,
    isInternalRunbookMe: isInternalRunbookMe,
    showPageLoading: showPageLoading,
    setPageLoadingMessage: setPageLoadingMessage,
    hidePageLoading: hidePageLoading,
    requireAdmin: requireAdmin,
    requireInternalRunbook: requireInternalRunbook,
  };
})(window);
