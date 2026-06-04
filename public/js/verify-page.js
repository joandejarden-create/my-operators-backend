/**
 * /verify — Memberstack email verification landing (matches Basic plan redirect slug "verify").
 */
(function (global) {
  "use strict";

  var MS_SCRIPT_SRC = "https://static.memberstack.com/scripts/v1/memberstack.js";

  var VERIFIED_TITLE = "Your email is verified.";
  var VERIFIED_DETAIL =
    "Your account stays under review until our team approves access (typically 1–2 business days). You will receive another email when you are approved.";

  function parseMemberQuery() {
    try {
      var params = new URLSearchParams(global.location.search || "");
      var raw = params.get("member");
      if (!raw) return null;
      var decoded = raw;
      try {
        decoded = decodeURIComponent(raw);
      } catch (_) {
        decoded = raw;
      }
      return JSON.parse(decoded);
    } catch (_) {
      return null;
    }
  }

  function urlIndicatesVerified() {
    var parsed = parseMemberQuery();
    if (parsed && parsed.verified === true) return true;
    var params = new URLSearchParams(global.location.search || "");
    if (params.get("forceRefetch") === "true" && params.get("member")) {
      var raw = params.get("member") || "";
      if (/verified["']?\s*:\s*true/i.test(raw)) return true;
    }
    return false;
  }

  function setStatus(title, detail, showResend) {
    var statusEl = global.document.getElementById("verify-status");
    var detailEl = global.document.getElementById("verify-detail");
    var resendWrap = global.document.getElementById("verify-resend-wrap");
    var panel = global.document.getElementById("verify-panel");
    if (statusEl) statusEl.textContent = title;
    if (detailEl) {
      detailEl.textContent = detail || "";
      detailEl.style.display = detail ? "block" : "none";
    }
    if (resendWrap) resendWrap.style.display = showResend ? "block" : "none";
    if (panel) panel.classList.add("verify-panel--ready");
    if (global.document.body) global.document.body.classList.add("verify-page--ready");
  }

  function showVerifiedFromUrl() {
    if (!urlIndicatesVerified()) return false;
    setStatus(VERIFIED_TITLE, VERIFIED_DETAIL, false);
    return true;
  }

  function loadMemberstackScript(appId) {
    return new Promise(function (resolve, reject) {
      if (!appId) {
        reject(new Error("MEMBERSTACK_APP_ID not configured"));
        return;
      }
      var auth = global.DealalityMemberstackAuth;
      var existing =
        global.$memberstackDom ||
        global.memberstack ||
        (auth && auth.getMemberstackDom && auth.getMemberstackDom());
      if (existing) {
        resolve(existing);
        return;
      }
      function waitReady() {
        if (auth && typeof auth.waitForMemberstackDom === "function") {
          auth
            .waitForMemberstackDom(20000)
            .then(function (ms) {
              if (ms) resolve(ms);
              else reject(new Error("Memberstack did not initialize"));
            })
            .catch(reject);
          return;
        }
        var start = Date.now();
        var poll = setInterval(function () {
          var ms2 = global.$memberstackDom || global.memberstack;
          if (ms2) {
            clearInterval(poll);
            resolve(ms2);
          } else if (Date.now() - start > 20000) {
            clearInterval(poll);
            reject(new Error("Memberstack did not initialize"));
          }
        }, 150);
      }
      if (global.document.querySelector("script[data-memberstack-app]")) {
        waitReady();
        return;
      }
      var script = global.document.createElement("script");
      script.src = MS_SCRIPT_SRC;
      script.async = true;
      script.setAttribute("data-memberstack-app", appId);
      script.onload = waitReady;
      script.onerror = function () {
        reject(new Error("Memberstack script failed to load"));
      };
      (global.document.head || global.document.documentElement).appendChild(script);
    });
  }

  function memberLooksVerified(member) {
    if (!member) return false;
    var data = member.data || member;
    if (data.verified === true) return true;
    if (data.auth && data.auth.verified === true) return true;
    return urlIndicatesVerified();
  }

  function applyHomeUrl(homeUrl) {
    if (!homeUrl) return;
    var link = global.document.getElementById("dealality-home-link");
    if (link) link.href = homeUrl;
  }

  async function run() {
    if (showVerifiedFromUrl()) {
      try {
        var cfgRes = await fetch("/api/signup/config");
        var cfg = await cfgRes.json();
        if (cfg && cfg.homeUrl) applyHomeUrl(cfg.homeUrl);
      } catch (_) {
        /* default href in HTML */
      }
      return;
    }

    try {
      var cfgRes0 = await fetch("/api/signup/config");
      var cfg0 = await cfgRes0.json();
      if (cfg0 && cfg0.homeUrl) applyHomeUrl(cfg0.homeUrl);
    } catch (_) {
      /* default href in HTML */
    }

    try {
      var cfgRes = await fetch("/api/auth/memberstack-config");
      var cfg = await cfgRes.json();
      if (!cfg.configured || !cfg.appId) {
        setStatus(
          "Verification page is not fully configured.",
          "Set MEMBERSTACK_APP_ID on the server, then open this link again.",
          false
        );
        return;
      }

      var ms = await loadMemberstackScript(cfg.appId);
      if (typeof ms.getCurrentMember === "function") {
        try {
          var member = await ms.getCurrentMember();
          if (memberLooksVerified(member)) {
            setStatus(VERIFIED_TITLE, VERIFIED_DETAIL, false);
            return;
          }
        } catch (_) {
          /* continue */
        }
      }

      setStatus(
        "Check your inbox for the verification link.",
        "If you already clicked the link, wait a moment and refresh this page. Otherwise use Resend below.",
        true
      );
    } catch (err) {
      if (urlIndicatesVerified()) {
        setStatus(VERIFIED_TITLE, VERIFIED_DETAIL, false);
        return;
      }
      setStatus(
        "We could not load verification.",
        (err && err.message) || "Refresh the page or contact support.",
        true
      );
    }
  }

  function boot() {
    showVerifiedFromUrl();
    run();
  }

  if (global.document.readyState === "loading") {
    global.document.addEventListener("DOMContentLoaded", boot);
  } else {
    boot();
  }
})(window);
