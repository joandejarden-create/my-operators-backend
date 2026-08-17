/**
 * Local /app shell: load Memberstack, obtain JWT, publish to embedded pages (My Deals, etc.).
 * Load after dealality-memberstack-auth.js and dealality-webflow-embed-parent.js.
 */
(function (global) {
  "use strict";

  /** v2 — same script as dealality.com Webflow; URL also served from /api/auth/memberstack-config. */
  var MS_SCRIPT_DEFAULT = "https://static.memberstack.com/scripts/v2/memberstack.js";
  var bootPromise = null;
  var shellAppId = "";
  var shellMemberstackScriptUrl = "";
  var shellMemberstackConfig = null;
  var lastBootstrappedJwt = "";
  var cachedBootstrapResult = null;
  var bootstrapInFlight = null;
  var loginPollBootstrapInFlight = null;
  var loginModalInFlight = false;
  var userNotFoundWarned = false;

  function clearBootstrapCache() {
    bootPromise = null;
    bootstrapInFlight = null;
    cachedBootstrapResult = null;
    lastBootstrappedJwt = "";
    userNotFoundWarned = false;
  }

  function apiBase() {
    var b = (global.DEALALITY_API_BASE || global.DEALALITY_API_BASE_URL || "").trim();
    if (b) return b.replace(/\/$/, "");
    try {
      return (global.location && global.location.origin) || "";
    } catch (_) {
      return "";
    }
  }

  function dispatch(name, detail) {
    try {
      global.dispatchEvent(new CustomEvent(name, { detail: detail || {} }));
    } catch (_) {}
  }

  function getAuthModule() {
    return global.DealalityMemberstackAuth || null;
  }

  function getEmbedParent() {
    return global.DealalityEmbedParent || null;
  }

  function jwtMemberstackId(jwt) {
    if (!jwt || typeof jwt !== "string") return "";
    try {
      var parts = jwt.split(".");
      if (parts.length < 2) return "";
      var b64 = parts[1].replace(/-/g, "+").replace(/_/g, "/");
      var pad = b64.length % 4;
      if (pad) b64 += "====".slice(pad);
      var payload = JSON.parse(global.atob(b64));
      return String(payload.sub || payload.memberId || payload.id || "").trim();
    } catch (_) {
      return "";
    }
  }

  function isSandboxMemberstackId(memberstackId) {
    return String(memberstackId || "").indexOf("mem_sb_") === 0;
  }

  function localhostSandboxLoginNote(config, jwt) {
    if (!config || !jwt) return null;
    var id = jwtMemberstackId(jwt);
    if (!isSandboxMemberstackId(id)) return null;
    return (
      config.localhostAuthNote ||
      "Localhost login uses Test Mode (mem_sb_…). /api/me matches by email; set MEMBERSTACK_TEST_SECRET_KEY in .env, " +
        "or use ?msToken= from dealality.com for live mem_cmq… ids."
    );
  }

  function publishJwt(jwt) {
    if (!jwt) return;
    global.__dealalityMemberstackJwt = jwt;
    var embed = getEmbedParent();
    if (embed && typeof embed.publishJwt === "function") {
      embed.publishJwt(jwt);
    }
  }

  function memberstackClientReady() {
    var auth = getAuthModule();
    var ms = auth && auth.getMemberstackDom ? auth.getMemberstackDom() : null;
    if (!ms) ms = global.$memberstackDom || global.memberstack || global.memberstackDom;
    if (!ms || typeof ms !== "object") return null;
    if (typeof ms.openModal === "function") return ms;
    if (typeof ms.getCurrentMember === "function") return ms;
    return null;
  }

  /** Memberstack pre-built modals do not auto-close — must call hideModal() after login. */
  function closeMemberstackModal() {
    var ms = memberstackClientReady();
    if (ms && typeof ms.hideModal === "function") {
      try {
        var out = ms.hideModal();
        if (out && typeof out.then === "function") out.catch(function () {});
      } catch (_) {}
    }
    if (global.$memberstackDom && global.$memberstackDom !== ms && typeof global.$memberstackDom.hideModal === "function") {
      try {
        var out2 = global.$memberstackDom.hideModal();
        if (out2 && typeof out2.then === "function") out2.catch(function () {});
      } catch (_) {}
    }
    try {
      global.document.body.classList.remove("ms-modal-open", "memberstack-modal-open");
      global.document.documentElement.classList.remove("ms-modal-open", "memberstack-modal-open");
    } catch (_) {}
  }

  function isEmptyMemberstackModalResult(result) {
    if (result == null) return false;
    if (typeof result !== "object") return false;
    if (result.data || result.type) return false;
    return Object.keys(result).length === 0;
  }

  function setAuthGateMemberstackModalOpen(isOpen) {
    var gate = global.document.getElementById("dealalityAppShellAuthGate");
    if (!gate) return;
    gate.classList.toggle("is-memberstack-modal-open", Boolean(isOpen));
  }

  function openMemberstackLoginModal(ms) {
    var names = ["LOGIN", "login"];
    var lastErr = null;
    return (async function () {
      for (var i = 0; i < names.length; i++) {
        try {
          return await ms.openModal(names[i]);
        } catch (err) {
          lastErr = err;
        }
      }
      if (lastErr) throw lastErr;
    })();
  }

  async function completeShellSessionAfterLogin() {
    var auth = getAuthModule();
    if (!auth) return false;
    var jwt = await auth.getMemberstackJwtWhenReady(12000);
    if (!jwt) return false;

    closeMemberstackModal();
    hideAuthGate();
    publishJwt(jwt);

    if (jwt !== lastBootstrappedJwt) {
      bootPromise = null;
      cachedBootstrapResult = null;
      userNotFoundWarned = false;
    }
    var result = await whenReady();
    return Boolean(result && result.ok && result.authorized !== false);
  }

  function watchForMemberstackLoginWhileModalOpen(ms, onSuccess) {
    var stopped = false;
    var unsub = null;
    var pollId = null;
    var timeoutId = null;

    function stop() {
      if (stopped) return;
      stopped = true;
      if (pollId) clearInterval(pollId);
      if (timeoutId) clearTimeout(timeoutId);
      if (typeof unsub === "function") {
        try {
          unsub();
        } catch (_) {}
      }
    }

    function tryComplete() {
      if (stopped || loginPollBootstrapInFlight) return;
      loginPollBootstrapInFlight = completeShellSessionAfterLogin()
        .then(function (ok) {
          if (ok) {
            stop();
            if (typeof onSuccess === "function") onSuccess();
          }
        })
        .finally(function () {
          loginPollBootstrapInFlight = null;
        });
    }

    if (ms && typeof ms.onAuthChange === "function") {
      try {
        unsub = ms.onAuthChange(function (member) {
          if (member && (member.data || member.auth)) tryComplete();
        });
      } catch (_) {}
    }

    var pollId = setInterval(tryComplete, 450);
    var timeoutId = setTimeout(stop, 10 * 60 * 1000);

    return stop;
  }

  function loadMemberstackDomScript(appId) {
    return new Promise(function (resolve, reject) {
      if (!appId) {
        reject(new Error("MEMBERSTACK_APP_ID not configured on server"));
        return;
      }
      var ready = memberstackClientReady();
      if (ready) {
        resolve(ready);
        return;
      }

      function waitForClient() {
        var auth = getAuthModule();
        if (!auth || typeof auth.waitForMemberstackDom !== "function") {
          var pollStart = Date.now();
          var poll = setInterval(function () {
            var ms = memberstackClientReady();
            if (ms) {
              clearInterval(poll);
              resolve(ms);
              return;
            }
            if (Date.now() - pollStart >= 20000) {
              clearInterval(poll);
              reject(new Error("Memberstack client did not initialize (no $memberstackDom)"));
            }
          }, 150);
          return;
        }
        auth
          .waitForMemberstackDom(20000)
          .then(function (ms) {
            if (ms) resolve(ms);
            else reject(new Error("Memberstack client did not initialize"));
          })
          .catch(reject);
      }

      var scriptSrc = (shellMemberstackScriptUrl || MS_SCRIPT_DEFAULT).trim();
      var existing = global.document && global.document.querySelector("script[data-memberstack-app]");
      if (existing) {
        if (existing.src === scriptSrc) {
          waitForClient();
          return;
        }
        try {
          existing.parentNode.removeChild(existing);
        } catch (_) {}
      }

      var script = global.document.createElement("script");
      script.src = scriptSrc;
      script.async = true;
      script.type = "text/javascript";
      script.setAttribute("data-memberstack-app", appId);
      script.onload = function () {
        waitForClient();
      };
      script.onerror = function () {
        reject(new Error("Memberstack script failed to load from " + scriptSrc));
      };
      (global.document.head || global.document.documentElement).appendChild(script);
    });
  }

  function setGateStatus(message, isError) {
    var el = global.document.getElementById("dealalityAppShellAuthStatus");
    if (!el) return;
    el.textContent = message || "";
    el.hidden = !message;
    el.classList.toggle("is-error", Boolean(isError));
  }

  function ensureAuthGateUi() {
    var container = global.document.getElementById("frameContainer");
    if (!container) return null;
    var gate = global.document.getElementById("dealalityAppShellAuthGate");
    if (gate) return gate;
    gate = global.document.createElement("div");
    gate.id = "dealalityAppShellAuthGate";
    gate.className = "app-shell-auth-gate";
    gate.setAttribute("role", "region");
    gate.setAttribute("aria-label", "Sign in required");
    gate.innerHTML =
      '<div class="app-shell-auth-gate-card">' +
      "<h2>Sign in to continue</h2>" +
      "<p>Log in with your Dealality account to load deals and other workspace pages on this server.</p>" +
      '<p id="dealalityAppShellAuthStatus" class="app-shell-auth-gate-status" hidden></p>' +
      '<div class="app-shell-auth-gate-actions">' +
      '<button type="button" class="app-shell-auth-btn primary" id="dealalityAppShellLoginBtn">Log in</button>' +
      '<button type="button" class="app-shell-auth-btn" id="dealalityAppShellReloadBtn">Reload after login</button>' +
      "</div>" +
      '<p class="app-shell-auth-gate-hint">Do not add localhost in Memberstack Application Domains — it is blocked in both Live and Test. Local login still works via Test Mode (<code>mem_sb_…</code>); the server matches your Airtable row by email. For live <code>mem_cmq…</code> ids, log in on <strong>dealality.com</strong> and append <code>?msToken=</code> with your <code>eyJ…</code> token.</p>' +
      "</div>";
    container.parentNode.insertBefore(gate, container);
    return gate;
  }

  async function openLoginModal(appId) {
    if (loginModalInFlight) return;
    loginModalInFlight = true;

    var auth = getAuthModule();
    var loginBtn = global.document.getElementById("dealalityAppShellLoginBtn");
    var stopWatch = null;
    var loginSucceeded = false;
    if (loginBtn) {
      loginBtn.disabled = true;
      loginBtn.textContent = "Loading Sign-In…";
    }
    setGateStatus("Loading Memberstack…", false);
    try {
      if (!appId) {
        setGateStatus("Set MEMBERSTACK_APP_ID in the server .env file, then restart npm start.", true);
        return;
      }
      var ms = await loadMemberstackDomScript(appId);
      if (!ms || typeof ms.openModal !== "function") {
        setGateStatus(
          "Memberstack loaded but login UI is unavailable. Use ?msToken= with your eyJ… token, or check the browser console.",
          true
        );
        return;
      }
      setGateStatus("", false);
      setAuthGateMemberstackModalOpen(true);
      stopWatch = watchForMemberstackLoginWhileModalOpen(ms, function () {
        loginSucceeded = true;
        setGateStatus("", false);
        setAuthGateMemberstackModalOpen(false);
      });
      var modalResult = null;
      var showedModalOpenError = false;
      try {
        modalResult = await openMemberstackLoginModal(ms);
      } catch (_) {
        /* User closed the modal or login failed — keep polling briefly for late auth events. */
      }

      if (
        isEmptyMemberstackModalResult(modalResult) &&
        auth &&
        typeof auth.getMemberstackJwtWhenReady === "function"
      ) {
        var jwtAfterOpen = await auth.getMemberstackJwtWhenReady(1200);
        if (!jwtAfterOpen) {
          showedModalOpenError = true;
          setGateStatus(
            "Login window did not stay open. Click Log in again, or use ?msToken= from dealality.com on localhost.",
            true
          );
        }
      }

      if (!loginSucceeded) {
        loginSucceeded = await completeShellSessionAfterLogin();
      }
      if (!loginSucceeded && !showedModalOpenError) {
        setGateStatus("Sign in was cancelled or could not complete. Click Log in to try again.", false);
      }
    } catch (err) {
      setGateStatus((err && err.message) || "Could not load Memberstack.", true);
    } finally {
      if (typeof stopWatch === "function") stopWatch();
      setAuthGateMemberstackModalOpen(false);
      if (loginBtn) {
        loginBtn.disabled = false;
        loginBtn.textContent = "Log in";
      }
      loginModalInFlight = false;
    }
  }

  function showAuthGate(appId) {
    shellAppId = appId || "";
    var gate = ensureAuthGateUi();
    if (!gate) return;
    gate.hidden = false;

    if (!shellAppId) {
      setGateStatus("Set MEMBERSTACK_APP_ID in the server .env file, then restart npm start.", true);
    } else {
      setGateStatus("Preparing sign-in…", false);
      loadMemberstackDomScript(shellAppId)
        .then(function () {
          setGateStatus("", false);
        })
        .catch(function (err) {
          setGateStatus((err && err.message) || "Memberstack failed to load.", true);
        });
    }

    var loginBtn = global.document.getElementById("dealalityAppShellLoginBtn");
    var reloadBtn = global.document.getElementById("dealalityAppShellReloadBtn");
    if (loginBtn) {
      loginBtn.onclick = function () {
        openLoginModal(shellAppId);
      };
    }
    if (reloadBtn) {
      reloadBtn.onclick = function () {
        global.location.reload();
      };
    }
  }

  function hideAuthGate() {
    var gate = global.document.getElementById("dealalityAppShellAuthGate");
    if (gate) gate.hidden = true;
    setGateStatus("", false);
  }

  function updateShellUserFromMe(data) {
    if (!data) return;
    if (global.DealalityWebflowUserChrome && typeof global.DealalityWebflowUserChrome.apply === "function") {
      global.DealalityWebflowUserChrome.apply(data);
      return;
    }
    var u = data.user || data.airtable;
    if (!u) return;
    var name =
      [u.firstName, u.lastName].filter(Boolean).join(" ").trim() ||
      (u.email ? String(u.email).split("@")[0] : "") ||
      "Account";
    var meta = u.email || (data.dealality && data.dealality.role ? data.dealality.role : "Account settings");
    global.document.querySelectorAll(".user-block .user-name, .account-dropdown-header .user-name").forEach(function (el) {
      el.textContent = name;
    });
    global.document.querySelectorAll(".user-block .user-meta, .account-dropdown-header .user-meta").forEach(function (el) {
      el.textContent = meta;
    });
    var initial = name.charAt(0).toUpperCase();
    global.document.querySelectorAll(".user-avatar").forEach(function (el) {
      el.textContent = initial;
    });
  }

  async function fetchMe(jwt) {
    var auth = getAuthModule();
    if (!auth || !jwt) return null;
    var headers = await auth.getAuthHeaders(null, { waitForLogin: false });
    if (headers.error) return null;
    var url = apiBase() + "/api/me";
    var res = await fetch(url, { method: "GET", headers: headers.headers, credentials: "omit" });
    var data = await res.json().catch(function () {
      return {};
    });
    if (!res.ok) {
      return { ok: false, status: res.status, data: data };
    }
    return { ok: true, status: res.status, data: data };
  }

  function warnUserNotFoundOnce(meResult) {
    if (userNotFoundWarned || !meResult || !meResult.data) return;
    if (meResult.data.error !== "user_not_found") return;
    userNotFoundWarned = true;
    var msId = meResult.data.memberstackId || jwtMemberstackId(lastBootstrappedJwt) || "unknown";
    var sandboxNote = localhostSandboxLoginNote(shellMemberstackConfig, lastBootstrappedJwt);
    var msg =
      "Memberstack signed in, but no Airtable Users row matched (member id " +
      msId +
      "). " +
      (sandboxNote
        ? sandboxNote
        : "Set Unique Webflow ID / Slug on the Users row, or log in as a linked account.");
    if (global.console && global.console.warn) {
      global.console.warn("[DealalityAppShellAuth]", msg);
    }
    dispatch("dealality-shell-auth-error", {
      ok: false,
      error: "user_not_found",
      message: msg,
      memberstackId: msId,
      me: meResult,
    });
    var gate = global.document.getElementById("dealalityAppShellAuthGate");
    if (gate) {
      gate.hidden = false;
      setGateStatus(msg, true);
    }
  }

  async function bootstrap() {
    global.DEALALITY_API_BASE = apiBase();
    var auth = getAuthModule();
    if (!auth) {
      var missing = { ok: false, error: "auth_js_missing", message: "dealality-memberstack-auth.js not loaded" };
      dispatch("dealality-shell-auth-error", missing);
      return missing;
    }

    var config = { appId: "", configured: false };
    try {
      var configRes = await fetch(apiBase() + "/api/auth/memberstack-config", {
        method: "GET",
        headers: { Accept: "application/json" },
      });
      config = await configRes.json().catch(function () {
        return config;
      });
    } catch (_) {}

    shellMemberstackConfig = config;
    shellMemberstackScriptUrl = (config.memberstackScript || MS_SCRIPT_DEFAULT).trim();
    var appId = (config.appId || "").trim();
    shellAppId = appId;
    var jwt = await auth.getMemberstackJwtWhenReady(800);

    if (jwt && jwt === lastBootstrappedJwt && cachedBootstrapResult) {
      return cachedBootstrapResult;
    }

    if (!jwt && appId) {
      try {
        await loadMemberstackDomScript(appId);
        jwt = await auth.getMemberstackJwtWhenReady(1500);
        if (!jwt) {
          showAuthGate(appId);
          jwt = await auth.getMemberstackJwtWhenReady(25000);
        }
      } catch (loadErr) {
        dispatch("dealality-shell-auth-error", {
          ok: false,
          error: "memberstack_load_failed",
          message: loadErr && loadErr.message ? loadErr.message : "Memberstack failed to load",
        });
        showAuthGate(appId);
        return { ok: false, error: "memberstack_load_failed" };
      }
    }

    if (!jwt) {
      showAuthGate(appId);
      dispatch("dealality-shell-auth-error", {
        ok: false,
        error: "not_logged_in",
        message: auth.MSG_EMBED_PARENT || "Not logged in",
        appIdConfigured: Boolean(appId),
      });
      return { ok: false, error: "not_logged_in" };
    }

    closeMemberstackModal();
    publishJwt(jwt);
    hideAuthGate();

    lastBootstrappedJwt = jwt;
    var meResult = await fetchMe(jwt);
    if (meResult && meResult.ok) {
      userNotFoundWarned = false;
      updateShellUserFromMe(meResult.data);
    } else {
      warnUserNotFoundOnce(meResult);
    }

    var result = {
      ok: true,
      jwt: jwt,
      me: meResult,
      authorized: !!(meResult && meResult.ok),
    };
    cachedBootstrapResult = result;
    dispatch("dealality-shell-auth-ready", result);
    return result;
  }

  function whenReady() {
    if (bootstrapInFlight) return bootstrapInFlight;
    if (!bootPromise) {
      bootstrapInFlight = bootstrap()
        .then(function (result) {
          bootPromise = Promise.resolve(result);
          return result;
        })
        .finally(function () {
          bootstrapInFlight = null;
        });
      return bootstrapInFlight;
    }
    return bootPromise;
  }

  function resetAndBootstrap() {
    clearBootstrapCache();
    return whenReady();
  }

  function clearMsTokenFromBrowserUrl() {
    try {
      var u = new URL(global.location.href);
      u.searchParams.delete("msToken");
      u.searchParams.delete("memberstackToken");
      var hash = u.hash || "";
      if (hash.indexOf("?") >= 0) {
        var base = hash.slice(0, hash.indexOf("?"));
        var hp = new URLSearchParams(hash.slice(hash.indexOf("?") + 1));
        hp.delete("msToken");
        hp.delete("memberstackToken");
        var qs = hp.toString();
        u.hash = base + (qs ? "?" + qs : "");
      }
      global.history.replaceState({}, "", u.pathname + u.search + u.hash);
    } catch (_) {}
  }

  async function logout() {
    closeMemberstackModal();
    var auth = getAuthModule();
    var clients = auth && auth.getMemberstackClients ? auth.getMemberstackClients() : [];
    if (!clients.length) {
      var single = memberstackClientReady();
      if (single) clients = [single];
    }
    for (var i = 0; i < clients.length; i++) {
      var ms = clients[i];
      if (!ms || typeof ms.logout !== "function") continue;
      try {
        var out = ms.logout();
        if (out && typeof out.then === "function") await out;
      } catch (_) {}
    }

    if (auth && typeof auth.clearSession === "function") auth.clearSession();
    global.__dealalityMemberstackJwt = null;
    var embed = getEmbedParent();
    if (embed && typeof embed.clearJwt === "function") embed.clearJwt();

    clearBootstrapCache();
    clearMsTokenFromBrowserUrl();
    showAuthGate(shellAppId);
    dispatch("dealality-shell-auth-logout", { ok: true });
    return { ok: true };
  }

  global.DealalityAppShellAuth = {
    bootstrap: bootstrap,
    whenReady: whenReady,
    resetAndBootstrap: resetAndBootstrap,
    publishJwt: publishJwt,
    showAuthGate: showAuthGate,
    hideAuthGate: hideAuthGate,
    openLoginModal: openLoginModal,
    logout: logout,
  };

  if (global.document && global.document.readyState === "loading") {
    global.document.addEventListener("DOMContentLoaded", function () {
      whenReady();
    });
  } else {
    whenReady();
  }
})(typeof window !== "undefined" ? window : globalThis);
