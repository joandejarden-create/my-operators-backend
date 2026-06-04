/**
 * Local /app shell: load Memberstack, obtain JWT, publish to embedded pages (My Deals, etc.).
 * Load after dealality-memberstack-auth.js and dealality-webflow-embed-parent.js.
 */
(function (global) {
  "use strict";

  /** Same script as Webflow / Memberstack DOM package quick start (creates window.$memberstackDom). */
  var MS_SCRIPT_SRC = "https://static.memberstack.com/scripts/v1/memberstack.js";
  var bootPromise = null;
  var shellAppId = "";

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

    bootPromise = null;
    var result = await bootstrap();
    return Boolean(result && result.ok);
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
      if (stopped) return;
      completeShellSessionAfterLogin().then(function (ok) {
        if (ok) {
          stop();
          if (typeof onSuccess === "function") onSuccess();
        }
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

      var existing = global.document && global.document.querySelector("script[data-memberstack-app]");
      if (existing) {
        waitForClient();
        return;
      }

      var script = global.document.createElement("script");
      script.src = MS_SCRIPT_SRC;
      script.async = true;
      script.type = "text/javascript";
      script.setAttribute("data-memberstack-app", appId);
      script.onload = function () {
        waitForClient();
      };
      script.onerror = function () {
        reject(new Error("Memberstack script failed to load from " + MS_SCRIPT_SRC));
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
      '<p class="app-shell-auth-gate-hint">Already logged in elsewhere? Append <code>?msToken=</code> with your <code>eyJ…</code> session token to this URL, or log in here once for localhost.</p>' +
      "</div>";
    container.parentNode.insertBefore(gate, container);
    return gate;
  }

  async function openLoginModal(appId) {
    var loginBtn = global.document.getElementById("dealalityAppShellLoginBtn");
    var stopWatch = null;
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
      stopWatch = watchForMemberstackLoginWhileModalOpen(ms, function () {
        setGateStatus("", false);
      });
      try {
        await openMemberstackLoginModal(ms);
      } catch (_) {
        /* User may close modal manually; polling/onAuthChange still handles success. */
      }
      closeMemberstackModal();
      var ok = await completeShellSessionAfterLogin();
      if (!ok) {
        setGateStatus("Sign-in finished — click Reload after login if the modal is still visible.", false);
      }
    } catch (err) {
      setGateStatus((err && err.message) || "Could not load Memberstack.", true);
    } finally {
      if (typeof stopWatch === "function") stopWatch();
      closeMemberstackModal();
      if (loginBtn) {
        loginBtn.disabled = false;
        loginBtn.textContent = "Log in";
      }
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

    var appId = (config.appId || "").trim();
    shellAppId = appId;
    var jwt = await auth.getMemberstackJwtWhenReady(800);

    if (!jwt && appId) {
      try {
        await loadMemberstackDomScript(appId);
        jwt = await auth.getMemberstackJwtWhenReady(25000);
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

    var meResult = await fetchMe(jwt);
    if (meResult && meResult.ok) {
      updateShellUserFromMe(meResult.data);
    }

    var result = { ok: true, jwt: jwt, me: meResult };
    dispatch("dealality-shell-auth-ready", result);
    return result;
  }

  function whenReady() {
    if (!bootPromise) {
      bootPromise = bootstrap();
    }
    return bootPromise;
  }

  function resetAndBootstrap() {
    bootPromise = null;
    return bootstrap();
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

    bootPromise = null;
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
