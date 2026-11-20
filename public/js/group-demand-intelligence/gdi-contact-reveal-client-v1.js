/**
 * Canonical GDI contact enrichment (Surfe) client — shared by main drawer +
 * founder comparison preview. One implementation; explicit click required.
 * Mock via ?gdiContactRevealMock=1 or window.__GDI_CONTACT_REVEAL_MOCK__=true
 * (no paid provider). Live Surfe stays session-only.
 *
 * Failure states are distinct — never reuse "Please log in again" for
 * provider / credits / miss failures.
 */
(function (root) {
  "use strict";

  var inFlight = Object.create(null);

  var MSG_SIGN_IN =
    "Sign in to Dealality to look up email and phone. Open Group & Demand Intelligence from the Dealality app after signing in.";
  var MSG_PERMISSIONS =
    "Your account does not have permission to look up contact details for this hotel.";
  var MSG_PROVIDER =
    "Contact lookup provider is unavailable. Email and phone enrichment is not configured for this environment.";
  var MSG_CREDITS =
    "Insufficient credits for this contact lookup. Try again later or contact Dealality support.";
  var MSG_MISS = "No matching email/phone found for this person.";
  var MSG_GENERIC = "Contact lookup did not return a usable result.";

  var STATUS_CLASSES = [
    "gdi-contact-reveal-status--loading",
    "gdi-contact-reveal-status--found",
    "gdi-contact-reveal-status--miss",
    "gdi-contact-reveal-status--auth",
    "gdi-contact-reveal-status--credits",
    "gdi-contact-reveal-status--provider",
    "gdi-contact-reveal-status--error",
  ];

  function text(v) {
    return String(v == null ? "" : v).trim();
  }

  function wantsMock() {
    try {
      if (root.__GDI_CONTACT_REVEAL_MOCK__ === true) return true;
      return /(?:^|[?&])gdiContactRevealMock=1(?:&|$)/.test(
        root.location && root.location.search ? root.location.search : "",
      );
    } catch (e) {
      return false;
    }
  }

  function escHtml(s) {
    return String(s == null ? "" : s)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;");
  }

  function clearStatusClasses(el) {
    if (!el || !el.classList) return;
    for (var i = 0; i < STATUS_CLASSES.length; i++) {
      el.classList.remove(STATUS_CLASSES[i]);
    }
  }

  function signInHref() {
    try {
      var origin = root.location && root.location.origin ? root.location.origin : "";
      return origin + "/app#/group-demand-intelligence";
    } catch (e) {
      return "/app#/group-demand-intelligence";
    }
  }

  /**
   * Apply session-only email/phone into the clicked contact card DOM only.
   * Does not mutate comparison payload objects or Airtable.
   */
  function applySessionDetailsToCard(rootEl, details) {
    if (!rootEl || !details) return;
    var slot =
      rootEl.closest("[data-contact-slot]") ||
      rootEl.closest(".gdi-contact-slot") ||
      rootEl;
    if (!slot) return;

    function ensureField(label, value, kind) {
      if (!value) return;
      var existing = slot.querySelector(
        '[data-gdi-session-contact-field="' + kind + '"]',
      );
      if (existing) {
        existing.querySelector(".gdi-contact-field__value") &&
          (existing.querySelector(".gdi-contact-field__value").textContent =
            value);
        return;
      }
      var p = document.createElement("p");
      p.className = "gdi-contact-field";
      p.setAttribute("data-gdi-session-contact-field", kind);
      p.innerHTML =
        '<span class="gdi-contact-field__label">' +
        escHtml(label) +
        '</span> <span class="gdi-contact-field__value">' +
        escHtml(value) +
        "</span>";
      var status = rootEl.querySelector("[data-gdi-contact-reveal-status]");
      if (status && status.parentNode === rootEl) {
        rootEl.insertBefore(p, status);
      } else {
        rootEl.appendChild(p);
      }
    }

    ensureField("Direct Business Email", details.email, "email");
    ensureField("Direct Business Phone", details.phone, "phone");

    // Hide Surfe CTA once requested channels are present on the card.
    var btn = rootEl.querySelector("[data-gdi-contact-cta]");
    if (btn && details.email && details.phone) {
      var ctaWrap = btn.closest(".gdi-contact-cta");
      if (ctaWrap) ctaWrap.hidden = true;
      var note = rootEl.querySelector(".gdi-contact-cta-note");
      if (note) note.hidden = true;
    }
  }

  function classifyRevealFailure(res) {
    var status = Number(res && res.status) || 0;
    var data = (res && res.data) || {};
    var code = text(data.error || data.code || "");
    var msg = text(data.message || "");

    if (
      status === 401 ||
      code === "SIGN_IN_REQUIRED" ||
      code === "AUTH_REQUIRED" ||
      code === "authentication_required" ||
      /please log in again/i.test(msg) ||
      /sign in to dealality/i.test(msg)
    ) {
      return {
        kind: "auth",
        message: msg && !/please log in again/i.test(msg) ? msg : MSG_SIGN_IN,
        signIn: true,
      };
    }
    if (
      status === 403 &&
      (code === "INSUFFICIENT_PERMISSIONS" ||
        code === "HOTEL_ACCESS_DENIED" ||
        code === "FEATURE_NOT_PERMITTED" ||
        code === "MEMBERSTACK_ID_INSUFFICIENT" ||
        code === "ACCESS_DENIED" ||
        /permission/i.test(msg) ||
        /does not have permission/i.test(msg))
    ) {
      return { kind: "auth", message: msg || MSG_PERMISSIONS, signIn: false };
    }
    if (
      status === 403 &&
      (code === "LIVE_PROVIDER_NOT_AUTHORIZED" ||
        code === "SHARE_VIEWER_BLOCKED")
    ) {
      // Share / unauthorized session — sign-in when message points there.
      return {
        kind: "auth",
        message: msg || MSG_SIGN_IN,
        signIn: code === "SHARE_VIEWER_BLOCKED",
      };
    }
    if (
      status === 402 ||
      code === "INSUFFICIENT_CREDITS"
    ) {
      return { kind: "credits", message: msg || MSG_CREDITS };
    }
    if (
      status === 503 ||
      code === "PROVIDER_UNAVAILABLE" ||
      code === "SURFE_CREDITS_UNAVAILABLE" ||
      code === "MISSING_SURFE_API_KEY" ||
      code === "PROVIDER_ADAPTER_NOT_WIRED" ||
      code === "CONTACT_ENRICHMENT_DISABLED"
    ) {
      return { kind: "provider", message: msg || MSG_PROVIDER };
    }
    if (data.miss || code === "NO_RESULT" || code === "NO_MATCHING_CONTACT") {
      return { kind: "miss", message: msg || MSG_MISS };
    }
    return { kind: "error", message: msg || code || MSG_GENERIC };
  }

  /**
   * @param {HTMLElement} container — click delegation root (drawer body or comparison panels)
   * @param {object} opts
   * @param {() => string} opts.getHotelId
   * @param {(on:boolean, message?:string) => void} [opts.setLoadingToast]
   * @param {(url:string, init:object) => Promise<Response>} [opts.fetchFn] — authenticated fetch
   * @param {(url:string, init:object) => Promise<Response>} [opts.plainFetch] — mock/unauth fetch
   * @param {string} [opts.surface] — e.g. comparison-before | comparison-after | drawer
   */
  function bindContactRevealClicks(container, opts) {
    opts = opts || {};
    if (!container || container.__gdiContactRevealBound) return;
    container.__gdiContactRevealBound = true;

    var getHotelId =
      typeof opts.getHotelId === "function"
        ? opts.getHotelId
        : function () {
            return text(opts.hotelId);
          };
    var setLoadingToast =
      typeof opts.setLoadingToast === "function" ? opts.setLoadingToast : null;
    var fetchFn =
      typeof opts.fetchFn === "function"
        ? opts.fetchFn
        : function (url, init) {
            return fetch(url, init);
          };
    var plainFetch =
      typeof opts.plainFetch === "function" ? opts.plainFetch : fetchFn;

    container.addEventListener("click", function (e) {
      var t = e.target;
      var surfeBtn =
        t && t.closest ? t.closest("[data-gdi-contact-cta]") : null;
      if (!surfeBtn || !container.contains(surfeBtn)) return;

      e.preventDefault();
      e.stopPropagation();

      var channel = text(
        surfeBtn.getAttribute("data-gdi-contact-channel") || "both",
      ).toLowerCase();
      var contactName = text(surfeBtn.getAttribute("data-contact-name") || "");
      var opportunityId = text(
        surfeBtn.getAttribute("data-opportunity-id") || "",
      );
      var panel =
        text(surfeBtn.getAttribute("data-gdi-cmp-panel")) ||
        text(
          surfeBtn.closest("[data-gdi-cmp-panel]") &&
            surfeBtn.closest("[data-gdi-cmp-panel]").getAttribute(
              "data-gdi-cmp-panel",
            ),
        ) ||
        text(opts.surface) ||
        "drawer";
      var rootEl =
        surfeBtn.closest("[data-gdi-contact-reveal-root]") ||
        surfeBtn.parentElement;
      var statusEl = rootEl
        ? rootEl.querySelector("[data-gdi-contact-reveal-status]")
        : null;

      function setRevealStatus(msg, kind) {
        if (!statusEl) return;
        clearStatusClasses(statusEl);
        statusEl.hidden = false;
        var k = kind || "error";
        statusEl.classList.add("gdi-contact-reveal-status--" + k);
        statusEl.setAttribute("data-gdi-reveal-state", k);
        if (k === "auth") {
          statusEl.innerHTML =
            '<span class="gdi-contact-reveal-status__text">' +
            escHtml(msg || MSG_SIGN_IN) +
            '</span> <a class="gdi-contact-reveal-signin" href="' +
            escHtml(signInHref()) +
            '">Sign in</a>';
        } else {
          statusEl.textContent = msg || "";
        }
      }

      if (!opportunityId || !contactName) {
        setRevealStatus(
          "Select a named contact before retrieving details.",
          "error",
        );
        return;
      }
      var hotelId = getHotelId();
      if (!hotelId) {
        setRevealStatus("Hotel context missing.", "error");
        return;
      }

      var flightKey = [
        hotelId,
        opportunityId,
        contactName,
        channel,
        panel,
      ].join("|");
      if (inFlight[flightKey]) return;
      inFlight[flightKey] = true;

      var prevLabel = surfeBtn.textContent;
      surfeBtn.disabled = true;
      surfeBtn.textContent = "Looking up…";
      setRevealStatus("Retrieving contact details…", "loading");
      if (setLoadingToast) {
        setLoadingToast(true, "Retrieving contact details…");
      }

      var useMock = wantsMock();
      var revealUrl =
        "/api/group-demand-intelligence/hotels/" +
        encodeURIComponent(hotelId) +
        "/opportunities/" +
        encodeURIComponent(opportunityId) +
        "/contacts/reveal";
      var revealOpts = {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Accept: "application/json",
        },
        body: JSON.stringify({
          channel: channel,
          contactName: contactName,
          authorizeLiveProvider: useMock ? false : true,
          useMock: useMock,
          mockScenario: "hit",
          comparisonPanel: panel,
        }),
      };

      // Always authenticated fetch — mock still requires hotel-authorized session.
      // plainFetch is unused for reveal (kept in opts for callers / future).
      void plainFetch;
      var revealFetch = fetchFn(revealUrl, revealOpts);

      revealFetch
        .then(function (r) {
          return r.json().then(
            function (data) {
              return { okHttp: r.ok, status: r.status, data: data || {} };
            },
            function () {
              return {
                okHttp: r.ok,
                status: r.status,
                data: { error: "INVALID_JSON", message: MSG_GENERIC },
              };
            },
          );
        })
        .then(function (res) {
          // Ignore if user switched cards / opportunities and this root is gone.
          if (!document.body.contains(surfeBtn)) return;
          var data = res.data || {};
          var details = data.contactDetails || null;
          if (res.okHttp && details && (details.email || details.phone)) {
            applySessionDetailsToCard(rootEl, details);
            var bits = [];
            if (details.email) bits.push("Email: " + details.email);
            if (details.phone) bits.push("Phone: " + details.phone);
            setRevealStatus(
              bits.join(" · ") +
                " (session only — not saved to Dealality records)",
              "found",
            );
            return;
          }
          if (data.miss || data.error === "NO_RESULT") {
            setRevealStatus(data.message || MSG_MISS, "miss");
            return;
          }
          var classified = classifyRevealFailure(res);
          setRevealStatus(classified.message, classified.kind);
        })
        .catch(function (err) {
          if (!document.body.contains(surfeBtn)) return;
          // Browser-side Memberstack helper throws AUTH_REQUIRED with LOGIN_MSG
          // before the POST fires — map that to sign-in, never raw login-again copy.
          if (
            err &&
            (err.code === "AUTH_REQUIRED" ||
              err.authRequired ||
              /please log in again/i.test(String(err.message || "")))
          ) {
            setRevealStatus(MSG_SIGN_IN, "auth");
            return;
          }
          setRevealStatus(
            (err && err.message) || "Contact lookup failed.",
            "error",
          );
        })
        .finally(function () {
          delete inFlight[flightKey];
          if (document.body.contains(surfeBtn)) {
            surfeBtn.disabled = false;
            surfeBtn.textContent = prevLabel;
          }
          if (setLoadingToast) setLoadingToast(false);
        });
    });
  }

  root.DealalityGdiContactReveal = {
    bindContactRevealClicks: bindContactRevealClicks,
    applySessionDetailsToCard: applySessionDetailsToCard,
    wantsMock: wantsMock,
    classifyRevealFailure: classifyRevealFailure,
    MSG_SIGN_IN: MSG_SIGN_IN,
    MSG_PROVIDER: MSG_PROVIDER,
    MSG_CREDITS: MSG_CREDITS,
    MSG_MISS: MSG_MISS,
  };

  if (root.DealalityGdiUi) {
    root.DealalityGdiUi.bindContactRevealClicks = bindContactRevealClicks;
  }
})(typeof window !== "undefined" ? window : globalThis);
