/**
 * Canonical GDI contact enrichment (Surfe) client — shared by main drawer +
 * founder comparison preview. One implementation; explicit click required.
 * Mock via ?gdiContactRevealMock=1 or window.__GDI_CONTACT_REVEAL_MOCK__=true
 * (no paid provider). Live Surfe stays session-only.
 */
(function (root) {
  "use strict";

  var inFlight = Object.create(null);

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

      function setRevealStatus(msg, isError) {
        if (!statusEl) return;
        statusEl.hidden = false;
        statusEl.textContent = msg || "";
        statusEl.classList.toggle(
          "gdi-contact-reveal-status--error",
          !!isError,
        );
      }

      if (!opportunityId || !contactName) {
        setRevealStatus(
          "Select a named contact before retrieving details.",
          true,
        );
        return;
      }
      var hotelId = getHotelId();
      if (!hotelId) {
        setRevealStatus("Hotel context missing.", true);
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
      setRevealStatus("Retrieving contact details…", false);
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

      var revealFetch = useMock
        ? plainFetch(revealUrl, revealOpts)
        : fetchFn(revealUrl, revealOpts);

      revealFetch
        .then(function (r) {
          return r.json().then(function (data) {
            return { okHttp: r.ok, status: r.status, data: data || {} };
          });
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
              false,
            );
            return;
          }
          if (data.miss || data.error === "NO_RESULT") {
            setRevealStatus(
              data.message ||
                "No matching email/phone found for this person.",
              false,
            );
            return;
          }
          if (res.status === 401 || res.status === 403) {
            setRevealStatus(
              data.message ||
                "Contact lookup requires a signed-in Dealality user.",
              true,
            );
            return;
          }
          if (res.status === 402) {
            setRevealStatus(
              data.message || "Insufficient Surfe credits for this lookup.",
              true,
            );
            return;
          }
          setRevealStatus(
            data.message ||
              data.error ||
              "Contact lookup did not return a usable result.",
            true,
          );
        })
        .catch(function (err) {
          if (!document.body.contains(surfeBtn)) return;
          setRevealStatus(
            (err && err.message) || "Contact lookup failed.",
            true,
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
  };

  if (root.DealalityGdiUi) {
    root.DealalityGdiUi.bindContactRevealClicks = bindContactRevealClicks;
  }
})(typeof window !== "undefined" ? window : globalThis);
