/**
 * GDI PDI-3 — Portfolio Demand Intelligence tab
 *
 * Reuses PDI-2 read-only API + existing #gdiDrawer / Brief v2 (no second drawer).
 * Live steward registry required for customer data. Explicit UNAVAILABLE / labeled
 * DEMO states — never silent fixture-as-production.
 */
(function (global) {
  "use strict";

  var API_MARKETS = "/api/group-demand-intelligence/portfolio/markets";
  var API_HOTELS = "/api/group-demand-intelligence/portfolio/hotels";
  var API_OPPS = "/api/group-demand-intelligence/portfolio/market-opportunities";

  var FIT_LABELS = {
    POTENTIAL_PRIMARY_HOST: "Potential primary host",
    OVERFLOW_ACCOMMODATION: "Overflow / secondary",
    INSUFFICIENT_EVIDENCE: "Insufficient evidence",
    NOT_SUITABLE: "Not suitable",
    COMPETITOR_PLACED: "Competitor placed",
  };

  function esc(s) {
    return String(s == null ? "" : s)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;");
  }

  function memberFetch() {
    if (
      global.DealalityMemberstackAuth &&
      typeof global.DealalityMemberstackAuth.fetchMyDealsApi === "function"
    ) {
      return global.DealalityMemberstackAuth.fetchMyDealsApi.bind(
        global.DealalityMemberstackAuth,
      );
    }
    return null;
  }

  function wantsExplicitDemo() {
    try {
      var p = new URLSearchParams(global.location.search || "");
      var v = String(p.get("pdiDemo") || "").trim();
      return v === "1" || v === "true" || v === "demo";
    } catch (e) {
      return false;
    }
  }

  function unavailableHtml(reason, opts) {
    opts = opts || {};
    var demoNote = opts.allowDemoCue
      ? '<p class="gdi-pdi-demo-cue">Founder / internal preview only: append <code>?pdiDemo=1</code> for a <strong>labeled DEMO</strong> matrix. Demo data is never a production customer surface.</p>'
      : "";
    return (
      '<div class="gdi-pdi-page" data-gdi-pdi-state="UNAVAILABLE" role="status">' +
      "<header class=\"gdi-pdi-header\">" +
      "<h1>Portfolio Demand Intelligence</h1>" +
      "<p class=\"gdi-pdi-sub\">Market-wide demand viewed across your participating hotels — same Commercial Intelligence Brief as the hotel page.</p>" +
      "</header>" +
      '<div class="gdi-pdi-banner gdi-pdi-banner--warn" role="alert">' +
      "<strong>Portfolio view unavailable.</strong> " +
      esc(reason || "Live operator membership registry is not provisioned for this session.") +
      "</div>" +
      demoNote +
      "</div>"
    );
  }

  function demoBannerHtml() {
    return (
      '<div class="gdi-pdi-banner gdi-pdi-banner--demo" role="status" data-gdi-pdi-demo="1">' +
      "<strong>DEMO — not a production customer.</strong> " +
      "Fixture steward identities and Guadalajara sample matrix for UI QA only. " +
      "Do not treat as live operator membership or published inventory." +
      "</div>"
    );
  }

  /**
   * Pure render helpers (exported for Node tests).
   */
  function renderHotelListHtml(hotels) {
    hotels = hotels || [];
    if (!hotels.length) {
      return '<p class="gdi-pdi-empty">No participating hotels in this market.</p>';
    }
    return (
      '<ul class="gdi-pdi-hotel-list" data-gdi-pdi-hotels="1">' +
      hotels
        .map(function (h) {
          return (
            "<li data-hotel-id=\"" +
            esc(h.hotelId) +
            "\">" +
            esc(h.hotelDisplayName || h.hotelId) +
            "</li>"
          );
        })
        .join("") +
      "</ul>"
    );
  }

  function fitLabel(fitClass) {
    return FIT_LABELS[fitClass] || fitClass || "—";
  }

  function renderMatrixHtml(payload, hotels) {
    var results = (payload && payload.results) || [];
    hotels = hotels || [];
    if (!results.length) {
      return '<p class="gdi-pdi-empty">No market opportunities available for this selection.</p>';
    }
    var hotelCols = hotels.length
      ? hotels
      : (function () {
          var map = {};
          results.forEach(function (r) {
            (r.hotelAssessments || []).forEach(function (a) {
              if (a && a.hotelId && !map[a.hotelId]) {
                map[a.hotelId] = {
                  hotelId: a.hotelId,
                  hotelDisplayName: a.hotelDisplayName || a.hotelId,
                };
              }
            });
          });
          return Object.keys(map).map(function (k) {
            return map[k];
          });
        })();

    var head =
      "<thead><tr><th scope=\"col\">Opportunity</th>" +
      hotelCols
        .map(function (h) {
          return "<th scope=\"col\">" + esc(h.hotelDisplayName || h.hotelId) + "</th>";
        })
        .join("") +
      "</tr></thead>";

    var body = results
      .map(function (r) {
        var packet = r.sharedMarketPacket || {};
        var title = packet.title || packet.eventName || r.marketOpportunityId || "Opportunity";
        var summary = packet.summaryWhat || "";
        var byHotel = {};
        (r.hotelAssessments || []).forEach(function (a) {
          byHotel[a.hotelId] = a;
        });
        var cells = hotelCols
          .map(function (h) {
            var a = byHotel[h.hotelId];
            if (!a) {
              return '<td class="gdi-pdi-cell gdi-pdi-cell--empty">—</td>';
            }
            var withheld =
              a.drawer &&
              (a.drawer.withheld === "UNPUBLISHED_HOTEL_DRAWER" || !a.drawer.detailPath);
            var pursuit = a.recommendedPursuit || "";
            var openAttrs = "";
            if (!withheld && a.drawer && a.drawer.hotelOpportunityId) {
              openAttrs =
                ' data-gdi-pdi-open="1"' +
                ' data-hotel-id="' +
                esc(a.drawer.hotelId || a.hotelId) +
                '"' +
                ' data-opportunity-id="' +
                esc(a.drawer.hotelOpportunityId) +
                '"' +
                (a.drawer.detailPath
                  ? ' data-detail-path="' + esc(a.drawer.detailPath) + '"'
                  : "");
            }
            return (
              '<td class="gdi-pdi-cell" data-fit="' +
              esc(a.fitClass || "") +
              '"' +
              openAttrs +
              ">" +
              '<div class="gdi-pdi-fit">' +
              esc(fitLabel(a.fitClass)) +
              (a.confidence ? ' · ' + esc(a.confidence) : "") +
              "</div>" +
              (pursuit
                ? '<div class="gdi-pdi-pursuit">' + esc(pursuit) + "</div>"
                : "") +
              (withheld
                ? '<div class="gdi-pdi-withheld">Drawer withheld — not customer-published for this hotel.</div>'
                : openAttrs
                  ? '<button type="button" class="gdi-btn gdi-btn-ghost gdi-pdi-open-btn">Open brief</button>'
                  : "") +
              "</td>"
            );
          })
          .join("");
        return (
          "<tr data-market-opportunity-id=\"" +
          esc(r.marketOpportunityId || "") +
          "\">" +
          "<th scope=\"row\" class=\"gdi-pdi-opp\">" +
          "<div class=\"gdi-pdi-opp-title\">" +
          esc(title) +
          "</div>" +
          (summary
            ? '<div class="gdi-pdi-opp-summary">' + esc(summary) + "</div>"
            : "") +
          '<div class="gdi-pdi-level2-note">Supporting Research (Level 2) opens in the existing opportunity drawer with the Commercial Intelligence Brief.</div>' +
          "</th>" +
          cells +
          "</tr>"
        );
      })
      .join("");

    return (
      '<div class="gdi-pdi-matrix-wrap" data-gdi-pdi-matrix="1">' +
      '<table class="gdi-pdi-matrix">' +
      head +
      "<tbody>" +
      body +
      "</tbody></table></div>"
    );
  }

  function shellHtml(state) {
    state = state || {};
    var markets = state.markets || [];
    var hotels = state.hotels || [];
    var marketId = state.marketId || "";
    var demo = !!state.demo;
    var err = state.error || null;
    var payload = state.opportunitiesPayload || null;

    if (err && err.code === "UNAVAILABLE") {
      return unavailableHtml(err.message, { allowDemoCue: true });
    }

    var marketOpts = markets
      .map(function (m) {
        return (
          '<option value="' +
          esc(m.marketId) +
          '"' +
          (m.marketId === marketId ? " selected" : "") +
          ">" +
          esc(m.marketLabel || m.marketId) +
          (m.hotelCount != null ? " (" + m.hotelCount + ")" : "") +
          "</option>"
        );
      })
      .join("");

    return (
      '<div class="gdi-pdi-page" data-gdi-pdi-state="' +
      (demo ? "DEMO" : "LIVE") +
      '">' +
      "<header class=\"gdi-pdi-header\">" +
      "<h1>Portfolio Demand Intelligence</h1>" +
      "<p class=\"gdi-pdi-sub\">Compare market demand across participating hotels. Opens the same Commercial Intelligence Brief and Supporting Research drawer — no separate portfolio content engine.</p>" +
      "</header>" +
      (demo ? demoBannerHtml() : "") +
      (err
        ? '<div class="gdi-pdi-banner gdi-pdi-banner--warn" role="alert">' +
          esc(err.message || "Unable to load portfolio data.") +
          "</div>"
        : "") +
      '<div class="gdi-pdi-controls">' +
      '<label class="gdi-pdi-label">Market' +
      '<select id="gdiPdiMarket" class="gdi-pdi-select" data-gdi-pdi-market="1">' +
      (marketOpts || '<option value="">No markets</option>') +
      "</select></label>" +
      "</div>" +
      '<section class="gdi-pdi-section" aria-label="Participating hotels">' +
      "<h2>Participating hotels</h2>" +
      renderHotelListHtml(hotels) +
      "</section>" +
      '<section class="gdi-pdi-section" aria-label="Opportunity fit matrix">' +
      "<h2>Market opportunity × hotel fit</h2>" +
      (payload
        ? renderMatrixHtml(payload, hotels)
        : '<p class="gdi-pdi-empty">Select a market to load opportunities.</p>') +
      "</section>" +
      "</div>"
    );
  }

  /** Minimal Guadalajara DEMO payload for labeled UI QA (never silent production). */
  function demoPayload() {
    return {
      markets: [
        {
          marketId: "market_guadalajara_mx_v1",
          marketLabel: "Guadalajara, Mexico (DEMO)",
          country: "Mexico",
          hotelCount: 2,
        },
      ],
      hotels: [
        {
          hotelId: "fixture_gdi_pdi_gdl_full_service_v1",
          hotelDisplayName: "DEMO Full-Service Guadalajara",
          marketId: "market_guadalajara_mx_v1",
        },
        {
          hotelId: "fixture_gdi_pdi_gdl_select_service_unknown_meetings",
          hotelDisplayName: "DEMO Select-Service (meetings unknown)",
          marketId: "market_guadalajara_mx_v1",
        },
      ],
      opportunitiesPayload: {
        marketId: "market_guadalajara_mx_v1",
        results: [
          {
            marketOpportunityId: "demo_mog_expo_v1",
            sharedMarketPacket: {
              title: "DEMO — Regional expo (sample)",
              summaryWhat:
                "Labeled demo opportunity for Portfolio UI QA. Not live research.",
              layer: "SHARED_PUBLIC_MARKET_INTELLIGENCE",
            },
            hotelAssessments: [
              {
                hotelId: "fixture_gdi_pdi_gdl_full_service_v1",
                fitClass: "POTENTIAL_PRIMARY_HOST",
                confidence: "HIGH",
                recommendedPursuit: "DEMO: qualify as potential primary host.",
                drawer: {
                  hotelId: "fixture_gdi_pdi_gdl_full_service_v1",
                  hotelOpportunityId: "demo_opp_full_v1",
                  detailPath: null,
                  withheld: "UNPUBLISHED_HOTEL_DRAWER",
                },
              },
              {
                hotelId: "fixture_gdi_pdi_gdl_select_service_unknown_meetings",
                fitClass: "INSUFFICIENT_EVIDENCE",
                confidence: "UNKNOWN",
                recommendedPursuit: "DEMO: meetings capability not evidenced.",
                missingAttributes: ["meeting_capability_unknown"],
                drawer: {
                  hotelId: "fixture_gdi_pdi_gdl_select_service_unknown_meetings",
                  hotelOpportunityId: "demo_opp_select_v1",
                  detailPath: null,
                  withheld: "UNPUBLISHED_HOTEL_DRAWER",
                },
              },
            ],
          },
        ],
      },
    };
  }

  function createTabController(opts) {
    opts = opts || {};
    var root = opts.root;
    if (!root) throw new Error("pdi_tab_root_required");
    var onOpenBrief =
      typeof opts.onOpenBrief === "function" ? opts.onOpenBrief : null;
    var state = {
      markets: [],
      hotels: [],
      marketId: "",
      opportunitiesPayload: null,
      demo: false,
      error: null,
      loading: false,
    };

    function paint() {
      root.innerHTML = shellHtml(state);
      bind();
    }

    function bind() {
      var sel = root.querySelector("#gdiPdiMarket");
      if (sel) {
        sel.addEventListener("change", function () {
          state.marketId = sel.value || "";
          loadMarket(state.marketId).catch(function (e) {
            state.error = {
              code: "LOAD_FAILED",
              message: (e && e.message) || "Failed to load market portfolio.",
            };
            paint();
          });
        });
      }
      root.querySelectorAll("[data-gdi-pdi-open] .gdi-pdi-open-btn, td[data-gdi-pdi-open]").forEach(
        function (el) {
          var cell = el.closest ? el.closest("[data-gdi-pdi-open]") : el;
          if (!cell || cell._pdiBound) return;
          cell._pdiBound = true;
          var btn = cell.querySelector(".gdi-pdi-open-btn") || cell;
          btn.addEventListener("click", function (ev) {
            ev.preventDefault();
            if (!onOpenBrief) return;
            onOpenBrief({
              hotelId: cell.getAttribute("data-hotel-id"),
              opportunityId: cell.getAttribute("data-opportunity-id"),
              detailPath: cell.getAttribute("data-detail-path"),
            });
          });
        },
      );
    }

    async function apiGet(url) {
      var fetchFn = memberFetch();
      if (!fetchFn) {
        var err = new Error("Authentication required for portfolio API.");
        err.code = "UNAUTHENTICATED";
        throw err;
      }
      var res = await fetchFn(url, { method: "GET" });
      var body = null;
      try {
        body = await res.json();
      } catch (e) {
        body = null;
      }
      return { res: res, body: body };
    }

    function classifyUnavailable(body, res) {
      var code =
        (body && body.error && body.error.code) ||
        (body && body.code) ||
        "";
      var status = (res && res.status) || (body && body.status) || 0;
      if (
        status === 503 ||
        /REGISTRY|INVENTORY|UNAVAILABLE|NOT_PROVISIONED/i.test(String(code)) ||
        /registry|not provisioned|unavailable/i.test(
          String((body && body.error && body.error.message) || body && body.message || ""),
        )
      ) {
        return true;
      }
      return false;
    }

    async function loadMarkets() {
      state.loading = true;
      state.error = null;
      try {
        var out = await apiGet(API_MARKETS);
        if (!out.res.ok || !(out.body && out.body.ok)) {
          if (classifyUnavailable(out.body, out.res)) {
            if (wantsExplicitDemo()) {
              applyDemo();
              return;
            }
            state.error = {
              code: "UNAVAILABLE",
              message:
                "Live operator membership / portfolio inventory is not available for this account. Portfolio Demand Intelligence stays off until the steward registry is provisioned.",
            };
            paint();
            return;
          }
          state.error = {
            code: "LOAD_FAILED",
            message:
              (out.body && out.body.error && out.body.error.message) ||
              "Could not load portfolio markets.",
          };
          paint();
          return;
        }
        state.demo = false;
        state.markets = out.body.markets || [];
        state.marketId =
          state.marketId ||
          (state.markets[0] && state.markets[0].marketId) ||
          "";
        paint();
        if (state.marketId) await loadMarket(state.marketId);
      } catch (e) {
        if (wantsExplicitDemo()) {
          applyDemo();
          return;
        }
        state.error = {
          code: e && e.code === "UNAUTHENTICATED" ? "UNAVAILABLE" : "LOAD_FAILED",
          message:
            (e && e.message) ||
            "Portfolio Demand Intelligence could not authenticate to the read-only API.",
        };
        paint();
      } finally {
        state.loading = false;
      }
    }

    async function loadMarket(marketId) {
      if (!marketId) return;
      state.marketId = marketId;
      if (state.demo) {
        paint();
        return;
      }
      var hotelsOut = await apiGet(API_HOTELS + "?marketId=" + encodeURIComponent(marketId));
      if (!hotelsOut.res.ok || !(hotelsOut.body && hotelsOut.body.ok)) {
        if (classifyUnavailable(hotelsOut.body, hotelsOut.res)) {
          state.error = {
            code: "UNAVAILABLE",
            message: "Portfolio inventory unavailable for this market.",
          };
          paint();
          return;
        }
        state.error = {
          code: "LOAD_FAILED",
          message: "Could not load participating hotels.",
        };
        paint();
        return;
      }
      state.hotels = hotelsOut.body.hotels || [];
      var oppOut = await apiGet(API_OPPS + "?marketId=" + encodeURIComponent(marketId));
      if (!oppOut.res.ok || !(oppOut.body && oppOut.body.ok)) {
        state.error = {
          code: "LOAD_FAILED",
          message: "Could not load market opportunities.",
        };
        state.opportunitiesPayload = null;
        paint();
        return;
      }
      state.opportunitiesPayload = oppOut.body;
      state.error = null;
      paint();
    }

    function applyDemo() {
      var d = demoPayload();
      state.demo = true;
      state.markets = d.markets;
      state.hotels = d.hotels;
      state.marketId = d.markets[0].marketId;
      state.opportunitiesPayload = d.opportunitiesPayload;
      state.error = null;
      paint();
    }

    function refresh() {
      if (wantsExplicitDemo()) {
        applyDemo();
        return Promise.resolve();
      }
      return loadMarkets();
    }

    function destroy() {
      root.innerHTML = "";
    }

    return {
      refresh: refresh,
      destroy: destroy,
      getState: function () {
        return state;
      },
      paint: paint,
    };
  }

  var api = {
    mount: function (container, options) {
      options = options || {};
      if (!container) throw new Error("pdi_tab_container_required");
      if (api._active && api._controller) {
        api._controller.destroy();
      }
      var controller = createTabController({
        root: container,
        onOpenBrief: options.onOpenBrief,
      });
      api._active = true;
      api._controller = controller;
      container.hidden = false;
      container.removeAttribute("aria-hidden");
      controller.refresh();
      return controller;
    },
    unmount: function () {
      if (api._controller) api._controller.destroy();
      api._controller = null;
      api._active = false;
    },
    _active: false,
    _controller: null,
    // test / render exports
    shellHtml: shellHtml,
    renderMatrixHtml: renderMatrixHtml,
    renderHotelListHtml: renderHotelListHtml,
    unavailableHtml: unavailableHtml,
    demoPayload: demoPayload,
    FIT_LABELS: FIT_LABELS,
  };

  global.DealalityGdiPortfolioTab = api;

  if (typeof module !== "undefined" && module.exports) {
    module.exports = api;
  }
})(typeof window !== "undefined" ? window : globalThis);
