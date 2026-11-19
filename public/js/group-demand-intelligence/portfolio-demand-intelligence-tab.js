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

  /** Align with PDI-1 PORTFOLIO_HOTEL_FIT_CLASS — display labels only. */
  var FIT_LABELS = {
    POTENTIAL_PRIMARY_HOST: "Potential primary host",
    OVERFLOW_ACCOMMODATION: "Overflow accommodation",
    PREFERRED_LODGING_OPPORTUNITY: "Preferred lodging",
    MEETING_CATERING_OPPORTUNITY: "Meeting / catering",
    OTHER_PARTNERSHIP: "Other partnership",
    UNSUITABLE: "Unsuitable",
    INSUFFICIENT_EVIDENCE: "Insufficient evidence",
  };

  var FIT_CLASS_ORDER = [
    "POTENTIAL_PRIMARY_HOST",
    "OVERFLOW_ACCOMMODATION",
    "PREFERRED_LODGING_OPPORTUNITY",
    "MEETING_CATERING_OPPORTUNITY",
    "OTHER_PARTNERSHIP",
    "UNSUITABLE",
    "INSUFFICIENT_EVIDENCE",
  ];

  function esc(s) {
    return String(s == null ? "" : s)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;");
  }

  function memberAuthApi() {
    return global.DealalityMemberstackAuth || null;
  }

  function isLoginRequiredMessage(msg) {
    return /please log in again to continue|please log in on dealality first/i.test(
      String(msg || ""),
    );
  }

  /** Demo preview is local/test hosts only — query flag alone never unlocks production. */
  function isAuthorizedLocalDemoHost(loc) {
    try {
      var host = String(
        (loc && loc.hostname) ||
          (global.location && global.location.hostname) ||
          "",
      ).toLowerCase();
      return (
        host === "localhost" ||
        host === "127.0.0.1" ||
        host === "0.0.0.0" ||
        host === "[::1]"
      );
    } catch (e) {
      return false;
    }
  }

  function readQueryParam(search, key) {
    var raw = String(search || "");
    if (raw.charAt(0) === "?") raw = raw.slice(1);
    if (!raw) return "";
    try {
      if (typeof URLSearchParams === "function") {
        return String(new URLSearchParams(raw).get(key) || "").trim();
      }
    } catch (e) {
      /* fall through */
    }
    var parts = raw.split("&");
    for (var i = 0; i < parts.length; i++) {
      var kv = parts[i].split("=");
      if (decodeURIComponent(kv[0] || "") === key) {
        return decodeURIComponent((kv[1] || "").replace(/\+/g, " ")).trim();
      }
    }
    return "";
  }

  function wantsExplicitDemo(loc) {
    try {
      var search =
        (loc && loc.search) ||
        (global.location && global.location.search) ||
        "";
      var v = readQueryParam(search, "pdiDemo");
      if (!(v === "1" || v === "true" || v === "demo")) return false;
      return isAuthorizedLocalDemoHost(loc || global.location);
    } catch (e) {
      return false;
    }
  }

  function statusPageHtml(stateCode, title, reason, opts) {
    opts = opts || {};
    var localOk = isAuthorizedLocalDemoHost(opts.location || global.location);
    var demoNote =
      opts.allowDemoCue && localOk
        ? '<p class="gdi-pdi-demo-cue">Local/test only: on <code>localhost</code> append <code>?pdiDemo=1</code> for a <strong>labeled DEMO</strong> matrix. The query flag is ignored on non-local hosts.</p>'
        : "";
    return (
      '<div class="gdi-pdi-page" data-gdi-pdi-state="' +
      esc(stateCode || "UNAVAILABLE") +
      '" role="status">' +
      "<header class=\"gdi-pdi-header\">" +
      "<h1>Portfolio Demand Intelligence</h1>" +
      "<p class=\"gdi-pdi-sub\">Market-wide demand viewed across your participating hotels — same Commercial Intelligence Brief as the hotel page.</p>" +
      "</header>" +
      '<div class="gdi-pdi-banner gdi-pdi-banner--warn" role="alert">' +
      "<strong>" +
      esc(title || "Portfolio view unavailable.") +
      "</strong> " +
      esc(
        reason ||
          "Live operator membership registry is not provisioned for this session.",
      ) +
      "</div>" +
      demoNote +
      "</div>"
    );
  }

  function unavailableHtml(reason, opts) {
    return statusPageHtml(
      "UNAVAILABLE",
      "Portfolio view unavailable.",
      reason,
      opts,
    );
  }

  function authRequiredHtml(reason, opts) {
    return statusPageHtml(
      "AUTH_REQUIRED",
      "Sign-in required for portfolio reads.",
      reason ||
        "Portfolio Demand Intelligence needs a signed-in Dealality session. Open GDI from the authenticated app shell, then return to this tab.",
      Object.assign({}, opts, { allowDemoCue: false }),
    );
  }

  function notMappedHtml(reason, opts) {
    return statusPageHtml(
      "OPERATOR_NOT_MAPPED",
      "No portfolio membership for this account.",
      reason ||
        "You are signed in, but this Memberstack identity is not mapped to a portfolio operator. Contact Dealality to provision operator membership — demo membership is not substituted.",
      Object.assign({}, opts, { allowDemoCue: true }),
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
            var canOpen =
              a.drawer &&
              a.drawer.hotelOpportunityId &&
              a.drawer.detailPath &&
              a.drawer.withheld !== "UNPUBLISHED_HOTEL_DRAWER";
            var withheld =
              !canOpen &&
              a.drawer &&
              (a.drawer.withheld === "UNPUBLISHED_HOTEL_DRAWER" || !a.drawer.detailPath);
            var pursuit = a.recommendedPursuit || "";
            var openAttrs = "";
            if (canOpen) {
              openAttrs =
                ' data-gdi-pdi-open="1"' +
                ' data-hotel-id="' +
                esc(a.drawer.hotelId || a.hotelId) +
                '"' +
                ' data-opportunity-id="' +
                esc(a.drawer.hotelOpportunityId) +
                '"' +
                ' data-detail-path="' +
                esc(a.drawer.detailPath) +
                '"';
            }
            var actionHtml = "";
            if (canOpen) {
              actionHtml =
                '<button type="button" class="gdi-btn gdi-btn-ghost gdi-pdi-open-btn">Open brief</button>' +
                '<div class="gdi-pdi-level2-note">Opens the existing opportunity drawer (Commercial Intelligence Brief). Supporting Research appears only when already persisted on that opportunity.</div>';
            } else if (withheld) {
              actionHtml =
                '<div class="gdi-pdi-withheld" data-gdi-pdi-l2="UNAVAILABLE">Brief and Supporting Research unavailable — not customer-published for this hotel.</div>';
            } else {
              actionHtml =
                '<div class="gdi-pdi-withheld" data-gdi-pdi-l2="UNAVAILABLE">Supporting Research unavailable — no published drawer path for this hotel.</div>';
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
              actionHtml +
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

  function countActivePdiFilters(state) {
    var n = 0;
    if (state.hotelId) n += 1;
    if (state.fitClass) n += 1;
    var markets = state.markets || [];
    var defaultMarket =
      (markets[0] && markets[0].marketId) || state.defaultMarketId || "";
    if (state.marketId && defaultMarket && state.marketId !== defaultMarket) {
      n += 1;
    }
    return n;
  }

  function filterPanelHtml(state) {
    var markets = state.markets || [];
    var hotels = state.hotels || [];
    var marketId = state.marketId || "";
    var hotelId = state.hotelId || "";
    var fitClass = state.fitClass || "";
    var activeCount = countActivePdiFilters(state);

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

    var hotelOpts =
      '<option value="">All participating hotels (' +
      hotels.length +
      ")</option>" +
      hotels
        .map(function (h) {
          return (
            '<option value="' +
            esc(h.hotelId) +
            '"' +
            (h.hotelId === hotelId ? " selected" : "") +
            ">" +
            esc(h.hotelDisplayName || h.hotelId) +
            "</option>"
          );
        })
        .join("");

    var fitOpts =
      '<option value="">All fit classes</option>' +
      FIT_CLASS_ORDER.map(function (k) {
        return (
          '<option value="' +
          esc(k) +
          '"' +
          (fitClass === k ? " selected" : "") +
          ">" +
          esc(FIT_LABELS[k] || k) +
          "</option>"
        );
      }).join("");

    var marketSelect =
      markets.length > 0
        ? '<select class="filter-select" id="gdiPdiMarket" data-gdi-pdi-market="1" aria-label="Filter by portfolio market">' +
          marketOpts +
          "</select>"
        : '<select class="filter-select" id="gdiPdiMarket" data-gdi-pdi-market="1" aria-label="Filter by portfolio market" disabled>' +
          '<option value="">Markets not provisioned</option></select>';

    var hotelSelect =
      hotels.length > 0
        ? '<select class="filter-select" id="gdiPdiHotel" data-gdi-pdi-hotel="1" aria-label="Filter by participating hotel">' +
          hotelOpts +
          "</select>"
        : '<select class="filter-select" id="gdiPdiHotel" data-gdi-pdi-hotel="1" aria-label="Filter by participating hotel" disabled>' +
          '<option value="">Select a market first</option></select>';

    var resetBadge =
      '<span class="filter-count-badge" id="gdiPdiFilterCountBadge"' +
      (activeCount > 0
        ? ' aria-label="' + esc(String(activeCount)) + ' active filters">'
        : ' style="display: none;" aria-hidden="true">') +
      esc(String(activeCount)) +
      "</span>";

    return (
      '<section class="filters-section aiv-filters-section gdi-property-bar" aria-label="Portfolio filters" data-gdi-pdi-filters="1">' +
      '<div class="gdi-property-bar__primary">' +
      '<div class="filter-group gdi-pdi-market-filter">' +
      '<label class="filter-label" for="gdiPdiMarket">Market</label>' +
      marketSelect +
      "</div>" +
      '<div class="filter-group gdi-pdi-hotel-filter">' +
      '<label class="filter-label" for="gdiPdiHotel">Participating hotel</label>' +
      hotelSelect +
      "</div>" +
      '<div class="filter-group gdi-pdi-fit-filter">' +
      '<label class="filter-label" for="gdiPdiFit">Hotel fit</label>' +
      '<select class="filter-select" id="gdiPdiFit" data-gdi-pdi-fit="1" aria-label="Filter by hotel fit class"' +
      (state.opportunitiesPayload ? "" : " disabled") +
      ">" +
      fitOpts +
      "</select></div>" +
      '<div class="filter-group aiv-filter-actions gdi-property-bar__actions">' +
      '<button type="button" class="btn-clear" id="gdiPdiResetViewBtn" aria-label="Reset View">' +
      "Reset View" +
      resetBadge +
      "</button></div>" +
      "</div></section>"
    );
  }

  function shellHtml(state) {
    state = state || {};
    var markets = state.markets || [];
    var hotels = state.hotels || [];
    var hotelId = state.hotelId || "";
    var fitClass = state.fitClass || "";
    var demo = !!state.demo;
    var err = state.error || null;
    var payload = state.opportunitiesPayload || null;

    if (err && err.code === "AUTH_REQUIRED") {
      return authRequiredHtml(err.message, { allowDemoCue: false });
    }
    if (err && err.code === "OPERATOR_NOT_MAPPED") {
      return notMappedHtml(err.message, { allowDemoCue: true });
    }
    if (err && err.code === "UNAVAILABLE") {
      return unavailableHtml(err.message, { allowDemoCue: true });
    }

    var visibleHotels = hotelId
      ? hotels.filter(function (h) {
          return h.hotelId === hotelId;
        })
      : hotels;
    var visiblePayload = payload;
    if (payload && fitClass) {
      visiblePayload = {
        marketId: payload.marketId,
        fitClasses: payload.fitClasses,
        results: (payload.results || []).map(function (r) {
          return {
            marketOpportunityId: r.marketOpportunityId,
            sharedMarketPacket: r.sharedMarketPacket,
            hotelAssessments: (r.hotelAssessments || []).filter(function (a) {
              return a && a.fitClass === fitClass;
            }),
          };
        }),
      };
    }

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
      filterPanelHtml(state) +
      '<section class="gdi-pdi-section" aria-label="Participating hotels">' +
      "<h2>Participating hotels" +
      (hotels.length ? " (" + hotels.length + ")" : "") +
      "</h2>" +
      renderHotelListHtml(visibleHotels.length ? visibleHotels : hotels) +
      "</section>" +
      '<section class="gdi-pdi-section" aria-label="Opportunity fit matrix">' +
      "<h2>Market opportunity × hotel fit</h2>" +
      (visiblePayload
        ? renderMatrixHtml(
            visiblePayload,
            visibleHotels.length ? visibleHotels : hotels,
          )
        : markets.length
          ? '<p class="gdi-pdi-empty">Select a market to load opportunities.</p>'
          : '<p class="gdi-pdi-empty">No authorized markets are available for this account yet.</p>') +
      "</section>" +
      "</div>"
    );
  }

  /** Minimal Guadalajara DEMO payload for labeled local UI QA (never production). */
  function demoPayload() {
    var hotels = [
      {
        hotelId: "fixture_gdi_pdi_demo_primary",
        hotelDisplayName: "DEMO Primary Host Hotel",
        marketId: "market_guadalajara_mx_v1",
      },
      {
        hotelId: "fixture_gdi_pdi_demo_overflow",
        hotelDisplayName: "DEMO Overflow Hotel",
        marketId: "market_guadalajara_mx_v1",
      },
      {
        hotelId: "fixture_gdi_pdi_demo_preferred",
        hotelDisplayName: "DEMO Preferred Lodging",
        marketId: "market_guadalajara_mx_v1",
      },
      {
        hotelId: "fixture_gdi_pdi_demo_meeting",
        hotelDisplayName: "DEMO Meeting / Catering",
        marketId: "market_guadalajara_mx_v1",
      },
      {
        hotelId: "fixture_gdi_pdi_demo_other",
        hotelDisplayName: "DEMO Other Partnership",
        marketId: "market_guadalajara_mx_v1",
      },
      {
        hotelId: "fixture_gdi_pdi_demo_unsuitable",
        hotelDisplayName: "DEMO Unsuitable",
        marketId: "market_guadalajara_mx_v1",
      },
      {
        hotelId: "fixture_gdi_pdi_demo_insufficient",
        hotelDisplayName: "DEMO Insufficient Evidence",
        marketId: "market_guadalajara_mx_v1",
      },
    ];
    var assessments = [
      {
        hotelId: "fixture_gdi_pdi_demo_primary",
        fitClass: "POTENTIAL_PRIMARY_HOST",
        confidence: "HIGH",
        recommendedPursuit: "DEMO: pursue as potential primary host.",
        drawer: {
          hotelId: "fixture_gdi_pdi_demo_primary",
          hotelOpportunityId: "demo_opp_primary_v1",
          detailPath:
            "/api/group-demand-intelligence/hotels/fixture_gdi_pdi_demo_primary/opportunities/demo_opp_primary_v1",
        },
      },
      {
        hotelId: "fixture_gdi_pdi_demo_overflow",
        fitClass: "OVERFLOW_ACCOMMODATION",
        confidence: "MEDIUM",
        recommendedPursuit: "DEMO: overflow / room-block pathway.",
        drawer: {
          hotelId: "fixture_gdi_pdi_demo_overflow",
          hotelOpportunityId: "demo_opp_overflow_v1",
          detailPath:
            "/api/group-demand-intelligence/hotels/fixture_gdi_pdi_demo_overflow/opportunities/demo_opp_overflow_v1",
        },
      },
      {
        hotelId: "fixture_gdi_pdi_demo_preferred",
        fitClass: "PREFERRED_LODGING_OPPORTUNITY",
        confidence: "MEDIUM",
        recommendedPursuit: "DEMO: preferred-lodging partnership route.",
        drawer: {
          hotelId: "fixture_gdi_pdi_demo_preferred",
          hotelOpportunityId: "demo_opp_preferred_v1",
          detailPath:
            "/api/group-demand-intelligence/hotels/fixture_gdi_pdi_demo_preferred/opportunities/demo_opp_preferred_v1",
        },
      },
      {
        hotelId: "fixture_gdi_pdi_demo_meeting",
        fitClass: "MEETING_CATERING_OPPORTUNITY",
        confidence: "MEDIUM",
        recommendedPursuit: "DEMO: meeting/catering capture.",
        drawer: {
          hotelId: "fixture_gdi_pdi_demo_meeting",
          hotelOpportunityId: "demo_opp_meeting_v1",
          detailPath:
            "/api/group-demand-intelligence/hotels/fixture_gdi_pdi_demo_meeting/opportunities/demo_opp_meeting_v1",
        },
      },
      {
        hotelId: "fixture_gdi_pdi_demo_other",
        fitClass: "OTHER_PARTNERSHIP",
        confidence: "LOW",
        recommendedPursuit: "DEMO: non-host partnership path.",
        drawer: {
          hotelId: "fixture_gdi_pdi_demo_other",
          hotelOpportunityId: "demo_opp_other_v1",
          detailPath:
            "/api/group-demand-intelligence/hotels/fixture_gdi_pdi_demo_other/opportunities/demo_opp_other_v1",
        },
      },
      {
        hotelId: "fixture_gdi_pdi_demo_unsuitable",
        fitClass: "UNSUITABLE",
        confidence: "HIGH",
        recommendedPursuit: "DEMO: do not pursue on current evidence.",
        drawer: {
          hotelId: "fixture_gdi_pdi_demo_unsuitable",
          hotelOpportunityId: "demo_opp_unsuitable_v1",
          detailPath: null,
          withheld: "UNPUBLISHED_HOTEL_DRAWER",
        },
      },
      {
        hotelId: "fixture_gdi_pdi_demo_insufficient",
        fitClass: "INSUFFICIENT_EVIDENCE",
        confidence: "UNKNOWN",
        recommendedPursuit: "DEMO: do not claim suitability yet.",
        missingAttributes: ["meeting_capability_unknown"],
        drawer: {
          hotelId: "fixture_gdi_pdi_demo_insufficient",
          hotelOpportunityId: "demo_opp_insufficient_v1",
          detailPath: null,
          withheld: "UNPUBLISHED_HOTEL_DRAWER",
        },
      },
    ];
    return {
      markets: [
        {
          marketId: "market_guadalajara_mx_v1",
          marketLabel: "Guadalajara, Mexico (DEMO)",
          country: "Mexico",
          hotelCount: hotels.length,
        },
      ],
      hotels: hotels,
      opportunitiesPayload: {
        marketId: "market_guadalajara_mx_v1",
        fitClasses: FIT_LABELS,
        results: [
          {
            marketOpportunityId: "demo_mog_expo_v1",
            sharedMarketPacket: {
              title: "DEMO — Regional expo (sample)",
              summaryWhat:
                "Labeled local-only demo opportunity for Portfolio UI QA. Not live research.",
              layer: "SHARED_PUBLIC_MARKET_INTELLIGENCE",
            },
            hotelAssessments: assessments,
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
    var onLoading =
      typeof opts.onLoading === "function" ? opts.onLoading : null;
    var state = {
      markets: [],
      hotels: [],
      marketId: "",
      defaultMarketId: "",
      hotelId: "",
      fitClass: "",
      opportunitiesPayload: null,
      demo: false,
      error: null,
      loading: false,
    };

    function setBusy(on, message) {
      state.loading = !!on;
      if (onLoading) onLoading(!!on, message || "Loading Portfolio Demand Intelligence…");
    }

    function paint() {
      root.innerHTML = shellHtml(state);
      bind();
    }

    function resetView() {
      state.hotelId = "";
      state.fitClass = "";
      state.marketId =
        state.defaultMarketId ||
        (state.markets[0] && state.markets[0].marketId) ||
        "";
      state.error = null;
      if (state.demo) {
        paint();
        return;
      }
      if (state.marketId) {
        loadMarket(state.marketId).catch(function (e) {
          state.error = classifyThrownError(e);
          paint();
        });
      } else {
        paint();
      }
    }

    function bind() {
      var sel = root.querySelector("#gdiPdiMarket");
      if (sel && !sel.disabled) {
        sel.addEventListener("change", function () {
          state.marketId = sel.value || "";
          state.hotelId = "";
          state.fitClass = "";
          loadMarket(state.marketId).catch(function (e) {
            state.error = classifyThrownError(e);
            paint();
          });
        });
      }
      var hotelSel = root.querySelector("#gdiPdiHotel");
      if (hotelSel && !hotelSel.disabled) {
        hotelSel.addEventListener("change", function () {
          state.hotelId = hotelSel.value || "";
          paint();
        });
      }
      var fitSel = root.querySelector("#gdiPdiFit");
      if (fitSel && !fitSel.disabled) {
        fitSel.addEventListener("change", function () {
          state.fitClass = fitSel.value || "";
          paint();
        });
      }
      var resetBtn = root.querySelector("#gdiPdiResetViewBtn");
      if (resetBtn) {
        resetBtn.addEventListener("click", function (ev) {
          ev.preventDefault();
          resetView();
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

    function classifyThrownError(e) {
      var msg = (e && e.message) || "Failed to load market portfolio.";
      var code = (e && e.code) || "LOAD_FAILED";
      if (
        code === "AUTH_REQUIRED" ||
        code === "UNAUTHENTICATED" ||
        isLoginRequiredMessage(msg)
      ) {
        return {
          code: "AUTH_REQUIRED",
          message:
            "Portfolio Demand Intelligence needs a signed-in Dealality session. You can still use Group & Demand Opportunities for hotel-scoped inventory when authorized.",
        };
      }
      return { code: code, message: msg };
    }

    async function apiGet(url) {
      var auth = memberAuthApi();
      if (!auth || typeof auth.getAuthHeaders !== "function") {
        var missing = new Error(
          "Portfolio Demand Intelligence needs a signed-in Dealality session.",
        );
        missing.code = "AUTH_REQUIRED";
        throw missing;
      }
      // Prefer getAuthHeaders (no alert) so in-page AUTH_REQUIRED is authoritative.
      var headersResult = await auth.getAuthHeaders(null, {
        waitForLogin: false,
        maxWaitMs: 4000,
      });
      if (headersResult.error) {
        var authErr = new Error(headersResult.error);
        authErr.code = "AUTH_REQUIRED";
        throw authErr;
      }
      var origin = "";
      try {
        origin = global.location.origin || "";
      } catch (e) {
        origin = "";
      }
      var absolute =
        String(url).indexOf("http") === 0
          ? url
          : origin + (String(url).charAt(0) === "/" ? url : "/" + url);
      var res = await fetch(absolute, {
        method: "GET",
        headers: headersResult.headers,
      });
      var body = null;
      try {
        body = await res.json();
      } catch (e) {
        body = null;
      }
      return { res: res, body: body };
    }

    function classifyApiFailure(body, res) {
      var code =
        (body && body.error && body.error.code) ||
        (body && body.code) ||
        "";
      var message =
        (body && body.error && body.error.message) ||
        (body && body.message) ||
        "";
      var status = (res && res.status) || (body && body.status) || 0;
      if (status === 401 || code === "UNAUTHENTICATED" || isLoginRequiredMessage(message)) {
        return {
          code: "AUTH_REQUIRED",
          message:
            "Portfolio Demand Intelligence needs a signed-in Dealality session.",
        };
      }
      if (
        status === 403 ||
        /OPERATOR_NOT_MAPPED|OPERATOR_MISMATCH|CROSS_OPERATOR/i.test(String(code))
      ) {
        return {
          code: "OPERATOR_NOT_MAPPED",
          message:
            message ||
            "Signed-in identity is not mapped to a portfolio operator membership.",
        };
      }
      if (
        status === 503 ||
        /REGISTRY|INVENTORY|UNAVAILABLE|NOT_PROVISIONED|MARKET_ALLOWLIST/i.test(
          String(code),
        ) ||
        /registry|not provisioned|unavailable|not configured/i.test(String(message))
      ) {
        return {
          code: "UNAVAILABLE",
          message:
            message ||
            "Live operator membership / portfolio inventory is not provisioned. Portfolio Demand Intelligence stays off until the steward registry is configured.",
        };
      }
      return {
        code: "LOAD_FAILED",
        message: message || "Could not load portfolio markets.",
      };
    }

    async function loadMarkets() {
      setBusy(true, "Loading Portfolio Demand Intelligence…");
      state.error = null;
      try {
        var out = await apiGet(API_MARKETS);
        if (!out.res.ok || !(out.body && out.body.ok)) {
          var classified = classifyApiFailure(out.body, out.res);
          if (classified.code === "UNAVAILABLE" && wantsExplicitDemo()) {
            applyDemo();
            return;
          }
          state.error = classified;
          paint();
          return;
        }
        state.demo = false;
        state.markets = out.body.markets || [];
        state.defaultMarketId =
          (state.markets[0] && state.markets[0].marketId) || "";
        state.marketId =
          state.marketId || state.defaultMarketId || "";
        if (!state.markets.length) {
          state.error = {
            code: "UNAVAILABLE",
            message:
              "No authorized portfolio markets are available for this account. Operator membership or market allowlist may not be provisioned yet.",
          };
          paint();
          return;
        }
        paint();
        if (state.marketId) await loadMarket(state.marketId);
      } catch (e) {
        if (wantsExplicitDemo()) {
          applyDemo();
          return;
        }
        state.error = classifyThrownError(e);
        paint();
      } finally {
        setBusy(false);
      }
    }

    async function loadMarket(marketId) {
      if (!marketId) return;
      state.marketId = marketId;
      if (state.demo) {
        paint();
        return;
      }
      setBusy(true, "Loading portfolio market…");
      try {
        var hotelsOut = await apiGet(
          API_HOTELS + "?marketId=" + encodeURIComponent(marketId),
        );
        if (!hotelsOut.res.ok || !(hotelsOut.body && hotelsOut.body.ok)) {
          state.error = classifyApiFailure(hotelsOut.body, hotelsOut.res);
          paint();
          return;
        }
        state.hotels = hotelsOut.body.hotels || [];
        if (
          state.hotelId &&
          !state.hotels.some(function (h) {
            return h.hotelId === state.hotelId;
          })
        ) {
          state.hotelId = "";
        }
        var oppOut = await apiGet(
          API_OPPS + "?marketId=" + encodeURIComponent(marketId),
        );
        if (!oppOut.res.ok || !(oppOut.body && oppOut.body.ok)) {
          state.error = classifyApiFailure(oppOut.body, oppOut.res);
          state.opportunitiesPayload = null;
          paint();
          return;
        }
        state.opportunitiesPayload = oppOut.body;
        state.error = null;
        paint();
      } finally {
        setBusy(false);
      }
    }

    function applyDemo() {
      var d = demoPayload();
      state.demo = true;
      state.markets = d.markets;
      state.hotels = d.hotels;
      state.defaultMarketId = d.markets[0].marketId;
      state.marketId = d.markets[0].marketId;
      state.hotelId = "";
      state.fitClass = "";
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
      setBusy(false);
      root.innerHTML = "";
    }

    return {
      refresh: refresh,
      destroy: destroy,
      getState: function () {
        return state;
      },
      paint: paint,
      resetView: resetView,
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
        onLoading: options.onLoading,
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
    authRequiredHtml: authRequiredHtml,
    notMappedHtml: notMappedHtml,
    filterPanelHtml: filterPanelHtml,
    demoPayload: demoPayload,
    wantsExplicitDemo: wantsExplicitDemo,
    isAuthorizedLocalDemoHost: isAuthorizedLocalDemoHost,
    FIT_LABELS: FIT_LABELS,
    FIT_CLASS_ORDER: FIT_CLASS_ORDER,
  };

  global.DealalityGdiPortfolioTab = api;

  if (typeof module !== "undefined" && module.exports) {
    module.exports = api;
  }
})(typeof window !== "undefined" ? window : globalThis);
