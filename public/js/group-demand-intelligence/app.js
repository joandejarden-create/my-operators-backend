(function () {
  "use strict";

  var UI = window.DealalityGdiUi;
  if (!UI) throw new Error("DealalityGdiUi missing");
  var PC = window.DealalityGdiUniversalPresentationController;
  if (!PC) throw new Error("DealalityGdiUniversalPresentationController missing");

  // Pilot default hotelId only (HOTEL_SPECIFIC_DATA). Display names come from profile/API.
  var DEFAULT_HOTEL_ID = "recLuxvwwxID7U2B8";
  var root = document.getElementById("gdiRoot");
  var loading = document.getElementById("gdiLoading");
  var drawer = document.getElementById("gdiDrawer");
  var drawerTitle = document.getElementById("gdiDrawerTitle");
  var drawerBody = document.getElementById("gdiDrawerBody");
  var drawerClose = document.getElementById("gdiDrawerClose");

  var STORAGE_PREFIX = "gdi-auth-";

  /** Merge iframe search + parent /app hash|search so founder preview query works in the shell. */
  function readGdiQueryParams() {
    var params = new URLSearchParams();
    function merge(src) {
      if (!src) return;
      src.forEach(function (value, key) {
        if (!params.has(key) && value != null && String(value).trim()) {
          params.set(key, String(value));
        }
      });
    }
    try {
      merge(new URLSearchParams(window.location.search || ""));
    } catch (eSearch) {
      /* ignore */
    }
    try {
      if (window.parent && window.parent !== window) {
        merge(new URLSearchParams(window.parent.location.search || ""));
        var hash = String(window.parent.location.hash || "").slice(1);
        var qIdx = hash.indexOf("?");
        if (qIdx >= 0) merge(new URLSearchParams(hash.slice(qIdx + 1)));
      }
    } catch (eParent) {
      /* cross-origin or unavailable */
    }
    return params;
  }

  function resolveInitialHotelId() {
    try {
      var params = readGdiQueryParams();
      var fromQuery = String(params.get("hotelId") || "").trim();
      if (fromQuery) return fromQuery;
      var fromStore = String(localStorage.getItem(STORAGE_PREFIX + "hotelId") || "").trim();
      if (fromStore) return fromStore;
    } catch (e) {
      /* ignore */
    }
    return DEFAULT_HOTEL_ID;
  }
  var state = {
    hotelId: resolveInitialHotelId(),
    hotels: [],
    hotelProfile: null,
    summary: null,
    opportunities: [],
    // Demand campaigns / generators are INTERNAL research objects — never customer UI state.
    demandCampaigns: [],
    demandCampaignMeta: { visibleCount: 0, storedCount: 0 },
    runs: [],
    tab: "opportunities",
    sortKey: "priority",
    sortDir: 1,
    viewMode: "tiles",
    filters: PC.createDefaultFilters(),
    pursuitsById: {},
    isAdmin: false,
    flag: null,
    visibilityWarning: null,
    /** Missing STAGED/CURRENT inventory — compact empty state, not full-page error. */
    inventoryEmpty: null,
    drawerReturnFocusEl: null,
    contactRevealInFlight: Object.create(null),
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
    // Prefer shared UI reset so opportunityType / sales facets clear consistently.
    state.filters =
      typeof UI.resetBrowseFilters === "function"
        ? UI.resetBrowseFilters()
        : PC.createDefaultFilters();
    state.filters.opportunityType = "";
    state.filters.weekly = "";
    state.filters.priority = "";
    state.filters.salesOpportunity = "";
    state.filters.marketScope = "ALL";
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

  function propertyBarActionsHtml() {
    // Hotel toolbar: Reset View only. Research + Publish remain available via
    // founder/admin APIs, Full E2E CLI, and one-click publish scripts — not here.
    if (typeof UI.resetViewButtonHtml === "function") {
      return UI.resetViewButtonHtml({ filters: state.filters });
    }
    return '<button type="button" class="btn-clear" id="gdiResetViewBtn">Reset View</button>';
  }

  function publishHotelInventory() {
    if (!memberFetch || !state.hotelId) return;
    var btn = document.getElementById("gdiPublishBtn");
    if (btn) btn.disabled = true;
    var url =
      "/api/group-demand-intelligence/hotels/" +
      encodeURIComponent(state.hotelId) +
      "/publish";

    function finish(msg, ok) {
      if (btn) btn.disabled = false;
      if (msg) window.alert(msg);
      if (ok) loadAll();
    }

    fetchFn(url, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ dryRun: true }),
    })
      .then(function (r) {
        return r.json().then(function (body) {
          return { status: r.status, body: body };
        });
      })
      .then(function (dry) {
        var result = (dry.body && dry.body.result) || {};
        if (!dry.body || dry.body.ok !== true) {
          finish(
            "Publish validation failed: " +
              (result.code || (dry.body && dry.body.error) || "unknown"),
            false
          );
          return null;
        }
        var plan = result.plan || {};
        var counts = plan.classificationCounts || {};
        var confirmed = window.confirm(
          "Publish " +
            (plan.opportunityCount || 0) +
            " approved opportunities to the customer link?\n\n" +
            "Sales Ready: " +
            (counts.SALES_READY || 0) +
            " · Account Development: " +
            (counts.ACCOUNT_DEVELOPMENT || 0) +
            "\n\nPrevious publication is preserved for rollback. Share token is unchanged."
        );
        if (!confirmed) {
          finish(null, false);
          return null;
        }
        return fetchFn(url, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            dryRun: false,
            approval: {
              authorized: true,
              authorizationPhrase: "GDI_ONE_CLICK_HOTEL_PUBLISH_V1",
            },
          }),
        }).then(function (r) {
          return r.json().then(function (body) {
            return { status: r.status, body: body };
          });
        });
      })
      .then(function (apply) {
        if (!apply) return;
        var result = (apply.body && apply.body.result) || {};
        if (apply.body && apply.body.ok === true && result.advanced === true) {
          finish(
            "Published. Customer share links now serve the approved inventory (" +
              ((result.plan && result.plan.opportunityCount) || 0) +
              " opportunities).",
            true
          );
          return;
        }
        finish(
          "Publish did not advance: " +
            (result.code || (apply.body && apply.body.error) || "unknown"),
          false
        );
      })
      .catch(function (err) {
        finish(
          "Publish failed: " + String(err && err.message ? err.message : err),
          false
        );
      });
  }

  readPersistedBrowse();

  // Prefer plain fetch for GDI pilot reads (PILOT_READ allows unauth).
  // Memberstack is used only when a session exists and for admin actions.
  function plainFetch(url, opts) {
    return fetch(url, opts);
  }

  var memberAuth = window.DealalityMemberstackAuth || null;
  var memberFetch =
    memberAuth && memberAuth.fetchMyDealsApi
      ? memberAuth.fetchMyDealsApi.bind(memberAuth)
      : null;

  /**
   * GDI optional/authenticated requests must never raise window.alert().
   * On missing credentials we either silently retry with a JWT (when present)
   * or return the original 401 so callers can show an inline state.
   */
  function withSilentAuthOptions(opts) {
    return Object.assign({}, opts || {}, {
      notifyLoginRequired: false,
      silent: true,
      waitForLogin: false,
    });
  }

  function retryWithMemberstackJwt(url, opts, priorResponse) {
    if (!memberAuth || typeof memberAuth.getAuthHeaders !== "function") {
      return Promise.resolve(priorResponse);
    }
    return memberAuth
      .getAuthHeaders(null, { waitForLogin: false, maxWaitMs: 2500 })
      .then(function (headersResult) {
        if (!headersResult || headersResult.error || !headersResult.headers) {
          return priorResponse;
        }
        var mergedHeaders = Object.assign(
          {},
          headersResult.headers,
          (opts && opts.headers) || {},
        );
        return plainFetch(url, Object.assign({}, opts || {}, { headers: mergedHeaders }));
      })
      .catch(function () {
        return priorResponse;
      });
  }

  function fetchFn(url, opts) {
    opts = opts || {};
    var method = String(opts.method || "GET").toUpperCase();
    var needsAuthWrite = method !== "GET" && method !== "HEAD";
    if (needsAuthWrite && memberFetch) {
      return memberFetch(url, withSilentAuthOptions(opts)).catch(function (err) {
        // Preserve auth failure without native alert; let api() surface inline.
        if (err && (err.code === "AUTH_REQUIRED" || err.authRequired)) throw err;
        throw err;
      });
    }
    return plainFetch(url, opts).then(function (r) {
      if (r.status === 401) return retryWithMemberstackJwt(url, opts, r);
      return r;
    });
  }

  /**
   * Contact reveal POST: convert browser-side AUTH_REQUIRED throws into a
   * JSON 401 Response so the reveal client can show Sign-in required instead
   * of the Memberstack "Please log in again" string for every failure.
   * When a JWT is present, uses the same authenticated write path as other GDI posts.
   */
  function contactRevealFetch(url, opts) {
    return fetchFn(url, withSilentAuthOptions(opts)).catch(function (err) {
      if (
        err &&
        (err.code === "AUTH_REQUIRED" ||
          err.authRequired ||
          /please log in again/i.test(String((err && err.message) || "")))
      ) {
        return new Response(
          JSON.stringify({
            ok: false,
            ephemeral: true,
            persisted: false,
            surfePiiPersisted: false,
            error: "SIGN_IN_REQUIRED",
            message:
              "Sign in to Dealality to look up email and phone. Open Group & Demand Intelligence from the Dealality app after signing in.",
            contactDetails: null,
            signInHref: "/app#/group-demand-intelligence",
          }),
          {
            status: 401,
            headers: { "Content-Type": "application/json" },
          },
        );
      }
      throw err;
    });
  }

  function esc(s) {
    return String(s == null ? "" : s)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;");
  }

  function fmtVal(v) {
    if (v == null || v === "" || v === "UNKNOWN") return "—";
    return v;
  }

  function formatHistoryDate(iso) {
    if (!iso) return "History unavailable";
    var d = new Date(iso);
    if (isNaN(d.getTime())) return String(iso);
    try {
      return (
        d.toLocaleString(undefined, {
          year: "numeric",
          month: "short",
          day: "numeric",
          hour: "2-digit",
          minute: "2-digit",
          timeZoneName: "short",
        }) || d.toISOString()
      );
    } catch (e) {
      return d.toISOString();
    }
  }

  function formatDate(iso) {
    return formatHistoryDate(iso);
  }

  function applyEmbedMode() {
    var params = new URLSearchParams(window.location.search || "");
    var inFrame = false;
    try {
      inFrame = window !== window.top;
    } catch (e) {
      inFrame = true;
    }
    if (inFrame || params.get("embed") === "1") {
      document.body.classList.add("embed-mode");
    }
  }

  function setLoading(on, message) {
    UI.setLoadingToast(loading, on, message);
  }

  function showError(msg) {
    setLoading(false);
    // Genuine load/auth failures only — missing STAGED/CURRENT uses compact empty state.
    root.innerHTML =
      '<div class="gdi-error" role="alert"><p>' +
      esc(msg) +
      "</p><p>Local servers enable pilot read automatically. On production set GROUP_DEMAND_INTELLIGENCE_V1=1. Admin actions still require sign-in.</p>" +
      '<p class="gdi-error__nav"><button type="button" class="gdi-btn gdi-btn-primary" data-gdi-recovery="retry-load">Retry</button></p></div>';
    wireRecoveryActions({ hasCurrentPublication: false });
  }

  function hotelLabelForId(hotelId) {
    var id = String(hotelId || "").trim();
    var hotels = state.hotels || [];
    for (var i = 0; i < hotels.length; i++) {
      if (String(hotels[i].hotelId) === id) {
        return (
          hotels[i].optionLabel ||
          hotels[i].displayName ||
          hotels[i].hotelName ||
          id
        );
      }
    }
    return id || "this property";
  }

  function setPreviewQueryParam(value) {
    try {
      var url = new URL(window.location.href);
      if (value) url.searchParams.set("preview", value);
      else url.searchParams.delete("preview");
      url.searchParams.delete("live");
      window.history.replaceState({}, "", url.pathname + url.search + url.hash);
    } catch (e) {
      /* ignore */
    }
  }

  function wireRecoveryActions(opts) {
    opts = opts || {};
    var published = root.querySelector(
      '[data-gdi-recovery="view-published"]'
    );
    if (published && opts.hasCurrentPublication) {
      published.addEventListener("click", function () {
        applyFounderInventoryMode("published");
        setLoading(true, UI.DEFAULT_PAGE_LOADING_MESSAGE);
        loadAll().catch(function (err) {
          handleLoadFailure(err);
        });
      });
    }
    // Research / lifecycle navigation uses root event delegation (_gdiLifecycleDelegated).
    var retry = root.querySelector('[data-gdi-recovery="retry-load"]');
    if (retry) {
      retry.addEventListener("click", function () {
        setLoading(true, UI.DEFAULT_PAGE_LOADING_MESSAGE);
        loadAll().catch(function (err) {
          handleLoadFailure(err);
        });
      });
    }
  }

  function isMissingInventoryError(err) {
    var code =
      (err && err.data && err.data.error) ||
      (err && err.code) ||
      "";
    return (
      String(code) === "STAGED_PUBLICATION_PREVIEW_UNAVAILABLE" ||
      String(code) === "CURRENT_PUBLICATION_PREVIEW_UNAVAILABLE"
    );
  }

  function researchInventoryCounts() {
    var s = state.summary || {};
    var recovery =
      (state.inventoryEmpty && state.inventoryEmpty.recovery) || {};
    var fromRecovery =
      (state.inventoryEmpty && state.inventoryEmpty.researchSummary) ||
      recovery.researchSummary ||
      null;
    var discovered = Number(
      (fromRecovery && fromRecovery.discovered) != null
        ? fromRecovery.discovered
        : s.opportunitiesDiscovered != null
          ? s.opportunitiesDiscovered
          : 0
    );
    var qualified = Number(
      (fromRecovery && fromRecovery.qualified) != null
        ? fromRecovery.qualified
        : s.qualifiedCount != null
          ? s.qualifiedCount
          : 0
    );
    if (!Number.isFinite(discovered)) discovered = 0;
    if (!Number.isFinite(qualified)) qualified = 0;
    return {
      discovered: discovered,
      qualified: qualified,
      hasResearch: discovered > 0 || qualified > 0,
    };
  }

  function finalizedStagedCount() {
    if (state.inventoryEmpty && state.inventoryEmpty.mode === "staged") {
      return 0;
    }
    if (resolveFounderInventoryMode() === "staged") {
      return (state.opportunities || []).length;
    }
    var pub = state.publication || {};
    if (pub.opportunityCount != null) return Number(pub.opportunityCount) || 0;
    if (
      String(pub.previewSource || "").toUpperCase() === "STAGED" &&
      Array.isArray(state.opportunities)
    ) {
      return state.opportunities.length;
    }
    return null;
  }

  function customerFacingMissingInventoryMessage(mode, data) {
    data = data || {};
    var research =
      (data.researchSummary && data.researchSummary.hasResearch) ||
      researchInventoryCounts().hasResearch;
    if (mode === "staged") {
      if (research) {
        return "Research available — no finalized package yet";
      }
      return "No finalized opportunities are available for this property.";
    }
    if (mode === "published") {
      return "No opportunities are available for this property yet.";
    }
    return "No opportunities are available for this property in the selected view.";
  }

  function applyMissingInventoryEmptyState(err) {
    var data = (err && err.data) || {};
    var code = String(data.error || (err && err.code) || "");
    var mode =
      code === "CURRENT_PUBLICATION_PREVIEW_UNAVAILABLE" ? "published" : "staged";
    state.opportunities = [];
    state.publication = null;
    state.visibilityWarning = null;
    if (data.researchSummary && !state.summary) {
      state.summary = {
        opportunitiesDiscovered: data.researchSummary.discovered,
        qualifiedCount: data.researchSummary.qualified,
        lastResearchAt: data.researchSummary.lastResearchAt,
        runStatus: data.researchSummary.runStatus,
      };
    } else if (data.researchSummary && state.summary) {
      if (state.summary.opportunitiesDiscovered == null) {
        state.summary.opportunitiesDiscovered = data.researchSummary.discovered;
      }
      if (state.summary.qualifiedCount == null) {
        state.summary.qualifiedCount = data.researchSummary.qualified;
      }
    }
    state.inventoryEmpty = {
      mode: mode,
      code: code,
      lifecycleState: data.lifecycleState || null,
      researchSummary: data.researchSummary || null,
      message: customerFacingMissingInventoryMessage(mode, data),
      recovery: data.recovery || null,
    };
  }

  function inventoryUnavailableEmptyHtml() {
    var empty = state.inventoryEmpty;
    if (!empty) return "";
    var recovery = empty.recovery || {};
    var counts = researchInventoryCounts();
    var researchAvailable =
      empty.mode === "staged" &&
      (counts.hasResearch ||
        (empty.researchSummary && empty.researchSummary.hasResearch));
    var publishedBtn =
      empty.mode === "staged" && recovery.hasCurrentPublication === true
        ? '<button type="button" class="gdi-btn" data-gdi-recovery="view-published">View Published</button>'
        : "";
    var researchBtn = researchAvailable
      ? '<button type="button" class="gdi-btn gdi-btn-primary" data-gdi-recovery="view-research">View Research Opportunities</button>'
      : "";
    var body = researchAvailable
      ? "Demand opportunities have been identified for this property, but no finalized STAGED package is currently available. Review the research inventory to see the existing findings and their qualification status."
      : empty.message;
    var title = researchAvailable
      ? "Research available — no finalized package yet"
      : empty.message;
    var countsHtml = researchAvailable
      ? '<p class="gdi-empty__counts" data-gdi-research-empty-counts="1">' +
        "<strong>Discovered:</strong> " +
        esc(String(counts.discovered)) +
        " · <strong>Qualified Research:</strong> " +
        esc(String(counts.qualified)) +
        " · <strong>Finalized STAGED:</strong> unavailable" +
        "</p>"
      : "";
    var actions =
      researchBtn || publishedBtn
        ? '<p class="gdi-empty__actions">' + researchBtn + publishedBtn + "</p>"
        : "";
    return (
      '<div class="gdi-empty gdi-empty--inventory-unavailable" role="status" data-gdi-inventory-empty="' +
      esc(empty.mode) +
      '" data-gdi-lifecycle-state="' +
      esc(empty.lifecycleState || (researchAvailable ? "STAGED_UNAVAILABLE_RESEARCH_AVAILABLE" : "STAGED_UNAVAILABLE_NO_RESEARCH")) +
      '">' +
      "<p><strong>" +
      esc(title) +
      "</strong></p>" +
      (researchAvailable
        ? "<p>" + esc(body) + "</p>"
        : "") +
      countsHtml +
      actions +
      "</div>"
    );
  }

  function handleLoadFailure(err) {
    if (isMissingInventoryError(err)) {
      var ensureCatalog =
        state.hotels && state.hotels.length
          ? Promise.resolve()
          : loadHotelCatalog().catch(function () {
              return null;
            });
      return ensureCatalog.then(function () {
        applyMissingInventoryEmptyState(err);
        setLoading(false);
        render();
      });
    }
    showError(
      (err && err.message) || "Failed to load Group & Demand Intelligence"
    );
    return Promise.resolve();
  }

  function priorityPill(p) {
    return UI.priorityPill(p);
  }

  function countEnteringWindow() {
    return state.opportunities.filter(function (o) {
      return (
        o.bookingWindowStatus === "CONTACT_NOW" ||
        o.bookingWindowStatus === "QUALIFY_NOW" ||
        o.bookingWindowStatus === "RESEARCH_FURTHER" ||
        (o.labels || []).indexOf("entering_booking_window") >= 0
      );
    }).length;
  }

  function qualifiedCount() {
    var s = state.summary || {};
    if (s.qualifiedCount != null) return s.qualifiedCount;
    return state.opportunities.length;
  }

  function oppById(id) {
    for (var i = 0; i < state.opportunities.length; i++) {
      if (state.opportunities[i].id === id) return state.opportunities[i];
    }
    return null;
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
      (c.phoneTypeLabel || c.phoneType ? " (" + esc(c.phoneTypeLabel || c.phoneType) + ")" : "") +
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

  function trunc(s, n) {
    s = s == null ? "" : String(s);
    if (s.length <= n) return s;
    return s.slice(0, n) + "…";
  }

  function api(path, opts) {
    return fetchFn(path, opts)
      .catch(function (networkErr) {
        if (
          networkErr &&
          (networkErr.code === "AUTH_REQUIRED" || networkErr.authRequired)
        ) {
          var authErr = new Error(
            networkErr.message ||
              "Sign in required to continue with this Group & Demand Intelligence action.",
          );
          authErr.code = "AUTH_REQUIRED";
          authErr.status = 401;
          authErr.cause = networkErr;
          throw authErr;
        }
        var err = new Error(
          "This opportunity could not be opened. Please try again. If the problem continues, refresh the list and reopen the card."
        );
        err.code = "GDI_NETWORK_FETCH_FAILED";
        err.cause = networkErr;
        err.status = 0;
        console.error("[gdi] network fetch failed", path, networkErr);
        throw err;
      })
      .then(function (r) {
        return r.json().then(function (data) {
          if (!r.ok || data.ok === false) {
            var msg = data.message || data.error || "Request failed";
            if (r.status === 401) {
              msg =
                "Sign in required to continue with this Group & Demand Intelligence action.";
            } else if (r.status === 403) {
              msg =
                data.message ||
                "Access denied for this Group & Demand Intelligence request.";
            } else if (/opportunity_not_found/i.test(String(msg))) {
              msg =
                data.message && !/opportunity_not_found/i.test(String(data.message))
                  ? data.message
                  : "This opportunity could not be opened. Please try again. If the problem continues, refresh the list and reopen the card.";
            }
            var err = new Error(msg);
            err.status = r.status;
            err.data = data;
            if (r.status === 401) err.code = "AUTH_REQUIRED";
            if (r.status === 403) err.code = "ACCESS_DENIED";
            throw err;
          }
          return data;
        });
      });
  }

  function clearHotelScopedState() {
    state.hotelProfile = null;
    state.summary = null;
    state.opportunities = [];
    state.publication = null;
    state.visibilityWarning = null;
    state.inventoryEmpty = null;
    state.runs = [];
    state.filters = PC.createDefaultFilters();
    state.sortKey = "priority";
    state.sortDir = 1;
    state.viewMode = "tiles";
    state.tab = "opportunities";
    state._prioritiesMountedHotelId = null;
    if (
      window.DealalityGdiNeedTimesPrioritiesTab &&
      typeof window.DealalityGdiNeedTimesPrioritiesTab.unmount === "function"
    ) {
      window.DealalityGdiNeedTimesPrioritiesTab.unmount();
    }
    if (typeof drawer !== "undefined" && drawer) {
      if (typeof drawer.close === "function") drawer.close();
      else drawer.removeAttribute("open");
    }
  }

  function selectHotel(hotelId, opts) {
    opts = opts || {};
    var next = String(hotelId || "").trim();
    if (!next || next === state.hotelId) return Promise.resolve();
    state.hotelId = next;
    try {
      localStorage.setItem(STORAGE_PREFIX + "hotelId", next);
      var url = new URL(window.location.href);
      url.searchParams.set("hotelId", next);
      // Preserve explicit Research/Staged/Published mode across hotel switches so
      // a missing STAGED/CURRENT shows the compact empty state for that mode.
      // Only clear when callers intentionally request a reset (clearPreview:true).
      if (opts.clearPreview === true) {
        url.searchParams.delete("preview");
        url.searchParams.delete("live");
        url.searchParams.delete("inventory");
        url.searchParams.delete("gdiInventory");
        url.searchParams.delete("publicationPackage");
      }
      window.history.replaceState({}, "", url.pathname + url.search + url.hash);
    } catch (e) {
      /* ignore */
    }
    clearHotelScopedState();
    notifyShellHotelContext();
    setLoading(true, UI.DEFAULT_PAGE_LOADING_MESSAGE);
    return loadAll();
  }

  function loadHotelCatalog() {
    return api("/api/group-demand-intelligence/hotels")
      .then(function (payload) {
        state.hotels = Array.isArray(payload.hotels) ? payload.hotels : [];
        var ids = state.hotels.map(function (h) {
          return h.hotelId;
        });
        if (ids.length && ids.indexOf(state.hotelId) === -1) {
          var explicitHotelId = "";
          try {
            explicitHotelId = String(
              new URLSearchParams(window.location.search || "").get("hotelId") ||
                ""
            ).trim();
          } catch (e) {
            explicitHotelId = "";
          }
          // Never silently remap an explicitly requested hotelId to another hotel
          // (historically caused Bethesda fallback when Sheraton was missing).
          if (explicitHotelId && explicitHotelId === state.hotelId) {
            state.visibilityWarning = {
              code: "HOTEL_NOT_IN_SELECTABLE_CATALOG",
              message:
                "Requested hotel is not in the selectable GDI catalog. Refusing to load a different hotel.",
              requestedHotelId: explicitHotelId,
            };
          } else if (!explicitHotelId) {
            state.hotelId = ids[0];
          }
        }
        return state.hotels;
      })
      .catch(function () {
        state.hotels = [];
        return state.hotels;
      });
  }

  function applyOpportunitiesPayload(oppPayload) {
    oppPayload = oppPayload || {};
    // Fail closed: never treat an unfiltered Bethesda union payload as the
    // primary commercial list (stale server / filter bypass).
    var listScope = String(oppPayload.listScope || "");
    var effectiveSurface = String(
      oppPayload.effectiveSurface ||
        (oppPayload.bethesdaInternalUnion &&
          oppPayload.bethesdaInternalUnion.commercialSurface &&
          oppPayload.bethesdaInternalUnion.commercialSurface.effectiveSurface) ||
        ""
    ).toLowerCase();
    var isBethesdaSalesTeamScope =
      listScope === "BETHESDA_SALES_TEAM_INVENTORY_67" ||
      String(oppPayload.inventoryAudience || "") === "SALES_TEAM_INVENTORY";
    var isBethesdaUnsafeCommercial =
      listScope === "BETHESDA_INTERNAL_UNION_88" &&
      effectiveSurface !== "commercial" &&
      !isBethesdaSalesTeamScope &&
      !String(state.filters && (state.filters.surface || state.filters.listSurface) || "")
        .trim();
    if (oppPayload.ok === false) {
      state.opportunities = [];
      state.visibilityWarning = {
        code: oppPayload.error || "COMMERCIAL_SURFACE_FILTER_FAILED",
        message:
          oppPayload.message ||
          "Commercial opportunity list unavailable (filter failed closed).",
      };
    } else if (isBethesdaUnsafeCommercial) {
      state.opportunities = [];
      state.visibilityWarning = {
        code: "UNSAFE_BETHESDA_LIST_SCOPE",
        message:
          "Commercial list blocked: server returned unfiltered research inventory. Restart the GDI server onto current main.",
        listScope: listScope,
        effectiveSurface: effectiveSurface || null,
        serverBuildSha: oppPayload.serverBuildSha || null,
      };
    } else {
      state.opportunities = oppPayload.opportunities || [];
      state.visibilityWarning = oppPayload.visibilityWarning || null;
    }
    state.inventoryAudience = oppPayload.inventoryAudience || null;
    state.inventoryPopulationCounts =
      oppPayload.inventoryPopulationCounts || null;
    state.bethesda67Baseline = oppPayload.bethesda67Baseline || null;
    state.publication = oppPayload.publication || null;
    state.listSurfaceMeta = {
      listScope: listScope || null,
      inventoryAudience: oppPayload.inventoryAudience || null,
      requestedSurface: oppPayload.requestedSurface || null,
      effectiveSurface: effectiveSurface || null,
      eligibleCount: oppPayload.eligibleCount,
      excludedCount: oppPayload.excludedCount,
      serverBuildSha: oppPayload.serverBuildSha || null,
      sourceOfTruth: oppPayload.sourceOfTruth || null,
      previewSource:
        (oppPayload.publication && oppPayload.publication.previewSource) || null,
      contentFingerprintSha256:
        (oppPayload.publication &&
          oppPayload.publication.contentFingerprintSha256) ||
        null,
      publicationVersion:
        (oppPayload.publication && oppPayload.publication.publicationVersion) ||
        null,
    };
    state.hotelNeedDateConfigV1 =
      oppPayload.hotelNeedDateConfigV1 ||
      (UI.normalizeHotelNeedDateConfigV1
        ? UI.normalizeHotelNeedDateConfigV1(null, state.hotelId)
        : {
            status: "NOT_ESTABLISHED",
            accessibleLabel: "Need dates not established",
            icon: "calendar-minus",
            colorClass: "gdi-hotel-need-date-status--not-established",
          });
    try {
      window.__GDI_HOTEL_NEED_DATE_CONFIG__ = state.hotelNeedDateConfigV1;
    } catch (_) {
      /* ignore */
    }
  }

  /**
   * Founder inventory views are separate from customer publication.
   * research = hotel research store (no STAGED/CURRENT required)
   * staged = explicit STAGED package only (fail-closed)
   * published = CURRENT customer package only
   */
  function resolveFounderInventoryMode() {
    try {
      var params = readGdiQueryParams();
      var inventory = String(
        params.get("inventory") || params.get("gdiInventory") || ""
      )
        .trim()
        .toLowerCase();
      var preview = String(params.get("preview") || "")
        .trim()
        .toLowerCase();
      if (
        inventory === "research" ||
        inventory === "development" ||
        inventory === "dev" ||
        inventory === "live"
      ) {
        return "research";
      }
      if (inventory === "staged") return "staged";
      if (inventory === "published" || inventory === "current") {
        return "published";
      }
      if (preview === "staged") return "staged";
      if (preview === "current" || preview === "published") return "published";
      if (
        String(params.get("live") || "") === "1" ||
        String(params.get("publicationPackage") || "") === "0"
      ) {
        return "research";
      }
      // Local founder review: default to STAGED finalized packages (Full E2E
      // contract). Research remains available only via explicit
      // ?inventory=research / ?live=1 — never as a silent substitute for STAGED.
      if (isLocalFounderHost()) return "staged";
      // Production authenticated: customer-visible CURRENT package by default.
      return "published";
    } catch (e) {
      return isLocalFounderHost() ? "staged" : "published";
    }
  }

  function wantsPublicationPackagePreview() {
    var mode = resolveFounderInventoryMode();
    return mode === "published" || mode === "staged";
  }

  function wantsLiveResearchBrowse() {
    return resolveFounderInventoryMode() === "research";
  }

  function wantsExplicitStagedPreview() {
    return resolveFounderInventoryMode() === "staged";
  }

  function isLocalFounderHost() {
    try {
      var host = String(
        (window.location && window.location.hostname) || ""
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

  function applyFounderInventoryMode(mode) {
    var next = String(mode || "").toLowerCase();
    try {
      var url = new URL(window.location.href);
      url.searchParams.delete("live");
      url.searchParams.delete("publicationPackage");
      url.searchParams.delete("preview");
      url.searchParams.delete("inventory");
      url.searchParams.delete("gdiInventory");
      if (next === "research") {
        url.searchParams.set("inventory", "research");
        url.searchParams.set("live", "1");
      } else if (next === "staged") {
        url.searchParams.set("inventory", "staged");
        url.searchParams.set("preview", "staged");
      } else if (next === "published") {
        url.searchParams.set("inventory", "published");
        url.searchParams.set("preview", "current");
      }
      window.history.replaceState({}, "", url.pathname + url.search + url.hash);
    } catch (e) {
      /* ignore */
    }
    notifyShellHotelContext();
  }

  function notifyShellHotelContext() {
    try {
      if (!window.parent || window.parent === window) return;
      window.parent.postMessage(
        {
          source: "dealality-gdi",
          type: "gdi-hotel-context",
          hotelId: state.hotelId || null,
          hotelLabel: hotelLabelForId(state.hotelId),
          inventoryMode: resolveFounderInventoryMode(),
        },
        window.location.origin
      );
    } catch (e) {
      /* cross-origin / closed */
    }
  }

  /** Explicit developer diagnostic only — never shown in normal auth/share UI. */
  function wantsPresentationModeDiagnostics() {
    try {
      return String(readGdiQueryParams().get("gdiDiag") || "") === "1";
    } catch (e) {
      return false;
    }
  }

  function opportunitiesApiPath(hotelIdEnc) {
    var path =
      "/api/group-demand-intelligence/hotels/" + hotelIdEnc + "/opportunities";
    var q = [];
    try {
      var params = readGdiQueryParams();
      var audience = String(
        params.get("inventoryAudience") || params.get("audience") || ""
      ).trim();
      // Explicit published audience only — default remains authorized sales-team.
      if (/^published/i.test(audience)) {
        q.push("inventoryAudience=" + encodeURIComponent("published"));
      }
      if (wantsLiveResearchBrowse()) {
        q.push("live=1");
      } else if (wantsPublicationPackagePreview()) {
        // Canonical customer surface — same immutable package as external share.
        q.push("publicationPackage=1");
        q.push("surface=customer");
        // Explicit founder staged-preview (no CURRENT / no silent Bethesda fallback).
        if (wantsExplicitStagedPreview()) {
          q.push("preview=staged");
        } else {
          // Published Inventory: CURRENT only — never STAGED or research substitute.
          q.push("preview=current");
        }
      }
    } catch (e) {
      /* ignore */
    }
    if (q.length) path += "?" + q.join("&");
    return path;
  }

  function loadOpportunitiesOrEmpty(hotelIdEnc) {
    return api(opportunitiesApiPath(hotelIdEnc)).then(
      function (payload) {
        return { ok: true, payload: payload };
      },
      function (err) {
        if (isMissingInventoryError(err)) {
          return { ok: false, missingInventory: true, err: err };
        }
        throw err;
      }
    );
  }

  function loadAll() {
    var knownHotelId = state.hotelId;
    var id = encodeURIComponent(knownHotelId);
    // Critical path: catalog + flag + summary + opportunities + profile in parallel.
    // Hotel-scoped fetches use the known hotelId immediately; catalog may refine it.
    // Research-run history/costs load only in AI Demand Admin (admin ledger tab).
    // Missing STAGED/CURRENT is a normal empty state — keep shell navigation.
    return Promise.all([
      loadHotelCatalog(),
      api("/api/group-demand-intelligence/flag"),
      api("/api/group-demand-intelligence/hotels/" + id + "/summary").catch(
        function () {
          return { summary: null };
        }
      ),
      loadOpportunitiesOrEmpty(id),
      api("/api/group-demand-intelligence/hotels/" + id + "/profile").catch(function () {
        return { ok: false, profile: null };
      }),
    ]).then(function (parts) {
      state.flag = parts[1].flag;
      // If catalog remapped hotelId, refetch hotel-scoped payloads once.
      if (state.hotelId !== knownHotelId) {
        id = encodeURIComponent(state.hotelId);
        return Promise.all([
          api("/api/group-demand-intelligence/hotels/" + id + "/summary").catch(
            function () {
              return { summary: null };
            }
          ),
          loadOpportunitiesOrEmpty(id),
          api("/api/group-demand-intelligence/hotels/" + id + "/profile").catch(function () {
            return { ok: false, profile: null };
          }),
        ]).then(function (scoped) {
          state.summary = scoped[0].summary;
          if (scoped[1] && scoped[1].missingInventory) {
            applyMissingInventoryEmptyState(scoped[1].err);
          } else {
            state.inventoryEmpty = null;
            applyOpportunitiesPayload((scoped[1] && scoped[1].payload) || {});
          }
          state.hotelProfile = (scoped[2] && scoped[2].profile) || null;
          state.demandCampaigns = [];
          state.demandCampaignMeta = { visibleCount: 0, storedCount: 0 };
          state.runs = [];
          render();
        });
      }
      state.summary = parts[2].summary;
      if (parts[3] && parts[3].missingInventory) {
        applyMissingInventoryEmptyState(parts[3].err);
      } else {
        state.inventoryEmpty = null;
        applyOpportunitiesPayload((parts[3] && parts[3].payload) || {});
      }
      state.hotelProfile = (parts[4] && parts[4].profile) || null;
      state.demandCampaigns = [];
      state.demandCampaignMeta = { visibleCount: 0, storedCount: 0 };
      state.runs = [];
      render();
    });
  }

  function runResearch() {
    if (
      !window.confirm(
        "Run Group Demand research for this hotel? This does not auto-spend Webhound unless configured in the run payload."
      )
    ) {
      return;
    }
    setLoading(true, "Refreshing\u2026");
    api(
      "/api/group-demand-intelligence/hotels/" +
        encodeURIComponent(state.hotelId) +
        "/research/run",
      {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({}),
      }
    )
      .then(function () {
        return loadAll();
      })
      .catch(function (err) {
        showError(err.message || "Research run failed");
      });
  }

  function filteredSorted() {
    return PC.filteredSorted(state, UI);
  }

    function applyTilePairFilter(priority, booking, e) {
    PC.applyTilePairFilter(state, priority, booking, e, render);
  }

    function wireBrowseControls() {
    PC.wireBrowseFilterControls({
      root: root,
      state: state,
      UI: UI,
      render: render,
      persistBrowse: persistBrowse,
      onExport: downloadCustomerCsv,
    });
    // Auth-only pursuit controls (capability-gated; not on share).
    root.querySelectorAll("[data-pursuit-start]").forEach(function (btn) {
      btn.addEventListener("click", function (e) {
        e.preventDefault();
        e.stopPropagation();
        var oppId = btn.getAttribute("data-pursuit-start");
        if (!oppId) return;
        startPursuit(oppId);
      });
    });
    root.querySelectorAll("[data-pursuit-view]").forEach(function (btn) {
      btn.addEventListener("click", function (e) {
        e.preventDefault();
        e.stopPropagation();
        var pursuitId = btn.getAttribute("data-pursuit-view");
        var oppId = btn.getAttribute("data-open");
        if (oppId) openDetail(oppId);
        else if (pursuitId) openPursuitDrawer(pursuitId);
      });
    });
  }

  function renderOpportunityCards(rows) {
    if (!rows.length) {
      if (state.inventoryEmpty) {
        return inventoryUnavailableEmptyHtml();
      }
      var total = (state.opportunities || []).length;
      if (total > 0) {
        var scope = String(
          (state.filters && state.filters.marketScope) || "ALL"
        ).toUpperCase();
        if (scope === "COMPETITIVE") {
          return (
            '<div class="gdi-empty">' +
            "No Competitive Market Intelligence records match the current filters for this hotel." +
            "</div>"
          );
        }
        if (resolveFounderInventoryMode() === "research") {
          return (
            '<div class="gdi-empty">' +
            "No research opportunities match the current filters. Use Reset View to clear filters." +
            "</div>"
          );
        }
        return '<div class="gdi-empty">No customer-ready opportunities match the current filters.</div>';
      }
      var warn = state.visibilityWarning;
      if (warn && warn.message) {
        var pubFail =
          warn.publicationDataConnection === false ||
          String(warn.code || "").indexOf("REGISTRY") === 0 ||
          String(warn.code || "") === "HOTEL_NOT_IN_ALLOWLIST" ||
          String(warn.code || "") === "REGISTRY_FLAG_OFF";
        return (
          '<div class="gdi-empty gdi-empty--error" role="alert">' +
          (pubFail
            ? "<strong>Unable to load approved opportunities.</strong> "
            : "<strong>Visibility configuration issue.</strong> ") +
          esc(warn.message) +
          (warn.reviewedInStore
            ? " (" +
              Number(warn.reviewedInStore) +
              " reviewed prospect" +
              (Number(warn.reviewedInStore) === 1 ? "" : "s") +
              " in storage.)"
            : "") +
          "</div>"
        );
      }
      return '<div class="gdi-empty">No customer-ready opportunities are available for this property yet.</div>';
    }
    return UI.opportunityCardsGridHtml(rows, state.viewMode, state.filters);
  }

  function buildExportHref() {
    if (!state.hotelId) return "";
    var rows = filteredSorted();
    var q = [];
    var mode = resolveFounderInventoryMode();
    if (mode === "research") {
      q.push("live=1");
      q.push("inventory=research");
    } else if (mode === "staged") {
      q.push("preview=staged");
      q.push("publicationPackage=1");
    } else {
      q.push("preview=current");
      q.push("publicationPackage=1");
    }
    var ids = rows
      .map(function (o) {
        return o.id || o.opportunityId;
      })
      .filter(Boolean);
    if (ids.length && ids.length <= 200) {
      q.push("ids=" + encodeURIComponent(ids.join(",")));
    } else {
      if (state.filters && state.filters.weekly) {
        q.push("weekly=" + encodeURIComponent(state.filters.weekly));
      }
      // Server export accepts a single opportunityType (population or demand
      // category). Prefer Market Scope population when narrowed; otherwise pass
      // the demand-category type. Visible-row `ids=` above is the primary path.
      var scopePop =
        state.filters && UI && UI.marketScopeToInventoryPopulation
          ? UI.marketScopeToInventoryPopulation(state.filters.marketScope)
          : "";
      if (
        scopePop &&
        String(scopePop).toUpperCase() !== "ALL_COMMERCIAL_INTELLIGENCE"
      ) {
        q.push("opportunityType=" + encodeURIComponent(scopePop));
      } else if (
        state.filters &&
        state.filters.opportunityType &&
        !/^(SALES_OPPORTUNITIES|COMPETITIVE_MARKET_INTELLIGENCE|ALL_COMMERCIAL_INTELLIGENCE)$/i.test(
          String(state.filters.opportunityType)
        )
      ) {
        q.push(
          "opportunityType=" + encodeURIComponent(state.filters.opportunityType)
        );
      }
      if (state.filters && state.filters.priority) {
        q.push("priority=" + encodeURIComponent(state.filters.priority));
      }
      if (state.filters && state.filters.booking) {
        q.push("booking=" + encodeURIComponent(state.filters.booking));
      }
      if (state.filters && state.filters.segment) {
        q.push("segment=" + encodeURIComponent(state.filters.segment));
      }
      if (state.filters && state.filters.territory) {
        q.push("territory=" + encodeURIComponent(state.filters.territory));
      }
    }
    return (
      "/api/group-demand-intelligence/hotels/" +
      encodeURIComponent(state.hotelId) +
      "/opportunities/export.csv" +
      (q.length ? "?" + q.join("&") : "")
    );
  }

  function founderLifecycleNavHtml() {
    var mode = resolveFounderInventoryMode();
    var finalizedActive = mode === "staged" || mode === "published";
    var researchActive = mode === "research";
    return (
      '<nav class="gdi-lifecycle-nav" aria-label="Opportunity lifecycle" data-gdi-lifecycle-nav="1">' +
      '<button type="button" class="gdi-lifecycle-nav__btn' +
      (finalizedActive ? " is-active" : "") +
      '" data-gdi-lifecycle-view="finalized" aria-pressed="' +
      (finalizedActive ? "true" : "false") +
      '">Finalized Opportunities</button>' +
      '<button type="button" class="gdi-lifecycle-nav__btn' +
      (researchActive ? " is-active" : "") +
      '" data-gdi-lifecycle-view="research" aria-pressed="' +
      (researchActive ? "true" : "false") +
      '">Research Opportunities</button>' +
      "</nav>"
    );
  }

  function founderInventorySummaryHtml() {
    var counts = researchInventoryCounts();
    var staged = finalizedStagedCount();
    var stagedLabel =
      staged == null
        ? "—"
        : state.inventoryEmpty && state.inventoryEmpty.mode === "staged"
          ? "unavailable"
          : String(staged);
    return (
      '<div class="gdi-inventory-summary" data-gdi-inventory-summary="1" role="group" aria-label="Hotel inventory summary">' +
      '<div class="gdi-inventory-summary__metric" data-metric="discovered">' +
      '<span class="gdi-inventory-summary__label">Discovered</span>' +
      '<span class="gdi-inventory-summary__value">' +
      esc(String(counts.discovered)) +
      "</span></div>" +
      '<div class="gdi-inventory-summary__metric" data-metric="qualified">' +
      '<span class="gdi-inventory-summary__label">Qualified Research</span>' +
      '<span class="gdi-inventory-summary__value">' +
      esc(String(counts.qualified)) +
      "</span></div>" +
      '<div class="gdi-inventory-summary__metric" data-metric="staged">' +
      '<span class="gdi-inventory-summary__label">Finalized STAGED</span>' +
      '<span class="gdi-inventory-summary__value">' +
      esc(stagedLabel) +
      "</span></div>" +
      "</div>"
    );
  }

  function researchViewBannerHtml() {
    if (resolveFounderInventoryMode() !== "research") return "";
    return (
      '<div class="gdi-research-view-banner" role="status" data-gdi-research-view-banner="1">' +
      "<p><strong>RESEARCH VIEW — NOT FINALIZED</strong></p>" +
      "<p>These opportunities are research findings and may require additional evidence, contact verification or commercial qualification before they can be presented as finalized sales opportunities.</p>" +
      '<p class="gdi-research-view-banner__actions">' +
      '<button type="button" class="gdi-btn" data-gdi-lifecycle-view="finalized">Return to Finalized Opportunities</button>' +
      "</p></div>"
    );
  }

  function downloadCustomerCsv(e) {
    if (e) e.preventDefault();
    var href = buildExportHref();
    if (!href || !state.hotelId) return;
    fetchFn(href)
      .then(function (r) {
        var ct = String(r.headers.get("content-type") || "").toLowerCase();
        if (!r.ok || ct.indexOf("text/csv") < 0) {
          return r.text().then(function (body) {
            var preview = String(body || "").slice(0, 120);
            throw new Error(
              "CSV export failed (expected text/csv). Got " + (ct || r.status) + ". " + preview
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
        console.error("gdi_csv_export_failed", err);
        alert("Could not download CSV. Please try again or contact Dealality support.");
      });
  }

  function renderBrowseBody(opts) {
    opts = opts || {};
    var rows = filteredSorted();
    var chromeOpts = PC.buildBrowseChromeOptions(state, UI, {
      mode: "auth",
      noun: opts.noun || "Opportunities",
      exportHref: buildExportHref(),
    });
    chromeOpts.exportHref = buildExportHref();
    var chrome = UI.opportunityBrowseChromeHtml(chromeOpts);
    // Customer GDI: Priority + Action Window only — no All/Ready/Watching workflow row.
    return chrome + (opts.lede || "") + renderOpportunityCards(rows);
  }

  function humanizeToken(value) {
    if (!value) return "";
    return String(value)
      .replace(/_/g, " ")
      .toLowerCase()
      .replace(/\b\w/g, function (c) {
        return c.toUpperCase();
      });
  }

  function formatShortDate(iso) {
    if (!iso) return "";
    try {
      var d = new Date(iso);
      if (isNaN(d.getTime())) return "";
      return d.toLocaleDateString("en-US", {
        month: "short",
        day: "numeric",
        year: "numeric",
      });
    } catch (e) {
      return "";
    }
  }

  function eventStamp(ev) {
    if (!ev) return "";
    return (
      ev.validatedAt ||
      ev.reportedAt ||
      ev.completedAt ||
      ev.actionDate ||
      ev.outcomeDate ||
      ev.createdAt ||
      ""
    );
  }

  /** Prefer commercial progression labels over raw lifecycle enums for customer UI. */
  function deriveDisplayStage(current) {
    if (!current) return null;
    if (current.commercialProgression && current.commercialProgression.currentStatusLabel) {
      return current.commercialProgression.currentStatusLabel;
    }
    if (current.latestOutcome && current.latestOutcome.outcomeType) {
      var outLabel =
        (UI &&
          UI.labelGdiOutcomeType &&
          UI.labelGdiOutcomeType(current.latestOutcome.outcomeType)) ||
        null;
      return outLabel || humanizeToken(current.latestOutcome.outcomeType);
    }
    if (current.latestAction && current.latestAction.actionType) {
      return humanizeToken(current.latestAction.actionType);
    }
    if (current.decisionLifecycleStage) {
      return humanizeToken(current.decisionLifecycleStage);
    }
    return null;
  }

  function renderLifecycleSummary(current) {
    if (!current) return "";
    if (
      current.commercialProgression &&
      current.commercialProgression.currentStatusLabel &&
      UI &&
      UI.commercialProgressionHtml
    ) {
      return UI.commercialProgressionHtml(current.commercialProgression);
    }
    var stage = deriveDisplayStage(current);
    var bits = [];
    if (stage) {
      bits.push(
        '<p class="gdi-lifecycle-stage">Current status: <strong>' + esc(stage) + "</strong></p>"
      );
    }
    var hist = [];
    if (current.latestAction && current.latestAction.actionType) {
      var actionLine = "Latest activity: Hotel contacted opportunity";
      if (current.latestAction.actionType !== "CONTACTED") {
        actionLine = "Latest activity: " + humanizeToken(current.latestAction.actionType);
      }
      var ad = formatShortDate(eventStamp(current.latestAction));
      if (ad) actionLine += " · " + ad;
      hist.push(actionLine);
    }
    if (current.latestOutcome && current.latestOutcome.outcomeType) {
      var outLabel =
        current.latestOutcome.outcomeType === "ADDED_TO_SOURCED_PROPERTIES"
          ? "Added to sourced properties"
          : humanizeToken(current.latestOutcome.outcomeType);
      var outLine = "Latest result: " + outLabel;
      var od = formatShortDate(eventStamp(current.latestOutcome));
      if (od) outLine += " · " + od;
      hist.push(outLine);
    } else if (current.latestAction) {
      hist.push("Latest result: Pending");
    }
    if (hist.length) {
      bits.push(
        '<p class="gdi-lifecycle-history">' +
          hist
            .map(function (line) {
              return esc(line);
            })
            .join("<br>") +
          "</p>"
      );
    }
    if (!bits.length) return "";
    return (
      '<div class="gdi-lifecycle-summary" id="gdiLifecycleSummary">' + bits.join("") + "</div>"
    );
  }

  function updateLifecycleSummary(current) {
    var el =
      document.getElementById("gdiCommercialProgression") ||
      document.getElementById("gdiLifecycleSummary");
    if (!el) return;
    var html = renderLifecycleSummary(current);
    if (!html) return;
    var tmp = document.createElement("div");
    tmp.innerHTML = html;
    var next = tmp.firstChild;
    if (next) el.replaceWith(next);
  }

  function startPursuit(opportunityId) {
    setLoading(true, "Starting pursuit\u2026");
    api(
      "/api/group-demand-intelligence/hotels/" +
        encodeURIComponent(state.hotelId) +
        "/opportunities/" +
        encodeURIComponent(opportunityId) +
        "/pursuit/start",
      {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({}),
      }
    )
      .then(function () {
        return loadAll();
      })
      .then(function () {
        openDetail(opportunityId);
      })
      .catch(function (err) {
        showError((err && err.message) || "Could not start pursuit");
      })
      .finally(function () {
        setLoading(false);
      });
  }

  function openPursuitDrawer(pursuitId) {
    drawerTitle.textContent = "Pursuit";
    drawerBody.innerHTML = UI.detailLoadingHtml();
    if (typeof drawer.showModal === "function") drawer.showModal();
    else drawer.setAttribute("open", "open");
    api(
      "/api/group-demand-intelligence/hotels/" +
        encodeURIComponent(state.hotelId) +
        "/pursuits/" +
        encodeURIComponent(pursuitId)
    )
      .then(function (data) {
        drawerTitle.textContent = (data.pursuit && data.pursuit.opportunityTitle) || "Pursuit";
        drawerBody.innerHTML =
          '<section class="gdi-section"><h3>Pursuit</h3>' +
          UI.pursuitPanelHtml(data.pursuit) +
          "</section>";
      })
      .catch(function (err) {
        drawerBody.innerHTML = UI.detailErrorHtml(
          (err && err.message) || "Unable to load pursuit."
        );
      });
  }

  function openDetail(id) {
    var listRow = (state.opportunities || []).find(function (o) {
      return o && o.id === id;
    });
    // Monotonic seq so a slower prior drawer response cannot replace a newer open.
    var detailOpenSeq = (state._detailOpenSeq = (state._detailOpenSeq || 0) + 1);
    function raceTimeout(promise, ms, fallback) {
      return Promise.race([
        promise,
        new Promise(function (resolve) {
          setTimeout(function () {
            resolve(fallback);
          }, ms);
        }),
      ]);
    }
    return PC.openOpportunityDetail({
      UI: UI,
      state: state,
      drawer: drawer,
      drawerTitle: drawerTitle,
      drawerBody: drawerBody,
      drawerClose: drawerClose,
      opportunityId: id,
      mode: "auth",
      api: function (url) {
        // Detail is authoritative for drawer open. Decision/pursuit are optional
        // enrichments and must never hang the drawer into "Failed to fetch".
        return api(url).then(function (data) {
          if (detailOpenSeq !== state._detailOpenSeq) {
            var stale = new Error("STALE_DETAIL_RESPONSE");
            stale.code = "STALE_DETAIL_RESPONSE";
            throw stale;
          }
          var o = data.opportunity || data;
          if (o && listRow) {
            o.canStartPursuit = listRow.canStartPursuit === true;
            o.pursuitId = listRow.pursuitId || null;
          }
          data.opportunity = o;
          var decisionUrl =
            "/api/hotels/" +
            encodeURIComponent(state.hotelId) +
            "/subjects/" +
            encodeURIComponent(id) +
            "/decision?module=GDI";
          var pursuitUrl =
            "/api/group-demand-intelligence/hotels/" +
            encodeURIComponent(state.hotelId) +
            "/opportunities/" +
            encodeURIComponent(id) +
            "/pursuit";
          return Promise.all([
            raceTimeout(
              api(decisionUrl).catch(function () {
                return null;
              }),
              2500,
              null
            ),
            raceTimeout(
              api(pursuitUrl).catch(function () {
                return { pursuit: null };
              }),
              2500,
              { pursuit: null }
            ),
          ]).then(function (extra) {
            if (detailOpenSeq !== state._detailOpenSeq) {
              var stale2 = new Error("STALE_DETAIL_RESPONSE");
              stale2.code = "STALE_DETAIL_RESPONSE";
              throw stale2;
            }
            var decisionBundle = extra[0];
            var pursuitBundle = extra[1];
            var opp = data.opportunity || data;
            if (opp && pursuitBundle && pursuitBundle.pursuit) {
              opp._pursuitDto = pursuitBundle.pursuit;
              opp.pursuitId = pursuitBundle.pursuit.pursuitId;
              opp.pursuitStatus = pursuitBundle.pursuit.pursuitStatus;
              opp.canStartPursuit = false;
            }
            var current =
              (decisionBundle && decisionBundle.current) ||
              (decisionBundle &&
                decisionBundle.decision &&
                decisionBundle.current) ||
              null;
            if (opp && opp.commercialProgression) {
              current = current
                ? Object.assign({}, current, {
                    commercialProgression: opp.commercialProgression,
                  })
                : { commercialProgression: opp.commercialProgression };
            }
            data.opportunity = opp;
            data._decisionCurrent = current;
            return data;
          });
        });
      },
      detailUrl: function (oppId) {
        var path =
          "/api/group-demand-intelligence/hotels/" +
          encodeURIComponent(state.hotelId) +
          "/opportunities/" +
          encodeURIComponent(oppId);
        if (wantsLiveResearchBrowse()) {
          path += "?live=1";
        } else if (wantsPublicationPackagePreview()) {
          path += "?publicationPackage=1&surface=customer";
          // Must match list opportunitiesApiPath — same STAGED package.
          if (wantsExplicitStagedPreview()) {
            path += "&preview=staged";
          }
        }
        return path;
      },
      buildValidationHtml: function (o) {
        if (!UI.customerFeedbackLifecycleHtml) return "";
        var dataCurrent = o && o.commercialProgression ? o.commercialProgression : null;
        return UI.customerFeedbackLifecycleHtml(
          o,
          UI.CUSTOMER_VALIDATION_ENUMS || {},
          {
            canValidate: true,
            canRecordAction: true,
            canRecordOutcome: true,
            canFounderReview: true,
          },
          dataCurrent
        );
      },
      afterDetailRender: function () {
        wireDetailLifecycleControls();
      },
    });
  }

  function wireDetailLifecycleControls() {
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
  }

  function claimLabel(kind) {
    if (kind === "FACT") return "VERIFIED";
    if (kind === "INFERENCE") return "INFERRED";
    if (kind === "ESTIMATED") return "ESTIMATED";
    return kind || "UNKNOWN";
  }

  /** Never show discovery vertical slugs (e.g. assoc_development_district_ass) in drawer. */
  function customerFacingSegment(o) {
    var seg = (o && o.segment) || "";
    if (/^[a-z][a-z0-9]*(?:_[a-z0-9]+){1,8}$/.test(String(seg).trim()) && !/\s/.test(String(seg))) {
      return (
        o.demandSignalTypeLabel || o.opportunityTypeLabel || o.demandFamilyLabel || "Discovered"
      );
    }
    return seg || o.opportunityTypeLabel || "—";
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

  function incrementalBadge(o) {
    var s = o.incrementalValueStatus || "";
    if (s === "NEW_TO_HOTEL" || s === "NEW_OPPORTUNITY") {
      return '<span class="gdi-pill gdi-pill-high">NEW TO HOTEL</span>';
    }
    if (s.indexOf("ALREADY_KNOWN") === 0) {
      return '<span class="gdi-pill gdi-pill-med">ALREADY KNOWN</span>';
    }
    if ((o.sourcingStatus || "").indexOf("HOTEL_CONFIRMED_ALREADY") === 0) {
      return '<span class="gdi-pill gdi-pill-watch">ALREADY SOURCED</span>';
    }
    return "";
  }

  function renderDetail(o, audit, decisionCurrent) {
    // Shared intelligence detail for all hotels (parity with share-app).
    // Preserves feedback / action / outcome controls.
    if (UI && typeof UI.intelligenceDetailHtml === "function") {
      var validationHtml = UI.customerFeedbackLifecycleHtml
        ? UI.customerFeedbackLifecycleHtml(
            o,
            UI.CUSTOMER_VALIDATION_ENUMS || {},
            {
              canValidate: true,
              canRecordAction: true,
              canRecordOutcome: true,
              canFounderReview: true,
            },
            (decisionCurrent && decisionCurrent.commercialProgression) ||
              o.commercialProgression ||
              null
          )
        : "";
      return UI.intelligenceDetailHtml(o, audit, {
        validationHtml: validationHtml,
      });
    }

    var hist = (o.meetingHistory || [])
      .map(function (h) {
        return (
          "<li>" +
          esc(h.year) +
          " — " +
          esc(h.city) +
          " (" +
          esc(claimLabel(h.claimKind)) +
          ")</li>"
        );
      })
      .join("");
    var comps = (audit && audit.hotelFit && audit.hotelFit.components) || {};
    var labels = o.hotelFitComponentLabels || {};
    var sources =
      (UI && UI.sourceListHtml
        ? UI.sourceListHtml(o.sources)
        : (o.sources || [])
            .map(function (s) {
              var label = s.name || s.title || s.url || "";
              var link = s.url
                ? '<a href="' +
                  esc(s.url) +
                  '" target="_blank" rel="noopener">' +
                  esc(label) +
                  "</a>"
                : esc(label);
              return (
                "<li>" +
                link +
                (s.sourceType ? " · " + esc(s.sourceType) : "") +
                (s.supportsFact ? " — supports " + esc(s.supportsFact) : "") +
                (s.date ? " · " + esc(s.date) : "") +
                "</li>"
              );
            })
            .join("")) || "<li>No source links available.</li>";
    var verified = ((o.knownVsEstimated && o.knownVsEstimated.verified) || [])
      .map(function (x) {
        return (
          "<li><strong>" +
          esc(x.field) +
          ":</strong> " +
          esc(x.value) +
          " <em>(" +
          esc(x.status) +
          ")</em></li>"
        );
      })
      .join("");
    var estimated = ((o.knownVsEstimated && o.knownVsEstimated.estimated) || [])
      .map(function (x) {
        return (
          "<li><strong>" +
          esc(x.field) +
          ":</strong> " +
          esc(x.value) +
          " <em>(" +
          esc(x.status) +
          ")</em></li>"
        );
      })
      .join("");
    var inferred = ((o.knownVsEstimated && o.knownVsEstimated.inferred) || [])
      .map(function (x) {
        return (
          "<li><strong>" +
          esc(x.field) +
          ":</strong> " +
          esc(x.value) +
          " <em>(" +
          esc(x.status) +
          ")</em></li>"
        );
      })
      .join("");
    var compsList = (o.competitors || [])
      .map(function (c) {
        return (
          "<li><strong>" +
          esc(c.name) +
          "</strong> · " +
          esc(c.classLabel || c.class) +
          "<br/><span class='gdi-meta-value'>" +
          esc(c.reason || "") +
          "</span></li>"
        );
      })
      .join("");

    return (
      '<div class="gdi-section"><h3>1. Opportunity Summary</h3><p>' +
      esc(o.summaryWhat) +
      "</p><p>" +
      esc(o.organizationName) +
      " · " +
      esc(customerFacingSegment(o)) +
      " · " +
      esc(o.eventStartDate || "TBD") +
      " → " +
      esc(o.eventEndDate || "TBD") +
      "</p></div>" +
      '<div class="gdi-section"><h3>2. Opportunity Type</h3><p><strong>' +
      esc(o.opportunityTypeLabel || o.opportunityType || "—") +
      "</strong></p></div>" +
      '<div class="gdi-section"><h3>3. Venue / Sourcing Status</h3><p><strong>' +
      esc(o.venueSourcingStatusLabel || o.venueSourcingStatus || "—") +
      "</strong></p><p>" +
      esc(o.venueSourcingRationale || "") +
      "</p><p>Venue note: " +
      esc(o.venueStatus || "—") +
      "</p></div>" +
      '<div class="gdi-section"><h3>4. Hotel Opportunity Thesis</h3><p>' +
      esc(o.hotelOpportunityThesis || o.bethesdaWinThesis || "—") +
      "</p></div>" +
      '<div class="gdi-section"><h3>5. Room Demand</h3><p><strong>' +
      esc(o.roomDemandStatusLabel || o.roomDemandStatus || "—") +
      "</strong>" +
      (o.roomDemandConfidence != null ? " · confidence " + esc(o.roomDemandConfidence) : "") +
      "</p><p>" +
      esc(o.roomDemandRationale || "") +
      "</p><p>Attendees: " +
      esc(fmtVal(o.estimatedAttendance)) +
      " · Peak rooms: " +
      esc(fmtVal(o.estimatedPeakRooms)) +
      " · Published peak: " +
      esc(fmtVal(o.publishedPeakRooms)) +
      "</p></div>" +
      '<div class="gdi-section"><h3>6. Why This Hotel?</h3><p>' +
      esc(
        o.summaryWhyHotel ||
          o.fitExplanation ||
          "Insufficient hotel-specific fit evidence after research."
      ) +
      "</p></div>" +
      '<div class="gdi-section"><h3>7. Why Now?</h3><p><strong>' +
      esc(o.bookingWindowLabel || o.bookingWindowStatus) +
      "</strong></p><p>" +
      esc(o.whyNow) +
      "</p></div>" +
      '<div class="gdi-section"><h3>8. Hotel Fit</h3>' +
      '<div class="gdi-score-grid">' +
      '<div class="gdi-score-cell"><div class="n">' +
      esc(o.hotelFitScore) +
      '</div><div class="l">Hotel Fit</div></div>' +
      Object.keys(comps)
        .map(function (k) {
          return (
            '<div class="gdi-score-cell"><div class="n">' +
            esc(comps[k]) +
            '</div><div class="l">' +
            esc(labels[k] || fitLabel(k)) +
            "</div></div>"
          );
        })
        .join("") +
      "</div><p class='gdi-lede'>" +
      esc((audit && audit.hotelFit && audit.hotelFit.explanation) || o.fitExplanation || "") +
      "</p><p class='gdi-lede'>Hotel Fit answers whether the hotel could serve the business — not whether the opportunity is still open.</p></div>" +
      '<div class="gdi-section"><h3>9. Opportunity Qualification</h3><p><strong>' +
      esc(o.opportunityQualificationLabel || o.opportunityQualification || "—") +
      "</strong></p><p>" +
      esc(o.qualificationNotes || "") +
      "</p>" +
      (o.qualificationFailureReasonLabel
        ? "<p>Failure reason: " + esc(o.qualificationFailureReasonLabel) + "</p>"
        : "") +
      "</div>" +
      '<div class="gdi-section"><h3>10. Evidence Confidence <span class="gdi-help" title="' +
      esc(
        o.evidenceConfidenceTooltip ||
          "How confident Dealality is in the facts underlying the opportunity based on source quality, recency, corroboration and how much is verified versus inferred."
      ) +
      '">?</span></h3><div class="gdi-score-grid"><div class="gdi-score-cell"><div class="n">' +
      esc(o.evidenceConfidence) +
      '</div><div class="l">Evidence Confidence</div></div></div><p>' +
      esc(o.evidenceConfidenceExplanation || (audit && audit.evidenceConfidenceExplanation) || "") +
      "</p></div>" +
      '<div class="gdi-section"><h3>11. Event Location</h3><p><strong>' +
      esc(o.eventLocationStatusLabel || o.eventLocationStatus || "—") +
      "</strong></p><p>" +
      esc(o.eventLocationSummary || o.destinationStatus || "—") +
      "</p><p>Demand Territory: " +
      esc(o.demandTerritoryFitLabel || "—") +
      "</p><p>" +
      esc(o.demandTerritoryRationale || "") +
      "</p></div>" +
      '<div class="gdi-section"><h3>12. Meeting / Venue History</h3><ul>' +
      (hist || "<li>None captured</li>") +
      "</ul></div>" +
      '<div class="gdi-section"><h3>13. Reactivation Signals</h3><p><strong>' +
      esc(o.reactivationSignalLabel || o.reactivationSignal || "Unknown") +
      "</strong></p><p>" +
      esc(o.reactivationThesis || "No reactivation thesis.") +
      "</p></div>" +
      '<div class="gdi-section"><h3>14. Competitive Context</h3><p><strong>Likely STR competitor:</strong> ' +
      esc(o.likelyStrCompetitor || "—") +
      "</p><p><strong>Likely group alternative:</strong> " +
      esc(o.likelyGroupCompetitor || "—") +
      "</p><p>" +
      esc(o.competitorRationale || "") +
      "</p><ul>" +
      (compsList || "<li>See hotel STR set and group alternatives in configuration.</li>") +
      "</ul></div>" +
      '<div class="gdi-section"><h3>15. Who Should Sales Contact?</h3>' +
      whoShouldSalesContactHtml(o) +
      "</div>" +
      '<div class="gdi-section"><h3>16. Sources</h3><ul>' +
      sources +
      "</ul><h4>What we know</h4><ul>" +
      (verified || "<li>See evidence table for verified fields.</li>") +
      "</ul><h4>Estimated</h4><ul>" +
      (estimated || "<li>No estimated fields highlighted.</li>") +
      "</ul><h4>Inferred</h4><ul>" +
      (inferred || "<li>No inferred fields highlighted.</li>") +
      "</ul></div>" +
      '<div class="gdi-section"><h3>17. Recommended Action</h3><p>' +
      esc(o.recommendedAction) +
      "</p><p>" +
      esc(o.summaryWhyMatters || "") +
      "</p></div>" +
      '<div class="gdi-customer-lifecycle-host">' +
      (UI.customerFeedbackLifecycleHtml
        ? UI.customerFeedbackLifecycleHtml(
            o,
            UI.CUSTOMER_VALIDATION_ENUMS || {},
            {
              canValidate: true,
              canRecordAction: true,
              canRecordOutcome: true,
              canFounderReview: true,
            },
            (decisionCurrent && decisionCurrent.commercialProgression) ||
              o.commercialProgression ||
              null
          )
        : "") +
      "</div>"
    );
  }

  function render() {
    setLoading(false);
    var s = state.summary || {};

    var body = renderBrowseBody({ noun: "Opportunities" });

    var identity = (state.hotelProfile && state.hotelProfile.identity) || {};
    var hotelName = identity.hotelName || "Hotel";
    var hotel = {
      hotelName: hotelName,
      city: identity.city || null,
      state: identity.state || null,
    };
    // Preserve Tab 2 DOM across full shell re-renders so calendar/form state
    // and loaded data survive Opportunities ↔ Need Times tab switches.
    var preservedPrioritiesRoot = null;
    var existingPriorities = document.getElementById("gdiNeedTimesPrioritiesRoot");
    if (
      existingPriorities &&
      state._prioritiesMountedHotelId === state.hotelId &&
      window.DealalityGdiNeedTimesPrioritiesTab &&
      window.DealalityGdiNeedTimesPrioritiesTab._active
    ) {
      preservedPrioritiesRoot = existingPriorities;
      if (existingPriorities.parentNode) {
        existingPriorities.parentNode.removeChild(existingPriorities);
      }
    }

    var showPriorities = state.tab === "priorities";
    var showPortfolio = state.tab === "portfolio";
    var hideOpportunityChrome = showPriorities || showPortfolio;
    root.className = "gdi-shell dashboard-container aiv-dashboard";
    var inventoryMode = resolveFounderInventoryMode();
    var presentationMode =
      inventoryMode === "research"
        ? "LIVE_RESEARCH"
        : inventoryMode === "staged"
          ? "FOUNDER_STAGED_PREVIEW"
          : "FOUNDER_PUBLICATION_PREVIEW";
    var pubMeta = state.publication || {};
    try {
      if (typeof console !== "undefined" && console.debug) {
        console.debug("[gdi] presentationMode=", presentationMode, {
          inventoryMode: inventoryMode,
          liveResearch: wantsLiveResearchBrowse(),
          packagePreview: wantsPublicationPackagePreview(),
          diag: wantsPresentationModeDiagnostics(),
          opportunityCount: (state.opportunities || []).length,
          publicationVersion: pubMeta.publicationVersion || null,
          contentFingerprintSha256: pubMeta.contentFingerprintSha256 || null,
          previewSource: pubMeta.previewSource || null,
        });
      }
    } catch (eDiag) {
      /* ignore */
    }
    // Inventory mode chrome lives in the app shell sidebar (Research/Staged/
    // Published + compact STAGED↔CURRENT indicator). Do not render a full-width
    // inventory-status banner in the GDI report for any hotel or mode.
    var presentationModeBanner = "";
    if (wantsPresentationModeDiagnostics()) {
      presentationModeBanner =
        '<div id="gdiPresentationModeBanner" class="gdi-presentation-mode-banner" data-gdi-presentation-mode="' +
        presentationMode +
        '" role="status">' +
        (presentationMode === "LIVE_RESEARCH"
          ? "<strong>Diag — Research store</strong> · Internal diagnostic only — not founder STAGED review."
          : presentationMode === "FOUNDER_STAGED_PREVIEW"
            ? "<strong>Diag — STAGED</strong> · Finalized packages only. Fail-closed when missing. Localhost founder default."
            : "<strong>Diag — Published CURRENT</strong> · Same contract as external share.") +
        "</div>";
    }
    root.innerHTML =
      UI.hotelShellHtml({
        mode: "auth",
        badges: {},
      }) +
      presentationModeBanner +
      UI.contentTabsHtml(state.tab, UI.GDI_CUSTOMER_TABS || [UI.GDI_MAIN_TAB]) +
      (hideOpportunityChrome
        ? ""
        : UI.propertyBarHtml({
            mode: "auth",
            hotel: hotel,
            hotelId: state.hotelId,
            hotels: state.hotels,
            hotelNeedDateConfigV1: state.hotelNeedDateConfigV1 || null,
            activeWeekly: state.filters.weekly,
            weeklyCounts: UI.countByWeeklyDelta(state.opportunities),
            activeOpportunityType: state.filters.opportunityType || "",
            opportunityTypeOptions: UI.opportunityTypeFilterOptionsFromCounts(
              UI.opportunityTypeCountsForFilters(state.opportunities, state.filters)
            ),
            activeMarketScope: state.filters.marketScope || "ALL",
            marketScopeCounts: UI.marketScopeCountsForFilters
              ? UI.marketScopeCountsForFilters(
                  state.opportunities,
                  state.filters
                )
              : UI.countByMarketScope
                ? UI.countByMarketScope(state.opportunities)
                : null,
            lastResearchAt: s.lastResearchAt || null,
            lastResearch: formatHistoryDate(s.lastResearchAt),
            runStatus: s.runStatus || null,
            runStatusLabel: UI.customerSafeRunStatusLabel
              ? UI.customerSafeRunStatusLabel(s)
              : "Research Completed",
            lastSourceVerificationAt: s.lastSourceVerificationAt || null,
            lastSourceVerification: formatHistoryDate(s.lastSourceVerificationAt),
            lastPublishedUpdateAt:
              s.lastPublishedUpdateAt || s.lastReviewedPackageImportAt || null,
            lastPublishedUpdate: formatHistoryDate(
              s.lastPublishedUpdateAt || s.lastReviewedPackageImportAt
            ),
            publishedUpdateLabel: "Last Published Update",
            actionHtml: propertyBarActionsHtml(),
          }) +
          founderInventorySummaryHtml() +
          founderLifecycleNavHtml() +
          researchViewBannerHtml()) +
      '<div id="gdiTabBody">' +
      '<div id="gdiTabOpportunities" class="gdi-tab-panel"' +
      (hideOpportunityChrome ? ' hidden aria-hidden="true"' : "") +
      ">" +
      body +
      "</div>" +
      '<div class="gdi-tab-panel gdi-pdi-root" id="gdiPortfolioRoot"' +
      (showPortfolio ? "" : ' hidden aria-hidden="true"') +
      "></div>" +
      '<div class="hnd-page gdi-tab-panel" id="gdiNeedTimesPrioritiesRoot"' +
      (showPriorities ? "" : ' hidden aria-hidden="true"') +
      "></div>" +
      "</div>";

    var prioritiesRoot = document.getElementById("gdiNeedTimesPrioritiesRoot");
    if (preservedPrioritiesRoot && prioritiesRoot) {
      preservedPrioritiesRoot.hidden = !showPriorities;
      preservedPrioritiesRoot.setAttribute("aria-hidden", showPriorities ? "false" : "true");
      prioritiesRoot.parentNode.replaceChild(preservedPrioritiesRoot, prioritiesRoot);
      prioritiesRoot = preservedPrioritiesRoot;
    }

    if (showPortfolio) {
      var portfolioRoot = document.getElementById("gdiPortfolioRoot");
      if (
        window.DealalityGdiPortfolioTab &&
        typeof window.DealalityGdiPortfolioTab.mount === "function"
      ) {
        window.DealalityGdiPortfolioTab.mount(portfolioRoot, {
          onLoading: function (on, message) {
            setLoading(!!on, message || "Loading Portfolio Demand Intelligence…");
          },
          onOpenBrief: function (ref) {
            if (!ref || !ref.hotelId || !ref.opportunityId) return;
            var runOpen = function () {
              return openDetail(ref.opportunityId).catch(function (err) {
                handleLoadFailure(err);
              });
            };
            if (String(state.hotelId || "") === String(ref.hotelId)) {
              runOpen();
              return;
            }
            selectHotel(ref.hotelId)
              .then(function () {
                return runOpen();
              })
              .catch(function (err) {
                handleLoadFailure(err);
              });
          },
        });
      } else if (portfolioRoot) {
        portfolioRoot.innerHTML =
          '<div class="gdi-pdi-banner gdi-pdi-banner--warn" role="alert">Portfolio Demand Intelligence module failed to load.</div>';
      }
    } else if (
      window.DealalityGdiPortfolioTab &&
      typeof window.DealalityGdiPortfolioTab.unmount === "function" &&
      window.DealalityGdiPortfolioTab._active
    ) {
      window.DealalityGdiPortfolioTab.unmount();
    }

    if (showPriorities) {
      if (
        window.DealalityGdiNeedTimesPrioritiesTab &&
        typeof window.DealalityGdiNeedTimesPrioritiesTab.mount === "function"
      ) {
        var isolation = (function () {
          try {
            var params = new URLSearchParams(window.location.search || "");
            var airtable = String(params.get("airtable") || "").trim();
            if (airtable === "0" || airtable === "false") return "portal_v1_test";
            var i = String(params.get("isolation") || "").trim();
            if (i === "portal_v1_test" || i === "1") return "portal_v1_test";
          } catch (e) {}
          return null;
        })();
        var alreadyMounted =
          state._prioritiesMountedHotelId === state.hotelId &&
          window.DealalityGdiNeedTimesPrioritiesTab._active;
        if (!alreadyMounted) {
          window.DealalityGdiNeedTimesPrioritiesTab.mount(prioritiesRoot, {
            hotelId: state.hotelId,
            loading: document.getElementById("gdiLoading"),
            isolation: isolation,
          });
          state._prioritiesMountedHotelId = state.hotelId;
        } else if (prioritiesRoot) {
          prioritiesRoot.hidden = false;
          prioritiesRoot.removeAttribute("aria-hidden");
        }
      } else if (prioritiesRoot) {
        prioritiesRoot.innerHTML =
          '<div class="hnd-banner error" role="alert">Commercial Priorities module failed to load.</div>';
      }
    }

    if (state.tab !== "priorities" && UI.bindNeedDateLegendInteractions) {
      UI.bindNeedDateLegendInteractions(root);
    }

    var hotelSelect = document.getElementById("gdiHotel");
    if (hotelSelect) {
      hotelSelect.addEventListener("change", function () {
        // Keep Research/Staged/Published mode; missing package → compact empty state.
        selectHotel(hotelSelect.value).catch(function (err) {
          handleLoadFailure(err);
        });
      });
    }

    notifyShellHotelContext();

    var weeklySelect = document.getElementById("gdiWeeklyFilter");
    if (weeklySelect) {
      weeklySelect.addEventListener("change", function () {
        state.filters.weekly = weeklySelect.value || "";
        state.filters = UI.reconcileBrowseFilters(state.opportunities, state.filters, "weekly");
        render();
      });
    }

    var typeSelect = document.getElementById("gdiOpportunityTypeFilter");
    if (typeSelect) {
      typeSelect.addEventListener("change", function () {
        state.filters.opportunityType = typeSelect.value || "";
        state.filters = UI.reconcileBrowseFilters(
          state.opportunities,
          state.filters,
          "opportunityType"
        );
        render();
      });
    }

    root.querySelectorAll("[data-gdi-market-scope]").forEach(function (btn) {
      btn.addEventListener("click", function () {
        var next = btn.getAttribute("data-gdi-market-scope") || "ALL";
        if (String(state.filters.marketScope || "ALL") === String(next)) return;
        state.filters.marketScope = next;
        state.filters.competitiveStatus = "ALL_COMPETITIVE";
        state.filters = UI.reconcileBrowseFilters(
          state.opportunities,
          state.filters,
          "marketScope"
        );
        render();
      });
    });

    // Event delegation — Reset View / lifecycle must survive full root.innerHTML re-renders.
    if (!root._gdiResetViewDelegated) {
      root._gdiResetViewDelegated = true;
      root.addEventListener("click", function (e) {
        var t = e.target && e.target.closest ? e.target.closest("#gdiResetViewBtn") : null;
        if (!t) return;
        e.preventDefault();
        resetBrowseView();
      });
    }
    if (!root._gdiLifecycleDelegated) {
      root._gdiLifecycleDelegated = true;
      root.addEventListener("click", function (e) {
        var recovery =
          e.target && e.target.closest
            ? e.target.closest(
                '[data-gdi-recovery="view-research"], [data-gdi-lifecycle-view]'
              )
            : null;
        if (!recovery) return;
        var next = "";
        if (recovery.getAttribute("data-gdi-recovery") === "view-research") {
          next = "research";
        } else {
          next = String(recovery.getAttribute("data-gdi-lifecycle-view") || "")
            .trim()
            .toLowerCase();
        }
        if (next === "research") {
          e.preventDefault();
          applyFounderInventoryMode("research");
        } else if (next === "finalized") {
          e.preventDefault();
          applyFounderInventoryMode(
            resolveFounderInventoryMode() === "published" ? "published" : "staged"
          );
        } else {
          return;
        }
        setLoading(true, UI.DEFAULT_PAGE_LOADING_MESSAGE);
        loadAll().catch(function (err) {
          handleLoadFailure(err);
        });
      });
    }

    root.querySelectorAll(".section-nav-item[data-tab]").forEach(function (btn) {
      btn.addEventListener("click", function () {
        state.tab = btn.getAttribute("data-tab");
        render();
      });
    });

    wireBrowseControls();
    if (state.inventoryEmpty) {
      var recoveryOpts = state.inventoryEmpty.recovery || {};
      wireRecoveryActions({
        hasCurrentPublication: recoveryOpts.hasCurrentPublication === true,
      });
    }

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

  applyEmbedMode();

  if (drawerClose) {
    drawerClose.addEventListener("click", function () {
      if (typeof drawer.close === "function") drawer.close();
      else drawer.removeAttribute("open");
    });
  }
  if (drawer) {
    drawer.addEventListener("close", function () {
      var el = state.drawerReturnFocusEl;
      state.drawerReturnFocusEl = null;
      if (el && typeof el.focus === "function") {
        try {
          el.focus();
        } catch (e) {
          /* ignore */
        }
      }
    });
  }

  // Canonical Surfe / contact reveal — shared with comparison preview.
  if (
    drawerBody &&
    window.DealalityGdiContactReveal &&
    typeof window.DealalityGdiContactReveal.bindContactRevealClicks === "function"
  ) {
    window.DealalityGdiContactReveal.bindContactRevealClicks(drawerBody, {
      getHotelId: function () {
        return state.hotelId || "";
      },
      setLoadingToast: function (on, message) {
        setLoading(on, message || "Retrieving contact details\u2026");
      },
      fetchFn: contactRevealFetch,
      plainFetch: plainFetch,
      surface: "drawer",
    });
  }

  drawerBody &&
    drawerBody.addEventListener("click", function (e) {
      var t = e.target;
      var retryBtn = t && t.closest ? t.closest("[data-gdi-detail-retry]") : null;
      if (retryBtn) {
        var retryId = retryBtn.getAttribute("data-gdi-detail-retry");
        if (retryId) openDetail(retryId);
        return;
      }

      function showSaveOk(statusId, message) {
        var el = document.getElementById(statusId);
        if (!el) return;
        el.hidden = false;
        el.textContent = message;
      }

      function setLastSaved(elId, iso) {
        var el = document.getElementById(elId);
        if (!el) return;
        var label =
          UI && UI.formatHotelFeedbackLastSavedLabel
            ? UI.formatHotelFeedbackLastSavedLabel(iso || null)
            : iso
              ? "Last saved: " + iso
              : "Not yet submitted";
        el.textContent = label;
        if (iso) el.setAttribute("data-iso", iso);
        else el.removeAttribute("data-iso");
      }

      function nextUnvalidatedAuthId(currentId) {
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

      if (t && (t.id === "gdiFbSave" || t.id === "gdiSvSave" || t.id === "gdiSvSaveNext")) {
        var id = t.getAttribute("data-id");
        var thenNext = t.id === "gdiSvSaveNext";
        var statusId = t.id === "gdiFbSave" ? "gdiFbSaveStatus" : "gdiSvStatus";
        var familiarityEl =
          document.getElementById("gdiSvFamiliarity") ||
          document.getElementById("gdiFbFamiliarity");
        var commercialEl =
          document.getElementById("gdiSvCommercial") || document.getElementById("gdiFbCommercial");
        var personEl = document.getElementById("gdiSvPerson");
        var emailEl = document.getElementById("gdiSvEmail");
        var phoneEl = document.getElementById("gdiSvPhone");
        var founderEl = document.getElementById("gdiSvFounderReview");
        var noteEl =
          document.getElementById("gdiSvNote") || document.getElementById("gdiFbComment");
        var formRoot = document.getElementById("gdiShareValidation");
        var familiarity = familiarityEl ? familiarityEl.value : "";
        var commercialStatus = commercialEl ? commercialEl.value : "";
        var comment = noteEl ? noteEl.value : "";
        var founderDecision = founderEl ? founderEl.value || "" : "";
        if (!familiarity && !commercialStatus && !founderDecision) {
          showSaveOk(statusId, "Pick familiarity or commercial value.");
          return;
        }
        if (t.disabled) return;
        t.disabled = true;
        var saveBtn = document.getElementById("gdiSvSave");
        var nextBtn = document.getElementById("gdiSvSaveNext");
        if (saveBtn) saveBtn.disabled = true;
        if (nextBtn) nextBtn.disabled = true;
        showSaveOk(statusId, "Saving…");
        api(
          "/api/group-demand-intelligence/hotels/" +
            encodeURIComponent(state.hotelId) +
            "/opportunities/" +
            encodeURIComponent(id) +
            "/customer-validation",
          {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
              familiarityStatus: familiarity || null,
              commercialValue: commercialStatus || null,
              contactPersonAssessment: personEl ? personEl.value || null : null,
              emailAssessment: emailEl ? emailEl.value || null : null,
              phoneAssessment: phoneEl ? phoneEl.value || null : null,
              note: comment || "",
              founderReviewDecision: founderDecision || null,
              expectedHotelValidationUpdatedAt: formRoot
                ? formRoot.getAttribute("data-hotel-validation-updated-at") || null
                : null,
              validator: "AUTH_REVIEWER",
            }),
          }
        )
          .then(function (data) {
            var ts =
              (data && data.hotelValidationUpdatedAt) ||
              (data && data.validation && data.validation.hotelValidationUpdatedAt) ||
              (data && data.validation && data.validation.validatedAt) ||
              null;
            if (formRoot && ts) {
              formRoot.setAttribute("data-hotel-validation-updated-at", ts);
            }
            setLastSaved("gdiSvLastSaved", ts);
            for (var i = 0; i < (state.opportunities || []).length; i++) {
              if (state.opportunities[i].id === id && data && data.validation) {
                state.opportunities[i].shareValidation = data.validation;
                break;
              }
            }
            showSaveOk(statusId, "✓ Validation saved");
            return api(
              "/api/hotels/" +
                encodeURIComponent(state.hotelId) +
                "/subjects/" +
                encodeURIComponent(id) +
                "/decision?module=GDI"
            ).catch(function () {
              return null;
            });
          })
          .then(function (bundle) {
            if (saveBtn) saveBtn.disabled = false;
            if (nextBtn) nextBtn.disabled = false;
            t.disabled = false;
            if (bundle && bundle.current) updateLifecycleSummary(bundle.current);
            if (thenNext) {
              var nid = nextUnvalidatedAuthId(id);
              if (nid) openDetail(nid);
              else showSaveOk(statusId, "✓ Saved — all reviewed");
            }
          })
          .catch(function (err) {
            if (saveBtn) saveBtn.disabled = false;
            if (nextBtn) nextBtn.disabled = false;
            t.disabled = false;
            showSaveOk(statusId, err.message || "Failed");
          });
        return;
      }

      function ensureThenRecord(opportunityId, kind, payload, btn, statusId, okMessage) {
        var opp = (state.opportunities || []).find(function (o) {
          return o.id === opportunityId;
        }) || {
          id: opportunityId,
          title: opportunityId,
          priority: "MEDIUM_PRIORITY",
          recommendedAction: "Recorded from GDI detail",
        };
        if (btn.disabled) return;
        btn.disabled = true;
        showSaveOk(statusId, "Saving…");
        api("/api/hotels/" + encodeURIComponent(state.hotelId) + "/decisions/ensure-gdi", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ opportunity: opp }),
        })
          .then(function (ensured) {
            var decisionId =
              (ensured.decision && ensured.decision.decisionId) ||
              (ensured.decision &&
                ensured.decision.decision &&
                ensured.decision.decision.decisionId);
            if (!decisionId) throw new Error("decision_missing");
            var path =
              kind === "action"
                ? "/api/decisions/" + encodeURIComponent(decisionId) + "/actions"
                : "/api/decisions/" + encodeURIComponent(decisionId) + "/outcomes";
            return api(path, {
              method: "POST",
              headers: { "Content-Type": "application/json" },
              body: JSON.stringify(Object.assign({ hotelId: state.hotelId }, payload)),
            });
          })
          .then(function (result) {
            btn.disabled = false;
            showSaveOk(statusId, okMessage);
            if (kind === "action") {
              var actionTs =
                (result && result.salesActionUpdatedAt) ||
                (result &&
                  result.current &&
                  result.current.commercialProgression &&
                  result.current.commercialProgression.salesActionUpdatedAt) ||
                null;
              setLastSaved("gdiDoActionLastSaved", actionTs);
              var actionStep = document.querySelector("[data-gdi-lifecycle-action]");
              if (actionStep && actionTs) {
                actionStep.setAttribute("data-sales-action-updated-at", actionTs);
              }
            } else {
              var outcomeTs =
                (result && result.salesOutcomeUpdatedAt) ||
                (result &&
                  result.current &&
                  result.current.commercialProgression &&
                  result.current.commercialProgression.salesOutcomeUpdatedAt) ||
                null;
              setLastSaved("gdiDoOutcomeLastSaved", outcomeTs);
              var outcomeStep = document.querySelector("[data-gdi-lifecycle-outcome]");
              if (outcomeStep && outcomeTs) {
                outcomeStep.setAttribute("data-sales-outcome-updated-at", outcomeTs);
              }
            }
            if (result && result.current) updateLifecycleSummary(result.current);
          })
          .catch(function (err) {
            btn.disabled = false;
            showSaveOk(statusId, err.message || "Failed");
          });
      }

      if (t && t.id === "gdiDoActionSave") {
        var actionEl = document.getElementById("gdiDoAction");
        var actionType = actionEl ? actionEl.value : "";
        if (!actionType) {
          showSaveOk("gdiDoActionSaveStatus", "Select action...");
          return;
        }
        var actionStepEl = document.querySelector("[data-gdi-lifecycle-action]");
        ensureThenRecord(
          t.getAttribute("data-id"),
          "action",
          {
            actionType: actionType,
            expectedSalesActionUpdatedAt: actionStepEl
              ? actionStepEl.getAttribute("data-sales-action-updated-at") || null
              : null,
          },
          t,
          "gdiDoActionSaveStatus",
          "✓ Action saved"
        );
      }
      if (t && t.id === "gdiDoOutcomeSave") {
        var outcomeEl = document.getElementById("gdiDoOutcome");
        var lossEl = document.getElementById("gdiDoLoss");
        var outcomeType = outcomeEl ? outcomeEl.value : "";
        if (!outcomeType) {
          showSaveOk("gdiDoOutcomeSaveStatus", "Select outcome...");
          return;
        }
        var outcomeStepEl = document.querySelector("[data-gdi-lifecycle-outcome]");
        ensureThenRecord(
          t.getAttribute("data-id"),
          "outcome",
          {
            outcomeType: outcomeType,
            outcomeReason: outcomeType === "LOST" && lossEl && lossEl.value ? lossEl.value : null,
            expectedSalesOutcomeUpdatedAt: outcomeStepEl
              ? outcomeStepEl.getAttribute("data-sales-outcome-updated-at") || null
              : null,
          },
          t,
          "gdiDoOutcomeSaveStatus",
          "✓ Outcome saved"
        );
      }
    });

  /** Deep-link: open the exact opportunity drawer after hotel inventory loads. */
  function openOpportunityFromQueryOnce() {
    var params = readGdiQueryParams();
    var opportunityId = String(params.get("opportunityId") || "").trim();
    if (!opportunityId) return Promise.resolve(null);
    if (state._openedOpportunityFromQuery === opportunityId) {
      return Promise.resolve(null);
    }
    state._openedOpportunityFromQuery = opportunityId;
    return openDetail(opportunityId).catch(function (err) {
      showError(
        (err && err.message) ||
          "Could not open opportunity from deep link: " + opportunityId,
      );
    });
  }

  loadAll()
    .then(function () {
      return openOpportunityFromQueryOnce();
    })
    .catch(function (err) {
      handleLoadFailure(err);
    });
})();
