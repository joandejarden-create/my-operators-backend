/**
 * Founder-only Sheraton Guadalajara Expo side-by-side drawer comparison.
 * Uses the real DealalityGdiUi.intelligenceDetailHtml renderer in both panels.
 * Loading feedback = canonical Dealality toast (Brand AI / GDI / ADP lineage).
 */
(function () {
  var UI = window.DealalityGdiUi;
  if (!UI || typeof UI.intelligenceDetailHtml !== "function") {
    document.body.innerHTML =
      '<div class="gdi-cmp-error">DealalityGdiUi missing — cannot render comparison.</div>';
    return;
  }

  var HOTEL_ID = "recCAGqkERSXgiku4";
  var CMP_LOADING_MESSAGE = "Loading opportunity comparison\u2026";
  var TOAST_MIN_MS = 320;
  var leftEl = document.getElementById("gdiCmpLeft");
  var rightEl = document.getElementById("gdiCmpRight");
  var leftTitleEl = document.getElementById("gdiCmpLeftTitle");
  var rightTitleEl = document.getElementById("gdiCmpRightTitle");
  var selectEl = document.getElementById("gdiCmpOpportunity");
  var metaEl = document.getElementById("gdiCmpMeta");
  var summaryEl = document.getElementById("gdiCmpSummary");
  var highlightEl = document.getElementById("gdiCmpHighlight");
  var syncEl = document.getElementById("gdiCmpSyncScroll");
  var loadingEl = document.getElementById("gdiCmpLoading");
  var panelsEl = document.getElementById("gdiCmpPanels");
  var syncing = false;
  var lastPayload = null;
  var loadSeq = 0;
  var toastShownAt = 0;
  var toastHideTimer = null;
  var contactRevealBound = false;

  function bindContactReveal() {
    if (contactRevealBound) return;
    var Reveal = window.DealalityGdiContactReveal;
    if (
      !Reveal ||
      typeof Reveal.bindContactRevealClicks !== "function" ||
      !panelsEl
    ) {
      return;
    }
    contactRevealBound = true;
    Reveal.bindContactRevealClicks(panelsEl, {
      getHotelId: function () {
        return HOTEL_ID;
      },
      setLoadingToast: function (on, message) {
        setToast(on, message || "Retrieving contact details\u2026");
      },
      // Comparison preview: mock by default when ?gdiContactRevealMock=1;
      // live Surfe requires authorizeLiveProvider (never auto-spend).
      fetchFn: function (url, init) {
        return fetch(url, init);
      },
      plainFetch: function (url, init) {
        return fetch(url, init);
      },
      surface: "comparison",
    });
  }

  function drawerTitle(opportunity) {
    return (
      (opportunity &&
        (opportunity.drawerHeaderTitle ||
          opportunity.displayTitle ||
          opportunity.title)) ||
      "Opportunity"
    );
  }

  function qs(name) {
    try {
      return new URLSearchParams(window.location.search).get(name) || "";
    } catch (e) {
      return "";
    }
  }

  function setQueryOpportunity(id) {
    try {
      var u = new URL(window.location.href);
      u.searchParams.set("hotelId", HOTEL_ID);
      if (id) u.searchParams.set("opportunityId", id);
      else u.searchParams.delete("opportunityId");
      window.history.replaceState({}, "", u.toString());
    } catch (e) {
      /* ignore */
    }
  }

  function apiUrl(opportunityId) {
    var path =
      "/api/group-demand-intelligence/hotels/" +
      encodeURIComponent(HOTEL_ID) +
      "/drawer-comparison";
    if (opportunityId) {
      path += "?opportunityId=" + encodeURIComponent(opportunityId);
    }
    return path;
  }

  function fetchComparison(opportunityId) {
    var controller = typeof AbortController !== "undefined" ? new AbortController() : null;
    var timeoutId = null;
    if (controller) {
      timeoutId = setTimeout(function () {
        try {
          controller.abort();
        } catch (e) {
          /* ignore */
        }
      }, 45000);
    }
    return fetch(apiUrl(opportunityId), {
      credentials: "same-origin",
      headers: { Accept: "application/json" },
      signal: controller ? controller.signal : undefined,
    })
      .then(function (res) {
        return res.json().then(function (body) {
          if (!res.ok || !body || body.ok === false) {
            var err = new Error(
              (body && (body.message || body.error)) ||
                "drawer_comparison_failed"
            );
            err.body = body;
            err.status = res.status;
            throw err;
          }
          return body;
        });
      })
      .finally(function () {
        if (timeoutId) clearTimeout(timeoutId);
      });
  }

  function renderDrawer(opportunity) {
    return UI.intelligenceDetailHtml(opportunity, null, {
      validationHtml: "",
      comparisonPreview: true,
    });
  }

  function dismissToastNow() {
    if (toastHideTimer) {
      clearTimeout(toastHideTimer);
      toastHideTimer = null;
    }
    UI.setLoadingToast(loadingEl, false);
  }

  function setToast(on, message) {
    if (toastHideTimer) {
      clearTimeout(toastHideTimer);
      toastHideTimer = null;
    }
    if (on) {
      toastShownAt = Date.now();
      UI.setLoadingToast(loadingEl, true, message || CMP_LOADING_MESSAGE);
      return;
    }
    // Minimum visible time only for very fast responses (avoid flicker).
    var elapsed = toastShownAt ? Date.now() - toastShownAt : TOAST_MIN_MS;
    var wait = Math.max(0, TOAST_MIN_MS - elapsed);
    if (wait > 0) {
      toastHideTimer = setTimeout(function () {
        toastHideTimer = null;
        UI.setLoadingToast(loadingEl, false);
      }, wait);
    } else {
      UI.setLoadingToast(loadingEl, false);
    }
  }

  function clearPanelsForLoad() {
    // Avoid stale opportunity content and avoid generic "Loading…" copy.
    // Feedback lives in the canonical top-right Dealality toast.
    leftEl.innerHTML = "";
    rightEl.innerHTML = "";
    leftEl.setAttribute("aria-busy", "true");
    rightEl.setAttribute("aria-busy", "true");
    if (leftTitleEl) leftTitleEl.textContent = "Opportunity";
    if (rightTitleEl) rightTitleEl.textContent = "Opportunity";
    if (summaryEl) summaryEl.textContent = "";
  }

  function sectionKey(section) {
    return (
      section.getAttribute("data-section-id") ||
      (section.querySelector("h3") && section.querySelector("h3").textContent) ||
      ""
    );
  }

  function sectionText(section) {
    var clone = section.cloneNode(true);
    clone.querySelectorAll("details.gdi-section-sources").forEach(function (d) {
      d.remove();
    });
    return String(clone.textContent || "")
      .replace(/\s+/g, " ")
      .trim();
  }

  function applyHighlight(leftRoot, rightRoot, enabled) {
    rightRoot.classList.toggle("gdi-cmp-highlight", enabled);
    leftRoot.classList.remove("gdi-cmp-highlight");
    rightRoot.querySelectorAll(".gdi-section").forEach(function (sec) {
      sec.classList.remove("gdi-cmp-changed", "gdi-cmp-added");
    });
    rightRoot
      .querySelectorAll(".gdi-inline-details.gdi-section-sources")
      .forEach(function (d) {
        d.classList.remove("gdi-cmp-added-sources");
      });
    if (!enabled) return;

    var leftMap = {};
    leftRoot.querySelectorAll(".gdi-section").forEach(function (sec) {
      leftMap[sectionKey(sec)] = sectionText(sec);
    });
    var leftSourceCount = leftRoot.querySelectorAll(
      "details.gdi-section-sources"
    ).length;

    rightRoot.querySelectorAll(".gdi-section").forEach(function (sec) {
      var key = sectionKey(sec);
      var leftText = leftMap[key];
      var rightText = sectionText(sec);
      if (leftText == null) {
        sec.classList.add("gdi-cmp-added");
      } else if (leftText !== rightText) {
        sec.classList.add("gdi-cmp-changed");
      }
    });

    var rightSources = rightRoot.querySelectorAll(
      "details.gdi-section-sources"
    );
    if (rightSources.length > leftSourceCount) {
      rightSources.forEach(function (d) {
        d.classList.add("gdi-cmp-added-sources");
      });
    }
  }

  function bindSyncScroll() {
    function wire(a, b) {
      a.addEventListener(
        "scroll",
        function () {
          if (!syncEl.checked || syncing) return;
          syncing = true;
          var maxA = a.scrollHeight - a.clientHeight;
          var maxB = b.scrollHeight - b.clientHeight;
          var ratio = maxA > 0 ? a.scrollTop / maxA : 0;
          b.scrollTop = ratio * Math.max(maxB, 0);
          syncing = false;
        },
        { passive: true }
      );
    }
    wire(leftEl, rightEl);
    wire(rightEl, leftEl);
  }

  function fillSelector(payload) {
    var selectedId = payload.selected && payload.selected.id;
    selectEl.innerHTML = "";
    (payload.selector || []).forEach(function (item, idx) {
      var opt = document.createElement("option");
      opt.value = item.id;
      var badge = item.inCurrentStaged
        ? "STAGED"
        : item.inLocalResearch
          ? "LOCAL"
          : "HIST";
      opt.textContent =
        idx +
        1 +
        ". [" +
        badge +
        "] " +
        (item.title || item.id) +
        (item.organizationName ? " — " + item.organizationName : "");
      if (item.id === selectedId) opt.selected = true;
      selectEl.appendChild(opt);
    });
  }

  function renderSummary(payload) {
    var c = payload.counts || {};
    var ch = payload.changeSummary || {};
    var sel = payload.selected || {};
    var fields = (ch.changedFields || [])
      .map(function (f) {
        return f.label + " (" + f.kind + ")";
      })
      .join(", ");
    metaEl.textContent =
      "Inventory " +
      (c.unionDeduped || 0) +
      " deduped · STAGED " +
      (c.currentStagedPackage || 0) +
      " · local " +
      (c.localResearchStore || 0) +
      " · historical-62 " +
      (c.historicalFullE2ePackage || 0);
    summaryEl.innerHTML =
      "<strong>" +
      UI.esc(sel.title || "") +
      "</strong> · status " +
      UI.esc(sel.status || "") +
      " · source " +
      UI.esc(sel.contentSource || "") +
      " · changed fields " +
      (ch.changedFieldCount || 0) +
      (fields ? " — " + UI.esc(fields) : "") +
      " · Brief v2 " +
      (ch.briefV2Applied ? "on" : "off") +
      " · Fact Selection " +
      (ch.factSelectionApplied ? "on" : "off");
  }

  function paint(payload) {
    lastPayload = payload;
    fillSelector(payload);
    renderSummary(payload);
    setQueryOpportunity(payload.selected && payload.selected.id);
    if (leftTitleEl) leftTitleEl.textContent = drawerTitle(payload.before);
    if (rightTitleEl) rightTitleEl.textContent = drawerTitle(payload.after);
    leftEl.innerHTML = renderDrawer(payload.before);
    rightEl.innerHTML = renderDrawer(payload.after);
    leftEl.setAttribute("aria-busy", "false");
    rightEl.setAttribute("aria-busy", "false");
    applyHighlight(leftEl, rightEl, highlightEl.checked);
    leftEl.scrollTop = 0;
    rightEl.scrollTop = 0;
    setToast(false);
  }

  function paintError(err, seq) {
    if (seq !== loadSeq) return;
    var msg =
      (err && err.body && (err.body.message || err.body.error)) ||
      (err && err.message) ||
      "Failed to load comparison";
    if (/abort/i.test(String(msg))) {
      msg = "Comparison request timed out. Try again.";
    }
    var errorHtml =
      typeof UI.detailErrorHtml === "function"
        ? UI.detailErrorHtml(msg)
        : '<div class="gdi-cmp-error" role="alert">' + UI.esc(msg) + "</div>";
    leftEl.innerHTML = errorHtml;
    rightEl.innerHTML =
      '<div class="gdi-error gdi-detail-error" role="status">' +
      "<p>Comparison unavailable for this opportunity.</p></div>";
    leftEl.setAttribute("aria-busy", "false");
    rightEl.setAttribute("aria-busy", "false");
    metaEl.textContent = "Unable to load comparison";
    summaryEl.textContent = "";
    if (leftTitleEl) leftTitleEl.textContent = "Unavailable";
    if (rightTitleEl) rightTitleEl.textContent = "Unavailable";
    setToast(false);
  }

  function load(opportunityId) {
    var seq = ++loadSeq;
    clearPanelsForLoad();
    setToast(true, CMP_LOADING_MESSAGE);
    return fetchComparison(opportunityId)
      .then(function (payload) {
        if (seq !== loadSeq) return null;
        paint(payload);
        return payload;
      })
      .catch(function (err) {
        if (seq !== loadSeq) return null;
        paintError(err, seq);
        return null;
      });
  }

  selectEl.addEventListener("change", function () {
    load(selectEl.value);
  });
  highlightEl.addEventListener("change", function () {
    if (!lastPayload) return;
    applyHighlight(leftEl, rightEl, highlightEl.checked);
  });

  bindSyncScroll();
  bindContactReveal();

  var initialOpp = qs("opportunityId");
  var hotelQs = qs("hotelId");
  if (hotelQs && hotelQs !== HOTEL_ID) {
    metaEl.textContent =
      "This page is locked to Sheraton Guadalajara Expo (" + HOTEL_ID + ").";
  }
  // Prefer an opportunity that still shows Find Email & Phone for smoke
  // (EUROGUSS LEFT retains enrichment CTA; RIGHT may already have public channels).
  load(initialOpp || undefined);
})();
