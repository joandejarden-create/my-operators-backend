/**
 * Admin Resources — ADP Action Plan (detailed Management Action Agenda).
 * Gate: AI_DEMAND_REVIEW_ACTION_AGENDA_ADMIN_ACTION_PLAN_PARITY
 */
(function () {
  "use strict";

  var CATALOG_API = "/api/admin/ai-demand-positioning/action-plan-catalog";
  var EXPORT_API = "/api/admin/ai-demand-positioning/action-plan-export";
  var state = {
    properties: [],
    selectedId: null,
    preview: null,
    externalLinks: null,
  };

  function authFetch(url, opts) {
    opts = opts || {};
    var auth = window.DealalityMemberstackAuth;
    if (auth && typeof auth.authFetch === "function") {
      return auth.authFetch(url, opts);
    }
    return fetch(url, Object.assign({ credentials: "same-origin" }, opts));
  }

  function copyText(text) {
    if (navigator.clipboard && navigator.clipboard.writeText) {
      return navigator.clipboard.writeText(text);
    }
    return Promise.resolve();
  }

  function setExternalMeta(text) {
    var el = document.getElementById("aapExternalMeta");
    if (el) el.textContent = text;
  }

  function updateExternalButtons() {
    var links = state.externalLinks;
    var adpOk = !!(links && links.adp && links.adp.available && links.adp.url);
    var gdiOk = !!(links && links.gdi && links.gdi.available && links.gdi.url);
    var adpOpen = document.getElementById("aapAdpOpen");
    var adpCopy = document.getElementById("aapAdpCopy");
    var gdiOpen = document.getElementById("aapGdiOpen");
    var gdiCopy = document.getElementById("aapGdiCopy");
    if (adpOpen) adpOpen.disabled = !adpOk;
    if (adpCopy) adpCopy.disabled = !adpOk;
    if (gdiOpen) gdiOpen.disabled = !gdiOk;
    if (gdiCopy) gdiCopy.disabled = !gdiOk;
    if (!state.selectedId) {
      setExternalMeta("Select a hotel to load external client URLs.");
      return;
    }
    var parts = [];
    parts.push(adpOk ? "ADP client URL ready" : "ADP: " + ((links && links.adp && links.adp.reason) || "unavailable"));
    parts.push(gdiOk ? "GDI client URL ready" : "GDI: " + ((links && links.gdi && links.gdi.reason) || "unavailable"));
    setExternalMeta(parts.join(" · "));
  }

  async function loadExternalLinks(propertyId) {
    state.externalLinks = null;
    updateExternalButtons();
    if (!propertyId) return;
    try {
      var res = await authFetch(
        "/api/admin/ai-demand/hotels/" +
          encodeURIComponent(propertyId) +
          "/external-client-links"
      );
      var json = await res.json();
      if (!res.ok || !json.ok) {
        setExternalMeta("Could not load external client URLs.");
        return;
      }
      state.externalLinks = json;
      updateExternalButtons();
    } catch (err) {
      setExternalMeta("Could not load external client URLs.");
    }
  }

  function esc(s) {
    return String(s == null ? "" : s)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;");
  }

  function showDenied(message) {
    var loadingEl = document.getElementById("supportGateLoading");
    var deniedEl = document.getElementById("supportGateDenied");
    var contentEl = document.getElementById("supportGateContent");
    if (window.SupportAdminGate) {
      window.SupportAdminGate.hidePageLoading(loadingEl);
    } else if (loadingEl) {
      loadingEl.hidden = true;
    }
    if (contentEl) contentEl.hidden = true;
    if (deniedEl) {
      deniedEl.hidden = false;
      var msg = deniedEl.querySelector("[data-gate-message]");
      if (msg && message) msg.textContent = message;
    }
  }

  function renderCounts() {
    var el = document.getElementById("aapCounts");
    var ok = state.properties.filter(function (p) {
      return p.ok;
    }).length;
    var actions = state.properties.reduce(function (n, p) {
      return n + (p.actionCount || 0);
    }, 0);
    el.innerHTML =
      '<div class="adr-count"><span class="adr-count__n">' +
      ok +
      '</span><span class="adr-count__l">Published hotels</span></div>' +
      '<div class="adr-count"><span class="adr-count__n">' +
      actions +
      '</span><span class="adr-count__l">Total Management Actions</span></div>';
  }

  function fillPropertySelect() {
    var sel = document.getElementById("aapProperty");
    var rows = state.properties.slice().sort(function (a, b) {
      return String(a.propertyName || "").localeCompare(String(b.propertyName || ""));
    });
    sel.innerHTML =
      '<option value="">Select a published hotel…</option>' +
      rows
        .map(function (p) {
          var label =
            (p.propertyName || p.propertyId) +
            " · " +
            (p.actionCount || 0) +
            " actions" +
            (p.ok ? "" : " (unavailable)");
          return (
            '<option value="' +
            esc(p.propertyId) +
            '"' +
            (p.ok ? "" : " disabled") +
            ">" +
            esc(label) +
            "</option>"
          );
        })
        .join("");
  }

  function renderPreview() {
    var meta = document.getElementById("aapMeta");
    var tbody = document.getElementById("aapTableBody");
    var dl = document.getElementById("aapDownload");
    var pack = state.preview;
    if (!pack) {
      meta.innerHTML = "<p>Select a published ADP hotel to preview the Management Action Agenda.</p>";
      tbody.innerHTML = '<tr><td colspan="12">No property selected.</td></tr>';
      dl.disabled = true;
      var pdfBtn0 = document.getElementById("aapViewPdf");
      if (pdfBtn0) pdfBtn0.disabled = true;
      return;
    }
    meta.innerHTML =
      "<p><strong>" +
      esc(pack.propertyName) +
      "</strong> · Period <code>" +
      esc(pack.periodId || "—") +
      "</code> · Review <code>" +
      esc(pack.reviewId || "—") +
      "</code> · Generated " +
      esc(pack.reviewGeneratedAt || "—") +
      " · <strong>" +
      esc(pack.actionCount) +
      "</strong> Management Actions</p>" +
      '<p class="adr-subtitle">Detailed Action Plan from AI Demand Performance Review · ' +
      esc(pack.gate || "") +
      " · Live Priority Actions (customer summary): " +
      esc(pack.livePriorityActionCount || 0) +
      "</p>";

    if (!pack.actions || !pack.actions.length) {
      tbody.innerHTML =
        '<tr><td colspan="12">No Management Action Agenda on the current Performance Review.</td></tr>';
    } else {
      tbody.innerHTML = pack.actions
        .map(function (a, i) {
          function joinList(v) {
            if (Array.isArray(v)) return v.join(" · ");
            return v || "—";
          }
          return (
            "<tr>" +
            "<td>" +
            (a.order || i + 1) +
            "</td>" +
            "<td>" +
            esc(a.priority || "—") +
            "</td>" +
            "<td>" +
            esc(a.status || "—") +
            "</td>" +
            "<td>" +
            esc(a.title || "—") +
            "</td>" +
            "<td>" +
            esc(a.owner || "—") +
            "</td>" +
            "<td>" +
            esc(a.dueDate || "—") +
            "</td>" +
            "<td>" +
            esc(a.observedIssue || "—") +
            "</td>" +
            "<td>" +
            esc(joinList(a.implementationSteps) || a.recommendedAction || "—") +
            "</td>" +
            "<td>" +
            esc(joinList(a.targetSources)) +
            "</td>" +
            "<td>" +
            esc(joinList(a.definitionOfDone)) +
            "</td>" +
            "<td>" +
            esc(a.expectedSignal || "—") +
            "</td>" +
            "<td>" +
            esc(a.nextMonitoringCheck || "—") +
            "</td>" +
            "</tr>"
          );
        })
        .join("");
    }
    dl.disabled = false;
    var pdfBtn = document.getElementById("aapViewPdf");
    if (pdfBtn) {
      pdfBtn.disabled = !(pack.reviewId || state.selectedId);
      pdfBtn.setAttribute("data-review-id", pack.reviewId || "");
    }
  }

  async function loadCatalog() {
    var res = await authFetch(CATALOG_API);
    var json = await res.json();
    if (!res.ok || !json.ok) {
      throw new Error(json.message || json.error || "catalog_failed");
    }
    state.properties = json.properties || [];
    renderCounts();
    fillPropertySelect();
  }

  async function loadPreview() {
    var propertyId = document.getElementById("aapProperty").value;
    if (!propertyId) {
      state.preview = null;
      state.selectedId = null;
      renderPreview();
      loadExternalLinks(null);
      return;
    }
    state.selectedId = propertyId;
    loadExternalLinks(propertyId);
    var url =
      EXPORT_API +
      "?propertyId=" +
      encodeURIComponent(propertyId) +
      "&preview=1&format=json";
    var res = await authFetch(url);
    var json = await res.json();
    if (!res.ok || !json.ok) {
      throw new Error(json.message || json.error || "preview_failed");
    }
    state.preview = json;
    renderPreview();
  }

  async function downloadCsv() {
    var propertyId =
      state.selectedId || document.getElementById("aapProperty").value;
    if (!propertyId) return;
    var url =
      EXPORT_API +
      "?propertyId=" +
      encodeURIComponent(propertyId) +
      "&format=csv";
    var res = await authFetch(url);
    if (!res.ok) {
      var errJson = null;
      try {
        errJson = await res.json();
      } catch (_e) {
        /* ignore */
      }
      throw new Error(
        (errJson && (errJson.message || errJson.error)) ||
          "download_failed_" + res.status
      );
    }
    var blob = await res.blob();
    if (!blob || blob.size < 8) {
      throw new Error("empty_download");
    }
    var cd = res.headers.get("Content-Disposition") || "";
    var match = /filename=\"([^\"]+)\"/i.exec(cd);
    var filename =
      (match && match[1]) ||
      (state.preview && state.preview.filenameHint) ||
      "ADP_Action_Plan.csv";
    var a = document.createElement("a");
    a.href = URL.createObjectURL(blob);
    a.download = filename;
    document.body.appendChild(a);
    a.click();
    setTimeout(function () {
      URL.revokeObjectURL(a.href);
      a.remove();
    }, 0);
  }

  async function init() {
    async function start() {
      var loadingEl = document.getElementById("supportGateLoading");
      if (window.SupportAdminGate && loadingEl) {
        window.SupportAdminGate.setPageLoadingMessage(
          loadingEl,
          "Loading ADP Action Plan…"
        );
      }
      try {
        await loadCatalog();
        if (window.SupportAdminGate && loadingEl) {
          window.SupportAdminGate.hidePageLoading(loadingEl);
        }
        var content = document.getElementById("supportGateContent");
        if (content) content.hidden = false;
        renderPreview();
        try {
          var stored = sessionStorage.getItem("dealality_adp_admin_property");
          if (stored && document.getElementById("aapProperty")) {
            document.getElementById("aapProperty").value = stored;
            await loadPreview();
          }
        } catch (_e) {}
      } catch (err) {
        showDenied(
          err && err.message ? err.message : "Unable to load ADP Action Plan."
        );
        return;
      }

      document.getElementById("aapLoadPreview").addEventListener("click", function () {
        loadPreview().catch(function (e) {
          alert(e.message);
        });
      });
      document.getElementById("aapProperty").addEventListener("change", function () {
        try {
          sessionStorage.setItem(
            "dealality_adp_admin_property",
            document.getElementById("aapProperty").value || ""
          );
        } catch (_e2) {}
        loadPreview().catch(function (e) {
          alert(e.message);
        });
      });
      function wireExternal(openId, copyId, kind) {
        var openBtn = document.getElementById(openId);
        var copyBtn = document.getElementById(copyId);
        if (openBtn) {
          openBtn.addEventListener("click", function () {
            var pack = state.externalLinks && state.externalLinks[kind];
            if (pack && pack.url) window.open(pack.url, "_blank");
          });
        }
        if (copyBtn) {
          copyBtn.addEventListener("click", function () {
            var pack = state.externalLinks && state.externalLinks[kind];
            if (!pack || !pack.url) return;
            copyText(pack.url).then(function () {
              setExternalMeta(
                "Copied " + (kind === "adp" ? "ADP" : "GDI") + " External Client URL."
              );
            });
          });
        }
      }
      wireExternal("aapAdpOpen", "aapAdpCopy", "adp");
      wireExternal("aapGdiOpen", "aapGdiCopy", "gdi");
      updateExternalButtons();
      document.getElementById("aapDownload").addEventListener("click", function () {
        downloadCsv().catch(function (e) {
          alert(e.message);
        });
      });
      var viewPdf = document.getElementById("aapViewPdf");
      if (viewPdf) {
        viewPdf.addEventListener("click", function () {
          var propertyId = state.selectedId || "";
          if (!propertyId) {
            alert("Select a property first.");
            return;
          }
          var pdfUrl =
            "/api/admin/ai-demand-positioning/current-report-pdf/" +
            encodeURIComponent(propertyId);
          authFetch(pdfUrl)
            .then(function (res) {
              if (res.ok) return res.blob().then(function (blob) {
                return { blob: blob, regenerated: false };
              });
              if (res.status !== 404) {
                throw new Error("Current ADP PDF not available (" + res.status + ").");
              }
              // Generate-on-miss for current published ADP PDF
              return authFetch(pdfUrl + "/generate", {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({ propertyId: propertyId }),
              }).then(function (genRes) {
                if (!genRes.ok) {
                  throw new Error("ADP PDF generate failed (" + genRes.status + ").");
                }
                return authFetch(pdfUrl).then(function (r2) {
                  if (!r2.ok) throw new Error("ADP PDF missing after generate.");
                  return r2.blob().then(function (blob) {
                    return { blob: blob, regenerated: true };
                  });
                });
              });
            })
            .then(function (pack) {
              window.open(URL.createObjectURL(pack.blob), "_blank");
            })
            .catch(function (e) {
              alert(e.message || "Unable to open ADP PDF.");
            });
        });
      }
    }

    if (window.__ADP_ADMIN_WORKSPACE_AUTH__) {
      await start();
      return;
    }
    if (document.getElementById("adaTabActionPlan")) {
      window.addEventListener(
        "dealality-adp-admin-ready",
        function () {
          start();
        },
        { once: true }
      );
      return;
    }
    if (!window.SupportAdminGate) {
      showDenied("Unable to load admin gate.");
      return;
    }
    var allowed = await window.SupportAdminGate.requireAdpMonthlyReviewAdmin({
      contentId: "supportGateContent",
    });
    if (!allowed) return;
    await start();
  }

  init();
})();
