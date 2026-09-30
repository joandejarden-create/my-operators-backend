/**
 * AI Demand Admin — GDI Reports tab
 * Generate / view / download hotel-specific GDI PDFs.
 */
(function () {
  "use strict";

  var catalog = [];
  var selectedId = "";
  var externalLinks = null;

  function authFetch(url, opts) {
    opts = opts || {};
    if (window.DealalityMemberstackAuth && window.DealalityMemberstackAuth.authFetch) {
      return window.DealalityMemberstackAuth.authFetch(url, opts);
    }
    return fetch(url, opts);
  }

  function $(id) {
    return document.getElementById(id);
  }

  function setMeta(html) {
    var el = $("gdiRptMeta");
    if (el) el.innerHTML = html;
  }

  function setExternalMeta(html) {
    var el = $("gdiRptExternalMeta");
    if (el) el.innerHTML = html;
  }

  function copyText(text) {
    if (navigator.clipboard && navigator.clipboard.writeText) {
      return navigator.clipboard.writeText(text);
    }
    return new Promise(function (resolve, reject) {
      try {
        var ta = document.createElement("textarea");
        ta.value = text;
        document.body.appendChild(ta);
        ta.select();
        document.execCommand("copy");
        ta.remove();
        resolve();
      } catch (e) {
        reject(e);
      }
    });
  }

  function updateExternalButtons() {
    var adpOk = !!(externalLinks && externalLinks.adp && externalLinks.adp.available && externalLinks.adp.url);
    var gdiOk = !!(externalLinks && externalLinks.gdi && externalLinks.gdi.available && externalLinks.gdi.url);
    if ($("gdiRptAdpOpen")) $("gdiRptAdpOpen").disabled = !adpOk;
    if ($("gdiRptAdpCopy")) $("gdiRptAdpCopy").disabled = !adpOk;
    if ($("gdiRptGdiOpen")) $("gdiRptGdiOpen").disabled = !gdiOk;
    if ($("gdiRptGdiCopy")) $("gdiRptGdiCopy").disabled = !gdiOk;
    if (!selectedId) {
      setExternalMeta("Select a hotel to load external client URLs.");
      return;
    }
    var parts = [];
    if (adpOk) parts.push("ADP client URL ready");
    else parts.push("ADP: " + ((externalLinks && externalLinks.adp && externalLinks.adp.reason) || "unavailable"));
    if (gdiOk) parts.push("GDI client URL ready");
    else parts.push("GDI: " + ((externalLinks && externalLinks.gdi && externalLinks.gdi.reason) || "unavailable"));
    setExternalMeta(parts.join(" · "));
  }

  async function loadExternalLinks() {
    externalLinks = null;
    updateExternalButtons();
    if (!selectedId) return;
    try {
      var res = await authFetch(
        "/api/admin/ai-demand/hotels/" +
          encodeURIComponent(selectedId) +
          "/external-client-links"
      );
      var json = await res.json();
      if (!res.ok || !json.ok) {
        setExternalMeta("Could not load external client URLs.");
        return;
      }
      externalLinks = json;
      updateExternalButtons();
    } catch (err) {
      setExternalMeta("Could not load external client URLs: " + String(err.message || err));
    }
  }

  function updateButtons() {
    var row = catalog.find(function (h) {
      return h.hotelId === selectedId;
    });
    var available = !!(row && row.available);
    var pdfReady = !!(row && row.pdfReady);
    if ($("gdiRptGenerate")) $("gdiRptGenerate").disabled = !available;
    if ($("gdiRptView")) $("gdiRptView").disabled = !pdfReady && !available;
    if ($("gdiRptDownload")) $("gdiRptDownload").disabled = !pdfReady && !available;
    if (!selectedId) {
      setMeta("<p>Select a hotel with Group &amp; Demand Intelligence data to generate or view its GDI PDF report.</p>");
      return;
    }
    if (!available) {
      setMeta(
        "<p><strong>" +
          (row && row.displayName ? row.displayName : selectedId) +
          "</strong>: No customer-ready GDI report is currently available for this hotel." +
          (row && row.reason ? " (" + row.reason + ")" : "") +
          "</p>"
      );
      return;
    }
    setMeta(
      "<p><strong>" +
        (row.displayName || selectedId) +
        "</strong> — Ready " +
        (row.ready || 0) +
        ", action set " +
        (row.actionSet || 0) +
        ", watch " +
        (row.watch || 0) +
        ". PDF: " +
        (pdfReady ? "READY" : "not generated yet") +
        ".</p>"
    );
  }

  function renderTable() {
    var body = $("gdiRptTableBody");
    if (!body) return;
    if (!catalog.length) {
      body.innerHTML = "<tr><td colspan='6'>No GDI hotels found.</td></tr>";
      return;
    }
    body.innerHTML = catalog
      .map(function (h) {
        return (
          "<tr data-hotel-id='" +
          h.hotelId +
          "'>" +
          "<td>" +
          (h.displayName || h.hotelId) +
          "</td>" +
          "<td>" +
          (h.available ? h.ready || 0 : "—") +
          "</td>" +
          "<td>" +
          (h.available ? h.actionSet || 0 : "—") +
          "</td>" +
          "<td>" +
          (h.available ? h.watch || 0 : "—") +
          "</td>" +
          "<td>" +
          (h.pdfReady ? "READY" : "—") +
          "</td>" +
          "<td>" +
          (h.available ? "Available" : "Unavailable") +
          "</td>" +
          "</tr>"
        );
      })
      .join("");
  }

  function fillSelect() {
    var sel = $("gdiRptHotel");
    if (!sel) return;
    sel.innerHTML =
      '<option value="">Select hotel…</option>' +
      catalog
        .map(function (h) {
          return (
            '<option value="' +
            h.hotelId +
            '">' +
            (h.displayName || h.hotelId) +
            (h.available ? "" : " (unavailable)") +
            "</option>"
          );
        })
        .join("");
    if (selectedId) sel.value = selectedId;
  }

  async function loadCatalog() {
    var res = await authFetch("/api/admin/group-demand-intelligence/reports");
    var json = await res.json();
    if (!res.ok || !json.ok) throw new Error((json && json.error) || "catalog_failed");
    catalog = json.hotels || [];
    var counts = $("gdiRptCounts");
    if (counts) {
      var avail = catalog.filter(function (h) {
        return h.available;
      }).length;
      counts.textContent =
        catalog.length + " GDI hotels · " + avail + " report-available · " +
        catalog.filter(function (h) {
          return h.pdfReady;
        }).length +
        " PDF ready";
    }
    fillSelect();
    renderTable();
    updateButtons();
  }

  async function openPdf(download) {
    if (!selectedId) return;
    var url =
      "/api/admin/group-demand-intelligence/hotels/" +
      encodeURIComponent(selectedId) +
      "/report-pdf" +
      (download ? "?download=1" : "?generate=1");
    setMeta("<p>Preparing PDF…</p>");
    var res = await authFetch(url);
    if (!res.ok) {
      var err = {};
      try {
        err = await res.json();
      } catch (_e) {}
      setMeta(
        "<p>Could not open PDF: " +
          (err.reason || err.error || res.status) +
          "</p>"
      );
      return;
    }
    var blob = await res.blob();
    var filename =
      res.headers.get("X-GDI-PDF-Filename") || "Dealality_GDI_Report.pdf";
    var objectUrl = URL.createObjectURL(blob);
    if (download) {
      var a = document.createElement("a");
      a.href = objectUrl;
      a.download = filename;
      a.click();
    } else {
      window.open(objectUrl, "_blank");
    }
    await loadCatalog();
  }

  async function generate() {
    if (!selectedId) return;
    setMeta("<p>Generating GDI PDF… this may take a minute.</p>");
    if ($("gdiRptGenerate")) $("gdiRptGenerate").disabled = true;
    try {
      var res = await authFetch(
        "/api/admin/group-demand-intelligence/hotels/" +
          encodeURIComponent(selectedId) +
          "/report-pdf/generate",
        { method: "POST", headers: { "Content-Type": "application/json" }, body: "{}" }
      );
      var json = await res.json();
      if (!res.ok || !json.ok) {
        setMeta(
          "<p>Generate failed: " +
            (json.reason || json.message || json.error || res.status) +
            "</p>"
        );
        return;
      }
      setMeta(
        "<p>Generated <strong>" +
          (json.filename || "PDF") +
          "</strong> (" +
          (json.byteLength || 0) +
          " bytes" +
          (json.pageCountHint != null ? ", ~" + json.pageCountHint + " pages" : "") +
          ").</p>"
      );
      await loadCatalog();
    } finally {
      updateButtons();
    }
  }

  function wire() {
    var sel = $("gdiRptHotel");
    if (sel) {
      sel.addEventListener("change", function () {
        selectedId = sel.value || "";
        updateButtons();
        loadExternalLinks();
      });
    }
    if ($("gdiRptGenerate")) $("gdiRptGenerate").addEventListener("click", generate);
    if ($("gdiRptView"))
      $("gdiRptView").addEventListener("click", function () {
        openPdf(false);
      });
    if ($("gdiRptDownload"))
      $("gdiRptDownload").addEventListener("click", function () {
        openPdf(true);
      });
    if ($("gdiRptAdpOpen"))
      $("gdiRptAdpOpen").addEventListener("click", function () {
        if (externalLinks && externalLinks.adp && externalLinks.adp.url) {
          window.open(externalLinks.adp.url, "_blank");
        }
      });
    if ($("gdiRptGdiOpen"))
      $("gdiRptGdiOpen").addEventListener("click", function () {
        if (externalLinks && externalLinks.gdi && externalLinks.gdi.url) {
          window.open(externalLinks.gdi.url, "_blank");
        }
      });
    if ($("gdiRptAdpCopy"))
      $("gdiRptAdpCopy").addEventListener("click", function () {
        var url = externalLinks && externalLinks.adp && externalLinks.adp.url;
        if (!url) return;
        copyText(url).then(function () {
          setExternalMeta("Copied ADP External Client URL.");
        });
      });
    if ($("gdiRptGdiCopy"))
      $("gdiRptGdiCopy").addEventListener("click", function () {
        var url = externalLinks && externalLinks.gdi && externalLinks.gdi.url;
        if (!url) return;
        copyText(url).then(function () {
          setExternalMeta("Copied GDI External Client URL.");
        });
      });
  }

  async function boot() {
    wire();
    window.addEventListener("dealality-adp-admin-ready", function () {
      loadCatalog().catch(function (err) {
        setMeta("<p>Failed to load GDI report catalog: " + String(err.message || err) + "</p>");
      });
    });
    if (window.__ADP_ADMIN_WORKSPACE_AUTH__) {
      loadCatalog().catch(function (err) {
        setMeta("<p>Failed to load GDI report catalog: " + String(err.message || err) + "</p>");
      });
    }
  }

  boot();
})();
