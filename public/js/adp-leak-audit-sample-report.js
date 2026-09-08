/**
 * ADP-native (BAS/DRS) sample AI Demand Leak Audit renderer.
 */
(function () {
  "use strict";

  var evidenceById = {};
  var LOGO_FALLBACK =
    "https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/69c166836c109719f94e055e_Dealality%20Logo%20(4)%20(1).png";

  function esc(s) {
    return String(s == null ? "" : s)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;");
  }

  function showError(message) {
    var loadingEl = document.getElementById("alaReportLoading");
    var reportEl = document.getElementById("alaReport");
    var err = document.getElementById("alaReportError");
    if (loadingEl) {
      loadingEl.hidden = true;
      loadingEl.style.display = "none";
    }
    if (reportEl) {
      reportEl.hidden = true;
      reportEl.style.display = "none";
    }
    if (err) {
      err.hidden = false;
      err.style.display = "block";
    }
    document.getElementById("alaReportErrorMsg").textContent =
      message || "This sample report could not be loaded.";
  }

  function showReport() {
    var loadingEl = document.getElementById("alaReportLoading");
    var reportEl = document.getElementById("alaReport");
    var err = document.getElementById("alaReportError");
    if (loadingEl) {
      loadingEl.hidden = true;
      loadingEl.style.display = "none";
    }
    if (err) {
      err.hidden = true;
      err.style.display = "none";
    }
    if (reportEl) {
      reportEl.hidden = false;
      reportEl.style.display = "block";
    }
  }

  function inlineList(items) {
    if (!items || !items.length) return "—";
    return items.map(esc).join(" · ");
  }

  function evidenceLinksHtml(ids, label) {
    var ui = window.AdpLeakAuditSharedUi;
    if (ui && ui.evidenceLinksHtml) {
      if (!ids || !ids.length) return "";
      return ids
        .map(function (id) {
          var ev = evidenceById[id];
          var btnLabel = label || (ev && ev.linkLabel) || "View evidence";
          return ui.evidenceLinksHtml([id], btnLabel);
        })
        .join("");
    }
    if (!ids || !ids.length) return "";
    return ids
      .map(function (id) {
        var ev = evidenceById[id];
        var btnLabel = label || (ev && ev.linkLabel) || "View evidence";
        return (
          '<button type="button" class="drs-link" data-ala-evidence="' +
          esc(id) +
          '">' +
          esc(btnLabel) +
          "</button>"
        );
      })
      .join("");
  }

  function openEvidence(id) {
    var ev = evidenceById[id];
    var drawer = document.getElementById("alaEvidenceDrawer");
    var body = document.getElementById("alaEvidenceBody");
    var title = document.getElementById("alaEvidenceTitle");
    if (!ev || !drawer || !body) return;
    if (
      ev.fullPromptText ||
      ev.promptText ||
      ev.promptId ||
      ev.propertyId ||
      ev.matchedHotelId ||
      ev.airtableId
    ) {
      return;
    }

    title.textContent = ev.evidenceTypeLabel || "Evidence";
    var ui = window.AdpLeakAuditSharedUi;
    body.innerHTML =
      ui && ui.renderEvidenceBody
        ? ui.renderEvidenceBody(ev)
        : "<p>" + esc(ev.excerpt || "") + "</p>";

    drawer.hidden = false;
    drawer.setAttribute("aria-hidden", "false");
    document.body.classList.add("ala-drawer-open");
  }

  function closeEvidence() {
    var drawer = document.getElementById("alaEvidenceDrawer");
    if (!drawer) return;
    drawer.hidden = true;
    drawer.setAttribute("aria-hidden", "true");
    document.body.classList.remove("ala-drawer-open");
  }

  function bindEvidenceClicks(root) {
    (root || document).addEventListener("click", function (e) {
      var btn = e.target.closest("[data-ala-evidence]");
      if (btn) {
        e.preventDefault();
        openEvidence(btn.getAttribute("data-ala-evidence"));
        return;
      }
      if (e.target.closest("[data-ala-close-evidence]")) closeEvidence();
    });
    document.addEventListener("keydown", function (e) {
      if (e.key === "Escape") closeEvidence();
    });
  }

  function renderKpis(signals) {
    var el = document.getElementById("alaKpis");
    var ui = window.AdpLeakAuditSharedUi;
    if (ui && ui.renderKpiBand) {
      ui.renderKpiBand(el, signals, {
        className: "aiv-kpi-row ala-adp-kpi-row",
        tourById: {
          ai_consideration: "ai-consideration",
          scenario_presence: "scenario-presence",
          reality_coverage: "reality-coverage",
          leisure_advantage: "demand-territories",
        },
      });
      return;
    }
    el.innerHTML = "";
  }

  function renderRank(rank) {
    var title = document.getElementById("alaRankTitle");
    var list = document.getElementById("alaRankList");
    var note = document.getElementById("alaRankNote");
    if (!rank) return;
    if (title) title.textContent = rank.title || "Competitor Displacement Rank";
    if (note) note.textContent = rank.note || "";
    if (list) {
      list.innerHTML = (rank.rows || [])
        .map(function (row) {
          return (
            '<li class="ala-rank-band__item">' +
            '<span class="ala-rank-band__badge">' +
            esc(row.badge || "#" + row.rank + " observed displacement signal") +
            "</span>" +
            '<span class="drs-muted" style="color:#aeb9e1">' +
            esc(row.signalLabel || "") +
            "</span><br/>" +
            evidenceLinksHtml(row.evidenceIds, row.evidenceLinkLabel) +
            "</li>"
          );
        })
        .join("");
    }
  }

  function renderCompetitors(blocks) {
    var el = document.getElementById("alaCompetitors");
    el.innerHTML = (blocks || [])
      .slice(0, 2)
      .map(function (c) {
        return (
          '<article class="drs-evidence drs-avoid-break">' +
          '<p class="drs-evidence__title">' +
          esc(c.competitorName) +
          (c.rankBadge ? " · " + esc(c.rankBadge) : "") +
          "</p>" +
          '<p class="drs-evidence__row"><strong>Displacements:</strong> ' +
          esc(String(c.displacementCount)) +
          "</p>" +
          '<p class="drs-evidence__row"><strong>Demand segment:</strong> ' +
          esc(c.demandSegment || "—") +
          "</p>" +
          (c.evidenceExcerpt
            ? '<p class="drs-evidence__row">“' + esc(c.evidenceExcerpt) + '”</p>'
            : "") +
          '<p class="drs-evidence__row"><strong>What this may indicate:</strong> ' +
          esc(c.interpretation || "") +
          "</p>" +
          evidenceLinksHtml(c.evidenceIds, c.evidenceLinkLabel) +
          "</article>"
        );
      })
      .join("");
  }

  function renderFixes(fixes) {
    var el = document.getElementById("alaFixes");
    var ui = window.AdpLeakAuditSharedUi;
    if (ui && ui.renderActionItems) {
      ui.renderActionItems(el, fixes);
      return;
    }
    el.innerHTML = "";
  }

  function renderWho(who) {
    var el = document.getElementById("alaWho");
    var ui = window.AdpLeakAuditSharedUi;
    if (ui && ui.renderWhoDoes) {
      ui.renderWhoDoes(el, who);
      return;
    }
    if (el) el.innerHTML = "";
  }

  function renderChips(report) {
    var chips = [
      ["Run date", report.runDate || "—"],
      ["Providers", (report.providersUsed || []).join(", ") || "—"],
      ["Report type", report.reportType || "Limited diagnostic"],
      ["Priority source", report.prioritySource || "inferred_from_results"],
      ["Status", report.reportStatus || "Sample"],
    ];
    document.getElementById("alaChips").innerHTML = chips
      .map(function (c) {
        return (
          '<span class="drs-pill">' +
          esc(c[0]) +
          ": " +
          esc(c[1]) +
          "</span>"
        );
      })
      .join("");
  }

  function bindHowToReadGuide() {
    if (
      window.AdpLeakAuditHowToReadGuide &&
      typeof window.AdpLeakAuditHowToReadGuide.init === "function"
    ) {
      window.AdpLeakAuditHowToReadGuide.init({ portfolio: false });
    }
  }

  function render(report) {
    showReport();
    evidenceById = {};
    (report.evidenceLibrary || []).forEach(function (ev) {
      if (ev && ev.id) evidenceById[ev.id] = ev;
    });

    var logo =
      (window.DealalityReportPrintChrome &&
        window.DealalityReportPrintChrome.DEALALITY_LOGO_URL) ||
      LOGO_FALLBACK;
    var logoEl = document.getElementById("alaCoverLogo");
    if (logoEl) logoEl.src = logo;

    document.getElementById("alaProductFamily").textContent =
      (window.AdpLeakAuditSharedUi &&
        window.AdpLeakAuditSharedUi.PRODUCT_FAMILY) ||
      report.productFamily ||
      "Dealality AI Demand Positioning (ADP)";
    document.getElementById("alaSampleBanner").textContent =
      report.sampleBanner || "Sample AI Demand Leak Audit · Limited Diagnostic";
    document.getElementById("alaSampleSubtext").textContent =
      report.sampleSubtext ||
      "Sample for demonstration only — not a live client deliverable";
    document.getElementById("alaConfidential").textContent =
      report.confidentialLabel || "Confidential · For recipient only";
    document.getElementById("alaTitle").textContent =
      report.hotelName || report.title || "AI Demand Leak Audit";
    document.getElementById("alaCoverMeta").textContent =
      (report.runDate || "") +
      " · " +
      ((report.providersUsed || []).join(" · ") || "Monitored providers") +
      " · Inferred priority";
    document.getElementById("alaDisclaimer").textContent =
      report.limitedDiagnosticDisclaimer || "";

    renderChips(report);
    renderKpis(report.executiveSignals);
    document.getElementById("alaBottomLine").textContent = report.bottomLineSummary || "";
    document.getElementById("alaBottomPriority").textContent =
      report.bottomLinePriority || "";

    var leak = report.inferredDemandLeak || {};
    document.getElementById("alaInferredLeak").textContent =
      leak.summary || report.biggestDemandLeak || "";
    document.getElementById("alaInferredDisplacement").textContent =
      leak.displacementLine || "";
    document.getElementById("alaInferredEvidenceLinks").innerHTML =
      evidenceLinksHtml(leak.evidenceIds, leak.evidenceLinkLabel);

    renderRank(report.competitorDisplacementRank);
    renderCompetitors(report.competitorDisplacement);
    var noteEl = document.getElementById("alaCompetitorNote");
    if (noteEl) {
      noteEl.textContent = report.competitorPriorityNote || "";
      noteEl.hidden = !report.competitorPriorityNote;
    }

    renderFixes(report.fixes);
    renderWho(report.whoDoesTheWork);

    var actionsHeading = document.getElementById("alaActionsHeading");
    if (
      actionsHeading &&
      window.AdpLeakAuditSharedUi &&
      window.AdpLeakAuditSharedUi.ACTION_SECTION_TITLE
    ) {
      actionsHeading.textContent =
        window.AdpLeakAuditSharedUi.ACTION_SECTION_TITLE;
    }

    var cover = document.querySelector(".bas-cover-page");
    if (window.AdpLeakAuditSharedUi) {
      window.AdpLeakAuditSharedUi.ensureCoverFootnotes(cover);
      window.AdpLeakAuditSharedUi.ensureReportFooter(
        document.getElementById("alaReport"),
        "AI Demand Leak Audit"
      );
      window.AdpLeakAuditSharedUi.bindInfoTooltips(document);
    }

    var nextEl = document.getElementById("alaNext");
    var nextText = report.recommendedNextStep || "";
    if (nextEl) {
      nextEl.innerHTML = nextText
        .split(/\n\s*\n/)
        .map(function (para) {
          return "<p>" + esc(para.replace(/\n/g, " ").trim()) + "</p>";
        })
        .filter(function (html) {
          return html !== "<p></p>";
        })
        .join("");
    }

    bindHowToReadGuide();
  }

  async function init() {
    bindEvidenceClicks(document);
    try {
      var res = await fetch("/api/adp-leak-audit/sample-report");
      var data = await res.json();
      if (!res.ok || !data.ok || !data.report) {
        showError((data && (data.message || data.error)) || "Sample not found.");
        return;
      }
      var report = data.report;
      if (
        report.fullPromptText ||
        report.promptText ||
        report.propertyId ||
        report.matchedHotelId ||
        report.airtableId
      ) {
        showError("Report failed safety checks.");
        return;
      }
      var missing = [];
      if (!report.whoDoesTheWork) missing.push("whoDoesTheWork");
      if (!report.executiveSignals || !report.executiveSignals.length) {
        missing.push("executiveSignals");
      }
      if (!report.howToRead) missing.push("howToRead");
      if (!report.inferredDemandLeak) missing.push("inferredDemandLeak");
      if (!report.evidenceLibrary || !report.evidenceLibrary.length) {
        missing.push("evidenceLibrary");
      }
      if (
        !report.competitorDisplacementRank ||
        !report.competitorDisplacementRank.rows ||
        !report.competitorDisplacementRank.rows.length
      ) {
        missing.push("competitorDisplacementRank");
      }
      if (!report.fixes || report.fixes.length < 3) missing.push("fixes");
      if (missing.length) {
        showError(
          "Sample report is missing required sections: " + missing.join(", ")
        );
        return;
      }
      if (
        report.layoutMode &&
        report.layoutMode !== "compressed_sales_diagnostic"
      ) {
        showError("Sample report is not in compressed sales-diagnostic mode.");
        return;
      }
      render(report);
    } catch (err) {
      showError(err.message || "Unable to load sample report.");
    }
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }
})();
