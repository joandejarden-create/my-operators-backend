/**
 * ADP-native (BAS/DRS) portfolio sample roll-up renderer.
 */
(function () {
  "use strict";

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
      message || "Unavailable.";
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

  function listHtml(items) {
    return (
      "<ul>" +
      (items || [])
        .map(function (i) {
          return "<li>" + esc(i) + "</li>";
        })
        .join("") +
      "</ul>"
    );
  }

  function renderNext(text) {
    var nextEl = document.getElementById("alaNext");
    if (!nextEl) return;
    nextEl.innerHTML = String(text || "")
      .split(/\n\s*\n/)
      .map(function (para) {
        return "<p>" + esc(para.replace(/\n/g, " ").trim()) + "</p>";
      })
      .filter(function (html) {
        return html !== "<p></p>";
      })
      .join("");
  }

  function kpiWide(id) {
    return (
      id === "top_leaking_segment" ||
      id === "top_competitor" ||
      id === "top_action"
    );
  }

  var PORTFOLIO_KPI_TOUR = {
    hotels_reviewed: "ai-consideration",
    strong_visibility: "scenario-presence",
  };

  function bindHowToReadGuide() {
    if (
      window.AdpLeakAuditHowToReadGuide &&
      typeof window.AdpLeakAuditHowToReadGuide.init === "function"
    ) {
      window.AdpLeakAuditHowToReadGuide.init({ portfolio: true });
    }
  }

  var PORTFOLIO_KPI_TOUR = {
    hotels_reviewed: "ai-consideration",
    strong_visibility: "scenario-presence",
  };

  function bindHowToReadGuide() {
    if (
      window.AdpLeakAuditHowToReadGuide &&
      typeof window.AdpLeakAuditHowToReadGuide.init === "function"
    ) {
      window.AdpLeakAuditHowToReadGuide.init({ portfolio: true });
    }
  }

  function render(report) {
    showReport();
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
      report.sampleBanner || "";
    document.getElementById("alaSampleSubtext").textContent =
      report.sampleSubtext || "";
    document.getElementById("alaConfidential").textContent =
      report.confidentialLabel || "";
    document.getElementById("alaTitle").textContent =
      report.portfolioName || report.title || "";
    document.getElementById("alaCoverMeta").textContent =
      (report.runDate || "") +
      " · portfolio_rollup · " +
      (report.prioritySource || "inferred_from_results");
    document.getElementById("alaDisclaimer").textContent =
      report.limitedDiagnosticDisclaimer || "";

    document.getElementById("alaChips").innerHTML = [
      ["Run date", report.runDate],
      ["Providers", (report.providersUsed || []).join(", ")],
      ["Mode", "portfolio_rollup"],
      ["Priority source", report.prioritySource || "inferred_from_results"],
      ["Status", report.reportStatus],
    ]
      .map(function (c) {
        return (
          '<span class="drs-pill">' +
          esc(c[0]) +
          ": " +
          esc(c[1] || "—") +
          "</span>"
        );
      })
      .join("");

    document.getElementById("alaContext").textContent =
      (report.portfolioAuditContext && report.portfolioAuditContext.body) || "";
    document.getElementById("alaSummary").textContent =
      report.portfolioSummary || "";

    document.getElementById("alaKpis").innerHTML = "";
    if (window.AdpLeakAuditSharedUi && window.AdpLeakAuditSharedUi.renderKpiBand) {
      window.AdpLeakAuditSharedUi.renderKpiBand(
        document.getElementById("alaKpis"),
        report.executiveSignals,
        {
          className: "aiv-kpi-row ala-adp-kpi-row ala-kpi-grid--portfolio",
          tourById: {
            hotels_reviewed: "ai-consideration",
            strong_visibility: "scenario-presence",
          },
        }
      );
    }

    document.getElementById("alaHotelsBody").innerHTML = (
      report.hotelsNeedingAttention || []
    )
      .map(function (h) {
        return (
          "<tr><td>" +
          esc(h.hotel) +
          "</td><td>" +
          esc(h.priorityDemand) +
          "</td><td>" +
          esc(h.aiVisibilityRead) +
          "</td><td>" +
          esc(h.mainLeak) +
          "</td><td>" +
          esc(h.competitorShowingUpInstead) +
          "</td><td>" +
          esc(h.firstFix) +
          "</td></tr>"
        );
      })
      .join("");

    document.getElementById("alaSegments").innerHTML = (
      report.inferredDemandSegmentsLeaking || []
    )
      .map(function (s) {
        return (
          "<li><strong>" +
          esc(s.segment) +
          "</strong> — " +
          esc(s.note || "") +
          "</li>"
        );
      })
      .join("");

    document.getElementById("alaCompetitors").innerHTML = (
      report.competitorsBenefiting || []
    )
      .map(function (c) {
        return (
          '<article class="drs-evidence"><p class="drs-evidence__title">' +
          esc(c.competitorName) +
          '</p><p class="drs-evidence__row"><strong>Hotels affected:</strong> ' +
          esc(String(c.hotelsAffected)) +
          '</p><p class="drs-evidence__row"><strong>Demand segments:</strong> ' +
          esc((c.demandSegments || []).join(", ")) +
          '</p><p class="drs-evidence__row">“' +
          esc(c.evidenceExcerpt || "") +
          "”</p></article>"
        );
      })
      .join("");

    document.getElementById("alaPattern").textContent =
      (report.portfolioPattern && report.portfolioPattern.body) || "";

    document.getElementById("alaActions").innerHTML = (
      report.portfolioActions || []
    )
      .map(function (a, i) {
        return (
          '<article class="drs-action ala-adp-action"><p class="drs-action__eyebrow">Action ' +
          (i + 1) +
          '</p><h3 class="drs-action__title">' +
          esc(a.title) +
          "</h3>" +
          listHtml(a.items) +
          "</article>"
        );
      })
      .join("");

    var actionsHeading = document.getElementById("alaActionsHeading");
    if (
      actionsHeading &&
      window.AdpLeakAuditSharedUi &&
      window.AdpLeakAuditSharedUi.ACTION_SECTION_TITLE
    ) {
      actionsHeading.textContent =
        window.AdpLeakAuditSharedUi.ACTION_SECTION_TITLE;
    }

    document.getElementById("alaHowToUse").textContent =
      (report.howToUseThis && report.howToUseThis.body) || "";
    renderNext(report.recommendedNextStep || "");

    var cover = document.querySelector(".bas-cover-page");
    if (window.AdpLeakAuditSharedUi) {
      window.AdpLeakAuditSharedUi.ensureCoverFootnotes(cover);
      window.AdpLeakAuditSharedUi.ensureReportFooter(
        document.getElementById("alaReport"),
        "Portfolio AI Demand Leak Audit"
      );
      window.AdpLeakAuditSharedUi.bindInfoTooltips(document);
    }
    bindHowToReadGuide();
  }

  async function init() {
    try {
      var res = await fetch("/api/adp-leak-audit/sample-portfolio-report");
      var data = await res.json();
      if (!res.ok || !data.ok || !data.report) {
        showError((data && data.message) || "Portfolio sample not found.");
        return;
      }
      var report = data.report;
      if (report.propertyId || report.matchedHotelId || report.fullPromptText) {
        showError("Report failed safety checks.");
        return;
      }
      var missing = [];
      if (!report.hotelsNeedingAttention || !report.hotelsNeedingAttention.length) {
        missing.push("hotelsNeedingAttention");
      }
      if (
        !report.inferredDemandSegmentsLeaking ||
        !report.inferredDemandSegmentsLeaking.length
      ) {
        missing.push("inferredDemandSegmentsLeaking");
      }
      if (!report.competitorsBenefiting || !report.competitorsBenefiting.length) {
        missing.push("competitorsBenefiting");
      }
      if (!report.portfolioActions || !report.portfolioActions.length) {
        missing.push("portfolioActions");
      }
      if (!report.executiveSignals || !report.executiveSignals.length) {
        missing.push("executiveSignals");
      }
      if (missing.length) {
        showError(
          "Portfolio sample missing required sections: " + missing.join(", ")
        );
        return;
      }
      render(report);
    } catch (err) {
      showError(err.message || "Unable to load portfolio sample.");
    }
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }
})();
