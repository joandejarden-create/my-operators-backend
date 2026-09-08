/**
 * Unified AI Demand Leak Audit renderer (sample + live).
 * ADP-native 3-page shell: cover + executive diagnostic + action/conversion.
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

  function isSamplePath() {
    if (window.__ALA_SAMPLE_MODE__ === true) return true;
    var path = String(window.location.pathname || "");
    if (/\/adp-leak-audit\/sample\/?$/.test(path)) return true;
    if (/adp-leak-audit-report\.html$/i.test(path)) {
      var q = new URLSearchParams(window.location.search);
      if (q.get("sample") === "1" || !q.get("reportId")) return true;
    }
    return false;
  }

  function reportIdFromPath() {
    var parts = window.location.pathname.split("/").filter(Boolean);
    var shareIdx = parts.indexOf("share");
    if (shareIdx >= 0 && parts[shareIdx + 1]) {
      return parts[shareIdx + 1];
    }
    var idx = parts.indexOf("adp-leak-audit");
    if (idx >= 0 && parts[idx + 1] && parts[idx + 1] !== "sample") {
      return parts[idx + 1];
    }
    return new URLSearchParams(window.location.search).get("reportId") || "";
  }

  function showError(message) {
    var loading = document.getElementById("alaReportLoading");
    var report = document.getElementById("alaReport");
    var err = document.getElementById("alaReportError");
    if (loading) loading.hidden = true;
    if (report) report.hidden = true;
    if (err) {
      err.hidden = false;
      document.getElementById("alaReportErrorMsg").textContent =
        message || "Unavailable.";
    }
  }

  function showReport() {
    var loading = document.getElementById("alaReportLoading");
    var report = document.getElementById("alaReport");
    var err = document.getElementById("alaReportError");
    if (loading) loading.hidden = true;
    if (err) err.hidden = true;
    if (report) report.hidden = false;
  }

  function inlineList(items) {
    if (!items || !items.length) return "—";
    return esc(items.join(" · "));
  }

  function setText(id, value) {
    var el = document.getElementById(id);
    if (el) el.textContent = value == null ? "" : String(value);
  }

  /**
   * Cover title with PDF-safe word spaces (Chromium often drops spaces under
   * tight letter-spacing). Prefer two explicit lines when the name ends in
   * Resort/Hotel/Spa style suffixes.
   */
  function setCoverTitle(id, hotelName) {
    var el = document.getElementById(id);
    if (!el) return;
    var name = String(hotelName || "Hotel").trim() || "Hotel";
    var line1 = name;
    var line2 = "";
    var m = name.match(
      /^(.*?)\s+((?:Resort|Hotel|Spa|Club|Inn|Lodge|Collection)\b.*)$/i
    );
    if (m) {
      line1 = m[1];
      line2 = m[2];
    }
    function lineHtml(line) {
      return String(line)
        .split(/\s+/)
        .filter(Boolean)
        .map(function (word) {
          return (
            '<span class="ala-cover-title-word">' +
            esc(word) +
            "</span>"
          );
        })
        .join('<span class="ala-cover-title-space">\u00A0</span>');
    }
    if (line2) {
      el.innerHTML =
        '<span class="ala-cover-title-line">' +
        lineHtml(line1) +
        "</span><br />" +
        '<span class="ala-cover-title-line">' +
        lineHtml(line2) +
        "</span>";
    } else {
      el.innerHTML =
        '<span class="ala-cover-title-line">' + lineHtml(line1) + "</span>";
    }
  }

  function formatCoverGenerated(runDate) {
    if (!runDate) return "September 7, 2026";
    var d = new Date(String(runDate) + (String(runDate).length <= 10 ? "T12:00:00" : ""));
    if (Number.isNaN(d.getTime())) return String(runDate);
    try {
      return d.toLocaleDateString("en-US", {
        month: "long",
        day: "numeric",
        year: "numeric",
      });
    } catch (_) {
      return String(runDate);
    }
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
    if (typeof drawer.showModal === "function") {
      if (!drawer.open) drawer.showModal();
    } else {
      drawer.hidden = false;
      drawer.setAttribute("aria-hidden", "false");
    }
    document.body.classList.add("ala-drawer-open");
  }

  function closeEvidence() {
    var drawer = document.getElementById("alaEvidenceDrawer");
    if (!drawer) return;
    if (typeof drawer.close === "function" && drawer.open) {
      drawer.close();
    } else {
      drawer.hidden = true;
      drawer.setAttribute("aria-hidden", "true");
    }
    document.body.classList.remove("ala-drawer-open");
  }

  function bindEvidence() {
    document.addEventListener("click", function (e) {
      var btn = e.target.closest("[data-ala-evidence]");
      if (btn) {
        e.preventDefault();
        openEvidence(btn.getAttribute("data-ala-evidence"));
      }
    });
    var drawer = document.getElementById("alaEvidenceDrawer");
    if (drawer) {
      drawer.addEventListener("close", function () {
        document.body.classList.remove("ala-drawer-open");
      });
    }
    document.addEventListener("keydown", function (e) {
      if (e.key === "Escape") closeEvidence();
    });
  }

  function webEvidenceBtn(ids, label) {
    if (!ids || !ids.length) return "";
    var ui = window.AdpLeakAuditSharedUi;
    if (ui && ui.evidenceLinksHtml) return ui.evidenceLinksHtml(ids, label);
    var id = ids[0];
    return (
      '<button type="button" class="aiv-btn-text aiv-link ala-web-ev ala-no-print" data-ala-evidence="' +
      esc(id) +
      '">' +
      esc(label || "View example") +
      "</button>"
    );
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

    var year = new Date().getFullYear();
    var coverMeta = report.coverMeta || {};
    var providersFromList = (report.providersUsed || []).length;
    var providers =
      providersFromList ||
      (coverMeta.providers != null ? coverMeta.providers : 0) ||
      4;
    var scenarios =
      coverMeta.scenarios != null
        ? coverMeta.scenarios
        : report.scenariosMonitored &&
            String(report.scenariosMonitored.value || "").match(/^(\d+)/)
          ? Number(String(report.scenariosMonitored.value).match(/^(\d+)/)[1])
          : 15;
    var observations =
      coverMeta.observations != null
        ? coverMeta.observations
        : Number(scenarios) * Number(providers);
    var actions =
      (report.fixes || []).length ||
      (coverMeta.actionItems != null ? coverMeta.actionItems : 0) ||
      3;
    var runLabel = formatCoverGenerated(report.runDate);

    /* HID cover tokens — text only; layout comes from BAS/HID classes */
    setText(
      "alaConfidential",
      "Dealality AI Demand Positioning · Confidential · For recipient only"
    );
    setText("alaDocType", "Limited AI Demand Leak Audit");
    setCoverTitle("alaTitle", report.hotelName || "Hotel");
    setText(
      "alaLocation",
      report.hotelLocation || report.locationLabel || "Sandys Parish, Bermuda"
    );
    setText(
      "alaCoverSub",
      "AI Visibility · Demand Territories · Competitor Displacement · Action Priorities"
    );
    setText(
      "alaCoverMetaPrimary",
      coverMeta.coverMetaLine ||
        "PROVIDERS " +
          providers +
          " · SCENARIOS " +
          scenarios +
          " · OBSERVATIONS " +
          observations +
          " · ACTION ITEMS " +
          actions
    );
    setText(
      "alaCoverMetaSecondary",
      "Generated " + runLabel + " · Limited diagnostic"
    );
    setText(
      "alaDisclaimer",
      report.coverDisclaimer ||
        "This AI Demand Leak Audit is a limited diagnostic based on monitored AI responses. It uses the same reading logic as Dealality AI Demand Positioning, but is not a full monthly monitoring report. Findings do not prove causation and should be reviewed alongside hotel commercial judgment."
    );
    setText(
      "alaCoverFootLegal",
      "Confidential · For recipient only · © " + year + " Dealality"
    );

    var ui = window.AdpLeakAuditSharedUi;
    var exec = report.executiveSummary;
    var execEl = document.getElementById("alaExecSummary");
    if (exec && Array.isArray(exec.paragraphs) && exec.paragraphs.length) {
      execEl.innerHTML = exec.paragraphs
        .map(function (p) {
          return "<p>" + esc(p) + "</p>";
        })
        .join("");
    } else {
      execEl.innerHTML =
        "<p>" +
        esc(report.bottomLineSummary || "") +
        "</p>" +
        (report.bottomLinePriority
          ? "<p>" + esc(report.bottomLinePriority) + "</p>"
          : "");
    }

    var kpiEl = document.getElementById("alaKpis");
    if (ui && typeof ui.renderKpiBand === "function") {
      ui.renderKpiBand(kpiEl, report.executiveSignals || [], {
        className: "aiv-kpi-row ala-adp-kpi-row",
      });
      if (typeof ui.bindInfoTooltips === "function") ui.bindInfoTooltips();
    } else {
      kpiEl.innerHTML = (report.executiveSignals || [])
        .map(function (kpi) {
          var watch =
            kpi.id === "primary_area_to_review" ? " aiv-kpi--watch" : "";
          return (
            '<article class="aiv-kpi' +
            watch +
            '"><h3><span class="aiv-kpi-label">' +
            esc(kpi.label) +
            '</span></h3><div class="aiv-value">' +
            esc(kpi.value) +
            '</div><div class="aiv-meta">' +
            esc(kpi.description || "") +
            "</div></article>"
          );
        })
        .join("");
    }

    var demand = report.demandAreaToReview || report.inferredDemandLeak || {};
    setText("alaDemandArea", demand.summary || report.biggestDemandLeak || "");
    document.getElementById("alaDemandRefs").innerHTML = (
      demand.evidenceRefs || []
    )
      .map(function (r) {
        var text = String(r || "").trim();
        if (text && !/[.!?]$/.test(text)) text += ".";
        return "<li>" + esc(text) + "</li>";
      })
      .join("");

    var rank = report.competitorDisplacementRank || {};
    setText(
      "alaCompetitorNote",
      rank.note ||
        report.competitorPriorityNote ||
        "This is not a full market ranking. It shows which competitors appeared most often when Cambridge Beaches was absent in the monitored sample."
    );
    if (ui && typeof ui.renderCompetitorCards === "function") {
      ui.renderCompetitorCards(
        document.getElementById("alaCompetitors"),
        rank.rows || [],
        { hotelName: report.hotelName }
      );
    } else {
      document.getElementById("alaCompetitors").innerHTML = "";
    }

    var support = report.supportingEvidence || {};
    var supportEl = document.getElementById("alaSupportingEvidence");
    if (ui && typeof ui.renderSupportingEvidence === "function") {
      ui.renderSupportingEvidence(supportEl, support.examples || []);
    } else if (supportEl) {
      supportEl.innerHTML = (support.examples || [])
        .map(function (ex) {
          return (
            '<article class="aiv-card ala-evidence-example">' +
            '<p class="ala-evidence-example__label">' +
            esc(ex.label || "Example") +
            "</p>" +
            '<p class="ala-evidence-example__summary">' +
            esc(ex.summary || "") +
            "</p>" +
            webEvidenceBtn(
              ex.relatedIds || (ex.id ? [ex.id] : []),
              ex.linkLabel || "View example"
            ) +
            "</article>"
          );
        })
        .join("");
    }

    var scenariosEl = document.getElementById("alaScenariosMonitored");
    if (ui && typeof ui.renderScenariosMonitored === "function") {
      ui.renderScenariosMonitored(
        scenariosEl,
        report.scenariosMonitored || {
          title: "Scenarios Monitored",
          value: "60 × 4",
          valueSubLabel: "Providers",
          description:
            "60 traveler scenarios × ChatGPT, Gemini, Perplexity and Claude.",
        }
      );
    }

    var heading = document.getElementById("alaActionsHeading");
    if (heading && ui && ui.ACTION_SECTION_TITLE) {
      heading.textContent = ui.ACTION_SECTION_TITLE;
    }
    var sub = document.getElementById("alaActionsSubtitle");
    if (sub && ui && ui.ACTION_SECTION_SUBTITLE) {
      sub.textContent = ui.ACTION_SECTION_SUBTITLE;
    }
    var nextHeading = document.getElementById("alaNextHeading");
    if (nextHeading && ui && ui.NEXT_STEP_SECTION_TITLE) {
      nextHeading.textContent = ui.NEXT_STEP_SECTION_TITLE;
    }
    var whoHeading = document.getElementById("alaWhoHeading");
    if (whoHeading && ui && ui.WHO_SECTION_TITLE) {
      whoHeading.textContent = ui.WHO_SECTION_TITLE;
    }
    var whoSub = document.getElementById("alaWhoSubtitle");
    if (whoSub && ui && ui.WHO_SECTION_SUBTITLE) {
      whoSub.textContent = ui.WHO_SECTION_SUBTITLE;
    }

    var fixesEl = document.getElementById("alaFixes");
    if (ui && typeof ui.renderActionItems === "function") {
      ui.renderActionItems(fixesEl, report.fixes || []);
    }

    var whoEl = document.getElementById("alaWho");
    if (ui && typeof ui.renderWhoDoes === "function") {
      ui.renderWhoDoes(whoEl, report.whoDoesTheWork);
    } else if (whoEl) {
      whoEl.innerHTML = "";
    }

    var nextEl = document.getElementById("alaNext");
    nextEl.innerHTML = String(report.recommendedNextStep || "")
      .split(/\n\s*\n/)
      .map(function (para) {
        return "<p>" + esc(para.replace(/\n/g, " ").trim()) + "</p>";
      })
      .filter(function (html) {
        return html !== "<p></p>";
      })
      .join("");

    if (
      window.AdpLeakAuditHowToReadGuide &&
      typeof window.AdpLeakAuditHowToReadGuide.init === "function"
    ) {
      window.AdpLeakAuditHowToReadGuide.init({
        portfolio: false,
        liveThinReport: false,
      });
    }
  }

  async function init() {
    bindEvidence();
    var sample = isSamplePath();
    var reportId = sample ? "sample" : reportIdFromPath();
    if (!sample && !reportId) {
      showError("Missing report id.");
      return;
    }
    try {
      var url = sample
        ? "/api/adp-leak-audit/sample-report"
        : /\/adp-leak-audit\/share\//.test(window.location.pathname)
          ? "/api/adp-leak-audit/share/" + encodeURIComponent(reportId)
          : "/api/adp-leak-audit/report/" + encodeURIComponent(reportId);
      var res = await fetch(url);
      var data = await res.json();
      if (!res.ok || !data.ok || !data.report) {
        showError((data && (data.message || data.error)) || "Report not found.");
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
      if (!report.whoDoesTheWork) {
        showError("Report is missing required sections.");
        return;
      }
      if (!report.executiveSignals || !report.executiveSignals.length) {
        showError("Report is missing executive signals.");
        return;
      }
      render(report);
    } catch (err) {
      showError(err.message || "Unable to load report.");
    }
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }
})();
