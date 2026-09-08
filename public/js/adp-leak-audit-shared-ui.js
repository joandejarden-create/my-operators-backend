/**
 * Shared Leak Audit ↔ ADP product UI helpers.
 *
 * Reuses live ADP metric cards (aiv-kpi + info tooltip), evidence (aiv-evidence*),
 * action grammar (drs-action adapted on navy), and aiv-link / aiv-btn-text.
 */
(function (global) {
  "use strict";

  var PRODUCT_FAMILY = "Dealality AI Demand Positioning (ADP)";
  var ACTION_SECTION_TITLE = "Priority AI Demand Improvements";
  var ACTION_SECTION_SUBTITLE =
    "Targeted work to strengthen how AI presents this hotel — and close the demand gaps flagged in this diagnostic.";
  var ACTION_CARD_EYEBROW = "AI Improvement";
  var NEXT_STEP_SECTION_TITLE = "Book Your ADP Walkthrough";
  var WHO_SECTION_TITLE = "How We Partner on These Improvements";
  var WHO_SECTION_SUBTITLE =
    "A clear path from the priorities above to public updates — Dealality readies the work, you stay in control, and we stay with you through the next monitoring cycle.";
  var DEALALITY_CAN_HELP_PREPARE_LABEL = "Dealality can help prepare";
  /** @deprecated Use DEALALITY_CAN_HELP_PREPARE_LABEL */
  var DEALALITY_PREPARES_LABEL = DEALALITY_CAN_HELP_PREPARE_LABEL;
  var HOTEL_APPROVES_LABEL = "Hotel/operator approves";
  var HOTEL_AGENCY_PUBLISHES_LABEL = "Hotel / Agency publishes";
  var DEALALITY_MONITORS_LABEL = "Dealality monitors";

  var KPI_HELP = {
    ai_consideration:
      "How often the hotel appeared in monitored AI answers. This helps show whether the hotel is part of the AI-generated consideration set.",
    scenario_presence:
      "How broadly the hotel appeared across the traveler scenarios tested. A hotel can appear often overall but still be weak in specific demand situations.",
    reality_coverage:
      "How well AI reflected the hotel’s actual product attributes. Lower coverage may indicate that important features are missing or underrepresented in public sources.",
    leisure_advantage:
      "How often the hotel appeared versus comparable hotels for leisure travel in the monitored sample.",
    priority_demand_watch:
      "The demand area that appears most important to review based on the monitored results. This should be confirmed with the hotel before action.",
    primary_area_to_review:
      "The demand area that appears most important to review based on the monitored results. This should be confirmed with the hotel before action.",
    scenarios_monitored:
      "How many traveler scenarios × providers were monitored for this diagnostic. Separate from how often the hotel appeared.",
    hotels_reviewed:
      "Number of properties included in this portfolio diagnostic roll-up.",
    strong_visibility:
      "Hotels with broad visibility across the traveler needs tested in this sample.",
    priority_leaks:
      "Hotels with an inferred demand segment that warrants management review.",
    top_leaking_segment:
      "Inferred demand segment appearing most often across hotels in this portfolio sample.",
    top_competitor:
      "Competitor appearing most often when a subject hotel was absent in monitored answers.",
    top_action:
      "Highest-leverage repeatable action across the portfolio sample.",
  };

  var EVIDENCE_HELP = {
    "positive signal":
      "A monitored example where the hotel appeared strongly for a demand area that is working well.",
    "competitor example":
      "A monitored example where a competitor appeared when the subject hotel was absent.",
    "source / attribute example":
      "A monitored example of how AI reflected (or under-reflected) a product attribute or cited source.",
    "scenarios monitored":
      "How many traveler scenarios × providers were monitored for this diagnostic.",
  };

  var TWO_LINE_LABELS = {
    "primary area to review": ["PRIMARY AREA", "TO REVIEW"],
    "scenarios monitored": ["SCENARIOS", "MONITORED"],
    "positive signal": ["POSITIVE", "SIGNAL"],
    "competitor example": ["COMPETITOR", "EXAMPLE"],
    "source / attribute example": ["SOURCE / ATTRIBUTE", "EXAMPLE"],
  };

  function esc(s) {
    return String(s == null ? "" : s)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;");
  }

  function twoLineLabelHtml(label) {
    var key = String(label || "")
      .trim()
      .toLowerCase();
    var lines = TWO_LINE_LABELS[key];
    if (lines && lines.length === 2) {
      return (
        '<span class="aiv-kpi-label ala-label-2line">' +
        '<span class="ala-label-2line__a">' +
        esc(lines[0]) +
        "</span>" +
        '<span class="ala-label-2line__b">' +
        esc(lines[1]) +
        "</span>" +
        "</span>"
      );
    }
    return '<span class="aiv-kpi-label">' + esc(label) + "</span>";
  }

  /** Exact ADP info icon markup (aiv-info-icon sprite + .info-tooltip.aiv-col-info). */
  function infoIconHtml(helpText, label) {
    var tip = esc(helpText || "");
    var aria = esc("About " + (label || "this metric"));
    return (
      '<span class="info-tooltip aiv-col-info">' +
      '<span class="info-icon" role="button" tabindex="0" aria-label="' +
      aria +
      '">' +
      '<svg width="14" height="14" aria-hidden="true"><use href="#aiv-info-icon"></use></svg>' +
      "</span>" +
      '<div class="tooltip-content adp-executive-metric-tooltip" hidden role="tooltip">' +
      "<strong>" +
      esc(label || "Metric") +
      "</strong><br><br>" +
      tip +
      "</div>" +
      "</span>"
    );
  }

  function evidenceLinksHtml(ids, label) {
    if (!ids || !ids.length) return "";
    /* One View example control per evidence card — never duplicate links. */
    var id = ids[0];
    return (
      '<div class="ala-evidence-links ala-web-ev ala-no-print">' +
      '<button type="button" class="aiv-btn-text aiv-link" data-ala-evidence="' +
      esc(id) +
      '">' +
      esc(label || "View example") +
      "</button>" +
      "</div>"
    );
  }

  function kpiValueHtml(kpi) {
    var raw = String(kpi && kpi.value != null ? kpi.value : "").trim();
    if (!raw) return '<div class="aiv-value"></div>';
    var isPhrase =
      (kpi &&
        (kpi.id === "primary_area_to_review" ||
          kpi.id === "priority_demand_watch")) ||
      (!/%$/.test(raw) && !/^\d+(\.\d+)?x?$/i.test(raw) && /\s/.test(raw));
    if (isPhrase && /\s&\s/.test(raw)) {
      var parts = raw.split(/\s&\s/);
      return (
        '<div class="aiv-value aiv-value--phrase">' +
        '<span>' +
        esc(parts[0] + " &") +
        "</span>" +
        '<span>' +
        esc(parts.slice(1).join(" & ")) +
        "</span>" +
        "</div>"
      );
    }
    if (isPhrase) {
      return (
        '<div class="aiv-value aiv-value--phrase">' + esc(raw) + "</div>"
      );
    }
    return '<div class="aiv-value">' + esc(raw) + "</div>";
  }

  function renderKpiBand(container, signals, opts) {
    if (!container) return;
    opts = opts || {};
    var tourById = opts.tourById || {};
    var count = (signals || []).length || 1;
    container.className =
      (opts.className || "aiv-kpi-row ala-adp-kpi-row") +
      " ala-adp-kpi-row--" +
      Math.min(count, 6);
    container.innerHTML = (signals || [])
      .map(function (kpi) {
        var tour = tourById[kpi.id];
        var tourAttr = tour
          ? ' data-adp-tour-target="' + esc(tour) + '"'
          : "";
        var help = KPI_HELP[kpi.id] || kpi.description || "";
        var watch =
          kpi.id === "priority_demand_watch" ||
          kpi.id === "primary_area_to_review"
            ? " aiv-kpi--watch"
            : "";
        return (
          '<article class="aiv-kpi' +
          watch +
          '"' +
          tourAttr +
          ' data-ala-kpi-id="' +
          esc(kpi.id || "") +
          '">' +
          "<h3>" +
          twoLineLabelHtml(kpi.label) +
          infoIconHtml(help, kpi.label) +
          "</h3>" +
          kpiValueHtml(kpi) +
          "</article>"
        );
      })
      .join("");
  }

  function renderEvidenceBody(ev) {
    if (!ev) return "<p>Evidence unavailable.</p>";
    var sources = (ev.sourceLinks || [])
      .map(function (s) {
        var href = typeof s === "string" ? s : s && s.url;
        var label =
          (s && (s.label || s.title)) ||
          (href ? String(href).replace(/^https?:\/\//, "") : "Source");
        if (!href) return "";
        return (
          '<a class="aiv-source-link aiv-link" href="' +
          esc(href) +
          '" target="_blank" rel="noopener noreferrer">' +
          esc(label) +
          "</a>"
        );
      })
      .filter(Boolean)
      .join("");
    return (
      '<article class="aiv-evidence ala-adp-evidence">' +
      '<section class="aiv-evidence-meta" aria-label="Evidence details">' +
      '<div class="aiv-evidence-meta-item"><div class="aiv-evidence-label">Evidence type</div>' +
      '<div class="aiv-evidence-value">' +
      esc(ev.evidenceTypeLabel || ev.evidenceType || "—") +
      "</div></div>" +
      '<div class="aiv-evidence-meta-item"><div class="aiv-evidence-label">Demand area</div>' +
      '<div class="aiv-evidence-value">' +
      esc(ev.demandSegment || "—") +
      "</div></div>" +
      '<div class="aiv-evidence-meta-item"><div class="aiv-evidence-label">Provider</div>' +
      '<div class="aiv-evidence-value">' +
      esc(ev.provider || "—") +
      "</div></div>" +
      '<div class="aiv-evidence-meta-item"><div class="aiv-evidence-label">Subject hotel</div>' +
      '<div class="aiv-evidence-value">' +
      esc(ev.subjectHotelMentioned || "—") +
      "</div></div>" +
      (ev.competitorMentioned
        ? '<div class="aiv-evidence-meta-item"><div class="aiv-evidence-label">Competitor</div>' +
          '<div class="aiv-evidence-value">' +
          esc(ev.competitorMentioned) +
          "</div></div>"
        : "") +
      "</section>" +
      '<section class="aiv-evidence-section aiv-evidence-section--ai-response">' +
      '<div class="aiv-evidence-label">AI response</div>' +
      '<pre class="aiv-evidence-response">' +
      esc(ev.excerpt || "") +
      "</pre>" +
      "</section>" +
      (sources
        ? '<section class="aiv-evidence-sources"><div class="aiv-evidence-label">Source links</div>' +
          sources +
          "</section>"
        : "") +
      '<section class="aiv-evidence-section"><div class="aiv-evidence-label">Why it matters</div>' +
      '<p class="aiv-excerpt-p">' +
      esc(ev.whyThisMatters || "") +
      "</p></section>" +
      '<section class="aiv-evidence-section"><div class="aiv-evidence-label">Management review</div>' +
      '<p class="aiv-excerpt-p">' +
      esc(ev.managementReview || "") +
      "</p></section>" +
      "</article>"
    );
  }

  function inlineList(items) {
    if (!items || !items.length) return "—";
    return esc(items.join(" · "));
  }

  function chipListHtml(items, chipClass) {
    if (!items || !items.length) {
      return '<p class="ala-adp-action__empty">—</p>';
    }
    return (
      '<ul class="ala-adp-action__chips' +
      (chipClass ? " " + chipClass : "") +
      '">' +
      items
        .map(function (item) {
          return (
            '<li class="ala-adp-action__chip">' + esc(item) + "</li>"
          );
        })
        .join("") +
      "</ul>"
    );
  }

  function renderActionItems(container, fixes) {
    if (!container) return;
    container.className =
      (container.className || "") +
      (/\bala-adp-actions\b/.test(container.className)
        ? ""
        : " ala-adp-actions");
    container.innerHTML = (fixes || [])
      .slice(0, 3)
      .map(function (f, i) {
        var nextCheck = f.expectedNextMonitoringCheck
          ? '<p class="ala-adp-action__next-check">' +
            esc(f.expectedNextMonitoringCheck) +
            "</p>"
          : "";
        return (
          '<article class="drs-action ala-adp-action aiv-card drs-avoid-break">' +
          '<div class="ala-adp-action__header">' +
          '<p class="drs-action__eyebrow"><span class="ala-adp-action__num">' +
          (i + 1) +
          "</span> " +
          esc(ACTION_CARD_EYEBROW) +
          "</p>" +
          '<h3 class="drs-action__title">' +
          esc(f.title) +
          "</h3>" +
          "</div>" +
          '<div class="drs-action__field ala-adp-action__why">' +
          '<p class="drs-label">Why it matters</p>' +
          '<p class="ala-adp-action__why-body">' +
          esc(f.whyThisMatters || "") +
          "</p></div>" +
          '<div class="drs-action__field ala-adp-action__dealality">' +
          '<p class="drs-label">' +
          esc(DEALALITY_CAN_HELP_PREPARE_LABEL) +
          "</p>" +
          chipListHtml(f.dealalityCanPrepare, "ala-adp-action__chips--dealality") +
          "</div>" +
          '<div class="drs-action__field ala-adp-action__hotel">' +
          '<p class="drs-label">Hotel confirms</p>' +
          chipListHtml(f.hotelMustConfirm, "ala-adp-action__chips--hotel") +
          "</div>" +
          '<div class="drs-action__field ala-adp-action__sources">' +
          '<p class="drs-label">Sources / next check</p>' +
          chipListHtml(f.targetSources, "ala-adp-action__chips--sources") +
          nextCheck +
          "</div>" +
          "</article>"
        );
      })
      .join("");
  }

  function whoCardHtml(step, lines, body, extraClass) {
    return (
      '<article class="aiv-card ala-who__card' +
      (extraClass ? " " + extraClass : "") +
      '">' +
      '<p class="ala-who__step">Step ' +
      esc(String(step)) +
      "</p>" +
      '<h3 class="ala-label-2line">' +
      '<span class="ala-label-2line__a">' +
      esc(lines[0]) +
      "</span>" +
      '<span class="ala-label-2line__b">' +
      esc(lines[1]) +
      "</span>" +
      "</h3><p>" +
      esc(body) +
      "</p></article>"
    );
  }

  function renderWhoDoes(container, who) {
    if (!container) return;
    if (!who) {
      container.innerHTML = "";
      return;
    }
    container.className = "ala-who ala-who--readable aiv-secondary-row";
    var prepareBody =
      who.dealalityPreparesShort ||
      "We assemble audits, draft copy, and checklists so you can move quickly";
    var approveBody =
      who.hotelApprovesShort ||
      "You approve facts, claims, and final wording before anything goes live";
    var publishBody =
      who.hotelOrAgencyPublishesShort ||
      "Your team or agency publishes on Website, OTAs, GBP, and TripAdvisor";
    var monitorBody =
      who.dealalityMonitorsShort ||
      "We re-check signals, competitors, and progress on the next run";
    container.innerHTML =
      whoCardHtml(
        1,
        ["DEALALITY", "PREPARES"],
        prepareBody,
        "ala-who__card--dealality"
      ) +
      whoCardHtml(2, ["YOU", "CONFIRM"], approveBody, "") +
      whoCardHtml(
        3,
        ["YOUR TEAM", "PUBLISHES"],
        publishBody,
        "ala-who__card--publish"
      ) +
      whoCardHtml(
        4,
        ["DEALALITY", "FOLLOWS THROUGH"],
        monitorBody,
        "ala-who__card--monitor"
      );
  }

  function hotelShortName(name) {
    var n = String(name || "").trim();
    if (!n) return "Cambridge";
    if (/cambridge/i.test(n)) return "Cambridge";
    var first = n.split(/\s+/)[0];
    return first || "Cambridge";
  }

  function renderCompetitorCards(container, rows, opts) {
    if (!container) return;
    opts = opts || {};
    var shortHotel = hotelShortName(opts.hotelName);
    container.className = "ala-comp-table ala-adp-comp-grid";
    container.innerHTML = (rows || [])
      .slice(0, 2)
      .map(function (row) {
        var count = row.displacementCount != null ? row.displacementCount : "";
        return (
          '<article class="aiv-card ala-comp-card">' +
          '<p class="ala-comp-card__name">' +
          esc(row.competitorName) +
          "</p>" +
          '<p class="ala-comp-card__badge">' +
          esc(shortHotel + " absent: " + String(count)) +
          "</p>" +
          '<p class="ala-comp-card__meta"><span>Demand area</span>' +
          '<span class="ala-comp-card__pill">' +
          esc(row.demandSegment || "") +
          "</span></p>" +
          '<p class="ala-comp-card__mean"><span>What this may mean</span>' +
          '<span class="ala-comp-card__mean-body">' +
          esc(row.whatThisMayMean || row.interpretation || "") +
          "</span></p>" +
          "</article>"
        );
      })
      .join("");
  }

  function renderSupportingEvidence(container, examples) {
    if (!container) return;
    container.className = "ala-evidence-examples ala-adp-evidence-grid";
    container.innerHTML = (examples || [])
      .map(function (ex) {
        var ids = ex.relatedIds || (ex.id ? [ex.id] : []);
        var label = ex.label || "Example";
        var helpKey = String(label).trim().toLowerCase();
        var help =
          EVIDENCE_HELP[helpKey] ||
          "Supporting monitored example for management review.";
        return (
          '<article class="aiv-card ala-evidence-example">' +
          '<div class="ala-evidence-example__head">' +
          twoLineLabelHtml(label) +
          infoIconHtml(help, label) +
          "</div>" +
          '<p class="ala-evidence-example__summary" title="' +
          esc(ex.summary || "") +
          '">' +
          esc(ex.summary || "") +
          "</p>" +
          evidenceLinksHtml(ids, ex.linkLabel || "View example") +
          "</article>"
        );
      })
      .join("");
  }

  function renderScenariosMonitored(container, scenarios) {
    if (!container) return;
    var data = scenarios || {};
    var title = data.title || "Scenarios Monitored";
    var value = data.value || "15 × 4";
    var sub = data.valueSubLabel || "60 observations";
    var m = String(value).match(/^(.+?)\s+Providers\s*$/i);
    if (m) {
      value = m[1].trim();
      if (!data.valueSubLabel) sub = "60 observations";
    }
    var description =
      data.description ||
      "15 traveler scenarios across ChatGPT, Gemini, Perplexity and Claude.";
    var help =
      KPI_HELP.scenarios_monitored ||
      "How many traveler scenarios × providers were monitored for this limited diagnostic.";
    container.className = "ala-scenarios-monitored";
    container.innerHTML =
      '<article class="aiv-kpi ala-scenarios-monitored__card ala-evidence-example" data-ala-scenarios-monitored="1">' +
      '<div class="ala-evidence-example__head">' +
      twoLineLabelHtml(title) +
      infoIconHtml(help, title) +
      "</div>" +
      '<div class="aiv-value ala-scenarios-monitored__value">' +
      esc(value) +
      "</div>" +
      '<div class="ala-scenarios-monitored__sub">' +
      esc(sub) +
      "</div>" +
      '<div class="aiv-meta ala-evidence-example__summary" title="' +
      esc(description) +
      '">' +
      esc(description) +
      "</div>" +
      "</article>";
  }

  function ensureTooltipContainer() {
    var el = document.getElementById("aivTooltipContainer");
    if (el) return el;
    el = document.createElement("div");
    el.id = "aivTooltipContainer";
    el.className = "aiv-tooltip-container";
    el.setAttribute("aria-live", "polite");
    document.body.appendChild(el);
    return el;
  }

  function closeColumnInfo() {
    var container = document.getElementById("aivTooltipContainer");
    if (container) container.innerHTML = "";
  }

  function openColumnInfo(tooltipContent) {
    var container = ensureTooltipContainer();
    if (!container || !tooltipContent) return;
    container.innerHTML = "";
    var cloned = tooltipContent.cloneNode(true);
    cloned.classList.add("aiv-tooltip-panel");
    cloned.style.display = "block";
    cloned.style.visibility = "visible";
    cloned.style.opacity = "1";
    cloned.removeAttribute("hidden");
    var closeBtn = document.createElement("button");
    closeBtn.type = "button";
    closeBtn.className = "tooltip-close-btn";
    closeBtn.setAttribute("aria-label", "Close");
    closeBtn.innerHTML = "\u00d7";
    closeBtn.addEventListener("click", function (ev) {
      ev.preventDefault();
      ev.stopPropagation();
      closeColumnInfo();
    });
    cloned.appendChild(closeBtn);
    container.appendChild(cloned);
  }

  /**
   * ADP-native info popups: clone tooltip into #aivTooltipContainer.
   * Inline .tooltip-content stays display:none via ai-visibility-shared.css.
   */
  function bindInfoTooltips(root) {
    var scope = root || document;
    if (document.documentElement.getAttribute("data-ala-info-bound")) return;
    document.documentElement.setAttribute("data-ala-info-bound", "1");
    ensureTooltipContainer();

    function openFromEvent(e) {
      var icon =
        e.target.closest &&
        e.target.closest(".aiv-page .aiv-col-info .info-icon");
      if (!icon) return false;
      e.preventDefault();
      e.stopPropagation();
      var tip = icon.closest(".info-tooltip");
      var content = tip && tip.querySelector(".tooltip-content");
      openColumnInfo(content);
      return true;
    }

    document.addEventListener("click", function (e) {
      if (
        e.target.closest &&
        e.target.closest(".aiv-page .aiv-col-info .info-icon")
      ) {
        openFromEvent(e);
        return;
      }
      if (
        e.target.closest &&
        !e.target.closest(".aiv-col-info") &&
        !e.target.closest("#aivTooltipContainer .tooltip-content")
      ) {
        closeColumnInfo();
      }
    });
    document.addEventListener("keydown", function (e) {
      if (e.key === "Escape") {
        closeColumnInfo();
        return;
      }
      if (
        (e.key === "Enter" || e.key === " ") &&
        e.target.closest &&
        e.target.closest(".aiv-page .aiv-col-info .info-icon")
      ) {
        openFromEvent(e);
      }
    });
    var container = document.getElementById("aivTooltipContainer");
    if (container) {
      container.addEventListener("click", function (e) {
        if (e.target === container) closeColumnInfo();
      });
    }
    void scope;
  }

  global.AdpLeakAuditSharedUi = {
    PRODUCT_FAMILY: PRODUCT_FAMILY,
    ACTION_SECTION_TITLE: ACTION_SECTION_TITLE,
    ACTION_SECTION_SUBTITLE: ACTION_SECTION_SUBTITLE,
    ACTION_CARD_EYEBROW: ACTION_CARD_EYEBROW,
    NEXT_STEP_SECTION_TITLE: NEXT_STEP_SECTION_TITLE,
    WHO_SECTION_TITLE: WHO_SECTION_TITLE,
    WHO_SECTION_SUBTITLE: WHO_SECTION_SUBTITLE,
    DEALALITY_CAN_HELP_PREPARE_LABEL: DEALALITY_CAN_HELP_PREPARE_LABEL,
    DEALALITY_PREPARES_LABEL: DEALALITY_PREPARES_LABEL,
    HOTEL_APPROVES_LABEL: HOTEL_APPROVES_LABEL,
    HOTEL_AGENCY_PUBLISHES_LABEL: HOTEL_AGENCY_PUBLISHES_LABEL,
    DEALALITY_MONITORS_LABEL: DEALALITY_MONITORS_LABEL,
    KPI_HELP: KPI_HELP,
    esc: esc,
    infoIconHtml: infoIconHtml,
    evidenceLinksHtml: evidenceLinksHtml,
    renderKpiBand: renderKpiBand,
    renderEvidenceBody: renderEvidenceBody,
    renderActionItems: renderActionItems,
    renderWhoDoes: renderWhoDoes,
    renderCompetitorCards: renderCompetitorCards,
    renderSupportingEvidence: renderSupportingEvidence,
    renderScenariosMonitored: renderScenariosMonitored,
    bindInfoTooltips: bindInfoTooltips,
  };
})(typeof window !== "undefined" ? window : globalThis);
