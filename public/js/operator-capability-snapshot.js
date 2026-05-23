/**
 * Operator Capability Snapshot — book-style document (aligned with Brand Alignment / Readiness).
 * Data: GET /api/deals/:dealId/operator-capability-snapshot
 */
(function (global) {
  "use strict";

  var STATUS_COPY = {
    allowed: "Ready for owner/advisor review",
    limited: "Limited draft — review before external use",
    blocked: "More deal information required",
  };

  var COVER_NOTE =
    "This snapshot identifies operator capabilities that may be relevant to this opportunity based on current deal inputs. " +
    "It is intended to support owner/advisor review and does not recommend, rank, endorse, or select operators.";

  var DEALALITY_LOGO_URL =
    "https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/69c166836c109719f94e055e_Dealality%20Logo%20(4)%20(1).png";

  function esc(t) {
    return String(t == null ? "" : t)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;");
  }

  function formatDate(iso) {
    if (!iso) return "";
    try {
      var d = new Date(iso);
      if (Number.isNaN(d.getTime())) return String(iso).slice(0, 10);
      return d.toLocaleDateString(undefined, { year: "numeric", month: "long", day: "numeric" });
    } catch (_) {
      return String(iso).slice(0, 10);
    }
  }

  function locationLine(deal) {
    deal = deal || {};
    var m = deal.market && deal.market !== "—" ? String(deal.market) : "";
    var c = deal.country && deal.country !== "—" ? String(deal.country) : "";
    if (!m && !c) return "—";
    if (!m) return c;
    if (!c) return m;
    if (m.toLowerCase().indexOf(c.toLowerCase()) >= 0) return m;
    return m + ", " + c;
  }

  function metaLine(deal) {
    deal = deal || {};
    var parts = [];
    if (deal.keyCount != null && deal.keyCount !== "" && deal.keyCount !== "—") {
      parts.push(deal.keyCount + " keys");
    }
    if (deal.projectType) parts.push(deal.projectType);
    return parts.length ? parts.join(" · ") : "—";
  }

  function statusBannerClass(status) {
    if (status === "allowed") return "ocs-banner--allowed";
    if (status === "blocked") return "ocs-banner--blocked";
    return "ocs-banner--limited";
  }

  function renderKvGrid(pairs) {
    var html = '<dl class="ocs-grid-2">';
    pairs.forEach(function (p) {
      html += '<div class="ocs-kv"><dt>' + esc(p[0]) + "</dt><dd>" + esc(p[1]) + "</dd></div>";
    });
    html += "</dl>";
    return html;
  }

  function renderBriefParagraphs(lines) {
    if (!lines || !lines.length) return "";
    var html = "";
    lines.forEach(function (line) {
      html += '<p class="bas-summary">' + esc(line) + "</p>";
    });
    return html;
  }

  function renderBriefSection(title, items, extraClass) {
    if (!items || !items.length) return "";
    var html =
      '<section class="bas-section bas-section--brief bas-section--keep' +
      (extraClass ? " " + extraClass : "") +
      '"><h2 class="bas-section-title">' +
      esc(title) +
      "</h2>";
    html += '<ul class="bas-detail-list">';
    items.forEach(function (item) {
      html += "<li>" + esc(item) + "</li>";
    });
    html += "</ul></section>";
    return html;
  }

  function renderStatusHighlights(data) {
    var status = data.snapshotStatus || "limited";
    var reviewLabel = data.reviewLabel || STATUS_COPY[status] || STATUS_COPY.limited;
    var isBlocked = status === "blocked";
    var isLimited = status === "limited" || data.requiresManualReview;
    var html = '<div class="bas-brief-highlights">';
    html += '<p class="bas-brief-kicker">Operator capability review</p>';
    html += '<div class="bas-brief-score-cards">';
    html +=
      '<div class="bas-brief-card"><div class="bas-brief-card-title">Review status</div><div class="bas-brief-card-body">' +
      esc(reviewLabel) +
      "</div></div>";
    if (data.confidence) {
      html +=
        '<div class="bas-brief-card"><div class="bas-brief-card-title">Confidence</div><div class="bas-brief-card-body">' +
        esc(String(data.confidence)) +
        "</div></div>";
    }
    html += "</div>";
    if (data.reviewContext) {
      html += '<p class="bas-brief-lead">' + esc(data.reviewContext) + "</p>";
    }
    if (data.requiresManualReview) {
      html += '<p class="bas-brief-lead bas-brief-lead--warn">Manual review required before external use.</p>';
    } else if (isLimited && !isBlocked) {
      html += '<p class="bas-brief-muted">Internal draft only — not approved for external distribution.</p>';
    }
    html += "</div>";
    return html;
  }

  function renderOperatingPathways(pathways) {
    if (!pathways || !pathways.length) return "";
    var html =
      '<section class="bas-section bas-section--brief bas-section--keep"><h2 class="bas-section-title">Operating pathways to validate</h2>';
    html += '<ul class="ocs-cap-list ocs-pathway-list">';
    pathways.forEach(function (p) {
      html += '<li class="ocs-cap-item ocs-pathway-item"><strong>' + esc(p.label) + "</strong>";
      html += '<span class="ocs-cap-meta"><span class="ocs-pathway-label">Relevance:</span> ' + esc(p.relevance) + "</span>";
      html += '<p class="ocs-pathway-body"><span class="ocs-pathway-label">Why it may matter:</span> ' + esc(p.whyItMayMatter || p.whyItMatters || "") + "</p>";
      html += '<p class="ocs-pathway-question"><span class="ocs-pathway-label">What to validate:</span> ' + esc(p.validationQuestion) + "</p></li>";
    });
    html += "</ul></section>";
    return html;
  }

  function renderEnrichedCapabilityCards(caps) {
    if (!caps || !caps.length) {
      return '<p class="ocs-lead">No capability areas surfaced from current inputs.</p>';
    }
    var html = '<ul class="ocs-cap-list ocs-cap-list--enriched">';
    caps.forEach(function (c) {
      html += '<li class="ocs-cap-item ocs-cap-item--enriched"><strong>' + esc(c.label) + "</strong>";
      html += '<span class="ocs-cap-meta"><span class="ocs-badge">' + esc(c.strengthLabel || c.strength || "inferred") + "</span>";
      html += " · <span class=\"ocs-pathway-label\">Relevance:</span> " + esc(c.relevance || "—") + "</span>";
      if (c.whyItMayMatter || c.whyItMatters) {
        html += '<p class="ocs-pathway-body"><span class="ocs-pathway-label">Why it may matter:</span> ' + esc(c.whyItMayMatter || c.whyItMatters) + "</p>";
      }
      if (c.whatToValidate) {
        html += '<p class="ocs-pathway-question"><span class="ocs-pathway-label">What to validate:</span> ' + esc(c.whatToValidate) + "</p>";
      }
      html +=
        '<p class="ocs-cap-source"><span class="ocs-pathway-label">Source:</span> ' +
        esc(c.sourceLabel || (c.sources || []).join(", ") || "—") +
        (c.ruleTrigger ? ' · <span class="ocs-pathway-label">Rule:</span> ' + esc(c.ruleTrigger) : "") +
        "</p></li>";
    });
    html += "</ul>";
    return html;
  }

  function renderStatusBanner(data) {
    var status = data.snapshotStatus || "limited";
    var reviewLabel = data.reviewLabel || STATUS_COPY[status] || STATUS_COPY.limited;
    var isLimited = status === "limited" || data.requiresManualReview;
    var isBlocked = status === "blocked";
    var html =
      '<div class="ocs-banner ' +
      statusBannerClass(status) +
      (data.requiresManualReview ? " ocs-banner--manual-review" : "") +
      '" role="status">';
    var statusCopy = STATUS_COPY[status] || status;
    html += '<p class="ocs-banner__label">' + esc(reviewLabel) + "</p>";
    if (String(reviewLabel).toLowerCase() !== String(statusCopy).toLowerCase()) {
      html += '<p class="ocs-banner__status">' + esc(statusCopy) + "</p>";
    }
    if (data.reviewContext) {
      html += '<p class="ocs-banner__context">' + esc(data.reviewContext) + "</p>";
    }
    if (data.requiresManualReview) {
      html += '<p class="ocs-banner__manual">Manual Review Required</p>';
    }
    if (isLimited && !isBlocked) {
      html +=
        '<p class="ocs-banner__draft-note">Internal draft only — not approved for external distribution.</p>';
    }
    html += "</div>";
    return html;
  }

  function renderCover(data, options) {
    var deal = data.deal || {};
    var generatedAt = options.generatedAt || data.generatedAt || new Date().toISOString();
    var html = '<section class="bas-cover-page bas-book-page-surface bas-avoid-break" aria-label="Cover">';
    html += '<div class="bas-cover-geometric" aria-hidden="true"></div>';
    html += '<p class="bas-cover-confidential">Draft for validation · Internal owner/advisor review</p>';
    html += '<div class="bas-cover-block">';
    html += '<p class="bas-cover-doc-type">DEALALITY OPERATOR CAPABILITY SNAPSHOT</p>';
    html += '<h1 class="bas-cover-title">' + esc(data.dealName || deal.name || "Deal") + "</h1>";
    html += '<p class="bas-cover-location">' + esc(locationLine(deal)) + "</p>";
    html += '<div class="bas-cover-accent-line" aria-hidden="true"></div>';
    html += '<p class="bas-cover-sub">' + esc(metaLine(deal)) + "</p>";
    html += '<p class="bas-cover-date">Generated ' + esc(formatDate(generatedAt)) + " · current deal inputs</p>";
    if (data.confidence) {
      html += '<p class="bas-cover-date">Confidence: ' + esc(data.confidence) + "</p>";
    }
    html += "</div>";
    html += '<p class="bas-cover-disclaimer">' + esc(data.disclaimer || COVER_NOTE) + "</p>";
    html +=
      '<div class="bas-cover-hero"><div class="bas-cover-logo-block"><img src="' +
      esc(DEALALITY_LOGO_URL) +
      '" alt="Dealality" class="bas-cover-logo-img" width="140" height="auto"></div></div>';
    html += "</section>";
    return html;
  }

  function renderPageOverview(data) {
    var ctx = data.operatingContext || {};
    var status = data.snapshotStatus || "limited";
    var missing = data.missingInputs || [];
    var diligence = data.diligenceQuestions || [];
    var gaps = data.knownGapsClarifications || data.clarifications || [];
    var isBlocked = status === "blocked";

    var html = '<div class="bas-book-page-inner bas-content-page bas-page-narrative">';
    html += renderStatusHighlights(data);
    html += '<div class="bas-brief-panel">';

    if (isBlocked) {
      html +=
        '<section class="bas-section bas-section--brief bas-section--keep"><h2 class="bas-section-title">Information needed</h2>';
      html += '<p class="bas-muted">Resolve the items below before using inferred capability themes.</p>';
      if (missing.length) {
        html += '<ul class="bas-detail-list">';
        missing.forEach(function (m) {
          html += "<li>Provide " + esc(m) + ".</li>";
        });
        html += "</ul>";
      }
      html += renderBriefSection("Known gaps / clarifications", gaps);
      if (diligence.length) {
        html += renderBriefSection("Diligence questions", diligence);
      }
      html += "</section>";
    } else {
      if ((data.executiveSummary || []).length) {
        html +=
          '<section class="bas-section bas-section--brief bas-section--keep"><h2 class="bas-section-title">Executive summary</h2>' +
          renderBriefParagraphs(data.executiveSummary) +
          "</section>";
      }
      if ((data.ownerAdvisorReviewTakeaway || []).length) {
        html +=
          '<section class="bas-section bas-section--brief bas-section--keep"><h2 class="bas-section-title">Owner/Advisor review takeaway</h2>' +
          renderBriefParagraphs(data.ownerAdvisorReviewTakeaway) +
          "</section>";
      }
      if ((data.operatingModelTransitionsToValidate || []).length) {
        html += renderBriefSection("Operating model transitions to validate", data.operatingModelTransitionsToValidate);
      }
      if ((data.whyOperatorStrategyMatters || []).length) {
        html +=
          '<section class="bas-section bas-section--brief bas-section--keep"><h2 class="bas-section-title">Why operator strategy matters</h2>' +
          renderBriefParagraphs(data.whyOperatorStrategyMatters) +
          "</section>";
      }
      html +=
        '<section class="bas-section bas-section--brief bas-section--keep"><h2 class="bas-section-title">Operating context</h2>';
      html += renderKvGrid([
        ["Current operating model", ctx.currentOperatingModel],
        ["Preferred future model", ctx.preferredFutureOperatingModel],
        ["Opening / transition phase", ctx.openingTransitionPhase],
        ["Primary market region", ctx.primaryMarketRegion],
        ["Project type", ctx.projectType],
        ["Stage of development", ctx.stage],
        ["Operator path in scope", ctx.operatorInScope ? "Yes" : "No / limited"],
      ]);
      html += "</section>";
      html += renderOperatingPathways(data.operatingPathways || []);
      html += renderBriefSection("Decision points before outreach", data.decisionPointsBeforeOutreach || []);
      if ((data.brandManagedGuidance || []).length) {
        html += renderBriefSection("Brand-managed pathway notes", data.brandManagedGuidance);
      }
      if (gaps.length) {
        html += renderBriefSection("Known gaps / clarifications", gaps);
      }
    }
    html += "</div></div>";
    return html;
  }

  function renderPageTechnical(data) {
    var status = data.snapshotStatus || "limited";
    var caps = data.capabilityAreas || [];
    var diligence = data.diligenceQuestions || [];
    var reporting = data.reporting || {};
    var isBlocked = status === "blocked";
    var newBuild = data.newBuildGuidance || [];

    var html = '<div class="bas-book-page-inner bas-content-page bas-page-technical">';
    html += '<div class="bas-brief-highlights">';
    html += '<p class="bas-brief-kicker">Capability detail</p>';
    html +=
      '<p class="bas-brief-lead">Supporting capability signals, reporting context, and diligence themes — not operator recommendations or shortlists.</p>';
    html += "</div>";
    html += '<div class="bas-brief-panel">';
    if (!isBlocked) {
      html += renderBriefSection("Capability implications", data.capabilityImplications || []);
      html +=
        '<section class="bas-section bas-section--brief bas-section--keep"><h2 class="bas-section-title">Capability areas to review</h2>';
      html += '<p class="bas-muted">Themes from stated priorities and deal context.</p>';
      html += renderEnrichedCapabilityCards(caps);
      if ((data.ruleTriggers || []).length) {
        html +=
          '<p class="bas-muted ocs-rules"><strong>Rules triggered:</strong> ' +
          esc((data.ruleTriggers || []).join(", ")) +
          "</p>";
      }
      html += "</section>";
      if (newBuild.length) {
        html +=
          '<section class="bas-section bas-section--brief bas-section--keep"><h2 class="bas-section-title">New build operating guidance</h2><ul class="ocs-cap-list">';
        newBuild.forEach(function (row) {
          html += '<li class="ocs-cap-item"><strong>' + esc(row.title) + "</strong>";
          html += '<span class="ocs-cap-meta">' + esc(row.detail) + "</span></li>";
        });
        html += "</ul></section>";
      }
      html +=
        '<section class="bas-section bas-section--brief bas-section--keep"><h2 class="bas-section-title">Reporting &amp; oversight</h2>';
      html += renderKvGrid([
        ["Owner reporting frequency", reporting.ownerReportingFrequency],
        ["Owner reporting package", reporting.ownerReportingPackage],
      ]);
      html += "</section>";
      html += renderBriefSection("Diligence questions", diligence);
    } else {
      html +=
        '<section class="bas-section bas-section--brief"><h2 class="bas-section-title">Capability detail</h2>';
      html += '<p class="bas-muted">Not available until required deal inputs are complete.</p></section>';
      html += renderBriefSection("Diligence questions", diligence);
    }
    html +=
      '<footer class="bas-output-note bas-output-note--brief bas-section--keep"><p>' +
      esc(data.disclaimer || COVER_NOTE) +
      "</p></footer>";
    html += "</div></div>";
    return html;
  }

  function wrapBookPage(index, innerHtml, active) {
    return (
      '<div class="bas-book-page' +
      (active ? " active" : "") +
      '" data-bas-page="' +
      index +
      '" role="region" aria-hidden="' +
      (active ? "false" : "true") +
      '">' +
      innerHtml +
      "</div>"
    );
  }

  function buildFullPageNav(options) {
    if (!options.fullPage) return "";
    return (
      '<nav class="snapshot-page-nav bas-no-print" aria-label="Page">' +
      '<a class="snapshot-page-back" href="' +
      esc(options.backHref || "/my-deals.html") +
      '">' +
      esc(options.backLabel || "\u2190 Back to My Deals") +
      "</a></nav>"
    );
  }

  function buildHtml(data, options) {
    options = options || {};
    var snapClass = "operator-capability-snapshot brand-alignment-snapshot";
    if (options.embed) snapClass += " bas--embed";
    if (options.fullPage) snapClass += " bas--full-page";

    var html = '<div class="' + snapClass + '">';
    html += buildFullPageNav(options);
    html += '<div class="bas-toolbar bas-no-print"><div class="bas-toolbar-actions">';
    html +=
      '<span class="bas-print-tip bas-no-print">Turn off <strong>Headers and footers</strong> and enable <strong>Background graphics</strong> in the print dialog.</span>';
    html += '<div class="bas-toolbar-buttons bas-no-print">';
    if (options.embed && options.fullPageHref) {
      html +=
        '<a class="bas-btn bas-btn-secondary" href="' +
        esc(options.fullPageHref) +
        '" target="_blank" rel="noopener">Open full page</a>';
    }
    html +=
      '<button type="button" class="bas-btn bas-btn-primary bas-toolbar-print" data-bas-print>Print / Save as PDF</button>';
    html += "</div></div></div>";

    html += '<div class="bas-book-shell"><article class="bas-document bas-book-document">';
    html += '<div class="bas-book-viewport" data-bas-book-viewport tabindex="0"><div class="bas-book-stage">';
    html += wrapBookPage(0, renderCover(data, options), true);
    html += wrapBookPage(1, renderPageOverview(data), false);
    html += wrapBookPage(2, renderPageTechnical(data), false);
    html += "</div>";
    html +=
      '<button type="button" class="bas-turn-btn bas-turn-prev bas-no-print" data-bas-turn-prev aria-label="Previous page" disabled>‹</button>';
    html +=
      '<button type="button" class="bas-turn-btn bas-turn-next bas-no-print" data-bas-turn-next aria-label="Next page">›</button>';
    html += '<span class="bas-page-indicator bas-no-print" data-bas-page-indicator>1 of 3</span>';
    html += "</div>";
    if (options.footerHtml) {
      html += '<div class="bas-host-footer bas-no-print">' + options.footerHtml + "</div>";
    }
    html += "</article></div></div>";
    return html;
  }

  function bindPageFlip(root) {
    if (!root) return;
    var viewport = root.querySelector("[data-bas-book-viewport]");
    var pages = viewport ? Array.prototype.slice.call(viewport.querySelectorAll(".bas-book-page")) : [];
    if (!viewport || pages.length < 2) return;
    var current = 0;
    var prevBtn = root.querySelector("[data-bas-turn-prev]");
    var nextBtn = root.querySelector("[data-bas-turn-next]");
    var indicator = root.querySelector("[data-bas-page-indicator]");
    var animating = false;
    var flipMs = 750;
    function updateControls() {
      if (indicator) indicator.textContent = current + 1 + " of " + pages.length;
      if (prevBtn) prevBtn.disabled = current === 0 || animating;
      if (nextBtn) nextBtn.disabled = current === pages.length - 1 || animating;
    }
    function clearFlipClasses() {
      pages.forEach(function (p) {
        p.classList.remove("flip-out-forward", "flip-out-back", "flip-in-forward", "flip-in-back");
      });
    }
    function goTo(nextIndex) {
      if (animating || nextIndex === current) return;
      if (nextIndex < 0 || nextIndex >= pages.length) return;
      animating = true;
      updateControls();
      var outPage = pages[current];
      var inPage = pages[nextIndex];
      var forward = nextIndex > current;
      clearFlipClasses();
      outPage.classList.add(forward ? "flip-out-forward" : "flip-out-back");
      inPage.classList.add(forward ? "flip-in-forward" : "flip-in-back");
      inPage.classList.add("active");
      inPage.setAttribute("aria-hidden", "false");
      global.setTimeout(function () {
        outPage.classList.remove("active", "flip-out-forward", "flip-out-back");
        outPage.setAttribute("aria-hidden", "true");
        inPage.classList.remove("flip-in-forward", "flip-in-back");
        pages.forEach(function (p, i) {
          if (i !== nextIndex) {
            p.classList.remove("active");
            p.setAttribute("aria-hidden", "true");
          }
        });
        current = nextIndex;
        animating = false;
        updateControls();
      }, flipMs);
    }
    if (prevBtn) prevBtn.addEventListener("click", function () { goTo(current - 1); });
    if (nextBtn) nextBtn.addEventListener("click", function () { goTo(current + 1); });
    viewport.addEventListener("keydown", function (e) {
      if (e.key === "ArrowRight" || e.key === "PageDown") { e.preventDefault(); goTo(current + 1); }
      else if (e.key === "ArrowLeft" || e.key === "PageUp") { e.preventDefault(); goTo(current - 1); }
    });
    updateControls();
  }

  function getSnapshotRoot(container) {
    if (!container) return null;
    if (container.classList && container.classList.contains("operator-capability-snapshot")) return container;
    return container.querySelector(".operator-capability-snapshot");
  }

  function printSnapshot(root) {
    var snapshot = getSnapshotRoot(root);
    if (!snapshot) {
      window.print();
      return;
    }
    var printHost = document.getElementById("bas-print-host");
    if (!printHost) {
      printHost = document.createElement("div");
      printHost.id = "bas-print-host";
      printHost.setAttribute("aria-hidden", "true");
      document.body.appendChild(printHost);
    }
    var clone = snapshot.cloneNode(true);
    clone.classList.add("bas-printing");
    clone.classList.remove("bas--embed");
    printHost.innerHTML = "";
    printHost.appendChild(clone);
    document.body.classList.add("bas-print-active");
    function cleanup() {
      document.body.classList.remove("bas-print-active");
      printHost.innerHTML = "";
    }
    function onAfterPrint() {
      cleanup();
      window.removeEventListener("afterprint", onAfterPrint);
    }
    window.addEventListener("afterprint", onAfterPrint);
    global.setTimeout(function () {
      if (document.body.classList.contains("bas-print-active")) cleanup();
    }, 3000);
    window.requestAnimationFrame(function () {
      global.setTimeout(function () { window.print(); }, 50);
    });
  }

  function bindPrint(root) {
    if (!root) return;
    var btn = root.querySelector("[data-bas-print]");
    if (btn && !btn._ocsPrintBound) {
      btn._ocsPrintBound = true;
      btn.addEventListener("click", function () { printSnapshot(root); });
    }
  }

  function render(container, data, options) {
    if (!container || !data) return null;
    options = options || {};
    container.innerHTML = buildHtml(data, options);
    bindPrint(container);
    bindPageFlip(container);
    return data;
  }

  global.OperatorCapabilitySnapshot = {
    render: render,
    buildHtml: buildHtml,
    STATUS_COPY: STATUS_COPY,
    COVER_NOTE: COVER_NOTE,
  };
})(typeof window !== "undefined" ? window : globalThis);
