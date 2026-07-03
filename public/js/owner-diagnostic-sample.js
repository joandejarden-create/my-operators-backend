/**
 * Owner Diagnostic — Phase A sample (Partner Overview visual system).
 * Static content only; print-first five-page layout.
 */
(function (global) {
  "use strict";

  function esc(t) {
    return String(t == null ? "" : t)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;");
  }

  function copy() {
    return global.OwnerDiagnosticCopy || null;
  }

  function logoSrc(c) {
    return esc((c && (c.logoPath || c.logoUrl)) || "/assets/dealality-logo.png");
  }

  function renderGrayBand(c, line1, line2, options) {
    options = options || {};
    var cv = c.cover || {};
    var outputNote =
      line2 && line2.length > 120
        ? '<p class="ods-gray-band-note"><strong>Output Note.</strong> ' + esc(line2) + "</p>"
        : "";
    var subLine =
      line2 && line2.length <= 120
        ? '<span class="ods-gray-band-line ods-gray-band-sub">' + esc(line2) + "</span>"
        : "";
    return (
      '<footer class="ods-gray-band' +
      (outputNote ? " ods-gray-band--stacked" : "") +
      (options.pageFooter ? " ods-gray-band--page-footer" : "") +
      '">' +
      '<div class="ods-gray-band-brand">' +
      '<img src="' +
      logoSrc(c) +
      '" alt="Dealality" class="ods-band-logo" width="96" height="auto">' +
      "</div>" +
      '<div class="ods-gray-band-meta">' +
      outputNote +
      '<span class="ods-gray-band-line">' +
      esc(line1 || cv.footerDate || "") +
      "</span>" +
      '<span class="ods-gray-band-line ods-gray-band-links">' +
      '<a href="' +
      esc(cv.footerWebsiteHref || "https://www.dealality.com") +
      '">' +
      esc(cv.footerWebsite || "www.Dealality.com") +
      "</a></span>" +
      subLine +
      "</div></footer>"
    );
  }

  function renderPageHeader(c, title, subtitle) {
    return (
      '<header class="ods-page-header">' +
      '<div class="ods-page-header-row">' +
      '<img src="' +
      logoSrc(c) +
      '" alt="Dealality" class="ods-header-logo" width="108" height="auto">' +
      '<div class="ods-page-header-copy">' +
      '<h2 class="ods-page-header-title">' +
      esc(title).toUpperCase() +
      "</h2>" +
      (subtitle ? '<p class="ods-page-header-sub">' + esc(subtitle) + "</p>" : "") +
      "</div></div>" +
      '<div class="ods-page-header-accent" aria-hidden="true"></div></header>'
    );
  }

  function renderSectionHeading(title) {
    return (
      '<h3 class="ods-section-title">' +
      '<span class="ods-section-title-text">' +
      esc(title) +
      "</span>" +
      '<span class="ods-section-title-line" aria-hidden="true"></span></h3>'
    );
  }

  function renderSectionNote(text) {
    return (
      '<p class="ods-section-note"><span class="ods-red-dot" aria-hidden="true"></span>' +
      esc(text) +
      "</p>"
    );
  }

  function renderInterpretationCallout(callout) {
    if (!callout || !callout.title) return "";
    return (
      '<div class="ods-interpretation-callout ods-avoid-break" role="note">' +
      '<p class="ods-interpretation-label">' +
      esc(callout.title) +
      "</p>" +
      '<p class="ods-interpretation-text">' +
      esc(callout.text) +
      "</p></div>"
    );
  }

  function renderTable(headers, rows) {
    if (!rows || !rows.length) return '<p class="ods-muted">None in this sample.</p>';
    var html = '<div class="ods-table-wrap"><table class="ods-table"><thead><tr>';
    headers.forEach(function (h) {
      html += "<th>" + esc(h) + "</th>";
    });
    html += "</tr></thead><tbody>";
    rows.forEach(function (row, ri) {
      html += '<tr class="' + (ri % 2 === 1 ? "ods-table-row--alt" : "") + '">';
      row.forEach(function (cell) {
        html += "<td>" + esc(cell) + "</td>";
      });
      html += "</tr>";
    });
    html += "</tbody></table></div>";
    return html;
  }

  function renderDetailCard(rows) {
    if (!rows || !rows.length) return "";
    var html = '<div class="ods-detail-card">';
    rows.forEach(function (row, i) {
      html +=
        '<div class="ods-detail-row' +
        (i === rows.length - 1 ? " ods-detail-row--last" : "") +
        '"><span class="ods-detail-label">' +
        esc(row.label) +
        '</span><span class="ods-detail-value">' +
        esc(row.value) +
        "</span></div>";
    });
    html += "</div>";
    return html;
  }

  function renderList(items) {
    if (!items || !items.length) return '<p class="ods-muted">None listed in this sample.</p>';
    var html = '<ul class="ods-list">';
    items.forEach(function (item) {
      html += "<li>" + esc(item) + "</li>";
    });
    html += "</ul>";
    return html;
  }

  function renderCard(title, innerHtml, extraClass) {
    return (
      '<div class="ods-card ods-avoid-break' +
      (extraClass ? " " + extraClass : "") +
      '">' +
      renderSectionHeading(title) +
      '<div class="ods-card-body">' +
      innerHtml +
      "</div></div>"
    );
  }

  function renderAtAGlance(tiles) {
    var html = '<div class="ods-glance ods-avoid-break" aria-label="At-a-glance diagnostic dashboard">';
    html += renderSectionHeading("At-a-Glance");
    html += '<div class="ods-glance-grid">';
    (tiles || []).forEach(function (item) {
      html +=
        '<div class="ods-glance-tile"><span class="ods-glance-label">' +
        esc(item.label) +
        '</span><span class="ods-glance-value">' +
        esc(item.value) +
        "</span></div>";
    });
    html += "</div></div>";
    return html;
  }

  function renderWhatIsIsNot(box) {
    if (!box) return "";
    var html = '<div class="ods-card ods-card--muted ods-is-isnot ods-avoid-break">';
    html += renderSectionHeading("What This Report Is / Is Not");
    html += '<div class="ods-is-isnot-cols">';
    html += '<div class="ods-is-col"><h4 class="ods-is-title">This Report</h4><ul class="ods-list ods-list--compact">';
    (box.is || []).forEach(function (line) {
      html += "<li>" + esc(line) + "</li>";
    });
    html += "</ul></div>";
    html += '<div class="ods-is-col"><h4 class="ods-is-title">This Report Is Not</h4><ul class="ods-list ods-list--compact">';
    (box.isNot || []).forEach(function (line) {
      html += "<li>" + esc(line) + "</li>";
    });
    html += "</ul></div></div></div>";
    return html;
  }

  function renderCover(c) {
    var p = c.sampleProject;
    var cv = c.cover || {};
    var badges = cv.badges || [];
    var html = '<section class="ods-print-page ods-page-cover" data-ods-page="0" aria-label="Cover">';
    html += '<div class="ods-cover-geometric" aria-hidden="true"></div>';
    html += '<div class="ods-cover-shell">';

    html += '<div class="ods-cover-top">';
    html +=
      '<img src="' +
      logoSrc(c) +
      '" alt="Dealality" class="ods-cover-logo-main" width="140" height="auto">';
    html += '<div class="ods-cover-badges">';
    badges.forEach(function (badge) {
      html += '<span class="ods-cover-badge">' + esc(badge) + "</span>";
    });
    html += "</div></div>";

    html += '<div class="ods-cover-main">';
    html += '<div class="ods-cover-hero">';
    html += '<h1 class="ods-cover-title-line1">' + esc(cv.titleLine1 || c.pageTitle) + "</h1>";
    html += '<h2 class="ods-cover-title-line2">' + esc(cv.titleLine2 || "Sample Output") + "</h2>";
    html += '<div class="ods-cover-accent-line" aria-hidden="true"></div>';
    html += '<p class="ods-cover-tagline">' + esc(cv.tagline || "") + "</p>";
    html += '<p class="ods-cover-project-name">' + esc(p.projectName) + "</p>";
    html += "</div>";

    html += '<div class="ods-cover-facts">';
    html +=
      '<div class="ods-fact-card"><span class="ods-fact-label">Market</span><span class="ods-fact-value">' +
      esc(p.market) +
      "</span></div>";
    html +=
      '<div class="ods-fact-card"><span class="ods-fact-label">Asset Type</span><span class="ods-fact-value">' +
      esc(p.assetType) +
      "</span></div>";
    html +=
      '<div class="ods-fact-card"><span class="ods-fact-label">Room Count</span><span class="ods-fact-value">' +
      esc(p.roomCount) +
      "</span></div>";
    html +=
      '<div class="ods-fact-card"><span class="ods-fact-label">Stage</span><span class="ods-fact-value">' +
      esc(p.stage) +
      "</span></div>";
    html +=
      '<div class="ods-fact-card ods-fact-card--wide"><span class="ods-fact-label">Current Status</span><span class="ods-fact-value">' +
      esc(p.currentStatus) +
      "</span></div>";
    html += "</div></div>";

    html +=
      '<div class="ods-cover-confidential-callout"><p>' + esc(c.confidentialityStatement) + "</p></div>";

    html += renderGrayBand(c, cv.footerDate);
    html += "</div></section>";
    return html;
  }

  function renderExecutiveMemoPage(c) {
    var m = c.executiveMemo;
    var html = '<section class="ods-print-page ods-page-memo" data-ods-page="1" aria-label="Executive Memo">';
    html += renderPageHeader(c, "Executive Memo", "Owner-facing decision summary · illustrative sample");
    html += '<div class="ods-page-body ods-memo-body">';
    html += renderAtAGlance(c.atAGlance);

    html += '<div class="ods-memo-grid">';
    html +=
      '<div class="ods-memo-block ods-avoid-break">' +
      renderSectionHeading("Situation") +
      '<p class="ods-text">' +
      esc(m.situation) +
      "</p></div>";
    html +=
      '<div class="ods-memo-block ods-avoid-break">' +
      renderSectionHeading("Current Posture") +
      '<p class="ods-text">' +
      esc(m.currentPosture) +
      "</p></div>";
    html += "</div>";

    html += '<div class="ods-memo-cols">';
    html +=
      '<div class="ods-memo-block ods-avoid-break">' +
      renderSectionHeading("What Looks Clear So Far") +
      renderList(m.clearSoFar) +
      "</div>";
    html +=
      '<div class="ods-memo-block ods-avoid-break">' +
      renderSectionHeading("What Is Not Clear Yet") +
      renderList(m.notClearYet) +
      "</div>";
    html += "</div>";

    html += '<div class="ods-memo-cols">';
    html +=
      '<div class="ods-memo-block ods-avoid-break">' +
      renderSectionHeading("Brand Path View") +
      '<p class="ods-text">' +
      esc(m.brandPathView) +
      "</p></div>";
    html +=
      '<div class="ods-memo-block ods-avoid-break">' +
      renderSectionHeading("Operator Path View") +
      '<p class="ods-text">' +
      esc(m.operatorPathView) +
      "</p></div>";
    html += "</div>";

    html += '<div class="ods-takeaway ods-avoid-break" role="note">';
    html += '<p class="ods-takeaway-label">Owner / Advisor Takeaway</p>';
    html += '<p class="ods-takeaway-text">' + esc(c.takeawayCallout) + "</p>";
    html += '<p class="ods-text ods-takeaway-detail">' + esc(m.takeaway) + "</p>";
    html += "</div>";

    html += '<div class="ods-memo-cols">';
    html +=
      '<div class="ods-memo-block ods-avoid-break">' +
      renderSectionHeading("Suggested Next Step") +
      '<p class="ods-text">' +
      esc(m.suggestedNextStep) +
      "</p></div>";
    html +=
      '<div class="ods-memo-block ods-avoid-break">' +
      renderSectionHeading("Outreach Posture") +
      '<p class="ods-text">' +
      esc(m.outreachPosture) +
      "</p></div>";
    html += "</div>";

    html += renderWhatIsIsNot(c.whatIsIsNot);
    html += "</div>";
    html += renderGrayBand(c, c.footerLine, null, { pageFooter: true });
    html += "</section>";
    return html;
  }

  function renderSupportingProjectReadiness(c) {
    var ps = c.projectSummary;
    var dr = c.dealReadiness;
    var html =
      '<section class="ods-print-page ods-page-evidence" data-ods-page="2" aria-label="Supporting Evidence">';
    html += renderPageHeader(c, "Supporting Evidence", "Project summary and deal readiness");
    html += '<div class="ods-page-body ods-page-body--spread">';

    html += renderCard(
      "Project Summary",
      '<p class="ods-text ods-lead">' +
        esc(ps.lead) +
        "</p>" +
        renderDetailCard(ps.highlights)
    );

    var readinessInner =
      '<p class="ods-readiness-lead"><span class="ods-readiness-score">' +
      esc(dr.stage) +
      " · " +
      esc(dr.score) +
      ' / 100</span> — ' +
      esc(dr.interpretation) +
      "</p>" +
      renderTable(
        ["Review Area", "Status", "Notes"],
        (dr.reviewAreas || []).map(function (r) {
          return [r.area, r.status, r.notes];
        })
      ) +
      '<h4 class="ods-subsection-title">Key Strengths</h4>' +
      renderList(dr.strengths);
    html += renderCard("Deal Readiness Snapshot", readinessInner);

    html += '<div class="ods-page-fill-spacer" aria-hidden="true"></div>';
    html +=
      renderInterpretationCallout(
        c.pageCallouts && c.pageCallouts.ownerReadinessInterpretation
      );

    html += "</div>";
    html += renderGrayBand(c, c.footerLine, null, { pageFooter: true });
    html += "</section>";
    return html;
  }

  function renderSupportingAlignment(c) {
    var ba = c.brandAlignment;
    var oa = c.operatorAlignment;
    var note = (c.sectionNotes && c.sectionNotes.alignmentReview) || "For owner review only · not a recommendation";
    var html =
      '<section class="ods-print-page ods-page-alignment" data-ods-page="3" aria-label="Alignment Evidence">';
    html += renderPageHeader(c, "Alignment Evidence", "Fit signals for structured review — not recommendations");
    html += '<div class="ods-page-body ods-page-body--spread">';

    html += renderCard(
      "Brand Alignment Snapshot",
      '<p class="ods-text">' +
        esc(ba.summary) +
        "</p>" +
        renderSectionNote(note) +
        renderTable(
          ["Brand Pathway (Illustrative)", "Fit Signal", "Key Consideration"],
          (ba.brands || []).map(function (b) {
            return [b.name, b.signal, b.consideration];
          })
        )
    );

    html += renderCard(
      "Operator Alignment Snapshot",
      '<p class="ods-text">' +
        esc(oa.summary) +
        "</p>" +
        renderSectionNote(note) +
        renderTable(
          ["Operating Company (Illustrative)", "Alignment Signal", "Key Consideration"],
          (oa.companies || []).map(function (row) {
            return [row.name, row.signal, row.consideration];
          })
        )
    );

    html += '<div class="ods-page-fill-spacer" aria-hidden="true"></div>';
    html +=
      renderInterpretationCallout(
        c.pageCallouts && c.pageCallouts.alignmentInterpretation
      );

    html += "</div>";
    html += renderGrayBand(c, c.footerLine, null, { pageFooter: true });
    html += "</section>";
    return html;
  }

  function renderTieredMissing(tiered) {
    var rows = [];
    function addTier(label, items) {
      (items || []).forEach(function (r) {
        rows.push([label, r.item, r.why]);
      });
    }
    addTier("Decision-Blocking", tiered.decisionBlocking);
    addTier("Review-Limiting", tiered.reviewLimiting);
    addTier("Enhancement", tiered.enhancement);
    if (!rows.length) return '<p class="ods-muted">None in this sample.</p>';
    return renderTable(["Tier", "Item", "Why It Matters"], rows);
  }

  function renderGroupedQuestions(q) {
    var groups = [
      { title: "Owner Confirmation", items: q.ownerConfirmation },
      { title: "Brand Conversations", items: q.brandConversations },
      { title: "Operator Conversations", items: q.operatorConversations },
      { title: "Advisor / Counsel Review", items: q.advisorCounsel },
    ];
    var html = '<div class="ods-questions-grid">';
    groups.forEach(function (g) {
      html += '<div class="ods-question-group ods-avoid-break">';
      html += '<h4 class="ods-subsection-title">' + esc(g.title) + "</h4>";
      html += renderList(g.items);
      html += "</div>";
    });
    html += "</div>";
    return html;
  }

  function renderDetailPage(c) {
    var or = c.outreachReadiness;
    var html =
      '<section class="ods-print-page ods-page-detail" data-ods-page="4" aria-label="Report Detail">';
    html += renderPageHeader(
      c,
      "Report Detail",
      "Checklists, questions, outreach readiness, and confidentiality"
    );
    html += '<div class="ods-page-body ods-page-body--spread ods-detail-body">';

    html += '<div class="ods-detail-cols">';
    html += '<div class="ods-detail-col">';
    html += renderCard("Missing Information Checklist", renderTieredMissing(c.missingInformationTiered));
    html += "</div>";
    html += '<div class="ods-detail-col">';
    html += renderCard("Suggested Questions to Clarify", renderGroupedQuestions(c.suggestedQuestions));
    html += "</div></div>";

    html += '<div class="ods-detail-split">';
    var outreachInner =
      '<p class="ods-text"><strong>Posture:</strong> ' +
      esc(or.currentPosture) +
      "</p>" +
      '<p class="ods-text"><strong>Selected outreach:</strong> ' +
      esc(or.whatSelectedMeans) +
      "</p>" +
      '<h4 class="ods-subsection-title">Preconditions</h4>' +
      renderList(or.preconditions) +
      '<h4 class="ods-subsection-title">After Approval</h4>' +
      renderList(or.afterOwnerApproval) +
      '<p class="ods-text"><strong>Sharing:</strong> ' +
      esc(or.sharingPosture) +
      "</p>";
    html += renderCard("Selected Outreach Readiness", outreachInner, "ods-card--half");

    var confInner =
      '<div class="ods-confidentiality-box">' +
      esc(c.confidentialityStatement) +
      "</div>" +
      '<p class="ods-governance">' +
      esc(c.governanceFooter) +
      "</p>";
    html += renderCard("Confidentiality & Owner Control", confInner, "ods-card--half ods-card--confidential");
    html += "</div>";

    html += "</div>";
    html += renderGrayBand(c, c.footerLine, c.outputNote, { pageFooter: true });
    html += "</section>";
    return html;
  }

  function buildFullPageNav(options) {
    if (!options.fullPage) return "";
    return (
      '<nav class="snapshot-page-nav ods-no-print" aria-label="Page">' +
      '<a class="snapshot-page-back" href="' +
      esc(options.backHref || "/") +
      '">' +
      esc(options.backLabel || "\u2190 Back") +
      "</a></nav>"
    );
  }

  function buildHtml(c, options) {
    options = options || {};
    var snapClass = "owner-diagnostic-document";
    if (options.embed) snapClass += " ods--embed";
    if (options.fullPage) snapClass += " ods--full-page";

    var pages = [
      renderCover(c),
      renderExecutiveMemoPage(c),
      renderSupportingProjectReadiness(c),
      renderSupportingAlignment(c),
      renderDetailPage(c),
    ];
    var pageCount = pages.length;

    var html = '<div class="' + snapClass + '">';
    html += buildFullPageNav(options);
    html += '<div class="ods-toolbar ods-no-print"><div class="ods-toolbar-inner">';
    html +=
      '<span class="ods-print-tip">Turn off <strong>Headers and footers</strong> and enable <strong>Background graphics</strong> in the print dialog.</span>';
    html +=
      '<button type="button" class="ods-btn ods-btn-primary" data-ods-print>Print / Save as PDF</button>';
    html += "</div></div>";

    html += '<div class="ods-viewport-shell">';
    html += '<div class="ods-viewport" data-ods-viewport tabindex="0">';
    html += '<div class="ods-stage">';
    pages.forEach(function (pageHtml, i) {
      html += pageHtml.replace(
        'class="ods-print-page',
        'class="ods-print-page' + (i === 0 ? " ods-print-page--active" : "")
      );
    });
    html += "</div>";
    html +=
      '<button type="button" class="ods-turn ods-turn-prev ods-no-print" data-ods-turn-prev aria-label="Previous page" disabled>‹</button>';
    html +=
      '<button type="button" class="ods-turn ods-turn-next ods-no-print" data-ods-turn-next aria-label="Next page">›</button>';
    html +=
      '<span class="ods-page-indicator ods-no-print" data-ods-page-indicator>1 of ' +
      pageCount +
      "</span>";
    html += "</div></div></div>";
    return html;
  }

  function bindPageFlip(root) {
    if (!root) return;
    var viewport = root.querySelector("[data-ods-viewport]");
    var pages = viewport
      ? Array.prototype.slice.call(viewport.querySelectorAll(".ods-print-page"))
      : [];
    if (!viewport || pages.length < 2) return;
    var current = 0;
    var prevBtn = root.querySelector("[data-ods-turn-prev]");
    var nextBtn = root.querySelector("[data-ods-turn-next]");
    var indicator = root.querySelector("[data-ods-page-indicator]");

    function showPage(index) {
      if (index < 0 || index >= pages.length) return;
      pages.forEach(function (p, i) {
        p.classList.toggle("ods-print-page--active", i === index);
        p.setAttribute("aria-hidden", i === index ? "false" : "true");
      });
      current = index;
      if (indicator) indicator.textContent = current + 1 + " of " + pages.length;
      if (prevBtn) prevBtn.disabled = current === 0;
      if (nextBtn) nextBtn.disabled = current === pages.length - 1;
    }

    if (prevBtn) prevBtn.addEventListener("click", function () { showPage(current - 1); });
    if (nextBtn) nextBtn.addEventListener("click", function () { showPage(current + 1); });
    viewport.addEventListener("keydown", function (e) {
      if (e.key === "ArrowRight" || e.key === "PageDown") {
        e.preventDefault();
        showPage(current + 1);
      } else if (e.key === "ArrowLeft" || e.key === "PageUp") {
        e.preventDefault();
        showPage(current - 1);
      }
    });
    showPage(0);
  }

  function bindPrint() {
    var btn = document.querySelector("[data-ods-print]");
    if (btn && !btn._odsPrintBound) {
      btn._odsPrintBound = true;
      btn.addEventListener("click", function () {
        global.print();
      });
    }
  }

  function render(container, options) {
    options = options || {};
    var c = copy();
    if (!container || !c) {
      if (container) {
        container.innerHTML =
          '<div class="ods-page-error">Owner diagnostic copy failed to load.</div>';
      }
      return null;
    }
    if (options.fullPage == null) options.fullPage = true;
    container.innerHTML = buildHtml(c, options);
    bindPrint();
    bindPageFlip(container);
    if (options.autoPrint) {
      global.setTimeout(function () {
        global.print();
      }, 600);
    }
    return c;
  }

  global.OwnerDiagnosticSample = {
    render: render,
    buildHtml: buildHtml,
  };
})(typeof window !== "undefined" ? window : globalThis);
