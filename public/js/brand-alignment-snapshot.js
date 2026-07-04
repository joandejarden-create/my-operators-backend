/**
 * Brand Alignment Snapshot — two-page document renderer (narrative + technical detail).
 * Data: POST /api/ai/brand-alignment-snapshot
 */
(function (global) {
  "use strict";

  var OUTPUT_NOTE =
    "This Brand Alignment Snapshot organizes potential brand alignment signals based on current deal and brand inputs. " +
    "It is intended to support structured owner/advisor review and does not constitute a recommendation, " +
    "endorsement, brand approval, franchise advice, valuation, legal advice, or investment advice.";

  var COVER_NOTE =
    "This output organizes brand alignment signals based on current deal inputs. " +
    "It is intended to support internal owner/advisor review and does not constitute a recommendation, endorsement, " +
    "valuation, legal advice, franchise advice, or investment advice.";

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

  function metaLine(deal) {
    deal = deal || {};
    var parts = [];
    if (deal.keyCount != null && deal.keyCount !== "") parts.push(deal.keyCount + " keys");
    if (deal.projectType) parts.push(deal.projectType);
    if (deal.targetPositioning) parts.push(deal.targetPositioning);
    return parts.length ? parts.join(" · ") : "—";
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

  function renderCover(data, options) {
    var deal = data.deal || {};
    var generatedAt = options.generatedAt || data.generatedAt || new Date().toISOString();
    var html = '<section class="bas-cover-page bas-book-page-surface bas-avoid-break" aria-label="Cover">';
    html += '<div class="bas-cover-geometric" aria-hidden="true"></div>';
    html += '<p class="bas-cover-confidential">Draft for validation · Internal owner/advisor review</p>';
    html += '<div class="bas-cover-block">';
    html += '<p class="bas-cover-doc-type">DEALALITY BRAND ALIGNMENT SNAPSHOT</p>';
    html += '<h1 class="bas-cover-title">' + esc(deal.name || "Deal") + "</h1>";
    html += '<p class="bas-cover-location">' + esc(locationLine(deal)) + "</p>";
    html += '<div class="bas-cover-accent-line" aria-hidden="true"></div>';
    html += '<p class="bas-cover-sub">' + esc(metaLine(deal)) + "</p>";
    html += '<p class="bas-cover-date">Generated ' + esc(formatDate(generatedAt)) + " · current deal inputs</p>";
    html += "</div>";
    html += '<p class="bas-cover-disclaimer">' + esc(COVER_NOTE) + "</p>";
    html +=
      '<div class="bas-cover-hero"><div class="bas-cover-logo-block"><img src="' +
      esc(DEALALITY_LOGO_URL) +
      '" alt="Dealality" class="bas-cover-logo-img" width="140" height="auto"></div></div>';
    html += "</section>";
    return html;
  }

  function renderTable(headers, rows, keepTogether) {
    var wrapClass = "bas-table-wrap" + (keepTogether ? " bas-table-wrap--keep" : "");
    var html = '<div class="' + wrapClass + '"><table class="bas-brief-table"><thead><tr>';
    headers.forEach(function (h) {
      html += "<th>" + esc(h) + "</th>";
    });
    html += "</tr></thead><tbody>";
    rows.forEach(function (row) {
      html += "<tr>";
      row.forEach(function (cell) {
        html += "<td>" + esc(cell) + "</td>";
      });
      html += "</tr>";
    });
    html += "</tbody></table></div>";
    return html;
  }

  function renderSummaryParagraphs(summary) {
    var paras = summary && summary.brandAlignmentSummaryParagraphs;
    var html = "";
    if (paras && paras.length) {
      paras.forEach(function (p) {
        html += '<p class="bas-summary">' + esc(p) + "</p>";
      });
      return html;
    }
    if (summary && summary.brandAlignmentSummary) {
      html += '<p class="bas-summary">' + esc(summary.brandAlignmentSummary) + "</p>";
    }
    return html;
  }

  function renderPage1(data) {
    var summary = data.summary || {};
    var brands = (data.brands || []).slice(0, 6);
    var html = '<div class="bas-book-page-inner bas-content-page bas-page-narrative">';

    html += '<div class="bas-brief-highlights">';
    html += '<p class="bas-brief-kicker">Brand Alignment Narrative</p>';
    if (!data.noBrands) {
      html += '<div class="bas-brief-score-cards">';
      html +=
        '<div class="bas-brief-card"><div class="bas-brief-card-title">Readiness Stage</div><div class="bas-brief-card-body">' +
        esc((data.readiness && data.readiness.stage) || "—") +
        "</div></div>";
      html +=
        '<div class="bas-brief-card"><div class="bas-brief-card-title">Brands in Review Set</div><div class="bas-brief-card-body">' +
        esc(String((data.brandUniverse && data.brandUniverse.brandCount) || brands.length)) +
        "</div></div>";
      html += "</div>";
    }
    if (data.limitedData) {
      html +=
        '<p class="bas-brief-lead bas-brief-lead--warn">Limited brand universe (fewer than three brands in scope). Additional owner-selected or pipeline brands may improve review coverage.</p>';
    } else if (data.noBrands) {
      html += '<p class="bas-brief-lead">Add preferred brands or pipeline brands to generate alignment detail.</p>';
    }
    html += "</div>";

    html += '<div class="bas-brief-panel">';
    if (data.noBrands) {
      html += '<section class="bas-section bas-section--brief bas-section--keep"><h2 class="bas-section-title">Brand Alignment</h2>';
      html += renderSummaryParagraphs(summary) + "</section>";
    } else {
      html += '<section class="bas-section bas-section--brief bas-section--keep"><h2 class="bas-section-title">1. Brand Alignment Summary</h2>';
      html += renderSummaryParagraphs(summary) + "</section>";

      html += '<section class="bas-section bas-section--brief bas-section--keep"><h2 class="bas-section-title">2. Brand Pathway View</h2>';
      var pathwayRows = (summary.pathwayView || []).map(function (r) {
        return [r.brandPathway, r.whyItMayMeritReview, r.clarificationNeeded];
      });
      if (pathwayRows.length) {
        html += renderTable(
          ["Brand Pathway", "Why It May Merit Review", "Clarification Needed"],
          pathwayRows,
          true
        );
      } else {
        html += '<p class="bas-muted">No pathway rows available.</p>';
      }
      html += "</section>";

      html += '<section class="bas-section bas-section--brief bas-section--keep"><h2 class="bas-section-title">3. Brands for Owner Review</h2>';
      if (!brands.length) {
        html += '<p class="bas-muted">No brands in the review set.</p>';
      } else {
        var brandRows = brands.map(function (b) {
          return [b.brandName, b.parentCompany || "—", b.tier || "—", b.reviewStatus || "—", b.keyConsideration || "—"];
        });
        html += renderTable(
          ["Brand", "Parent Company", "Alignment Signal", "Review Status", "Key Consideration"],
          brandRows,
          true
        );
      }
      html += "</section>";

      html += '<section class="bas-section bas-section--brief bas-section--keep"><h2 class="bas-section-title">4. Primary Review Considerations</h2>';
      html += '<ul class="bas-detail-list">';
      (data.primaryReviewConsiderations || []).forEach(function (item) {
        html += "<li>" + esc(item) + "</li>";
      });
      html +=
        '</ul><p class="bas-muted">These considerations should be validated before controlled brand outreach.</p></section>';

      html += '<section class="bas-section bas-section--brief bas-section--keep"><h2 class="bas-section-title">5. Clarification Areas Before Outreach</h2>';
      var clar = (data.clarificationAreas || []).slice(0, 8);
      if (!clar.length) clar = (data.readiness && data.readiness.validationItems) || [];
      if (!clar.length) {
        html += '<p class="bas-muted">No primary clarification areas are flagged at this time.</p>';
      } else {
        html += '<ul class="bas-detail-list">';
        clar.slice(0, 8).forEach(function (item) {
          html += "<li>" + esc(item) + "</li>";
        });
        html += "</ul>";
      }
      html += "</section>";
    }

    html += '<div class="bas-narrative-tail">';
    html += '<section class="bas-section bas-section--brief bas-section--keep"><h2 class="bas-section-title">6. Current Review Status</h2>';
    html += '<p class="bas-review-status-label">' + esc(summary.currentReviewStatus || "—") + "</p></section>";
    html +=
      '<footer class="bas-output-note bas-output-note--brief bas-section--keep"><p><strong>Output Note.</strong> ' +
      esc(OUTPUT_NOTE) +
      "</p></footer>";
    html += "</div>";
    html += "</div></div>";
    return html;
  }

  function renderListSection(title, items) {
    var html = '<h4 class="bas-brand-card-h4">' + esc(title) + "</h4>";
    if (!(items || []).length) {
      html += '<p class="bas-muted">Not available for current inputs.</p>';
      return html;
    }
    html += '<ul class="bas-detail-list">';
    items.forEach(function (item) {
      html += "<li>" + esc(item) + "</li>";
    });
    html += "</ul>";
    return html;
  }

  function renderBrandCard(brand) {
    var technical =
      brand.alignmentFactorsReviewed || brand.potentialAlignmentSignals || brand.signals || [];
    var html = '<article class="bas-brand-card bas-avoid-break bas-section--keep">';
    html += '<header class="bas-brand-card-header"><h3 class="bas-brand-card-title">' + esc(brand.brandName) + "</h3>";
    html += '<p class="bas-brand-card-sub">' + esc(brand.parentCompany || "—") + "</p></header>";
    html +=
      '<p class="bas-brand-card-score">Alignment score: <strong>' +
      esc(brand.score != null && brand.score !== "" ? brand.score + " / 100" : "Not enough data") +
      "</strong> · " +
      esc(brand.tier || "") +
      "</p>";
    html +=
      '<h4 class="bas-brand-card-h4">Owner-Facing Alignment Rationale</h4><p class="bas-brand-card-text">' +
      esc(brand.alignmentRationale || "") +
      "</p>";
    html += renderListSection("What Supports Review", brand.whatSupportsReview);
    html += renderListSection("What Needs Validation", brand.whatNeedsValidation);
    html += renderListSection(
      "What Could Weaken Alignment",
      brand.whatCouldWeakenAlignment || brand.fitBoundariesWatchouts
    );
    html += renderListSection("Owner Questions This Brand Raises", brand.ownerQuestionsThisBrandRaises);
    html += '<h4 class="bas-brand-card-h4 bas-brand-card-h4--technical">Alignment Factors Reviewed</h4>';
    html += '<p class="bas-muted bas-technical-hint">Technical scoring factors behind the alignment signal (not the primary owner rationale).</p>';
    if (!technical.length) {
      html += '<p class="bas-muted">No factor breakdown available.</p>';
    } else {
      html += '<ul class="bas-signal-list bas-signal-list--technical">';
      technical.forEach(function (s) {
        html += "<li><strong>" + esc(s.label) + ":</strong> " + esc(s.assessment);
        if (s.ownerExplanation) html += " — " + esc(s.ownerExplanation);
        html += "</li>";
      });
      html += "</ul>";
    }
    html += "</article>";
    return html;
  }

  function renderPage2(data) {
    var brands = data.brands || [];
    var html = '<div class="bas-book-page-inner bas-content-page bas-page-technical">';

    html += '<div class="bas-brief-highlights">';
    html += '<p class="bas-brief-kicker">Brand Alignment Detail</p>';
    html += '<p class="bas-brief-lead">Supporting brand-level alignment signals and review notes</p>';
    html += "</div>";

    html += '<div class="bas-brief-panel">';
    html += '<section class="bas-section bas-section--brief bas-section--keep"><h2 class="bas-section-title">1. Brand Alignment Snapshot Table</h2>';
    if (!brands.length) {
      html += '<p class="bas-muted">No brand detail available.</p>';
    } else {
      var rows = brands.map(function (b) {
        return [
          b.brandName,
          b.parentCompany || "—",
          b.score != null ? String(b.score) : "—",
          b.tier || "—",
          b.keyConsideration || "—",
        ];
      });
      html += renderTable(
        [
          "Brand",
          "Parent Company",
          "Numeric Score / 100",
          "Alignment Tier",
          "Key Consideration (business rationale)",
        ],
        rows,
        true
      );
    }
    html += "</section>";

    html += '<section class="bas-section bas-section--brief"><h2 class="bas-section-title">2. Brand-by-Brand Review Cards</h2>';
    if (!brands.length) {
      html += '<p class="bas-muted">No brands in the review set.</p>';
    } else {
      brands.forEach(function (b) {
        html += renderBrandCard(b);
      });
    }
    html += "</section>";

    html +=
      '<section class="bas-section bas-section--brief bas-section--keep"><h2 class="bas-section-title">3. Common Questions to Clarify Before Outreach</h2>';
    var commonQ = data.commonQuestionsToClarify || [];
    if (!commonQ.length) {
      html += '<p class="bas-muted">No shared clarification questions available.</p>';
    } else {
      html += '<ul class="bas-detail-list">';
      commonQ.forEach(function (q) {
        html += "<li>" + esc(q) + "</li>";
      });
      html += "</ul>";
    }
    html += "</section>";

    html += '<section class="bas-section bas-section--brief bas-section--technical-note bas-section--keep"><h2 class="bas-section-title">Methodology Note</h2>';
    html += '<p class="bas-summary">' + esc(data.methodologyNote || "") + "</p></section>";
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
    var snapClass = "brand-alignment-snapshot";
    if (options.embed) snapClass += " bas--embed";
    if (options.fullPage) snapClass += " bas--full-page";

    var html = '<div class="' + snapClass + '">';
    html += buildFullPageNav(options);
    html += '<div class="bas-toolbar bas-no-print"><div class="bas-toolbar-actions">';
    html +=
      '<span class="bas-print-tip bas-no-print">Turn off <strong>Headers and footers</strong> and enable <strong>Background graphics</strong> in the print dialog.</span>';
    html += '<div class="bas-toolbar-buttons bas-no-print">';
    html +=
      '<button type="button" class="bas-btn bas-btn-primary bas-toolbar-print" data-bas-print>Print / Save as PDF</button>';
    html += "</div></div></div>";

    html += '<div class="bas-book-shell"><article class="bas-document bas-book-document">';
    html += '<div class="bas-book-viewport" data-bas-book-viewport tabindex="0"><div class="bas-book-stage">';
    html += wrapBookPage(0, renderCover(data, options), true);
    html += wrapBookPage(1, renderPage1(data), false);
    html += wrapBookPage(2, renderPage2(data), false);
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
    if (container.classList && container.classList.contains("brand-alignment-snapshot")) return container;
    return container.querySelector(".brand-alignment-snapshot");
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
    window.setTimeout(function () {
      if (document.body.classList.contains("bas-print-active")) cleanup();
    }, 3000);
    window.requestAnimationFrame(function () {
      window.setTimeout(function () { window.print(); }, 50);
    });
  }

  function bindPrint(root) {
    if (!root) return;
    var btn = root.querySelector("[data-bas-print]");
    if (btn && !btn._basPrintBound) {
      btn._basPrintBound = true;
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

  global.BrandAlignmentSnapshot = {
    render: render,
    buildHtml: buildHtml,
    OUTPUT_NOTE: OUTPUT_NOTE,
  };
})(typeof window !== "undefined" ? window : globalThis);
