/**
 * Deal Brief — My Deals modal renderer (same book shell / flip / toolbar as Brand Alignment Snapshot).
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

  function resolvePayload(data, options) {
    options = options || {};
    var R = global.DealBriefRenderHtml;
    if (!R) return null;
    var fields = R.mergeFieldsFromNormalized(
      data.fields || (data.deal && data.deal.fields) || {},
      data.normalized || {}
    );
    var normalized = data.normalized || {};
    var ctx = {
      mode:
        options.mode ||
        (global.DealBriefV2 && global.DealBriefV2.MODES
          ? global.DealBriefV2.MODES.OWNER_DRAFT
          : "ownerDraft"),
      readiness: data.readiness || null,
    };
    return { fields: fields, normalized: normalized, ctx: ctx };
  }

  function buildHtml(data, options) {
    options = options || {};
    var R = global.DealBriefRenderHtml;
    if (!R) return '<div class="my-deals-readiness-error">Deal brief renderer failed to load.</div>';
    var payload = resolvePayload(data, options);
    if (!payload) return "";

    var snapClass = "deal-brief-snapshot brand-alignment-snapshot";
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
    html += wrapBookPage(0, R.buildCoverPageHtml(payload.fields, payload.normalized, payload.ctx, options), true);
    html += wrapBookPage(1, R.buildContentPageHtml(payload.fields, payload.normalized, payload.ctx), false);
    html += "</div>";
    html +=
      '<button type="button" class="bas-turn-btn bas-turn-prev bas-no-print" data-bas-turn-prev aria-label="Previous page" disabled>‹</button>';
    html +=
      '<button type="button" class="bas-turn-btn bas-turn-next bas-no-print" data-bas-turn-next aria-label="Next page">›</button>';
    html += '<span class="bas-page-indicator bas-no-print" data-bas-page-indicator>1 of 2</span>';
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
    if (container.classList && container.classList.contains("deal-brief-snapshot")) return container;
    return container.querySelector(".deal-brief-snapshot");
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
    global.requestAnimationFrame(function () {
      global.setTimeout(function () { window.print(); }, 50);
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

  global.DealBriefSnapshot = {
    render: render,
    buildHtml: buildHtml,
  };
})(typeof window !== "undefined" ? window : globalThis);
