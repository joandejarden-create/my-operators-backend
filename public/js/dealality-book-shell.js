/**
 * Shared BAS book-shell flip + print binder.
 * Reused by Hotel Intelligence Dossier (and available to other snapshots).
 */
(function (global) {
  "use strict";

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

  function bookChrome(pagesHtml, options) {
    options = options || {};
    var pageCount = options.pageCount || 1;
    var html = "";
    if (options.toolbar !== false) {
      html += '<div class="bas-toolbar bas-no-print"><div class="bas-toolbar-actions">';
      html +=
        '<span class="bas-print-tip bas-no-print">Turn off <strong>Headers and footers</strong> and enable <strong>Background graphics</strong> in the print dialog.</span>';
      html += '<div class="bas-toolbar-buttons bas-no-print">';
      if (options.extraToolbarHtml) html += options.extraToolbarHtml;
      html +=
        '<button type="button" class="bas-btn bas-btn-primary bas-toolbar-print" data-bas-print>Print / Save as PDF</button>';
      html += "</div></div></div>";
    }
    html += '<div class="bas-book-shell"><article class="bas-document bas-book-document">';
    html += '<div class="bas-book-viewport" data-bas-book-viewport tabindex="0"><div class="bas-book-stage">';
    html += pagesHtml;
    html += "</div>";
    html +=
      '<button type="button" class="bas-turn-btn bas-turn-prev bas-no-print" data-bas-turn-prev aria-label="Previous page" disabled>‹</button>';
    html +=
      '<button type="button" class="bas-turn-btn bas-turn-next bas-no-print" data-bas-turn-next aria-label="Next page">›</button>';
    html +=
      '<span class="bas-page-indicator bas-no-print" data-bas-page-indicator>1 of ' +
      pageCount +
      "</span>";
    html += "</div>";
    if (options.footerHtml) {
      html += '<div class="bas-host-footer bas-no-print">' + options.footerHtml + "</div>";
    }
    html += "</article></div>";
    return html;
  }

  function bindPageFlip(root) {
    if (!root) return;
    var viewport = root.querySelector("[data-bas-book-viewport]");
    var pages = viewport
      ? Array.prototype.slice.call(viewport.querySelectorAll(".bas-book-page"))
      : [];
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
      if (e.key === "ArrowRight" || e.key === "PageDown") {
        e.preventDefault();
        goTo(current + 1);
      } else if (e.key === "ArrowLeft" || e.key === "PageUp") {
        e.preventDefault();
        goTo(current - 1);
      }
    });
    updateControls();
  }

  function bindPrint(root, getRootFn) {
    if (!root) return;
    var btn = root.querySelector("[data-bas-print]");
    if (!btn) return;
    btn.addEventListener("click", function () {
      var snapshot = typeof getRootFn === "function" ? getRootFn(root) : root;
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
      window.print();
      global.setTimeout(function () {
        document.body.classList.remove("bas-print-active");
        printHost.innerHTML = "";
      }, 500);
    });
  }

  global.DealalityBookShell = {
    wrapBookPage: wrapBookPage,
    bookChrome: bookChrome,
    bindPageFlip: bindPageFlip,
    bindPrint: bindPrint,
  };
})(typeof window !== "undefined" ? window : globalThis);
