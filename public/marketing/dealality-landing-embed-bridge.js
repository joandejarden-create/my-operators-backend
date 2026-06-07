/**
 * Child-side bridge when landing is embedded in Webflow (?embed=1).
 * Posts height to parent; scrolls to #sections on parent nav postMessage.
 */
(function (global) {
  "use strict";

  var params = new URLSearchParams(global.location.search);
  if (params.get("embed") !== "1") return;

  var SOURCE = "dealality-landing-embed";
  var PARENT_SOURCE = "dealality-landing-parent";

  function measureDocumentHeight() {
    var doc = global.document.documentElement;
    var body = global.document.body;
    var footer = global.document.querySelector("footer");
    if (footer) {
      var rect = footer.getBoundingClientRect();
      var scrollTop = global.pageYOffset || (doc && doc.scrollTop) || 0;
      return Math.ceil(rect.bottom + scrollTop);
    }
    return Math.max(
      doc ? doc.scrollHeight : 0,
      body ? body.scrollHeight : 0
    );
  }

  function postHeight() {
    if (global.parent === global) return;
    var height = measureDocumentHeight();
    global.parent.postMessage({ source: SOURCE, type: "resize", height: height }, "*");
  }

  function getOffsetTopInIframeDocument(el) {
    if (!el) return 0;
    var frame = global.frameElement;
    if (!frame) {
      return (
        el.getBoundingClientRect().top +
        (global.pageYOffset || global.document.documentElement.scrollTop || 0)
      );
    }
    return el.getBoundingClientRect().top - frame.getBoundingClientRect().top;
  }

  function postScrollToParent(id, top) {
    if (global.parent === global) return;
    var payload = { source: SOURCE, type: "scrollToOffset", id: id, top: top };
    global.parent.postMessage(payload, "*");
    global.setTimeout(function () {
      global.parent.postMessage(payload, "*");
    }, 150);
    global.setTimeout(function () {
      global.parent.postMessage(payload, "*");
    }, 500);
  }

  var SECTION_ID_ALIASES = { platform: "how", persona: "owners" };
  var AUDIENCE_SECTION_IDS = { owners: 0, brands: 1, partners: 2, persona: 0, audiences: 0 };

  function activateAudienceTab(idx) {
    var tabs = global.document.querySelectorAll("#audiences .audt");
    var panels = global.document.querySelectorAll("#audiences .audp");
    if (!tabs.length || !panels.length) return;
    tabs.forEach(function (tab, i) {
      var on = i === idx;
      tab.classList.toggle("on", on);
      tab.setAttribute("aria-selected", on ? "true" : "false");
    });
    panels.forEach(function (panel, i) {
      var on = i === idx;
      panel.classList.toggle("on", on);
      panel.hidden = !on;
    });
  }

  function sectionScrollAnchor(section) {
    if (!section) return null;
    return (
      section.querySelector(":scope > .si > .ey, :scope > .si > .h2, :scope > .si, :scope > .ey, :scope > .h2") ||
      section
    );
  }

  function resolveScrollTarget(id) {
    if (!id) return null;
    if (SECTION_ID_ALIASES[id]) id = SECTION_ID_ALIASES[id];
    if (Object.prototype.hasOwnProperty.call(AUDIENCE_SECTION_IDS, id)) {
      activateAudienceTab(AUDIENCE_SECTION_IDS[id]);
      var audiences = global.document.getElementById("audiences");
      return audiences ? { el: sectionScrollAnchor(audiences), hashId: id } : null;
    }
    var el = global.document.getElementById(id);
    return el ? { el: sectionScrollAnchor(el), hashId: id } : null;
  }

  function scrollToId(id) {
    var target = resolveScrollTarget(id);
    if (!target || !target.el) return;
    function runScroll() {
      if (global.parent !== global) {
        postScrollToParent(target.hashId, getOffsetTopInIframeDocument(target.el));
        postHeight();
        return;
      }
      target.el.scrollIntoView({ behavior: "smooth", block: "start" });
    }
    [60, 220, 520, 1000].forEach(function (delay) {
      global.setTimeout(runScroll, delay);
    });
  }

  function resolveHashLink(link) {
    if (!link) return null;
    var href = (link.getAttribute("href") || "").trim();
    if (!href || href === "#") return null;
    if (href.charAt(0) === "#" && href.length > 1) return href.slice(1);
    try {
      var url = new URL(href, global.location.href);
      if (url.hash && url.hash.length > 1) return url.hash.slice(1);
    } catch (err) {
      if (global.console && global.console.warn) {
        global.console.warn("[dealality-landing-embed] resolveHashLink failed", err);
      }
    }
    return null;
  }

  function handleInPageAnchorClick(event) {
    var link = event.target && event.target.closest ? event.target.closest("a[href]") : null;
    if (!link) return;
    var id = resolveHashLink(link);
    if (!id || !resolveScrollTarget(id)) return;
    event.preventDefault();
    scrollToId(id);
    if (global.history && global.history.replaceState) {
      global.history.replaceState(null, "", "#" + id);
    }
  }

  global.document.addEventListener("click", handleInPageAnchorClick, true);

  global.addEventListener("message", function (event) {
    var data = event.data;
    if (!data || data.source !== PARENT_SOURCE) return;
    if (data.type === "scrollTo") scrollToId(data.id);
  });

  global.addEventListener("load", function () {
    postHeight();
    global.setTimeout(postHeight, 400);
    global.setTimeout(postHeight, 1200);
    global.setTimeout(postHeight, 3000);
  });

  global.addEventListener(
    "load",
    function () {
      global.document.querySelectorAll("img").forEach(function (img) {
        if (!img.complete) {
          img.addEventListener("load", postHeight, { once: true });
        }
      });
    },
    true
  );
  global.addEventListener("resize", postHeight);

  if (typeof global.ResizeObserver === "function" && global.document.body) {
    var ro = new global.ResizeObserver(function () {
      postHeight();
    });
    ro.observe(global.document.body);
  }

  global.document.querySelectorAll(".faq-q").forEach(function (btn) {
    btn.addEventListener("click", function () {
      global.setTimeout(postHeight, 500);
    });
  });
})(window);
