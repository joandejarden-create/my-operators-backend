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

  var HEIGHT_BUFFER_PX = 64;

  function measureDocumentHeight() {
    var doc = global.document.documentElement;
    var body = global.document.body;
    return Math.max(
      doc ? doc.scrollHeight : 0,
      doc ? doc.offsetHeight : 0,
      body ? body.scrollHeight : 0,
      body ? body.offsetHeight : 0
    );
  }

  function postHeight() {
    if (global.parent === global) return;
    var height = measureDocumentHeight() + HEIGHT_BUFFER_PX;
    global.parent.postMessage({ source: SOURCE, type: "resize", height: height }, "*");
  }

  function scrollToId(id) {
    if (!id) return;
    var el = global.document.getElementById(id);
    if (!el) return;
    if (global.parent !== global) {
      var top =
        el.getBoundingClientRect().top +
        (global.pageYOffset || global.document.documentElement.scrollTop || 0);
      global.parent.postMessage(
        { source: SOURCE, type: "scrollToOffset", id: id, top: top },
        "*"
      );
      return;
    }
    el.scrollIntoView({ behavior: "smooth", block: "start" });
  }

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
