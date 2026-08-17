(function () {
  try {
    var path = (location.pathname || "").replace(/\/+$/, "").toLowerCase() || "/";
    if (path !== "/old-home") return;

    var href =
      "https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/PLACEHOLDER_dealality-old-home-quote-tiles.v20260729b.css";

    if (!document.querySelector('link[data-oh-quote-tiles="1"]')) {
      var link = document.createElement("link");
      link.rel = "stylesheet";
      link.href = href;
      link.setAttribute("data-oh-quote-tiles", "1");
      (document.head || document.documentElement).appendChild(link);
    }

    function hardenAvatars(root) {
      if (!root) return;
      var imgs = root.querySelectorAll("#testimonials-viewport article img");
      for (var i = 0; i < imgs.length; i++) {
        var img = imgs[i];
        try {
          img.loading = "eager";
          img.decoding = "sync";
          img.setAttribute("fetchpriority", "high");
          img.style.setProperty("display", "block", "important");
          img.style.setProperty("object-fit", "cover", "important");
          img.style.setProperty("object-position", "center center", "important");
          img.style.setProperty("width", "56px", "important");
          img.style.setProperty("height", "56px", "important");
          img.style.setProperty("border-radius", "50%", "important");
          img.style.setProperty("overflow", "hidden", "important");
          var inActive = !!(img.closest && img.closest("[data-slide].is-active"));
          if (inActive) {
            img.style.setProperty("visibility", "visible", "important");
            img.style.setProperty("opacity", "1", "important");
          }
          if (!img.complete || !img.naturalWidth) {
            var src = img.currentSrc || img.getAttribute("src") || "";
            if (src) {
              img.removeAttribute("srcset");
              img.src = src;
            }
          }
        } catch (e) {
          if (typeof console !== "undefined" && console.warn) {
            console.warn("[oh-quote-tiles] avatar", e);
          }
        }
      }
    }

    function run() {
      var root =
        document.getElementById("testimonials") ||
        document.getElementById("trust");
      hardenAvatars(root);
    }

    if (document.readyState === "loading") {
      document.addEventListener("DOMContentLoaded", run);
    } else {
      run();
    }
    window.addEventListener("load", run);
    setTimeout(run, 800);
    setTimeout(run, 2200);
  } catch (err) {
    if (typeof console !== "undefined" && console.warn) {
      console.warn("[oh-quote-tiles]", err);
    }
  }
})();
