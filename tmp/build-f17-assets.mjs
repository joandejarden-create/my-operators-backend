import fs from "fs";
const p = "public/marketing/old-home-manual-process.v20260801f17.css";
let s = fs.readFileSync(p, "utf8");

const insertAfter = `/* Connectors — sit behind cards; overlap card edges so lines touch the border */
#dealality-manual-process .dmp-connectors {
  position: relative;
  z-index: 1;
  min-width: 0;
  align-self: stretch;
  pointer-events: none;
}`;

const readyBlock = `/* Hide connectors/dots until JS remaps geometry — kills refresh flash */
#dealality-manual-process:not(.is-connectors-ready) .dmp-connectors--desktop,
#dealality-manual-process:not(.is-connectors-ready) .dmp-connectors--mobile,
#dealality-manual-process:not(.is-connectors-ready) .dmp-path-dot {
  opacity: 0 !important;
  visibility: hidden !important;
}
#dealality-manual-process.is-connectors-ready .dmp-connectors--desktop,
#dealality-manual-process.is-connectors-ready .dmp-connectors--mobile,
#dealality-manual-process.is-connectors-ready .dmp-path-dot {
  opacity: 1;
  visibility: visible;
}

/* Connectors — sit behind cards; overlap card edges so lines touch the border */
#dealality-manual-process .dmp-connectors {
  position: relative;
  z-index: 1;
  min-width: 0;
  align-self: stretch;
  pointer-events: none;
}`;

if (!s.includes(insertAfter)) {
  console.error("connectors block missing");
  process.exit(1);
}
s = s.replace(insertAfter, readyBlock);

s = s.replace(
  `#dealality-manual-process .dmp-connectors--desktop.dmp-connectors--in {
  margin-left: -18px;
  /* Deeper underlap into Manual Process so trails disappear behind its left edge */
  margin-right: -28px;
  width: calc(100% + 46px);
}`,
  `#dealality-manual-process .dmp-connectors--desktop.dmp-connectors--in {
  margin-left: -18px;
  /* Deeper underlap into Manual Process so trails disappear behind its left edge */
  margin-right: -28px;
  width: calc(100% + 46px);
  /* Clip residual curve spill so lines never paint into problem cards */
  overflow: hidden;
}`
);

s = s.replace(
  `#dealality-manual-process .dmp-connectors svg {
  position: absolute;
  inset: 0;
  width: 100%;
  height: 100%;
  overflow: visible;
}`,
  `#dealality-manual-process .dmp-connectors svg {
  position: absolute;
  inset: 0;
  width: 100%;
  height: 100%;
  overflow: hidden;
}`
);

fs.writeFileSync(p, s);

const boot = `/**
 * Old Home Manual Process v1.1.39 boot — section HTML is inlined / CDN-loaded.
 * fonts → bind demo CTA → load draw JS (f17: clamped bottom trails + no flash).
 */
(function () {
  var JS_URL =
    "REPLACE_JS_URL";
  var FONT_HREF =
    "https://fonts.googleapis.com/css2?family=Fraunces:opsz,wght@9..144,500;9..144,550;9..144,600&family=Inter+Tight:wght@400;500;600;700&family=Plus+Jakarta+Sans:wght@700;800&display=swap";

  function ensureFonts() {
    if (document.querySelector('link[data-dmp-fonts="1"]')) return;
    var link = document.createElement("link");
    link.rel = "stylesheet";
    link.href = FONT_HREF;
    link.setAttribute("data-dmp-fonts", "1");
    document.head.appendChild(link);
  }

  function loadDrawJs() {
    if (document.querySelector('script[data-dmp-js="1"]')) return;
    var s = document.createElement("script");
    s.src = JS_URL;
    s.defer = true;
    s.setAttribute("data-dmp-js", "1");
    var root = document.getElementById("dealality-manual-process");
    (root || document.body).appendChild(s);
  }

  function openRequestDemo() {
    if (typeof window.ohOpenRequestDemo === "function") {
      window.ohOpenRequestDemo();
      return;
    }
    var demoBtn =
      document.querySelector("[data-dealality-demo-open]") ||
      document.querySelector('a[href="#request-demo"]') ||
      document.querySelector("#fsw-demo-link");
    if (demoBtn) demoBtn.click();
  }

  function bindDemoCta() {
    var root = document.getElementById("dealality-manual-process");
    if (!root) return;
    var btn = root.querySelector('[data-dmp-cta="demo"]');
    if (!btn || btn.getAttribute("data-dmp-bound") === "1") return;
    btn.setAttribute("data-dmp-bound", "1");
    btn.addEventListener("click", function (e) {
      e.preventDefault();
      openRequestDemo();
    });
  }

  function boot() {
    var root = document.getElementById("dealality-manual-process");
    if (!root) {
      console.warn(
        "[dealality-manual-process] section markup missing from page embed"
      );
      return;
    }
    if (root.getAttribute("data-dmp-boot") === "1") return;
    root.setAttribute("data-dmp-boot", "1");
    ensureFonts();
    bindDemoCta();
    loadDrawJs();
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", boot);
  } else {
    boot();
  }
})();
`;
fs.writeFileSync("public/marketing/old-home-manual-process.boot.v20260801f17.js", boot);
console.log("css+boot stub ok", s.length);
