(function () {
  var path = (location.pathname || "").replace(/\/+$/, "").toLowerCase();
  if (path !== "/old-home") return;
  if (document.getElementById("oh-hero-signals-quiet-w18")) return;
  var style = document.createElement("style");
  style.id = "oh-hero-signals-quiet-w18";
  style.textContent = [
    "/* Hero before/after value strip — quiet compact comparison (w18) */",
    "#hero-signals{display:grid!important;grid-template-columns:1fr 1fr!important;gap:.85rem!important;align-items:start!important;margin:.15rem 0 .1rem!important;width:100%!important;max-width:28rem!important}",
    '#hero-signals[hidden],#hero-signals[aria-hidden="true"]{display:grid!important;visibility:visible!important}',
    "#hero-signals-pos,#hero-signals-neg{display:flex!important;flex-direction:column!important;align-items:stretch!important;gap:.2rem!important;margin:0!important;padding:.55rem .65rem .6rem!important;border-radius:10px!important;box-sizing:border-box!important;min-height:0!important;box-shadow:none!important}",
    "#hero-signals-pos{background:rgba(255,255,255,.03)!important;border:1px solid rgba(215,142,44,.18)!important}",
    "#hero-signals-neg{background:rgba(255,255,255,.015)!important;border:1px solid rgba(255,255,255,.07)!important}",
    '#hs-pos-label,#hs-neg-label{margin:0 0 .2rem!important;font-family:"Inter Tight","Plus Jakarta Sans",system-ui,sans-serif!important;font-size:.78rem!important;font-weight:600!important;letter-spacing:-.01em!important;text-transform:none!important;line-height:1.25!important}',
    "#hs-pos-label{color:rgba(255,255,255,.78)!important}",
    "#hs-neg-label{color:rgba(255,255,255,.48)!important}",
    '#hero-signals-pos > span,#hero-signals-neg > span{display:flex!important;align-items:center!important;gap:.4rem!important;margin:0!important;padding:.22rem 0!important;border:0!important;border-radius:0!important;background:transparent!important;box-shadow:none!important;font-family:"Inter Tight","Plus Jakarta Sans",system-ui,sans-serif!important;font-size:.8rem!important;font-weight:500!important;line-height:1.3!important;box-sizing:border-box!important;text-transform:none!important}',
    "#hero-signals-pos > span{color:rgba(255,255,255,.78)!important}",
    "#hero-signals-neg > span{color:rgba(255,255,255,.5)!important}",
    "#hero-signals-pos > span::before,#hero-signals-neg > span::before{display:inline-block!important;width:.7rem!important;flex:0 0 .7rem!important;font-weight:600!important;font-size:.75rem!important;line-height:1!important;opacity:.85!important}",
    '#hero-signals-pos > span::before{content:"+"!important;color:rgba(215,142,44,.75)!important}',
    '#hero-signals-neg > span::before{content:"–"!important;color:rgba(255,255,255,.35)!important}',
    "@media(max-width:720px){#hero-signals{grid-template-columns:1fr!important;max-width:22rem!important;gap:.55rem!important}}",
  ].join("");
  document.head.appendChild(style);
})();
