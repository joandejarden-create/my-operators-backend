const fs = require("fs");

const GLOBE_CSS = `
/* Hero + Finsweet 3D globe (left column) */
#hero-inner{display:grid!important;grid-template-columns:minmax(260px,.92fr) minmax(300px,1.15fr)!important;grid-template-areas:"globe badge" "globe h1" "globe lead" "globe form"!important;align-items:center!important;column-gap:clamp(1.5rem,4vw,3rem)!important;row-gap:0!important;max-width:1240px!important;margin:0 auto!important;text-align:left!important}
#hero-globe{grid-area:globe!important;position:relative!important;width:100%!important;max-width:540px!important;justify-self:center!important}
#hero-globe-container{position:relative!important;width:100%!important;aspect-ratio:1/1!important;max-width:520px!important;margin:0 auto!important}
#hero-globe-container .fs-3dglobe-container,#hero-globe-container .canvas-3dglobe-container{width:100%!important;height:100%!important;position:absolute!important;inset:0!important}
#hero-globe-container canvas{width:100%!important;height:100%!important;display:block!important}
#hero-globe-list{position:absolute!important;width:1px!important;height:1px!important;overflow:hidden!important;clip:rect(0,0,0,0)!important}
#section-subtitle{grid-area:badge!important;justify-content:flex-start!important;margin:0 0 1.5rem!important}
#h1wrap,.oh-h1wrap{grid-area:h1!important;justify-content:flex-start!important;margin:0 0 1.75rem!important}
#hero-h1{grid-area:h1!important}
#hero-lead,.oh-hero-lead{grid-area:lead!important;margin:0 0 2rem!important;text-align:left!important;max-width:40rem!important}
#form-subscribe-wrap,.oh-fsw-wrap{grid-area:form!important;align-items:flex-start!important;margin:0!important}
#fsw-secondary-wrap{text-align:left!important}
#hg-dot-1,#hg-dot-2,#hg-dot-3,#hg-dot-4,#hg-dot-5{display:block!important;width:12px!important;height:12px!important;border-radius:50%!important;background:#6C72FF!important;box-shadow:0 0 0 3px rgba(108,114,255,.28),0 0 16px rgba(108,114,255,.55)!important}
#hg-tip-1,#hg-tip-2,#hg-tip-3,#hg-tip-4,#hg-tip-5{padding:.35rem .65rem!important;border-radius:8px!important;background:rgba(8,15,37,.92)!important;border:1px solid rgba(108,114,255,.35)!important;color:#fff!important;font-family:"Inter Tight",system-ui,sans-serif!important;font-size:.78rem!important;font-weight:500!important;white-space:nowrap!important}
.fs-3dglobe-info-box{pointer-events:none!important}
@media(max-width:960px){
#hero-inner{grid-template-columns:1fr!important;grid-template-areas:"globe" "badge" "h1" "lead" "form"!important;text-align:center!important;row-gap:.25rem!important}
#hero-globe{max-width:360px!important;margin:0 auto 1.25rem!important}
#section-subtitle{justify-content:center!important;margin:0 0 1.25rem!important}
#h1wrap,.oh-h1wrap{justify-content:center!important;margin:0 auto 1.5rem!important}
#hero-lead,.oh-hero-lead{text-align:center!important;margin:0 auto 1.75rem!important}
#form-subscribe-wrap,.oh-fsw-wrap{align-items:center!important}
#fsw-secondary-wrap{text-align:center!important}
}
`;

const GLOBE_BOOT = `<script>
(function(){
  var TEX="https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a691e4f3b0bf638b1052fc6_dealality-globe-texture.jpg";
  var container=document.getElementById("hero-globe-container");
  var list=document.getElementById("hero-globe-list");
  if(!container||!list)return;
  container.setAttribute("fs-3dglobe-element","container");
  container.setAttribute("fs-3dglobe-img",TEX);
  list.setAttribute("fs-3dglobe-element","list");
  var items=list.children;
  for(var i=0;i<items.length;i++){
    items[i].classList.add("w-dyn-item");
  }
  function setEl(id,attr,val){
    var el=document.getElementById(id);
    if(el)el.setAttribute(attr,val);
  }
  for(var n=1;n<=5;n++){
    setEl("hg-pin-"+n,"fs-3dglobe-element","pin");
    setEl("hg-tip-"+n,"fs-3dglobe-element","tooltip");
    setEl("hg-lat-"+n,"fs-3dglobe-element","lat");
    setEl("hg-lon-"+n,"fs-3dglobe-element","lon");
  }
  function loadScript(src){
    return new Promise(function(resolve,reject){
      var s=document.createElement("script");
      s.src=src;
      s.async=false;
      s.onload=function(){resolve();};
      s.onerror=function(){reject(new Error(src));};
      document.body.appendChild(s);
    });
  }
  var reduce=window.matchMedia&&window.matchMedia("(prefers-reduced-motion: reduce)").matches;
  var mobile=window.matchMedia&&window.matchMedia("(max-width:700px)").matches;
  if(mobile&&reduce)return;
  loadScript("https://cdnjs.cloudflare.com/ajax/libs/three.js/r125/three.min.js")
    .then(function(){return loadScript("https://cdn.jsdelivr.net/npm/@finsweet/3dglobes@1/OrbitControls.min.js");})
    .then(function(){return loadScript("https://cdn.jsdelivr.net/npm/@finsweet/3dglobes@1/FsGlobe.min.js");})
    .catch(function(err){if(typeof console!=="undefined"&&console.warn)console.warn("[oh-globe]",err);});
})();
</script>`;

// Build head from current pattern + globe CSS
const headPath = "tmp-old-home-head-globe.txt";
const head = `<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Inter+Tight:wght@400;500;600;700;800&family=Plus+Jakarta+Sans:wght@400;500;600;700;800&family=Lora:ital,wght@0,400;1,400&display=swap" rel="stylesheet">
<link rel="stylesheet" href="https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a68c28696192b91c48d1768_dealality-old-home-dark.v20260728ag.css">
<link rel="stylesheet" href="https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a68f96d1f20a4a06d72162c_dealality-old-home-freeform.v20260728benefits.css">
<link rel="stylesheet" href="https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a6906d02cfa3b13446a3236_dealality-old-home-benefits-tabs.v20260728b.css">
<link rel="stylesheet" href="https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a69179b0ce72c9fded41454_dealality-old-home-perspectives.v20260728.css">
<link async rel="stylesheet" href="https://cdn.jsdelivr.net/npm/@finsweet/3dglobes@1/styles.min.css">
<style>
/* Footer full-width + logo */
#footer,#footer-new{width:100%!important;max-width:none!important;margin-left:0!important;margin-right:0!important;box-sizing:border-box!important;padding:4rem clamp(1.5rem,4vw,3rem) 2.25rem!important;background:#080F25!important}
#footer-inner,#footer-inner-new{max-width:1320px!important;width:100%!important;margin:0 auto!important;box-sizing:border-box!important}
#footer-grid,#footer-grid-new{display:grid!important;grid-template-columns:1.35fr repeat(3,minmax(0,1fr))!important;gap:2.25rem 2.5rem!important;width:100%!important}
#footer-logo{display:inline-flex!important;align-items:center!important;margin:0 0 1.15rem!important;text-decoration:none!important}
#footer-logo-img,#footer-logo img{display:block!important;width:168px!important;height:auto!important;max-width:100%!important;object-fit:contain!important}
#footer-h-company{display:none!important}
#footer-blurb{max-width:26rem!important}
@media(max-width:960px){#footer-grid,#footer-grid-new{grid-template-columns:1fr 1fr!important}#footer-col-company{grid-column:1/-1!important}}
@media(max-width:560px){#footer-grid,#footer-grid-new{grid-template-columns:1fr!important}}
/* Footer columns right + bottom-align to blurb */
#footer-grid,#footer-grid-new{display:grid!important;grid-template-columns:minmax(240px,26rem) minmax(2rem,1fr) repeat(3,minmax(9.5rem,11.5rem))!important;gap:0 2rem!important;margin-bottom:5.75rem!important;align-items:end!important;width:100%!important}
#footer-col-company{grid-column:1!important;position:relative!important;justify-self:start!important;max-width:26rem!important}
#footer-col-products{grid-column:3!important;justify-self:stretch!important}
#footer-col-resources{grid-column:4!important;justify-self:stretch!important}
#footer-col-links{grid-column:5!important;justify-self:stretch!important}
#footer-blurb{margin:0!important;max-width:26rem!important}
#footer-social-label{position:absolute!important;left:0!important;top:calc(100% + 1.35rem)!important;margin:0!important;font-size:1.05rem!important;font-weight:700!important;color:#fff!important}
#footer-social{position:absolute!important;left:0!important;top:calc(100% + 3.05rem)!important;display:flex!important;flex-wrap:wrap!important;gap:.65rem!important;align-items:center!important;margin:0!important}
@media(max-width:960px){
#footer-grid,#footer-grid-new{grid-template-columns:1fr 1fr!important;gap:2rem 1.5rem!important;align-items:start!important;margin-bottom:2rem!important}
#footer-col-company{grid-column:1/-1!important;max-width:none!important}
#footer-col-products,#footer-col-resources,#footer-col-links{grid-column:auto!important;justify-self:start!important}
#footer-social-label,#footer-social{position:static!important;top:auto!important;left:auto!important}
#footer-social-label{margin:1.5rem 0 .85rem!important}
}
@media(max-width:560px){
#footer-grid,#footer-grid-new{grid-template-columns:1fr!important}
}
/* Footer tagline */
#footer-tagline{margin:0 0 1rem!important;font-size:clamp(1.05rem,1.4vw,1.25rem)!important;font-weight:700!important;line-height:1.35!important;letter-spacing:.02em!important;text-transform:uppercase!important;color:#fff!important}
/* Benefits header match FAQ */
#modules{padding:100px 1.5rem!important;background:#080F25!important;position:relative!important;overflow:hidden!important}
#modules-inner{max-width:1120px!important;margin:0 auto!important;text-align:center!important;position:relative!important;z-index:1!important}
#modules-badges{display:flex!important;justify-content:center!important;margin:0 0 14px!important}
#modules-badge{display:inline-flex!important;align-items:center!important;overflow:hidden!important;border-radius:999px!important;border:1px solid rgba(255,255,255,.14)!important;background:rgba(8,15,37,.92)!important;padding:5px 15px 5px 5px!important;box-shadow:0 0 0 1px rgba(109,92,216,.1),0 0 28px rgba(109,92,216,.18)!important;margin:0!important;font-size:inherit!important;letter-spacing:normal!important;text-transform:none!important;color:inherit!important}
#modules-badge-left{display:inline-flex!important;align-items:center!important;padding:0 10px!important;height:32px!important;border-radius:10px!important;background:#343259!important;color:#fff!important;font-size:1rem!important;font-weight:500!important;line-height:1!important;text-transform:capitalize!important;white-space:nowrap!important}
#modules-badge-right{display:inline-flex!important;align-items:center!important;margin-left:15px!important;color:#fff!important;font-size:1rem!important;font-weight:500!important;line-height:1!important;text-transform:capitalize!important;white-space:nowrap!important}
#modules-h2{margin:0 auto 18px!important;max-width:22em!important;font-family:"Inter Tight","Plus Jakarta Sans",system-ui,sans-serif!important;font-size:clamp(2rem,1.567rem + 1.846vw,3rem)!important;line-height:1.22!important;font-weight:500!important;letter-spacing:-.015em!important;color:#fff!important;text-transform:capitalize!important;text-align:center!important}
#modules-lead{display:block!important;margin:0 auto 48px!important;max-width:42rem!important;font-family:"Inter Tight","Plus Jakarta Sans",system-ui,sans-serif!important;font-size:1.05rem!important;line-height:1.55!important;font-weight:400!important;color:rgba(255,255,255,.68)!important;text-align:center!important}
#modules-glow{position:absolute!important;left:50%!important;top:-120px!important;width:640px!important;height:640px!important;transform:translateX(-50%)!important;border-radius:50%!important;background:rgba(109,92,216,.22)!important;filter:blur(70px)!important;pointer-events:none!important;z-index:0!important}
#modules-grid{position:relative!important;z-index:1!important}
@media(max-width:640px){#modules{padding:72px 1.25rem!important}#modules-h2{margin-bottom:14px!important;font-size:clamp(1.55rem,5.2vw,2.25rem)!important}#modules-lead{margin:0 auto 36px!important;font-size:.98rem!important;padding:0 .5rem!important}#modules-badge-left,#modules-badge-right{font-size:.95rem!important}#modules-badge-right{margin-left:10px!important}}

/* Hero badge match FAQ/Insights + hide preview */
#hero-preview,#hero-preview-img{display:none!important}
#sst-inner{display:inline-flex!important;align-items:center!important;overflow:hidden!important;border-radius:999px!important;border:1px solid rgba(255,255,255,.14)!important;background:rgba(8,15,37,.92)!important;padding:5px 15px 5px 5px!important;box-shadow:0 0 0 1px rgba(109,92,216,.1),0 0 28px rgba(109,92,216,.18)!important;position:relative!important;isolation:isolate!important}
#sst-pill{display:inline-flex!important;align-items:center!important;padding:0 10px!important;height:32px!important;border-radius:10px!important;background:#343259!important;color:#fff!important;font-family:"Inter Tight","Plus Jakarta Sans",system-ui,sans-serif!important;font-size:1rem!important;font-weight:500!important;line-height:1!important;text-transform:capitalize!important;white-space:nowrap!important;position:relative!important;z-index:1!important;border:0!important;box-shadow:none!important}
#sst-text{display:inline-flex!important;align-items:center!important;margin-left:15px!important;color:#fff!important;font-family:"Inter Tight","Plus Jakarta Sans",system-ui,sans-serif!important;font-size:1rem!important;font-weight:500!important;line-height:1!important;text-transform:capitalize!important;white-space:nowrap!important;position:relative!important;z-index:1!important}
#sst-glow,#sst-bg1,#sst-bg2,#sst-rotate,#sst-rotate-inner{display:none!important}
@media(max-width:640px){#sst-pill,#sst-text{font-size:.95rem!important}#sst-text{margin-left:10px!important}}
/* Hero signup — kill spinning border beam + button shimmer */
#fsw-glow-rotate{display:none!important;animation:none!important}
#fsw-btn-grad{animation:none!important;inset:0!important;width:100%!important;transform:none!important}

/* Hero vertical breathing room */
#hero,.oh-hero{padding:5rem 1.5rem 3.75rem!important}
#hero-lead,.oh-hero-lead{display:block!important;max-width:42rem!important;font-family:"Inter Tight","Plus Jakarta Sans",system-ui,sans-serif!important;font-size:1.05rem!important;line-height:1.55!important;font-weight:400!important;color:rgba(255,255,255,.68)!important}
#fsw-secondary-wrap{margin:1.75rem 0 0!important}
${GLOBE_CSS}
</style>
`;

fs.writeFileSync(headPath, head, "utf8");
fs.writeFileSync("tmp-globe-boot.js", GLOBE_BOOT, "utf8");
console.log("head", head.length, "boot", GLOBE_BOOT.length);
