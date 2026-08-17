const fs = require('fs');
const headPath = 'tmp-old-home-head-hero-badge.txt';
let head = `<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Inter+Tight:wght@400;500;600;700;800&family=Plus+Jakarta+Sans:wght@400;500;600;700;800&family=Lora:ital,wght@0,400;1,400&display=swap" rel="stylesheet">
<link rel="stylesheet" href="https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a68c28696192b91c48d1768_dealality-old-home-dark.v20260728ag.css">
<link rel="stylesheet" href="https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a68f96d1f20a4a06d72162c_dealality-old-home-freeform.v20260728benefits.css">
<link rel="stylesheet" href="https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a6906d02cfa3b13446a3236_dealality-old-home-benefits-tabs.v20260728b.css">
<link rel="stylesheet" href="https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a69179b0ce72c9fded41454_dealality-old-home-perspectives.v20260728.css">
<style>
`;
// Use previous live style body from file if present
const prev = fs.readFileSync('tmp-old-home-head-with-persp.txt', 'utf8');
const styleStart = prev.indexOf('<style>') + 7;
const styleEnd = prev.lastIndexOf('</style>');
const styleBody = prev.slice(styleStart, styleEnd);
const heroCss = `
/* Hero badge match FAQ/Insights + hide preview */
#hero-preview,#hero-preview-img{display:none!important}
#section-subtitle{display:flex!important;justify-content:center!important;margin:0 0 1.15rem!important}
#sst-inner{display:inline-flex!important;align-items:center!important;overflow:hidden!important;border-radius:999px!important;border:1px solid rgba(255,255,255,.14)!important;background:rgba(8,15,37,.92)!important;padding:5px 15px 5px 5px!important;box-shadow:0 0 0 1px rgba(109,92,216,.1),0 0 28px rgba(109,92,216,.18)!important;position:relative!important;isolation:isolate!important}
#sst-pill{display:inline-flex!important;align-items:center!important;padding:0 10px!important;height:32px!important;border-radius:10px!important;background:#343259!important;color:#fff!important;font-family:"Inter Tight","Plus Jakarta Sans",system-ui,sans-serif!important;font-size:1rem!important;font-weight:500!important;line-height:1!important;text-transform:capitalize!important;white-space:nowrap!important;position:relative!important;z-index:1!important;border:0!important;box-shadow:none!important}
#sst-text{display:inline-flex!important;align-items:center!important;margin-left:15px!important;color:#fff!important;font-family:"Inter Tight","Plus Jakarta Sans",system-ui,sans-serif!important;font-size:1rem!important;font-weight:500!important;line-height:1!important;text-transform:capitalize!important;white-space:nowrap!important;position:relative!important;z-index:1!important}
#sst-glow,#sst-bg1,#sst-bg2,#sst-rotate,#sst-rotate-inner{display:none!important}
@media(max-width:640px){#sst-pill,#sst-text{font-size:.95rem!important}#sst-text{margin-left:10px!important}}
`;
fs.writeFileSync(headPath, head + styleBody.trim() + '\n' + heroCss + '</style>\n');
console.log(fs.statSync(headPath).size);
