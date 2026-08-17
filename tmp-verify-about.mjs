const r = await fetch("https://www.dealality.com/old-home?nocache=" + Date.now());
const t = await r.text();
const checks = [
  "freeform-head.v20260729w10.css",
  "founder-joan-dejarden",
  "Joan Dejarden",
  " – Founder",
  "testimonials-dots",
  "structured, confidential control",
  "display:none!important", // should not hide dots in NEW css; old might still be cached briefly
];
for (const c of checks) console.log((t.includes(c) ? "YES" : "NO "), c);

const cssUrl = (t.match(/https:\/\/cdn\.prod\.website-files\.com\/[^"']+freeform-head[^"']+/) || [])[0];
console.log("css", cssUrl);
if (cssUrl) {
  const c = await (await fetch(cssUrl)).text();
  console.log("css has hide img", c.includes("#testimonials-viewport article img{display:none"));
  console.log("css has show dots", /#testimonials-dots\{\s*display:flex/.test(c));
  console.log("css has border-top divider", c.includes("border-top:1px solid rgba(255,255,255,.16)"));
}

const i = t.indexOf('id="testimonials-viewport"');
console.log("---viewport slice---");
console.log(t.slice(i, i + 2800));
