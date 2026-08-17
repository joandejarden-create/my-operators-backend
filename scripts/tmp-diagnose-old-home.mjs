import fs from "fs";

const urls = [
  "https://www.dealality.com/",
  "https://www.dealality.com/old-home",
];

for (const u of urls) {
  const r = await fetch(u);
  const t = await r.text();
  const ids = [...t.matchAll(/id="([^"]+)"/g)].map((m) => m[1]);
  const want = [
    "nav",
    "mnav",
    "hero",
    "proofbar",
    "problem",
    "better-approach",
    "outcome",
    "how",
    "audiences",
    "why",
    "faq",
    "cta",
    "footer",
    "hero-overview-wrap",
    "dc-page",
  ];
  const found = want.filter((id) => ids.includes(id));
  const faqNav = (t.match(/>\s*FAQ[s]?\s*</gi) || []).length;
  const footerIdx = t.indexOf('id="footer"');
  const after = [];
  for (const id of ["cta", "faq", "audiences", "why", "how", "problem", "better-approach", "outcome", "hero"]) {
    const i = t.indexOf(`id="${id}"`);
    if (footerIdx >= 0 && i > footerIdx) after.push(`${id}@${i}`);
  }
  const iframes = [...t.matchAll(/<iframe[^>]+src="([^"]+)"/gi)].map((m) => m[1]).slice(0, 4);
  const minH = (t.match(/min-height:\s*100vh/gi) || []).length;
  const title = (t.match(/<title>([^<]+)<\/title>/i) || [])[1];
  console.log("\n===", u, r.status, "===");
  console.log("title:", title);
  console.log("ids:", found.join(", "));
  console.log("FAQ label hits:", faqNav);
  console.log("sections after footer:", after.join(", ") || "none");
  console.log("iframes:", iframes.join(" | ") || "none");
  console.log("min-height:100vh CSS hits:", minH);
  fs.writeFileSync(
    `tmp-diagnose-${u.includes("old-home") ? "old-home" : "root"}.html`,
    t.slice(0, 200000)
  );
}
