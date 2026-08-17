const url =
  "https://www.dealality.com/insights-posts/hotel-soft-brands-vs-hard-brands-owner-guide";
const r = await fetch(url);
const t = await r.text();
console.log("status", r.status, "len", t.length);

// Find date text context
const idx = t.indexOf("July 21");
console.log("july idx", idx);
if (idx >= 0) console.log(t.slice(Math.max(0, idx - 400), idx + 200));

// CSS files
const cssHrefs = [...t.matchAll(/href="([^"]+\.css[^"]*)"/gi)].map((m) => m[1]);
console.log("css count", cssHrefs.length);
for (const href of cssHrefs.slice(0, 8)) console.log("css", href);

// Inline style blocks mentioning date
const styles = [...t.matchAll(/<style[^>]*>([\s\S]*?)<\/style>/gi)].map((m) => m[1]);
for (const s of styles) {
  const hits = s.match(/[^{}]*date[^{}]*\{[^}]*\}/gi) || [];
  if (hits.length) console.log("style hits", hits);
  const muted = s.match(/[^{}]*(meta|posted|publish)[^{}]*\{[^}]*\}/gi) || [];
  if (muted.length) console.log("meta hits", muted.slice(0, 20));
}

// Fetch main css and search date colors
for (const href of cssHrefs) {
  if (!/website-files|webflow/i.test(href)) continue;
  const abs = href.startsWith("http") ? href : new URL(href, url).href;
  try {
    const css = await (await fetch(abs)).text();
    const rules = css.match(/[^{}\n]*date[^{}\n]*\{[^}]*\}/gi) || [];
    if (rules.length) {
      console.log("\nFROM", abs);
      console.log(rules.slice(0, 30).join("\n"));
    }
    const meta = css.match(/[^{}\n]*(post-date|article-date|blog-date|insights-date|published)[^{}\n]*\{[^}]*\}/gi) || [];
    if (meta.length) {
      console.log("meta FROM", abs);
      console.log(meta.slice(0, 20).join("\n"));
    }
  } catch (e) {
    console.log("css fail", abs, e.message);
  }
}
