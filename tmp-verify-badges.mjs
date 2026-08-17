import fs from "fs";

const h = fs.readFileSync("c:/Users/joand/OneDrive/Documents/deal-capture-proxy/tmp-old-home-live.html", "utf8");

function snippets(term, radius = 120) {
  const out = [];
  let idx = 0;
  while (true) {
    const i = h.indexOf(term, idx);
    if (i === -1) break;
    out.push(h.slice(Math.max(0, i - radius), i + term.length + radius));
    idx = i + term.length;
    if (out.length >= 5) break;
  }
  return out;
}

console.log(JSON.stringify({
  cssLink: (h.match(/dealality-old-home-dark\.v[^"']+\.css/) || [])[0],
  newsBlog: h.includes("News & Blog"),
  faqsPill: h.includes(">FAQs<"),
  insightsPill: h.includes(">Insights<"),
  insightsSnippets: snippets("Insights", 80),
  faqsSnippets: snippets("FAQs", 80),
}, null, 2));
