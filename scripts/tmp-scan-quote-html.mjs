import https from "https";
import fs from "fs";

function get(url) {
  return new Promise((resolve, reject) => {
    https
      .get(url, { headers: { "user-agent": "dealality-verify/1.0" } }, (res) => {
        let d = "";
        res.on("data", (c) => (d += c));
        res.on("end", () => resolve({ status: res.statusCode, body: d, headers: res.headers }));
      })
      .on("error", reject);
  });
}

const page = await get(`https://www.dealality.com/old-home?cb=${Date.now()}`);
const html = page.body;
const quoteHits = [...html.matchAll(/old-home-quote-tiles[^"'\\\s>]*/gi)].map((m) => m[0]);
const freeform = [...html.matchAll(/dealality-old-home-freeform-head[^"'\\\s>]*/gi)].map((m) => m[0]);
const problem = [...html.matchAll(/old-home-problem-v2[^"'\\\s>]*/gi)].map((m) => m[0]);

// Extract testimonial articles with imgs from HTML (static CMS/Webflow markup)
const arts = [];
const re = /<article[\s\S]*?<\/article>/gi;
let m;
while ((m = re.exec(html)) && arts.length < 8) {
  const chunk = m[0];
  if (!/blockquote/i.test(chunk)) continue;
  const img = chunk.match(/<img[^>]+>/i)?.[0] || null;
  const src = img?.match(/src=["']([^"']+)/i)?.[1] || null;
  const srcset = img?.match(/srcset=["']([^"']+)/i)?.[1] || null;
  const alt = img?.match(/alt=["']([^"']*)/i)?.[1] || null;
  const quote = chunk.match(/<blockquote[^>]*>([\s\S]*?)<\/blockquote>/i)?.[1]?.replace(/<[^>]+>/g, "").trim().slice(0, 140);
  const attr = chunk.match(/<p[^>]*>([\s\S]*?)<\/p>/i)?.[1]?.replace(/<[^>]+>/g, " ").replace(/\s+/g, " ").trim().slice(0, 140);
  arts.push({ src, srcset: srcset?.slice(0, 180) || null, alt, quote, attr, hasImg: !!img });
}

fs.writeFileSync("tmp-quote-html-scan.json", JSON.stringify({ quoteHits, freeform, problem, arts }, null, 2));
console.log(JSON.stringify({ quoteHits, freeform, problem, artCount: arts.length, arts }, null, 2));

// Probe each unique avatar src
const srcs = [...new Set(arts.map((a) => a.src).filter(Boolean))];
for (const src of srcs) {
  const abs = src.startsWith("http") ? src : `https://www.dealality.com${src}`;
  try {
    const r = await get(abs);
    console.log(JSON.stringify({ src: abs.slice(0, 120), status: r.status, type: r.headers["content-type"], len: r.body?.length || 0 }));
  } catch (e) {
    console.log(JSON.stringify({ src: abs.slice(0, 120), error: String(e.message || e) }));
  }
}
