const https = require("https");
const fs = require("fs");

function get(u) {
  return new Promise((res, rej) => {
    https
      .get(u, (r) => {
        let d = "";
        r.on("data", (c) => (d += c));
        r.on("end", () => res(d));
      })
      .on("error", rej);
  });
}

(async () => {
  const html = fs.readFileSync("tmp-old-home-live-full.html", "utf8");
  // Extract freeform style with 360
  const styles = [...html.matchAll(/<style>([\s\S]*?)<\/style>/g)].map((m) => m[1]);
  const carouselStyle = styles.find((s) => s.includes("360px")) || "";
  const idx = carouselStyle.indexOf("Insights carousel");
  console.log(carouselStyle.slice(idx, idx + 1200));

  // Find CDN url full
  const m = html.match(
    /https:\/\/cdn\.prod\.website-files\.com\/[^"']+dealality-old-home-dark[^"']+/
  );
  console.log("cdn", m && m[0]);
  if (m) {
    const css = await get(m[0]);
    fs.writeFileSync("tmp-old-home-cdn.css", css);
    const parts = css
      .split(/[{}]/)
      .map((x, i, a) => (i % 2 === 0 ? null : a[i - 1] + "{" + x + "}"))
      .filter(Boolean);
    const relevant = css.match(/#insights[^]*?(?=#insights-|#cta|#faq|#footer|$)/);
    // simpler greps
    for (const key of [
      "#insights-grid",
      "#ins-1",
      ".oh-ins-card",
      "#insights-prev",
      "pointer-events",
      "flex-wrap",
      "360px",
      "overflow-x",
    ]) {
      const n = (css.match(new RegExp(key.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"), "g")) || [])
        .length;
      console.log("cdn", key, n);
    }
    const ig = css.indexOf("#insights-grid");
    console.log("cdn grid snippet", css.slice(ig, ig + 500));
    const ic = css.indexOf("#ins-1,#ins-2");
    console.log("cdn cards snippet", css.slice(ic, ic + 450));
    const ip = css.indexOf("#insights-prev");
    console.log("cdn prev snippet", css.slice(ip, ip + 600));
  }

  // Check grid HTML for display/style attrs
  const g = html.match(/id="insights-grid"[^>]*>/);
  console.log("grid open", g && g[0]);
  // Count article children quickly
  const sec = html.slice(html.indexOf('id="insights-grid"'), html.indexOf('id="insights-grid"') + 15000);
  console.log(
    "articles in grid slice",
    (sec.match(/<article /g) || []).length,
    (sec.match(/id="ins-/g) || []).length
  );
})();
