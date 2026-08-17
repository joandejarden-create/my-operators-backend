import https from "https";

function get(url) {
  return new Promise((resolve, reject) => {
    https
      .get(url, {
        headers: { "Cache-Control": "no-cache", Pragma: "no-cache", "user-agent": "Mozilla/5.0" },
      }, (res) => {
        let d = "";
        res.on("data", (c) => (d += c));
        res.on("end", () => resolve(d));
      })
      .on("error", reject);
  });
}

const page = await get(`https://www.dealality.com/old-home?nocache=${Date.now()}`);
const cssUrl =
  "https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a686943cd6b16965b33ed01_dealality-old-home-dark.v20260728h.css";
const css = await get(cssUrl);

const prevIdx = page.indexOf('id="insights-prev"');
const snippet = page.slice(prevIdx, prevIdx + 180);

console.log(
  JSON.stringify(
    {
      hasH: page.includes("v20260728h.css"),
      preventDefault: page.includes("e.preventDefault();e.stopPropagation()"),
      hrefInsights: /insights-prev[^>]*href="#insights"/.test(page) || page.includes('href="#insights"'),
      hrefHashOnly: /insights-prev[^>]*href="#"/ .test(page),
      snippet,
      css: {
        order2: css.includes("order:2"),
        centered: css.includes("justify-content:center"),
        wideCards: css.includes("min(420px,78vw)"),
      },
    },
    null,
    2
  )
);
