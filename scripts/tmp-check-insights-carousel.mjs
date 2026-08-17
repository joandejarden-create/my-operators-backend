import https from "https";

function get(url) {
  return new Promise((resolve, reject) => {
    https
      .get(url, { headers: { "Cache-Control": "no-cache", Pragma: "no-cache" } }, (res) => {
        let d = "";
        res.on("data", (c) => (d += c));
        res.on("end", () => resolve(d));
      })
      .on("error", reject);
  });
}

const page = await get("https://www.dealality.com/old-home");
const css = await get(
  "https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a6865acc88d097414f1e9ad_dealality-old-home-dark.v20260728d.css"
);

const checks = {
  page: {
    carousel: page.includes("insights-carousel"),
    prev: page.includes("insights-prev"),
    next: page.includes("insights-next"),
    cssV: page.includes("v20260728d.css"),
  },
  css: {
    aspect: css.includes("aspect-ratio:16/10"),
    flexTrack: css.includes("scroll-snap-type:x"),
    noAbsoluteFill: !/#ins-1-img\{[^}]*position:absolute/.test(css),
    wrapImgImportant: css.includes("#ins-1-img-wrap img"),
  },
};
console.log(JSON.stringify(checks, null, 2));
