import https from "https";

function get(url) {
  return new Promise((resolve, reject) => {
    https
      .get(url, { headers: { "Cache-Control": "no-cache", Pragma: "no-cache" } }, (res) => {
        let d = "";
        res.on("data", (c) => (d += c));
        res.on("end", () => resolve({ status: res.statusCode, body: d }));
      })
      .on("error", reject);
  });
}

const page = await get("https://www.dealality.com/old-home");
const cssUrl =
  "https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a6861b85ac86286dfad6c5d_dealality-old-home-dark.v20260728b.css";
const css = await get(cssUrl);

console.log(
  JSON.stringify(
    {
      page: {
        status: page.status,
        hasNewCss: page.body.includes("v20260728b.css"),
        hasJakarta: page.body.includes("Plus+Jakarta"),
      },
      css: {
        status: css.status,
        len: css.body.length,
        insightsBg080F25: /#insights\{[^}]*background:#080F25/.test(css.body),
        noVarkoBlack: !css.body.includes("#060610"),
        absoluteImg: css.body.includes("#ins-1-img{position:absolute"),
        jakartaH2: css.body.includes('Plus Jakarta Sans') && /#insights-h2\{[^}]*font-weight:800/.test(css.body),
        letterSpacing: /#insights-h2\{[^}]*letter-spacing:-.03em/.test(css.body),
      },
    },
    null,
    2
  )
);
