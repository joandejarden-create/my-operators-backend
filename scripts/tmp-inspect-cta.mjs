import https from "https";

function get(url) {
  return new Promise((resolve, reject) => {
    https
      .get(url, { headers: { "Cache-Control": "no-cache" } }, (res) => {
        let d = "";
        res.on("data", (c) => (d += c));
        res.on("end", () => resolve(d));
      })
      .on("error", reject);
  });
}

const d = await get("https://www.dealality.com/old-home?t=" + Date.now());
const iInsights = d.indexOf('id="insights"');
const iCta = d.indexOf('id="cta-band"');
const iFooter = d.indexOf('id="footer"');
console.log(
  JSON.stringify(
    {
      hasCta: iCta > 0,
      orderOk: iInsights > 0 && iCta > iInsights && iFooter > iCta,
      hasCssX: d.includes("v20260728x.css"),
      hasAccent: d.includes("Intelligent Solutions"),
      hasBtn: d.includes("Get Started Now"),
    },
    null,
    2
  )
);
