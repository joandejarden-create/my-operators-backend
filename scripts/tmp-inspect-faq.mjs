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
const divAfter = /<\/details>\s*<div[^>]*id="faq-1-div"/.test(d);
const faq1 = d.match(/id="faq-1"[^>]*>[\s\S]*?<\/details>/);
const divInside = faq1 ? faq1[0].includes("faq-1-div") : null;
console.log(
  JSON.stringify(
    {
      divAfterDetails: divAfter,
      divInsideFaq1: divInside,
      hasOpenOnFaq1: /id="faq-1"[^>]*\sopen/.test(d) || /id="faq-1"[^>]*open=/.test(d),
      faq1Tail: faq1 ? faq1[0].slice(-280) : null,
      aroundDiv: (() => {
        const i = d.indexOf('id="faq-1-div"');
        return i >= 0 ? d.slice(i - 80, i + 120) : null;
      })(),
    },
    null,
    2
  )
);
