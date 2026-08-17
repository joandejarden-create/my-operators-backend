const urls = [
  "https://mvp-deal-capture.webflow.io/old-home",
  "https://www.dealality.com/old-home",
];
for (const u of urls) {
  const t = await (await fetch(u, { cache: "no-store" })).text();
  const hasSection =
    /id="platform-features"/.test(t) || /id='platform-features'/.test(t);
  const m = t.match(/data-oh-problem="([^"]+)/);
  console.log(
    JSON.stringify({
      host: u.includes("webflow") ? "staging" : "prod",
      hasSection,
      hasCssRef: t.includes("platform-features"),
      dataOh: m ? m[1] : null,
      hasManual: t.includes("manual-process.boot") || t.includes("oh-manual-process"),
      hasDealDesk: /deal-desk|oh-deal-desk/.test(t),
    })
  );
}
