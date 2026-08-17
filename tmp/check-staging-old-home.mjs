const t = await (
  await fetch("https://mvp-deal-capture.webflow.io/old-home", { cache: "no-store" })
).text();
const m = t.match(/data-oh-problem="([^"]+)/);
console.log(
  JSON.stringify(
    {
      hasDealDeskEmbed:
        t.includes("oh-deal-desk") || t.includes("dealality-problem-desk"),
      hasManualBoot: t.includes("manual-process.boot"),
      hasManualCssD: t.includes("v20260731d"),
      hasManualHtmlB: t.includes("v20260731b.html"),
      dataOh: m ? m[1] : null,
      hasOhManualEmbed: t.includes("oh-manual-process-embed"),
    },
    null,
    2
  )
);
