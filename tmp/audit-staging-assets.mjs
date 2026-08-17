const staging = await (
  await fetch("https://mvp-deal-capture.webflow.io/old-home", { cache: "no-store" })
).text();
const prod = await (
  await fetch("https://www.dealality.com/old-home", { cache: "no-store" })
).text();

function extractAssets(html, label) {
  const css = [...html.matchAll(/old-home-manual-process[^"'\\s]*\.css/g)].map((m) => m[0]);
  const js = [...html.matchAll(/old-home-manual-process[^"'\\s]*\.js/g)].map((m) => m[0]);
  const htmlAssets = [...html.matchAll(/old-home-manual-process[^"'\\s]*\.html/g)].map(
    (m) => m[0]
  );
  const dataOh = (html.match(/data-oh-problem="([^"]+)/) || [])[1] || null;
  return {
    label,
    dataOh,
    css: [...new Set(css)],
    js: [...new Set(js)],
    htmlAssets: [...new Set(htmlAssets)],
    hasDealDesk: /deal-desk|oh-deal-desk|dealality-problem-desk/i.test(html),
    hasManualEmbed: html.includes("oh-manual-process-embed"),
    hasManyFutures: html.includes("many-futures") || html.includes("id=\"many-futures\""),
    hasPlatformFeatures: html.includes("platform-features"),
  };
}

console.log(JSON.stringify({ staging: extractAssets(staging), prod: extractAssets(prod) }, null, 2));

// Resolve boot HTML target from live boot asset if present
const bootMatch = staging.match(
  /https:\/\/cdn\.prod\.website-files\.com\/[^"']+old-home-manual-process\.boot[^"']+\.js/
);
if (bootMatch) {
  const bootUrl = bootMatch[0];
  const boot = await (await fetch(bootUrl, { cache: "no-store" })).text();
  const htmlUrl = (boot.match(/https:\/\/cdn\.prod\.website-files\.com\/[^"']+\.html/) || [])[0];
  const drawUrl = (boot.match(/https:\/\/cdn\.prod\.website-files\.com\/[^"']+\.js/) || []).filter(
    (u) => !u.includes(".boot.")
  )[0];
  console.log(JSON.stringify({ bootUrl, htmlUrl, drawUrlFromBoot: drawUrl }, null, 2));
  if (htmlUrl) {
    const h = await (await fetch(htmlUrl, { cache: "no-store" })).text();
    const titles = [...h.matchAll(/class="dmp-problem-h">([^<]+)</g)].map((m) => m[1]);
    console.log(JSON.stringify({ liveTitles: titles }, null, 2));
  }
}
