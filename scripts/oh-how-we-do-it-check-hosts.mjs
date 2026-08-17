const urls = [
  "https://www.dealality.com/old-home",
  "https://dealality.com/old-home",
  "https://mvp-deal-capture.webflow.io/old-home",
];
for (const base of urls) {
  const url = base + "?cb=" + Date.now();
  try {
    const html = await (await fetch(url, { headers: { "cache-control": "no-cache", pragma: "no-cache" } })).text();
    const hw = [...html.matchAll(/old-home-how-we-do-it[^"'\s]*/g)].map((m) => m[0]);
    const bg = [...html.matchAll(/old-home-boot-guard[^"'\s]*/g)].map((m) => m[0]);
    console.log(base, { hw, bg, has30e: /30e|6a6afeab/.test(html) });
  } catch (e) {
    console.log(base, "ERR", e.message);
  }
}
