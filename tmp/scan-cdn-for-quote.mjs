const page = await (
  await fetch("https://mvp-deal-capture.webflow.io/old-home", { cache: "no-store" })
).text();
const scripts = [
  ...page.matchAll(/https:\/\/cdn\.prod\.website-files\.com\/[^"']+/g),
].map((m) => m[0]);
const uniq = [...new Set(scripts)].filter((u) =>
  /old-home|ecosystem|modules|many|benefits|section-order|footer/i.test(u)
);
const needles = [
  "disciplined",
  "deserve closer",
  "better visibility into the alternatives",
  "perfect tool to centralize",
  "committing to a direction",
];
for (const u of uniq) {
  try {
    const t = await (await fetch(u)).text();
    if (needles.some((n) => t.toLowerCase().includes(n.toLowerCase()))) {
      console.log("HIT", u);
      for (const n of needles) {
        const i = t.toLowerCase().indexOf(n.toLowerCase());
        if (i >= 0) console.log(n, "=>", t.slice(Math.max(0, i - 80), i + 220).replace(/\s+/g, " "));
      }
    }
  } catch (e) {
    console.log("fail", u, String(e));
  }
}
console.log("scanned", uniq.length);
