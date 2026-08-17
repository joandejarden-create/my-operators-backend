const t = await (await fetch("https://www.dealality.com/old-home?v=" + Date.now())).text();
const keys = [
  "old-home-problem",
  "freeform-head",
  "about-frag",
  "oh-problem",
  "data-oh-problem",
  "w21",
  "w20",
  "FooterOH",
  "problem-v2",
  "oh-pvl",
  "oldhomebootguard",
  "assetboot",
];
for (const k of keys) {
  const re = new RegExp(k.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"), "gi");
  console.log(k + ":", (t.match(re) || []).length);
}
const m = t.match(/cdn\.prod\.website-files\.com[^"']+/g) || [];
console.log("---cdn assets---");
[...new Set(m)]
  .filter((x) => /old-home|freeform|problem|footer|boot/i.test(x))
  .slice(0, 50)
  .forEach((x) => console.log(x));
const about = t.match(/id=["']about["'][\s\S]{0,800}/);
console.log("---about snippet---");
console.log(about ? about[0].slice(0, 600) : "none");
