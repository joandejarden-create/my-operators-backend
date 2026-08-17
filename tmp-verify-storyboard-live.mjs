const url = "https://www.dealality.com/old-home?v=" + Date.now();
const t = await (await fetch(url)).text();
const checks = [
  "old-home-problem-storyboard.v20260729a.js",
  "old-home-problem-v2.v20260729e.js",
  "Most hotel owners do not lack options",
  "The same hotel story is repeated",
  "Responses return in different formats",
  "The first path gains momentum",
];
for (const c of checks) console.log(c, t.includes(c));
