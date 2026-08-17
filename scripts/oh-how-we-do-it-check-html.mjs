const url = "https://www.dealality.com/old-home?cb=" + Date.now();
const html = await (await fetch(url, { headers: { "cache-control": "no-cache" } })).text();
const scripts = [...html.matchAll(/src="([^"]+)"/g)]
  .map((m) => m[1])
  .filter((s) => /how-we-do|ohhow|boot-guard|fouc|30[cde]/i.test(s));
console.log(JSON.stringify({
  has30e: html.includes("30e") || html.includes("6a6afeab"),
  has30dAcc: html.includes("6a6afacc"),
  has30dFd6e: html.includes("6a6afd6e"),
  scripts,
}, null, 2));
