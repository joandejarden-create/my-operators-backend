const html = await (
  await fetch("https://www.dealality.com/old-home?cb=" + Date.now())
).text();
const scripts = [...html.matchAll(/src="([^"]+)"/g)].map((m) => m[1]);
console.log(
  "footer-ish",
  scripts.filter((s) => /footer|rotat|premium|oh\.|fouc|boot|globe/i.test(s))
);
const inlineHasRot =
  html.includes("oh-hrword-on") && html.includes("getElementById(\"rotator\")");
console.log({
  inlineRotatorCode: html.includes('getElementById("rotator")'),
  footerOh: scripts.filter((s) => /footer-oh|old-home-footer/i.test(s)),
  hasOhReady: html.includes("oh-ready"),
  hasOhBoot: html.includes("oh-boot"),
});

// find end scripts chunk
const idx = html.lastIndexOf("oh-hrword");
console.log("last oh-hrword idx", idx);
if (idx > 0) console.log(html.slice(Math.max(0, idx - 200), idx + 400));
