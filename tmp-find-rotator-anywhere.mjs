const html = await (
  await fetch("https://www.dealality.com/old-home?cb=" + Date.now())
).text();

const scripts = [...html.matchAll(/<script[^>]*>([\s\S]*?)<\/script>/gi)];
let hits = [];
for (const m of scripts) {
  const srcMatch = m[0].match(/src="([^"]+)"/);
  const body = m[1] || "";
  if (
    /rotator|oh-hrword|footer-oh|translateY\(\(\(c-ri\)/i.test(m[0]) ||
    /rotator|oh-hrword/i.test(body)
  ) {
    hits.push({
      src: srcMatch && srcMatch[1],
      inlineLen: body.length,
      hasRot: /getElementById\(["']rotator["']\)/.test(body),
      snippet: (srcMatch ? srcMatch[1] : body.slice(0, 120)).slice(0, 180),
    });
  }
}
console.log(JSON.stringify(hits, null, 2));

// all script srcs
console.log(
  "ALL_SCRIPTS",
  [...html.matchAll(/<script[^>]+src="([^"]+)"/gi)].map((m) => m[1])
);
