const urls = [
  "https://www.dealality.com/old-home",
  "https://www.dealality.com/insights",
];

for (const u of urls) {
  const r = await fetch(u);
  const t = await r.text();
  console.log("\n===", u, r.status, "===");
  const hrefs = [...t.matchAll(/href="(\/[^"]+)"/gi)]
    .map((m) => m[1])
    .filter((h) => /soft|brand|insight|hotel|marriott|owner/i.test(h))
    .slice(0, 20);
  console.log("links", hrefs);
  console.log(
    "ins-date rules",
    t.match(/#ins-\d-date[^}]*}/g) || t.match(/oh-ins-date[^}]*}/g)
  );
  console.log(
    "date color snippets",
    [...t.matchAll(/date[^;{]{0,40}color:[^;}{]+/gi)].slice(0, 10).map((m) => m[0])
  );
}

// Try common article slug
const articleCandidates = [
  "https://www.dealality.com/post/hotel-soft-brands-vs-hard-brands-how-owners-should-decide",
  "https://www.dealality.com/insights/hotel-soft-brands-vs-hard-brands-how-owners-should-decide",
  "https://www.dealality.com/blog-posts/hotel-soft-brands-vs-hard-brands-how-owners-should-decide",
];

for (const u of articleCandidates) {
  const r = await fetch(u);
  console.log(u, r.status);
  if (r.ok) {
    const t = await r.text();
    fsWriteSnippet(u, t);
  }
}

function fsWriteSnippet(u, t) {
  const styleBits = [
    ...t.matchAll(/(\.|#)[a-zA-Z0-9_-]*(date|meta|posted)[^{]*\{[^}]*\}/gi),
  ]
    .map((m) => m[0])
    .slice(0, 30);
  console.log("article styles", styleBits);
  const inline = [...t.matchAll(/style="[^"]*color:[^"]+"/gi)]
    .map((m) => m[0])
    .slice(0, 20);
  console.log("inline colors", inline);
}
