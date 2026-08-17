const urls = [];
for (let i = 1; i <= 7; i++) {
  urls.push(
    `https://www.ritzcarlton.com/content/dam/marriott-hws/sitemap-xmls/trc-en-sitemap-hws-${i}.xml`
  );
}
for (const u of urls) {
  const t = await fetch(u).then((r) => r.text());
  const hits = [...t.matchAll(/https:\/\/www\.ritzcarlton\.com[^<]*\/hotels\/[^<]+/gi)].filter((m) =>
    /plsrr|turks/i.test(m[0])
  );
  if (hits.length) {
    console.log(u);
    for (const h of hits) console.log(" ", h[0]);
  }
}
