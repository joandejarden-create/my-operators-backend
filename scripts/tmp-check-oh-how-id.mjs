const t = await (await fetch("https://mvp-deal-capture.webflow.io/old-home", { cache: "no-store" })).text();
const ids = [...t.matchAll(/\bid=["']([^"']+)["']/g)].map((m) => m[1]);
const interesting = ids.filter((id) =>
  /about|how|modules|pricing|faq|insights|trust|nav|mnav/i.test(id)
);
console.log(
  JSON.stringify(
    {
      staticOhHow: ids.includes("oh-how-we-do-it"),
      scriptMentions: /oh-how-we-do-it/.test(t),
      howScript: (t.match(/old-home-how-we-do-it[^"'<\s]*/g) || []).slice(0, 5),
      interestingIds: [...new Set(interesting)].sort(),
    },
    null,
    2
  )
);
