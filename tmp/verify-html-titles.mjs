const html = await (
  await fetch(
    "https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a6cdd21ed65db752aa22a24_old-home-manual-process.v20260731b.html"
  )
).text();
const titles = [...html.matchAll(/class="dmp-problem-h">([^<]+)</g)].map((m) => m[1]);
const old = ["Fragmented Process", "Comparison Weakened", "Value Left Unseen"];
console.log(
  JSON.stringify(
    {
      titles,
      oldPresent: old.some((o) => html.includes(o)),
      hasProbe: /probe|DEBUG|TODO/i.test(html),
    },
    null,
    2
  )
);
