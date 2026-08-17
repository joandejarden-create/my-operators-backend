async function check(url) {
  const r = await fetch(url, { headers: { "cache-control": "no-cache" } });
  const t = await r.text();
  const scripts = [...t.matchAll(/<script[^>]+src="([^"]+)"/gi)]
    .map((x) => x[1])
    .filter((s) => /fouc|hero-fit/i.test(s));
  const has29d = t.includes(
    "6a6a68e1fedb6e8369ce6830_dealality-old-home-hero-fit.v20260729d.css"
  );
  const has29c = t.includes(
    "6a6a5be1b239faf73c7e267f_dealality-old-home-hero-fit.v20260729c.css"
  );
  const hasGate29b = t.includes(
    "6a6a725d0ebd16d97971205a_old-home-fouc-gate.v20260729b.js"
  );
  const hasGate29a = t.includes("old-home-fouc-gate.v20260729a.js");
  const gateJs = await fetch(
    "https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a6a725d0ebd16d97971205a_old-home-fouc-gate.v20260729b.js",
    { headers: { "cache-control": "no-cache" } }
  ).then((x) => x.text());
  console.log(
    JSON.stringify(
      {
        url,
        status: r.status,
        hasGate29b,
        hasGate29a,
        has29dCssRefInHtml: has29d,
        has29cCssRefInHtml: has29c,
        heroFitScripts: scripts,
        gateHas29d:
          gateJs.includes("v20260729d.css") &&
          gateJs.includes("6a6a68e1fedb6e8369ce6830"),
        gateHas29c: gateJs.includes("v20260729c.css"),
        gateHasOhBoot: gateJs.includes("oh-boot") && gateJs.includes("oh-ready"),
        gateHasGlobeHide: gateJs.includes("#hero-globe-list"),
        gateHasW21: gateJs.includes("v20260729w21"),
      },
      null,
      2
    )
  );
}

await check("https://www.dealality.com/old-home");
await check("https://dealality.com/old-home");
