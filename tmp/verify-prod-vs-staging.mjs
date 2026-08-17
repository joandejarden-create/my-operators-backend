const urls = [
  "https://www.dealality.com/old-home",
  "https://dealality.com/old-home",
  "https://mvp-deal-capture.webflow.io/old-home",
];
for (const u of urls) {
  const r = await fetch(u, {
    cache: "no-store",
    redirect: "manual",
    headers: { "Cache-Control": "no-cache", Pragma: "no-cache" },
  });
  const loc = r.headers.get("location");
  let body = "";
  if (r.status >= 200 && r.status < 400 && r.status !== 301 && r.status !== 302) {
    body = await r.text();
  } else if (loc) {
    const r2 = await fetch(new URL(loc, u), { cache: "no-store", redirect: "follow" });
    body = await r2.text();
  }
  const m = body.match(/data-oh-problem="([^"]+)/);
  console.log(
    JSON.stringify({
      u,
      status: r.status,
      location: loc,
      dataOh: m ? m[1] : null,
      hasManualBoot: body.includes("manual-process.boot"),
      hasDealDesk: /deal-desk|oh-deal-desk/.test(body),
      hasCssD: body.includes("v20260731d.css"),
      len: body.length,
    })
  );
}
