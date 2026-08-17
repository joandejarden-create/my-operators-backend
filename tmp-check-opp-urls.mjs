async function check(url) {
  const html = await (
    await fetch(url, { headers: { "cache-control": "no-cache" } })
  ).text();
  const needle = "See the opportunity before selecting the relationship";
  console.log(
    JSON.stringify({
      url,
      hasNeedle: html.includes(needle),
      hasFeaturesId: html.includes('id="features"'),
      hasOhFeatures: html.includes("oh-features"),
      statusLen: html.length,
    })
  );
}

await check("https://www.dealality.com/old-home?cb=" + Date.now());
await check("https://www.dealality.com/?cb=" + Date.now());
await check("https://www.dealality.com/home?cb=" + Date.now());
