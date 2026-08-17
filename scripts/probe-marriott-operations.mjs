#!/usr/bin/env node
const url =
  "https://www.marriott.com/en-us/hotel-sitemap/dominican-republic-hotel-sitemap";
const r = await fetch(url, { headers: { "User-Agent": "Mozilla/5.0", Accept: "text/html" } });
const pp = JSON.parse((await r.text()).match(/<script id="__NEXT_DATA__"[^>]*>([\s\S]*?)<\/script>/i)[1])
  ?.props?.pageProps;
console.log("operationSignatures", JSON.stringify(pp?.operationSignatures, null, 2)?.slice(0, 2000));
console.log("\napolloEnvVars keys", Object.keys(pp?.apolloEnvVars || {}));
