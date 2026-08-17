const t = await (await fetch("https://www.dealality.com/old-home?v=" + Date.now())).text();
for (const c of [
  'id="pricing"',
  "Three Monetization Paths",
  "Pay on success",
  "pricing-card-brands",
  "pricing-card-operators",
  "dealality-old-home-pricing.v20260729a.css",
  'href="#pricing"',
]) {
  console.log(t.includes(c) ? "YES" : "NO ", c);
}
