const r = await fetch("https://www.dealality.com/old-home?nocache=" + Date.now());
const t = await r.text();
console.log("has cta handler", t.includes('getElementById("cta-band-btn")'));
console.log("has loading opp", t.includes("Loading opportunity review"));
console.log("has opportunity-review check", t.includes("isOpportunityUrl") || t.includes("opportunity-review/?"));
console.log("cta btn still links", /id="cta-band-btn"[^>]*href="[^"]*opportunity-review/.test(t));
