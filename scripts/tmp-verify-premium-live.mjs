const t = await (await fetch("https://www.dealality.com/old-home")).text();
const ids = [...t.matchAll(/\bid=["']([^"']+)["']/g)].map((m) => m[1]);
const uniq = [...new Set(ids)].filter((id) =>
  /nav|hero|problem|how|proof|trust|cta|footer|premium|dc-/.test(id)
);
console.log(uniq.join("\n"));
console.log("---");
console.log("Understand H1", /Understand the possibilities/.test(t));
console.log("how-it-works", /how-it-works/.test(t));
console.log("product-proof", /product-proof/.test(t));
console.log("Explore CTA", /Explore Your Hotel Opportunity/.test(t));
console.log("old H1", /Do not let the first conversation/.test(t));
console.log("FAQ", />\s*FAQ/i.test(t));
console.log("logo", /cdn\.prod\.website-files\.com.*Dealality|alt="Dealality"/.test(t));
