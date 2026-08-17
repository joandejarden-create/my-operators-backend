const t = await (
  await fetch("https://mvp-deal-capture.webflow.io/old-home", { cache: "no-store" })
).text();

// Rough section order from static HTML id= on section-ish landmarks
const re =
  /<(section|div)[^>]*\bid=["'](hero|fsw|about|oh-how-we-do-it|modules|platform-features|features|capabilities|ecosystem|engagement|trust|pricing|faq|insights|cta[^"']*|cta-band[^"']*)["'][^>]*>/gi;
const found = [];
let m;
while ((m = re.exec(t))) {
  found.push({ tag: m[1], id: m[2], index: m.index });
}
found.sort((a, b) => a.index - b.index);

const desired = [
  "Hero",
  "The Problem",
  "How It Works",
  "Benefits",
  "Capabilities",
  "Ecosystem",
  "Proof",
  "Pricing",
  "FAQ",
  "Insights",
  "CTA band",
];

console.log(
  JSON.stringify(
    {
      staticOrder: found,
      hasHowScript: /old-home-how-we-do-it/.test(t),
      desired,
    },
    null,
    2
  )
);
