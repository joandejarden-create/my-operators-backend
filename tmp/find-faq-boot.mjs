import fs from "fs";

const html = await (await fetch("https://mvp-deal-capture.webflow.io/old-home")).text();
const boots = [...html.matchAll(/old-home-boot-guard[^\s\"']+/g)].map((m) => m[0]);
const faqs = [...html.matchAll(/dealality-old-home-faqs[^\s\"']+/g)].map((m) => m[0]);
const guards = [...html.matchAll(/6a6d0a7244a2d902a02b428b[^\s\"']*|oldhomebootguard[^\s\"']*/gi)].map(
  (m) => m[0]
);
console.log({ boots: [...new Set(boots)], faqs: [...new Set(faqs)], guards: [...new Set(guards)] });
fs.writeFileSync("tmp/staging-old-home-snip.html", html.slice(0, 500));
