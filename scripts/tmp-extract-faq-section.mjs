import fs from "fs";

const html = fs.readFileSync("public/marketing/dealality-old-home-premium.html", "utf8");
const match = html.match(/<section id="faq"[\s\S]*?<\/section>/);
if (!match) {
  console.error("FAQ section not found");
  process.exit(1);
}
const out = match[0].replace(/\r\n/g, "\n").replace(/>\s+</g, "><").trim();
fs.writeFileSync("tmp-faq-section.html", out);
console.log(
  JSON.stringify({
    bytes: out.length,
    has7: out.includes("faq-7"),
    broker: out.includes("Is Dealality a broker"),
    beta: out.includes("private beta"),
  })
);
