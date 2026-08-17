import fs from "fs";

const html = fs.readFileSync("c:/Dev/deal-capture-proxy/tmp-old-home.html", "utf8");
const m = html.match(/<footer id="footer-new"[\s\S]*?<\/footer>/);
if (!m) {
  console.error("NO FOOTER");
  process.exit(1);
}
const footer = m[0];
fs.writeFileSync("c:/Dev/deal-capture-proxy/tmp/_footer-extracted.html", footer);
console.log("len", footer.length);

// Pretty-ish: replace tags for readability
const pretty = footer
  .replace(/></g, ">\n<")
  .replace(/\s{2,}/g, " ");
fs.writeFileSync("c:/Dev/deal-capture-proxy/tmp/_footer-pretty.html", pretty);

// Extract links
const links = [];
const re = /<a\b([^>]*)>([\s\S]*?)<\/a>/gi;
let match;
while ((match = re.exec(footer))) {
  const attrs = match[1];
  const rawInner = match[2];
  const href = (attrs.match(/href="([^"]*)"/i) || [])[1] || "";
  const id = (attrs.match(/\bid="([^"]*)"/i) || [])[1] || "";
  const aria = (attrs.match(/aria-label="([^"]*)"/i) || [])[1] || "";
  const text = rawInner
    .replace(/<[^>]+>/g, " ")
    .replace(/&amp;/g, "&")
    .replace(/&#x27;/g, "'")
    .replace(/&apos;/g, "'")
    .replace(/\s+/g, " ")
    .trim();
  links.push({ id, aria, href, text: text || aria || "(icon/empty)" });
}
console.log(JSON.stringify(links, null, 2));

// Column headings
const heads = [...footer.matchAll(/id="(footer-h-[^"]+)"[^>]*>([\s\S]*?)<\//g)].map((x) => ({
  id: x[1],
  text: x[2].replace(/<[^>]+>/g, "").trim(),
}));
console.log("HEADS", heads);

const blurb = footer.match(/id="footer-blurb"[^>]*>([\s\S]*?)<\//);
const tagline = footer.match(/id="footer-tagline"[^>]*>([\s\S]*?)<\//);
const socialLabel = footer.match(/id="footer-social-label"[^>]*>([\s\S]*?)<\//);
console.log("TAGLINE", tagline && tagline[1].replace(/<[^>]+>/g, "").trim());
console.log("BLURB", blurb && blurb[1].replace(/<[^>]+>/g, "").trim());
console.log("SOCIAL_LABEL", socialLabel && socialLabel[1].replace(/<[^>]+>/g, "").trim());
