import fs from "fs";
import { decode } from "html-entities";

// Prefer premium HTML for fuller paragraph structure
const html = fs.readFileSync(
  "public/marketing/dealality-old-home-premium.html",
  "utf8"
);

function decodeHtml(s) {
  return s
    .replace(/&#x27;/g, "'")
    .replace(/&quot;/g, '"')
    .replace(/&amp;/g, "&")
    .replace(/&lt;/g, "<")
    .replace(/&gt;/g, ">");
}

const items = [];
for (let i = 1; i <= 10; i++) {
  const qMatch = html.match(
    new RegExp(`id="faq-${i}-q"[^>]*>([\\s\\S]*?)</`, "i")
  );
  const bodyMatch = html.match(
    new RegExp(
      `id="faq-${i}-body"[^>]*>([\\s\\S]*?)</div>\\s*</details>`,
      "i"
    )
  );
  if (!qMatch || !bodyMatch) {
    console.log("missing", i);
    continue;
  }
  const q = decodeHtml(qMatch[1].replace(/<[^>]+>/g, "").trim());
  const paras = [...bodyMatch[1].matchAll(/<p[^>]*>([\\s\\S]*?)<\\/p>/gi)].map(
    (m) =>
      decodeHtml(m[1].replace(/<[^>]+>/g, " ").replace(/\\s+/g, " ").trim())
  );
  // fallback if no <p>
  if (!paras.length) {
    const plain = decodeHtml(
      bodyMatch[1].replace(/<[^>]+>/g, " ").replace(/\\s+/g, " ").trim()
    );
    if (plain) paras.push(plain);
  }
  items.push({ i, q, paras });
}
fs.writeFileSync(
  "tmp/original-old-home-faqs.json",
  JSON.stringify(items, null, 2)
);
console.log(JSON.stringify(items, null, 2));
