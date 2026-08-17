import fs from "fs";

const html = fs.readFileSync("tmp/old-home-live.html", "utf8");
const qs = [];
const re = /id="faq-(\d+)-q"[^>]*>([^<]*)</g;
let m;
while ((m = re.exec(html))) qs.push({ n: m[1], q: m[2].trim() });
console.log("count", qs.length);
qs.forEach((x) => console.log(x.n, x.q));

for (let i = 1; i <= 12; i++) {
  const re2 = new RegExp(
    `id="faq-${i}-body"[\\s\\S]*?(?=<details|id="faq-${i + 1}|</div></div></section|$)`,
    "i"
  );
  const mm = html.match(re2);
  if (!mm) continue;
  const text = mm[0]
    .replace(/<[^>]+>/g, " ")
    .replace(/\s+/g, " ")
    .trim()
    .slice(0, 280);
  console.log("BODY", i, text);
}
