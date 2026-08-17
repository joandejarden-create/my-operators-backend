import fs from "fs";
import https from "https";

function fetch(url) {
  return new Promise((resolve, reject) => {
    https
      .get(url, { headers: { "user-agent": "DealalityFooterAudit/1.0" } }, (res) => {
        const chunks = [];
        res.on("data", (c) => chunks.push(c));
        res.on("end", () =>
          resolve({
            status: res.statusCode,
            headers: res.headers,
            body: Buffer.concat(chunks).toString("utf8"),
            url,
          })
        );
      })
      .on("error", reject);
  });
}

function head(url) {
  return new Promise((resolve, reject) => {
    const req = https.request(
      url,
      { method: "HEAD", headers: { "user-agent": "DealalityFooterAudit/1.0" } },
      (res) => {
        resolve({ status: res.statusCode, location: res.headers.location || "", url });
        res.resume();
      }
    );
    req.on("error", reject);
    req.end();
  });
}

const ids = [
  "trust",
  "faq",
  "modules",
  "about",
  "insights",
  "pricing",
  "many-futures",
  "oh-how-we-do-it",
  "ecosystem",
  "testimonials",
  "footer-new",
  "footer",
];

const local = fs.readFileSync("c:/Dev/deal-capture-proxy/tmp-old-home.html", "utf8");
console.log("=== LOCAL tmp-old-home.html section IDs ===");
for (const id of ids) {
  const found = new RegExp(`id=["']${id}["']`, "i").test(local);
  console.log(`${id}: ${found ? "FOUND" : "MISSING"}`);
}

// Case studies / trust mentions
const caseIdx = local.toLowerCase().indexOf("case stud");
console.log("case stud local idx", caseIdx);

const urls = [
  "https://www.dealality.com/old-home",
  "https://www.dealality.com/who-its-for",
  "https://www.dealality.com/opportunity-review",
  "https://www.dealality.com/privacy",
  "https://www.dealality.com/terms",
  "https://www.dealality.com/insights",
  "https://www.dealality.com/",
  "https://www.dealality.com/login",
  "https://www.dealality.com/log-in",
  "https://www.dealality.com/sign-in",
  "https://www.dealality.com/member-login",
  "https://www.dealality.com/faq",
  "https://www.dealality.com/about",
  "https://www.dealality.com/case-studies",
  "https://www.dealality.com/pricing",
  "https://www.dealality.com/contact",
  "https://www.dealality.com/method",
];

console.log("\n=== LIVE HEAD/GET status ===");
for (const u of urls) {
  try {
    const h = await head(u);
    console.log(`${h.status}\t${u}\t${h.location}`);
  } catch (e) {
    console.log(`ERR\t${u}\t${e.message}`);
  }
}

console.log("\n=== LIVE /old-home footer + anchors ===");
const live = await fetch("https://www.dealality.com/old-home");
console.log("status", live.status, "len", live.body.length);
for (const id of ids) {
  const found = new RegExp(`id=["']${id}["']`, "i").test(live.body);
  console.log(`${id}: ${found ? "FOUND" : "MISSING"}`);
}
const fm = live.body.match(/<footer id="footer-new"[\s\S]*?<\/footer>/);
if (fm) {
  fs.writeFileSync("c:/Dev/deal-capture-proxy/tmp/_live-footer.html", fm[0]);
  const links = [];
  const re = /<a\b([^>]*)>([\s\S]*?)<\/a>/gi;
  let m;
  while ((m = re.exec(fm[0]))) {
    const attrs = m[1];
    const href = (attrs.match(/href="([^"]*)"/i) || [])[1] || "";
    const id = (attrs.match(/\bid="([^"]*)"/i) || [])[1] || "";
    const aria = (attrs.match(/aria-label="([^"]*)"/i) || [])[1] || "";
    const text = m[2]
      .replace(/<[^>]+>/g, " ")
      .replace(/&amp;/g, "&")
      .replace(/&#x27;/g, "'")
      .replace(/\s+/g, " ")
      .trim();
    links.push({ id, aria, href, text: text || aria || "(icon)" });
  }
  console.log(JSON.stringify(links, null, 2));
}

console.log("\n=== LIVE /who-its-for title/noindex ===");
const wif = await fetch("https://www.dealality.com/who-its-for");
const title = (wif.body.match(/<title[^>]*>([\s\S]*?)<\/title>/i) || [])[1];
const robots = (wif.body.match(/name=["']robots["'][^>]*>/i) || [])[0];
const noindex = /noindex/i.test(wif.body);
console.log("title", title && title.replace(/\s+/g, " ").trim());
console.log("robots meta", robots || "(none)");
console.log("noindex anywhere", noindex);
console.log("has #pricing", /id=["']pricing["']/i.test(wif.body));
console.log("has footer-new", /id=["']footer-new["']/i.test(wif.body));

console.log("\n=== LIVE / privacy/terms titles ===");
for (const u of [
  "https://www.dealality.com/privacy",
  "https://www.dealality.com/terms",
  "https://www.dealality.com/insights",
  "https://www.dealality.com/",
]) {
  const r = await fetch(u);
  const t = (r.body.match(/<title[^>]*>([\s\S]*?)<\/title>/i) || [])[1];
  console.log(r.status, u, t && t.replace(/\s+/g, " ").trim().slice(0, 80));
}
