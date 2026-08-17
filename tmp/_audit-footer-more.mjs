import https from "https";
import fs from "fs";

function fetch(url) {
  return new Promise((resolve, reject) => {
    https
      .get(url, { headers: { "user-agent": "DealalityFooterAudit/1.0" } }, (res) => {
        const chunks = [];
        res.on("data", (c) => chunks.push(c));
        res.on("end", () =>
          resolve({ status: res.statusCode, body: Buffer.concat(chunks).toString("utf8"), url })
        );
      })
      .on("error", reject);
  });
}

function snippetAround(html, needle, before = 120, after = 200) {
  const i = html.toLowerCase().indexOf(needle.toLowerCase());
  if (i < 0) return null;
  return html.slice(Math.max(0, i - before), Math.min(html.length, i + after)).replace(/\s+/g, " ");
}

function extractLinks(footerHtml) {
  const links = [];
  const re = /<a\b([^>]*)>([\s\S]*?)<\/a>/gi;
  let m;
  while ((m = re.exec(footerHtml))) {
    const attrs = m[1];
    const href = (attrs.match(/href="([^"]*)"/i) || [])[1] || "";
    const text = m[2]
      .replace(/<[^>]+>/g, " ")
      .replace(/&amp;/g, "&")
      .replace(/&#x27;/g, "'")
      .replace(/\s+/g, " ")
      .trim();
    links.push({ href, text });
  }
  return links;
}

const old = await fetch("https://www.dealality.com/old-home");
console.log("=== #trust heading context ===");
console.log(snippetAround(old.body, 'id="trust"'));
console.log(snippetAround(old.body, "testimonials-h2"));
console.log(snippetAround(old.body, "What owners"));
// Find h2 near trust
const trustMatch = old.body.match(/id=["']trust["'][\s\S]{0,1200}/);
if (trustMatch) {
  const heads = [...trustMatch[0].matchAll(/<(h[1-4])[^>]*>([\s\S]*?)<\/\1>/gi)].map((x) =>
    x[2].replace(/<[^>]+>/g, "").replace(/\s+/g, " ").trim()
  );
  console.log("headings near #trust:", heads.slice(0, 8));
}

console.log("\n=== #about heading ===");
const aboutMatch = old.body.match(/id=["']about["'][\s\S]{0,800}/);
if (aboutMatch) {
  const heads = [...aboutMatch[0].matchAll(/<(h[1-4]|p)[^>]*id=["'][^"']*["'][^>]*>([\s\S]*?)<\//gi)].map(
    (x) => x[0].replace(/<[^>]+>/g, " ").replace(/\s+/g, " ").trim().slice(0, 100)
  );
  console.log(heads.slice(0, 6));
}

console.log("\n=== #modules heading ===");
const mod = old.body.match(/id=["']modules["'][\s\S]{0,600}/);
if (mod) {
  const heads = [...mod[0].matchAll(/<(h[1-4])[^>]*>([\s\S]*?)<\/\1>/gi)].map((x) =>
    x[2].replace(/<[^>]+>/g, "").replace(/\s+/g, " ").trim()
  );
  console.log("modules heads", heads);
}

console.log("\n=== #pricing presence / hidden by CSS in HTML ===");
console.log("pricing id", /id=["']pricing["']/.test(old.body));
console.log("oh-hide-landing-pricing in page?", /oh-hide-landing-pricing/.test(old.body));
console.log("boot-guard script?", /old-home-boot-guard/.test(old.body));
const boot = old.body.match(/old-home-boot-guard\.v[0-9a-z]+\.js/);
console.log("boot version", boot && boot[0]);

console.log("\n=== /login page ===");
const login = await fetch("https://www.dealality.com/login");
console.log("status", login.status, "len", login.body.length);
console.log("title", (login.body.match(/<title[^>]*>([\s\S]*?)<\/title>/i) || [])[1]);
console.log("has memberstack", /memberstack/i.test(login.body));
console.log("snippet", login.body.slice(0, 400).replace(/\s+/g, " "));

console.log("\n=== root / footer ===");
const root = await fetch("https://www.dealality.com/");
const rf = root.body.match(/<footer[\s\S]*?<\/footer>/i);
console.log("has footer", !!rf, "footer-new", /footer-new/.test(root.body));
if (rf) {
  fs.writeFileSync("c:/Dev/deal-capture-proxy/tmp/_root-footer.html", rf[0]);
  console.log(JSON.stringify(extractLinks(rf[0]), null, 2));
}

console.log("\n=== /who-its-for footer/nav ===");
const wif = await fetch("https://www.dealality.com/who-its-for");
const wf = wif.body.match(/<footer[\s\S]*?<\/footer>/i);
console.log("has footer", !!wf);
if (wf) console.log(JSON.stringify(extractLinks(wf[0]), null, 2));
console.log("pricing id on who-its-for", /id=["']pricing["']/.test(wif.body));
console.log("oh-pricing class", /oh-pricing/.test(wif.body));
console.log("snippet pricing", snippetAround(wif.body, "pricing", 80, 160));

console.log("\n=== insights collection vs #insights ===");
const ins = await fetch("https://www.dealality.com/insights");
console.log("insights title", (ins.body.match(/<title[^>]*>([\s\S]*?)<\/title>/i) || [])[1]);
console.log("footer on insights?", /footer-new|<footer/i.test(ins.body));
