import https from "https";

function fetch(url) {
  return new Promise((resolve, reject) => {
    https
      .get(url, { headers: { "user-agent": "DealalityFooterAudit/1.0" } }, (res) => {
        const chunks = [];
        res.on("data", (c) => chunks.push(c));
        res.on("end", () =>
          resolve({ status: res.statusCode, body: Buffer.concat(chunks).toString("utf8") })
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
        resolve({ status: res.statusCode, location: res.headers.location || "" });
        res.resume();
      }
    );
    req.on("error", reject);
    req.end();
  });
}

const wif = await fetch("https://www.dealality.com/who-its-for");
const scripts = [...wif.body.matchAll(/src="([^"]+)"/g)]
  .map((m) => m[1])
  .filter((s) => /who-its|pricing|footer|boot|old-home|signup/i.test(s));
console.log("who-its-for relevant scripts:\n" + scripts.join("\n"));
console.log("has dealality-who-its-for?", /dealality-who-its-for/i.test(wif.body));
console.log("title", (wif.body.match(/<title[^>]*>([\s\S]*?)<\/title>/i) || [])[1]);
console.log("body snippet", wif.body.replace(/\s+/g, " ").slice(0, 500));

for (const u of [
  "https://www.dealality.com/signup",
  "https://www.dealality.com/sign-up",
  "https://www.dealality.com/request-access",
  "https://www.dealality.com/old-home#trust",
]) {
  if (u.includes("#")) continue;
  const h = await head(u);
  console.log(h.status, u, h.location);
}

// Check many-futures / ecosystem for product naming
const old = await fetch("https://www.dealality.com/old-home");
for (const id of ["many-futures", "ecosystem", "oh-how-we-do-it", "modules", "faq", "insights"]) {
  const re = new RegExp(`id=["']${id}["'][\\s\\S]{0,900}`);
  const m = old.body.match(re);
  if (!m) {
    console.log(id, "MISSING");
    continue;
  }
  const h2 = [...m[0].matchAll(/<(h[1-3])[^>]*>([\s\S]*?)<\/\1>/gi)].map((x) =>
    x[2].replace(/<[^>]+>/g, "").replace(/\s+/g, " ").trim()
  );
  console.log(id, "->", h2.slice(0, 3));
}
