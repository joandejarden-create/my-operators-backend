import fs from "fs";
import crypto from "crypto";

const buf = fs.readFileSync(
  "C:/Dev/deal-capture-proxy/public/marketing/old-home-motion.prod.v20260801g.js"
);
const sha256 =
  "sha256-" + crypto.createHash("sha256").update(buf).digest("base64");
const sha384 =
  "sha384-" + crypto.createHash("sha384").update(buf).digest("base64");
const md5 = crypto.createHash("md5").update(buf).digest("hex");
const src = buf.toString("utf8");
console.log(
  JSON.stringify(
    {
      sha256,
      sha384,
      md5,
      size: buf.length,
      pathUnlock: /path === \"\/\"/.test(src),
      isOldHomeSnippet: src.match(/function isOldHome\(\)[\s\S]{0,220}/)?.[0],
    },
    null,
    2
  )
);

const cdn =
  "https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a6e2be2eb6dc0e334533fbc_old-home-motion.prod.v20260801g.js";
const remote = await fetch(cdn).then((r) => r.text());
console.log(
  JSON.stringify({
    cdnLen: remote.length,
    cdnPathUnlock: /path === \"\/\"/.test(remote),
    matchesLocal: remote === src,
  })
);
