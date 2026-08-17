const https = require("https");
const fs = require("fs");
const { patchHead } = require("./tmp-patch-insights-head.cjs");

function get(u) {
  return new Promise((res, rej) => {
    https
      .get(u, (r) => {
        let d = "";
        r.on("data", (c) => (d += c));
        r.on("end", () => res(d));
      })
      .on("error", rej);
  });
}

(async () => {
  const html = await get("https://www.dealality.com/old-home");
  const start = html.indexOf('<link rel="preconnect" href="https://fonts.googleapis.com">');
  if (start < 0) throw new Error("preconnect not found");
  // Freeform head ends at </style> after the insights overrides
  const styleStart = html.indexOf("<style>", start);
  const styleEnd = html.indexOf("</style>", styleStart);
  if (styleStart < 0 || styleEnd < 0) throw new Error("style block not found");
  let head = html.slice(start, styleEnd + "</style>".length);
  // Unescape if needed — live HTML is raw
  head = head + "\n";
  fs.writeFileSync("tmp-old-home-head-live.txt", head);
  console.log("extracted head bytes", head.length);
  console.log("has old calc", head.includes("var(--ins-visible,3)"));
  const patched = patchHead(head);
  fs.writeFileSync("tmp-old-home-head-patched.txt", patched);
  console.log("patched bytes", patched.length);
  console.log("has 360px", patched.includes("360px"));
  console.log("still has old calc override", /flex:0 0 calc\(\(100% - \(var\(--ins-visible/.test(patched));
  const payload = {
    actions: [
      {
        label: "set-head",
        set_page_freeform_code: {
          page_id: "68108c2a063eeb5d1bd7ae90",
          location: "head",
          content: patched,
        },
      },
    ],
    context:
      "Patch Old Home head freeform CSS so Insights carousel scrolls six fixed-width cards.",
  };
  fs.writeFileSync("tmp-mcp-set-head-payload.json", JSON.stringify(payload));
  console.log("payload ready", Buffer.byteLength(JSON.stringify(payload)));
})();
