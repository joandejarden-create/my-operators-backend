import fs from "fs";

const head = fs.readFileSync("tmp-old-home-head-w16.txt", "utf8");
const payload = {
  actions: [
    {
      label: "set-head-w16-modules-fix",
      set_page_freeform_code: {
        page_id: "68108c2a063eeb5d1bd7ae90",
        location: "head",
        content: head,
      },
    },
  ],
  context:
    "Publish Old Home freeform head w16 to fix How Dealality Works tab visibility.",
};

fs.writeFileSync("tmp-mcp-set-head-w16.json", JSON.stringify(payload));
console.log("payload bytes", Buffer.byteLength(JSON.stringify(payload)));
console.log("head has w16", head.includes("w16.css"));
console.log("head has setPanel", head.includes("setPanel"));
console.log("head has pricing", head.includes("pricing.v20260729a"));
console.log("head has platform-features", head.includes("platform-features"));
