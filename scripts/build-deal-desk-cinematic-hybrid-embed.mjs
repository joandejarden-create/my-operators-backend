import fs from "fs";
import path from "path";

const root = process.cwd();
const hotel =
  "https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a6bde85c014ee4e80e65c24_deal-desk-coastal-hotel-480.jpg";
const cssCdn =
  "https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a6c3db76db7f7c28a1f6fe0_oh-deal-desk-cinematic-v1.css";

let html = fs.readFileSync(
  path.join(root, "public/marketing/old-home-problem-deal-desk.v1.html"),
  "utf8"
);
html = html.replace(/src="[^"]*coastal-hotel[^"]*"/g, `src="${hotel}"`);
html = html.replace(/src="data:image\/[^"]+"/g, `src="${hotel}"`);
if (/<script/i.test(html)) throw new Error("script not allowed");

// Hybrid: @import inside embed <style> so Designer HtmlEmbed can fetch CDN CSS
// without a 75KB MCP payload. Repo CSS remains SoT; CDN asset matches current file.
const hybrid = `<style id="oh-deal-desk">@import url("${cssCdn}");</style>\n${html}`;

fs.writeFileSync(
  path.join(root, "docs/old-home-problem-deal-desk-embed-cinematic-v1-hybrid.html"),
  hybrid,
  "utf8"
);

const payload = {
  siteId: "68108c29063eeb5d1bd7ae4a",
  pageId: "68108c2a063eeb5d1bd7ae90",
  context:
    "Replace temporary polish-v2 probe with cinematic-v1 Deal Desk HtmlEmbed using in-embed style import and CDN hotel image.",
  actions: [
    {
      label: "set_cinematic_v1",
      set_settings: {
        operations: [
          {
            label: "embed_code",
            element_id: {
              component: "68108c2a063eeb5d1bd7ae90",
              element: "a64ef2f7-2f5f-ab92-9711-5f43f9eeb3fa",
            },
            settings: [{ key: "code", static_text: { value: hybrid } }],
          },
        ],
      },
    },
  ],
};

fs.writeFileSync(
  path.join(root, "docs/_mcp-set-cinematic-embed-hybrid-args.json"),
  JSON.stringify(payload),
  "utf8"
);

console.log(
  JSON.stringify(
    {
      hybridChars: hybrid.length,
      payloadBytes: Buffer.byteLength(JSON.stringify(payload)),
      hasCinematic: hybrid.includes("cinematic-v1"),
      hasHotel: hybrid.includes("6a6bde85c014ee4e80e65c24"),
      hasImport: hybrid.includes("@import"),
      hasScript: /<script/i.test(hybrid),
      hasAnimation: /@keyframes|animation\s*:/.test(hybrid),
      defaultState: (hybrid.match(/data-story-state="([^"]+)"/) || [])[1],
      cssCdn,
    },
    null,
    2
  )
);
