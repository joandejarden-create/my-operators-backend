import fs from "fs";

const CDN =
  "https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a6d8271cf83f720d5b8c61b_old-home-manual-process.v20260801f15.css";
let code = fs.readFileSync("docs/_dmp_embed_inline.html", "utf8");
code = code.replace(
  /6a6d7cdc22501d12c82141ab_old-home-manual-process\.v20260801f14\.css/,
  "6a6d8271cf83f720d5b8c61b_old-home-manual-process.v20260801f15.css"
);
code = code.replace(/data-dmp-version="[^"]+"/, 'data-dmp-version="1.1.37"');
code = code.replace(/data-oh-manual-process="[^"]+"/, 'data-oh-manual-process="1.1.37"');
if (!code.includes("v20260801f15.css")) throw new Error("f15 missing");
fs.writeFileSync("docs/_dmp_embed_inline.html", code);

const payload = {
  siteId: "68108c29063eeb5d1bd7ae4a",
  pageId: "68108c2a063eeb5d1bd7ae90",
  context:
    "Updates Manual Process CSS: muted problem body again; Email/Spreadsheets/Conversations white.",
  actions: [
    {
      label: "set-embed-f15",
      set_settings: {
        operations: [
          {
            label: "code",
            element_id: {
              component: "68108c2a063eeb5d1bd7ae90",
              element: "a64ef2f7-2f5f-ab92-9711-5f43f9eeb3fa",
            },
            settings: [{ key: "code", static_text: { value: code } }],
          },
        ],
      },
    },
  ],
};
fs.writeFileSync("tmp/dmp-set-embed-f15.json", JSON.stringify(payload));
console.log(JSON.stringify({ chars: code.length, cdn: CDN, hasF15: true }));
