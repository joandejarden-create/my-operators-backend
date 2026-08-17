import fs from "fs";

const src = JSON.parse(fs.readFileSync("tmp/dmp-set-embed-shell-c.json", "utf8"));
const code = src.actions[0].set_settings.operations[0].settings[0].static_text.value
  .replace(
    "6a6d675501f2a1c97e016821_old-home-manual-process.v20260801f12.css",
    "6a6d78c96d7424a2087ed030_old-home-manual-process.v20260801f13.css",
  )
  .replace('data-dmp-version="1.1.33"', 'data-dmp-version="1.1.35"');

if (!code.includes("v20260801f13.css")) throw new Error("f13 missing");
if (code.includes("v20260801f12")) throw new Error("f12 still present");

const payload = {
  siteId: src.siteId,
  pageId: src.pageId,
  context:
    "Updates Manual Process HtmlEmbed CSS to v20260801f13 and version 1.1.35.",
  actions: [
    {
      label: "set-embed-f13",
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

fs.writeFileSync("tmp/dmp-set-embed-f13.json", JSON.stringify(payload));
console.log(
  "ok",
  fs.statSync("tmp/dmp-set-embed-f13.json").size,
  code.includes("v20260801f13.css"),
  code.includes('data-dmp-version="1.1.35"'),
);
