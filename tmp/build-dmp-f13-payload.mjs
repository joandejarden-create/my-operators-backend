import fs from "fs";

const code = fs.readFileSync("docs/_dmp_embed_inline.html", "utf8");
if (!/v20260801f13\.css/.test(code)) {
  throw new Error("embed missing f13 css");
}
const payload = {
  siteId: "68108c29063eeb5d1bd7ae4a",
  pageId: "68108c2a063eeb5d1bd7ae90",
  context:
    "Updates Manual Process embed CSS so Selected Direction question mark is hero yellow.",
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
  JSON.stringify({
    chars: code.length,
    hasF13: /f13\.css/.test(code),
    hasF12: /f12\.css/.test(code),
  })
);
