import fs from "fs";

const code =
  '<link rel="stylesheet" href="https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a6cdb367404e90afdfdb29a_old-home-manual-process.shell.v20260731a.css" />' +
  '<link rel="stylesheet" href="https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a6d209a3cdf91ccf55e43f0_old-home-manual-process.v20260731k.css" />' +
  '<div id="dealality-manual-process-host" data-dmp-state="loading" aria-busy="true">Loading...</div>' +
  '<script src="https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a6d20e1497cf9281d50f248_old-home-manual-process.boot.v20260731j.js" defer></script>';

const payload = {
  siteId: "68108c29063eeb5d1bd7ae4a",
  pageId: "68108c2a063eeb5d1bd7ae90",
  context:
    "Point Manual Process HtmlEmbed at winding dead-end CSS k and boot j CDN assets.",
  actions: [
    {
      label: "set_embed",
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

fs.writeFileSync("tmp/wf-set-dmp-embed-winding.json", JSON.stringify(payload));
console.log("ok", code.length);
