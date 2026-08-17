import fs from "fs";

const code =
  "<link rel='stylesheet' href='https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a6cdb367404e90afdfdb29a_old-home-manual-process.shell.v20260731a.css' />" +
  "<link rel='stylesheet' href='https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a6d288ba5d086476da7fc53_old-home-manual-process.v20260731n.css' />" +
  "<div id='dealality-manual-process-host' data-dmp-state='loading' aria-busy='true'>Loading...</div>" +
  "<script src='https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a6d28e423ff2b11270441b0_old-home-manual-process.boot.v20260731m.js' defer></script>";

const args = {
  siteId: "68108c29063eeb5d1bd7ae4a",
  pageId: "68108c2a063eeb5d1bd7ae90",
  context:
    "Point Manual Process HtmlEmbed at CTA-restore CSS and boot with demo overlay.",
  actions: [
    {
      label: "embed",
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

fs.writeFileSync("tmp/oh-manual-embed-set.json", JSON.stringify(args));
fs.writeFileSync("tmp/oh-manual-embed-code-only.txt", code);
console.log(JSON.stringify({ chars: code.length, out: "tmp/oh-manual-embed-set.json" }));
