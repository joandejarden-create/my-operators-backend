import fs from "fs";

const html = fs
  .readFileSync("public/marketing/old-home-manual-process.v20260801f.html", "utf8")
  .trim();
const css =
  "https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a6d582db282df68e6b87f91_old-home-manual-process.v20260801f7.css";
const shell =
  "https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a6cdb367404e90afdfdb29a_old-home-manual-process.shell.v20260731a.css";
const boot = process.argv[2] || "__BOOT_URL__";

const embed =
  `<link rel='stylesheet' href='${shell}' />` +
  `<link rel='stylesheet' href='${css}' />` +
  html +
  `<script src='${boot}' defer></script>`;

fs.writeFileSync("docs/_dmp_embed_inline.html", embed);
console.log(
  JSON.stringify({
    chars: embed.length,
    hasLoading: /Loading\.\.\./.test(embed),
    hasHost: /dealality-manual-process-host/.test(embed),
    version128: /1\.1\.28/.test(embed),
    boot,
  })
);
