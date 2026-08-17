import fs from "fs";

const code =
  "<link rel='stylesheet' href='https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a6cdb367404e90afdfdb29a_old-home-manual-process.shell.v20260731a.css' />" +
  "<link rel='stylesheet' href='https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a6d2b1b7b717c132c653412_old-home-manual-process.v20260731o.css' />" +
  "<div id='dealality-manual-process-host' data-dmp-state='loading' aria-busy='true'>Loading...</div>" +
  "<script src='https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a6d28e423ff2b11270441b0_old-home-manual-process.boot.v20260731m.js' defer></script>";

fs.writeFileSync("tmp/oh-manual-embed-code-o.txt", code);
console.log(code);
