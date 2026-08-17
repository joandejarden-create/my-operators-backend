/**
 * Build immutable HtmlEmbed payload for Old Home Manual Process v1.1.
 * Usage: node scripts/build-manual-process-webflow-embed.mjs
 */
import fs from "fs";
import path from "path";

const CSS =
  "https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a6cd9146403b81b28b3a9bf_old-home-manual-process.v20260731a.css";
const JS =
  "https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a6cd9140230e8c779bd70c9_old-home-manual-process.v20260731a.js";

const root = process.cwd();
const html = fs.readFileSync(
  path.join(root, "public/marketing/old-home-manual-process.v1.html"),
  "utf8"
);

const embed = `<!-- Old Home Manual Process v1.1 — immutable CDN assets; do not overwrite -->
<link rel="preconnect" href="https://fonts.googleapis.com" />
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin />
<link href="https://fonts.googleapis.com/css2?family=Fraunces:opsz,wght@9..144,500;9..144,600&family=Inter+Tight:wght@400;500;600;700&display=swap" rel="stylesheet" />
<link rel="stylesheet" href="${CSS}" />
<style id="dmp-shell-neutralize">
/* Scoped shell neutralize so #about padding does not inflate approved heights */
#about[data-oh-problem="manual-process"]{
  padding-top:0!important;
  padding-bottom:0!important;
}
#about[data-oh-problem="manual-process"] .oh-problem-shell{
  max-width:none!important;
  width:100%!important;
  margin:0!important;
  padding:0!important;
}
#about[data-oh-problem="manual-process"] .oh-problem-stage{
  margin:0!important;
  padding:0!important;
  min-height:0!important;
}
</style>
${html.trim()}
<script src="${JS}" defer></script>
`;

const outPath = path.join(root, "tmp/oh-manual-process-embed.v20260731a.html");
fs.mkdirSync(path.dirname(outPath), { recursive: true });
fs.writeFileSync(outPath, embed, "utf8");

const meta = {
  cssUrl: CSS,
  jsUrl: JS,
  chars: embed.length,
  outPath,
};
fs.writeFileSync(
  path.join(root, "tmp/oh-manual-process-cdn-meta.json"),
  JSON.stringify(meta, null, 2)
);
console.log(JSON.stringify(meta));
