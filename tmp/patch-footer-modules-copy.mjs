import fs from "fs";

let c = fs.readFileSync("tmp/site-footer-freeform.html", "utf8");
const oldBlock = /<!-- ohModulesCopy30h:[\s\S]*?<\/script>/;
const neu = `<!-- ohModulesCopy30i: Benefits outcomes 3-line body lock -->
<script src="https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a6d3aea13c4dae89915e613_old-home-modules-copy.v20260730i.js" integrity="sha384-JRvUgIod7On2YE8IYpD0YtwC13j2aLNqk+8eqd97QbBYsakEntmhcFu7bTiH0Uce" crossorigin="anonymous"></script>`;

if (!oldBlock.test(c)) {
  console.error("block not found");
  process.exit(1);
}
c = c.replace(oldBlock, neu);
fs.writeFileSync("tmp/site-footer-freeform-updated.html", c);
console.log({
  hasI: c.includes("v20260730i"),
  hasH: c.includes("v20260730h"),
  len: c.length,
});
