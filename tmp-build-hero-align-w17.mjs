import fs from "fs";
import crypto from "crypto";
import path from "path";

const src = path.resolve(
  "public/marketing/dealality-old-home-freeform-head.v20260729w16.css"
);
const dest = path.resolve(
  "public/marketing/dealality-old-home-freeform-head.v20260729w17.css"
);

let css = fs.readFileSync(src, "utf8");

const oldBlock = `/* Left-aligned hero copy — inset from left edge, clear of globe */
#hero-inner{
  display:flex!important;
  flex-direction:column!important;
  align-items:flex-start!important;
  text-align:left!important;
  max-width:min(680px,58%)!important;
  width:100%!important;
  margin:0!important;
  margin-left:clamp(2.5rem,20vw,24rem)!important;
  margin-right:auto!important;
  grid-template-columns:none!important;
  grid-template-areas:none!important;
}`;

const newBlock = `/* Left-aligned hero copy — match nav/logo left gutter (laptop+) */
#hero,.oh-hero{
  box-sizing:border-box!important;
  padding-left:calc((100% - 1120px) / 2 + clamp(1.5rem,4vw,3rem))!important;
  padding-right:clamp(1.5rem,4vw,3rem)!important;
}
#hero-inner{
  display:flex!important;
  flex-direction:column!important;
  align-items:flex-start!important;
  text-align:left!important;
  max-width:min(640px,56%)!important;
  width:100%!important;
  margin:0!important;
  margin-left:0!important;
  margin-right:auto!important;
  padding-left:0!important;
  grid-template-columns:none!important;
  grid-template-areas:none!important;
}
@media(max-width:1180px){
  #hero,.oh-hero{
    padding-left:clamp(1.25rem,4vw,2.5rem)!important;
    padding-right:clamp(1.25rem,4vw,2.5rem)!important;
  }
}`;

if (!css.includes(oldBlock)) {
  console.error("OLD BLOCK NOT FOUND");
  process.exit(1);
}
css = css.replace(oldBlock, newBlock);

const oldMobile = `@media(max-width:960px){
  #hero-inner{max-width:100%!important;margin-left:0!important}
}`;
const newMobile = `@media(max-width:960px){
  #hero,.oh-hero{
    padding-left:clamp(1.25rem,4vw,1.5rem)!important;
    padding-right:clamp(1.25rem,4vw,1.5rem)!important;
  }
  #hero-inner{max-width:100%!important;margin-left:0!important}
}`;

if (!css.includes(oldMobile)) {
  console.error("MOBILE BLOCK NOT FOUND");
  process.exit(1);
}
css = css.replace(oldMobile, newMobile);

fs.writeFileSync(dest, css);
const buf = fs.readFileSync(dest);
const hash = crypto.createHash("md5").update(buf).digest("hex");
console.log(
  JSON.stringify(
    {
      dest,
      bytes: buf.length,
      hash,
      hasOldMargin: css.includes("20vw,24rem"),
      hasNewGutter: css.includes("padding-left:calc((100% - 1120px)"),
    },
    null,
    2
  )
);
