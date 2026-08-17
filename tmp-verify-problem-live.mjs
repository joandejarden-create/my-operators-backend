import fs from "fs";

const html = await (await fetch("https://www.dealality.com/old-home", { cache: "no-store" })).text();
fs.writeFileSync("tmp-verify-problem-live.html", html);

const checks = {
  hasProblemInjector: html.includes("old-home-problem-v2.v20260729a.js"),
  hasW18CssDirect: html.includes("freeform-head.v20260729w18.css"),
  hasW15Css: html.includes("freeform-head.v20260729w15.css"),
  hasSiteAuth: html.includes("dealality-site-footer-auth.v20260729a.js"),
  hasMemberstack: html.includes("memberstack.js"),
  hasAbout: html.includes('id="about"'),
  hasOldDecisionPath: html.includes("OWNER DECISION PATH"),
  hasOldHeadline: html.includes("several possible futures"),
};

const css = await (
  await fetch(
    "https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a6a19b23c529c0bd6a9e33f_dealality-old-home-freeform-head.v20260729w18.css"
  )
).text();
checks.cssHard = css.includes("Hard to Compare");
checks.cssFrag = css.includes("#about-frag");
checks.cssGutter = css.includes("Left-aligned hero copy");

const js = await (
  await fetch(
    "https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a6a1d50cef0212b9997fc62_old-home-problem-v2.v20260729a.js"
  )
).text();
checks.jsProblem = js.includes("data-oh-problem-v2");
checks.jsW18Swap = js.includes("v20260729w18.css");

console.log(JSON.stringify(checks, null, 2));
