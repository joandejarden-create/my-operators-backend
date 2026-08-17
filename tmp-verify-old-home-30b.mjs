const checks = [];
function ok(label, v) {
  checks.push([label, !!v]);
  console.log(label, !!v);
}

const css = await (
  await fetch(
    "https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a6a95d4c41ba2c194a43045_dealality-old-home-platform-features.v20260730b.css"
  )
).text();
const js = await (
  await fetch(
    "https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a6a9636eacb1f1245de7c65_dealality-old-home-platform-features.v20260730b.js"
  )
).text();
const boot = await (
  await fetch(
    "https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a6a96374666ea8d4ef66e27_old-home-boot-guard.v20260730b.js"
  )
).text();
const fouc = await (
  await fetch(
    "https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a6a9637238d26cf780475f3_old-home-fouc-gate.v20260729e.js"
  )
).text();
const how = await (
  await fetch(
    "https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a6a971d4a1e47751ddfdf54_old-home-how-we-do-it.v20260730b.js"
  )
).text();

ok("css kills row-span", css.includes("grid-row:auto!important"));
ok("css 3col", css.includes("repeat(3,minmax(0,1fr))"));
ok("js Opportunity Review", js.includes("Opportunity Review"));
ok("js Market Intelligence", js.includes("Market Intelligence"));
ok("js no And More", !js.includes("And More"));
ok("boot w22", boot.includes("w22.css"));
ok("boot pf30b", boot.includes("platform-features.v20260730b.css"));
ok("boot testimonials30a", boot.includes("testimonials.v20260730a.js"));
ok("boot globe307", boot.includes("hero-globe-bg.v202607307.js"));
ok("fouc w22", fouc.includes("w22.css"));
ok("fouc herofit29e", fouc.includes("hero-fit.v20260729e.css"));
ok("fouc pf30b", fouc.includes("platform-features.v20260730b.css"));
ok("how Define", how.includes("Define the Opportunity"));
ok("how Compare", how.includes("Compare What Matters"));
ok("how namespace", how.includes("dealality-process_"));
ok("how version", how.includes("202607302"));

const failed = checks.filter((c) => !c[1]);
if (failed.length) {
  console.error("FAILED", failed.map((f) => f[0]).join(", "));
  process.exit(1);
}
console.log("ALL_OK");
