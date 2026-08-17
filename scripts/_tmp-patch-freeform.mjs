import fs from "fs";

let head = fs.readFileSync(
  "C:/Dev/deal-capture-proxy/data/_tmp-freeform-head.html",
  "utf8"
);
head = head.replace(
  "if (path !== '/old-home') return;",
  "if (path !== '/' && path !== '/old-home') return;"
);
fs.writeFileSync(
  "C:/Dev/deal-capture-proxy/data/_tmp-freeform-head-cutover.html",
  head
);

let foot = fs.readFileSync(
  "C:/Dev/deal-capture-proxy/data/_tmp-freeform-footer.html",
  "utf8"
);
const reps = [
  [
    'https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a6d7f7556632564b9f5e1a7_dealality-old-home-nav-cleanup.v20260801a.js" integrity="sha384-QO2axCI10Rg0ZCV4HxK/PRrlhuPCZo4wzeysmISBk8eybtImNE2ttNQ2glSrPg18"',
    'https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a6e2be3291ad17ff6f4a6ef_dealality-old-home-nav-cleanup.v20260801b.js" integrity="sha256-f5g8p8T3n9YD0NAF6/M3kI3PknJl0dclvP5r5P8VMQk"',
  ],
  [
    'https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a6b2db186e4d37d0649debc_old-home-modules-icons.v20260730f.js" integrity="sha384-Utvmeie61pW5ZK1lcRcYkSvsaqFAmQ0Yvdgofj/vlUz16c4sC8R8XQr4UWugHizI"',
    'https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a6e2c86291ad17ff6f4df65_old-home-modules-icons.v20260801a.js" integrity="sha256-z1i/B/NBn26KuMM+jFnyHvKCi10ktUh615GUR0irpK0="',
  ],
  [
    'https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a6d3aea13c4dae89915e613_old-home-modules-copy.v20260730i.js" integrity="sha384-JRvUgIod7On2YE8IYpD0YtwC13j2aLNqk+8eqd97QbBYsakEntmhcFu7bTiH0Uce"',
    'https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a6e2c87183fbba14fc80533_old-home-modules-copy.v20260801a.js" integrity="sha256-gi159nPPhxnDst49q4jL5CI9LNDcfaGY/fs92uF5CIE="',
  ],
  [
    'https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a6cb357d9c170a4e9c5fa34_old-home-section-order.v20260731c.js" integrity="sha384-stWrebw7++2/AzKZIRG5SKB7/0jB/39t/2yzR7ptOZw0gVh5h6T1xSnWPZLjvQ6J"',
    'https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a6e2c86d2843e12337cb105_old-home-section-order.v20260801a.js" integrity="sha256-WGPigKraQtXfwJqgVjTD4LwTMCd5mEQ8PETTi7TKo68="',
  ],
  ["<!-- ohNavCleanup01a:", "<!-- ohNavCleanup01b:"],
  ["<!-- ohModulesIcons30f:", "<!-- ohModulesIcons01a:"],
  ["<!-- ohModulesCopy30i:", "<!-- ohModulesCopy01a:"],
  ["<!-- ohSectionOrder31c:", "<!-- ohSectionOrder01a:"],
];
for (const [a, b] of reps) {
  if (!foot.includes(a)) console.log("MISS", a.slice(0, 90));
  foot = foot.split(a).join(b);
}
fs.writeFileSync(
  "C:/Dev/deal-capture-proxy/data/_tmp-freeform-footer-cutover.html",
  foot
);
console.log("head", head.length, head.includes("path !== '/' && path !== '/old-home'"));
console.log(
  "foot",
  foot.length,
  foot.includes("nav-cleanup.v20260801b"),
  foot.includes("section-order.v20260801a")
);
