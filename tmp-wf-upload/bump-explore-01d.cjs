const fs = require("fs");
const path = "public/marketing/old-home-explore-cta.v20260801d.js";
let s = fs.readFileSync(path, "utf8");
s = s.replace(
  "unify Explore / Opportunity Review CTAs (v20260801c)",
  "unify Explore / Opportunity Review CTAs (v20260801d)\n * 01d: also runs on /who-its-for (role page)."
);
s = s.replace("__ohExploreCta >= 202608013", "__ohExploreCta >= 202608014");
s = s.replace("__ohExploreCta = 202608013", "__ohExploreCta = 202608014");
s = s.replace(/oh-explore-cta-01c/g, "oh-explore-cta-01d");
if (!s.includes('getElementById("oh-explore-cta-01c")')) {
  s = s.replace(
    'var oldB = document.getElementById("oh-explore-cta-01b");',
    'var oldC = document.getElementById("oh-explore-cta-01c");\n      if (oldC && oldC.parentNode) oldC.parentNode.removeChild(oldC);\n      var oldB = document.getElementById("oh-explore-cta-01b");'
  );
}
fs.writeFileSync(path, s);
console.log({
  stamp: /202608014/.test(s),
  style: /oh-explore-cta-01d/.test(s),
  cleans01c: /oh-explore-cta-01c/.test(s),
});
