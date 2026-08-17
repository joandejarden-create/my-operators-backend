const fs = require("fs");
const html = fs.readFileSync("tmp-old-home.html", "utf8");
const start = html.indexOf('<section id="pricing"');
const end = html.indexOf("</section>", start) + "</section>".length;
console.log({ start, end, len: end - start });
fs.writeFileSync("tmp-wf-upload/pricing-section.html", html.slice(start, end));
const fstart = html.indexOf('<footer id="footer-new"');
const fend = html.indexOf("</footer>", fstart) + "</footer>".length;
fs.writeFileSync("tmp-wf-upload/footer-section.html", html.slice(fstart, fend));
console.log("footer len", fend - fstart);
// find navbar
const nids = ["navbar", "nav", "oh-nav", "nav-bar"];
for (const id of nids) {
  const i = html.indexOf('id="' + id + '"');
  console.log(id, i);
}
