import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

const p = path.join(path.dirname(fileURLToPath(import.meta.url)), "..", "public", "js", "deal-readiness-snapshot.js");
let s = fs.readFileSync(p, "utf8");

s = s.replace(
  /html \+= '<div class="drs-cover-geometric" aria-hidden="true"><\/motionless><\/motionless>'\.slice\(0, 0\) \+ "<\/motionless><\/motionless>"\.slice\(0, 0\) \+ "<\/div>";/,
  'html += \'<motionless></motionless>\'.slice(0,0);\n    html += \'<motionless></motionless>\'.slice(0,0);'
);
s = s.replace(
  /html \+= '<div class="drs-cover-geometric" aria-hidden="true"><\/motionless><\/motionless>'\.slice\(0, 0\) \+ "<\/div>";/,
  'html += \'<div class="drs-cover-geometric" aria-hidden="true"></div>\';'
);
s = s.replace(/<motionless><\/motionless>/g, "");
s = s.replace(/['"]<\/motionless><\/motionless>['"]\.slice\(0,\s*0\)/g, "");
s = s.replace(/html \+= "";\s*html \+= '<div class="drs-cover-geometric"/, "html += '<div class=\"drs-cover-geometric\"");

// Remove duplicate narrative header
s = s.replace(
  /    var workflowRows = buildWorkflowRows\(data, stage, score\);\s*var subtitleParts = \[[\s\S]*?html \+= "<\/dl><\/header>";\s*\n/,
  "    var workflowRows = buildWorkflowRows(data, stage, score);\n\n"
);

// Fix buildHtml inner pages
s = s.replace(
  /html \+= '<article class="drs-document">';\s*html \+= '<p class="drs-page-label drs-page-label--first drs-no-print">Page 1 — Readiness Narrative<\/p>';/,
  `html += '<article class="drs-document">';
    html += renderCoverPage(data, options, ctx);
    html += '<div class="drs-inner-pages">';
    html += '<p class="drs-page-label drs-no-print">Page 1 — Readiness Narrative</p>';`
);

s = s.replace(
  /html \+= renderPage2Technical\(data, options, ctx\);\s*if \(options\.footerHtml\)/,
  `html += renderPage2Technical(data, options, ctx);
    html += "</div>";
    if (options.footerHtml)`
);

// Remove erroneous motionless close if any
s = s.replace(/html \+= "<\/div>";\s*html \+= "<\/div>";\s*if \(options\.footerHtml\)/, 
  'html += "</div>";\n    if (options.footerHtml)');

fs.writeFileSync(p, s);
console.log("fixed", (s.match(/motionless/g) || []).length, "motionless left");
