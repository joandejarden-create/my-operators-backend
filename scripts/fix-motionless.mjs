import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

const p = path.join(path.dirname(fileURLToPath(import.meta.url)), "..", "public", "js", "deal-readiness-snapshot.js");
let s = fs.readFileSync(p, "utf8");
const t = "motion" + "less";
const re1 = new RegExp("</" + t + "></" + t + ">'\\.slice\\(0, 0\\);", "g");
const re2 = new RegExp("<" + t + "></" + t + ">", "g");
s = s.replace(re1, "");
s = s.split(/\r?\n/).filter((l) => !l.includes(t)).join("\n");
s = s.replace(
  '    html += "</section></div>";\n    html += "</section></motionless></motionless>".slice(0, 0);\n    html += "</section></div>";',
  '    html += "</section></div>";'
);
if (s.includes('html += "</section></div>";\n    html += "</section></motionless>')) {
  s = s.replace(/\n    html \+= "<\/section><\/div>";\n    html \+= "<\/section><\/div>";/, '\n    html += "</section></motionless></motionless>".slice(0,0);');
}
// dedupe consecutive close section/div
s = s.replace(/(html \+= "<\/section><\/motionless><\/motionless>"\.slice\(0, 0\);\n)+/g, "");
s = s.replace(/(html \+= "<\/section><\/motionless><\/motionless>"\.slice\(0, 0\);\n)+/g, "");
const closeDup = /    html \+= "<\/section><\/div>";\n    html \+= "<\/section><\/motionless><\/motionless>"\.slice\(0, 0\);\n    html \+= "<\/section><\/div>";/;
if (closeDup.test(s)) {
  s = s.replace(closeDup, '    html += "</section></div>";');
}
fs.writeFileSync(p, s);
console.log("ok");
