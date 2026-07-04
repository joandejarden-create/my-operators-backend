import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

const root = path.join(path.dirname(fileURLToPath(import.meta.url)), "..");
const head = fs.readFileSync(path.join(root, "public/js/deal-readiness-snapshot.js"), "utf8");
const headLines = head.split("\n");
const cleanHead = headLines.slice(0, 183).join("\n");

function strip(s) {
  return s
    .replace(/\s*'<' \+ 'motionless><\/motionless>'\.slice\(0,\s*0\)/g, "")
    .replace(/\s*'<motionless><\/motionless>'\.slice\(0,\s*0\)/g, "")
    .replace(/\s*"<' \+ 'motionless><\/motionless>"\.slice\(0,\s*0\)/g, "")
    .replace(/\s*"<motionless><\/motionless>"\.slice\(0,\s*0\)/g, "")
    .replace(/html \+= '<div class="drs-toolbar drs-no-print"><motionless><\/motionless>'\.slice\(0,\s*0\);\s*html \+= '<div class="drs-toolbar-actions">';/g, 'html += \'<motionless></motionless>\'.slice(0,0);\n    html += \'<div class="drs-toolbar drs-no-print"><div class="drs-toolbar-actions">\';')
    .replace(/html \+= '<motionless><\/motionless>'\.slice\(0,\s*0\);\s*html \+= '<div class="drs-toolbar drs-no-print"><div class="drs-toolbar-actions">';/g, 'html += \'<div class="drs-toolbar drs-no-print"><div class="drs-toolbar-actions">\';')
    .replace(/<motionless><\/motionless>/g, "");
}

const tail = strip(fs.readFileSync(path.join(root, "scripts/drs-tail.fragment.js"), "utf8"));
const build = strip(fs.readFileSync(path.join(root, "scripts/drs-build.fragment.js"), "utf8"));

const out = cleanHead + "\n" + tail + "\n" + build.replace(/^\s*function buildHtml/, "  function buildHtml");
const final = strip(out);
fs.writeFileSync(path.join(root, "public/js/deal-readiness-snapshot.js"), final);
const n = (final.match(/motionless/g) || []).length;
console.log("assembled", final.split("\n").length, "lines, motionless left:", n);
