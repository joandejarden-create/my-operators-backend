import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

const p = path.resolve(
  path.dirname(fileURLToPath(import.meta.url)),
  "../public/js/brand-explorer-atelier-from-api.js"
);
let s = fs.readFileSync(p, "utf8");

s = s.replaceAll(
  '<motion class="scenario-card-grid scenario-card-grid--owner-value">',
  '<div class="scenario-card-grid scenario-card-grid--owner-value">'
);
s = s.replaceAll('<motion class="explorer-detail-stack">', '<div class="explorer-detail-stack">');

s = s.replace(
  `'</motion></motion>'.replace('<motion class="scenario-card-grid scenario-card-grid--owner-value">', '<div class="scenario-card-grid scenario-card-grid--owner-value">').replace('</motion></motion>', '</div>') +`,
  `'</div>' +`
);
s = s.replace(
  `'</motion></motion>'.replace('<motion class="explorer-detail-stack">', '<motion class="explorer-detail-stack">'.replace('<motion class="explorer-detail-stack">', '<div class="explorer-detail-stack">').replace('</motion></motion>', '</motion></motion>') +`,
  `'</div>' +`
);
s = s.replace(
  `'</motion></motion>'.replace('<motion class="explorer-detail-stack">', '<div class="explorer-detail-stack">').replace('</motion></motion>', '</motion></motion>') +`,
  `'</div>' +`
);
s = s.replace(
  `explorerDetailCardMultiline('Talking points', performanceExit) +
        '</motion></motion>'.replace('<motion class="explorer-detail-stack">', '<motion class="explorer-detail-stack">'.replace('<motion class="explorer-detail-stack">', '<motion class="explorer-detail-stack">')).replace('</motion></motion>', '</motion></motion>') +`,
  `explorerDetailCardMultiline('Talking points', performanceExit) +
        '</div></section>' +
        '<section class="oe-section" data-be-econ-fix="' +`
);
s = s.replace(
  `'</motion></motion>'.replace('<motion class="explorer-detail-stack">', '<motion class="explorer-detail-stack">'.replace('<motion class="explorer-detail-stack">', '<motion class="explorer-detail-stack">')).replace('</motion></motion>', '</motion></motion>') +`,
  `'</div></section>' +
        '<section class="oe-section" data-be-econ-fix="' +`
);
s = s.replace(
  `'</div></section>' +
        '<section class="oe-section" data-be-econ-fix="' +
        '</section>' +
        '<section class="oe-section">' +
        '<h2 class="oe-section-title">Legal`,
  `'</div></section>' +
        '<section class="oe-section">' +
        '<h2 class="oe-section-title">Legal`
);

fs.writeFileSync(p, s);
console.log("remaining motion:", (s.match(/<motion/g) || []).length);
