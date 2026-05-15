import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

const file = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../public/js/brand-explorer-atelier-from-api.js");
let s = fs.readFileSync(file, "utf8");

const from = `explorerDetailCardMultiline('Patterns', incentives) +
        '</div></section>' +
        '</section>' +
        '<section class="oe-section">' +
        '<h2 class="oe-section-title">Opening &amp; Conversion Journey</h2>'`;

const to = `explorerDetailCardMultiline('Patterns', incentives) +
        '</motion></motion></section>' +
        '<section class="oe-section">' +
        '<h2 class="oe-section-title">Opening &amp; Conversion Journey</h2>'`.replace(
  `'</motion></motion></section>'`,
  `'</div></section>'`
);

if (s.includes(from)) {
  s = s.replace(from, to);
  fs.writeFileSync(file, s);
  console.log("removed duplicate </section>");
} else {
  console.log("not found");
  process.exit(1);
}
