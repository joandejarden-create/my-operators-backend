import fs from "fs";

const d = fs.readFileSync("tmp-varko-home.html", "utf8");
const start = d.indexOf('id="section-questions"');
const end = d.indexOf("</section>", start + 100);
const section = d.slice(start, end + 10);
fs.writeFileSync("tmp-varko-faq-section.html", section);
console.log("section len", section.length);

// Find CSS link
const cssLinks = [...d.matchAll(/href="([^"]+\.css[^"]*)"/g)].map((m) => m[1]);
console.log("css", cssLinks.slice(0, 5));

// Extract accordion-related class snippets from section
const classes = [...new Set([...section.matchAll(/class="([^"]+)"/g)].flatMap((m) => m[1].split(/\s+/)))];
console.log("classes", classes.filter((c) => /faq|accord|question|dropdown|chevron|glow|sub-title|main-title/i.test(c)));
