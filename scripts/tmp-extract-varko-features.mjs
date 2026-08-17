import fs from "fs";

const html = fs.readFileSync("tmp-varko-home.html", "utf8");
const start = html.indexOf('id="section-features"');
const end = html.indexOf('id="section-benefits"');
const chunk = html.slice(start, end);
fs.writeFileSync("tmp-varko-features-section.html", chunk);

const texts = [...chunk.matchAll(/>([^<]{3,160})</g)]
  .map((m) => m[1].trim())
  .filter(
    (t) =>
      !t.startsWith("http") &&
      !t.includes("svg") &&
      !t.includes("M1 ") &&
      !t.includes("stroke") &&
      !t.includes("viewBox")
  );

console.log("CHUNK LEN", chunk.length);
console.log(texts.join("\n"));
