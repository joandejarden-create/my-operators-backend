import fs from "fs";

const w17 = fs.readFileSync(
  "public/marketing/dealality-old-home-freeform-head.v20260729w17.css",
  "utf8"
);
const w16 = fs.readFileSync(
  "public/marketing/dealality-old-home-freeform-head.v20260729w16.css",
  "utf8"
);

const problemStart17 = w17.indexOf("/* Problem (#about)");
const testimonials17 = w17.indexOf("/* Testimonials — Varko Clients Feedback carousel */");
const problemStart16 = w16.indexOf("/* Problem (#about)");
const testimonials16 = w16.indexOf("/* Testimonials — Varko Clients Feedback carousel */");

if (problemStart17 < 0 || testimonials17 < 0 || problemStart16 < 0 || testimonials16 < 0) {
  console.error("markers missing", {
    problemStart17,
    testimonials17,
    problemStart16,
    testimonials16,
  });
  process.exit(1);
}

const problemBlock = w16.slice(problemStart16, testimonials16);
if (!problemBlock.includes("Hard to Compare") || !problemBlock.includes("#about-frag")) {
  console.error("w16 problem block incomplete");
  process.exit(1);
}

const w18 =
  w17.slice(0, problemStart17) + problemBlock + w17.slice(testimonials17);
const dest = "public/marketing/dealality-old-home-freeform-head.v20260729w18.css";
fs.writeFileSync(dest, w18);
console.log("wrote", dest, "bytes", Buffer.byteLength(w18));
console.log("Hard", w18.includes("Hard to Compare"));
console.log("frag", w18.includes("#about-frag"));
console.log("hero gutter", w18.includes("Left-aligned hero copy"));
console.log("Difficult", w18.includes("Difficult to Compare"));
