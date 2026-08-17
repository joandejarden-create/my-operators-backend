import fs from "fs";

const footerPath = "tmp-old-home-footer-current.html";
const current = fs.readFileSync(footerPath, "utf8");
const newReader = fs.readFileSync("tmp-oh-reader-script.html", "utf8");

// Replace the last <script>...</script> that contains oh-article-reader
const re = /<script>\(function\(\)\{\s*var root=document\.getElementById\("insights"\)[\s\S]*?<\/script>\s*$/;
if (!re.test(current)) {
  // try looser
  const idx = current.lastIndexOf('<script>(function(){\nvar root=document.getElementById("insights")');
  if (idx < 0) throw new Error("reader script not found");
  const end = current.indexOf("</script>", idx);
  if (end < 0) throw new Error("reader script end not found");
  const next = current.slice(0, idx) + newReader + current.slice(end + "</script>".length);
  fs.writeFileSync("tmp-old-home-footer-with-cta-reader.html", next);
  fs.writeFileSync(
    "tmp-old-home-footer-with-cta-reader.json",
    JSON.stringify({ content: next })
  );
  console.log("patched via index", next.length, "has ctaBtn", next.includes("cta-band-btn"));
} else {
  const next = current.replace(re, newReader);
  fs.writeFileSync("tmp-old-home-footer-with-cta-reader.html", next);
  fs.writeFileSync(
    "tmp-old-home-footer-with-cta-reader.json",
    JSON.stringify({ content: next })
  );
  console.log("patched via regex", next.length, "has ctaBtn", next.includes("cta-band-btn"));
}
