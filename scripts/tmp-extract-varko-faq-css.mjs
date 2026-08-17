import https from "https";
import fs from "fs";

const cssUrl =
  "https://cdn.prod.website-files.com/694368fec078b4de6ca4f1b6/css/varko-template.webflow.shared.f6d97ae3d.css";

https
  .get(cssUrl, (res) => {
    let d = "";
    res.on("data", (c) => (d += c));
    res.on("end", () => {
      fs.writeFileSync("tmp-varko.css", d);
      const selectors = [
        "section-sub-title",
        "main-title",
        "sub-title",
        "single-accordion",
        "accordion-header",
        "accordion-title",
        "accordion-arrow",
        "accordion-content",
        "main-divider-glowing",
        "glowing-round",
        "section-questions",
        "container-box",
      ];
      const out = [];
      for (const sel of selectors) {
        const re = new RegExp(`\\.${sel}[^{]*\\{[^}]*\\}`, "g");
        const matches = d.match(re) || [];
        out.push(`\n/* === ${sel} (${matches.length}) === */\n` + matches.slice(0, 12).join("\n"));
      }
      fs.writeFileSync("tmp-varko-faq-css.css", out.join("\n"));
      console.log("css len", d.length, "extracted", out.join("").length);
    });
  })
  .on("error", (e) => {
    console.error(e);
    process.exit(1);
  });
