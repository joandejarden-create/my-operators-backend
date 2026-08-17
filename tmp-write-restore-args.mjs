import fs from "fs";
const content = fs.readFileSync("tmp-restore-site-footer-final.html", "utf8");
const args = {
  context: "Restore site footer after accidental overwrite and add Old Home pricing CSS/CTA wiring.",
  actions: [
    {
      label: "restore-site-footer",
      set_site_freeform_code: {
        site_id: "68108c29063eeb5d1bd7ae4a",
        location: "footer",
        content,
      },
    },
  ],
};
fs.writeFileSync("tmp-mcp-restore-site-footer-args.json", JSON.stringify(args));
console.log("args bytes", Buffer.byteLength(JSON.stringify(args)));
