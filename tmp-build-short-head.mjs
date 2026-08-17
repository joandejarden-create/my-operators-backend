import fs from "fs";

const hosted =
  "https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a68f96d1f20a4a06d72162c_dealality-old-home-freeform.v20260728benefits.css";
const links = fs.readFileSync("tmp-old-home-head-links-only.txt", "utf8").trimEnd();
const head = `${links}\n<link rel="stylesheet" href="${hosted}">\n`;
fs.writeFileSync("tmp-old-home-head-short-set.txt", head);

const args = {
  actions: [
    {
      label: "set_old_home_head_benefits_css_link",
      set_page_freeform_code: {
        page_id: "68108c2a063eeb5d1bd7ae90",
        location: "head",
        content: head,
      },
    },
  ],
  context:
    "Sets Old Home head freeform to font/CDN links plus uploaded freeform CSS that includes DashDark Benefits modules styles.",
};
fs.writeFileSync("tmp-short-head-set-args.json", JSON.stringify(args));
fs.writeFileSync(
  "tmp-short-head-set-args.cjs",
  "module.exports=" + JSON.stringify(args) + ";\n"
);
console.log(
  JSON.stringify({
    len: head.length,
    hasBenefitsUrl: head.includes("v20260728benefits.css"),
    hasAgCdn: head.includes("v20260728ag.css"),
    head,
  })
);
