import fs from "fs";

const hosted =
  "https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a68f96d1f20a4a06d72162c_dealality-old-home-freeform.v20260728benefits.css";
const links = fs.readFileSync("tmp-old-home-head-links-only.txt", "utf8").trimEnd();
const benefits = fs
  .readFileSync("tmp-benefits-css-only.txt", "utf8")
  .replace(/\r\n/g, "\n")
  .trimEnd();
const head =
  links +
  `\n<link rel="stylesheet" href="${hosted}">\n<style>\n` +
  benefits +
  `\n</style>\n`;

fs.writeFileSync("tmp-old-home-head-short-set.txt", head);
fs.writeFileSync("tmp-old-home-head-patched.txt", head);

const args = {
  actions: [
    {
      label: "set_old_home_head_benefits_inline_and_link",
      set_page_freeform_code: {
        page_id: "68108c2a063eeb5d1bd7ae90",
        location: "head",
        content: head,
      },
    },
  ],
  context:
    "Sets Old Home head with freeform CSS link plus inline DashDark Benefits modules styles for verification.",
};
fs.writeFileSync("tmp-short-head-set-args.json", JSON.stringify(args));
console.log(
  JSON.stringify({
    len: head.length,
    benefits: head.includes("DashDark Benefits / modules"),
    badge: head.includes("modules-badge-left"),
    hosted: head.includes("v20260728benefits.css"),
  })
);
console.log("---HEAD_START---");
process.stdout.write(head);
console.log("---HEAD_END---");
