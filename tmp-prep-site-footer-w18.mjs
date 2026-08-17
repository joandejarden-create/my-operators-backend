import fs from "fs";

const SITE_ID = "68108c29063eeb5d1bd7ae4a";
const W18 =
  "https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a6a1ae7165816fbddab9228_dealality-old-home-freeform-head.v20260729w18.css";

const inject = `
<!-- Old Home hero-signals quiet comparison (path-scoped; page freeform 406 workaround) -->
<script>
(function () {
  var path = (location.pathname || "").replace(/\\/+$/, "").toLowerCase();
  if (path !== "/old-home") return;
  if (document.getElementById("oh-freeform-head-w18")) return;
  var css = document.createElement("link");
  css.id = "oh-freeform-head-w18";
  css.rel = "stylesheet";
  css.href = ${JSON.stringify(W18)};
  document.head.appendChild(css);
})();
</script>
`;

const footerPath = "tmp-site-footer-live.txt";
if (!fs.existsSync(footerPath)) {
  console.error("missing", footerPath, "- write footer from get first");
  process.exit(1);
}

let footer = fs.readFileSync(footerPath, "utf8");
if (footer.includes("oh-freeform-head-w18") || footer.includes("v20260729w18.css")) {
  console.log("already has w18 inject");
} else {
  footer = footer.replace(/\s*$/, "") + "\n" + inject.trim() + "\n";
  fs.writeFileSync(footerPath, footer);
}

const payload = {
  context:
    "Inject Old Home w18 quiet comparison CSS via site footer after page head 406.",
  actions: [
    {
      label: "set-site-footer-w18-inject",
      set_site_freeform_code: {
        site_id: SITE_ID,
        location: "footer",
        content: footer,
      },
    },
  ],
};
fs.writeFileSync("tmp-set-site-footer-w18.json", JSON.stringify(payload));
console.log(
  JSON.stringify(
    {
      footerChars: footer.length,
      hasW18: footer.includes("v20260729w18.css"),
      hasPricingWorkaround: footer.includes("page freeform 406 workaround"),
    },
    null,
    2
  )
);
