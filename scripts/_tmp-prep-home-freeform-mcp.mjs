import fs from "fs";

const head = fs.readFileSync(
  "C:/Dev/deal-capture-proxy/data/_tmp-home-page-freeform-head.html",
  "utf8"
);
const footer = fs.readFileSync(
  "C:/Dev/deal-capture-proxy/data/_tmp-home-page-freeform-footer.html",
  "utf8"
);

const payload = {
  actions: [
    {
      label: "set_home_head",
      set_page_freeform_code: {
        page_id: "68108c2a063eeb5d1bd7ae90",
        location: "head",
        content: head,
      },
    },
    {
      label: "set_home_footer",
      set_page_freeform_code: {
        page_id: "68108c2a063eeb5d1bd7ae90",
        location: "footer",
        content: footer,
      },
    },
  ],
  context:
    "Updating Home freeform for footer top-align CSS and motion 01g path unlock.",
};

fs.writeFileSync(
  "C:/Dev/deal-capture-proxy/data/_mcp-set-home-freeform-motion-footer.json",
  JSON.stringify(payload)
);
console.log(
  JSON.stringify({
    headLen: head.length,
    footerLen: footer.length,
    hasAlign: head.includes("oh-footer-top-align"),
    hasMotionG: footer.includes("v20260801g.js"),
  })
);
