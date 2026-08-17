import fs from "fs";
const raw = fs.readFileSync("tmp-restore-site-footer.html", "utf8");
const needle = '</script><script>(function(){\nvar b=document.getElementById("nmenu")';
let cut = raw.indexOf(needle);
if (cut < 0) {
  cut = raw.indexOf("</script><script>(function(){");
}
if (cut < 0) throw new Error("cut not found");
const siteFooter = raw.slice(0, cut + "</script>".length);
const append = `

<!-- Old Home pricing refinements (path-scoped; page freeform 406 workaround) -->
<script>
(function () {
  var path = (location.pathname || "").replace(/\\/+$/, "").toLowerCase();
  if (path !== "/old-home") return;
  var css = document.createElement("link");
  css.rel = "stylesheet";
  css.href = "https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a6a17778e9c5c5804c67752_dealality-old-home-pricing.v20260729b.css";
  document.head.appendChild(css);
  function wirePricing() {
    var pricingOwnersCta = document.getElementById("pricing-owners-cta");
    if (pricingOwnersCta && !pricingOwnersCta.getAttribute("data-oh-pricing-wired")) {
      pricingOwnersCta.setAttribute("data-oh-pricing-wired", "1");
      pricingOwnersCta.addEventListener("click", function (e) {
        var href = pricingOwnersCta.getAttribute("href") || "https://www.dealality.com/opportunity-review";
        if (!/opportunity-review/i.test(href)) return;
        if (typeof window.ohOpenOpportunityReview !== "function") return;
        e.preventDefault();
        var label = (pricingOwnersCta.textContent || "Explore Your Opportunity").trim();
        window.ohOpenOpportunityReview(pricingOwnersCta.href || href, label);
      });
    }
    var pricing = document.getElementById("pricing");
    if (!pricing || typeof IntersectionObserver !== "function") return;
    if (pricing.getAttribute("data-oh-pricing-io") === "1") return;
    pricing.setAttribute("data-oh-pricing-io", "1");
    var io = new IntersectionObserver(function (entries) {
      var on = false;
      for (var i = 0; i < entries.length; i++) {
        if (entries[i].isIntersecting && entries[i].intersectionRatio > 0.12) {
          on = true;
          break;
        }
      }
      document.body.classList.toggle("oh-pricing-inview", on);
    }, { root: null, threshold: [0, 0.12, 0.25, 0.5], rootMargin: "-8% 0px -8% 0px" });
    io.observe(pricing);
  }
  if (document.readyState === "loading") document.addEventListener("DOMContentLoaded", wirePricing);
  else wirePricing();
})();
</script>
`;
const out = siteFooter + append;
fs.writeFileSync("tmp-restore-site-footer-final.html", out);
console.log("ok", siteFooter.length, out.length);
