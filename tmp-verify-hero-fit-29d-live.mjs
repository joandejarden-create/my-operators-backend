const r = await fetch("https://www.dealality.com/old-home", {
  headers: { "cache-control": "no-cache" },
});
const h = await r.text();
console.log("status", r.status);
console.log(
  "has29d",
  h.includes("ohherofitboot29d") || h.includes("hero-fit-boot.v20260729d")
);
console.log(
  "has29c",
  h.includes("ohherofitboot29c") || h.includes("hero-fit-boot.v20260729c")
);
console.log(
  "has307",
  h.includes("ohglobepindim307") || h.includes("v202607307")
);
const m = [...h.matchAll(/ohherofitboot\w+|ohglobepindim\w+/g)].map((x) => x[0]);
console.log("ids", [...new Set(m)]);
