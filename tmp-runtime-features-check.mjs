import puppeteer from "puppeteer";

const browser = await puppeteer.launch({
  headless: "new",
  args: ["--no-sandbox"],
});
const page = await browser.newPage();
await page.setViewport({ width: 1440, height: 900 });
await page.goto("https://www.dealality.com/old-home?cb=" + Date.now(), {
  waitUntil: "networkidle2",
  timeout: 90000,
});
await new Promise((r) => setTimeout(r, 3500));

const info = await page.evaluate(() => {
  const needle = "See the opportunity before selecting the relationship";
  const bodyText = (document.body && document.body.innerText) || "";
  const features = document.getElementById("features");
  const how =
    document.getElementById("oh-how-we-do-it") ||
    document.getElementById("dealality-process") ||
    document.querySelector("[id*='how-we-do']");
  const h2s = [...document.querySelectorAll("h2")].map((h) => ({
    id: h.id,
    text: (h.textContent || "").trim().slice(0, 140),
    display: getComputedStyle(h).display,
    parentId: h.closest("section") && h.closest("section").id,
  }));
  return {
    hasText: bodyText.includes(needle),
    featuresExists: !!features,
    featuresDisplay: features ? getComputedStyle(features).display : null,
    featuresHeight: features
      ? Math.round(features.getBoundingClientRect().height)
      : null,
    howId: how && how.id,
    howDisplay: how ? getComputedStyle(how).display : null,
    matchingH2: h2s.filter((h) =>
      /opportunity|relationship|process|credible/i.test(h.text)
    ),
    allH2: h2s,
  };
});

console.log(JSON.stringify(info, null, 2));
await browser.close();
