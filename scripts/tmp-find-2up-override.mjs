/**
 * Find which stylesheet sets 2nd testimonial article to display:none.
 */
import puppeteer from "puppeteer";

const url = "https://www.dealality.com/old-home?cb=" + Date.now() + "#trust";
const browser = await puppeteer.launch({
  headless: "new",
  args: ["--no-sandbox", "--disable-setuid-sandbox"],
});
const page = await browser.newPage();
await page.setViewport({ width: 1440, height: 900 });
await page.goto(url, { waitUntil: "networkidle2", timeout: 90000 });
await page.waitForSelector('#testimonials-viewport > div[data-slide="0"] article', {
  timeout: 30000,
});

const info = await page.evaluate(() => {
  const root = document.getElementById("testimonials") || document.getElementById("trust");
  const art2 = root.querySelector(
    '#testimonials-viewport > div[data-slide="0"] > div > article:nth-child(2)'
  );
  const grid = root.querySelector('#testimonials-viewport > div[data-slide="0"] > div');
  const matches = [];
  for (const sheet of document.styleSheets) {
    let href = sheet.href || (sheet.ownerNode && sheet.ownerNode.id) || "inline";
    let rules;
    try {
      rules = sheet.cssRules;
    } catch (e) {
      matches.push({ href, error: String(e.message || e) });
      continue;
    }
    for (const rule of rules) {
      const text = rule.cssText || "";
      if (
        /testimonials-viewport[\s\S]*nth-child\(n\+2\)|nth-child\(n\+2\)[\s\S]*display:\s*none/i.test(
          text
        ) ||
        (/testimonials-viewport/.test(text) &&
          /nth-child\(n\+2\)/.test(text) &&
          /display:\s*none/i.test(text))
      ) {
        matches.push({ href, text: text.slice(0, 500) });
      }
      if (
        rule.type === CSSRule.MEDIA_RULE ||
        rule.cssRules
      ) {
        try {
          for (const inner of rule.cssRules || []) {
            const it = inner.cssText || "";
            if (
              /testimonials-viewport/.test(it) &&
              /nth-child\(n\+2\)/.test(it) &&
              /display:\s*none/i.test(it)
            ) {
              matches.push({ href, media: rule.conditionText || "", text: it.slice(0, 500) });
            }
          }
        } catch (_) {}
      }
    }
  }

  // Also dump ALL matching rules for testimonials grid + nth-child regardless of display
  const allTt = [];
  for (const sheet of document.styleSheets) {
    const href = sheet.href || (sheet.ownerNode && (sheet.ownerNode.id || sheet.ownerNode.tagName)) || "inline";
    let rules;
    try {
      rules = [...(sheet.cssRules || [])];
    } catch {
      continue;
    }
    const walk = (list, media) => {
      for (const rule of list) {
        if (rule.cssRules) {
          walk([...rule.cssRules], rule.conditionText || media);
          continue;
        }
        const text = rule.cssText || "";
        if (/#testimonials-viewport/.test(text) && /(nth-child|grid-template-columns|max-width:48rem)/.test(text)) {
          allTt.push({ href, media: media || null, text: text.slice(0, 400) });
        }
      }
    };
    walk(rules, null);
  }

  return {
    art2Display: art2 ? getComputedStyle(art2).display : null,
    gridCols: grid ? getComputedStyle(grid).gridTemplateColumns : null,
    gridWidth: grid ? Math.round(grid.getBoundingClientRect().width) : null,
    hideMatches: matches,
    relatedRules: allTt.slice(0, 80),
    relatedCount: allTt.length,
  };
});

console.log(JSON.stringify(info, null, 2));
await browser.close();
