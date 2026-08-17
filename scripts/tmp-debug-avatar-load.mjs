import puppeteer from "puppeteer";

const browser = await puppeteer.launch({
  headless: "new",
  args: ["--no-sandbox", "--disable-dev-shm-usage"],
});
try {
  const page = await browser.newPage();
  const failed = [];
  page.on("requestfailed", (req) => {
    if (/\.(png|jpe?g|webp|gif|svg)/i.test(req.url()) || /founder-joan|testimonial-avatar/i.test(req.url())) {
      failed.push({ url: req.url(), err: req.failure()?.errorText || "fail" });
    }
  });
  await page.setViewport({ width: 1440, height: 900 });
  await page.goto(`https://www.dealality.com/old-home?cb=${Date.now()}`, {
    waitUntil: "networkidle2",
    timeout: 90000,
  });
  await new Promise((r) => setTimeout(r, 3000));
  const info = await page.evaluate(async () => {
    const root =
      document.getElementById("testimonials") || document.getElementById("trust");
    root?.scrollIntoView({ block: "center" });
    const imgs = [...document.querySelectorAll("#testimonials-viewport article img")];
    // Force eager decode
    await Promise.all(
      imgs.map(
        (img) =>
          new Promise((resolve) => {
            if (img.complete && img.naturalWidth > 0) return resolve();
            img.addEventListener("load", resolve, { once: true });
            img.addEventListener("error", resolve, { once: true });
            // kick reload
            const s = img.src;
            img.src = "";
            img.src = s;
          })
      )
    );
    return imgs.map((img) => {
      const s = getComputedStyle(img);
      return {
        src: img.currentSrc || img.src,
        complete: img.complete,
        naturalW: img.naturalWidth,
        naturalH: img.naturalHeight,
        display: s.display,
        visibility: s.visibility,
        opacity: s.opacity,
        w: s.width,
        h: s.height,
        objectFit: s.objectFit,
        objectPosition: s.objectPosition,
        overflow: s.overflow,
        filter: s.filter,
        mixBlend: s.mixBlendMode,
        contentVisibility: s.contentVisibility,
        parentOverflow: getComputedStyle(img.parentElement).overflow,
      };
    });
  });
  console.log(JSON.stringify({ failed, info }, null, 2));
} finally {
  await browser.close();
}
