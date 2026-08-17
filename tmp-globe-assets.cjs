fetch("https://www.3dglobes-wf.com/")
  .then((r) => r.text())
  .then((t) => {
    const imgs = [...t.matchAll(/https?:[^"'\\\s>]+\.(?:jpg|jpeg|png|webp)/gi)].map((m) => m[0]);
    console.log("imgs", [...new Set(imgs)].slice(0, 50).join("\n"));
    const attrs = [...t.matchAll(/fs-3dglobe-[a-z-]+=(?:"[^"]*"|'[^']*')/gi)].slice(0, 40);
    console.log("attrs\n", attrs.join("\n"));
  });

fetch("https://www.3dglobes-wf.com/resources")
  .then((r) => r.text())
  .then((t) => {
    const imgs = [...t.matchAll(/https?:[^"'\\\s>]+\.(?:jpg|jpeg|png|webp)/gi)].map((m) => m[0]);
    console.log("\nresources imgs\n", [...new Set(imgs)].join("\n"));
    const hrefs = [...t.matchAll(/href="([^"]+)"/g)].map((m) => m[1]).filter((h) => /jpg|download|cdn|cloudinary|webflow/i.test(h));
    console.log("\nhrefs\n", [...new Set(hrefs)].slice(0, 40).join("\n"));
  });
