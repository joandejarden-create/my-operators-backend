const page = await (
  await fetch("https://mvp-deal-capture.webflow.io/old-home", { cache: "no-store" })
).text();
const scripts = [...page.matchAll(/src="(https:[^"]+)"/g)].map((m) => m[1]);
for (const u of scripts) {
  if (/boot|guard|testimonial|oh-/i.test(u) || u.includes("website-files")) {
    if (/boot|guard|testimonial/i.test(u)) console.log(u);
  }
}
// find who references 31ab
for (const u of scripts.filter((s) => s.includes("website-files"))) {
  const t = await (await fetch(u)).text();
  if (t.includes("testimonials.v20260731ab") || t.includes("6a6cbc6952ac452a38cd8d69")) {
    console.log("LOADER", u);
  }
}
