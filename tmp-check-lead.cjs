fetch("https://www.dealality.com/old-home?v=" + Date.now())
  .then((r) => r.text())
  .then((t) => {
    const i = t.indexOf("cta-band-lead");
    console.log(t.slice(i, i + 450));
  });
