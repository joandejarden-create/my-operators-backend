fetch("https://www.dealality.com/old-home?v=" + Date.now())
  .then((r) => r.text())
  .then((t) => {
    const i = t.indexOf('id="cta-band-lead"');
    console.log("idx", i);
    console.log(t.slice(i, i + 500));
  });
