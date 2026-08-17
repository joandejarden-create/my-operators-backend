fetch("https://www.dealality.com/old-home?v=" + Date.now(), {
  headers: { "Cache-Control": "no-cache" },
})
  .then((r) => r.text())
  .then((t) => {
    const i = t.indexOf('id="cta-band-h2"');
    console.log("idx", i);
    console.log(t.slice(i, i + 320));
    console.log("Leaving", t.includes("Leaving on the Table?"));
    console.log("has br+accent", /How Much Value Are You[\s\S]{0,80}Leaving on the Table\?/.test(t));
  });
