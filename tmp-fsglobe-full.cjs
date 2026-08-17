fetch("https://cdn.jsdelivr.net/npm/@finsweet/3dglobes@1/FsGlobe.js")
  .then((r) => r.text())
  .then((t) => {
    console.log(t);
  });
