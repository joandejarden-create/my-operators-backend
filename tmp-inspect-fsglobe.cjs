fetch("https://cdn.jsdelivr.net/npm/@finsweet/3dglobes@1/FsGlobe.min.js")
  .then((r) => r.text())
  .then((t) => {
    console.log("len", t.length);
    console.log(t.slice(0, 500));
    const keys = [
      "fs-3dglobe-element",
      "fs-3dglobe-img",
      "container",
      "querySelector",
      "list",
    ];
    keys.forEach((k) => console.log(k, t.includes(k)));
  });
