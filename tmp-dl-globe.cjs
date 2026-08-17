const fs = require("fs");
const path = require("path");
const out = path.join("public", "marketing", "assets");
fs.mkdirSync(out, { recursive: true });

const urls = [
  [
    "https://cdn.prod.website-files.com/6050d670ff268e86e669416f/606e04fa32f36e517e87bb93_globe-texture-3.jpg",
    "globe-texture-3.jpg",
  ],
  [
    "https://www.dropbox.com/s/tip24r93q7rkpbs/fs-globe-image-3.jpg?dl=1",
    "fs-globe-image-3.jpg",
  ],
  [
    "https://www.dropbox.com/s/h6jxtk7ufz7y19g/fs-globe-image-4.jpg?dl=1",
    "fs-globe-image-4.jpg",
  ],
];

(async () => {
  for (const [url, name] of urls) {
    try {
      const res = await fetch(url, { redirect: "follow" });
      if (!res.ok) throw new Error(String(res.status));
      const buf = Buffer.from(await res.arrayBuffer());
      fs.writeFileSync(path.join(out, name), buf);
      console.log("saved", name, buf.length);
    } catch (e) {
      console.log("fail", name, e.message);
    }
  }
})();
