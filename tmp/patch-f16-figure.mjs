import fs from "fs";
const p = "public/marketing/old-home-manual-process.v20260801f16.css";
let s = fs.readFileSync(p, "utf8");
const old = `#dealality-manual-process .dmp-hotel-frame {
  position: relative;
  z-index: 2;
  overflow: hidden;
  border-radius: 10px;
  border: 1px solid rgba(255, 255, 255, 0.1);
  background: #06101f;
  /* min-height:0 lets grid shrink below intrinsic image size (prevents footer overlap) */
  min-height: 0;
  min-width: 0;
  width: 100%;
  height: 100%;
  max-height: 100%;
  aspect-ratio: auto;
  align-self: stretch;
}

#dealality-manual-process .dmp-hotel-frame img {
  display: block;
  width: 100%;
  max-width: 100%;
  height: 100%;
  max-height: 100%;
  object-fit: cover;
  /* Keep facade in frame; avoid edge crop against card border */
  object-position: 70% 26%;
}`;
const neu = `#dealality-manual-process .dmp-hotel-frame {
  position: relative;
  z-index: 2;
  /* Kill UA figure margins — they were pushing the image into the footer */
  margin: 0;
  padding: 0;
  overflow: hidden;
  border-radius: 10px;
  border: 1px solid rgba(255, 255, 255, 0.1);
  background: #06101f;
  /* min-height:0 lets grid shrink below intrinsic image size */
  min-height: 0;
  min-width: 0;
  width: 100%;
  height: 100%;
  max-height: 100%;
  aspect-ratio: auto;
  align-self: stretch;
}

#dealality-manual-process .dmp-hotel-frame img {
  position: absolute;
  inset: 0;
  display: block;
  width: 100%;
  max-width: none;
  height: 100%;
  object-fit: cover;
  /* Keep facade in frame; avoid edge crop against card border */
  object-position: 70% 26%;
}`;
if (!s.includes(old)) { console.error('block missing'); process.exit(1); }
s = s.replace(old, neu);
fs.writeFileSync(p, s);
console.log('patched', s.length);
