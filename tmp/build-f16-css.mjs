import fs from "fs";

const src = "public/marketing/old-home-manual-process.v20260801f15.css";
const dst = "public/marketing/old-home-manual-process.v20260801f16.css";
let s = fs.readFileSync(src, "utf8");

const replacements = [
  [
    `#dealality-manual-process .dmp-card--opp {
  overflow: visible;
  /* Above connectors; path dots tuck under this face via underlap + cover strip */
  z-index: 4;
  isolation: isolate;
  height: 100%;
  align-self: stretch;
  padding: 8px 10px 6px;`,
    `#dealality-manual-process .dmp-card--opp {
  overflow: visible;
  /* Above connectors; path dots tuck under this face via underlap + cover strip */
  z-index: 4;
  isolation: isolate;
  height: 100%;
  align-self: stretch;
  justify-content: flex-start;
  /* Extra left/right/bottom inset so hotel + paths breathe inside the border */
  padding: 10px 16px 14px 16px;`,
  ],
  [
    `#dealality-manual-process .dmp-card-title {
  margin: 0 0 6px;`,
    `#dealality-manual-process .dmp-card-title {
  margin: 0 0 8px;`,
  ],
  [
    `/* Opportunity body — wider hotel image; path list tucked to the right */
#dealality-manual-process .dmp-opp-body {
  display: grid;
  grid-template-columns: minmax(0, 1.55fr) minmax(120px, 0.7fr);
  gap: 6px 6px;
  align-items: stretch;
  flex: 1 1 auto;
  min-height: 0;
  overflow: visible;
}`,
    `/* Opportunity body — hotel + paths with gutters; room for footer below */
#dealality-manual-process .dmp-opp-body {
  display: grid;
  grid-template-columns: minmax(0, 1.25fr) minmax(140px, 0.85fr);
  gap: 8px 14px;
  align-items: stretch;
  flex: 1 1 0;
  min-height: 0;
  overflow: visible;
}`,
  ],
  [
    `#dealality-manual-process .dmp-hotel-frame {
  position: relative;
  z-index: 2;
  overflow: hidden;
  border-radius: 10px;
  border: 1px solid rgba(255, 255, 255, 0.1);
  background: #06101f;
  min-height: 0;
  width: 100%;
  height: 100%;
  max-height: none;
  aspect-ratio: auto;
}

#dealality-manual-process .dmp-hotel-frame img {
  display: block;
  width: 100%;
  max-width: 100%;
  height: 100%;
  object-fit: cover;
  /* Building sits on the right of the 16:9 source — bias crop to upper facade */
  object-position: 85% 35%;
}`,
    `#dealality-manual-process .dmp-hotel-frame {
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
}`,
  ],
  [
    `#dealality-manual-process .dmp-paths {
  list-style: none;
  margin: 0;
  padding: 2px 0;
  display: flex;
  flex-direction: column;
  justify-content: center;
  gap: 2px;
  min-height: 0;
  height: auto;
  align-self: center;
  overflow: visible;
  position: relative;
  /* auto — do not trap icons/labels under the card ::after face cover */
  z-index: auto;
}`,
    `#dealality-manual-process .dmp-paths {
  list-style: none;
  margin: 0;
  padding: 0;
  display: flex;
  flex-direction: column;
  justify-content: space-between;
  gap: 0;
  min-height: 0;
  height: 100%;
  align-self: stretch;
  overflow: visible;
  position: relative;
  /* auto — do not trap icons/labels under the card ::after face cover */
  z-index: auto;
}`,
  ],
  [
    `#dealality-manual-process .dmp-path {
  position: relative;
  display: grid;
  grid-template-columns: 30px minmax(0, 1fr);
  align-items: center;
  justify-items: start;
  column-gap: 9px;
  flex: 0 0 auto;
  min-height: 0;
  /* More vertical room between path rows */
  padding: 9px 20px 9px 4px;
  /* Shift icon+label cluster toward the card's right edge (room for hotel image) */
  margin-left: 10px;
  margin-right: -22px;
}`,
    `#dealality-manual-process .dmp-path {
  position: relative;
  display: grid;
  grid-template-columns: 30px minmax(0, 1fr);
  align-items: center;
  justify-items: start;
  column-gap: 8px;
  flex: 1 1 0;
  min-height: 0;
  padding: 5px 30px 5px 2px;
  /* Icons/labels sit left of the right edge; dots still underlap at right:0 */
  margin-left: 0;
  margin-right: -8px;
}`,
  ],
  [
    `#dealality-manual-process .dmp-path + .dmp-path::before {
  /* Match FAQ / About hairline dividers (.oh-faq-div) */
  content: "";
  position: absolute;
  left: 0;
  right: 18px;
  top: 0;`,
    `#dealality-manual-process .dmp-path + .dmp-path::before {
  /* Match FAQ / About hairline dividers (.oh-faq-div) */
  content: "";
  position: absolute;
  left: 0;
  right: 26px;
  top: 0;`,
  ],
  [
    `#dealality-manual-process .dmp-opp-foot {
  margin: 5px 0 0;
  color: #fff;
  font-family: "Inter Tight", "Plus Jakarta Sans", system-ui, sans-serif;
  font-size: 13px;
  font-weight: 500;
  line-height: 18px;
  text-align: center;
}`,
    `#dealality-manual-process .dmp-opp-foot {
  margin: 14px 0 0;
  flex: 0 0 auto;
  position: relative;
  z-index: 3;
  color: #fff;
  font-family: "Inter Tight", "Plus Jakarta Sans", system-ui, sans-serif;
  font-size: 13px;
  font-weight: 500;
  line-height: 18px;
  text-align: center;
}`,
  ],
];

for (const [a, b] of replacements) {
  if (!s.includes(a)) {
    console.error("MISSING:\\n", a.slice(0, 140));
    process.exit(1);
  }
  s = s.replace(a, b);
}

s = s.replace(
  `#dealality-manual-process .dmp-opp-body {
    grid-template-columns: minmax(0, 1.05fr) minmax(128px, 0.95fr);
  }`,
  `#dealality-manual-process .dmp-opp-body {
    grid-template-columns: minmax(0, 1.05fr) minmax(132px, 1fr);
    gap: 8px 10px;
  }`
);

fs.writeFileSync(dst, s);
console.log("ok", dst, s.length);
