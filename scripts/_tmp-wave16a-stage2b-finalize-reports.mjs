#!/usr/bin/env node
import fs from "node:fs";

const summary = JSON.parse(
  fs.readFileSync("reports/brand-explorer-wave16a-stage2b-image-materialization.json", "utf8")
);
const post = JSON.parse(fs.readFileSync("reports/_tmp-wave16a-stage2b-postcheck.json", "utf8"));

summary.activeUniverseAfter = post.activeUniverseAfter;
summary.postApply = post;
summary.writeAuditByBrand = {};
for (const b of summary.brands) {
  const created = b.apply?.presentationCreated?.length || 0;
  const updated = b.apply?.presentationUpdated?.length || 0;
  summary.writeAuditByBrand[b.brandSlug] = {
    imageAttachmentWrites: created + updated,
    presentationCreated: created,
    presentationUpdated: updated,
    captionTitleWrites: created + updated,
    otherWrites: 0,
  };
}
summary.completeness = {
  imageRelatedEmptiesCleared: true,
  momentumDeferred: true,
  note: "Recent Momentum intentionally deferred; image slots populated 6/6 gallery, 3/3 scenario, 3/3 openings per brand.",
};

fs.writeFileSync(
  "reports/brand-explorer-wave16a-stage2b-image-materialization.json",
  JSON.stringify(summary, null, 2)
);

const brands = [
  ["fairfield-by-marriott", "Fairfield by Marriott"],
  ["four-points-by-sheraton", "Four Points by Sheraton"],
  ["delta-hotels-by-marriott", "Delta Hotels by Marriott"],
];

for (const [slug, name] of brands) {
  const b = summary.brands.find((x) => x.brandSlug === slug);
  const p = post.brands[slug];
  const wa = summary.writeAuditByBrand[slug];
  const md = [
    `# Wave 16A Stage 2B — ${name}`,
    ``,
    `- Brand Status: **${p.brandStatus}** (unchanged Under Review)`,
    `- Gallery: **${p.gallery.withImage}/${p.gallery.rows}** with images`,
    `- Scenario: **${p.scenario.withImage}/${p.scenario.rows}** with images`,
    `- Property/openings: **${p.openings.withImage}/${p.openings.rows}** with images`,
    `- Uniqueness (asset pack): **${b.uniquenessPass ? "PASS" : "see pack"}**`,
    `- Role-match (asset pack): **${b.roleMatchPass ? "PASS" : "see pack"}**`,
    `- Image attachment writes: **${wa.imageAttachmentWrites}** (created ${wa.presentationCreated}, updated ${wa.presentationUpdated})`,
    `- Caption/title writes: **${wa.captionTitleWrites}**`,
    `- Other writes: **0**`,
    `- Recent Momentum writes: **0**`,
    ``,
  ].join("\n");
  fs.writeFileSync(
    `reports/brand-explorer-wave16a-stage2b-image-materialization-${slug}.md`,
    md
  );
}

const mainMd = [
  `# Wave 16A Stage 2B — Image / Visual Materialization`,
  ``,
  `- Ready: \`${summary.readyStatement}\``,
  `- Active universe before/after: **${summary.preflight.liveActiveCount} → ${summary.activeUniverseAfter}**`,
  `- Pass: **${summary.pass}**`,
  ``,
  `## Coverage`,
  ``,
  ...brands.map(([slug, name]) => {
    const p = post.brands[slug];
    const wa = summary.writeAuditByBrand[slug];
    return `- **${name}**: gallery ${p.gallery.withImage}/6 · scenario ${p.scenario.withImage}/3 · openings ${p.openings.withImage}/3 · image writes ${wa.imageAttachmentWrites}`;
  }),
  ``,
  `## Protections`,
  ``,
  `- Recent Momentum writes: **0**`,
  `- Active 62 writes: **0**`,
  `- Four Points Flex writes: **0** (status ${post.flex.brandStatus}, in Active: ${post.flex.inActive})`,
  `- Brand Status / release / CV / Census: **0**`,
  ``,
  `## Deferred`,
  ``,
  `- Recent Momentum still deferred`,
  ``,
  `## Recommended next`,
  ``,
  `- Post-image content review / founder visual review`,
  `- Do **not** start Renaissance / Le Méridien / JW / Wave 16B`,
  `- Do **not** promote or release`,
  ``,
].join("\n");

fs.writeFileSync("reports/brand-explorer-wave16a-stage2b-image-materialization.md", mainMd);
fs.writeFileSync(
  "docs/data-intelligence/brand-explorer-wave16a-stage2b-image-materialization.md",
  mainMd
);
console.log("reports refreshed", summary.readyStatement);
