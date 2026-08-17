import fs from 'fs';

const fullCss = fs.readFileSync('docs/old-home-problem-phase1b-visual-css.min.css', 'utf8');
// Strip media blocks first so base rules are not overwritten by breakpoint overrides
const withoutMedia = fullCss.replace(/@media[^{]+\{(?:[^{}]|\{[^{}]*\})*\}/g, '');
const rules = new Map();
for (const m of withoutMedia.matchAll(/(\.oh-p1b-[a-zA-Z0-9_-]+)\{([^}]*)\}/g)) {
  rules.set(m[1].slice(1), `${m[1]}{${m[2]}}`);
}
const mediaBlocks = fullCss.match(/@media[^{]+\{(?:[^{}]|\{[^{}]*\})*\}/g) || [];

function relevantMedia(names) {
  const set = new Set(names);
  return mediaBlocks
    .map((block) => {
      const head = block.match(/^@media[^{]+\{/)[0];
      const inner = [...block.matchAll(/(\.oh-p1b-[a-zA-Z0-9_-]+)\{([^}]*)\}/g)]
        .filter((m) => set.has(m[1].slice(1)))
        .map((m) => `${m[1]}{${m[2]}}`)
        .join('');
      return inner ? `${head}${inner}}` : '';
    })
    .filter(Boolean)
    .join('');
}

function cssForClasses(names) {
  return names.map((n) => rules.get(n)).filter(Boolean).join('') + relevantMedia(names);
}

const scenes = {
  1: [
    'oh-p1b-kicker','oh-p1b-scene','oh-p1b-title','oh-p1b-caption','oh-p1b-opp','oh-p1b-opp-card','oh-p1b-opp-top','oh-p1b-thumb','oh-p1b-hotel-mark','oh-p1b-hotel-win','oh-p1b-dot','oh-p1b-branch-lines','oh-p1b-branch-hub','oh-p1b-branch-fan','oh-p1b-path-ico','oh-p1b-ico-sq','oh-p1b-ico-op','oh-p1b-ico-bars','oh-p1b-ico-bar','oh-p1b-ico-bar-dim','oh-p1b-ico-spin','oh-p1b-ico-cap','oh-p1b-ico-ring','oh-p1b-opp-name','oh-p1b-opp-meta','oh-p1b-meta-grid','oh-p1b-meta-item','oh-p1b-meta-text','oh-p1b-branch','oh-p1b-paths','oh-p1b-path','oh-p1b-path-label',
  ],
  2: [
    'oh-p1b-kicker','oh-p1b-scene','oh-p1b-title','oh-p1b-caption','oh-p1b-flow','oh-p1b-participants','oh-p1b-person','oh-p1b-avatar','oh-p1b-av-head','oh-p1b-av-body','oh-p1b-person-name','oh-p1b-dup','oh-p1b-dup-chip',
  ],
  3: [
    'oh-p1b-kicker','oh-p1b-scene','oh-p1b-title','oh-p1b-lanes','oh-p1b-lane','oh-p1b-lane-h','oh-p1b-lane-label','oh-p1b-avatar-sm','oh-p1b-av-head-sm','oh-p1b-av-body-sm','oh-p1b-art-email','oh-p1b-art-email-bar','oh-p1b-art-email-line','oh-p1b-line-w70','oh-p1b-line-w80','oh-p1b-line-w65','oh-p1b-line-w55','oh-p1b-line-w45','oh-p1b-art-pdf','oh-p1b-art-pdf-page','oh-p1b-art-pdf-badge','oh-p1b-art-sheet','oh-p1b-grid','oh-p1b-cell','oh-p1b-cell-warn','oh-p1b-cell-empty','oh-p1b-art-deck','oh-p1b-deck-thumb','oh-p1b-deck-bar','oh-p1b-art-note','oh-p1b-art-call','oh-p1b-bubble','oh-p1b-art-attach','oh-p1b-art-label',
  ],
  4: [
    'oh-p1b-kicker','oh-p1b-scene','oh-p1b-title','oh-p1b-caption','oh-p1b-compare','oh-p1b-compare-head','oh-p1b-compare-row','oh-p1b-compare-head-cell','oh-p1b-row-label','oh-p1b-cell-ok','oh-p1b-cell-partial','oh-p1b-cell-miss','oh-p1b-cell-unclear','oh-p1b-cell-diff','oh-p1b-cell-blank',
  ],
  5: [
    'oh-p1b-kicker','oh-p1b-scene','oh-p1b-title','oh-p1b-caption','oh-p1b-tracks','oh-p1b-track','oh-p1b-track-advancing','oh-p1b-track-name','oh-p1b-rail','oh-p1b-rail-w82','oh-p1b-rail-w48','oh-p1b-rail-w28','oh-p1b-rail-w12','oh-p1b-track-status','oh-p1b-track-status-hot',
  ],
  6: [
    'oh-p1b-kicker','oh-p1b-outcome','oh-p1b-ghost','oh-p1b-ghost-lines','oh-p1b-ghost-hub','oh-p1b-ghost-fan','oh-p1b-outcome-inner','oh-p1b-outcome-title','oh-p1b-outcome-body',
  ],
};

const parent = {
  component: '68108c2a063eeb5d1bd7ae90',
  element: '1da347bd-f52c-dadf-da10-6906df4da740',
};

const actions = [];
for (const n of [1, 2, 3, 4, 5, 6]) {
  const html = fs.readFileSync(`docs/old-home-problem-phase1b-scene${n}.html`, 'utf8').trim();
  const css = cssForClasses(scenes[n]);
  fs.writeFileSync(`docs/_p1b_scene${n}_css.css`, css);
  console.log(`scene${n} css`, css.length, 'starts', css.slice(0, 80));
  actions.push({
    build_label: `p1b-fix-scene-${n}`,
    parent_element_id: parent,
    creation_position: 'append',
    html,
    css,
    return_element_info: true,
  });
}

fs.writeFileSync('docs/_p1b_fix_whtml_all.json', JSON.stringify(actions));
fs.writeFileSync('docs/_p1b_fix_whtml_A.json', JSON.stringify(actions.slice(0, 2)));
fs.writeFileSync('docs/_p1b_fix_whtml_B.json', JSON.stringify(actions.slice(2, 4)));
fs.writeFileSync('docs/_p1b_fix_whtml_C.json', JSON.stringify(actions.slice(4, 6)));
