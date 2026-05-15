const fs = require('fs');
const path = require('path');

const root = path.join(__dirname, '..');
const src = path.join(root, 'public', 'operator-explorer-gold-mock.html');
const outDir = path.join(root, 'public', 'css');
const outFile = path.join(outDir, 'brand-explorer-gold-surface.css');

const s = fs.readFileSync(src, 'utf8');
const i = s.indexOf('<style>');
const j = s.indexOf('</style>');
if (i < 0 || j < 0) throw new Error('style block not found');
const css = s.slice(i + 7, j);

const extra = `
#brandTabs.tabs-section {
  grid-template-columns: repeat(2, minmax(100px, 1fr));
}
@media (max-width: 900px) {
  #brandTabs.tabs-section { grid-template-columns: repeat(2, minmax(90px, 1fr)); }
}
.be-panel { margin-bottom: 12px; }
.be-card-grid { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 10px; }
.be-card-grid--2 { grid-template-columns: repeat(2, minmax(0, 1fr)); }
.be-field-card {
  background: var(--panel);
  border: 0.6px solid var(--border);
  border-radius: 10px;
  padding: 12px;
  min-height: 72px;
}
.be-field-card--wide { grid-column: 1 / -1; }
.be-field-label {
  font-size: 11px;
  text-transform: uppercase;
  letter-spacing: 0.05em;
  color: var(--muted);
  margin-bottom: 6px;
}
.be-field-value { font-size: 14px; color: #eaf2ff; line-height: 1.45; word-break: break-word; }
.be-field-value--muted { color: var(--muted); font-size: 13px; }
.be-subsection { margin-bottom: 18px; }
.be-subsection-title { font-size: 14px; color: #b8c5e8; margin: 0 0 8px 0; }
.be-tags { display: flex; flex-wrap: wrap; gap: 6px; }
.be-tag {
  font-size: 12px;
  padding: 4px 10px;
  border-radius: 999px;
  background: rgba(108, 114, 255, 0.15);
  border: 0.6px solid var(--border);
  color: #dbe6f8;
}
.be-prose { color: #dbe6f8; font-size: 13px; line-height: 1.55; }
.be-prose p { margin: 0 0 10px 0; }
.be-note { font-size: 13px; color: var(--muted); margin: 8px 0; }
.be-link { color: #8fc3ff; }
.be-bool { display: inline-block; padding: 2px 10px; border-radius: 6px; font-size: 12px; font-weight: 600; }
.be-bool--yes { background: rgba(20, 202, 116, 0.2); color: #14ca74; }
.be-bool--no { background: rgba(255, 90, 101, 0.15); color: #ff8a92; }
.be-mkt-summary-grid {
  display: grid;
  grid-template-columns: repeat(3, minmax(0, 1fr));
  gap: 10px;
  margin-bottom: 12px;
}
@media (max-width: 720px) {
  .be-mkt-summary-grid { grid-template-columns: 1fr; }
}
.be-loc-wrap { display: flex; flex-direction: column; gap: 8px; }
.be-loc-row {
  display: grid;
  grid-template-columns: 1fr 120px 48px;
  gap: 10px;
  align-items: center;
  font-size: 13px;
}
.be-loc-name { color: #eaf2ff; }
.be-loc-track {
  height: 8px;
  background: rgba(255, 255, 255, 0.08);
  border-radius: 4px;
  overflow: hidden;
}
.be-loc-fill { height: 100%; background: var(--accent--primary-1); border-radius: 4px; }
.be-loc-pct { text-align: right; color: var(--muted); font-variant-numeric: tabular-nums; }
.be-dist {
  border: 0.6px solid var(--border);
  border-radius: 10px;
  background: var(--panel);
  padding: 12px;
  margin: 10px 0;
}
.be-dist__toolbar { margin-bottom: 10px; display: flex; flex-wrap: wrap; gap: 8px; }
.be-dist-toggle {
  padding: 6px 12px;
  border-radius: 8px;
  border: 0.6px solid var(--border);
  background: rgba(8, 15, 37, 0.5);
  color: var(--muted);
  cursor: pointer;
  font-size: 12px;
}
.be-dist-toggle.active { border-color: var(--accent--primary-1); color: #fff; }
.be-dist-group { display: none; }
.be-dist-group.active { display: block; }
.be-dist-row { margin-bottom: 10px; }
.be-dist-row__head {
  display: flex;
  justify-content: space-between;
  font-size: 12px;
  color: var(--muted);
  margin-bottom: 4px;
}
.be-dist-row__track {
  height: 8px;
  background: rgba(255, 255, 255, 0.08);
  border-radius: 4px;
  overflow: hidden;
}
.be-dist-row__fill { height: 100%; background: var(--secondary--color-3); border-radius: 4px; }
.be-portdist {
  border: 0.6px solid var(--border);
  border-radius: 10px;
  padding: 12px;
  margin: 10px 0;
  background: var(--panel);
}
.be-portdist__hint { font-size: 12px; color: var(--muted); margin-bottom: 8px; }
.be-portdist__toggles { display: flex; gap: 8px; margin-bottom: 10px; flex-wrap: wrap; }
.be-portdist-toggle {
  padding: 6px 12px;
  border-radius: 8px;
  border: 0.6px solid var(--border);
  background: rgba(8, 15, 37, 0.5);
  color: var(--muted);
  cursor: pointer;
  font-size: 12px;
}
.be-portdist-toggle.active { border-color: var(--accent--primary-1); color: #fff; }
.be-portdist-panel { display: none; }
.be-portdist-panel.active { display: block; }
.brand-hero.be-combined-presentation-hero .meta-card .value { min-height: 0; }
.brand-hero.be-combined-presentation-hero .meta-card .value .meta-card__value-clamp {
  display: -webkit-box;
  -webkit-box-orient: vertical;
  -webkit-line-clamp: 3;
  overflow: hidden;
  word-break: break-word;
  font-size: 12px;
  font-weight: 600;
  line-height: 1.38;
}
.brand-hero.be-combined-presentation-hero .meta-card .value .meta-card__value-clamp--empty {
  display: block;
  -webkit-line-clamp: unset;
  overflow: visible;
}
`;

fs.mkdirSync(outDir, { recursive: true });
fs.writeFileSync(outFile, css + extra);
console.log('Wrote', outFile);
