import fs from 'fs';

const css = fs.readFileSync('docs/old-home-problem-phase1b-visual-css.min.css', 'utf8');
const classNames = [
  ...new Set((css.match(/\.oh-p1b-[a-zA-Z0-9_-]+/g) || []).map((s) => s.slice(1))),
];
const registerHtml = `<div class="oh-p1b-register" aria-hidden="true" data-oh-p1b="register">${classNames
  .map((c) => `<div class="${c}"></div>`)
  .join('')}</div>`;

const chunks = [];
let cur = '';
for (const rule of css.match(/@[^{]+\{(?:[^{}]|\{[^{}]*\})*\}|\.oh-p1b-[^{]+\{[^}]*\}/g) || []) {
  if ((cur + rule).length > 6500) {
    chunks.push(cur);
    cur = rule;
  } else {
    cur += rule;
  }
}
if (cur) chunks.push(cur);

const parent = {
  component: '68108c2a063eeb5d1bd7ae90',
  element: '1da347bd-f52c-dadf-da10-6906df4da740',
};

const registerActions = chunks.map((chunkCss, i) => ({
  build_label: `p1b-register-styles-${i + 1}`,
  parent_element_id: parent,
  creation_position: 'append',
  html: registerHtml,
  css: chunkCss,
  return_element_info: true,
}));

fs.writeFileSync('docs/_p1b_register_html.html', registerHtml);
fs.writeFileSync('docs/_p1b_register_actions.json', JSON.stringify(registerActions));
chunks.forEach((c, i) => fs.writeFileSync(`docs/_p1b_css_chunk${i + 1}.css`, c));
console.log('classes', classNames.length, 'html_len', registerHtml.length);
chunks.forEach((c, i) => console.log('chunk', i + 1, c.length));
console.log(classNames.slice(0, 8).join(','));
