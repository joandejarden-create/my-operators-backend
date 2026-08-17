import fs from 'fs';

const testimonials = fs.readFileSync('docs/_p1b_existing_testimonials_head.txt', 'utf8').trimEnd() + '\n';
const css = fs.readFileSync('docs/old-home-problem-phase1b-visual-css.min.css', 'utf8');
const p1b = `<style id="oh-p1b">\n${css}\n</style>\n`;
const content = testimonials + p1b;
fs.writeFileSync('docs/_p1b_merged_head.html', content);

const payload = {
  actions: [
    {
      label: 'set-old-home-head-p1b',
      set_page_freeform_code: {
        page_id: '68108c2a063eeb5d1bd7ae90',
        location: 'head',
        content,
      },
    },
  ],
  context:
    'Adds Phase 1B visual storyboard CSS to Old Home page head while preserving testimonials styles.',
};

fs.writeFileSync('docs/_p1b_set_head.json', JSON.stringify(payload));
console.log(JSON.stringify({ contentLen: content.length, payloadLen: Buffer.byteLength(JSON.stringify(payload)) }));
