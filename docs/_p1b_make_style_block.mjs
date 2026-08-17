import fs from 'fs';

const css = fs.readFileSync('docs/old-home-problem-phase1b-visual-css.min.css', 'utf8');
const styleBlock = `<style id="oh-p1b">\n${css}\n</style>\n`;
fs.writeFileSync('docs/_p1b_style_block.html', styleBlock);

const testimonials = fs.readFileSync('docs/_p1b_existing_testimonials_head.txt', 'utf8');
// If testimonials file missing, caller provides via MCP merge only for style block size check
console.log(JSON.stringify({ styleBlockLen: styleBlock.length, cssLen: css.length }));
