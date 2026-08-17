import fs from 'fs';

const html = fs.readFileSync('public/marketing/old-home-problem-deal-desk.v1.html', 'utf8');
const css = fs.readFileSync('public/marketing/old-home-problem-deal-desk.v1.css', 'utf8');
const tt = fs.readFileSync('docs/_p1b_existing_testimonials_head.txt', 'utf8');
const head = `${tt.trim()}\n<style id="oh-deal-desk">\n${css}\n</style>\n`;
fs.writeFileSync('docs/_dd_head.html', head);
fs.writeFileSync('docs/_dd_embed_code.html', html);
console.log('html', html.length, 'css', css.length, 'head', head.length);
