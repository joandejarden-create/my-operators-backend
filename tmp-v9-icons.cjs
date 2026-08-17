const fs = require('fs');
const h = fs.readFileSync('public/marketing/dealality-landing-v9-standalone.html', 'utf8');

// Extract all inline SVG icons used as stage/audience icons
function extractSection(id) {
  const start = h.indexOf(`id="${id}"`);
  const next = h.indexOf('<section', start + 10);
  return h.slice(start, next > 0 ? next : start + 8000);
}

const how = extractSection('how');
const aud = extractSection('audiences');
const problem = extractSection('problem');

// Stage icons
const fbtIcos = [...how.matchAll(/<div class="fbt-ico">([\s\S]*?)<\/div>/g)].map((m) => m[1].trim().slice(0, 120));
console.log('stage icons count', fbtIcos.length);
fbtIcos.forEach((s, i) => console.log(i, s.replace(/\s+/g, ' ').slice(0, 100)));

const audIcos = [...aud.matchAll(/aud-ico[\s\S]*?<\/svg>/g)].map((m) => m[0].slice(0, 150));
console.log('\naudience icons', audIcos.length);

// Problem icons
const pico = [...problem.matchAll(/class="[^"]*icon[^"]*"[\s\S]{0,200}|<svg[\s\S]*?<\/svg>/g)].slice(0, 6);
console.log('\nproblem svg count', (problem.match(/<svg/g) || []).length);

// Screenshots in how
console.log('\nscreenshots', [...how.matchAll(/screenshots\/[^\"]+/g)].map((m) => m[0]));

// dg / bg-blob occurrences
console.log('\ndg count', (h.match(/class="dg"/g) || []).length);
console.log('bg-blob count', (h.match(/bg-blob/g) || []).length);

// wgrid CSS
const i = h.indexOf('.wgrid{');
console.log('\nwgrid', h.slice(i, i + 500));
