const fs = require('fs');
const body = fs.readFileSync('tmp-landing-body.html', 'utf8');
// Close any unclosed main if present — original has <main> wrapping from hero
const embed = `<!-- Dealality Railway landing recreate -->\n${body}`;
fs.writeFileSync('tmp-landing-embed.html', embed);
console.log(embed.length);
