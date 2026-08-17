
const fs = require('fs');
const head = fs.readFileSync('tmp-old-home-head-visual.html','utf8');
const footer = fs.readFileSync('tmp-old-home-footer-visual.html','utf8');
console.log(JSON.stringify({ headLen: head.length, footerLen: footer.length }));
