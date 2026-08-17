const fs = require('fs');
const html = fs.readFileSync('tmp-landing-chunk-01.html', 'utf8');
// Escape for embedding in a PowerShell here-string later if needed
fs.writeFileSync('tmp-chunk-01-b64.txt', Buffer.from(html).toString('base64'));
console.log('b64', Buffer.from(html).toString('base64').length);
