const https = require('https');
https.get('https://www.dealality.com/old-home', (res) => {
  let d = '';
  res.on('data', (c) => (d += c));
  res.on('end', () => {
    const idx = d.indexOf('id="footer-logo"');
    console.log('html logo idx', idx);
    if (idx >= 0) console.log(d.slice(idx, idx + 500));
    const re = /src="([^"]*dealality[^"]*wordmark[^"]*)"/gi;
    let m;
    const imgs = [];
    while ((m = re.exec(d))) imgs.push(m[1]);
    console.log('wordmark srcs', imgs);
  });
}).on('error', (e) => console.error(e));
