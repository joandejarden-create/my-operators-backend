const https = require('https');
https.get('https://www.dealality.com/old-home', (res) => {
  let d = '';
  res.on('data', (c) => (d += c));
  res.on('end', () => {
    const checks = [
      'footer-logo',
      'footer-logo-img',
      'dealality-wordmark-nav',
      'max-width:1320px',
      'Footer full-width',
      '>Company<',
    ];
    for (const c of checks) console.log(d.includes(c) ? 'OK' : 'MISS', c);
    const i = d.indexOf('footer-logo');
    console.log('snippet', i >= 0 ? d.slice(i, i + 400) : 'none');
  });
}).on('error', (e) => console.error(e));
