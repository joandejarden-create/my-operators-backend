const https = require('https');
https.get('https://www.dealality.com/old-home', (res) => {
  let d = '';
  res.on('data', (c) => (d += c));
  res.on('end', () => {
    const checks = [
      'ONE HOTEL.',
      'BETTER DECISIONS.',
      'MORE VALUE.',
      'Helping hotel owners discover the strategic paths that create the greatest value before they commit.',
      '>Platform<',
      'Opportunity Assessment',
      'Strategic Paths',
      'Brand Intelligence',
      'Operator Intelligence',
      'Proposal Comparison',
      '>Learn<',
      'The Dealality Method',
      '>Company<',
      'Start Your Assessment',
      'Products',
      'Resources',
      '>Links<',
    ];
    for (const c of checks) console.log(d.includes(c) ? 'OK' : 'MISS', c);
  });
}).on('error', (e) => console.error(e));
