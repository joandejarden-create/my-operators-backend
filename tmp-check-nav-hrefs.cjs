const fs = require('fs');
fetch('https://www.dealality.com/old-home?t=' + Date.now())
  .then((r) => r.text())
  .then((t) => {
    const labels = [
      'For Owners',
      'For Brands & Operators',
      'For Partners',
      'How it Works',
      'FAQs',
      'Insights',
    ];
    for (const label of labels) {
      const i = t.indexOf('>' + label + '<');
      if (i < 0) {
        console.log(label, 'NOT FOUND');
        continue;
      }
      const slice = t.slice(Math.max(0, i - 300), i);
      const matches = [...slice.matchAll(/href="([^"]+)"/g)];
      const href = matches.length ? matches[matches.length - 1][1] : 'no href';
      console.log(label, '=>', href);
    }
    for (const id of ['owners', 'brands', 'partners', 'how', 'faq', 'audiences']) {
      console.log('id', id, t.includes('id="' + id + '"'));
    }
  });
