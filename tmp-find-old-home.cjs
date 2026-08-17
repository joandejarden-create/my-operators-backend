const fs = require('fs');
const path = String.raw`C:\Users\joand\.cursor\projects\c-Users-joand-OneDrive-Documents-deal-capture-proxy\agent-tools\14d2034d-458c-41bb-a558-d1846636dcc9.txt`;
const j = JSON.parse(fs.readFileSync(path, 'utf8'));
const pages = j.result.pages;
const hits = pages.filter((p) =>
  /old|home|landing/i.test(`${p.title} ${p.slug} ${p.publishedPath || ''}`)
);
console.log(
  JSON.stringify(
    hits.map((p) => ({
      id: p.id,
      title: p.title,
      slug: p.slug,
      path: p.publishedPath,
    })),
    null,
    2
  )
);
console.log('total', pages.length);
console.log(
  'all titles',
  pages.map((p) => `${p.title} | ${p.slug} | ${p.id}`).join('\n')
);
