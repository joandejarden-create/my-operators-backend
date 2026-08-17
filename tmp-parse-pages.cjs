const fs = require('fs');
const t = fs.readFileSync(
  'C:/Users/joand/.cursor/projects/c-Users-joand-OneDrive-Documents-deal-capture-proxy/agent-tools/422f0363-9138-410b-b345-a0c1929f6de6.txt',
  'utf8'
);
for (const line of t.split(/\n/).slice(0, 5)) {
  try {
    const j = JSON.parse(line);
    const r = j.result || j;
    if (r.id || r.pages) {
      console.log(
        JSON.stringify(
          {
            label: j.label,
            id: r.id,
            title: r.title,
            slug: r.slug,
            path: r.publishedPath,
            seo: r.seo,
            pagesSample: (r.pages || [])
              .filter((p) => /home|old/i.test(`${p.title} ${p.slug || ''}`))
              .map((p) => ({ id: p.id, title: p.title, slug: p.slug, path: p.publishedPath })),
          },
          null,
          2
        )
      );
    }
  } catch (_) {
    /* ignore */
  }
}
