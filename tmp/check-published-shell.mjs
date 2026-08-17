import https from 'https';

function get(url) {
  return new Promise((resolve, reject) => {
    https
      .get(url, { headers: { 'Cache-Control': 'no-cache', Pragma: 'no-cache' } }, (res) => {
        let d = '';
        res.on('data', (c) => (d += c));
        res.on('end', () => resolve({ status: res.statusCode, html: d }));
      })
      .on('error', reject);
  });
}

for (const url of [
  'https://mvp-deal-capture.webflow.io/old-home',
  'https://www.dealality.com/old-home',
]) {
  const { status, html } = await get(url);
  const shell = html.match(/old-home-manual-process\.shell\.v[0-9a-z]+\.css/);
  const version = html.match(/data-dmp-version="([^"]+)"/);
  const attr = html.match(/data-oh-manual-process="([^"]+)"/);
  console.log(
    JSON.stringify({
      url,
      status,
      shell: shell?.[0] ?? null,
      version: version?.[1] ?? null,
      attr: attr?.[1] ?? null,
      hasC: html.includes('shell.v20260731c.css'),
      hasB: html.includes('shell.v20260731b.css'),
      hasAssetC: html.includes('6a6d6f8824a07ced4109566b'),
    })
  );
}
