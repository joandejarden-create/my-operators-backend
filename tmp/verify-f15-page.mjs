import https from 'https';
import http from 'http';

function get(url, redirects = 0) {
  return new Promise((resolve, reject) => {
    const lib = url.startsWith('https') ? https : http;
    const req = lib.get(url, { timeout: 25000 }, (res) => {
      if ([301, 302, 303, 307, 308].includes(res.statusCode) && res.headers.location) {
        if (redirects > 5) return reject(new Error('too many redirects'));
        const next = new URL(res.headers.location, url).toString();
        res.resume();
        return resolve(get(next, redirects + 1));
      }
      let body = '';
      res.on('data', (c) => (body += c));
      res.on('end', () => resolve({ status: res.statusCode, body }));
    });
    req.on('timeout', () => {
      req.destroy();
      reject(new Error('timeout ' + url));
    });
    req.on('error', reject);
  });
}

const page = await get('https://www.dealality.com/old-home?f15=1');
console.log(
  JSON.stringify(
    {
      status: page.status,
      hasF15: page.body.includes('v20260801f15.css'),
      hasLoader: page.body.includes('dmp-root'),
      hasSectionStatic: page.body.includes('id="dealality-manual-process"') || page.body.includes("id='dealality-manual-process'"),
      hasFile: page.body.includes('FILE:'),
      f15Href: (page.body.match(/https:\/\/cdn\.prod\.website-files\.com\/[^"'\\\s>]*v20260801f15\.css/) || [])[0] || null,
      bodyLen: page.body.length,
    },
    null,
    2
  )
);
