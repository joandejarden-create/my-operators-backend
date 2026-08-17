import fs from 'fs';

const htmlPath = 'tmp-old-home-live.html';
if (!fs.existsSync(htmlPath)) {
  const res = await fetch('https://www.dealality.com/old-home');
  fs.writeFileSync(htmlPath, await res.text());
}

const html = fs.readFileSync(htmlPath, 'utf8');
const checks = {
  css_v20260728s: /v20260728s\.css/.test(html),
  oh_article_reader: /oh-article-reader/.test(html),
};
console.log('HTML checks:', JSON.stringify(checks, null, 2));

const cssMatch = html.match(/href="([^"]*v20260728s\.css[^"]*)"/);
const cssUrl = cssMatch ? cssMatch[1] : null;
console.log('css_url:', cssUrl);

if (cssUrl) {
  const cssRes = await fetch(cssUrl);
  const css = await cssRes.text();
  const cssChecks = {
    status: cssRes.status,
    has_oh_article_reader: /#oh-article-reader/.test(css),
    has_oh_ar_dialog: /#oh-ar-dialog/.test(css),
  };
  console.log('CSS checks:', JSON.stringify(cssChecks, null, 2));
}
