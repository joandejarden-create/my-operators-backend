const url = 'https://www.dealality.com/old-home';
const res = await fetch(url);
const html = await res.text();
const css = html.includes('dealality-old-home-dark.v20260728u.css');
const faq = html.includes('Find clear, straightforward answers');
const cssMatch = html.match(/dealality-old-home-dark\.v[^"']+\.css/);
console.log(JSON.stringify({
  url,
  status: res.status,
  css_v20260728u: css,
  faq_text: faq,
  found_css: cssMatch ? cssMatch[0] : null,
  pass: css && faq,
}, null, 2));
