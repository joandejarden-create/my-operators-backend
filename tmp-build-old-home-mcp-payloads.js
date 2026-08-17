const fs = require('fs');

const footer = JSON.parse(
  fs.readFileSync('tmp-old-home-footer-with-cta-reader.json', 'utf8')
).content;

const OLD_CSS =
  'https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a68c0c23946c27237e50206_dealality-old-home-dark.v20260728af.css';
const NEW_CSS =
  'https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a68c28696192b91c48d1768_dealality-old-home-dark.v20260728ag.css';

const APPEND_CSS = [
  '#insights-grid,.oh-insights-grid{display:flex!important;flex-wrap:nowrap!important;overflow-x:auto!important;width:100%!important;max-width:100%!important}',
  '#ins-1,#ins-2,#ins-3,#ins-4,#ins-5,#ins-6,.oh-ins-card{flex:0 0 calc((100% - (var(--ins-visible,3) - 1) * var(--ins-gap,2rem)) / var(--ins-visible,3))!important;width:calc((100% - (var(--ins-visible,3) - 1) * var(--ins-gap,2rem)) / var(--ins-visible,3))!important;min-width:calc((100% - (var(--ins-visible,3) - 1) * var(--ins-gap,2rem)) / var(--ins-visible,3))!important;max-width:none!important}',
  '#insights-prev.is-disabled,#insights-next.is-disabled,#insights-prev[aria-disabled="true"],#insights-next[aria-disabled="true"]{pointer-events:auto!important}',
].join('\n');

const head = fs.readFileSync('tmp-old-home-head-from-mcp.txt', 'utf8');
let updated = head.replace(OLD_CSS, NEW_CSS);
if (!updated.includes('v20260728ag')) {
  updated = updated.replace(
    /dealality-old-home-dark\.v[^."]+\.css/g,
    'dealality-old-home-dark.v20260728ag.css'
  );
  updated = updated.replace(
    /https:\/\/cdn\.prod\.website-files\.com\/68108c29063eeb5d1bd7ae4a\/[^"]+_dealality-old-home-dark\.v20260728ag\.css/,
    NEW_CSS
  );
}

if (!updated.includes('#insights-grid,.oh-insights-grid{display:flex!important')) {
  if (!updated.includes('</style>')) {
    throw new Error('head missing </style>');
  }
  updated = updated.replace('</style>', APPEND_CSS + '\n</style>');
}

fs.writeFileSync('tmp-old-home-head-updated.txt', updated);
fs.writeFileSync(
  'tmp-mcp-set-footer.json',
  JSON.stringify({
    page_id: '68108c2a063eeb5d1bd7ae90',
    location: 'footer',
    content: footer,
  })
);
fs.writeFileSync(
  'tmp-mcp-set-head.json',
  JSON.stringify({
    page_id: '68108c2a063eeb5d1bd7ae90',
    location: 'head',
    content: updated,
  })
);

console.log(
  JSON.stringify({
    footerLen: footer.length,
    footerScrollMax: footer.includes('scrollMax'),
    footerCta: footer.includes('cta-band-btn'),
    headLen: updated.length,
    headCss: updated.includes('v20260728ag'),
    headInsights: updated.includes('#insights-grid,.oh-insights-grid{display:flex!important'),
    headDisabledPtr: updated.includes(
      '#insights-prev.is-disabled,#insights-next.is-disabled'
    ),
  })
);
