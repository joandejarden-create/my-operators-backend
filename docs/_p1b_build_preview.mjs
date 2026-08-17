import fs from 'fs';

const scenes = [1, 2, 3, 4, 5, 6]
  .map((n) =>
    fs.readFileSync(`docs/old-home-problem-phase1b-scene${n}.html`, 'utf8').replace(/\r\n/g, '\n').trim()
  )
  .join('\n\n');

const css = fs.readFileSync('docs/old-home-problem-phase1b-visual-css.css', 'utf8');

const html = `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8" />
<meta name="viewport" content="width=device-width, initial-scale=1" />
<title>Phase 1B Problem composition preview</title>
<style>
html,body{margin:0;background:#070b1a;color:#fff;font-family:Inter,Segoe UI,sans-serif}
.wrap{max-width:1120px;margin:0 auto;padding:2rem 1.25rem 3rem}
.note{color:rgba(183,196,255,.65);font-size:.85rem;margin:0 0 1.25rem;line-height:1.45}
${css}
</style>
</head>
<body>
  <div class="wrap">
    <p class="note">Local static preview of Phase 1B inserted markup (Designer draft; not published). Viewport screenshots for composition review.</p>
${scenes}
  </div>
</body>
</html>
`;

fs.writeFileSync('docs/old-home-problem-phase1b-preview.html', html);
console.log('wrote preview', html.length);
