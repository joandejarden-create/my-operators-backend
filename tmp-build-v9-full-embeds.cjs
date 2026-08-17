const fs = require('fs');

const STANDALONE = 'public/marketing/dealality-landing-v9-standalone.html';
const LOGO =
  'https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/69c166836c109719f94e055e_Dealality%20Logo%20(4)%20(1).png';
const RAIL = 'https://my-operators-backend-production.up.railway.app';

const html = fs.readFileSync(STANDALONE, 'utf8');
const css = html.match(/<style>([\s\S]*?)<\/style>/)[1].trim();
let body = html
  .match(/<body>([\s\S]*)<\/body>/i)[1]
  .replace(/<script[\s\S]*?<\/script>/gi, '')
  .replace(/src="\/assets\/dealality-logo\.png"/g, `src="${LOGO}"`)
  .replace(/src="assets\/founder-joan-dejarden\.png"/g, `src="${RAIL}/marketing/assets/founder-joan-dejarden.png"`)
  .replace(/src="assets\/sales-home-bg-blob\.svg"/g, `src="${RAIL}/marketing/assets/sales-home-bg-blob.svg"`)
  .replace(/href="#"/g, 'href="https://www.dealality.com/login"')
  .trim();

function sliceBetween(src, startNeedle, endNeedle) {
  const start = src.indexOf(startNeedle);
  if (start < 0) throw new Error('missing start ' + startNeedle);
  const end = endNeedle ? src.indexOf(endNeedle, start + startNeedle.length) : src.length;
  if (end < 0) throw new Error('missing end ' + endNeedle);
  return src.slice(start, end).trim();
}

const embeds = [
  { label: '01-nav-hero-problem', html: sliceBetween(body, '<nav id="nav">', '<section id="how">') },
  { label: '02-how', html: sliceBetween(body, '<section id="how">', '<section id="audiences">') },
  { label: '03-audiences', html: sliceBetween(body, '<section id="audiences">', '<section id="why"') },
  { label: '04-why', html: sliceBetween(body, '<section id="why"', '<section id="faq">') },
  { label: '05-faq-cta-footer', html: sliceBetween(body, '<section id="faq">', null) },
];

const head = `<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@300;400;500;600;700;800&family=Lora:ital,wght@0,400;0,500;1,400&display=swap" rel="stylesheet">
<style>
${css}
html,body{margin:0;padding:0;background:#080F25;overflow-x:hidden}
</style>`;

fs.writeFileSync('tmp-v9-full-css.css', css);
fs.writeFileSync('tmp-v9-full-head.html', head);

embeds.forEach((e, i) => {
  const n = String(i + 1).padStart(2, '0');
  fs.writeFileSync(`tmp-v9-full-embed-${n}.html`, e.html);
  console.log(n, e.label, e.html.length);
});

// Minimal interactivity for published page (tabs/FAQ)
const js = `<script>
(function(){
  function on(sel,evt,fn){document.querySelectorAll(sel).forEach(function(el){el.addEventListener(evt,fn);});}
  // FAQ
  on('.faq-q','click',function(){
    var item=this.closest('.faq-item');
    if(!item)return;
    var open=item.classList.contains('open');
    document.querySelectorAll('.faq-item.open').forEach(function(i){i.classList.remove('open');});
    if(!open)item.classList.add('open');
  });
  // Stage tabs
  on('.fbt','click',function(){
    var i=this.getAttribute('data-p');
    document.querySelectorAll('.fbt').forEach(function(b){b.classList.toggle('on',b===this);b.setAttribute('aria-selected',b===this?'true':'false');}.bind(this));
    document.querySelectorAll('.fbp').forEach(function(p){var on=p.id==='stage-panel-'+i;p.classList.toggle('on',on);p.hidden=!on;});
  });
  // Audience tabs
  on('.audt','click',function(){
    var i=this.getAttribute('data-a');
    document.querySelectorAll('.audt').forEach(function(b){b.classList.toggle('on',b===this);b.setAttribute('aria-selected',b===this?'true':'false');}.bind(this));
    document.querySelectorAll('.audp').forEach(function(p){var on=p.id==='aud-panel-'+i;p.classList.toggle('on',on);p.hidden=!on;});
  });
  // Mobile nav
  var nmenu=document.getElementById('nmenu'), mnav=document.getElementById('mnav');
  if(nmenu&&mnav){
    nmenu.addEventListener('click',function(){
      var open=!mnav.classList.contains('open');
      mnav.classList.toggle('open',open);
      mnav.setAttribute('aria-hidden',open?'false':'true');
      nmenu.setAttribute('aria-expanded',open?'true':'false');
    });
    on('.mnav-link','click',function(){mnav.classList.remove('open');});
  }
  // Overview video
  var learn=document.getElementById('hero-learn-more');
  var wrap=document.getElementById('hero-overview-wrap');
  var close=document.getElementById('hero-overview-close');
  var vid=document.getElementById('hero-overview-video');
  function setOpen(o){
    if(!wrap)return;
    wrap.hidden=!o; wrap.classList.toggle('is-open',o);
    if(learn)learn.setAttribute('aria-expanded',o?'true':'false');
    if(!o&&vid){try{vid.pause();}catch(e){}}
  }
  if(learn)learn.addEventListener('click',function(){setOpen(wrap.hidden);});
  if(close)close.addEventListener('click',function(){setOpen(false);});
})();
</script>`;
fs.writeFileSync('tmp-v9-full-footer.js.html', js);
console.log('head', head.length, 'footer-js', js.length);
fs.writeFileSync(
  'tmp-v9-full-manifest.json',
  JSON.stringify(
    embeds.map((e, i) => ({
      file: `tmp-v9-full-embed-${String(i + 1).padStart(2, '0')}.html`,
      label: e.label,
      bytes: e.html.length,
    })),
    null,
    2
  )
);
