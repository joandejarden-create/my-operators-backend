const fs = require('fs');

// Current visual JS was retrieved earlier; rebuild from known script with AUD icon fix.
// We patch by loading the CSS payload companion and writing a full JS embed file.

const AUD = [
  '<svg class="dc-aud-ico dc-aud-ico--o" width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M3 21h18"/><path d="M6 21V10.5L12 6l6 4.5V21"/><path d="M10 21v-5.5h4V21"/><path d="M9.5 12.5h1"/><path d="M13.5 12.5h1"/><path d="M9.5 9.5h1"/><path d="M13.5 9.5h1"/><path d="M10 6h4"/><path d="M12 6V4.5"/></svg>',
  '<svg class="dc-aud-ico dc-aud-ico--b" width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><circle cx="12" cy="12" r="2.25"/><circle cx="5.25" cy="7.75" r="1.75"/><circle cx="18.75" cy="7.75" r="1.75"/><circle cx="7.25" cy="17.5" r="1.75"/><circle cx="16.75" cy="17.5" r="1.75"/><path d="M10.15 10.55L6.55 8.85"/><path d="M13.85 10.55l3.6-1.7"/><path d="M10.55 13.75L8.35 16.1"/><path d="M13.45 13.75l2.2 2.35"/></svg>',
  '<svg class="dc-aud-ico dc-aud-ico--p" width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M9 5.5h6a1.75 1.75 0 011.75 1.75V19a1 1 0 01-1 1H8.25a1 1 0 01-1-1V7.25A1.75 1.75 0 019 5.5z"/><path d="M9 4.25h6v2.25H9V4.25z"/><path d="M9.5 11.5h5"/><path d="M9.5 14.25h5"/><path d="M9.5 17h3.5"/><path d="M14.5 4.25v1.5"/></svg>',
];

const script = `<script>(function(){function ready(fn){if(document.readyState!=='loading')fn();else document.addEventListener('DOMContentLoaded',fn);}ready(function(){var ICONS=['<svg viewBox="0 0 24 24"><circle cx="12" cy="12" r="10"/><polygon points="16.24 7.76 14.12 14.12 7.76 16.24 9.88 9.88" fill="currentColor"/></svg>','<svg viewBox="0 0 24 24"><circle cx="12" cy="12" r="10"/><path d="M12 6v6l3.5 2"/><circle cx="12" cy="12" r="2" fill="currentColor" stroke="none"/></svg>','<svg viewBox="0 0 24 24"><circle cx="12" cy="12" r="10"/><circle cx="12" cy="12" r="6"/><circle cx="12" cy="12" r="2"/></svg>','<svg viewBox="0 0 24 24"><path d="M19 21l-7-5-7 5V5a2 2 0 0 1 2-2h10a2 2 0 0 1 2 2z"/></svg>','<svg viewBox="0 0 24 24"><rect x="4" y="4" width="6" height="16" rx="1"/><rect x="14" y="8" width="6" height="12" rx="1"/><path d="M2 20h20"/></svg>','<svg viewBox="0 0 24 24"><path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"/><polyline points="14 2 14 8 20 8"/><path d="M12 12v6"/><path d="M9 15h6"/></svg>'];document.querySelectorAll('.dc-stage-tab').forEach(function(tab,i){if(tab.querySelector('.dc-fbt-ico'))return;var ico=document.createElement('span');ico.className='dc-fbt-ico';ico.setAttribute('aria-hidden','true');ico.innerHTML=ICONS[i]||ICONS[0];tab.insertBefore(ico,tab.firstChild);});var AUD=${JSON.stringify(AUD)};document.querySelectorAll('.dc-aud-tab').forEach(function(tab,i){var existing=tab.querySelector('.dc-aud-ico');if(existing)existing.remove();var w=document.createElement('span');w.innerHTML=AUD[i]||AUD[0];tab.insertBefore(w.firstChild,tab.firstChild);});document.querySelectorAll('.dc-faq-q').forEach(function(q){if(q.querySelector('.dc-faq-ico'))return;var ico=document.createElement('span');ico.className='dc-faq-ico';ico.innerHTML='<svg viewBox="0 0 10 10" width="10" height="10"><line x1="5" y1="1" x2="5" y2="9" stroke="currentColor" stroke-width="2"/><line x1="1" y1="5" x2="9" y2="5" stroke="currentColor" stroke-width="2"/></svg>';q.appendChild(ico);});['#how','#faq','#audiences','#why'].forEach(function(sel){var el=document.querySelector(sel);if(!el||el.querySelector('.dc-dg'))return;var dg=document.createElement('div');dg.className='dc-dg';dg.setAttribute('aria-hidden','true');el.insertBefore(dg,el.firstChild);});(function(){var hero=document.getElementById('hero');var canvas=document.getElementById('hero-mesh');if(!hero||!canvas)return;var ctx=canvas.getContext('2d');if(!ctx)return;var CONNECT=148,DOT='rgba(155,160,255,.88)',DRIFT=.22;var w=0,h=0,splitX=0,splitY=0,pts=[],raf=null,visible=true;var reduceMotion=window.matchMedia('(prefers-reduced-motion: reduce)').matches;function count(){return window.innerWidth<768?48:72;}function updateSplit(){var hr=hero.getBoundingClientRect();splitX=hr.width*.52;splitY=hr.height*.48;hero.style.setProperty('--hero-split','52%');hero.style.setProperty('--hero-split-y','48%');}function mkPt(){var zoneX=Math.max(splitX*1.08,w*.55);return{x:Math.random()*zoneX,y:Math.random()*h,vx:(Math.random()-.5)*DRIFT,vy:(Math.random()-.5)*DRIFT,r:2.2+Math.random()*1.6};}function resize(){var r=hero.getBoundingClientRect();var dpr=Math.min(window.devicePixelRatio||1,2);w=Math.max(1,Math.round(r.width));h=Math.max(1,Math.round(r.height));updateSplit();canvas.width=Math.round(w*dpr);canvas.height=Math.round(h*dpr);canvas.style.width=w+'px';canvas.style.height=h+'px';ctx.setTransform(dpr,0,0,dpr,0,0);var n=count();while(pts.length<n)pts.push(mkPt());while(pts.length>n)pts.pop();pts.forEach(function(p){p.x=Math.min(w,Math.max(0,p.x));p.y=Math.min(h,Math.max(0,p.y));});}function draw(move){ctx.clearRect(0,0,w,h);if(move){pts.forEach(function(p){p.x+=p.vx;p.y+=p.vy;if(p.x<-16)p.x=w+16;else if(p.x>w+16)p.x=-16;if(p.y<-16)p.y=h+16;else if(p.y>h+16)p.y=-16;});}for(var i=0;i<pts.length;i++){for(var j=i+1;j<pts.length;j++){var d=Math.hypot(pts[i].x-pts[j].x,pts[i].y-pts[j].y);if(d<CONNECT){var a=(1-d/CONNECT)*.55;ctx.strokeStyle='rgba(108,114,255,'+a.toFixed(3)+')';ctx.lineWidth=1.15;ctx.beginPath();ctx.moveTo(pts[i].x,pts[i].y);ctx.lineTo(pts[j].x,pts[j].y);ctx.stroke();}}}pts.forEach(function(p){ctx.beginPath();ctx.arc(p.x,p.y,p.r,0,Math.PI*2);ctx.fillStyle=DOT;ctx.fill();});}function loop(){if(!visible)return;draw(true);raf=requestAnimationFrame(loop);}function boot(){resize();draw(false);}boot();requestAnimationFrame(boot);window.addEventListener('load',boot,{once:true});if(!reduceMotion){if('IntersectionObserver' in window){var mIo=new IntersectionObserver(function(e){e.forEach(function(x){visible=x.isIntersecting;if(visible&&!raf)raf=requestAnimationFrame(loop);else if(!visible&&raf){cancelAnimationFrame(raf);raf=null;}});},{threshold:0});mIo.observe(hero);}raf=requestAnimationFrame(loop);}window.addEventListener('resize',boot,{passive:true});})();function animateReadiness(){var ring=document.getElementById('drring');var val=document.getElementById('drval');document.querySelectorAll('#drwrap .dc-drm-bar').forEach(function(b){b.style.width=(b.getAttribute('data-v')||0)+'%';});if(ring)ring.style.strokeDashoffset=String(364*(1-0.74));if(val){var n=0,t=setInterval(function(){n+=2;if(n>=74){n=74;clearInterval(t);}val.textContent=n+'%';},30);}}var fill=document.getElementById('fbprogress-fill');document.querySelectorAll('.dc-stage-tab').forEach(function(tab){tab.addEventListener('click',function(){var idx=+(tab.getAttribute('data-p')||0);if(fill)fill.style.width=((idx+1)/6*100).toFixed(2)+'%';if(idx===1)animateReadiness();});});if('IntersectionObserver' in window){var io=new IntersectionObserver(function(entries){entries.forEach(function(x){if(x.isIntersecting){var on=[].slice.call(document.querySelectorAll('.dc-stage-tab')).findIndex(function(t){return t.classList.contains('dc-stage-tab-on');});if(on===1)animateReadiness();}});},{threshold:0.2});var prep=document.querySelector('.dc-stage-panel[data-p="1"]');if(prep)io.observe(prep);}});})();</script>`;

fs.writeFileSync('tmp-dc-visual-js.html', script);
const css = fs.readFileSync('tmp-dc-visual-css.html', 'utf8');
const payload = {
  siteId: '68108c29063eeb5d1bd7ae4a',
  pageId: '68108c2a063eeb5d1bd7ae90',
  context: 'Update visual CSS and JS embeds for Railway audiences why owners FAQ parity.',
  actions: [
    {
      label: 'set_css',
      set_settings: {
        operations: [
          {
            label: 'css',
            element_id: {
              component: '68108c2a063eeb5d1bd7ae90',
              element: 'fa54fe3e-4cb4-cfea-0aa4-61c303aa3ad4',
            },
            settings: [{ key: 'code', static_text: { value: css } }],
          },
        ],
      },
    },
    {
      label: 'set_js',
      set_settings: {
        operations: [
          {
            label: 'js',
            element_id: {
              component: '68108c2a063eeb5d1bd7ae90',
              element: 'ad5f32bb-f1d0-9817-179a-e8bc6c3192d6',
            },
            settings: [{ key: 'code', static_text: { value: script } }],
          },
        ],
      },
    },
  ],
};
fs.writeFileSync('tmp-set-visual-css-js.json', JSON.stringify(payload));
console.log('css', css.length, 'js', script.length, 'payload bytes', Buffer.byteLength(JSON.stringify(payload)));
console.log('aud has building', script.includes('M3 21h18'));
console.log('aud force replace', script.includes('existing.remove'));
