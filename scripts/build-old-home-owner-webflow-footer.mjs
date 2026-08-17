/**
 * Builds a self-contained Old Home footer freeform snippet:
 * CSS + HTML embedded in JS (no Railway asset dependency for render).
 */
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const root = path.join(__dirname, '..');
const html = fs.readFileSync(path.join(root, 'public/marketing/dealality-old-home-owner.html'), 'utf8');
const css = fs.readFileSync(path.join(root, 'public/marketing/dealality-old-home-owner.css'), 'utf8');

const footer = `<script>
(function(){
  var API_BASE='https://my-operators-backend-production.up.railway.app';
  var OPP='https://www.dealality.com/opportunity-review';
  var EV=API_BASE+'/api/marketing/landing-events';
  var CSS=${JSON.stringify(css)};
  var HTML=${JSON.stringify(html)};
  function track(event,extra){
    try{
      var payload=Object.assign({
        event:event,
        surface:'old_home_owner',
        path:location.pathname,
        device:innerWidth<768?'mobile':innerWidth<1024?'tablet':'desktop',
        referrer:document.referrer||'',
        landingVersion:'old-home-owner-v1'
      },extra||{});
      var body=JSON.stringify(payload);
      if(navigator.sendBeacon)navigator.sendBeacon(EV,new Blob([body],{type:'application/json'}));
      else fetch(EV,{method:'POST',headers:{'Content-Type':'application/json'},body:body,credentials:'omit',keepalive:true}).catch(function(){});
    }catch(e){}
  }
  function boot(){
    if(!document.getElementById('dc-owner-inline-css')){
      var s=document.createElement('style');
      s.id='dc-owner-inline-css';
      s.textContent=CSS;
      document.head.appendChild(s);
    }
    var page=document.getElementById('dc-page')||document.body;
    ['nav','mnav','hero','hero-overview-wrap','proofbar','problem','how','audiences','why','faq','cta','footer'].forEach(function(id){
      var el=document.getElementById(id);
      if(el){el.style.display='none';el.setAttribute('aria-hidden','true');}
    });
    var existing=document.getElementById('dc-owner-root');
    if(existing) existing.remove();
    var rootEl=document.createElement('div');
    rootEl.id='dc-owner-root';
    rootEl.className='dc-owner-root';
    rootEl.innerHTML=HTML;
    rootEl.querySelectorAll('a[href*=\"opportunity-review\"]').forEach(function(a){
      a.setAttribute('href',OPP);
    });
    page.insertBefore(rootEl,page.firstChild);
    document.documentElement.classList.add('dc-owner-home');
    var nmenu=rootEl.querySelector('#nmenu');
    var mnav=rootEl.querySelector('#mnav');
    function setM(open){
      if(!mnav||!nmenu)return;
      if(open)mnav.removeAttribute('hidden');
      else mnav.setAttribute('hidden','');
      nmenu.setAttribute('aria-expanded',open?'true':'false');
      document.body.style.overflow=open?'hidden':'';
    }
    if(nmenu)nmenu.addEventListener('click',function(){
      setM(mnav.hasAttribute('hidden'));
      track('mobile_nav_open');
    });
    if(mnav)mnav.querySelectorAll('a').forEach(function(a){
      a.addEventListener('click',function(){setM(false);});
    });
    rootEl.querySelectorAll('.dc-faq-q').forEach(function(btn){
      btn.setAttribute('aria-expanded','false');
      btn.addEventListener('click',function(){
        var item=btn.closest('.dc-faq-item');
        var open=item.classList.toggle('is-open');
        btn.setAttribute('aria-expanded',open?'true':'false');
        if(open)track('homepage_faq_open',{label:(btn.textContent||'').trim().slice(0,64)});
      });
    });
    rootEl.addEventListener('click',function(ev){
      var t=ev.target&&ev.target.closest&&ev.target.closest('a,button');
      if(!t)return;
      var cta=t.getAttribute('data-dc-cta');
      if(cta==='primary'){
        track('homepage_primary_cta_click',{location:t.getAttribute('data-dc-loc')||'unknown'});
        track('cta_click',{label:'discuss_your_hotel_opportunity',location:t.getAttribute('data-dc-loc')||'unknown'});
      }
      if(cta==='secondary')track('homepage_secondary_cta_click',{location:t.getAttribute('data-dc-loc')||'unknown'});
      if(t.getAttribute('data-dc-track')==='signin')track('signin_click');
      if(t.getAttribute('data-dc-track')==='secondary_audience'){
        track('secondary_audience_click',{label:(t.textContent||'').trim().slice(0,64)});
      }
    });
    rootEl.querySelectorAll('a[href^=\"#\"]').forEach(function(a){
      a.addEventListener('click',function(e){
        var id=(a.getAttribute('href')||'').slice(1);
        var el=id&&rootEl.querySelector('#'+CSS.escape(id));
        if(!el)return;
        e.preventDefault();
        el.scrollIntoView({behavior:'smooth',block:'start'});
        try{history.pushState(null,'','#'+id);}catch(err){}
      });
    });
    if('IntersectionObserver' in window){
      var how=rootEl.querySelector('#how-it-works');
      if(how){
        var seen=false;
        new IntersectionObserver(function(entries){
          entries.forEach(function(x){
            if(x.isIntersecting&&!seen){seen=true;track('how_it_works_view');}
          });
        },{threshold:0.35}).observe(how);
      }
    }
    track('homepage_view');
    track('page_land');
  }
  if(document.readyState==='loading')document.addEventListener('DOMContentLoaded',boot);
  else boot();
})();
</script>
`;

const out = path.join(root, 'tmp-old-home-owner-footer.html');
fs.writeFileSync(out, footer);
console.log('Wrote', out, 'bytes', Buffer.byteLength(footer));
