const fs = require('fs');
const head = fs.readFileSync('tmp-old-home-head-visual.html', 'utf8');

// Build footer with existing interactions + visual injectors
const footer = `<script>
(function(){
  function ready(fn){if(document.readyState!=='loading')fn();else document.addEventListener('DOMContentLoaded',fn);}
  ready(function(){
    // Stage icons
    var ICONS=[
      '<svg viewBox="0 0 24 24"><circle cx="12" cy="12" r="10"/><polygon points="16.24 7.76 14.12 14.12 7.76 16.24 9.88 9.88" fill="currentColor"/></svg>',
      '<svg viewBox="0 0 24 24"><circle cx="12" cy="12" r="10"/><path d="M12 6v6l3.5 2"/><circle cx="12" cy="12" r="2" fill="currentColor" stroke="none"/></svg>',
      '<svg viewBox="0 0 24 24"><circle cx="12" cy="12" r="10"/><circle cx="12" cy="12" r="6"/><circle cx="12" cy="12" r="2"/></svg>',
      '<svg viewBox="0 0 24 24"><path d="M19 21l-7-5-7 5V5a2 2 0 0 1 2-2h10a2 2 0 0 1 2 2z"/></svg>',
      '<svg viewBox="0 0 24 24"><rect x="4" y="4" width="6" height="16" rx="1"/><rect x="14" y="8" width="6" height="12" rx="1"/><path d="M2 20h20"/></svg>',
      '<svg viewBox="0 0 24 24"><path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"/><polyline points="14 2 14 8 20 8"/><path d="M12 12v6"/><path d="M9 15h6"/></svg>'
    ];
    document.querySelectorAll('.dc-stage-tab').forEach(function(tab,i){
      if(tab.querySelector('.dc-fbt-ico'))return;
      var ico=document.createElement('span');
      ico.className='dc-fbt-ico';
      ico.setAttribute('aria-hidden','true');
      ico.innerHTML=ICONS[i]||ICONS[0];
      tab.insertBefore(ico,tab.firstChild);
    });

    // Audience icons
    var AUD=[
      '<svg class="dc-aud-ico dc-aud-ico--o" width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M16 21v-2a4 4 0 0 0-4-4H6a4 4 0 0 0-4 4v2"/><circle cx="9" cy="7" r="4"/><path d="M22 21v-2a4 4 0 0 0-3-3.87"/><path d="M16 3.13a4 4 0 0 1 0 7.75"/></svg>',
      '<svg class="dc-aud-ico dc-aud-ico--b" width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><circle cx="12" cy="12" r="10"/><path d="M12 8v8"/><path d="M8 12h8"/></svg>',
      '<svg class="dc-aud-ico dc-aud-ico--p" width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M3 21h18"/><path d="M5 21V7l7-4 7 4v14"/><path d="M9 21v-6h6v6"/></svg>'
    ];
    document.querySelectorAll('.dc-aud-tab').forEach(function(tab,i){
      if(tab.querySelector('.dc-aud-ico'))return;
      var wrap=document.createElement('span');
      wrap.innerHTML=AUD[i]||AUD[0];
      tab.insertBefore(wrap.firstChild,tab.firstChild);
    });

    // FAQ plus icons
    document.querySelectorAll('.dc-faq-q').forEach(function(q){
      if(q.querySelector('.dc-faq-ico'))return;
      var ico=document.createElement('span');
      ico.className='dc-faq-ico';
      ico.setAttribute('aria-hidden','true');
      ico.innerHTML='<svg viewBox="0 0 10 10" width="10" height="10"><line x1="5" y1="1" x2="5" y2="9" stroke="currentColor" stroke-width="2"/><line x1="1" y1="5" x2="9" y2="5" stroke="currentColor" stroke-width="2"/></svg>';
      q.appendChild(ico);
    });

    // Dot grids on key sections if missing
    ['#how','#faq','#audiences','#why'].forEach(function(sel){
      var el=document.querySelector(sel);
      if(!el||el.querySelector('.dc-dg'))return;
      var dg=document.createElement('div');
      dg.className='dc-dg';
      dg.setAttribute('aria-hidden','true');
      el.insertBefore(dg,el.firstChild);
    });

    // Hero mesh animation
    (function(){
      var hero=document.getElementById('hero');
      var canvas=document.getElementById('hero-mesh');
      if(!hero||!canvas)return;
      var ctx=canvas.getContext('2d');
      if(!ctx)return;
      var pts=[], raf=0, reduce=window.matchMedia('(prefers-reduced-motion: reduce)').matches;
      function resize(){
        var r=hero.getBoundingClientRect();
        var dpr=Math.min(window.devicePixelRatio||1,2);
        var w=Math.max(1,Math.floor(r.width)), h=Math.max(1,Math.floor(r.height));
        canvas.width=Math.round(w*dpr); canvas.height=Math.round(h*dpr);
        canvas.style.width=w+'px'; canvas.style.height=h+'px';
        ctx.setTransform(dpr,0,0,dpr,0,0);
        pts=[];
        var n=Math.min(70,Math.floor((w*h)/18000));
        for(var i=0;i<n;i++){
          pts.push({x:Math.random()*w,y:Math.random()*h,vx:(Math.random()-.5)*.25,vy:(Math.random()-.5)*.25});
        }
      }
      function draw(){
        var w=canvas.clientWidth, h=canvas.clientHeight;
        ctx.clearRect(0,0,w,h);
        ctx.fillStyle='rgba(108,114,255,.55)';
        ctx.strokeStyle='rgba(108,114,255,.16)';
        for(var i=0;i<pts.length;i++){
          var p=pts[i];
          if(!reduce){p.x+=p.vx;p.y+=p.vy;if(p.x<0||p.x>w)p.vx*=-1;if(p.y<0||p.y>h)p.vy*=-1;}
          ctx.beginPath();ctx.arc(p.x,p.y,1.3,0,Math.PI*2);ctx.fill();
          for(var j=i+1;j<pts.length;j++){
            var q=pts[j], dx=p.x-q.x, dy=p.y-q.y, d=Math.sqrt(dx*dx+dy*dy);
            if(d<120){ctx.globalAlpha=1-d/120;ctx.beginPath();ctx.moveTo(p.x,p.y);ctx.lineTo(q.x,q.y);ctx.stroke();ctx.globalAlpha=1;}
          }
        }
        if(!reduce)raf=requestAnimationFrame(draw);
      }
      resize(); draw();
      window.addEventListener('resize',function(){cancelAnimationFrame(raf);resize();draw();},{passive:true});
    })();

    function animateReadiness(){
      var ring=document.getElementById('drring');
      var val=document.getElementById('drval');
      document.querySelectorAll('#drwrap .dc-drm-bar').forEach(function(b){b.style.width=(b.getAttribute('data-v')||0)+'%';});
      if(ring){ring.style.strokeDashoffset=String(364*(1-0.74));}
      if(val){
        var n=0, target=74;
        var t=setInterval(function(){n+=2; if(n>=target){n=target;clearInterval(t);} val.textContent=n+'%';},30);
      }
    }

    var overview=document.getElementById('hero-overview-wrap');
    if(overview&&!document.getElementById('hero-overview-video')){
      var wrap=document.createElement('div');
      wrap.innerHTML='<video id="hero-overview-video" class="dc-overview-video" controls playsinline preload="none" poster="https://res.cloudinary.com/dos2eqnzd/video/upload/so_0/Video_Deck_Dealality_-_Short_Elevator_Pitch_Deck_Video_irnayw.jpg" style="width:100%;max-width:720px;margin:0 auto;display:block;border-radius:12px;background:#000"><source src="https://res.cloudinary.com/dos2eqnzd/video/upload/v1775832428/Video_Deck_Dealality_-_Short_Elevator_Pitch_Deck_Video_irnayw.mp4" type="video/mp4"></video>';
      overview.appendChild(wrap.firstChild);
    }
    function setOverview(open){
      if(!overview)return;
      overview.classList.toggle('dc-overview-open',open);
      overview.style.display=open?'block':'none';
      var btn=document.querySelector('.hero-learn-more');
      if(btn){btn.setAttribute('aria-expanded',open?'true':'false');try{btn.textContent=open?'Hide Overview':'Watch Overview';}catch(e){}}
      var vid=document.getElementById('hero-overview-video');
      if(vid&&!open){try{vid.pause();}catch(e){}}
      if(open&&overview.scrollIntoView)overview.scrollIntoView({behavior:'smooth',block:'start'});
    }
    document.querySelectorAll('.hero-learn-more').forEach(function(btn){
      btn.addEventListener('click',function(e){e.preventDefault();setOverview(!(overview&&overview.classList.contains('dc-overview-open')));});
    });
    var closeBtn=document.getElementById('hero-overview-close');
    if(closeBtn)closeBtn.addEventListener('click',function(e){e.preventDefault();setOverview(false);});

    var rot=document.getElementById('rotator');
    if(rot){
      var words=[].slice.call(rot.children);
      var wrapEl=rot.parentElement;
      var start=2; var suffixBuffer=2; var loopEnd=words.length-suffixBuffer-1; var ri=start;
      var h1=rot.closest('.dc-h1wrap');
      function centerSlot(){return window.matchMedia('(max-width:960px)').matches?1:2;}
      function gh(){var probe=words[start]||words[0];if(!probe)return 48;var had=probe.classList.contains('on');probe.classList.add('on');var h=probe.offsetHeight||48;if(!had)probe.classList.remove('on');return h;}
      function setWidth(){if(!wrapEl)return;var mx=0;words.forEach(function(w){w.classList.add('on');mx=Math.max(mx,w.scrollWidth||0);w.classList.remove('on');});var mobile=window.matchMedia('(max-width:960px)').matches;var cap=h1?h1.clientWidth:0;if(mobile&&cap>0)mx=Math.min(mx,cap);wrapEl.style.setProperty('--hr-w',mx+'px');}
      function paint(animate){var h=gh();var c=centerSlot();if(h1)h1.style.setProperty('--hr-lh',h+'px');rot.style.transition=animate?'transform .65s cubic-bezier(.77,0,.18,1)':'none';rot.style.transform='translateY('+((c-ri)*h)+'px)';words.forEach(function(w,i){w.style.transition=animate?'opacity .45s ease':'none';var dist=Math.abs(i-ri);w.classList.toggle('on',dist===0);w.classList.toggle('near',dist===1);w.classList.toggle('far',dist===2);});}
      setWidth();paint(false);
      var rotMs=window.matchMedia('(prefers-reduced-motion: reduce)').matches?0:3400;
      if(rotMs>0){setInterval(function(){ri++;paint(true);if(ri>=loopEnd)setTimeout(function(){ri=start;paint(false);},700);},rotMs);window.addEventListener('resize',function(){setWidth();paint(false);},{passive:true});}
    }

    document.querySelectorAll('.dc-faq-item').forEach(function(item){
      var q=item.querySelector('.dc-faq-q');
      var a=item.querySelector('.dc-faq-a');
      if(!q||!a)return;
      q.addEventListener('click',function(){var open=item.classList.toggle('open');a.classList.toggle('dc-faq-a-open',open);a.style.display=open?'block':'none';});
    });

    var stageTabs=document.querySelectorAll('.dc-stage-tab');
    var stagePanels=document.querySelectorAll('.dc-stage-panel');
    var platformSection=document.getElementById('how');
    var fbtourPause=document.getElementById('fbtour-pause')||document.querySelector('.dc-fbtour-pause');
    var progressFill=document.getElementById('fbprogress-fill');
    var currentStage=0;
    var stageTourTimer=null;
    var stageTourIdleTimer=null;
    var stageTourPaused=false;
    function activateStage(idx){
      currentStage=idx;
      stageTabs.forEach(function(t,i){var on=i===idx;t.classList.toggle('dc-stage-tab-on',on);t.setAttribute('aria-selected',on?'true':'false');});
      stagePanels.forEach(function(panel,i){var on=(panel.getAttribute('data-p')===String(idx))||i===idx;panel.classList.toggle('dc-stage-panel-on',on);panel.style.display=on?'block':'none';});
      if(progressFill)progressFill.style.width=((idx+1)/Math.max(stageTabs.length,1)*100).toFixed(2)+'%';
      if(idx===1)animateReadiness();
    }
    function startStageTour(){
      if(stageTourPaused||stageTourTimer)return;
      var ms=window.matchMedia('(prefers-reduced-motion: reduce)').matches?0:8000;
      if(!ms||!stageTabs.length)return;
      if(stageTourIdleTimer){clearTimeout(stageTourIdleTimer);stageTourIdleTimer=null;}
      stageTourTimer=setInterval(function(){if(stageTourPaused)return;activateStage((currentStage+1)%stageTabs.length);},ms);
    }
    function stopStageTour(){if(stageTourTimer){clearInterval(stageTourTimer);stageTourTimer=null;}}
    function scheduleStageTour(){
      if(stageTourPaused||stageTourTimer||stageTourIdleTimer)return;
      if(window.matchMedia('(prefers-reduced-motion: reduce)').matches)return;
      stageTourIdleTimer=setTimeout(function(){stageTourIdleTimer=null;if(!stageTourPaused)startStageTour();},3000);
    }
    function userPauseStageTour(){
      stageTourPaused=true;stopStageTour();
      if(stageTourIdleTimer){clearTimeout(stageTourIdleTimer);stageTourIdleTimer=null;}
      if(fbtourPause)fbtourPause.textContent='Resume tour';
    }
    stageTabs.forEach(function(tab,i){
      tab.addEventListener('click',function(){userPauseStageTour();activateStage(tab.getAttribute('data-p')!=null?+tab.getAttribute('data-p'):i);});
    });
    stagePanels.forEach(function(panel){if(!panel.classList.contains('dc-stage-panel-on'))panel.style.display='none';});
    if(platformSection&&stageTabs.length){
      var tourIo=new IntersectionObserver(function(entries){
        entries.forEach(function(x){
          if(x.isIntersecting&&!stageTourPaused)scheduleStageTour();
          else{stopStageTour();if(stageTourIdleTimer){clearTimeout(stageTourIdleTimer);stageTourIdleTimer=null;}}
        });
      },{threshold:0.25});
      tourIo.observe(platformSection);
    }
    if(fbtourPause){
      fbtourPause.addEventListener('click',function(){
        if(stageTourPaused){stageTourPaused=false;fbtourPause.textContent='Pause tour';scheduleStageTour();}
        else{userPauseStageTour();}
      });
    }

    var audTabs=document.querySelectorAll('.dc-aud-tab');
    var audPanels=document.querySelectorAll('.dc-aud-panel');
    var audSlugs=['owners','brands','partners'];
    var audHashMap={owners:0,brands:1,partners:2,persona:0,audiences:0};
    function activateAudience(idx,updateHash){
      audTabs.forEach(function(t,i){t.classList.toggle('dc-aud-tab-on',i===idx);t.setAttribute('aria-selected',i===idx?'true':'false');});
      audPanels.forEach(function(panel,i){var on=(panel.getAttribute('data-a')===String(idx))||i===idx;panel.classList.toggle('dc-aud-panel-on',on);panel.style.display=on?'block':'none';});
      if(updateHash!==false&&audSlugs[idx]){try{history.replaceState(null,'','#'+audSlugs[idx]);}catch(e){}}
    }
    function audienceFromHash(){var key=location.hash.replace('#','');return key in audHashMap?audHashMap[key]:null;}
    audTabs.forEach(function(tab,i){tab.addEventListener('click',function(){activateAudience(tab.getAttribute('data-a')!=null?+tab.getAttribute('data-a'):i);});});
    audPanels.forEach(function(panel){if(!panel.classList.contains('dc-aud-panel-on'))panel.style.display='none';});
    function syncAudienceHash(scroll){
      var idx=audienceFromHash();
      if(idx===null)return;
      activateAudience(idx,false);
      if(scroll){requestAnimationFrame(function(){setTimeout(function(){var el=document.getElementById('audiences');if(el)el.scrollIntoView({block:'start',behavior:'smooth'});},60);});}
    }
    syncAudienceHash(!!location.hash);
    window.addEventListener('hashchange',function(){syncAudienceHash(true);});

    var nmenu=document.getElementById('nmenu');
    var mnav=document.getElementById('mnav');
    function setMnav(open){
      if(!mnav||!nmenu)return;
      mnav.classList.toggle('dc-mnav-open',open);
      mnav.style.display=open?'block':'none';
      nmenu.setAttribute('aria-expanded',open?'true':'false');
      document.body.style.overflow=open?'hidden':'';
    }
    if(nmenu)nmenu.addEventListener('click',function(){setMnav(!(mnav&&mnav.classList.contains('dc-mnav-open')));});
    if(mnav)mnav.querySelectorAll('a').forEach(function(a){a.addEventListener('click',function(){setMnav(false);});});

    var nav=document.getElementById('nav');
    if(nav)window.addEventListener('scroll',function(){nav.classList.toggle('dc-nav-sc',window.scrollY>60);},{passive:true});

    if('IntersectionObserver' in window){
      var io=new IntersectionObserver(function(entries){entries.forEach(function(x){if(x.isIntersecting)x.target.classList.add('vis');});},{threshold:0.07});
      document.querySelectorAll('.fu,.dc-section,.dc-hero').forEach(function(el){io.observe(el);});
    }
  });
})();
</script>`;

fs.writeFileSync('tmp-old-home-footer-visual.html', footer);
fs.writeFileSync('tmp-apply-visual-code.cjs', `
const fs = require('fs');
const head = fs.readFileSync('tmp-old-home-head-visual.html','utf8');
const footer = fs.readFileSync('tmp-old-home-footer-visual.html','utf8');
console.log(JSON.stringify({ headLen: head.length, footerLen: footer.length }));
`);
console.log('footer', footer.length);
