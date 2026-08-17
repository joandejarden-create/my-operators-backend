(function(){
var b=document.getElementById("nmenu");
var m=document.getElementById("mnav");
if(b&&m){
  b.addEventListener("click",function(e){
    e.preventDefault();
    var open=m.classList.toggle("is-open");
    b.setAttribute("aria-expanded",open?"true":"false");
    if(open){m.removeAttribute("hidden");}else{m.setAttribute("hidden","");}
  });
  m.querySelectorAll("a").forEach(function(a){
    a.addEventListener("click",function(){
      m.classList.remove("is-open");
      m.setAttribute("hidden","");
      b.setAttribute("aria-expanded","false");
    });
  });
}
var tabs=document.querySelectorAll("#features-tabs a, #features-tabs button");
var panels=["fpanel-1","fpanel-2","fpanel-3","fpanel-4","fpanel-5"];
function showPanel(id){
  panels.forEach(function(pid){
    var el=document.getElementById(pid);
    if(!el)return;
    var on=pid===id;
    if(on){el.removeAttribute("hidden");el.classList.remove("oh-panel-hidden");}
    else{el.setAttribute("hidden","");el.classList.add("oh-panel-hidden");}
  });
}
tabs.forEach(function(tab){
  tab.addEventListener("click",function(e){
    e.preventDefault();
    tabs.forEach(function(t){
      t.setAttribute("aria-selected","false");
      t.classList.remove("oh-ftab-on");
      t.classList.add("oh-ftab");
    });
    tab.setAttribute("aria-selected","true");
    tab.classList.remove("oh-ftab");
    tab.classList.add("oh-ftab-on");
    showPanel(tab.getAttribute("data-panel"));
  });
});
var track=document.getElementById("insights-grid");
var prev=document.getElementById("insights-prev");
var next=document.getElementById("insights-next");
function cardStep(){
  if(!track)return 420;
  var card=track.querySelector("article,#ins-1,#ins-2,#ins-3");
  if(!card)return 420;
  var styles=window.getComputedStyle(track);
  var gap=parseFloat(styles.columnGap||styles.gap)||24;
  return Math.max(280,Math.round(card.getBoundingClientRect().width+gap));
}
function scrollMax(){
  if(!track)return 0;
  return Math.max(0,track.scrollWidth-track.clientWidth-2);
}
function setNavState(el,disabled){
  if(!el)return;
  el.setAttribute("aria-disabled",disabled?"true":"false");
  el.classList.toggle("is-disabled",disabled);
  if(disabled){el.setAttribute("tabindex","-1");}
  else{el.removeAttribute("tabindex");}
}
function syncInsightsNav(){
  if(!track||!prev||!next)return;
  var max=scrollMax();
  if(max<=0&&track.children&&track.children.length>1){
    setNavState(prev,true);
    setNavState(next,false);
    return;
  }
  setNavState(prev,track.scrollLeft<=2);
  setNavState(next,track.scrollLeft>=max);
}
function scrollInsights(dir,e){
  if(e){e.preventDefault();e.stopPropagation();}
  if(!track)return;
  var max=scrollMax();
  if(dir<0&&track.scrollLeft<=2)return;
  if(dir>0&&max>0&&track.scrollLeft>=max)return;
  var step=cardStep();
  if(max<=0){
    track.scrollBy({left:dir*step,behavior:"smooth"});
    setTimeout(syncInsightsNav,80);
    setTimeout(syncInsightsNav,320);
    return;
  }
  track.scrollBy({left:dir*step,behavior:"smooth"});
}
function bindInsights(){
  track=document.getElementById("insights-grid");
  prev=document.getElementById("insights-prev");
  next=document.getElementById("insights-next");
  if(!track||!prev||!next)return false;
  prev.setAttribute("href","#insights");
  next.setAttribute("href","#insights");
  if(!track.getAttribute("data-oh-ins-bound")){
    track.setAttribute("data-oh-ins-bound","1");
    track.addEventListener("scroll",syncInsightsNav,{passive:true});
    if(typeof ResizeObserver==="function"){
      try{new ResizeObserver(function(){syncInsightsNav();}).observe(track);}catch(err){}
    }
  }
  syncInsightsNav();
  return true;
}
document.addEventListener("click",function(e){
  var t=e.target&&e.target.closest?e.target.closest("#insights-prev,#insights-next"):null;
  if(!t)return;
  e.preventDefault();
  e.stopPropagation();
  bindInsights();
  scrollInsights(t.id==="insights-prev"?-1:1,e);
  setTimeout(syncInsightsNav,80);
  setTimeout(syncInsightsNav,320);
},true);
window.addEventListener("resize",function(){bindInsights();});
window.addEventListener("load",function(){bindInsights();});
bindInsights();
setTimeout(bindInsights,100);
setTimeout(bindInsights,400);
setTimeout(bindInsights,1000);
var rot=document.getElementById("rotator");
if(rot){
  var words=[].slice.call(rot.children);
  var wrap=rot.parentElement;
  var start=2;
  var suffixBuffer=2;
  var loopEnd=words.length-suffixBuffer-1;
  var ri=start;
  var h1=document.getElementById("h1wrap");
  var centerSlot=function(){return window.matchMedia("(max-width:960px)").matches?1:2;};
  var setActiveClass=function(w,on){
    w.classList.toggle("on",on);
    w.classList.toggle("oh-hrword-on",on);
    w.classList.toggle("oh-hrword",!on);
  };
  var gh=function(){
    var probe=words[start]||words[0];
    if(!probe)return 48;
    var hadOn=probe.classList.contains("on")||probe.classList.contains("oh-hrword-on");
    setActiveClass(probe,true);
    var h=probe.offsetHeight||48;
    if(!hadOn)setActiveClass(probe,false);
    return h;
  };
  var setWidth=function(){
    if(!wrap)return;
    var mx=0;
    words.forEach(function(w){
      setActiveClass(w,true);
      mx=Math.max(mx,w.scrollWidth);
      setActiveClass(w,false);
    });
    mx=Math.ceil(mx+24);
    var mobile=window.matchMedia("(max-width:960px)").matches;
    var cap=h1?h1.clientWidth:(wrap.parentElement?wrap.parentElement.clientWidth:0);
    if(mobile&&cap>0)mx=Math.min(mx,cap);
    wrap.style.setProperty("--hr-w",mx+"px");
    wrap.style.width=mx+"px";
  };
  var paint=function(animate){
    var h=gh(),c=centerSlot();
    if(h1)h1.style.setProperty("--hr-lh",h+"px");
    rot.style.transition=animate?"transform .65s cubic-bezier(.77,0,.18,1)":"none";
    rot.style.transform="translateY("+((c-ri)*h)+"px)";
    words.forEach(function(w,i){
      w.style.transition=animate?"opacity .45s ease":"none";
      var dist=Math.abs(i-ri);
      setActiveClass(w,dist===0);
      w.classList.toggle("near",dist===1);
      w.classList.toggle("far",dist===2);
    });
  };
  var boot=function(){setWidth();paint(false);};
  if(document.fonts&&document.fonts.ready){document.fonts.ready.then(boot).catch(boot);}else{boot();}
  var rotMs=window.matchMedia("(prefers-reduced-motion: reduce)").matches?0:3400;
  if(rotMs>0){
    setInterval(function(){
      ri++;
      paint(true);
      if(ri>=loopEnd)setTimeout(function(){ri=start;paint(false);},700);
    },rotMs);
    window.addEventListener("resize",function(){setWidth();paint(false);},{passive:true});
  }
}
})();
(function(){
var email=document.getElementById("fsw-email");
if(email){
  if(!email.getAttribute("placeholder"))email.setAttribute("placeholder","Email Address");
  email.setAttribute("autocomplete","email");
}
var form=document.getElementById("fsw-inner");
function go(e){
  if(e){e.preventDefault();e.stopPropagation();}
  // Email wrap is hidden on the hero CTA — do not block the iframe on empty/hidden email.
  var wrap=document.getElementById("fsw-field-wrap");
  var emailVisible=!(!email||(wrap&&window.getComputedStyle(wrap).display==="none")||email.offsetParent===null);
  if(emailVisible&&email&&typeof email.reportValidity==="function"&&!email.reportValidity())return;
  var v=(email&&email.value||"").trim();
  var url="https://www.dealality.com/opportunity-review";
  if(v)url+="?email="+encodeURIComponent(v);
  if(typeof window.ohOpenOpportunityReview==="function"){
    window.ohOpenOpportunityReview(url,"Explore Your Hotel Opportunity");
    return;
  }
  window.location.href=url;
}
if(form){
  form.setAttribute("action","https://www.dealality.com/opportunity-review");
  form.setAttribute("method","get");
  form.addEventListener("submit",go,true);
}
var btn=document.getElementById("fsw-btn");
if(btn){btn.addEventListener("click",go);}
var hit=document.getElementById("fsw-submit-hit");
if(hit){hit.addEventListener("click",go);}
})();
(function(){
var root=document.getElementById("insights");
var ctaBtn=document.getElementById("cta-band-btn");
var fswBtn=document.getElementById("fsw-btn");
if(!root&&!ctaBtn&&!fswBtn)return;
function isArticleUrl(href){
  try{
    var u=new URL(href,window.location.href);
    return /\/insights-posts\//i.test(u.pathname);
  }catch(err){return false;}
}
function isOpportunityUrl(href){
  try{
    var u=new URL(href,window.location.href);
    return /\/opportunity-review\/?$/i.test(u.pathname);
  }catch(err){return false;}
}
function ensureModal(){
  var el=document.getElementById("oh-article-reader");
  if(el)return el;
  el=document.createElement("div");
  el.id="oh-article-reader";
  el.setAttribute("hidden","");
  el.setAttribute("aria-hidden","true");
  el.innerHTML='<div id="oh-ar-dialog" role="dialog" aria-modal="true" aria-labelledby="oh-ar-title">'+
    '<div id="oh-ar-bar">'+
      '<p id="oh-ar-title">Article</p>'+
      '<div id="oh-ar-actions">'+
        '<a id="oh-ar-open" href="#" target="_blank" rel="noopener noreferrer">Open full page</a>'+
        '<button type="button" id="oh-ar-close" aria-label="Close">×</button>'+
      '</div>'+
    '</div>'+
    '<div id="oh-ar-body">'+
      '<div id="oh-ar-loading" class="is-on" role="status">Loading…</div>'+
      '<div id="oh-ar-error" role="alert">This page could not be loaded in the reader. <a id="oh-ar-error-link" href="#" target="_blank" rel="noopener noreferrer">Open it in a new tab</a>.</div>'+
      '<iframe id="oh-ar-frame" title="Embedded page reader" loading="lazy" referrerpolicy="no-referrer-when-downgrade"></iframe>'+
    '</div>'+
  '</div>';
  document.body.appendChild(el);
  return el;
}
var modal=ensureModal();
var dialog=document.getElementById("oh-ar-dialog");
var frame=document.getElementById("oh-ar-frame");
var titleEl=document.getElementById("oh-ar-title");
var openEl=document.getElementById("oh-ar-open");
var closeEl=document.getElementById("oh-ar-close");
var loadingEl=document.getElementById("oh-ar-loading");
var errorEl=document.getElementById("oh-ar-error");
var errorLink=document.getElementById("oh-ar-error-link");
var lastFocus=null;
var loadTimer=null;

function setLoading(on,label){
  if(loadingEl){
    loadingEl.classList.toggle("is-on",!!on);
    if(on)loadingEl.textContent=label||"Loading…";
  }
}
function setError(on){
  if(errorEl)errorEl.classList.toggle("is-on",!!on);
}
function hideEmbedChrome(doc){
  if(!doc)return;
  try{
    var links=doc.querySelectorAll(".insights-back-link,a.insights-back-link");
    links.forEach(function(link){
      link.style.setProperty("display","none","important");
      link.setAttribute("aria-hidden","true");
      link.setAttribute("hidden","");
    });
    if(!doc.getElementById("oh-embed-hide-back")){
      var style=doc.createElement("style");
      style.id="oh-embed-hide-back";
      style.textContent=".insights-back-link{display:none!important;}";
      (doc.head||doc.documentElement).appendChild(style);
    }
  }catch(err){}
}
function closeReader(){
  if(loadTimer){clearTimeout(loadTimer);loadTimer=null;}
  modal.classList.remove("is-open");
  modal.setAttribute("hidden","");
  modal.setAttribute("aria-hidden","true");
  document.documentElement.style.overflow="";
  document.body.style.overflow="";
  if(frame){frame.removeAttribute("src");frame.src="about:blank";}
  setLoading(false);
  setError(false);
  if(lastFocus&&typeof lastFocus.focus==="function")lastFocus.focus();
  lastFocus=null;
}
function openReader(url,label,opts){
  opts=opts||{};
  lastFocus=document.activeElement;
  titleEl.textContent=label||"Dealality";
  openEl.href=url;
  if(errorLink)errorLink.href=url;
  setError(false);
  setLoading(true,opts.loadingText||"Loading…");
  modal.removeAttribute("hidden");
  modal.classList.add("is-open");
  modal.setAttribute("aria-hidden","false");
  document.documentElement.style.overflow="hidden";
  document.body.style.overflow="hidden";
  if(loadTimer)clearTimeout(loadTimer);
  loadTimer=setTimeout(function(){
    if(loadingEl&&loadingEl.classList.contains("is-on")){
      setLoading(false);
      setError(true);
    }
  },12000);
  frame.onload=function(){
    if(loadTimer){clearTimeout(loadTimer);loadTimer=null;}
    setLoading(false);
    setError(false);
    try{hideEmbedChrome(frame.contentDocument);}catch(err){}
  };
  frame.onerror=function(){
    if(loadTimer){clearTimeout(loadTimer);loadTimer=null;}
    setLoading(false);
    setError(true);
  };
  frame.src=url;
  if(closeEl)closeEl.focus();
}
if(root){
  root.addEventListener("click",function(e){
    var a=e.target&&e.target.closest?e.target.closest("a[href]"):null;
    if(!a||!root.contains(a))return;
    var href=a.getAttribute("href")||"";
    if(!isArticleUrl(href))return;
    e.preventDefault();
    var label="";
    var card=a.closest("article");
    if(card){
      var t=card.querySelector("h3,h3 a,#ins-1-title,#ins-2-title");
      if(t)label=(t.textContent||"").trim();
    }
    if(!label)label=(a.textContent||"").trim()||"Article";
    openReader(a.href,label,{loadingText:"Loading article…"});
  });
}
window.ohOpenOpportunityReview=function(url,label){
  openReader(url,label||"Explore Your Hotel Opportunity",{loadingText:"Loading opportunity review…"});
};
if(ctaBtn){
  ctaBtn.addEventListener("click",function(e){
    var href=ctaBtn.getAttribute("href")||"https://www.dealality.com/opportunity-review";
    if(!isOpportunityUrl(href)&&!/opportunity-review/i.test(href))return;
    e.preventDefault();
    var label=(document.getElementById("cta-band-btn-text")&&document.getElementById("cta-band-btn-text").textContent||ctaBtn.textContent||"Start an Opportunity Review").trim();
    window.ohOpenOpportunityReview(ctaBtn.href||href,label);
  });
}
if(fswBtn){
  fswBtn.addEventListener("click",function(e){
    e.preventDefault();
    e.stopPropagation();
    var em=document.getElementById("fsw-email");
    var wrap=document.getElementById("fsw-field-wrap");
    var emailVisible=!(!em||(wrap&&window.getComputedStyle(wrap).display==="none")||em.offsetParent===null);
    if(emailVisible&&em&&typeof em.reportValidity==="function"&&!em.reportValidity())return;
    var v=(em&&em.value||"").trim();
    var url="https://www.dealality.com/opportunity-review";
    if(v)url+="?email="+encodeURIComponent(v);
    if(typeof window.ohOpenOpportunityReview==="function"){
      window.ohOpenOpportunityReview(url,"Explore Your Hotel Opportunity");
      return;
    }
    window.location.href=url;
  });
}
closeEl.addEventListener("click",function(e){e.preventDefault();closeReader();});
modal.addEventListener("click",function(e){
  if(e.target===modal)closeReader();
});
document.addEventListener("keydown",function(e){
  if(e.key==="Escape"&&modal.classList.contains("is-open")){
    e.preventDefault();
    closeReader();
  }
});
if(dialog){
  dialog.addEventListener("click",function(e){e.stopPropagation();});
}
})();
(function(){
var dot1=document.getElementById("modules-dot-1");
var dot2=document.getElementById("modules-dot-2");
var tab1=document.getElementById("modules-tab-outcomes");
var tab2=document.getElementById("modules-tab-platform");
var panel1=document.getElementById("modules-panel-outcomes");
var panel2=document.getElementById("modules-panel-platform");
if(!panel1||!panel2)return;
function setPanel(el,on){
  if(!el)return;
  if(on){
    el.removeAttribute("hidden");
    el.setAttribute("aria-hidden","false");
    el.style.display="";
  }else{
    el.setAttribute("hidden","");
    el.setAttribute("aria-hidden","true");
    el.style.display="none";
  }
}
function activate(which){
  var is1=which===1;
  if(dot1){
    dot1.classList.toggle("is-active",is1);
    dot1.setAttribute("aria-selected",is1?"true":"false");
  }
  if(dot2){
    dot2.classList.toggle("is-active",!is1);
    dot2.setAttribute("aria-selected",is1?"false":"true");
  }
  if(tab1)tab1.setAttribute("aria-selected",is1?"true":"false");
  if(tab2)tab2.setAttribute("aria-selected",is1?"false":"true");
  setPanel(panel1,is1);
  setPanel(panel2,!is1);
}
function bind(el,which){
  if(!el||el.dataset.ohModulesBound==="1")return;
  el.dataset.ohModulesBound="1";
  el.addEventListener("click",function(e){
    e.preventDefault();
    e.stopPropagation();
    activate(which);
  },true);
}
bind(dot1,1);
bind(dot2,2);
bind(tab1,1);
bind(tab2,2);
})();
(function(){
var icons={
"mod-1-icon":'<svg viewBox="0 0 40 40" fill="none" xmlns="http://www.w3.org/2000/svg"><circle cx="20" cy="20" r="16" stroke="#9B8AFB" stroke-width="1.5" stroke-dasharray="3 3"/><circle cx="20" cy="10" r="3" fill="#9B8AFB"/><circle cx="10" cy="26" r="3" fill="#9B8AFB" opacity=".6"/><circle cx="20" cy="26" r="3" fill="#9B8AFB" opacity=".6"/><circle cx="30" cy="26" r="3" fill="#9B8AFB" opacity=".6"/><path d="M20 13v5M14 24l4-4M26 24l-4-4" stroke="#9B8AFB" stroke-width="1.2" stroke-linecap="round"/></svg>',
"mod-2-icon":'<svg viewBox="0 0 40 40" fill="none" xmlns="http://www.w3.org/2000/svg"><rect x="8" y="12" width="24" height="18" rx="3" stroke="#9B8AFB" stroke-width="1.5"/><path d="M8 18h24" stroke="#9B8AFB" stroke-width="1.2"/><rect x="12" y="22" width="7" height="4" rx="1" fill="#9B8AFB" opacity=".5"/><rect x="21" y="22" width="7" height="4" rx="1" fill="#9B8AFB" opacity=".5"/><path d="M16 9v4M24 9v4" stroke="#9B8AFB" stroke-width="1.5" stroke-linecap="round"/><circle cx="15.5" cy="15" r="1" fill="#9B8AFB"/><circle cx="20" cy="15" r="1" fill="#9B8AFB"/><circle cx="24.5" cy="15" r="1" fill="#9B8AFB"/></svg>',
"mod-3-icon":'<svg viewBox="0 0 40 40" fill="none" xmlns="http://www.w3.org/2000/svg"><circle cx="20" cy="14" r="5" stroke="#9B8AFB" stroke-width="1.5"/><path d="M12 30c0-4.4 3.6-8 8-8s8 3.6 8 8" stroke="#9B8AFB" stroke-width="1.5" stroke-linecap="round"/><circle cx="31" cy="14" r="3" stroke="#9B8AFB" stroke-width="1.2" opacity=".5"/><path d="M28 24c1.7-1.3 3.2-1.5 5-1" stroke="#9B8AFB" stroke-width="1.2" stroke-linecap="round" opacity=".5"/><circle cx="9" cy="14" r="3" stroke="#9B8AFB" stroke-width="1.2" opacity=".5"/><path d="M12 24c-1.7-1.3-3.2-1.5-5-1" stroke="#9B8AFB" stroke-width="1.2" stroke-linecap="round" opacity=".5"/></svg>',
"mod-4-icon":'<svg viewBox="0 0 40 40" fill="none" xmlns="http://www.w3.org/2000/svg"><path d="M10 28V14l10-5 10 5v14" stroke="#9B8AFB" stroke-width="1.5" stroke-linejoin="round"/><path d="M10 20h20M20 9v19" stroke="#9B8AFB" stroke-width="1.2" stroke-dasharray="2 2" opacity=".4"/><circle cx="15" cy="17" r="2" fill="#9B8AFB" opacity=".6"/><circle cx="25" cy="17" r="2" fill="#9B8AFB" opacity=".6"/><circle cx="15" cy="24" r="2" fill="#9B8AFB" opacity=".4"/><circle cx="25" cy="24" r="2" fill="#9B8AFB" opacity=".4"/><path d="M7 28h26" stroke="#9B8AFB" stroke-width="1.5" stroke-linecap="round"/></svg>',
"mod-5-icon":'<svg viewBox="0 0 40 40" fill="none" xmlns="http://www.w3.org/2000/svg"><rect x="6" y="10" width="12" height="20" rx="2" stroke="#9B8AFB" stroke-width="1.5"/><rect x="22" y="10" width="12" height="20" rx="2" stroke="#9B8AFB" stroke-width="1.5"/><path d="M10 15h4M10 19h4M10 23h4" stroke="#9B8AFB" stroke-width="1.2" stroke-linecap="round" opacity=".6"/><path d="M26 15h4M26 19h4M26 23h4" stroke="#9B8AFB" stroke-width="1.2" stroke-linecap="round" opacity=".6"/><path d="M18 17l4 0M18 23l4 0" stroke="#9B8AFB" stroke-width="1.2" stroke-dasharray="1.5 1.5" opacity=".35"/><circle cx="28" cy="27" r="1.5" fill="#9B8AFB"/><path d="M12 26l2 2 3-4" stroke="#9B8AFB" stroke-width="1.3" stroke-linecap="round" stroke-linejoin="round"/></svg>',
"mod-6-icon":'<svg viewBox="0 0 40 40" fill="none" xmlns="http://www.w3.org/2000/svg"><circle cx="20" cy="20" r="12" stroke="#9B8AFB" stroke-width="1.5"/><path d="M20 12v8l5.5 5.5" stroke="#9B8AFB" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"/><path d="M14 8l-3-3M26 8l3-3" stroke="#9B8AFB" stroke-width="1.3" stroke-linecap="round"/><circle cx="20" cy="20" r="2" fill="#9B8AFB"/></svg>',
"modp-1-icon":'<svg viewBox="0 0 40 40" fill="none" xmlns="http://www.w3.org/2000/svg"><rect x="9" y="6" width="22" height="28" rx="2.5" stroke="#9B8AFB" stroke-width="1.5"/><path d="M14 13h12M14 18h12M14 23h8" stroke="#9B8AFB" stroke-width="1.2" stroke-linecap="round" opacity=".6"/><circle cx="28" cy="28" r="6" stroke="#9B8AFB" stroke-width="1.5"/><path d="M32 32l3.5 3.5" stroke="#9B8AFB" stroke-width="1.5" stroke-linecap="round"/></svg>',
"modp-2-icon":'<svg viewBox="0 0 40 40" fill="none" xmlns="http://www.w3.org/2000/svg"><circle cx="8" cy="20" r="3" fill="#9B8AFB"/><path d="M11 20h5" stroke="#9B8AFB" stroke-width="1.5"/><circle cx="20" cy="12" r="3" stroke="#9B8AFB" stroke-width="1.5" opacity=".7"/><circle cx="20" cy="20" r="3" stroke="#9B8AFB" stroke-width="1.5" opacity=".7"/><circle cx="20" cy="28" r="3" stroke="#9B8AFB" stroke-width="1.5" opacity=".7"/><path d="M16 20l1-5.5M16 20l1 5.5" stroke="#9B8AFB" stroke-width="1.2" opacity=".5"/><path d="M23 12h5M23 20h5M23 28h5" stroke="#9B8AFB" stroke-width="1.2" stroke-linecap="round" stroke-dasharray="2 2" opacity=".4"/><circle cx="32" cy="12" r="2.5" fill="#9B8AFB" opacity=".4"/><circle cx="32" cy="20" r="2.5" fill="#9B8AFB" opacity=".4"/><circle cx="32" cy="28" r="2.5" fill="#9B8AFB" opacity=".4"/></svg>',
"modp-3-icon":'<svg viewBox="0 0 40 40" fill="none" xmlns="http://www.w3.org/2000/svg"><circle cx="15" cy="16" r="4" stroke="#9B8AFB" stroke-width="1.5"/><circle cx="27" cy="16" r="4" stroke="#9B8AFB" stroke-width="1.5"/><path d="M9 30c0-3.3 2.7-6 6-6s6 2.7 6 6" stroke="#9B8AFB" stroke-width="1.5" stroke-linecap="round" opacity=".6"/><path d="M21 30c0-3.3 2.7-6 6-6s6 2.7 6 6" stroke="#9B8AFB" stroke-width="1.5" stroke-linecap="round" opacity=".6"/><path d="M19 16h2" stroke="#9B8AFB" stroke-width="1.5" stroke-linecap="round" stroke-dasharray="1.5 1.5" opacity=".4"/><path d="M18 11l2-3M24 11l-2-3" stroke="#9B8AFB" stroke-width="1.2" stroke-linecap="round" opacity=".5"/><circle cx="21" cy="7" r="1.5" fill="#9B8AFB" opacity=".5"/></svg>',
"modp-4-icon":'<svg viewBox="0 0 40 40" fill="none" xmlns="http://www.w3.org/2000/svg"><rect x="6" y="12" width="20" height="16" rx="2.5" stroke="#9B8AFB" stroke-width="1.5"/><path d="M6 16l10 6 10-6" stroke="#9B8AFB" stroke-width="1.2" opacity=".5"/><rect x="22" y="8" width="12" height="9" rx="2" stroke="#9B8AFB" stroke-width="1.2" opacity=".6"/><path d="M25 12h6M25 14h4" stroke="#9B8AFB" stroke-width="1" stroke-linecap="round" opacity=".4"/><circle cx="30" cy="26" r="5" stroke="#9B8AFB" stroke-width="1.5"/><path d="M30 23v3l2 1.5" stroke="#9B8AFB" stroke-width="1.2" stroke-linecap="round"/></svg>',
"modp-5-icon":'<svg viewBox="0 0 40 40" fill="none" xmlns="http://www.w3.org/2000/svg"><rect x="5" y="8" width="13" height="24" rx="2" stroke="#9B8AFB" stroke-width="1.5"/><rect x="22" y="8" width="13" height="24" rx="2" stroke="#9B8AFB" stroke-width="1.5"/><path d="M9 14h5M9 18h5M9 22h5" stroke="#9B8AFB" stroke-width="1.2" stroke-linecap="round" opacity=".5"/><path d="M26 14h5M26 18h5M26 22h5" stroke="#9B8AFB" stroke-width="1.2" stroke-linecap="round" opacity=".5"/><path d="M18 16l4 0M18 20l4 0" stroke="#9B8AFB" stroke-width="1.3" stroke-dasharray="1.5 1.5" opacity=".35"/><path d="M9 27l2 2 3-3.5" stroke="#9B8AFB" stroke-width="1.3" stroke-linecap="round" stroke-linejoin="round"/><path d="M26 27l2 2 3-3.5" stroke="#9B8AFB" stroke-width="1.3" stroke-linecap="round" stroke-linejoin="round"/></svg>',
"modp-6-icon":'<svg viewBox="0 0 40 40" fill="none" xmlns="http://www.w3.org/2000/svg"><path d="M8 22c0 0 4-8 12-8s12 8 12 8" stroke="#9B8AFB" stroke-width="1.5" stroke-linecap="round"/><path d="M8 22c0 0 4 8 12 8s12-8 12-8" stroke="#9B8AFB" stroke-width="1.5" stroke-linecap="round"/><circle cx="20" cy="22" r="5" stroke="#9B8AFB" stroke-width="1.5"/><circle cx="20" cy="22" r="2" fill="#9B8AFB"/><path d="M17 8h6M20 6v4" stroke="#9B8AFB" stroke-width="1.3" stroke-linecap="round" opacity=".5"/><path d="M31 15l2-2M9 15l-2-2" stroke="#9B8AFB" stroke-width="1.2" stroke-linecap="round" opacity=".4"/></svg>'
};
Object.keys(icons).forEach(function(id){
  var el=document.getElementById(id);
  if(!el)return;
  el.classList.add("mod-icon");
  el.innerHTML=icons[id];
});
})();

(function(){
var root=document.getElementById("about");
if(!root||root.getAttribute("data-oh-problem-v2")==="1")return;
root.setAttribute("data-oh-problem-v2","1");
var h2=document.getElementById("about-h2");
if(h2)h2.innerHTML="Most hotel owners do not lack options.<br>They lack a good way to compare them.";
var lead=document.getElementById("about-lead");
if(lead)lead.textContent="Hotel opportunities are still evaluated across emails, slide decks, spreadsheets, calls, and separate advisor conversations. Different parties receive different information, respond in different formats, and use different assumptions. That makes the process slower, the options harder to compare, and the full potential of the asset easier to miss.";
var lead2=document.getElementById("about-lead-2");
if(lead2)lead2.setAttribute("hidden","");
var close=document.getElementById("about-close");
if(close)close.setAttribute("hidden","");
var cards=[
  {id:"about-point-1",title:"Fragmented outreach",body:"Owners repeat the same story across separate conversations.",icon:'<svg viewBox="0 0 40 40" fill="none" xmlns="http://www.w3.org/2000/svg"><circle cx="12" cy="14" r="3.5" stroke="#9B8AFB" stroke-width="1.5"/><circle cx="28" cy="14" r="3.5" stroke="#9B8AFB" stroke-width="1.5"/><circle cx="20" cy="28" r="3.5" stroke="#9B8AFB" stroke-width="1.5"/><path d="M15 15.5l8-1M25.5 16.5l-4.5 8M14.5 16.5l4.5 8" stroke="#9B8AFB" stroke-width="1.2" stroke-linecap="round" opacity=".55"/><path d="M8 8l2.5 2.5M32 8l-2.5 2.5M20 34v-2" stroke="#9B8AFB" stroke-width="1.2" stroke-linecap="round" opacity=".4"/></svg>'},
  {id:"about-point-2",title:"Slower comparison",body:"Brands and partners respond in different formats.",icon:'<svg viewBox="0 0 40 40" fill="none" xmlns="http://www.w3.org/2000/svg"><rect x="6" y="10" width="11" height="20" rx="2" stroke="#9B8AFB" stroke-width="1.5"/><rect x="23" y="10" width="11" height="20" rx="2" stroke="#9B8AFB" stroke-width="1.5"/><path d="M9 15h5M9 19h5M9 23h4" stroke="#9B8AFB" stroke-width="1.2" stroke-linecap="round" opacity=".55"/><path d="M26 15h5M26 19h3M26 23h5" stroke="#9B8AFB" stroke-width="1.2" stroke-linecap="round" opacity=".55"/><path d="M18 16h3M18 24h3" stroke="#9B8AFB" stroke-width="1.2" stroke-dasharray="1.5 1.5" opacity=".35"/><path d="M29 27l1.5 1.5 3-3.5" stroke="#9B8AFB" stroke-width="1.3" stroke-linecap="round" stroke-linejoin="round" opacity=".45"/></svg>'},
  {id:"about-point-3",title:"Missed upside",body:"Better-fit paths may never be explored.",icon:'<svg viewBox="0 0 40 40" fill="none" xmlns="http://www.w3.org/2000/svg"><circle cx="20" cy="20" r="12" stroke="#9B8AFB" stroke-width="1.5" stroke-dasharray="3 3" opacity=".55"/><path d="M20 10v4M20 26v4M10 20h4M26 20h4" stroke="#9B8AFB" stroke-width="1.2" stroke-linecap="round" opacity=".4"/><circle cx="20" cy="20" r="3" fill="#9B8AFB" opacity=".55"/><path d="M27 11l5-5M32 6v4M32 6h-4" stroke="#9B8AFB" stroke-width="1.4" stroke-linecap="round" stroke-linejoin="round"/><path d="M13 27c2.2-2.8 5.2-4 7-4" stroke="#9B8AFB" stroke-width="1.2" stroke-linecap="round" opacity=".35"/></svg>'}
];
cards.forEach(function(card){
  var li=document.getElementById(card.id);
  if(!li)return;
  li.classList.add("oh-about-point");
  var iconId=card.id+"-icon";
  var existing=document.getElementById(iconId);
  if(!existing){
    existing=document.createElement("div");
    existing.id=iconId;
    existing.className="about-point-icon mod-icon";
    existing.setAttribute("aria-hidden","true");
    li.insertBefore(existing,li.firstChild);
  }
  existing.classList.add("about-point-icon","mod-icon");
  existing.innerHTML=card.icon;
  var strong=li.querySelector("strong");
  var span=li.querySelector("span");
  if(strong)strong.textContent=card.title;
  if(span)span.textContent=card.body;
});
var visual=document.getElementById("about-visual");
if(!visual)return;
visual.setAttribute("aria-label","Fragmented evaluation process");
visual.innerHTML=
  '<div id="about-frag">'+
    '<p id="about-frag-eyebrow">How it usually happens today</p>'+
    '<div id="about-frag-anchor">One hotel opportunity</div>'+
    '<div id="about-frag-scatter" aria-hidden="true">'+
      '<span class="about-frag-chip is-channel c1">Brand conversation</span>'+
      '<span class="about-frag-chip is-channel c2">Operator introduction</span>'+
      '<span class="about-frag-chip is-channel c3">Advisor recommendation</span>'+
      '<span class="about-frag-chip is-channel c4">Capital discussion</span>'+
      '<span class="about-frag-chip is-format c5">Proposal in email</span>'+
      '<span class="about-frag-chip is-format c6">Terms in spreadsheet</span>'+
      '<span class="about-frag-chip is-format c7">Questions in calls</span>'+
      '<span class="about-frag-chip is-format c8">Documents in PDFs</span>'+
    '</div>'+
    '<div id="about-frag-verdict">'+
      '<p id="about-frag-hard">Hard to compare fairly</p>'+
      '<div id="about-frag-diffs"><span>Different info</span><span>Different assumptions</span><span>Different formats</span></div>'+
      '<p id="about-frag-hidden">Potential value stays hidden</p>'+
    '</div>'+
  '</div>';
})();
