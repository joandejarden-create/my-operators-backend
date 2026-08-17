(function(){
  var TEX="https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a691e4f3b0bf638b1052fc6_dealality-globe-texture.jpg";
  var container=document.getElementById("hero-globe-container");
  var list=document.getElementById("hero-globe-list");
  if(!container||!list)return;
  container.setAttribute("fs-3dglobe-element","container");
  container.setAttribute("fs-3dglobe-img",TEX);
  list.setAttribute("fs-3dglobe-element","list");
  var items=list.children;
  for(var i=0;i<items.length;i++){
    items[i].classList.add("w-dyn-item");
  }
  function setEl(id,attr,val){
    var el=document.getElementById(id);
    if(el)el.setAttribute(attr,val);
  }
  for(var n=1;n<=5;n++){
    setEl("hg-pin-"+n,"fs-3dglobe-element","pin");
    setEl("hg-tip-"+n,"fs-3dglobe-element","tooltip");
    setEl("hg-lat-"+n,"fs-3dglobe-element","lat");
    setEl("hg-lon-"+n,"fs-3dglobe-element","lon");
  }
  function loadScript(src){
    return new Promise(function(resolve,reject){
      var s=document.createElement("script");
      s.src=src;
      s.async=false;
      s.onload=function(){resolve();};
      s.onerror=function(){reject(new Error(src));};
      document.body.appendChild(s);
    });
  }
  var reduce=window.matchMedia&&window.matchMedia("(prefers-reduced-motion: reduce)").matches;
  var mobile=window.matchMedia&&window.matchMedia("(max-width:700px)").matches;
  if(mobile&&reduce)return;
  loadScript("https://cdnjs.cloudflare.com/ajax/libs/three.js/r125/three.min.js")
    .then(function(){return loadScript("https://cdn.jsdelivr.net/npm/@finsweet/3dglobes@1/OrbitControls.min.js");})
    .then(function(){return loadScript("https://cdn.jsdelivr.net/npm/@finsweet/3dglobes@1/FsGlobe.min.js");})
    .catch(function(err){if(typeof console!=="undefined"&&console.warn)console.warn("[oh-globe]",err);});
})();