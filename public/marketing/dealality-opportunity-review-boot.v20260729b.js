(function(){
  var css="https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a6a0f9780e64a21dca29a33_dealality-opportunity-review.v20260729b.css";
  var js="https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a6a0f9749dbab830caa1036_dealality-opportunity-review.v20260729b.js";
  document.querySelectorAll('link[href*="dealality-opportunity-review"]').forEach(function(n){n.remove();});
  document.querySelectorAll('script[src*="dealality-opportunity-review"]').forEach(function(n){n.remove();});
  var l=document.createElement("link");
  l.rel="stylesheet";
  l.href=css;
  document.head.appendChild(l);
  var s=document.createElement("script");
  s.src=js;
  s.defer=true;
  (document.body||document.documentElement).appendChild(s);
})();
