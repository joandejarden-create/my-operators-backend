const fs = require('fs');
let foot = fs.readFileSync('tmp-old-home-footer-fixed.txt', 'utf8');
const modulesJs = `<script>(function(){
var out=document.getElementById("modules-tab-outcomes");
var plat=document.getElementById("modules-tab-platform");
var pOut=document.getElementById("modules-panel-outcomes");
var pPlat=document.getElementById("modules-panel-platform");
function show(which){
  var isOut=which==="outcomes";
  if(out)out.setAttribute("aria-selected",isOut?"true":"false");
  if(plat)plat.setAttribute("aria-selected",isOut?"false":"true");
  if(pOut){if(isOut)pOut.removeAttribute("hidden");else pOut.setAttribute("hidden","");}
  if(pPlat){if(isOut)pPlat.setAttribute("hidden","");else pPlat.removeAttribute("hidden");}
}
if(out)out.addEventListener("click",function(e){e.preventDefault();show("outcomes");});
if(plat)plat.addEventListener("click",function(e){e.preventDefault();show("platform");});
})();</script>`;
if (!foot.includes('modules-tab-outcomes')) {
  foot = foot.trimEnd() + '\n' + modulesJs + '\n';
}
fs.writeFileSync('tmp-old-home-footer-benefits-tabs.txt', foot);
console.log('foot', foot.length);
