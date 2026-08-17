(function(){
var out=document.getElementById("modules-tab-outcomes");
var plat=document.getElementById("modules-tab-platform");
var pOut=document.getElementById("modules-panel-outcomes");
var pPlat=document.getElementById("modules-panel-platform");
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
function show(which){
  var isOut=which==="outcomes";
  if(out)out.setAttribute("aria-selected",isOut?"true":"false");
  if(plat)plat.setAttribute("aria-selected",isOut?"false":"true");
  setPanel(pOut,isOut);
  setPanel(pPlat,!isOut);
}
if(out)out.addEventListener("click",function(e){e.preventDefault();show("outcomes");});
if(plat)plat.addEventListener("click",function(e){e.preventDefault();show("platform");});
show("outcomes");
})();
