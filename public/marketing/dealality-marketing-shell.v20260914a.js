/* Minimal mobile nav toggle for static Dealality shell previews */
(function () {
  function bind() {
    var btn = document.getElementById("nmenu");
    var menu = document.getElementById("mnav");
    if (!btn || !menu || btn.getAttribute("data-oh-shell-bound") === "1") return;
    btn.setAttribute("data-oh-shell-bound", "1");
    btn.addEventListener("click", function (e) {
      e.preventDefault();
      var open = menu.classList.toggle("is-open");
      btn.setAttribute("aria-expanded", open ? "true" : "false");
    });
  }
  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", bind);
  } else {
    bind();
  }
})();
