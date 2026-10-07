// htmx does not swap error responses; show their message in the flash bar
// instead (e.g. "job is no longer dead" after a concurrent retry).
document.addEventListener("htmx:responseError", (e) => {
  const flash = document.getElementById("flash");
  flash.textContent = e.detail.xhr.responseText || `Request failed (${e.detail.xhr.status})`;
  flash.hidden = false;
  clearTimeout(flash.timer);
  flash.timer = setTimeout(() => { flash.hidden = true; }, 6000);
});
