/**
 * Old Home hero — subtle full-bleed 3D globe background.
 * Thin orange square pins; staggered appear; ultra-slow spin; Americas-first.
 */
(function () {
  // Newer builds win — skip if a newer globe already booted (stale body scripts).
  var GLOBE_BUILD = 202607298;
  if (window.__ohGlobeBuild && window.__ohGlobeBuild >= GLOBE_BUILD) return;
  window.__ohGlobeBuild = GLOBE_BUILD;

  var TEX = "https://cdn.finsweet.com/files/globe/earthmap1k.jpg";
  var TEX_FALLBACK =
    "https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a691e4f3b0bf638b1052fc6_dealality-globe-texture.jpg";
  var THREE_SRC = "https://cdnjs.cloudflare.com/ajax/libs/three.js/r125/three.min.js";
  var PIN_ORANGE = 0xd78e2c;
  var PIN_ORANGE_CSS = "#D78E2C";

  // Latin America + Caribbean market markers (N→S cascade order)
  var LATAM_PINS = [
    { lat: 21.1619, lon: -86.8515, name: "Cancún" },
    { lat: 20.9674, lon: -89.5926, name: "Mérida" },
    { lat: 19.4326, lon: -99.1332, name: "Mexico City" },
    { lat: 20.6597, lon: -103.3496, name: "Guadalajara" },
    { lat: 23.1136, lon: -82.3666, name: "Havana" },
    { lat: 18.4861, lon: -69.9312, name: "Santo Domingo" },
    { lat: 18.4655, lon: -66.1057, name: "San Juan" },
    { lat: 18.1096, lon: -77.2975, name: "Kingston" },
    { lat: 13.1132, lon: -59.5988, name: "Bridgetown" },
    { lat: 12.1224, lon: -68.8824, name: "Willemstad" },
    { lat: 9.9281, lon: -84.0907, name: "San José" },
    { lat: 8.9824, lon: -79.5199, name: "Panama City" },
    { lat: 10.4806, lon: -66.9036, name: "Caracas" },
    { lat: 10.391, lon: -75.4794, name: "Cartagena" },
    { lat: 6.2476, lon: -75.5658, name: "Medellín" },
    { lat: 4.711, lon: -74.0721, name: "Bogotá" },
    { lat: -0.1807, lon: -78.4678, name: "Quito" },
    { lat: -2.1709, lon: -79.9224, name: "Guayaquil" },
    { lat: -12.0464, lon: -77.0428, name: "Lima" },
    { lat: -13.5319, lon: -71.9675, name: "Cusco" },
    { lat: -16.4897, lon: -68.1193, name: "La Paz" },
    { lat: -25.2637, lon: -57.5759, name: "Asunción" },
    { lat: -15.7942, lon: -47.8822, name: "Brasília" },
    { lat: -12.9777, lon: -38.5016, name: "Salvador" },
    { lat: -8.0476, lon: -34.877, name: "Recife" },
    { lat: -23.5505, lon: -46.6333, name: "São Paulo" },
    { lat: -22.9068, lon: -43.1729, name: "Rio de Janeiro" },
    { lat: -27.5954, lon: -48.548, name: "Florianópolis" },
    { lat: -33.4489, lon: -70.6693, name: "Santiago" },
    { lat: -32.8895, lon: -68.8458, name: "Mendoza" },
    { lat: -34.9011, lon: -56.1645, name: "Montevideo" },
    { lat: -34.6037, lon: -58.3816, name: "Buenos Aires" },
  ];

  var root = document.getElementById("hero-globe");
  var host = document.getElementById("hero-globe-container");
  var list = document.getElementById("hero-globe-list");
  if (!root || !host) return;

  var reduce =
    window.matchMedia &&
    window.matchMedia("(prefers-reduced-motion: reduce)").matches;
  var mobile =
    window.matchMedia && window.matchMedia("(max-width:700px)").matches;

  root.setAttribute("aria-hidden", "true");
  root.classList.add("oh-globe-bg");

  var booted = false;
  var raf = 0;
  var teardown = null;

  function loadScript(src) {
    return new Promise(function (resolve, reject) {
      if (window.THREE) {
        resolve();
        return;
      }
      var s = document.createElement("script");
      s.src = src;
      s.async = true;
      s.onload = function () {
        resolve();
      };
      s.onerror = function () {
        reject(new Error(src));
      };
      document.head.appendChild(s);
    });
  }

  function latLonToVec3(lat, lon, radius) {
    var phi = ((90 - lat) * Math.PI) / 180;
    var theta = ((lon + 180) * Math.PI) / 180;
    return new THREE.Vector3(
      -radius * Math.sin(phi) * Math.cos(theta),
      radius * Math.cos(phi),
      radius * Math.sin(phi) * Math.sin(theta)
    );
  }

  /** Nested square pin: thin outer stroke + small inner fill (Dealality orange). */
  function makePinTexture() {
    var size = 128;
    var c = document.createElement("canvas");
    c.width = size;
    c.height = size;
    var ctx = c.getContext("2d");
    ctx.clearRect(0, 0, size, size);

    ctx.strokeStyle = PIN_ORANGE_CSS;
    ctx.lineWidth = 4;
    ctx.lineJoin = "miter";
    ctx.strokeRect(28, 28, 72, 72);

    ctx.fillStyle = PIN_ORANGE_CSS;
    ctx.fillRect(52, 52, 24, 24);

    var tex = new THREE.CanvasTexture(c);
    tex.needsUpdate = true;
    return tex;
  }

  function makePin(pos, sharedTexture) {
    var group = new THREE.Group();
    group.position.copy(pos.clone().normalize().multiplyScalar(pos.length() * 1.018));

    var mat = new THREE.SpriteMaterial({
      map: sharedTexture,
      transparent: true,
      depthWrite: false,
      depthTest: true,
      opacity: 0,
      sizeAttenuation: true,
    });
    var sprite = new THREE.Sprite(mat);
    var scale = mobile ? 0.038 : 0.032;
    sprite.scale.set(0.001, 0.001, 1);
    group.add(sprite);
    group.userData.sprite = sprite;
    group.userData.baseScale = scale;
    group.userData.revealed = false;
    group.userData.revealAt = Infinity;
    return group;
  }

  function boot() {
    if (booted) return;
    if (!window.THREE) return;
    if (window.__ohGlobeBuild !== GLOBE_BUILD) return;
    booted = true;

    var pins = LATAM_PINS.slice();

    while (host.firstChild) host.removeChild(host.firstChild);
    if (list) list.style.display = "none";

    var canvas = document.createElement("canvas");
    canvas.id = "oh-globe-canvas";
    canvas.setAttribute("aria-hidden", "true");
    host.appendChild(canvas);

    var renderer = new THREE.WebGLRenderer({
      canvas: canvas,
      alpha: true,
      antialias: !mobile,
      powerPreference: "low-power",
      preserveDrawingBuffer: true,
    });
    renderer.setClearColor(0x000000, 0);
    renderer.setPixelRatio(Math.min(window.devicePixelRatio || 1, mobile ? 1.25 : 1.75));

    var scene = new THREE.Scene();
    var camera = new THREE.PerspectiveCamera(45, 1, 0.1, 20);
    camera.position.set(-0.42, 0.1, 1.85);

    var globeRoot = new THREE.Group();
    globeRoot.rotation.y = -0.58;
    globeRoot.rotation.x = 0.1;
    globeRoot.position.x = 0.38;
    scene.add(globeRoot);

    var radius = 1;
    var sphereGeo = new THREE.SphereBufferGeometry(radius, 64, 48);
    var material = new THREE.MeshBasicMaterial({
      color: 0x6c72ff,
      transparent: true,
      opacity: 0.18,
    });
    var earth = new THREE.Mesh(sphereGeo, material);
    globeRoot.add(earth);

    var loader = new THREE.TextureLoader();
    loader.load(
      TEX,
      function (tex) {
        tex.anisotropy = Math.min(4, renderer.capabilities.getMaxAnisotropy());
        material.map = tex;
        material.color = new THREE.Color(0x9aa0ff);
        material.opacity = 0.32;
        material.needsUpdate = true;
      },
      undefined,
      function () {
        loader.load(TEX_FALLBACK, function (tex) {
          material.map = tex;
          material.color = new THREE.Color(0x9aa0ff);
          material.opacity = 0.34;
          material.needsUpdate = true;
        });
      }
    );

    var pinTex = makePinTexture();
    var markers = [];
    for (var i = 0; i < pins.length; i++) {
      var p = latLonToVec3(pins[i].lat, pins[i].lon, radius);
      var pin = makePin(p, pinTex);
      if (reduce) {
        var spr0 = pin.userData.sprite;
        var bs0 = pin.userData.baseScale;
        spr0.material.opacity = 0.95;
        spr0.scale.set(bs0, bs0, 1);
        pin.userData.revealed = true;
        pin.userData.revealAt = 0;
      }
      globeRoot.add(pin);
      markers.push(pin);
    }

    // Cascade starts when hero is on screen so the stagger is actually visible.
    var revealStarted = reduce;
    var revealDur = reduce ? 1 : 1100;
    var revealStagger = reduce ? 0 : 950;
    var revealLead = reduce ? 0 : 700;

    function startPinCascade() {
      if (revealStarted || reduce) return;
      revealStarted = true;
      var start = performance.now() + revealLead;
      for (var i = 0; i < markers.length; i++) {
        markers[i].userData.revealAt = start + i * revealStagger;
        markers[i].userData.revealed = false;
        var spr = markers[i].userData.sprite;
        if (spr) {
          spr.material.opacity = 0;
          spr.scale.set(0.001, 0.001, 1);
        }
      }
    }

    function watchHeroVisibility() {
      var hero = document.getElementById("hero") || root;
      if (!hero || typeof IntersectionObserver === "undefined") {
        startPinCascade();
        return;
      }
      var io = new IntersectionObserver(
        function (entries) {
          for (var e = 0; e < entries.length; e++) {
            if (entries[e].isIntersecting && entries[e].intersectionRatio > 0.2) {
              startPinCascade();
              io.disconnect();
              break;
            }
          }
        },
        { threshold: [0.2, 0.35] }
      );
      io.observe(hero);
      // Fallback if already in view / observer never fires
      setTimeout(function () {
        var rect = hero.getBoundingClientRect();
        var vh = window.innerHeight || 1;
        if (rect.top < vh * 0.85 && rect.bottom > vh * 0.15) {
          startPinCascade();
          try {
            io.disconnect();
          } catch (_e) {}
        }
      }, 900);
    }

    function resize() {
      var w = host.clientWidth || root.clientWidth || 1;
      var h = host.clientHeight || root.clientHeight || 1;
      renderer.setSize(w, h, false);
      camera.aspect = w / h;
      camera.updateProjectionMatrix();
    }
    resize();
    window.addEventListener("resize", resize);

    var spin = reduce ? 0 : 0.00002;
    var t0 = performance.now();

    function easeOutBack(t) {
      var c1 = 1.70158;
      var c3 = c1 + 1;
      return 1 + c3 * Math.pow(t - 1, 3) + c1 * Math.pow(t - 1, 2);
    }

    function frame(now) {
      raf = requestAnimationFrame(frame);
      if (spin) globeRoot.rotation.y += spin;
      var pulse = reduce
        ? 0.95
        : 0.78 + 0.2 * Math.sin((now - t0) * 0.0016);
      for (var m = 0; m < markers.length; m++) {
        var marker = markers[m];
        var spr = marker.userData.sprite;
        if (!spr || !spr.material) continue;
        var base = marker.userData.baseScale || 0.032;
        if (marker.userData.revealed) {
          spr.material.opacity = pulse;
          continue;
        }
        var elapsed = now - marker.userData.revealAt;
        if (!isFinite(elapsed) || elapsed < 0) {
          spr.material.opacity = 0;
          spr.scale.set(0.001, 0.001, 1);
          continue;
        }
        var t = Math.min(1, elapsed / revealDur);
        var e = easeOutBack(t);
        var s = base * Math.max(0.001, Math.min(1.35, e));
        spr.scale.set(s, s, 1);
        spr.material.opacity = Math.min(1, t * 1.25) * Math.max(pulse, 0.85);
        if (t >= 1) {
          marker.userData.revealed = true;
          spr.scale.set(base, base, 1);
        }
      }
      camera.lookAt(0.22, 0.04, 0);
      renderer.render(scene, camera);
    }
    raf = requestAnimationFrame(frame);

    function onVisibility() {
      if (document.hidden) {
        if (raf) cancelAnimationFrame(raf);
        raf = 0;
      } else if (!raf) {
        raf = requestAnimationFrame(frame);
      }
    }
    document.addEventListener("visibilitychange", onVisibility);

    teardown = function () {
      if (raf) cancelAnimationFrame(raf);
      raf = 0;
      window.removeEventListener("resize", resize);
      document.removeEventListener("visibilitychange", onVisibility);
    };

    if (!reduce) watchHeroVisibility();
  }

  loadScript(THREE_SRC)
    .then(boot)
    .catch(function (err) {
      if (typeof console !== "undefined" && console.warn) {
        console.warn("[oh-globe-bg]", err);
      }
    });

  // Reclaim once if a stale body script wiped the canvas before our boot finished.
  window.addEventListener("load", function () {
    if (window.__ohGlobeBuild !== GLOBE_BUILD) return;
    if (booted) return;
    if (!window.THREE) return;
    try {
      boot();
    } catch (err) {
      if (typeof console !== "undefined" && console.warn) {
        console.warn("[oh-globe-bg] reclaim", err);
      }
    }
  });
})();
