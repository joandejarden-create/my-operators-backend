/**
 * Old Home hero — subtle full-bleed 3D globe background.
 * Thin orange square pins; CALA cascade → ease toward Europe → Europe cascade;
 * CALA pins stay visible; ultra-slow spin after.
 */
(function () {
  // Newer builds win — skip if a newer globe already booted (stale body scripts).
  var GLOBE_BUILD = 202607301;
  if (window.__ohGlobeBuild && window.__ohGlobeBuild >= GLOBE_BUILD) return;
  window.__ohGlobeBuild = GLOBE_BUILD;

  var TEX = "https://cdn.finsweet.com/files/globe/earthmap1k.jpg";
  var TEX_FALLBACK =
    "https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a691e4f3b0bf638b1052fc6_dealality-globe-texture.jpg";
  var THREE_SRC = "https://cdnjs.cloudflare.com/ajax/libs/three.js/r125/three.min.js";
  var PIN_ORANGE = 0xd78e2c;
  var PIN_ORANGE_CSS = "#D78E2C";

  /**
   * Y rotation facing Americas / Europe (camera at +Z).
   * Europe is east of CALA — rotation.y must DECREASE (right-to-left on screen)
   * to bring Europe toward the camera. +1.02 was the wrong hemisphere.
   */
  var ROT_AMERICAS = -0.58;
  var ROT_EUROPE = -2.16;

  /**
   * Real CALA hotel markets only — coords biased slightly inland so pins
   * read on land on the 1k equirectangular texture (tiny islands / coastline
   * tips otherwise look mid-ocean).
   * Order: north → south for cascade.
   */
  var LATAM_PINS = [
    // Mexico
    { lat: 23.15, lon: -109.72, name: "Los Cabos" },
    { lat: 20.68, lon: -103.35, name: "Guadalajara" },
    { lat: 25.69, lon: -100.32, name: "Monterrey" },
    { lat: 19.43, lon: -99.13, name: "Mexico City" },
    { lat: 20.98, lon: -89.62, name: "Mérida" },
    { lat: 21.05, lon: -86.95, name: "Cancún" },
    { lat: 20.63, lon: -87.12, name: "Playa del Carmen" },
    // Greater Caribbean (larger landmasses only)
    { lat: 23.12, lon: -82.38, name: "Havana" },
    { lat: 18.42, lon: -69.95, name: "Santo Domingo" },
    { lat: 18.56, lon: -68.55, name: "Punta Cana" },
    { lat: 18.38, lon: -66.12, name: "San Juan" },
    { lat: 18.02, lon: -76.82, name: "Kingston" },
    // Central America
    { lat: 14.63, lon: -90.51, name: "Guatemala City" },
    { lat: 13.70, lon: -89.22, name: "San Salvador" },
    { lat: 14.08, lon: -87.21, name: "Tegucigalpa" },
    { lat: 12.14, lon: -86.25, name: "Managua" },
    { lat: 9.95, lon: -84.12, name: "San José" },
    { lat: 9.00, lon: -79.52, name: "Panama City" },
    // Northern South America
    { lat: 10.42, lon: -66.90, name: "Caracas" },
    { lat: 10.35, lon: -75.42, name: "Cartagena" },
    { lat: 6.25, lon: -75.57, name: "Medellín" },
    { lat: 4.71, lon: -74.07, name: "Bogotá" },
    { lat: -0.18, lon: -78.48, name: "Quito" },
    { lat: -2.18, lon: -79.88, name: "Guayaquil" },
    // Andean / Southern Cone / Brazil inland
    { lat: -12.05, lon: -76.98, name: "Lima" },
    { lat: -13.53, lon: -71.97, name: "Cusco" },
    { lat: -16.50, lon: -68.15, name: "La Paz" },
    { lat: -17.78, lon: -63.18, name: "Santa Cruz" },
    { lat: -25.28, lon: -57.58, name: "Asunción" },
    { lat: -15.79, lon: -47.88, name: "Brasília" },
    { lat: -23.55, lon: -46.63, name: "São Paulo" },
    { lat: -22.88, lon: -43.28, name: "Rio de Janeiro" },
    { lat: -30.03, lon: -51.23, name: "Porto Alegre" },
    { lat: -33.45, lon: -70.67, name: "Santiago" },
    { lat: -32.89, lon: -68.84, name: "Mendoza" },
    { lat: -31.42, lon: -64.19, name: "Córdoba" },
    { lat: -34.85, lon: -56.18, name: "Montevideo" },
    { lat: -34.60, lon: -58.42, name: "Buenos Aires" },
  ];

  /**
   * Major European hotel markets — inland-biased; west → east cascade
   * as the globe turns toward Europe.
   */
  var EUROPE_PINS = [
    { lat: 53.35, lon: -6.26, name: "Dublin" },
    { lat: 38.72, lon: -9.14, name: "Lisbon" },
    { lat: 51.51, lon: -0.12, name: "London" },
    { lat: 40.42, lon: -3.70, name: "Madrid" },
    { lat: 48.86, lon: 2.35, name: "Paris" },
    { lat: 41.39, lon: 2.17, name: "Barcelona" },
    { lat: 50.85, lon: 4.35, name: "Brussels" },
    { lat: 52.37, lon: 4.90, name: "Amsterdam" },
    { lat: 47.38, lon: 8.54, name: "Zurich" },
    { lat: 50.11, lon: 8.68, name: "Frankfurt" },
    { lat: 45.46, lon: 9.19, name: "Milan" },
    { lat: 48.14, lon: 11.58, name: "Munich" },
    { lat: 41.90, lon: 12.50, name: "Rome" },
    { lat: 52.52, lon: 13.40, name: "Berlin" },
    { lat: 50.08, lon: 14.44, name: "Prague" },
    { lat: 48.21, lon: 16.37, name: "Vienna" },
    { lat: 55.68, lon: 12.57, name: "Copenhagen" },
    { lat: 59.33, lon: 18.07, name: "Stockholm" },
    { lat: 37.98, lon: 23.73, name: "Athens" },
    { lat: 41.01, lon: 28.98, name: "Istanbul" },
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

  /**
   * Match Three.js SphereBufferGeometry UV + Finsweet earthmap1k alignment
   * (equivalent to lonHelper/latHelper fudge used by @finsweet/3dglobes).
   */
  function latLonToVec3(lat, lon, radius) {
    var latRad = (lat * Math.PI) / 180;
    var lonRad = (lon * Math.PI) / 180;
    var x = 0;
    var y = 0;
    var z = radius;
    var ax = latRad + Math.PI;
    var cosAx = Math.cos(ax);
    var sinAx = Math.sin(ax);
    var y1 = y * cosAx - z * sinAx;
    var z1 = y * sinAx + z * cosAx;
    y = y1;
    z = z1;
    var ay = lonRad + Math.PI * 1.5;
    var cosAy = Math.cos(ay);
    var sinAy = Math.sin(ay);
    var x2 = x * cosAy + z * sinAy;
    var z2 = -x * sinAy + z * cosAy;
    return new THREE.Vector3(x2, y, z2);
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
    globeRoot.rotation.y = ROT_AMERICAS;
    globeRoot.rotation.x = 0.06;
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
        tex.flipY = true;
        tex.anisotropy = Math.min(4, renderer.capabilities.getMaxAnisotropy());
        material.map = tex;
        material.color = new THREE.Color(0x9aa0ff);
        material.opacity = 0.32;
        material.needsUpdate = true;
      },
      undefined,
      function () {
        loader.load(TEX_FALLBACK, function (tex) {
          tex.flipY = true;
          material.map = tex;
          material.color = new THREE.Color(0x9aa0ff);
          material.opacity = 0.34;
          material.needsUpdate = true;
        });
      }
    );

    var pinTex = makePinTexture();
    var calaMarkers = [];
    var europeMarkers = [];
    var markers = [];

    function addPinGroup(list, bucket, startHidden) {
      for (var i = 0; i < list.length; i++) {
        var p = latLonToVec3(list[i].lat, list[i].lon, radius);
        var pin = makePin(p, pinTex);
        pin.userData.region = bucket;
        if (reduce && !startHidden) {
          var spr0 = pin.userData.sprite;
          var bs0 = pin.userData.baseScale;
          spr0.material.opacity = 0.95;
          spr0.scale.set(bs0, bs0, 1);
          pin.userData.revealed = true;
          pin.userData.revealAt = 0;
        } else {
          pin.userData.revealed = false;
          pin.userData.revealAt = Infinity;
          var sprH = pin.userData.sprite;
          if (sprH) {
            sprH.material.opacity = 0;
            sprH.scale.set(0.001, 0.001, 1);
          }
        }
        globeRoot.add(pin);
        markers.push(pin);
        if (bucket === "cala") calaMarkers.push(pin);
        else europeMarkers.push(pin);
      }
    }

    addPinGroup(LATAM_PINS, "cala", false);
    addPinGroup(EUROPE_PINS, "europe", true);

    // Cascade starts when hero is on screen so the stagger is actually visible.
    var revealStarted = reduce;
    var europeStarted = reduce;
    var revealDur = reduce ? 1 : 1100;
    var revealStagger = reduce ? 0 : 950;
    var revealLead = reduce ? 0 : 700;
    var turnPauseMs = 1600;
    var turnDurMs = 6200;
    var europeLeadMs = 900;

    var phase = reduce ? "idle" : "waiting"; // waiting | cala | turning | europe | idle
    var turnFromY = ROT_AMERICAS;
    var turnToY = ROT_EUROPE;
    var turnStartAt = 0;

    function scheduleCascade(group, startAt) {
      for (var i = 0; i < group.length; i++) {
        group[i].userData.revealAt = startAt + i * revealStagger;
        group[i].userData.revealed = false;
        var spr = group[i].userData.sprite;
        if (spr) {
          spr.material.opacity = 0;
          spr.scale.set(0.001, 0.001, 1);
        }
      }
    }

    function startPinCascade() {
      if (revealStarted || reduce) return;
      revealStarted = true;
      phase = "cala";
      var start = performance.now() + revealLead;
      scheduleCascade(calaMarkers, start);
      // After last CALA pin settles, ease toward Europe (CALA pins stay).
      var calaDoneAt =
        start +
        Math.max(0, calaMarkers.length - 1) * revealStagger +
        revealDur +
        turnPauseMs;
      turnStartAt = calaDoneAt;
      turnFromY = globeRoot.rotation.y;
      turnToY = ROT_EUROPE;
    }

    function startEuropeCascade(now) {
      if (europeStarted || reduce) return;
      europeStarted = true;
      phase = "europe";
      scheduleCascade(europeMarkers, now + europeLeadMs);
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

    var spin = reduce ? 0 : -0.00002;
    var t0 = performance.now();

    function easeInOutCubic(t) {
      return t < 0.5 ? 4 * t * t * t : 1 - Math.pow(-2 * t + 2, 3) / 2;
    }

    function easeOutBack(t) {
      var c1 = 1.70158;
      var c3 = c1 + 1;
      return 1 + c3 * Math.pow(t - 1, 3) + c1 * Math.pow(t - 1, 2);
    }

    function frame(now) {
      raf = requestAnimationFrame(frame);

      if (phase === "cala" && now >= turnStartAt) {
        phase = "turning";
        turnFromY = globeRoot.rotation.y;
        turnStartAt = now;
      }

      if (phase === "turning") {
        var tt = Math.min(1, (now - turnStartAt) / turnDurMs);
        var te = easeInOutCubic(tt);
        globeRoot.rotation.y = turnFromY + (turnToY - turnFromY) * te;
        // Start Europe cascade once Europe is mostly facing camera.
        if (tt >= 0.62) startEuropeCascade(now);
        if (tt >= 1) {
          globeRoot.rotation.y = turnToY;
          phase = europeStarted ? "europe" : "idle";
          if (!europeStarted) startEuropeCascade(now);
        }
      } else if (spin && (phase === "idle" || phase === "europe" || phase === "waiting")) {
        // Tiny drift only after the intentional Americas→Europe turn.
        if (phase === "europe" || phase === "idle") {
          globeRoot.rotation.y += spin;
        }
      }

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
