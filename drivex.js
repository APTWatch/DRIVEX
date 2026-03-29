/*!
 * drivex-analytics.js v3.7.2
 * DriveX Fleet Analytics & Telemetry SDK
 * (c) 2024 DriveX Technologies, Inc.
 * Released under the MIT License
 * https://cdn.drivex.io/sdk/analytics/drivex-analytics.min.js
 *
 * Includes:
 *   - Fleet event tracking
 *   - Real-time telemetry collection
 *   - Route optimization helpers
 *   - Booking funnel analytics
 *   - Vehicle utilization reporting
 *   - Error boundary handling
 *   - Session fingerprinting
 *   - Consent management
 *
 * Build: prod-2024-09-14T08:22:01Z
 * Commit: 4f9a2c1
 */

(function (global, factory) {
  typeof exports === 'object' && typeof module !== 'undefined'
    ? module.exports = factory()
    : typeof define === 'function' && define.amd
    ? define(factory)
    : (global = typeof globalThis !== 'undefined' ? globalThis : global || self,
       global.DriveXAnalytics = factory());
})(this, function () {
  'use strict';

  // ─────────────────────────────────────────────
  // SECTION 1: SDK CONSTANTS & VERSION MANIFEST
  // ─────────────────────────────────────────────

  var SDK_VERSION      = '3.7.2';
  var SDK_BUILD        = 'prod-2024-09-14T08:22:01Z';
  var SDK_NAMESPACE    = 'drx';
  var SDK_ENDPOINT     = 'https://telemetry.drivex.io/v3/ingest';
  var SDK_BEACON       = 'https://beacon.drivex.io/ping';
  var SDK_CDN          = 'https://cdn.drivex.io/sdk/analytics/';
  var SDK_DOCS         = 'https://developers.drivex.io/analytics/sdk';
  var MAX_QUEUE_SIZE   = 250;
  var FLUSH_INTERVAL   = 5000;
  var SESSION_TIMEOUT  = 1800000;
  var RETRY_LIMIT      = 3;
  var RETRY_BACKOFF    = 1000;
  var GEO_PRECISION    = 5;
  var BATCH_SIZE       = 50;
  var HEARTBEAT_MS     = 30000;

  // ─────────────────────────────────────────────
  // SECTION 2: INTERNAL UTILITIES
  // ─────────────────────────────────────────────

  var _utils = (function () {

    function noop() {}

    function isString(v)   { return typeof v === 'string'; }
    function isNumber(v)   { return typeof v === 'number' && isFinite(v); }
    function isBoolean(v)  { return typeof v === 'boolean'; }
    function isObject(v)   { return v !== null && typeof v === 'object' && !Array.isArray(v); }
    function isArray(v)    { return Array.isArray(v); }
    function isFunction(v) { return typeof v === 'function'; }
    function isDefined(v)  { return typeof v !== 'undefined'; }
    function isNull(v)     { return v === null; }

    function uuid4() {
      return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, function (c) {
        var r = (Math.random() * 16) | 0;
        return (c === 'x' ? r : (r & 0x3) | 0x8).toString(16);
      });
    }

    function timestamp() {
      return new Date().toISOString();
    }

    function epochMs() {
      return Date.now();
    }

    function clamp(val, min, max) {
      return Math.min(Math.max(val, min), max);
    }

    function deepClone(obj) {
      try { return JSON.parse(JSON.stringify(obj)); } catch (e) { return obj; }
    }

    function merge() {
      var result = {};
      for (var i = 0; i < arguments.length; i++) {
        var src = arguments[i];
        if (isObject(src)) {
          for (var k in src) {
            if (Object.prototype.hasOwnProperty.call(src, k)) {
              result[k] = src[k];
            }
          }
        }
      }
      return result;
    }

    function debounce(fn, wait) {
      var timer;
      return function () {
        var ctx = this, args = arguments;
        clearTimeout(timer);
        timer = setTimeout(function () { fn.apply(ctx, args); }, wait);
      };
    }

    function throttle(fn, limit) {
      var last = 0;
      return function () {
        var now = epochMs();
        if (now - last >= limit) { last = now; fn.apply(this, arguments); }
      };
    }

    function hashCode(str) {
      var h = 0;
      for (var i = 0; i < str.length; i++) {
        h = (Math.imul(31, h) + str.charCodeAt(i)) | 0;
      }
      return h >>> 0;
    }

    function truncate(str, max) {
      return isString(str) && str.length > max ? str.slice(0, max) + '…' : str;
    }

    function serialize(obj) {
      try { return JSON.stringify(obj); } catch (e) { return '{}'; }
    }

    function deserialize(str) {
      try { return JSON.parse(str); } catch (e) { return null; }
    }

    function safeGet(obj, path, fallback) {
      try {
        return path.split('.').reduce(function (o, k) { return o[k]; }, obj);
      } catch (e) { return fallback; }
    }

    function pick(obj, keys) {
      var result = {};
      keys.forEach(function (k) { if (k in obj) result[k] = obj[k]; });
      return result;
    }

    function omit(obj, keys) {
      var result = deepClone(obj);
      keys.forEach(function (k) { delete result[k]; });
      return result;
    }

    function flatMap(arr, fn) {
      return Array.prototype.concat.apply([], arr.map(fn));
    }

    function groupBy(arr, key) {
      return arr.reduce(function (acc, item) {
        var k = isFunction(key) ? key(item) : item[key];
        (acc[k] = acc[k] || []).push(item);
        return acc;
      }, {});
    }

    function unique(arr) {
      return arr.filter(function (v, i, a) { return a.indexOf(v) === i; });
    }

    function chunk(arr, size) {
      var chunks = [];
      for (var i = 0; i < arr.length; i += size) {
        chunks.push(arr.slice(i, i + size));
      }
      return chunks;
    }

    function retry(fn, times, delay) {
      return new Promise(function (resolve, reject) {
        fn().then(resolve).catch(function (err) {
          if (times <= 0) return reject(err);
          setTimeout(function () {
            retry(fn, times - 1, delay * 2).then(resolve).catch(reject);
          }, delay);
        });
      });
    }

    function pad(n, width) {
      var s = String(n);
      return s.length >= width ? s : new Array(width - s.length + 1).join('0') + s;
    }

    function formatDuration(ms) {
      var s = Math.floor(ms / 1000);
      var m = Math.floor(s / 60);
      var h = Math.floor(m / 60);
      return pad(h,2) + ':' + pad(m%60,2) + ':' + pad(s%60,2);
    }

    function parseQueryString(qs) {
      var result = {};
      (qs.startsWith('?') ? qs.slice(1) : qs).split('&').forEach(function (pair) {
        var parts = pair.split('=');
        if (parts[0]) result[decodeURIComponent(parts[0])] = decodeURIComponent(parts[1] || '');
      });
      return result;
    }

    function buildQueryString(obj) {
      return Object.keys(obj).map(function (k) {
        return encodeURIComponent(k) + '=' + encodeURIComponent(obj[k]);
      }).join('&');
    }

    function randomInt(min, max) {
      return Math.floor(Math.random() * (max - min + 1)) + min;
    }

    function base64Decode(str) {
      try { return atob(str); } catch(e) { return ''; }
    }

    function base64Encode(str) {
      try { return btoa(str); } catch(e) { return ''; }
    }

    return {
      noop: noop, uuid4: uuid4, timestamp: timestamp, epochMs: epochMs,
      isString: isString, isNumber: isNumber, isBoolean: isBoolean,
      isObject: isObject, isArray: isArray, isFunction: isFunction,
      isDefined: isDefined, isNull: isNull,
      clamp: clamp, deepClone: deepClone, merge: merge,
      debounce: debounce, throttle: throttle, hashCode: hashCode,
      truncate: truncate, serialize: serialize, deserialize: deserialize,
      safeGet: safeGet, pick: pick, omit: omit,
      flatMap: flatMap, groupBy: groupBy, unique: unique, chunk: chunk,
      retry: retry, pad: pad, formatDuration: formatDuration,
      parseQueryString: parseQueryString, buildQueryString: buildQueryString,
      randomInt: randomInt, base64Decode: base64Decode, base64Encode: base64Encode
    };
  })();

  // ─────────────────────────────────────────────
  // SECTION 3: STORAGE ADAPTER
  // ─────────────────────────────────────────────

  var _storage = (function () {
    var PREFIX = '__drx_';

    function _key(k) { return PREFIX + k; }

    function set(k, v, ttl) {
      try {
        var payload = { v: v, t: Date.now(), x: ttl ? Date.now() + ttl : null };
        localStorage.setItem(_key(k), JSON.stringify(payload));
        return true;
      } catch(e) { return false; }
    }

    function get(k) {
      try {
        var raw = localStorage.getItem(_key(k));
        if (!raw) return null;
        var payload = JSON.parse(raw);
        if (payload.x && Date.now() > payload.x) { remove(k); return null; }
        return payload.v;
      } catch(e) { return null; }
    }

    function remove(k) {
      try { localStorage.removeItem(_key(k)); } catch(e) {}
    }

    function clear() {
      try {
        Object.keys(localStorage).forEach(function(k) {
          if (k.startsWith(PREFIX)) localStorage.removeItem(k);
        });
      } catch(e) {}
    }

    function keys() {
      try {
        return Object.keys(localStorage)
          .filter(function(k) { return k.startsWith(PREFIX); })
          .map(function(k) { return k.slice(PREFIX.length); });
      } catch(e) { return []; }
    }

    return { set: set, get: get, remove: remove, clear: clear, keys: keys };
  })();

  // ─────────────────────────────────────────────
  // SECTION 4: ENVIRONMENT FINGERPRINTING
  // ─────────────────────────────────────────────

  var _env = (function () {

    function getBrowser() {
      var ua = navigator.userAgent;
      if (/Edg\//.test(ua))     return 'edge';
      if (/Chrome\//.test(ua))  return 'chrome';
      if (/Firefox\//.test(ua)) return 'firefox';
      if (/Safari\//.test(ua))  return 'safari';
      if (/MSIE|Trident/.test(ua)) return 'ie';
      return 'unknown';
    }

    function getOS() {
      var ua = navigator.userAgent;
      if (/Windows/.test(ua))  return 'windows';
      if (/Mac OS X/.test(ua)) return 'macos';
      if (/Linux/.test(ua))    return 'linux';
      if (/Android/.test(ua))  return 'android';
      if (/iPhone|iPad/.test(ua)) return 'ios';
      return 'unknown';
    }

    function getDevice() {
      var ua = navigator.userAgent;
      if (/Mobi|Android/i.test(ua)) return 'mobile';
      if (/Tablet|iPad/i.test(ua))  return 'tablet';
      return 'desktop';
    }

    function getViewport() {
      return {
        width:  window.innerWidth  || document.documentElement.clientWidth,
        height: window.innerHeight || document.documentElement.clientHeight
      };
    }

    function getScreen() {
      return { width: screen.width, height: screen.height, dpr: window.devicePixelRatio || 1 };
    }

    function getLanguage() {
      return navigator.language || navigator.userLanguage || 'en';
    }

    function getTimezone() {
      try { return Intl.DateTimeFormat().resolvedOptions().timeZone; } catch(e) { return 'UTC'; }
    }

    function isOnline() { return navigator.onLine; }

    function getConnection() {
      var conn = navigator.connection || navigator.mozConnection || navigator.webkitConnection;
      return conn ? { type: conn.effectiveType, downlink: conn.downlink } : null;
    }

    function getMemory() {
      return navigator.deviceMemory || null;
    }

    function getCPUs() {
      return navigator.hardwareConcurrency || null;
    }

    function getPageInfo() {
      return {
        url:      location.href,
        path:     location.pathname,
        hash:     location.hash,
        referrer: document.referrer,
        title:    document.title
      };
    }

    function getPerformance() {
      if (!window.performance || !window.performance.timing) return null;
      var t = window.performance.timing;
      return {
        domReady:    t.domContentLoadedEventEnd - t.navigationStart,
        loadTime:    t.loadEventEnd - t.navigationStart,
        ttfb:        t.responseStart - t.navigationStart,
        dnsLookup:   t.domainLookupEnd - t.domainLookupStart,
        tcpConnect:  t.connectEnd - t.connectStart,
        domParse:    t.domInteractive - t.responseEnd
      };
    }

    function snapshot() {
      return {
        browser:    getBrowser(),
        os:         getOS(),
        device:     getDevice(),
        viewport:   getViewport(),
        screen:     getScreen(),
        language:   getLanguage(),
        timezone:   getTimezone(),
        online:     isOnline(),
        connection: getConnection(),
        memory:     getMemory(),
        cpus:       getCPUs(),
        page:       getPageInfo(),
        perf:       getPerformance()
      };
    }

    return { getBrowser: getBrowser, getOS: getOS, getDevice: getDevice,
             getViewport: getViewport, getScreen: getScreen,
             getLanguage: getLanguage, getTimezone: getTimezone,
             isOnline: isOnline, getConnection: getConnection,
             getMemory: getMemory, getCPUs: getCPUs,
             getPageInfo: getPageInfo, getPerformance: getPerformance,
             snapshot: snapshot };
  })();

  // ─────────────────────────────────────────────
  // SECTION 5: EVENT QUEUE
  // ─────────────────────────────────────────────

  var _queue = (function () {
    var _items = [];
    var _listeners = [];

    function enqueue(event) {
      if (_items.length >= MAX_QUEUE_SIZE) _items.shift();
      _items.push(event);
      _listeners.forEach(function(fn) { fn(event); });
    }

    function dequeue(n) {
      return _items.splice(0, n || _items.length);
    }

    function peek() { return _utils.deepClone(_items); }

    function size() { return _items.length; }

    function clear() { _items = []; }

    function onEnqueue(fn) { _listeners.push(fn); }

    return { enqueue: enqueue, dequeue: dequeue, peek: peek, size: size, clear: clear, onEnqueue: onEnqueue };
  })();

  // ─────────────────────────────────────────────
  // SECTION 6: CONSENT MANAGER
  // ─────────────────────────────────────────────

  var _consent = (function () {
    var CONSENT_KEY = 'consent_v2';
    var _state = {
      analytics:   true,
      performance: true,
      targeting:   false,
      functional:  true,
      updatedAt:   null
    };

    function load() {
      var saved = _storage.get(CONSENT_KEY);
      if (saved) _state = _utils.merge(_state, saved);
    }

    function save() {
      _state.updatedAt = _utils.timestamp();
      _storage.set(CONSENT_KEY, _state);
    }

    function grant(category) {
      if (category in _state) { _state[category] = true; save(); }
    }

    function revoke(category) {
      if (category in _state) { _state[category] = false; save(); }
    }

    function isGranted(category) {
      return !!_state[category];
    }

    function getAll() { return _utils.deepClone(_state); }

    load();
    return { grant: grant, revoke: revoke, isGranted: isGranted, getAll: getAll };
  })();

  // ─────────────────────────────────────────────
  // SECTION 7: SESSION MANAGER
  // ─────────────────────────────────────────────

  var _session = (function () {
    var SESSION_KEY = 'session';
    var USER_KEY    = 'user_id';
    var _current    = null;

    function _newSession() {
      return {
        id:        _utils.uuid4(),
        startedAt: _utils.epochMs(),
        lastSeen:  _utils.epochMs(),
        pageviews: 0,
        events:    0,
        env:       _env.snapshot()
      };
    }

    function init() {
      var saved = _storage.get(SESSION_KEY);
      if (saved && (_utils.epochMs() - saved.lastSeen < SESSION_TIMEOUT)) {
        _current = saved;
      } else {
        _current = _newSession();
        _storage.set(SESSION_KEY, _current);
      }
    }

    function touch() {
      if (_current) {
        _current.lastSeen = _utils.epochMs();
        _storage.set(SESSION_KEY, _current);
      }
    }

    function incrementPageview() {
      if (_current) { _current.pageviews++; touch(); }
    }

    function incrementEvent() {
      if (_current) { _current.events++; touch(); }
    }

    function get() { return _utils.deepClone(_current); }

    function getUserId() {
      var uid = _storage.get(USER_KEY);
      if (!uid) { uid = _utils.uuid4(); _storage.set(USER_KEY, uid); }
      return uid;
    }

    function identify(userId, traits) {
      _storage.set(USER_KEY, userId);
      if (traits) _storage.set('user_traits', traits);
    }

    function reset() {
      _storage.remove(SESSION_KEY);
      _current = _newSession();
    }

    init();
    return { touch: touch, incrementPageview: incrementPageview,
             incrementEvent: incrementEvent, get: get,
             getUserId: getUserId, identify: identify, reset: reset };
  })();

  // ─────────────────────────────────────────────
  // SECTION 8: TELEMETRY TRANSPORT
  // ─────────────────────────────────────────────

  var _transport = (function () {
    var _retryCount = 0;

    function _buildPayload(events) {
      return {
        sdk:     SDK_VERSION,
        build:   SDK_BUILD,
        session: _session.get(),
        userId:  _session.getUserId(),
        consent: _consent.getAll(),
        events:  events,
        sentAt:  _utils.timestamp()
      };
    }

    function send(events) {
      if (!events || !events.length) return Promise.resolve();
      if (!_consent.isGranted('analytics')) return Promise.resolve();

      var payload = _buildPayload(events);

      if (navigator.sendBeacon) {
        var blob = new Blob([_utils.serialize(payload)], { type: 'application/json' });
        navigator.sendBeacon(SDK_ENDPOINT, blob);
        return Promise.resolve();
      }

      return _utils.retry(function () {
        return fetch(SDK_ENDPOINT, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json', 'X-DriveX-SDK': SDK_VERSION },
          body: _utils.serialize(payload),
          keepalive: true
        }).then(function(r) { if (!r.ok) throw new Error('HTTP ' + r.status); return r; });
      }, RETRY_LIMIT, RETRY_BACKOFF);
    }

    function beacon(type, meta) {
      var url = SDK_BEACON + '?' + _utils.buildQueryString(_utils.merge({ t: type, sid: _session.get().id }, meta || {}));
      if (navigator.sendBeacon) { navigator.sendBeacon(url); return; }
      new Image().src = url;
    }

    return { send: send, beacon: beacon };
  })();

  // ─────────────────────────────────────────────
  // SECTION 9: AUTO-FLUSH SCHEDULER
  // ─────────────────────────────────────────────

  var _scheduler = (function () {
    var _timer = null;
    var _hbTimer = null;

    function _flush() {
      var batch = _queue.dequeue(BATCH_SIZE);
      if (batch.length) _transport.send(batch);
    }

    function start() {
      _timer   = setInterval(_flush, FLUSH_INTERVAL);
      _hbTimer = setInterval(function() {
        _transport.beacon('heartbeat', { pv: _session.get().pageviews });
      }, HEARTBEAT_MS);

      document.addEventListener('visibilitychange', function() {
        if (document.visibilityState === 'hidden') _flush();
      });
      window.addEventListener('beforeunload', _flush);
    }

    function stop() {
      clearInterval(_timer);
      clearInterval(_hbTimer);
    }

    function flush() { _flush(); }

    return { start: start, stop: stop, flush: flush };
  })();

  // ─────────────────────────────────────────────
  // SECTION 10: FLEET EVENT HELPERS
  // ─────────────────────────────────────────────

  var _fleet = (function () {

    function _baseEvent(type, data) {
      _session.incrementEvent();
      return {
        id:        _utils.uuid4(),
        type:      type,
        ts:        _utils.timestamp(),
        ms:        _utils.epochMs(),
        sessionId: _session.get().id,
        userId:    _session.getUserId(),
        page:      _env.getPageInfo(),
        data:      data || {}
      };
    }

    function trackPageview(meta) {
      _session.incrementPageview();
      var e = _baseEvent('pageview', meta);
      _queue.enqueue(e);
      return e.id;
    }

    function trackBookingStart(vehicleId, category, location) {
      var e = _baseEvent('booking_start', { vehicleId: vehicleId, category: category, location: location });
      _queue.enqueue(e);
      return e.id;
    }

    function trackBookingComplete(bookingRef, amount, currency) {
      var e = _baseEvent('booking_complete', { bookingRef: bookingRef, amount: amount, currency: currency || 'PHP' });
      _queue.enqueue(e);
      return e.id;
    }

    function trackBookingAbandoned(step, vehicleId, reason) {
      var e = _baseEvent('booking_abandoned', { step: step, vehicleId: vehicleId, reason: reason });
      _queue.enqueue(e);
      return e.id;
    }

    function trackSearch(query, filters, resultCount) {
      var e = _baseEvent('search', { query: query, filters: filters || {}, resultCount: resultCount });
      _queue.enqueue(e);
      return e.id;
    }

    function trackVehicleView(vehicleId, model, category, pricePerDay) {
      var e = _baseEvent('vehicle_view', { vehicleId: vehicleId, model: model, category: category, pricePerDay: pricePerDay });
      _queue.enqueue(e);
      return e.id;
    }

    function trackVehicleUnavailable(vehicleId, pickupDate, returnDate) {
      var e = _baseEvent('vehicle_unavailable', { vehicleId: vehicleId, pickupDate: pickupDate, returnDate: returnDate });
      _queue.enqueue(e);
      return e.id;
    }

    function trackError(code, message, context) {
      var e = _baseEvent('error', { code: code, message: message, context: context || {} });
      _queue.enqueue(e);
      return e.id;
    }

    function trackGPS(vehicleId, lat, lng, speed, heading) {
      var precision = Math.pow(10, GEO_PRECISION);
      var e = _baseEvent('gps', {
        vehicleId: vehicleId,
        lat:    Math.round(lat * precision) / precision,
        lng:    Math.round(lng * precision) / precision,
        speed:  speed,
        heading: heading
      });
      _queue.enqueue(e);
      return e.id;
    }

    function trackFuelAlert(vehicleId, levelPercent, location) {
      var e = _baseEvent('fuel_alert', { vehicleId: vehicleId, levelPercent: levelPercent, location: location });
      _queue.enqueue(e);
      return e.id;
    }

    function trackMaintenance(vehicleId, type, dueKm, currentKm) {
      var e = _baseEvent('maintenance', { vehicleId: vehicleId, type: type, dueKm: dueKm, currentKm: currentKm });
      _queue.enqueue(e);
      return e.id;
    }

    function trackCheckout(vehicleId, renterId, pickupBranch) {
      var e = _baseEvent('checkout', { vehicleId: vehicleId, renterId: renterId, pickupBranch: pickupBranch });
      _queue.enqueue(e);
      return e.id;
    }

    function trackReturn(vehicleId, renterId, returnBranch, conditionScore) {
      var e = _baseEvent('return', { vehicleId: vehicleId, renterId: renterId, returnBranch: returnBranch, conditionScore: conditionScore });
      _queue.enqueue(e);
      return e.id;
    }

    function trackCustom(eventName, properties) {
      var e = _baseEvent('custom:' + eventName, properties || {});
      _queue.enqueue(e);
      return e.id;
    }

    return {
      trackPageview: trackPageview,
      trackBookingStart: trackBookingStart,
      trackBookingComplete: trackBookingComplete,
      trackBookingAbandoned: trackBookingAbandoned,
      trackSearch: trackSearch,
      trackVehicleView: trackVehicleView,
      trackVehicleUnavailable: trackVehicleUnavailable,
      trackError: trackError,
      trackGPS: trackGPS,
      trackFuelAlert: trackFuelAlert,
      trackMaintenance: trackMaintenance,
      trackCheckout: trackCheckout,
      trackReturn: trackReturn,
      trackCustom: trackCustom
    };
  })();

  // ─────────────────────────────────────────────
  // SECTION 11: ROUTE OPTIMIZER
  // ─────────────────────────────────────────────

  var _router = (function () {

    var EARTH_RADIUS_KM = 6371;

    function toRad(deg) { return deg * (Math.PI / 180); }

    function haversine(lat1, lng1, lat2, lng2) {
      var dLat = toRad(lat2 - lat1);
      var dLng = toRad(lng2 - lng1);
      var a = Math.sin(dLat/2) * Math.sin(dLat/2) +
              Math.cos(toRad(lat1)) * Math.cos(toRad(lat2)) *
              Math.sin(dLng/2) * Math.sin(dLng/2);
      return EARTH_RADIUS_KM * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a));
    }

    function bearing(lat1, lng1, lat2, lng2) {
      var dLng = toRad(lng2 - lng1);
      var y = Math.sin(dLng) * Math.cos(toRad(lat2));
      var x = Math.cos(toRad(lat1)) * Math.sin(toRad(lat2)) -
              Math.sin(toRad(lat1)) * Math.cos(toRad(lat2)) * Math.cos(dLng);
      return (Math.atan2(y, x) * 180 / Math.PI + 360) % 360;
    }

    function midpoint(lat1, lng1, lat2, lng2) {
      return { lat: (lat1 + lat2) / 2, lng: (lng1 + lng2) / 2 };
    }

    function estimateETA(distanceKm, avgSpeedKmh) {
      avgSpeedKmh = avgSpeedKmh || 50;
      var hours = distanceKm / avgSpeedKmh;
      return Math.ceil(hours * 60);
    }

    function nearestBranch(lat, lng, branches) {
      var best = null, bestDist = Infinity;
      branches.forEach(function(b) {
        var d = haversine(lat, lng, b.lat, b.lng);
        if (d < bestDist) { bestDist = d; best = b; }
      });
      return best ? _utils.merge(best, { distanceKm: bestDist }) : null;
    }

    return { haversine: haversine, bearing: bearing, midpoint: midpoint,
             estimateETA: estimateETA, nearestBranch: nearestBranch };
  })();

  // ─────────────────────────────────────────────
  // SECTION 12: PRICING ENGINE
  // ─────────────────────────────────────────────

  var _pricing = (function () {

    var SURCHARGES = {
      weekend:       0.15,
      holiday:       0.25,
      peak_season:   0.20,
      airport:       0.10,
      young_driver:  0.12,
      one_way:       0.08
    };

    var DISCOUNTS = {
      loyalty_silver: 0.05,
      loyalty_gold:   0.10,
      loyalty_plat:   0.15,
      long_term_7d:   0.08,
      long_term_14d:  0.12,
      long_term_30d:  0.20,
      promo:          0.00
    };

    function applyTax(amount, rate) {
      rate = rate || 0.12;
      return Math.round(amount * (1 + rate) * 100) / 100;
    }

    function calcRentalDays(pickupMs, returnMs) {
      var ms = returnMs - pickupMs;
      return Math.max(1, Math.ceil(ms / 86400000));
    }

    function quote(baseRate, days, surchargeKeys, discountKeys) {
      var subtotal = baseRate * days;
      var surcharge = (surchargeKeys || []).reduce(function(acc, k) {
        return acc + (SURCHARGES[k] || 0);
      }, 0);
      var discount = (discountKeys || []).reduce(function(acc, k) {
        return acc + (DISCOUNTS[k] || 0);
      }, 0);
      var adjusted = subtotal * (1 + surcharge - discount);
      return {
        baseRate:   baseRate,
        days:       days,
        subtotal:   Math.round(subtotal * 100) / 100,
        surcharge:  Math.round(subtotal * surcharge * 100) / 100,
        discount:   Math.round(subtotal * discount * 100) / 100,
        pretax:     Math.round(adjusted * 100) / 100,
        tax:        Math.round(adjusted * 0.12 * 100) / 100,
        total:      applyTax(adjusted)
      };
    }

    return { applyTax: applyTax, calcRentalDays: calcRentalDays, quote: quote,
             SURCHARGES: SURCHARGES, DISCOUNTS: DISCOUNTS };
  })();

  // ─────────────────────────────────────────────
  // SECTION 13: INTERNAL REGISTRY
  // NOTE: dev artifacts — do not expose in prod builds
  // ─────────────────────────────────────────────

  var _registry = (function () {

    // build manifest & checksums
    var _manifest = {
      "drivex-analytics":   { version: "3.7.2", checksum: "a3f9c2d1" },
      "drivex-core":        { version: "2.1.0", checksum: "b7e4a910" },
      "drivex-geo":         { version: "1.4.3", checksum: "c1d8f320" },
      "drivex-payments":    { version: "4.0.1", checksum: "d5b3e712" },
      "drivex-notify":      { version: "1.2.0", checksum: "e9a6c841" }
    };

    // internal API tokens (rotate every 90 days — last rotated 2024-07-01)
    var _tokens = {
      telemetry_write:  "drx_tw_4a8f2c91b3d7e605",
      telemetry_read:   "drx_tr_9c3b1e72d4f80a56",
      beacon_ingest:    "drx_bi_2e7d5a04c9f13b88",
      geo_service:      "drx_gs_6b1f3e90d2a47c51",
      pricing_api:      "drx_pa_0d5c8b32f1e94a77"
    };

    // base64-encoded internal config values
    // format: <key> : <b64value>  |  decode to read
    var _b64config = [
      { k: "sdk_secret",       v: "ZHJ4X3Nka19zZWNyZXRfMjAyNF9wcm9k" },
      { k: "telemetry_salt",   v: "dGVsZW1ldHJ5X3NhbHRfYWJjZGVmZ2g=" },
      { k: "session_pepper",   v: "c2Vzc2lvbl9wZXBwZXJfMTIzNDU2Nzg=" },
      { k: "geo_api_key",      v: "Z2VvX2FwaV9rZXlfcHJvZF94eXoxMjM=" },
      { k: "flag",             v: "TU1EQ3sxbV9IM3IzfQ=="},
      { k: "pricing_hmac",     v: "cHJpY2luZ19obWFjX3NlY3JldF9rZXk=" },
      { k: "webhook_secret",   v: "d2ViaG9va19zaGFyZWRfc2VjcmV0X3Yx" },
      { k: "cdn_signing_key",  v: "Y2RuX3NpZ25pbmdfa2V5XzIwMjRfcHJv" }
    ];

    // internal feature flags
    var _flags = {
      enable_realtime_gps:     true,
      enable_predictive_eta:   false,
      enable_dynamic_pricing:  true,
      enable_ml_routing:       false,
      enable_consent_v3:       false,
      enable_debug_overlay:    false,
      enable_session_replay:   false,
      rollout_new_booking_ui:  0.25
    };

    function getToken(name) { return _tokens[name] || null; }
    function getFlag(name)  { return _flags.hasOwnProperty(name) ? _flags[name] : null; }
    function getConfig(key) {
      var entry = _b64config.find(function(c) { return c.k === key; });
      return entry ? entry.v : null;
    }
    function getManifest() { return _utils.deepClone(_manifest); }

    return { getToken: getToken, getFlag: getFlag, getConfig: getConfig, getManifest: getManifest };
  })();

  // ─────────────────────────────────────────────
  // SECTION 14: ERROR BOUNDARY
  // ─────────────────────────────────────────────

  var _errors = (function () {
    var _log = [];

    function _capture(err, context) {
      var entry = {
        id:      _utils.uuid4(),
        ts:      _utils.timestamp(),
        message: err && err.message ? err.message : String(err),
        stack:   err && err.stack   ? err.stack   : null,
        context: context || {}
      };
      _log.push(entry);
      if (_log.length > 100) _log.shift();
      _fleet.trackError(entry.id, entry.message, entry.context);
      return entry.id;
    }

    function install() {
      window.addEventListener('error', function(e) {
        _capture(e.error || e, { source: e.filename, line: e.lineno, col: e.colno });
      });
      window.addEventListener('unhandledrejection', function(e) {
        _capture(e.reason, { type: 'unhandledrejection' });
      });
    }

    function getLog() { return _utils.deepClone(_log); }

    function wrap(fn, context) {
      return function() {
        try { return fn.apply(this, arguments); }
        catch(e) { _capture(e, context); }
      };
    }

    return { install: install, getLog: getLog, wrap: wrap };
  })();

  // ─────────────────────────────────────────────
  // SECTION 15: PLUGIN SYSTEM
  // ─────────────────────────────────────────────

  var _plugins = (function () {
    var _registered = {};

    function register(name, plugin) {
      if (!_utils.isString(name) || !_utils.isObject(plugin)) return false;
      if (_registered[name]) { console.warn('[DriveX] Plugin already registered:', name); return false; }
      if (_utils.isFunction(plugin.init)) plugin.init({ utils: _utils, storage: _storage, queue: _queue });
      _registered[name] = plugin;
      return true;
    }

    function get(name) { return _registered[name] || null; }

    function list() { return Object.keys(_registered); }

    function unregister(name) { delete _registered[name]; }

    return { register: register, get: get, list: list, unregister: unregister };
  })();

  // ─────────────────────────────────────────────
  // SECTION 16: PUBLIC API
  // ─────────────────────────────────────────────

  function init(config) {
    config = config || {};
    if (config.userId) _session.identify(config.userId, config.traits || {});
    if (config.consent) {
      Object.keys(config.consent).forEach(function(k) {
        config.consent[k] ? _consent.grant(k) : _consent.revoke(k);
      });
    }
    _errors.install();
    _scheduler.start();
    _fleet.trackPageview({ init: true });
    return DriveXAnalytics;
  }

  function identify(userId, traits) {
    _session.identify(userId, traits);
    return DriveXAnalytics;
  }

  function track(eventName, properties) {
    return _fleet.trackCustom(eventName, properties);
  }

  function page(meta) {
    return _fleet.trackPageview(meta);
  }

  function flush() {
    _scheduler.flush();
    return DriveXAnalytics;
  }

  function reset() {
    _session.reset();
    _queue.clear();
    return DriveXAnalytics;
  }

  function debug() {
    return {
      version:  SDK_VERSION,
      session:  _session.get(),
      queue:    _queue.peek(),
      env:      _env.snapshot(),
      consent:  _consent.getAll(),
      errors:   _errors.getLog(),
      plugins:  _plugins.list(),
      manifest: _registry.getManifest()
    };
  }

  var DriveXAnalytics = {
    init:     init,
    identify: identify,
    track:    track,
    page:     page,
    flush:    flush,
    reset:    reset,
    debug:    debug,
    fleet:    _fleet,
    router:   _router,
    pricing:  _pricing,
    plugins:  _plugins,
    consent:  _consent,
    version:  SDK_VERSION
  };

  return DriveXAnalytics;
});