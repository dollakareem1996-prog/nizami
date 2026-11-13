/* نظامي — طبقة التخزين المستقلة
 * تحفظ البيانات على الجهاز أولاً (تعمل دون إنترنت)، ثم تزامنها مع خادم Apps Script في حسابكِ على Google.
 * توفّر للتطبيق نفس الواجهة التي كان يستخدمها: db و assets و downloads.
 */
(function () {
  'use strict';
  const K = { docs: 'nz-docs', queue: 'nz-queue', cfg: 'nz-cfg' };
  const load = (k, d) => { try { const v = JSON.parse(localStorage.getItem(k)); return v == null ? d : v; } catch (e) { return d; } };
  const save = (k, v) => { try { localStorage.setItem(k, JSON.stringify(v)); return true; } catch (e) { setStatus('storage_full'); return false; } };

  let docs = load(K.docs, {});
  let queue = load(K.queue, []);
  let cfg = load(K.cfg, {});
  const listeners = [];
  let lastStamp = '';
  function stamp() {
    let t = new Date().toISOString();
    if (t <= lastStamp) t = new Date(Date.parse(lastStamp) + 1).toISOString();
    lastStamp = t; return t;
  }
  const clone = o => JSON.parse(JSON.stringify(o));
  function deepMerge(a, b) {
    const out = clone(a || {});
    for (const [k, v] of Object.entries(b || {})) {
      if (v && typeof v === 'object' && !Array.isArray(v) && out[k] && typeof out[k] === 'object' && !Array.isArray(out[k])) out[k] = deepMerge(out[k], v);
      else out[k] = clone(v);
    }
    return out;
  }

  /* ---------- لقطات بنفس شكل الواجهة القديمة ---------- */
  function snapDoc(path) {
    const r = docs[path], exists = !!(r && !r.deleted);
    return { id: path.split('/').pop(), exists, data: () => (exists ? r.data : undefined),
      metadata: { fromCache: false, hasPendingWrites: queue.some(q => q.path === path) } };
  }
  function colDocs(col) {
    const pre = col + '/';
    return Object.keys(docs).filter(p => p.startsWith(pre) && p.slice(pre.length).indexOf('/') < 0 && !docs[p].deleted).sort().map(snapDoc);
  }
  function fire(l) {
    setTimeout(() => {
      try {
        if (l.kind === 'col') { const d = colDocs(l.path); l.cb({ docs: d, size: d.length, empty: !d.length, docChanges: () => [], metadata: { fromCache: false, hasPendingWrites: queue.length > 0 } }); }
        else l.cb(snapDoc(l.path));
      } catch (e) { console.error(e); }
    }, 0);
  }
  function notify(paths) {
    listeners.forEach(l => {
      if (!paths) return fire(l);
      if (l.kind === 'col' ? paths.some(p => p.startsWith(l.path + '/')) : paths.includes(l.path)) fire(l);
    });
  }
  function write(path, data, deleted, silent) {
    const u = stamp();
    docs[path] = deleted ? { deleted: true, updated: u } : { data: clone(data), updated: u };
    queue = queue.filter(q => q.path !== path);
    queue.push({ path, updated: u, deleted: !!deleted, data: deleted ? null : docs[path].data });
    save(K.docs, docs); save(K.queue, queue);
    if (!silent) notify([path]);
    scheduleSync();
  }
  function listen(kind, path, cb) {
    const l = { kind, path, cb }; listeners.push(l); fire(l);
    return () => { const i = listeners.indexOf(l); if (i >= 0) listeners.splice(i, 1); };
  }
  const newKey = () => 'd' + Date.now().toString(36) + Math.random().toString(36).slice(2, 8);
  function docRef(path) {
    return {
      id: path.split('/').pop(), path,
      get: async () => snapDoc(path),
      set: async d => write(path, d, false),
      update: async d => { const r = docs[path]; if (!r || r.deleted) throw { code: 'invalid_argument' }; write(path, deepMerge(r.data, d), false); },
      delete: async () => write(path, null, true),
      onSnapshot: (cb) => listen('doc', path, cb),
      collection: sub => colRef(path + '/' + sub)
    };
  }
  function colRef(path) {
    const q = {
      path,
      doc: id => docRef(path + '/' + (id || newKey())),
      add: async d => { const r = docRef(path + '/' + newKey()); await r.set(d); return r; },
      get: async () => { const d = colDocs(path); return { docs: d, size: d.length, empty: !d.length, metadata: { fromCache: false } }; },
      onSnapshot: (cb) => listen('col', path, cb),
      where() { return q; }, orderBy() { return q; }, limit() { return q; }
    };
    return q;
  }
  const db = { doc: docRef, collection: colRef };

  /* ---------- الاتصال بالخادم ---------- */
  async function api(action, body) {
    if (!cfg.url) throw { code: 'unlinked' };
    let r;
    try { r = await fetch(cfg.url, { method: 'POST', body: JSON.stringify(Object.assign({ action, token: cfg.token || '' }, body || {})), redirect: 'follow' }); }
    catch (e) { throw { code: 'network' }; }
    let j; try { j = await r.json(); } catch (e) { throw { code: 'bad_response' }; }
    if (!j.ok) throw { code: j.error || 'server', message: j.message };
    return j;
  }

  let syncing = false, syncTimer = null, lastErr = null;
  function scheduleSync(ms) { clearTimeout(syncTimer); syncTimer = setTimeout(sync, ms == null ? 700 : ms); }
  async function sync() {
    if (syncing) return;
    if (!cfg.url || !cfg.token) { setStatus('unlinked'); return; }
    if (!navigator.onLine) { setStatus('offline'); return; }
    syncing = true; setStatus('syncing');
    try {
      while (queue.length) {
        const batch = queue.slice(0, 40);
        await api('push', { ops: batch });
        const sent = new Set(batch.map(b => b.path + '|' + b.updated));
        queue = queue.filter(q => !sent.has(q.path + '|' + q.updated));
        save(K.queue, queue);
      }
      const since = cfg.lastPull ? new Date(Date.parse(cfg.lastPull) - 120000).toISOString() : '';
      const j = await api('pull', { since });
      const changed = [];
      (j.docs || []).forEach(d => {
        if (queue.some(q => q.path === d.path)) return;
        const loc = docs[d.path];
        if (!loc || d.updated > loc.updated) {
          docs[d.path] = d.deleted ? { deleted: true, updated: d.updated } : { data: d.data, updated: d.updated };
          changed.push(d.path);
        }
      });
      cfg.lastPull = j.now; save(K.cfg, cfg);
      if (changed.length) { save(K.docs, docs); notify(changed); }
      lastErr = null; setStatus('ok');
    } catch (e) {
      lastErr = e; setStatus(e.code === 'auth' ? 'auth' : (e.code === 'network' ? 'offline' : 'error'));
    } finally { syncing = false; }
  }
  setInterval(() => { if (document.visibilityState === 'visible') sync(); }, 45000);
  window.addEventListener('online', () => sync());
  document.addEventListener('visibilitychange', () => { if (document.visibilityState === 'visible') sync(); });

  /* ---------- حالة المزامنة في الترويسة ---------- */
  let statusNow = '';
  function setStatus(s) {
    statusNow = s;
    const dot = document.getElementById('syncDot'), txt = document.getElementById('syncTxt');
    if (!dot || !txt) return;
    const n = queue.length;
    const M = {
      ok: ['ok', 'متزامن'], syncing: ['', 'جارٍ المزامنة…'],
      offline: ['', n ? `دون اتصال — ${n} تعديل بانتظار المزامنة` : 'دون اتصال'],
      unlinked: ['err', 'غير مربوط بحساب Google'], auth: ['err', 'رمز الربط غير صحيح'],
      error: ['err', 'تعذّرت المزامنة — سأعيد المحاولة'], storage_full: ['err', 'ذاكرة الجهاز ممتلئة']
    }[s] || ['', ''];
    dot.className = 'dot ' + M[0]; txt.textContent = M[1];
  }

  /* ---------- المستمسكات: Google Drive مع نسخة على الجهاز ---------- */
  const IDB = 'nizami-files';
  function idb() {
    return new Promise((res, rej) => {
      const r = indexedDB.open(IDB, 1);
      r.onupgradeneeded = () => r.result.createObjectStore('files');
      r.onsuccess = () => res(r.result); r.onerror = () => rej(r.error);
    });
  }
  async function cacheGet(id) { try { const d = await idb(); return await new Promise(res => { const q = d.transaction('files').objectStore('files').get(id); q.onsuccess = () => res(q.result || null); q.onerror = () => res(null); }); } catch (e) { return null; } }
  async function cachePut(id, blob) { try { const d = await idb(); await new Promise(res => { const t = d.transaction('files', 'readwrite'); t.objectStore('files').put(blob, id); t.oncomplete = res; t.onerror = res; }); } catch (e) {} }
  async function cacheDel(id) { try { const d = await idb(); d.transaction('files', 'readwrite').objectStore('files').delete(id); } catch (e) {} }
  const toB64 = b => new Promise((res, rej) => { const r = new FileReader(); r.onload = () => res(String(r.result).split(',')[1] || ''); r.onerror = rej; r.readAsDataURL(b); });
  function fromB64(b64, type) { const bin = atob(b64); const u = new Uint8Array(bin.length); for (let i = 0; i < bin.length; i++) u[i] = bin.charCodeAt(i); return new Blob([u], { type: type || 'application/octet-stream' }); }

  const MAX_UPLOAD = 30 * 1048576;
  const assets = {
    async upload(blob, opt) {
      opt = opt || {};
      if (!cfg.url || !cfg.token) throw { code: 'unlinked' };
      if (!navigator.onLine) throw { code: 'offline' };
      if (blob.size > MAX_UPLOAD) throw { code: 'too_large' };
      const type = opt.type || blob.type || 'application/octet-stream';
      const j = await api('upload', { name: blob.name || opt.name || 'ملف', mime: type, b64: await toB64(blob) });
      await cachePut(j.id, new Blob([blob], { type }));
      return { id: j.id, url: '', sizeBytes: blob.size, contentType: type };
    },
    async delete(id) { cacheDel(id); try { await api('delfile', { id }); } catch (e) {} return { deleted: true }; },
    async list() { return { assets: [], usage: {} }; }
  };
  async function getBlob(id, typeHint) {
    const c = await cacheGet(id); if (c) return c;
    const j = await api('file', { id });
    const b = fromB64(j.b64, typeHint || j.mime); await cachePut(id, b); return b;
  }

  const downloads = {
    async save({ filename, data }) {
      const blob = data instanceof Blob ? data : new Blob([data]);
      const url = URL.createObjectURL(blob);
      const a = document.createElement('a'); a.href = url; a.download = filename; document.body.appendChild(a); a.click(); a.remove();
      setTimeout(() => URL.revokeObjectURL(url), 60000);
      return { status: 'saved' };
    }
  };

  window.claude = { use: async name => ({ db, assets, downloads }[name] || null) };

  /* ---------- الربط والإعدادات والاستيراد ---------- */
  async function link(url, token) {
    url = String(url || '').trim();
    if (!/^https:\/\/script\.google(usercontent)?\.com\//.test(url)) throw { code: 'bad_url' };
    const prev = cfg; cfg = { url, token: (token || '').trim() };
    try {
      const j = await api('hello', {});
      if (j.token) cfg.token = j.token;
      cfg.lastPull = ''; save(K.cfg, cfg);
      await sync();
      return { first: !!j.first, token: cfg.token };
    } catch (e) { cfg = prev; throw e; }
  }
  function unlink() { cfg = {}; save(K.cfg, cfg); setStatus('unlinked'); }

  function findMeta(data, id) {
    let found = null;
    (function walk(o) { if (found || !o || typeof o !== 'object') return; if (o.id === id && o.name) { found = o; return; } for (const v of Object.values(o)) walk(v); })(data);
    return found;
  }
  async function importZip(file, onProgress) {
    if (typeof JSZip === 'undefined') throw { code: 'no_zip' };
    const zip = await JSZip.loadAsync(file);
    const dj = zip.file('data.json'); if (!dj) throw { code: 'bad_zip' };
    const data = JSON.parse(await dj.async('string'));
    const files = zip.file(/^files\//).filter(f => !f.dir);
    const map = {};
    for (let i = 0; i < files.length; i++) {
      const f = files[i], oldId = f.name.slice(6), meta = findMeta(data, oldId) || {};
      onProgress && onProgress(i, files.length, meta.name || oldId);
      const blob = await f.async('blob');
      const typed = new File([blob], meta.name || oldId, { type: meta.type || 'application/octet-stream' });
      let r; try { r = await assets.upload(typed, { type: meta.type }); }
      catch (e) { if (e && e.code === 'network') { await new Promise(z => setTimeout(z, 2000)); r = await assets.upload(typed, { type: meta.type }); } else throw e; }
      map[oldId] = r.id;
    }
    let s = JSON.stringify(data);
    for (const [o, n] of Object.entries(map)) s = s.split(o).join(n);
    const nd = JSON.parse(s);
    for (const [path, doc] of Object.entries(nd)) write(path, doc, false, true);
    notify(null); onProgress && onProgress(files.length, files.length, '');
    cfg.imported = new Date().toISOString(); save(K.cfg, cfg);
    await sync();
    return { docs: Object.keys(nd).length, files: files.length };
  }

  window.nizami = {
    api, sync, link, unlink, importZip, getBlob,
    cfg: () => cfg, pending: () => queue.length, status: () => statusNow, lastError: () => lastErr,
    exportAll: () => ({ docs: Object.fromEntries(Object.entries(docs).filter(([, r]) => !r.deleted).map(([p, r]) => [p, r.data])) })
  };

  document.addEventListener('DOMContentLoaded', () => { setStatus(cfg.url ? (navigator.onLine ? 'syncing' : 'offline') : 'unlinked'); scheduleSync(300); });
})();
