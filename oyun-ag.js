        (function (global) {
            'use strict';

            const DEFAULT_BROKERS = [
                'wss://broker.emqx.io:8084/mqtt',
                'wss://test.mosquitto.org:8081/mqtt',
                'wss://broker.hivemq.com:8884/mqtt',
                { url: 'wss://public.cloud.shiftr.io', username: 'public', password: 'public' } // 443 portu: kısıtlı ağlarda da geçer
            ];
            const HELLO_EVERY_MS = 1400;     // misafir: oda sahibine "geldim" tekrarı
            const RESEND_AFTER_MS = 1800;    // onaylanmayan veriyi yeniden gönder
            const KEEPALIVE_MS = 2500;       // misafir -> ev sahibi yoklama
            const HOST_DEAD_MS = 20000;      // ev sahibi: bu kadar sessiz kalan misafiri düşür
            const STALE_MS = 7000;           // misafir: ev sahibi bu kadar sessizse "bekleniyor" de
            const OPEN_TIMEOUT_MS = 20000;   // hiçbir sunucuya ulaşılamazsa hata bildir
            const MAX_OUTBOX = 400;

            const te = new TextEncoder();
            const td = new TextDecoder();
            const now = () => Date.now();
            const dbg = (...a) => { if (global.SOS_NET_DEBUG) console.log('[SOS-NET]', ...a); };

            function rid(n) {
                const a = new Uint8Array(n);
                (global.crypto || global.msCrypto).getRandomValues(a);
                let s = '';
                for (let i = 0; i < n; i++) s += (a[i] % 36).toString(36);
                return s;
            }

            let memDeviceId = null;
            function deviceId() {
                try {
                    let v = localStorage.getItem('sos_net_device');
                    if (!v || v.length < 8) {
                        v = rid(12);
                        localStorage.setItem('sos_net_device', v);
                    }
                    return v;
                } catch (e) {
                    if (!memDeviceId) memDeviceId = rid(12);
                    return memDeviceId;
                }
            }

            /* ---------- Küçük olay yayıcı ---------- */
            class Emitter {
                constructor() { this._ev = {}; }
                on(name, fn) { (this._ev[name] || (this._ev[name] = [])).push(fn); return this; }
                off(name, fn) {
                    if (!this._ev[name]) return this;
                    this._ev[name] = fn ? this._ev[name].filter(f => f !== fn) : [];
                    return this;
                }
                emit(name, ...args) {
                    (this._ev[name] || []).slice().forEach(fn => {
                        try { fn(...args); } catch (e) { console.error('[SOS-NET] olay hatası (' + name + '):', e); }
                    });
                }
            }

            /* ---------- MQTT 3.1.1 paketleri (yalnızca gerekenler) ---------- */
            function vlen(n) {
                const o = [];
                do {
                    let b = n % 128;
                    n = Math.floor(n / 128);
                    if (n > 0) b |= 128;
                    o.push(b);
                } while (n > 0);
                return o;
            }
            function mstr(s) {
                const b = te.encode(s);
                const o = new Uint8Array(b.length + 2);
                o[0] = b.length >> 8;
                o[1] = b.length & 255;
                o.set(b, 2);
                return o;
            }
            function mpkt(head, parts) {
                let n = 0;
                parts.forEach(p => { n += p.length; });
                const l = vlen(n);
                const o = new Uint8Array(1 + l.length + n);
                o[0] = head;
                o.set(l, 1);
                let off = 1 + l.length;
                parts.forEach(p => { o.set(p, off); off += p.length; });
                return o;
            }
            const PINGREQ = new Uint8Array([0xC0, 0x00]);
            const DISCONNECT = new Uint8Array([0xE0, 0x00]);

            /* ---------- Tek bir sunucuya kalıcı bağlantı ---------- */
            class Link {
                constructor(spec, hooks) {
                    this.url = typeof spec === 'string' ? spec : spec.url;
                    this.user = (spec && spec.username) || '';
                    this.pass = (spec && spec.password) || '';
                    this.hooks = hooks;
                    this.ws = null;
                    this.ready = false;
                    this.dead = false;
                    this.topics = new Set();
                    this.rx = new Uint8Array(0);
                    this.pid = 0;
                    this.lastRx = 0;
                    this.fail = 0;
                    this.retryTimer = null;
                    this.connTimer = null;
                    this.pingTimer = null;
                    this.open();
                }

                open() {
                    if (this.dead || this.ws) return;
                    clearTimeout(this.retryTimer);
                    this.retryTimer = null;
                    let ws;
                    try {
                        ws = new WebSocket(this.url, ['mqtt']);
                    } catch (e) {
                        this.retry();
                        return;
                    }
                    ws.binaryType = 'arraybuffer';
                    this.ws = ws;
                    this.rx = new Uint8Array(0);
                    this.connTimer = setTimeout(() => {
                        if (this.ws === ws && !this.ready) this.drop('bağlantı zaman aşımı');
                    }, 12000);
                    ws.onopen = () => {
                        if (this.ws !== ws) return;
                        // CONNECT: protokol "MQTT" v4, temiz oturum, keepalive 60 sn (+ varsa kullanıcı adı/parola)
                        const flags = 0x02 | (this.user ? 0x80 : 0) | (this.pass ? 0x40 : 0);
                        const parts = [mstr('MQTT'), [4, flags, 0, 60], mstr('sos' + rid(16))];
                        if (this.user) parts.push(mstr(this.user));
                        if (this.pass) parts.push(mstr(this.pass));
                        this.raw(mpkt(0x10, parts));
                    };
                    ws.onmessage = (ev) => {
                        if (this.ws !== ws) return;
                        this.lastRx = now();
                        if (ev.data instanceof ArrayBuffer) this.feed(new Uint8Array(ev.data));
                    };
                    ws.onerror = () => { };
                    ws.onclose = () => {
                        if (this.ws === ws) this.drop('kapandı');
                    };
                }

                drop(why) {
                    const ws = this.ws;
                    const wasReady = this.ready;
                    this.ws = null;
                    this.ready = false;
                    clearTimeout(this.connTimer);
                    clearInterval(this.pingTimer);
                    if (ws) {
                        ws.onopen = ws.onmessage = ws.onclose = ws.onerror = null;
                        try { ws.close(); } catch (e) { }
                    }
                    dbg('bağlantı düştü:', this.url, why);
                    if (wasReady) this.hooks.state(this);
                    if (!this.dead) this.retry();
                }

                retry() {
                    if (this.dead || this.retryTimer) return;
                    this.fail++;
                    const wait = Math.min(4000, 500 * Math.pow(2, Math.min(this.fail - 1, 3))) + Math.random() * 400;
                    this.retryTimer = setTimeout(() => {
                        this.retryTimer = null;
                        this.open();
                    }, wait);
                }

                /* Hemen dene (sekmeye dönüş / internet geldi) ya da canlılığı sına */
                kick() {
                    if (this.dead) return;
                    if (!this.ws) {
                        clearTimeout(this.retryTimer);
                        this.retryTimer = null;
                        this.fail = 0;
                        this.open();
                    } else if (this.ready) {
                        this.ping(4000);
                    }
                }

                ping(graceMs) {
                    if (!this.ready) return;
                    const ws = this.ws;
                    const sentAt = now();
                    if (!this.raw(PINGREQ)) return;
                    setTimeout(() => {
                        if (this.ws === ws && this.lastRx < sentAt) this.drop('yoklama yanıtsız');
                    }, graceMs);
                }

                feed(chunk) {
                    let buf = chunk;
                    if (this.rx.length) {
                        buf = new Uint8Array(this.rx.length + chunk.length);
                        buf.set(this.rx, 0);
                        buf.set(chunk, this.rx.length);
                    }
                    let off = 0;
                    for (; ;) {
                        if (buf.length - off < 2) break;
                        let len = 0, mul = 1, i = off + 1, complete = false, k = 0;
                        for (; k < 4 && i < buf.length; k++) {
                            const b = buf[i++];
                            len += (b & 127) * mul;
                            mul *= 128;
                            if (!(b & 128)) { complete = true; break; }
                        }
                        if (!complete) {
                            if (k >= 4) { this.drop('bozuk paket'); return; }
                            break; // uzunluk alanı henüz tamamlanmadı
                        }
                        if (buf.length - i < len) break; // gövde henüz tamamlanmadı
                        this.packet(buf[off], buf.subarray(i, i + len));
                        if (!this.ws) return; // paket işlenirken bağlantı düşürüldü
                        off = i + len;
                    }
                    this.rx = off < buf.length ? buf.slice(off) : new Uint8Array(0);
                }

                packet(head, body) {
                    const type = head >> 4;
                    if (type === 2) { // CONNACK
                        if (body.length < 2 || body[1] !== 0) { this.drop('sunucu reddetti'); return; }
                        clearTimeout(this.connTimer);
                        this.ready = true;
                        this.fail = 0;
                        this.topics.forEach(t => this.sendSub(t));
                        clearInterval(this.pingTimer);
                        this.pingTimer = setInterval(() => this.ping(10000), 20000);
                        dbg('bağlandı:', this.url);
                        this.hooks.state(this);
                    } else if (type === 3) { // PUBLISH
                        if (body.length < 2) return;
                        const tl = (body[0] << 8) | body[1];
                        let p = 2 + tl;
                        if (p > body.length) return;
                        const topic = td.decode(body.subarray(2, p));
                        if ((head >> 1) & 3) p += 2; // QoS>0 ise paket kimliği
                        this.hooks.message(topic, body.slice(p));
                    }
                    // SUBACK (9) ve PINGRESP (13): yalnızca canlılık kanıtı (lastRx)
                }

                sendSub(topic) {
                    this.pid = (this.pid % 65535) + 1;
                    this.raw(mpkt(0x82, [[this.pid >> 8, this.pid & 255], mstr(topic), [0]]));
                }

                sub(topic) {
                    if (this.topics.has(topic)) return;
                    this.topics.add(topic);
                    if (this.ready) this.sendSub(topic);
                }

                pub(topic, payload) {
                    if (!this.ready) return false;
                    return this.raw(mpkt(0x30, [mstr(topic), payload]));
                }

                raw(u8) {
                    try {
                        if (!this.ws || this.ws.readyState !== 1) return false;
                        this.ws.send(u8);
                        return true;
                    } catch (e) {
                        this.drop('gönderim hatası');
                        return false;
                    }
                }

                close() {
                    this.dead = true;
                    clearTimeout(this.retryTimer);
                    clearTimeout(this.connTimer);
                    clearInterval(this.pingTimer);
                    const ws = this.ws;
                    this.ws = null;
                    this.ready = false;
                    if (ws) {
                        ws.onopen = ws.onmessage = ws.onclose = ws.onerror = null;
                        try {
                            if (ws.readyState === 1) ws.send(DISCONNECT);
                            ws.close();
                        } catch (e) { }
                    }
                }
            }

            /* ---------- Oda anahtarları ve şifreleme ---------- */
            function hex(u8) {
                let s = '';
                for (let i = 0; i < u8.length; i++) s += (u8[i] < 16 ? '0' : '') + u8[i].toString(16);
                return s;
            }
            function weakHash(str) { // yalnızca WebCrypto yoksa (güvenli olmayan bağlam) kullanılır
                let h1 = 0xdeadbeef, h2 = 0x41c6ce57;
                for (let i = 0; i < str.length; i++) {
                    const ch = str.charCodeAt(i);
                    h1 = Math.imul(h1 ^ ch, 2654435761);
                    h2 = Math.imul(h2 ^ ch, 1597334677);
                }
                h1 = Math.imul(h1 ^ (h1 >>> 16), 2246822507) ^ Math.imul(h2 ^ (h2 >>> 13), 3266489909);
                h2 = Math.imul(h2 ^ (h2 >>> 16), 2246822507) ^ Math.imul(h1 ^ (h1 >>> 13), 3266489909);
                return (h2 >>> 0).toString(16).padStart(8, '0') + (h1 >>> 0).toString(16).padStart(8, '0');
            }
            async function roomKeys(roomId) {
                const id = String(roomId || '').trim().toUpperCase();
                const subtle = global.crypto && global.crypto.subtle;
                if (subtle) {
                    try {
                        const h1 = new Uint8Array(await subtle.digest('SHA-256', te.encode('sos-relay-v1|konu|' + id)));
                        const h2 = await subtle.digest('SHA-256', te.encode('sos-relay-v1|anahtar|' + id));
                        const key = await subtle.importKey('raw', h2, { name: 'AES-GCM' }, false, ['encrypt', 'decrypt']);
                        return { root: 'sosr1/' + hex(h1.subarray(0, 12)), key: key };
                    } catch (e) { }
                }
                return { root: 'sosr1p/' + weakHash('sos-relay-v1|' + id), key: null };
            }
            async function seal(keys, obj) {
                const plain = te.encode(JSON.stringify(obj));
                if (!keys.key) {
                    const o = new Uint8Array(plain.length + 1);
                    o[0] = 0;
                    o.set(plain, 1);
                    return o;
                }
                const iv = global.crypto.getRandomValues(new Uint8Array(12));
                const ct = new Uint8Array(await global.crypto.subtle.encrypt({ name: 'AES-GCM', iv: iv }, keys.key, plain));
                const o = new Uint8Array(13 + ct.length);
                o[0] = 1;
                o.set(iv, 1);
                o.set(ct, 13);
                return o;
            }
            async function unseal(keys, u8) {
                try {
                    if (!u8 || u8.length < 2) return null;
                    if (keys.key) {
                        if (u8[0] !== 1 || u8.length < 30) return null; // anahtar varken düz metin kabul edilmez
                        const pt = await global.crypto.subtle.decrypt({ name: 'AES-GCM', iv: u8.slice(1, 13) }, keys.key, u8.slice(13));
                        return JSON.parse(td.decode(pt));
                    }
                    if (u8[0] !== 0) return null;
                    return JSON.parse(td.decode(u8.subarray(1)));
                } catch (e) {
                    return null;
                }
            }

            /* ---------- Veri bağlantısı (PeerJS DataConnection eşleniği) ---------- */
            class DataConn extends Emitter {
                constructor(peer, o) {
                    super();
                    this.open = false;
                    this.reliable = true;
                    this.label = 'sos';
                    this.metadata = null;
                    this._p = peer;
                    this._role = o.role;           // 'host' | 'guest'
                    this._closed = false;
                    this._closing = false;
                    this._closeBy = 0;
                    this._out = [];                // onay bekleyen giden veriler
                    this._seq = 0;
                    this._in = 0;                  // sırayla teslim edilen son gelen numara
                    this._hold = new Map();        // sırası gelmemiş gelenler
                    this._ackTimer = null;
                    this._lastRx = now();
                    this._lastTx = 0;
                    this._lastTick = now();
                    this._born = now();
                    this.closeReason = '';
                    this._stale = false;
                    this._keys = null;
                    this._hs = null;
                    if (o.role === 'host') {
                        this.peer = o.cid;
                        this._cid = o.cid;
                        this._sid = o.sid;
                        this._hs = peer._hs;
                        this._keys = peer._keys;
                        this._txTopic = peer._keys.root + '/c/' + o.cid;
                        this.open = true;
                        setTimeout(() => { if (!this._closed) this.emit('open'); }, 0);
                    } else {
                        this.peer = o.target;
                        this._cid = deviceId();
                        this._sid = rid(8);
                        roomKeys(o.target).then(keys => {
                            if (this._closed) return;
                            this._keys = keys;
                            this._txTopic = keys.root + '/h';
                            this._rxTopic = keys.root + '/c/' + this._cid;
                            peer._links.forEach(l => l.sub(this._rxTopic));
                            this._tx({ t: 'H' });
                        });
                    }
                    this._timer = setInterval(() => this._tick(), 700);
                }

                /* Dışa açık API */
                send(data) {
                    if (this._closed || this._closing) return;
                    let snap;
                    try { snap = JSON.parse(JSON.stringify(data)); } catch (e) { return; } // gönderim anındaki durumun kopyası
                    const item = { q: ++this._seq, d: snap, at: 0 };
                    this._out.push(item);
                    if (this._out.length > MAX_OUTBOX) { this._finish(true, 'kuyruk doldu'); return; }
                    if (this.open) this._txData(item);
                }

                close() {
                    if (this._closed || this._closing) return;
                    if (this.open && this._out.length) {
                        // Son gönderilenler (ör. "şifre hatalı") karşıya ulaşsın diye kısa süre bekle
                        this._closing = true;
                        this._closeBy = now() + 3000;
                    } else {
                        this._finish(true, 'kapatıldı');
                    }
                }

                /* İç işleyiş */
                _tx(frame) {
                    if (!this._keys) return;
                    frame.c = this._cid;
                    frame.s = this._sid;
                    if (this._hs) frame.h = this._hs;
                    this._lastTx = now();
                    const topic = this._txTopic, p = this._p;
                    seal(this._keys, frame).then(bytes => { p._publish(topic, bytes); }).catch(() => { });
                }

                _txData(item) {
                    item.at = now();
                    this._tx({ t: 'D', q: item.q, a: this._in, d: item.d });
                }

                _ackSoon() {
                    if (this._ackTimer || this._closed) return;
                    this._ackTimer = setTimeout(() => {
                        this._ackTimer = null;
                        if (!this._closed) this._tx({ t: 'K', a: this._in });
                    }, 60);
                }

                _acked(a) {
                    if (typeof a !== 'number') return;
                    while (this._out.length && this._out[0].q <= a) this._out.shift();
                    if (this._closing && !this._out.length) this._finish(true, 'kapatıldı');
                }

                _flush() {
                    if (this._closed) return;
                    if (!this.open) {
                        if (this._keys) this._tx({ t: 'H' });
                        return;
                    }
                    const t = now();
                    this._out.forEach(it => { if (t - it.at > 300) this._txData(it); });
                    if (this._role === 'guest') this._tx({ t: 'K', a: this._in });
                }

                _tick() {
                    if (this._closed) return;
                    const t = now();
                    if (t - this._lastTick > 5000) this._lastRx = t; // sayfa uyutulmuştu: karşı tarafa yeniden süre tanı
                    this._lastTick = t;
                    if (!this.open) { // misafir: ev sahibi henüz yanıtlamadı (uzun bekleyişte seyrekleşir)
                        const waited = t - this._born;
                        const every = waited < 30000 ? HELLO_EVERY_MS : (waited < 180000 ? 3000 : 6000);
                        if (this._keys && t - this._lastTx >= every) this._tx({ t: 'H' });
                        return;
                    }
                    let n = 0;
                    for (let i = 0; i < this._out.length && n < 25; i++) {
                        if (t - this._out[i].at >= RESEND_AFTER_MS) { this._txData(this._out[i]); n++; }
                    }
                    const silent = t - this._lastRx;
                    if (this._role === 'guest') {
                        if (t - this._lastTx >= KEEPALIVE_MS) this._tx({ t: 'K', a: this._in });
                        const stale = silent > STALE_MS;
                        if (stale !== this._stale) {
                            this._stale = stale;
                            this.emit(stale ? 'stale' : 'fresh');
                        }
                    } else if (silent > HOST_DEAD_MS) {
                        this._tx({ t: 'R' });
                        this._finish(false, 'sessizlik');
                        return;
                    }
                    if (this._closing && t > this._closeBy) this._finish(true, 'kapatıldı');
                }

                _onFrame(f) {
                    if (this._closed) return;
                    this._lastRx = now();
                    if (this._stale) { this._stale = false; this.emit('fresh'); }
                    switch (f.t) {
                        case 'A':
                            if (this._role !== 'guest' || this.open || typeof f.h !== 'string') return;
                            this._hs = f.h;
                            this.open = true;
                            this.emit('open');
                            this._flush();
                            return;
                        case 'R':
                            if (this._role === 'guest') this._finish(false, 'reset');
                            return;
                        case 'X': // bu cihazdan başka bir sekme/oturum odaya girdi
                            if (this._role === 'guest') this._finish(false, 'superseded');
                            return;
                        case 'B':
                            this._finish(false, 'karşı taraf ayrıldı');
                            return;
                        case 'K':
                            this._acked(f.a);
                            if (this._role === 'host') this._ackSoon();
                            return;
                        case 'D':
                            this._acked(f.a);
                            if (typeof f.q !== 'number') return;
                            if (f.q === this._in + 1) {
                                this._in++;
                                this.emit('data', f.d);
                                while (!this._closed && this._hold.has(this._in + 1)) {
                                    const d = this._hold.get(this._in + 1);
                                    this._hold.delete(this._in + 1);
                                    this._in++;
                                    this.emit('data', d);
                                }
                            } else if (f.q > this._in + 1 && this._hold.size < 200) {
                                this._hold.set(f.q, f.d);
                            }
                            this._ackSoon();
                            return;
                    }
                }

                _finish(sendBye, why) {
                    if (this._closed) return;
                    if (sendBye) this._tx({ t: 'B' });
                    this._closed = true;
                    this._closing = false;
                    this.open = false;
                    clearInterval(this._timer);
                    clearTimeout(this._ackTimer);
                    this._out = [];
                    this._hold.clear();
                    this._p._forget(this);
                    this.closeReason = why || '';
                    dbg('bağlantı kapandı:', this._role, why || '');
                    this.emit('close');
                }
            }

            /* ---------- Peer (PeerJS Peer eşleniği) ---------- */
            class Peer extends Emitter {
                constructor(id, options) {
                    super();
                    if (id && typeof id === 'object') { options = id; id = null; }
                    this.options = options || {};
                    this.id = id || ('misafir-' + rid(10));
                    this.open = false;
                    this.disconnected = true;
                    this.destroyed = false;
                    this._isHost = !!id;
                    this._hs = rid(8);
                    this._keys = null;
                    this._conns = new Map();       // host: cihazKimliği -> bağlantı | misafir: oturum -> bağlantı
                    this._closedSids = [];
                    this._everOpen = false;

                    const urls = (Array.isArray(global.SOS_NET_BROKERS) && global.SOS_NET_BROKERS.length) ? global.SOS_NET_BROKERS
                        : ((this.options.brokers && this.options.brokers.length) ? this.options.brokers : DEFAULT_BROKERS);
                    const hooks = {
                        state: () => this._refresh(),
                        message: (topic, payload) => this._onMessage(topic, payload)
                    };
                    this._links = urls.map(u => new Link(u, hooks));

                    if (this._isHost) {
                        roomKeys(id).then(keys => {
                            if (this.destroyed) return;
                            this._keys = keys;
                            this._links.forEach(l => l.sub(keys.root + '/h'));
                            this._refresh();
                        });
                    }

                    this._openTimer = setTimeout(() => {
                        if (!this.destroyed && !this._everOpen) {
                            const err = new Error('Aktarma sunucularına ulaşılamadı');
                            err.type = 'network';
                            this.emit('error', err);
                        }
                    }, OPEN_TIMEOUT_MS);

                    this._onVisible = () => { if (!global.document || !document.hidden) this._wake(); };
                    this._onOnline = () => this._wake();
                    this._onHide = () => { this._conns.forEach(c => { if (c.open) c._tx({ t: 'B' }); }); };
                    if (global.addEventListener) {
                        if (global.document) document.addEventListener('visibilitychange', this._onVisible);
                        global.addEventListener('online', this._onOnline);
                        global.addEventListener('pageshow', this._onOnline);
                        global.addEventListener('focus', this._onOnline);
                        global.addEventListener('pagehide', this._onHide);
                    }
                }

                /* Dışa açık API */
                connect(targetId) {
                    if (this.destroyed) throw new Error('Peer kapatılmış');
                    const c = new DataConn(this, { role: 'guest', target: targetId });
                    this._conns.set(c._sid, c);
                    return c;
                }

                reconnect() { this._wake(); }

                destroy() {
                    if (this.destroyed) return;
                    Array.from(this._conns.values()).forEach(c => c._finish(true, 'peer kapatıldı'));
                    this.destroyed = true;
                    this.open = false;
                    this.disconnected = true;
                    clearTimeout(this._openTimer);
                    if (global.removeEventListener) {
                        if (global.document) document.removeEventListener('visibilitychange', this._onVisible);
                        global.removeEventListener('online', this._onOnline);
                        global.removeEventListener('pageshow', this._onOnline);
                        global.removeEventListener('focus', this._onOnline);
                        global.removeEventListener('pagehide', this._onHide);
                    }
                    const links = this._links;
                    setTimeout(() => links.forEach(l => l.close()), 500); // "ayrıldım" mesajı çıksın
                    this.emit('close');
                }

                /* İç işleyiş */
                _wake() {
                    if (this.destroyed) return;
                    this._links.forEach(l => l.kick());
                    this._conns.forEach(c => { c._lastRx = now(); c._flush(); });
                }

                _refresh() {
                    if (this.destroyed) return;
                    const up = this._links.some(l => l.ready) && (!this._isHost || !!this._keys);
                    if (up && !this.open) {
                        this.open = true;
                        this.disconnected = false;
                        if (!this._everOpen) {
                            this._everOpen = true;
                            clearTimeout(this._openTimer);
                            this.emit('open', this.id);
                        } else {
                            this.emit('reconnected');
                        }
                    } else if (!up && this.open) {
                        this.open = false;
                        this.disconnected = true;
                        this.emit('disconnected');
                    }
                    if (up) this._conns.forEach(c => c._flush());
                }

                _publish(topic, bytes) {
                    let sent = false;
                    this._links.forEach(l => { if (l.pub(topic, bytes)) sent = true; });
                    return sent;
                }

                _forget(c) {
                    if (c._role === 'host') {
                        if (this._conns.get(c._cid) === c) this._conns.delete(c._cid);
                        this._closedSids.push(c._cid + '|' + c._sid);
                        if (this._closedSids.length > 60) this._closedSids.shift();
                    } else {
                        this._conns.delete(c._sid);
                    }
                }

                _sendRaw(topic, frame) {
                    if (!this._keys) return;
                    seal(this._keys, frame).then(bytes => { this._publish(topic, bytes); }).catch(() => { });
                }

                _onMessage(topic, payload) {
                    if (this.destroyed) return;
                    if (this._isHost) {
                        if (!this._keys || topic !== this._keys.root + '/h') return;
                        unseal(this._keys, payload).then(f => { if (f && !this.destroyed) this._hostFrame(f); });
                        return;
                    }
                    this._conns.forEach(c => {
                        if (c._rxTopic !== topic || c._closed) return;
                        unseal(c._keys, payload).then(f => {
                            if (!f || c._closed || f.s !== c._sid) return;
                            if (f.t !== 'A' && f.t !== 'R' && f.t !== 'X' && f.h !== c._hs) return; // eski ev sahibi oturumundan kalan
                            c._onFrame(f);
                        });
                    });
                }

                _hostFrame(f) {
                    if (typeof f.c !== 'string' || typeof f.s !== 'string' || !f.c || !f.s) return;
                    let c = this._conns.get(f.c);
                    if (f.t === 'H') {
                        if (c && c._sid === f.s) { c._lastRx = now(); c._tx({ t: 'A' }); return; } // tekrar gelen selam
                        if (this._closedSids.indexOf(f.c + '|' + f.s) !== -1) {
                            this._sendRaw(this._keys.root + '/c/' + f.c, { t: (c ? 'X' : 'R'), s: f.s });
                            return;
                        }
                        if (c) {
                            this._sendRaw(this._keys.root + '/c/' + f.c, { t: 'X', s: c._sid });
                            c._finish(false, 'aynı cihazdan yeni oturum');
                        }
                        c = new DataConn(this, { role: 'host', cid: f.c, sid: f.s });
                        this._conns.set(f.c, c);
                        c._tx({ t: 'A' });
                        this.emit('connection', c);
                        return;
                    }
                    if (!c || c._sid !== f.s || f.h !== this._hs) {
                        // Tanımadığımız oturum. Aynı cihazın yeni oturumu içerideyse "devralındı" (X),
                        // değilse (zaman aşımı / ev sahibi yenilendi) "baştan katıl" (R) de.
                        if (f.t !== 'B') {
                            const superseded = !!(c && c._sid !== f.s);
                            this._sendRaw(this._keys.root + '/c/' + f.c, { t: superseded ? 'X' : 'R', s: f.s });
                        }
                        return;
                    }
                    c._onFrame(f);
                }
            }

            global.Peer = Peer;
            global.SosRelay = { version: 2, brokers: DEFAULT_BROKERS, _Link: Link };
        })(window);