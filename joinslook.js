// ============================================================
// JoinsLookP2P — Framework P2P (Server + Client Sincronizados)
// ============================================================

const SQL = {
  parse(raw) {
    const s = raw.trim().replace(/\s+/g, ' ');
    let m;
    if ((m = s.match(/^INSERT INTO (\w+) \(([^)]+)\) VALUES \((.+)\)$/i))) 
      return { type: 'insert', table: m[1], cols: m[2].split(',').map(x => x.trim()), vals: this.splitValues(m[3]) };
    if ((m = s.match(/^UPDATE (\w+) SET (.+) WHERE (\w+) = '([^']+)'$/i))) 
      return { type: 'update', table: m[1], set: this.parseSet(m[2]), where: { col: m[3], val: m[4] } };
    if ((m = s.match(/^SELECT \* FROM (\w+)(?: WHERE (\w+) (=|>|<) '?([^']+)'?)?$/i))) 
      return { type: 'select', table: m[1], where: m[2] ? { col: m[2], op: m[3], val: m[4] } : null };
    if ((m = s.match(/^DELETE FROM (\w+) WHERE (\w+) = '([^']+)'$/i))) 
      return { type: 'delete', table: m[1], where: { col: m[2], val: m[3] } };
    throw new Error('SQL inválido: ' + raw);
  },
  splitValues(raw) {
    const out = []; let buf = '', inStr = false;
    for (const c of raw) {
      if (c === "'") { inStr = !inStr; buf += c; }
      else if (c === ',' && !inStr) { out.push(this.castVal(buf.trim())); buf = ''; }
      else buf += c;
    }
    if (buf.trim()) out.push(this.castVal(buf.trim()));
    return out;
  },
  parseSet(raw) {
    const out = {};
    raw.split(',').forEach(pair => {
      const [k, v] = pair.split('=').map(x => x.trim());
      out[k] = this.castVal(v);
    });
    return out;
  },
  castVal(s) {
    if (s.startsWith("'") && s.endsWith("'")) return s.slice(1, -1);
    if (!isNaN(+s)) return +s;
    return s;
  }
};

// ============================================================
// CLASSE 1: SERVIDOR (Autoridade Total)
// ============================================================
class JoinsLookServer {
  constructor(manualIp = '') {
    this.tables = new Map();
    this.listeners = [];
    this.slots = new Map();
    this.peerDirectory = new Map();
    this.manualIp = manualIp;
    this.blockHeight = 0;
    this.db = null;
    this.serverKey = 'sk_' + Math.random().toString(36).slice(2, 10); 
  }

  async start(isNewGame = true) {
    await this._initDB();
    if (isNewGame) await this._clearDB();
    else await this._loadFromDB();

    const pc = await this._newPC();
    const dc = pc.createDataChannel('game');
    this.slots.set('host', { pc, dc });
    this._wireSlot(dc, 'host');
    
    await pc.setLocalDescription(await pc.createOffer());
    await this._waitIce(pc);
    
    const sdp = this._replaceMdns(pc.localDescription.sdp);
    const b64 = btoa(encodeURIComponent(sdp)).replace(/\+/g, '-').replace(/\//g, '_').replace(/=/g, '');
    return `${window.location.href.split('#')[0]}#offer=${b64}&key=${this.serverKey}`;
  }

  async acceptAnswer(link) {
    const sdp = this._extractSdp(link);
    const slot = this.slots.get('host');
    if (slot && slot.pc.signalingState !== 'stable') {
      await slot.pc.setRemoteDescription({ type: 'answer', sdp: this._normalizeSdp(sdp) });
    }
  }

  async resetGame() {
    this.tables.clear();
    this.blockHeight = 0;
    await this._clearDB();
    this._broadcast({ op: 'server_reset' });
  }

  onAction(callback) { this._actionCallback = callback; }

  onChange(sql, callback) {
    const stmt = SQL.parse(sql);
    this.listeners.push({ stmt, callback });
    callback(this._query(stmt));
  }

  async exec(sql) {
    const stmt = SQL.parse(sql);

    if (stmt.type === 'insert') {
      const row = { _id: Math.random().toString(36).slice(2), _from: 'HOST', _ts: Date.now() };
      stmt.cols.forEach((c, i) => row[c] = stmt.vals[i]);
      this._addRow(stmt.table, row);
      this._broadcast({ op: 'insert', table: stmt.table, row });
      await this._saveToDB();
      return row;
    }

    if (stmt.type === 'update') {
      const rows = this.tables.get(stmt.table) || [];
      const row = rows.find(r => String(r[stmt.where.col]) === stmt.where.val);
      if (row) {
        Object.assign(row, stmt.set);
        this._notify(stmt.table);
        this.blockHeight++;
        this._broadcast({ op: 'update', table: stmt.table, where: stmt.where, set: stmt.set, block: this.blockHeight });
        await this._saveToDB();
      }
      return row;
    }

    if (stmt.type === 'delete') {
      const rows = this.tables.get(stmt.table) || [];
      this.tables.set(stmt.table, rows.filter(r => String(r[stmt.where.col]) !== stmt.where.val));
      this._notify(stmt.table);
      this._broadcast({ op: 'delete', table: stmt.table, where: stmt.where });
      await this._saveToDB();
    }

    if (stmt.type === 'select') return this._query(stmt);
  }

  _broadcast(msg) {
    const json = JSON.stringify(msg);
    for (const { dc } of this.slots.values()) {
      if (dc?.readyState === 'open') dc.send(json);
    }
  }

  _wireSlot(dc, slotId) {
    dc.onopen = () => {
      dc.send(JSON.stringify({ op: 'hello' }));
      for (const [table, rows] of this.tables) {
        for (const row of rows) dc.send(JSON.stringify({ op: 'insert', table, row }));
      }
      dc.send(JSON.stringify({ op: 'sync', block: this.blockHeight }));
      this.onConnect?.();
    };
    
    dc.onmessage = e => {
      const msg = JSON.parse(e.data);
      if (msg.op === 'join') {
        if (msg.key !== this.serverKey) { 
          console.warn('Chave inválida do cliente:', msg.key, 'Esperado:', this.serverKey);
          dc.close(); return; 
        }
        this.peerDirectory.set(msg.peerId, slotId);
      } else if (msg.op === 'action') {
        if (this._actionCallback) this._actionCallback(msg.action);
      }
    };
  }

  _addRow(table, row) {
    if (!this.tables.has(table)) this.tables.set(table, []);
    const rows = this.tables.get(table);
    if (!rows.some(r => r._id === row._id)) {
      rows.push(row);
      this._notify(table);
    }
  }
  
  _query(stmt) {
    const rows = this.tables.get(stmt.table) || [];
    if (!stmt.where) return [...rows];
    return rows.filter(r => String(r[stmt.where.col]) === stmt.where.val);
  }
  
  _notify(table) {
    for (const { stmt, callback } of this.listeners) {
      if (stmt.table === table) callback(this._query(stmt));
    }
  }

  async _initDB() {
    return new Promise((resolve) => {
      const req = indexedDB.open('JoinsLookDB', 1);
      req.onupgradeneeded = e => e.target.result.createObjectStore('state');
      req.onsuccess = e => { this.db = e.target.result; resolve(); };
    });
  }
  async _clearDB() {
    if (!this.db) return;
    const tx = this.db.transaction('state', 'readwrite');
    tx.objectStore('state').clear();
  }
  async _loadFromDB() {
    if (!this.db) return;
    return new Promise(resolve => {
      const tx = this.db.transaction('state', 'readonly');
      const req = tx.objectStore('state').get('current');
      req.onsuccess = () => {
        if (req.result) {
          const data = JSON.parse(req.result);
          this.tables = new Map(data.tables);
          this.blockHeight = data.blockHeight || 0;
        }
        resolve();
      };
    });
  }
  async _saveToDB() {
    if (!this.db) return;
    const data = JSON.stringify({ tables: [...this.tables], blockHeight: this.blockHeight });
    const tx = this.db.transaction('state', 'readwrite');
    tx.objectStore('state').put(data, 'current');
  }

  async _newPC() { return new RTCPeerConnection({ iceServers: [{ urls: 'stun:stun.l.google.com:19302' }] }); }
  async _waitIce(pc) {
    return new Promise(res => {
      if (pc.iceGatheringState === 'complete') return res();
      pc.addEventListener('icegatheringstatechange', () => { if (pc.iceGatheringState === 'complete') res(); });
      setTimeout(res, 3000);
    });
  }
  _replaceMdns(sdp) { return this.manualIp ? sdp.replace(/[a-zA-Z0-9-]+\.local/g, this.manualIp) : sdp; }
  _normalizeSdp(sdp) { return sdp.trim().replace(/\r\n|\r|\n/g, '\n').split('\n').map(l => l.trimEnd()).join('\n').replace(/\n/g, '\r\n') + '\r\n'; }
  
  // BLINDAGEM DO LINK
  _extractSdp(input) {
    if (!input) throw new Error('O campo está vazio.');
    const t = input.trim();
    
    // Se for o SDP puro
    if (t.startsWith('v=0')) return t;

    // Procura por offer= ou answer= e captura apenas caracteres válidos de Base64 URL
    const match = t.match(/(?:offer|answer)=([a-zA-Z0-9\-_]+)/);
    if (!match) {
      console.log("Texto recebido:", t);
      throw new Error('Link inválido. Certifique-se de copiar o link inteiro.');
    }

    let b64 = match[1].replace(/\s+/g, '').replace(/-/g, '+').replace(/_/g, '/');
    
    // Corrige o padding
    while (b64.length % 4) b64 += '=';

    try {
      const decoded = decodeURIComponent(atob(b64));
      if (!decoded.startsWith('v=0')) throw new Error('Código não é um SDP válido.');
      return decoded;
    } catch (e) {
      console.error("Erro no Base64:", e);
      throw new Error('Link corrompido. Copie o link completo novamente.');
    }
  }
}

// ============================================================
// CLASSE 2: CLIENTE (Apenas Jogador)
// ============================================================
class JoinsLookClient {
  constructor(manualIp = '') {
    this.tables = new Map();
    this.listeners = [];
    this.manualIp = manualIp;
    this.pc = null;
    this.dc = null;
    this.serverKey = null;
    this.peerId = 'P' + Math.random().toString(36).slice(2, 6).toUpperCase();
  }

  async join(link) {
    // Captura a chave do servidor
    const keyMatch = link.match(/key=([a-zA-Z0-9_]+)/);
    this.serverKey = keyMatch ? keyMatch[1] : 'unknown';

    const sdp = this._extractSdp(link);
    this.pc = await this._newPC();
    
    this.pc.ondatachannel = e => {
      this.dc = e.channel;
      this._wireSlot();
    };

    await this.pc.setRemoteDescription({ type: 'offer', sdp: this._normalizeSdp(sdp) });
    await this.pc.setLocalDescription(await this.pc.createAnswer());
    await this._waitIce(this.pc);

    const sdpAns = this._replaceMdns(this.pc.localDescription.sdp);
    const b64 = btoa(encodeURIComponent(sdpAns)).replace(/\+/g, '-').replace(/\//g, '_').replace(/=/g, '');
    return `${window.location.href.split('#')[0]}#answer=${b64}`;
  }

  sendAction(action) {
    if (this.dc?.readyState === 'open') {
      this.dc.send(JSON.stringify({ op: 'action', action }));
    }
  }

  onChange(sql, callback) {
    const stmt = SQL.parse(sql);
    this.listeners.push({ stmt, callback });
    callback(this._query(stmt));
  }

  _wireSlot() {
    this.dc.onopen = () => {
      this.dc.send(JSON.stringify({ op: 'join', peerId: this.peerId, key: this.serverKey }));
      this.onConnect?.();
    };
    
    this.dc.onmessage = e => {
      const msg = JSON.parse(e.data);
      
      if (msg.op === 'insert') {
        this._addRow(msg.table, msg.row);
      } 
      else if (msg.op === 'update') {
        const rows = this.tables.get(msg.table) || [];
        const row = rows.find(r => String(r[msg.where.col]) === msg.where.val);
        if (row) {
          Object.assign(row, msg.set);
          this._notify(msg.table);
        }
      } 
      else if (msg.op === 'delete') {
        const rows = this.tables.get(msg.table) || [];
        this.tables.set(msg.table, rows.filter(r => String(r[msg.where.col]) !== msg.where.val));
        this._notify(msg.table);
      } 
      else if (msg.op === 'server_reset') {
        this.tables.clear();
        for (const { stmt } of this.listeners) this._notify(stmt.table);
        this.onReset?.();
      }
    };
  }

  _addRow(table, row) {
    if (!this.tables.has(table)) this.tables.set(table, []);
    const rows = this.tables.get(table);
    if (!rows.some(r => r._id === row._id)) {
      rows.push(row);
      this._notify(table);
    }
  }

  _notify(table) {
    for (const { stmt, callback } of this.listeners) {
      if (stmt.table === table) callback(this._query(stmt));
    }
  }

  _query(stmt) {
    const rows = this.tables.get(stmt.table) || [];
    if (!stmt.where) return [...rows];
    return rows.filter(r => String(r[stmt.where.col]) === stmt.where.val);
  }

  async _newPC() { return new RTCPeerConnection({ iceServers: [{ urls: 'stun:stun.l.google.com:19302' }] }); }
  async _waitIce(pc) {
    return new Promise(res => {
      if (pc.iceGatheringState === 'complete') return res();
      pc.addEventListener('icegatheringstatechange', () => { if (pc.iceGatheringState === 'complete') res(); });
      setTimeout(res, 3000);
    });
  }
  _replaceMdns(sdp) { return this.manualIp ? sdp.replace(/[a-zA-Z0-9-]+\.local/g, this.manualIp) : sdp; }
  _normalizeSdp(sdp) { return sdp.trim().replace(/\r\n|\r|\n/g, '\n').split('\n').map(l => l.trimEnd()).join('\n').replace(/\n/g, '\r\n') + '\r\n'; }
  
  // MESMA BLINDAGEM NO CLIENTE
  _extractSdp(input) {
    if (!input) throw new Error('O campo está vazio.');
    const t = input.trim();
    if (t.startsWith('v=0')) return t;

    const match = t.match(/(?:offer|answer)=([a-zA-Z0-9\-_]+)/);
    if (!match) throw new Error('Link inválido. O texto colado não parece um Link Mágico.');

    let b64 = match[1].replace(/\s+/g, '').replace(/-/g, '+').replace(/_/g, '/');
    while (b64.length % 4) b64 += '=';

    try {
      const decoded = decodeURIComponent(atob(b64));
      if (!decoded.startsWith('v=0')) throw new Error('Código não é um SDP válido.');
      return decoded;
    } catch (e) {
      throw new Error('Link corrompido. Copie o link completo novamente.');
    }
  }
}
