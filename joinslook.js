// ============================================================
// JoinsLookP2P — framework P2P com DSL SQL + multi-peer + STUN/TURN
// ============================================================

const SQL = {
  parse(raw) {
    const s = raw.trim().replace(/\s+/g, ' ');
    let m;
    m = s.match(/^CREATE ROOM '([^']+)' AS LATTICE\((\d+)\)$/i);
    if (m) return { type: 'create', name: m[1], size: +m[2] };
    m = s.match(/^JOIN '([^']+)'$/i);
    if (m) return { type: 'join', code: m[1] };
    m = s.match(/^INSERT INTO (\w+) \(([^)]+)\) VALUES \((.+)\)$/i);
    if (m) return {
      type: 'insert', table: m[1],
      cols: m[2].split(',').map(x => x.trim()),
      vals: this.splitValues(m[3])
    };
    m = s.match(/^SELECT \* FROM (\w+)(?: WHERE (\w+) (=|>|<) '?([^']+)'?)?$/i);
    if (m) return {
      type: 'select', table: m[1],
      where: m[2] ? { col: m[2], op: m[3], val: m[4] } : null
    };
    m = s.match(/^DELETE FROM (\w+) WHERE (\w+) = '([^']+)'$/i);
    if (m) return { type: 'delete', table: m[1], where: { col: m[2], val: m[3] } };
    m = s.match(/^UPDATE ME SET (\w+) = '([^']+)'$/i);
    if (m) return { type: 'update_me', col: m[1], val: m[2] };
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
  castVal(s) {
    if (s.startsWith("'") && s.endsWith("'")) return s.slice(1, -1);
    if (s === 'true') return true;
    if (s === 'false') return false;
    if (!isNaN(+s)) return +s;
    return s;
  }
};

async function sha256(str) {
  const buf = new TextEncoder().encode(str);
  const h = await crypto.subtle.digest('SHA-256', buf);
  return [...new Uint8Array(h)].map(b => b.toString(16).padStart(2, '0')).join('');
}

function normalizeSdp(sdp) {
  return sdp.trim().replace(/\r\n|\r|\n/g, '\n')
    .split('\n').map(l => l.trimEnd()).join('\n')
    .replace(/\n/g, '\r\n') + '\r\n';
}

class JoinsLookP2P {
  constructor() {
    this.tables = new Map();
    this.listeners = [];
    this.slots = new Map();
    this.peerDirectory = new Map();
    this.peers = new Map();
    this.connectedPeers = new Set();
    this.peerId = 'P' + Math.random().toString(36).slice(2, 8).toUpperCase();
    this.role = null;
    this.options = {};
    this.maxDirectPeers = 6;
    this.seenMessages = new Set();
    this.messageTTL = 5;
    this.me = { peerId: this.peerId };
  }

  static async create(sql, options = {}) {
    const stmt = SQL.parse(sql);
    if (stmt.type !== 'create') throw new Error('Use CREATE ROOM');
    const room = new JoinsLookP2P();
    room.role = 'host';
    room.name = stmt.name;
    room.latticeSize = stmt.size;
    room.options = options;
    room.code = (await sha256(stmt.name + '|' + stmt.size)).slice(0, 8).toUpperCase();
    return room;
  }

  static async join(code, options = {}) {
    const room = new JoinsLookP2P();
    room.role = 'peer';
    room.code = code.toUpperCase();
    room.options = options;
    return room;
  }

  async exec(sql) {
    const stmt = SQL.parse(sql);
    if (stmt.type === 'insert') {
      const row = { _id: this._uid(), _from: this.peerId, _ts: Date.now() };
      stmt.cols.forEach((c, i) => row[c] = stmt.vals[i]);
      this._addRow(stmt.table, row);
      this.seenMessages.add(row._id);
      this._gossip({ op: 'insert', table: stmt.table, row });
      return row;
    }
    if (stmt.type === 'select') return this._query(stmt);
    if (stmt.type === 'delete') {
      const rows = this.tables.get(stmt.table) || [];
      this.tables.set(stmt.table, rows.filter(r => String(r[stmt.where.col]) !== stmt.where.val));
      this._notify(stmt.table);
      this._gossip({ op: 'delete', table: stmt.table, where: stmt.where });
      return;
    }
    if (stmt.type === 'update_me') {
      this.me[stmt.col] = stmt.val;
      this._gossip({ op: 'me', peerId: this.peerId, col: stmt.col, val: stmt.val });
      return this.me;
    }
    throw new Error('Comando inválido: ' + stmt.type);
  }

  on(sql, callback) {
    const stmt = SQL.parse(sql);
    const entry = { stmt, callback };
    this.listeners.push(entry);
    callback(this._query(stmt));
    return () => { this.listeners = this.listeners.filter(l => l !== entry); };
  }

  _uid() { return Math.random().toString(36).slice(2, 10) + Date.now().toString(36).slice(-4); }

  _addRow(table, row) {
    if (!this.tables.has(table)) this.tables.set(table, []);
    const rows = this.tables.get(table);
    if (rows.some(r => r._id === row._id)) return;
    rows.push(row);
    this._notify(table);
  }

  _query(stmt) {
    const rows = this.tables.get(stmt.table) || [];
    if (!stmt.where) return [...rows];
    const { col, op, val } = stmt.where;
    return rows.filter(r => {
      const v = String(r[col]);
      if (op === '=') return v === val;
      if (op === '>') return +v > +val;
      if (op === '<') return +v < +val;
      return false;
    });
  }

  _notify(table) {
    for (const { stmt, callback } of this.listeners) {
      if (stmt.table === table) callback(this._query(stmt));
    }
  }

  _gossip(msg) {
    msg._ttl = this.messageTTL;
    this._sendToNeighbors(msg);
  }

  _sendToNeighbors(msg) {
    const json = JSON.stringify(msg);
    if (this.role === 'peer') {
      for (const { dc } of this.slots.values()) {
        if (dc?.readyState === 'open') dc.send(json);
      }
      return;
    }
    const neighbors = [...this.slots.entries()]
      .filter(([_, s]) => s.dc?.readyState === 'open')
      .slice(0, this.maxDirectPeers);
    for (const [_, slot] of neighbors) {
      if (slot.dc?.readyState === 'open') slot.dc.send(json);
    }
  }

  _receive(msg, fromSlot) {
    const key = msg.row?._id;
    if (key && this.seenMessages.has(key)) return;
    if (key) this.seenMessages.add(key);

    if (msg.op === 'insert') {
      this._addRow(msg.table, msg.row);
    } else if (msg.op === 'delete') {
      const rows = this.tables.get(msg.table) || [];
      this.tables.set(msg.table, rows.filter(r => String(r[msg.where.col]) !== msg.where.val));
      this._notify(msg.table);
    } else if (msg.op === 'me') {
      this.peers.set(msg.peerId, { ...this.peers.get(msg.peerId), [msg.col]: msg.val });
    }

    if (msg._ttl > 0) {
      msg._ttl--;
      this._sendToNeighbors(msg);
    }
  }

  async _newPC() {
    // ⚡ STUN + TURN (OpenRelay) para furar NATs restritivos de 4G/5G
    const pc = new RTCPeerConnection({
      iceServers: [
        { urls: 'stun:stun.l.google.com:19302' },
        {
          urls: [
            'turn:openrelay.metered.ca:80',
            'turn:openrelay.metered.ca:443',
            'turn:openrelay.metered.ca:443?transport=tcp'
          ],
          username: 'openrelayproject',
          credential: 'openrelayproject'
        }
      ]
    });
    pc.oniceconnectionstatechange = () => this.onState?.(pc.iceConnectionState);
    return pc;
  }

  async _waitIce(pc, timeout = 10000) {
    return new Promise(res => {
      if (pc.iceGatheringState === 'complete') return res();
      const check = () => {
        if (pc.iceGatheringState === 'complete') {
          pc.removeEventListener('icegatheringstatechange', check);
          clearTimeout(t);
          res();
        }
      };
      pc.addEventListener('icegatheringstatechange', check);
      const t = setTimeout(() => {
        pc.removeEventListener('icegatheringstatechange', check);
        res();
      }, timeout);
    });
  }

  _wireSlot(slotId, dc) {
    dc.onopen = () => {
      try {
        dc.send(JSON.stringify({ op: 'hello', peerId: this.peerId }));
        if (this.role === 'host') {
          for (const [table, rows] of this.tables) {
            for (const row of rows) {
              dc.send(JSON.stringify({ op: 'insert', table, row }));
            }
          }
          dc.send(JSON.stringify({ op: 'directory', peers: [...this.peerDirectory.keys()] }));
        }
      } catch (e) { console.error('[onopen send]', e); }
      this.onConnect?.();
      this.onPeerConnect?.(slotId);
    };

    dc.onclose = () => {
      for (const [peerId, sid] of [...this.peerDirectory]) {
        if (sid === slotId) this.peerDirectory.delete(peerId);
      }
      this.slots.delete(slotId);
      if (this.role === 'host') this._broadcastDirectory();
      this.onDisconnect?.();
      this.onPeerDisconnect?.(slotId);
    };

    dc.onmessage = e => {
      try {
        const msg = JSON.parse(e.data);
        if (msg.op === 'hello') {
          if (this.role === 'host') {
            this.peerDirectory.set(msg.peerId, slotId);
            this._broadcastDirectory();
            this.onDirectoryChange?.();
          }
          return;
        }
        if (msg.op === 'directory') {
          this.connectedPeers = new Set(msg.peers.filter(p => p !== this.peerId));
          this.onDirectoryChange?.();
          return;
        }
        this._receive(msg, slotId);
      } catch (err) { console.warn('msg inválida', err); }
    };
  }

  _broadcastDirectory() {
    const json = JSON.stringify({ op: 'directory', peers: [...this.peerDirectory.keys()] });
    for (const { dc } of this.slots.values()) {
      if (dc?.readyState === 'open') dc.send(json);
    }
  }

  async createOfferForPeer(slotId) {
    slotId = slotId || 'peer-' + this._uid().slice(0, 6);
    const pc = await this._newPC();
    const dc = pc.createDataChannel('joinslook');
    this.slots.set(slotId, { pc, dc });
    this._wireSlot(slotId, dc);
    const offer = await pc.createOffer();
    await pc.setLocalDescription(offer);
    await this._waitIce(pc);
    return { slotId, sdp: pc.localDescription.sdp };
  }

  async acceptAnswerForSlot(slotId, answerSdp) {
    const slot = this.slots.get(slotId);
    if (!slot) throw new Error('Slot não encontrado: ' + slotId);
    if (slot.pc.signalingState !== 'have-local-offer') {
      throw new Error('Slot já conectado ou estado inválido: ' + slot.pc.signalingState);
    }
    await slot.pc.setRemoteDescription({ type: 'answer', sdp: normalizeSdp(answerSdp) });
  }

  async acceptOffer(offerSdp) {
    const slotId = 'host-' + this._uid().slice(0, 6);
    const pc = await this._newPC();
    pc.ondatachannel = e => {
      this.slots.set(slotId, { pc, dc: e.channel });
      this._wireSlot(slotId, e.channel);
    };
    await pc.setRemoteDescription({ type: 'offer', sdp: normalizeSdp(offerSdp) });
    const ans = await pc.createAnswer();
    await pc.setLocalDescription(ans);
    await this._waitIce(pc);
    return { slotId, sdp: pc.localDescription.sdp };
  }
}
