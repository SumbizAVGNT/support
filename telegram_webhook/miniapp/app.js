(function () {
  const tg = window.Telegram && window.Telegram.WebApp;
  if (tg) { try { tg.expand(); tg.MainButton.hide(); } catch (e) {} }

  const chatEl = document.getElementById('chat');
  const statusEl = document.getElementById('status');
  const textEl = document.getElementById('text');
  const fileEl = document.getElementById('file');
  const sendBtn = document.getElementById('sendBtn');
  const closeBtn = document.getElementById('closeBtn');
  const toastEl = document.getElementById('toast');

  let token = null;
  let convId = null;
  let lastId = 0;
  let polling = false;
  let closed = false;

  function showToast(msg, ms = 2200) {
    toastEl.textContent = msg;
    toastEl.hidden = false;
    setTimeout(() => { toastEl.hidden = true; }, ms);
  }
  function el(tag, cls, text) {
    const e = document.createElement(tag);
    if (cls) e.className = cls;
    if (text !== undefined && text !== null) e.textContent = text;
    return e;
  }
  function fmt(ts) {
    const d = new Date(ts * 1000);
    return d.toLocaleString();
  }
  function addMsg(m) {
    const wrap = el('div', 'msg ' + (m.from === 'user' ? 'self' : 'other'));
    if (m.text) wrap.appendChild(el('div', 'text', m.text));
    (m.attachments || []).forEach(a => {
      const link = el('a', 'attach', '📎 ' + (a.name || a.url));
      link.href = a.url; link.target = '_blank'; link.rel = 'noreferrer noopener';
      wrap.appendChild(link);
    });
    wrap.appendChild(el('div', 'meta', (m.from === 'user' ? 'Вы' : 'Агент') + ' • ' + fmt(m.ts)));
    chatEl.appendChild(wrap);
    window.scrollTo({ top: document.body.scrollHeight, behavior: 'smooth' });
  }

  async function jfetch(url, opts = {}) {
    const headers = opts.headers || {};
    if (token) headers['X-WebApp-Session'] = token;
    opts.headers = headers;
    const res = await fetch(url, opts);
    let js = {};
    try { js = await res.json(); } catch (e) {}
    if (!res.ok || js.ok === false) {
      const msg = js.error || ('HTTP ' + res.status);
      throw new Error(msg);
    }
    return js;
  }

  async function auth() {
    const initData = (tg && tg.initData) || '';
    if (!initData) {
      showToast('Откройте внутри Telegram');
      statusEl.textContent = 'ошибка';
      throw new Error('No initData');
    }
    const js = await jfetch('/api/webapp/auth', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ init_data: initData })
    });
    token = js.token;
    convId = js.conversation_id;
    statusEl.textContent = 'Тикет #' + convId;
  }

  async function loadHistory(after = 0) {
    const url = after ? `/api/webapp/history?after=${after}` : '/api/webapp/history';
    const js = await jfetch(url);
    const list = js.messages || [];
    if (!after) chatEl.innerHTML = '';
    if (list.length) {
      list.forEach(addMsg);
      lastId = list[list.length - 1].id;
    }
  }

  async function poll() {
    if (polling || !token || closed) return;
    polling = true;
    try {
      const js = await jfetch(`/api/webapp/history?after=${lastId}`);
      const list = js.messages || [];
      if (list.length) {
        list.forEach(addMsg);
        lastId = list[list.length - 1].id;
      }
    } catch (e) {
      // молчим
    } finally {
      polling = false;
    }
  }

  async function send() {
    if (closed) return;
    const t = (textEl.value || '').trim();
    const f = fileEl.files[0];
    if (!t && !f) return;

    try {
      if (f) {
        const fd = new FormData();
        fd.append('file', f);
        if (t) fd.append('text', t);
        await jfetch('/api/webapp/send_file', { method: 'POST', body: fd });
        fileEl.value = '';
      } else {
        await jfetch('/api/webapp/send', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ text: t })
        });
      }
      textEl.value = '';
      setTimeout(poll, 250);
    } catch (e) {
      showToast('Не отправлено: ' + e.message);
    }
  }

  async function closeTicket() {
    if (closed) return;
    if (!confirm('Закрыть тикет?')) return;
    try {
      const js = await jfetch('/api/webapp/close', { method: 'POST' });
      if (js.ok) {
        closed = true;
        statusEl.textContent = 'Закрыт';
        showToast('Тикет закрыт');
      }
    } catch (e) {
      showToast('Ошибка закрытия: ' + e.message);
    }
  }

  sendBtn.addEventListener('click', send);
  textEl.addEventListener('keydown', (ev) => {
    if (ev.key === 'Enter' && !ev.shiftKey) { ev.preventDefault(); send(); }
  });
  closeBtn.addEventListener('click', closeTicket);

  (async () => {
    try {
      await auth();
      await loadHistory(0);
      setInterval(poll, 2000);
    } catch (e) {
      showToast('Ошибка инициализации: ' + e.message, 3000);
    }
  })();
})();
