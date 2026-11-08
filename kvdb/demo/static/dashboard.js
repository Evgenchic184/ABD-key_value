const $ = (s) => document.querySelector(s),
  colors = ["#65e0b5", "#7bb6ff", "#ffd284"];
let cursor = 0,
  events = [],
  lastRun = null;
function el(tag, text, cls) {
  const n = document.createElement(tag);
  if (text !== undefined) n.textContent = text;
  if (cls) n.className = cls;
  return n;
}
function timeText(t) {
  return t ? new Date(t).toLocaleTimeString("ru-RU") : "—";
}
function option(select, value) {
  if (!Array.from(select.options).some((o) => o.value === value))
    select.add(new Option(value, value));
}
function decoded(value) {
  if (value === null || value === undefined) return "—";
  try {
    const bytes = Uint8Array.from(atob(value), (c) => c.charCodeAt(0));
    try {
      return new TextDecoder("utf-8", { fatal: true }).decode(bytes);
    } catch {
      return `[бинарные данные: ${bytes.length} байт]`;
    }
  } catch {
    return "[неверный Base64]";
  }
}
function eventDetails(e) {
  if (e.kind !== "request") return e.message ?? "";
  if (e.error)
    return `${e.method === "PUT" ? `value=${decoded(e.sent_value)} · ` : ""}${e.error}`;
  if (e.method === "PUT")
    return `value=${decoded(e.sent_value)} → ${e.ok ? "сохранено" : JSON.stringify(e.body)}`;
  if (e.method === "GET" && e.status === 404) return "ключ отсутствует";
  if (e.method === "GET" && e.body?.value !== undefined)
    return `value=${decoded(e.body.value)}`;
  if (e.method === "DELETE" && e.ok) return "удалено";
  return JSON.stringify(e.body);
}
async function apiPost(path, body) {
  const response = await fetch(path, {
    method: "POST",
    ...(body
      ? {
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(body),
        }
      : {}),
  });
  const result = await response.json();
  if (!response.ok)
    throw Error(
      typeof result.detail === "string"
        ? result.detail
        : JSON.stringify(result.detail ?? result),
    );
  return result;
}
const syncBusy = new Set();
async function syncAction(node, action) {
  syncBusy.add(node);
  $("#notice").textContent = "";
  try {
    await apiPost(`/api/replicas/${encodeURIComponent(node)}/${action}`);
  } catch (error) {
    $("#notice").textContent = error.message;
  } finally {
    syncBusy.delete(node);
  }
}
function renderValues(state) {
  const names = [
    "leader",
    ...Object.keys(state.history).filter((n) => n !== "leader"),
  ];
  const heading = el("tr");
  heading.append(el("th", "Ключ"));
  names.forEach((n) => heading.append(el("th", n)));
  $("#values-head").replaceChildren(heading);
  $("#values-body").replaceChildren();
  for (const key of state.user_keys ?? []) {
    const row = el("tr");
    row.append(el("td", key));
    const lead = state.values?.leader?.[key];
    const leadText = lead?.status === 200 ? decoded(lead.value) : null;
    for (const name of names) {
      const sample = state.values?.[name]?.[key],
        node = state.nodes[name];
      const text = sample?.status === 200 ? decoded(sample.value) : "—";
      const stale = !node?.available || !state.nodes.leader?.available;
      const differs =
        !stale &&
        name !== "leader" &&
        lead?.status === 200 &&
        (sample?.status !== 200 || text !== leadText);
      const cell = el("td");
      cell.append(
        el(
          "span",
          text,
          `value-number ${differs ? "warn" : stale ? "muted" : "ok"}`,
        ),
      );
      let detail = stale
        ? "последний ответ · нет связи"
        : sample?.status === 404
          ? "ключ отсутствует"
          : sample?.status === 200
            ? "актуальный опрос"
            : "ожидание";
      if (differs) {
        const delta = Number(leadText) - Number(text);
        detail =
          Number.isFinite(delta) && delta > 0
            ? `на ${delta} ${{ one: "цикл", few: "цикла", many: "циклов", other: "цикла" }[new Intl.PluralRules("ru").select(delta)]} позади`
            : "значение отличается от ведущего";
      }
      cell.append(el("span", detail, "meta " + (differs ? "warn" : "muted")));
      row.append(cell);
    }
    $("#values-body").append(row);
  }
}
function renderNodes(nodes) {
  $("#nodes").replaceChildren();
  $("#transfers").replaceChildren();
  Object.entries(nodes)
    .sort(([a], [b]) =>
      a === "leader" ? -1 : b === "leader" ? 1 : a.localeCompare(b),
    )
    .forEach(([name, n]) => {
      const card = el("article", undefined, "card"),
        head = el("div", undefined, "node-head");
      head.append(el("strong", name));
      const status = el(
        "span",
        undefined,
        n.available ? (n.ready ? "ok" : "warn") : "bad",
      );
      status.append(
        el("span", undefined, "dot"),
        document.createTextNode(
          n.available ? (n.ready ? "доступен" : "загрузка") : "недоступен",
        ),
      );
      head.append(status);
      card.append(
        head,
        el("div", n.last_seq ?? "—", "seq"),
        el("div", "последняя применённая операция", "muted"),
      );
      const stale = n.display_lag_stale || n.lag_stale;
      const lines = [
        `Роль: ${n.role ?? "—"} · состояние: ${n.state ?? "—"}`,
        `Отставание: ${n.display_lag ?? "—"}${stale ? " (оценка устарела)" : ""}`,
        `Последний ответ: ${timeText(n.last_success)}`,
      ];
      if (n.last_error || n.error) lines.push(n.last_error || n.error);
      for (const line of lines) card.append(el("div", line, "meta muted"));
      card.append(
        el(
          "div",
          `Отставание WAL: ${n.display_lag ?? "—"} операций`,
          "lag-number " + (n.display_lag > 0 ? "warn" : "ok"),
        ),
      );
      if (name !== "leader") {
        const button = el(
          "button",
          n.paused
            ? "▶ Возобновить синхронизацию"
            : "Ⅱ Приостановить синхронизацию",
          "sync-button",
        );
        button.disabled =
          !n.available || syncBusy.has(name) || n.state === "error";
        button.addEventListener("click", () =>
          syncAction(name, n.paused ? "resume" : "pause"),
        );
        card.append(button);
        if (n.paused)
          card.append(
            el("div", "Синхронизация на паузе · чтение доступно", "meta warn"),
          );
      }
      $("#nodes").append(card);
      option($("#manual-node"), name);
      option($("#node-filter"), name);
      if (name !== "leader")
        $("#transfers").append(
          el(
            "div",
            `${name}: WAL ${n.wal_operations ?? 0} · снимки ${n.snapshots_installed ?? 0} · последний: ${n.last_transfer ?? "—"}`,
            "meta",
          ),
        );
    });
}
function chart(history) {
  const svg = $("#chart"),
    width = Math.max(320, svg.clientWidth);
  svg.setAttribute("viewBox", `0 0 ${width} 215`);
  svg.replaceChildren();
  const entries = Object.entries(history),
    points = entries.flatMap(([, p]) => p).filter((p) => p.seq !== null);
  $("#legend").replaceChildren();
  entries.forEach(([name], i) => {
    const label = el("span", name);
    label.style.color = colors[i % colors.length];
    $("#legend").append(label);
  });
  if (!points.length) return;
  const now = Date.now() / 1000,
    minT = Math.min(...points.map((p) => p.time)),
    maxT = Math.max(now, minT + 10),
    low = Math.min(...points.map((p) => p.seq)),
    high = Math.max(low + 5, ...points.map((p) => p.seq));
  const make = (tag, attrs) => {
    const n = document.createElementNS("http://www.w3.org/2000/svg", tag);
    Object.entries(attrs).forEach(([k, v]) => n.setAttribute(k, v));
    svg.append(n);
    return n;
  };
  for (let i = 0; i < 4; i++) {
    const y = 15 + i * 55;
    make("line", { x1: 55, y1: y, x2: width - 10, y2: y, stroke: "#26364b" });
    make("text", {
      x: 0,
      y: y + 4,
      fill: "#93a5bd",
      "font-size": 11,
    }).textContent = Math.round(high - ((high - low) * i) / 3);
  }
  entries.forEach(([name, list], i) => {
    let path = "",
      gap = true;
    list.forEach((p) => {
      if (p.seq === null) {
        gap = true;
        return;
      }
      const x = 55 + ((p.time - minT) / (maxT - minT)) * (width - 65),
        y = 15 + ((high - p.seq) / (high - low)) * 165;
      path += `${gap ? "M" : "L"}${x.toFixed(1)},${y.toFixed(1)} `;
      gap = false;
    });
    make("path", {
      d: path,
      fill: "none",
      stroke: colors[i % colors.length],
      "stroke-width": 2,
    });
  });
  make("text", {
    x: 55,
    y: 210,
    fill: "#93a5bd",
    "font-size": 11,
  }).textContent = timeText(minT * 1000);
  make("text", {
    x: width - 65,
    y: 210,
    fill: "#93a5bd",
    "font-size": 11,
  }).textContent = "сейчас";
}
function renderEvents() {
  const user = $("#user-filter").value,
    node = $("#node-filter").value,
    result = $("#result-filter").value;
  const filtered = events.filter(
    (e) =>
      (!user || e.user === user) &&
      (!node || e.node === node) &&
      (!result || (result === "ok" ? e.ok === true : e.ok === false)),
  );
  const fragment = document.createDocumentFragment();
  filtered
    .slice(-250)
    .reverse()
    .forEach((e) => {
      const row = el("tr");
      const details = eventDetails(e);
      const status = e.status ?? (e.kind === "request" ? "NETWORK" : e.kind);
      [
        timeText(e.time),
        e.user ?? "система",
        e.node ?? "—",
        e.method ?? "—",
        e.key ?? "—",
        String(status),
        e.duration_ms ?? "—",
        `${e.outcome === "unknown" ? "НЕИЗВЕСТНО · " : ""}${details ?? ""}`,
      ].forEach((value, i) =>
        row.append(
          el(
            "td",
            value,
            i === 4
              ? "key"
              : i === 7
                ? "result"
                : i === 5
                  ? e.ok === false
                    ? "bad"
                    : e.ok === true
                      ? "ok"
                      : "muted"
                  : "",
          ),
        ),
      );
      fragment.append(row);
    });
  $("#events").replaceChildren(fragment);
}
async function poll() {
  try {
    let response = await fetch("/api/state?after=" + cursor);
    if (!response.ok) throw Error(response.status);
    let s = await response.json();
    if (lastRun && lastRun !== s.run_id) {
      response = await fetch("/api/state?after=0");
      if (!response.ok) throw Error(response.status);
      s = await response.json();
    }
    if (lastRun !== s.run_id) {
      events = [];
      lastRun = s.run_id;
    }
    if (s.events_truncated)
      $("#notice").textContent =
        "Показана последняя часть истории; полный журнал — JSONL.";
    events.push(...s.events);
    events = events.slice(-2000);
    cursor = s.cursor;
    $("#run").textContent = "run " + s.run_id;
    $("#connection").textContent = "● интерфейс подключён";
    $("#connection").className = "badge ok";
    $("#workload").textContent = !s.initialized
      ? "Ожидание контрольного ключа на всех узлах"
      : s.running
        ? "Нагрузка активна"
        : `Нагрузка на паузе · выполняется ${s.active}`;
    $("#verification").textContent =
      s.verification.message ?? "Проверка не запускалась";
    $("#verification").className =
      s.verification.state === "pass"
        ? "ok"
        : s.verification.state === "fail"
          ? "bad"
          : "muted";
    $("#log").textContent = "Журнал: " + s.log_path;
    renderNodes(s.nodes);
    renderValues(s);
    chart(s.history);
    if (s.control) {
      $("#control").textContent = s.control.ok
        ? "Чтение продолжается"
        : "Нет подтверждённого чтения";
      $("#control").className =
        "control-result " + (s.control.ok ? "ok" : "bad");
      $("#control-meta").textContent =
        `${s.control.node} · ${s.control.status ?? "NETWORK"} · ${timeText(s.control.time)}`;
    }
    $("#stats").replaceChildren();
    [
      ["read", "Чтения"],
      ["write", "Записи"],
      ["delete", "Удаления"],
    ].forEach(([key, label]) => {
      const box = el("div", undefined, "box");
      box.append(
        el("div", label, "muted"),
        el("div", `${s.counters[key + "_ok"] ?? 0} успешных`, "stat-number ok"),
        el(
          "div",
          `${s.counters[key + "_error"] ?? 0} ошибок / неизвестных`,
          "meta bad",
        ),
      );
      $("#stats").append(box);
    });
    s.events.forEach((e) => {
      if (e.user) option($("#user-filter"), e.user);
    });
    renderEvents();
  } catch (error) {
    $("#connection").textContent = "Нет связи с интерфейсом";
    $("#connection").className = "badge bad";
  } finally {
    setTimeout(poll, 1000);
  }
}
document
  .querySelectorAll(".filters select")
  .forEach((s) => s.addEventListener("change", renderEvents));
document.querySelectorAll("[data-action]").forEach((button) =>
  button.addEventListener("click", async () => {
    button.disabled = true;
    $("#notice").textContent = "";
    try {
      const response = await fetch("/api/action/" + button.dataset.action, {
        method: "POST",
      });
      if (!response.ok) {
        const body = await response.json();
        throw Error(body.detail ?? response.status);
      }
    } catch (error) {
      $("#notice").textContent = error.message;
    } finally {
      button.disabled = false;
    }
  }),
);
document.querySelectorAll("[data-method]").forEach((button) =>
  button.addEventListener("click", async () => {
    button.disabled = true;
    try {
      const result = await apiPost("/api/kv", {
        node: $("#manual-node").value,
        method: button.dataset.method,
        key: $("#manual-key").value,
        value: $("#manual-value").value,
      });
      $("#manual-result").textContent =
        `${result.method} ${result.key} · ${result.node} · ${result.status ?? "NETWORK"} · ${eventDetails(result)}`;
      $("#manual-result").className = "meta " + (result.ok ? "ok" : "bad");
    } catch (error) {
      $("#manual-result").textContent = error.message;
    } finally {
      button.disabled = false;
    }
  }),
);
poll();
