const summaryEl = document.getElementById("summary");
const tableEl = document.getElementById("gpu-table");
const updatedEl = document.getElementById("updated");
const refreshBtn = document.getElementById("refresh");
const refreshSeconds = Number(document.body.dataset.refreshSeconds || "15");

async function fetchStatus() {
  const res = await fetch("/api/status", { cache: "no-store" });
  if (!res.ok) {
    throw new Error(`API error: ${res.status}`);
  }
  return res.json();
}

function renderSummary(data) {
  const totals = {
    devices: data.devices.length,
    gpus: 0,
    busy: 0,
    free: 0,
    error: 0,
  };

  data.devices.forEach((d) => {
    if (d.status !== "ok") {
      totals.error += 1;
      return;
    }
    d.gpus.forEach((g) => {
      totals.gpus += 1;
      if (g.busy) totals.busy += 1;
      else totals.free += 1;
    });
  });

  summaryEl.innerHTML = `
    <div class="card">
      <div class="card-title">Devices</div>
      <div class="card-value">${totals.devices}</div>
    </div>
    <div class="card">
      <div class="card-title">GPUs</div>
      <div class="card-value">${totals.gpus}</div>
    </div>
    <div class="card">
      <div class="card-title">Busy</div>
      <div class="card-value busy">${totals.busy}</div>
    </div>
    <div class="card">
      <div class="card-title">Free</div>
      <div class="card-value ok">${totals.free}</div>
    </div>
    <div class="card">
      <div class="card-title">Errors</div>
      <div class="card-value error">${totals.error}</div>
    </div>
  `;
}

function formatMem(used, total, pct) {
  return `${used} / ${total} MiB (${pct}%)`;
}

function renderTable(data) {
  tableEl.innerHTML = "";

  data.devices.forEach((d) => {
    if (d.status !== "ok") {
      const row = document.createElement("tr");
      row.className = "row-error";
      row.innerHTML = `
        <td>${d.name}<div class="muted">${d.host}</div></td>
        <td colspan="6">${d.error || "Unknown error"}</td>
      `;
      tableEl.appendChild(row);
      return;
    }

    d.gpus.forEach((g) => {
      const row = document.createElement("tr");
      row.className = g.busy ? "row-busy" : "row-ok";
      row.innerHTML = `
        <td>${d.name}<div class="muted">${d.host}</div></td>
        <td>#${g.index} ${g.name}</td>
        <td><span class="pill ${g.busy ? "busy" : "ok"}">${
        g.busy ? "Busy" : "Available"
      }</span></td>
        <td>${g.utilization_gpu}%</td>
        <td>${formatMem(g.memory_used_mb, g.memory_total_mb, g.memory_used_pct)}</td>
        <td>${g.temperature_c}°C</td>
        <td class="mono">${g.uuid}</td>
      `;
      tableEl.appendChild(row);
    });
  });
}

function renderUpdated(ts) {
  const date = new Date(ts * 1000);
  updatedEl.textContent = `Last updated: ${date.toLocaleString()}`;
}

async function refresh() {
  try {
    const data = await fetchStatus();
    renderSummary(data);
    renderTable(data);
    renderUpdated(data.updated_at);
  } catch (err) {
    updatedEl.textContent = `Error: ${err.message}`;
  }
}

refreshBtn.addEventListener("click", refresh);

setInterval(refresh, 1000 * refreshSeconds);

refresh();
