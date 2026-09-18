"use strict";

(() => {
  const payload = JSON.parse(document.getElementById("viewer-data").textContent);
  const byId = id => document.getElementById(id);
  const format = number => number.toLocaleString("en-US");
  const escape = value => String(value).replace(/[&<>"']/g, char => ({"&":"&amp;", "<":"&lt;", ">":"&gt;", '"':"&quot;", "'":"&#39;"}[char]));
  const pickers = {};
  const selected = id => pickers[id].filter(input => input.checked).map(input => input.value);
  let result;
  let renderSequence = 0;

  function addPicker(id, options) {
    pickers[id] = options.map(value => {
      const label = document.createElement("label");
      const input = document.createElement("input");
      input.type = "checkbox";
      input.value = String(value);
      input.checked = true;
      label.append(input, document.createTextNode(id === "years" ? `${value} Year` : value));
      byId(id).append(label);
      input.addEventListener("change", id === "councils" ? councilChanged : update);
      return input;
    });
  }
  addPicker("semesters", payload.semesters);
  addPicker("chapters", payload.chapters);
  addPicker("councils", payload.councils);
  for (const council of [...payload.councils, "Council group"]) byId("council-mode").add(new Option(council, council));
  addPicker("years", [1, 2, 3, 4, 5, 6]);
  for (let year = 1; year <= 6; year++) byId("single-year").add(new Option(`${year} Year`, String(year)));
  byId("single-year").value = "6";
  byId("published").textContent = `Published ${new Date(payload.published).toLocaleString()}`;
  byId("data-through").textContent = `Data complete through ${payload.dataThrough}`;

  for (const id of ["semesters", "chapters"]) {
    byId(`all-${id}`).addEventListener("change", event => {
      for (const input of pickers[id]) if (!input.disabled) input.checked = event.target.checked;
      update();
    });
  }
  byId("chapter-search").addEventListener("input", applyChapterVisibility);
  byId("council-mode").addEventListener("change", councilChanged);
  byId("breakdown").addEventListener("change", update);
  byId("single-year").addEventListener("change", update);
  byId("reset").addEventListener("click", () => {
    for (const inputs of Object.values(pickers)) for (const input of inputs) input.checked = true;
    for (const input of pickers.chapters) input.parentElement.hidden = false;
    byId("chapter-search").value = "";
    byId("council-mode").value = "All councils";
    byId("breakdown").value = "Overall";
    byId("single-year").value = "6";
    update();
  });

  function selectedCouncils() {
    const mode = byId("council-mode").value;
    return mode === "All councils" ? null : mode === "Council group" ? selected("councils") : [mode];
  }

  function applyChapterVisibility() {
    const allowed = new Set(chaptersForCouncils(payload, payload.chapters, selectedCouncils()));
    const query = byId("chapter-search").value.trim().toLowerCase();
    for (const input of pickers.chapters) {
      input.disabled = !allowed.has(input.value);
      input.parentElement.hidden = input.disabled || !input.value.toLowerCase().includes(query);
    }
  }

  function councilChanged() {
    for (const input of pickers.chapters) input.checked = true;
    byId("chapter-search").value = "";
    update();
  }

  function renderTable() {
    byId("chart-rows").replaceChildren();
    if (!byId("chart-data").open) return;
    for (const row of result.rows) {
      const tr = document.createElement("tr");
      for (const value of [row.group, row.year, row.outcome, `${(row.share * 100).toFixed(1)}%`, format(row.count), format(row.eligible), format(row.future)]) {
        const td = document.createElement("td");
        td.textContent = value;
        tr.append(td);
      }
      byId("chart-rows").append(tr);
    }
  }
  byId("chart-data").addEventListener("toggle", renderTable);

  function wrap(value) {
    const lines = [];
    for (const word of String(value).split(/\s+/)) {
      if (lines.length && lines[lines.length - 1].length + word.length < 22) lines[lines.length - 1] += ` ${word}`;
      else lines.push(word);
    }
    return lines.map(escape).join("<br>");
  }

  async function update() {
    const sequence = ++renderSequence;
    const councils = selectedCouncils();
    const availableChapters = chaptersForCouncils(payload, payload.chapters, councils);
    const semesters = selected("semesters"), chapters = chaptersForCouncils(payload, selected("chapters"), councils);
    applyChapterVisibility();
    byId("councils-field").hidden = byId("council-mode").value !== "Council group";
    const breakdown = byId("breakdown").value;
    const years = breakdown === "Overall" ? selected("years").map(Number) : [Number(byId("single-year").value)];
    for (const id of ["semesters", "chapters"]) {
      const count = id === "chapters" ? chapters.length : semesters.length;
      const total = id === "chapters" ? availableChapters.length : payload.semesters.length;
      byId(`all-${id}`).checked = total > 0 && count === total;
      byId(`all-${id}`).indeterminate = count > 0 && count < total;
      byId(`all-${id}`).disabled = !total;
    }
    byId("years-field").hidden = breakdown !== "Overall";
    byId("single-year-field").hidden = breakdown === "Overall";
    result = aggregateViewer(payload, semesters, chapters, years, breakdown, councils);
    byId("cohort-size").textContent = format(result.students);
    const cohortLabel = semesters.length === payload.semesters.length ? "All Time" : semesters.length === 1 ? semesters[0] : `${semesters.length} semesters`;
    const chapterLabel = !chapters.length ? "None" : chapters.length === availableChapters.length ? "ALL" : chapters.length === 1 ? chapters[0] : `${chapters.length} chapters`;
    const councilLabel = councils === null ? "ALL" : councils.join(" + ") || "None";
    byId("cohort-label").textContent = cohortLabel;
    byId("chapter-label").textContent = chapterLabel;
    byId("council-label").textContent = councilLabel;
    byId("chart-title").textContent = breakdown === "Overall" ? "Outcome Rates: Years Since Joining FSL" : `Outcome Rates by ${breakdown}`;
    byId("selection-label").textContent = `${cohortLabel} | ${councilLabel} councils | ${chapterLabel} chapters | ${years.map(year => `${year} Year`).join(", ")}`;
    byId("empty").hidden = result.groups.length > 0;
    byId("empty").textContent = years.length ? "No students match the selected semesters, councils, and chapters." : "No milestones selected.";
    byId("chart").hidden = !result.groups.length;
    byId("chart-error").hidden = true;
    renderTable();
    if (!result.groups.length) return;
    byId("chart").style.minWidth = `${Math.max(540, result.groups.length * 96)}px`;
    const groupIds = new Map(result.groups.map((group, index) => [group.label, String(index)]));
    const labels = result.groups.map(group => `${wrap(group.label)}<br>${format(group.eligible)}<br>eligible students${group.future ? `<br>${format(group.future)} future` : ""}`);
    const traces = payload.outcomes.map(outcome => {
      const rows = result.rows.filter(row => row.outcome === outcome);
      if (!rows.length) return null;
      return {
        type: "bar", name: outcome, marker: {color: payload.colors[outcome]},
        x: rows.map(row => groupIds.get(row.group)), y: rows.map(row => row.share),
        text: rows.map(row => outcome === "Future" ? "Future" : row.share >= 0.085 ? `${escape(outcome)}<br>${(row.share * 100).toFixed(1)}%<br>(n=${format(row.count)})` : ""),
        textposition: "inside", textfont: {size: 11, color: ["Early Alumni", "Unknown", "Future"].includes(outcome) ? "#2f2e2a" : "white"},
        customdata: rows.map(row => [row.count, row.eligible, row.future, row.total, escape(row.group)]),
        hovertemplate: `${escape(outcome)}<br>%{customdata[4]}<br>%{y:.1%}<br>Students: %{customdata[0]:,}<br>Eligible: %{customdata[1]:,}<br>Future: %{customdata[2]:,}<br>Cohort: %{customdata[3]:,}<extra></extra>`
      };
    }).filter(Boolean);
    try {
      await Plotly.react(byId("chart"), traces, {
        barmode: "stack", bargap: 0.18, height: 660,
        font: {family: "Segoe UI, Arial, sans-serif", color: "#17213a"},
        paper_bgcolor: "white", plot_bgcolor: "white",
        margin: {l: 60, r: 18, t: 25, b: 205},
        xaxis: {type: "category", categoryorder: "array", categoryarray: [...groupIds.values()], tickmode: "array", tickvals: [...groupIds.values()], ticktext: labels, tickfont: {size: 11}, tickangle: 0, fixedrange: true},
        yaxis: {range: [0, 1], tickformat: ".0%", title: {text: "Share of eligible students", font: {size: 12}}, gridcolor: "#e3e8f0", fixedrange: true},
        legend: {orientation: "h", x: 0, y: -0.3, yanchor: "top", font: {size: 11}, itemclick: false, itemdoubleclick: false},
        uniformtext: {minsize: 8, mode: "hide"}
      }, {responsive: true, displaylogo: false, displayModeBar: false});
    } catch (error) {
      if (sequence === renderSequence) byId("chart-error").hidden = false;
    }
  }
  update();
})();
