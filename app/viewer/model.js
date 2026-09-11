"use strict";

function aggregateViewer(payload, semesters, chapters, years, breakdown) {
  const selectedSemesters = new Set(semesters);
  const selectedChapters = new Set(chapters);
  const offsets = breakdown === "Overall" ? years : years.slice(0, 1);
  const groups = new Map();
  let students = 0;
  for (const [semester, chapter, year, outcome, count] of payload.units) {
    if (!selectedSemesters.has(semester) || !selectedChapters.has(chapter)) continue;
    if (year === 1) students += count;
    if (!offsets.includes(year)) continue;
    const label = breakdown === "Semester joined" ? semester : breakdown === "Chapter joined" ? chapter : `${year} Year`;
    if (!groups.has(label)) groups.set(label, {label, year, eligible: 0, future: 0, counts: {}});
    const group = groups.get(label);
    if (outcome === "Future") group.future += count;
    else {
      group.eligible += count;
      group.counts[outcome] = (group.counts[outcome] || 0) + count;
    }
  }
  const order = breakdown === "Semester joined" ? payload.semesters : breakdown === "Chapter joined" ? payload.chapters : offsets.map(year => `${year} Year`);
  const ordered = [...groups.values()].sort((a, b) => order.indexOf(a.label) - order.indexOf(b.label));
  const rows = [];
  for (const group of ordered) {
    group.total = group.eligible + group.future;
    group.status = group.eligible ? (group.future ? "Partially Future" : "Measured") : "Future";
    for (const outcome of payload.outcomes) {
      const count = outcome === "Future" ? (group.eligible ? 0 : group.future) : (group.counts[outcome] || 0);
      if (!count) continue;
      rows.push({group: group.label, year: group.year, outcome, count,
        share: count / (outcome === "Future" ? group.total : group.eligible),
        eligible: group.eligible, future: group.future, total: group.total, status: group.status});
    }
  }
  return {students, groups: ordered, rows};
}

if (typeof module !== "undefined") module.exports = {aggregateViewer};
