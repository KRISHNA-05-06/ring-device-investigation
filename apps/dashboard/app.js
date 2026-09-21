'use strict';
const $ = id => document.getElementById(id);
const token = document.querySelector('meta[name="dashboard-token"]').content;
let busy = false, timer, hasSnapshot = false;
const text = (id, value) => { $(id).textContent = value; };
const node = (tag, value, cls) => { const n = document.createElement(tag); if (value !== undefined) n.textContent = value; if (cls) n.className = cls; return n; };
const display = value => value === null || value === undefined ? '—' : String(value);
const pct = value => value === null || value === undefined ? '—' : `${value}%`;
const utc = value => { if (!value) return '—'; const d = new Date(value); return Number.isNaN(d.valueOf()) ? 'Unavailable' : d.toISOString().replace('T', ' ').slice(0,19) + ' UTC'; };
const pill = value => node('span', display(value).replaceAll('_',' '), `pill ${['reporting','suspected_offline','unknown','completed','failed','running'].includes(value) ? value : ''}`);
function empty(id, message) { $(id).replaceChildren(node('p', message, 'empty')); }
function panel(data, name) { const item = data.panels[name]; const err = $(`${name}-error`); err.hidden = !!item?.ok; err.textContent = item?.ok ? '' : (item?.error || 'Query unavailable'); return item?.ok ? item.result : null; }
function addRow(target, cells) { const row = node('tr'); cells.forEach(value => { const cell = node('td'); if (value instanceof Node) cell.append(value); else cell.textContent = display(value); row.append(cell); }); $(target).append(row); }
function detail(card, label, value) { const line = node('div', undefined, 'detail'); line.append(node('span',label),node('span',value)); card.append(line); }

function render(data) {
  $('banner').hidden = data.mode !== 'demo';
  text('banner', 'OFFLINE SAMPLE DATA — layout preview only. These numbers are fictional and no AWS requests were made.');
  text('mode', data.mode === 'demo' ? 'Offline sample data' : 'AWS / read only');
  const h = panel(data, 'health');
  $('devices').replaceChildren();
  if (h) {
    const devices = h.devices || [];
    for (const [id,state] of [['reporting','reporting'],['offline','suspected_offline'],['unknown','unknown']]) text(id, devices.filter(d => d.health_status === state).length);
    text('health-scope', `${utc(h.as_of)} · ${data.options.silence_minutes}-minute threshold · ${h.truncated ? 'PARTIAL DEVICE LIST' : 'Returned device list complete'} · queried ${utc(data.panels.health.generated_at)}`);
    if (!devices.length) empty('devices','No registered devices returned at this cutoff.');
    devices.forEach(d => {
      const card = node('article',undefined,'device'); const top = node('div',undefined,'device-top');
      top.append(node('h3',d.device_id),pill(d.health_status)); card.append(top,node('p',d.device_type,'device-type'));
      const battery = node('div',pct(d.battery_percent),'battery'); battery.append(node('small','last reported battery')); card.append(battery);
      detail(card,'Heartbeat age', d.minutes_since_heartbeat == null ? 'No accepted heartbeat' : `${d.minutes_since_heartbeat} min`);
      detail(card,'Event time',utc(d.last_heartbeat));
      const evidence = node('details'); evidence.append(node('summary','View event evidence'),node('p',`Event: ${display(d.event_id)}`),node('p',`Run: ${display(d.run_id)}`),node('p',`Received: ${utc(d.received_at)}`)); card.append(evidence); $('devices').append(card);
    });
  } else { for (const id of ['reporting','offline','unknown']) text(id,'—'); text('health-scope','Health query unavailable'); }
  const p = panel(data,'pipeline'); $('outcomes').replaceChildren(); $('run-rows').replaceChildren();
  if (p) {
    const s = p.summary;
    for (const [field,label] of [['accepted_records','Accepted'],['duplicate_records','Duplicates'],['rejected_records','Rejected']]) {
      const card = node('div',undefined,'outcome'); card.append(node('strong',display(s[field])),node('span',label)); $('outcomes').append(card);
    }
    text('freshness',utc(s.latest_success_at));
    text('run-summary',`${s.total_runs} recorded runs · ${s.completed_runs} completed · ${s.running_runs} running · ${s.failed_runs} failed. ${s.input_records} recorded inputs. Queried ${utc(data.panels.pipeline.generated_at)}. Failed counts include intentional failure tests.`);
    for (const r of p.recent_runs || []) addRow('run-rows',[utc(r.started_at),pill(r.status),`${r.accepted_count} / ${r.duplicate_count} / ${r.rejected_count}`,node('span',r.source_key,'source')]);
    if (!p.recent_runs?.length) addRow('run-rows',['No recorded runs','—','—','—']);
  } else { text('freshness','Unavailable'); text('run-summary','No current pipeline result. Previous values have been cleared.'); addRow('run-rows',['Query unavailable','—','—','—']); }
  const r = panel(data,'rejections'); $('rejections').replaceChildren();
  if (r) {
    if (!r.errors?.length) empty('rejections','No rejection occurrences returned.');
    for (const e of r.errors || []) { const line = node('div',undefined,'rejection'); line.append(node('span',e.error_code),node('strong',display(e.rejection_count))); $('rejections').append(line); }
    $('rejections').append(node('p',`${r.truncated ? 'PARTIAL: additional error categories omitted. ' : ''}Queried ${utc(data.panels.rejections.generated_at)}.`, 'note'));
  }
  const daily = panel(data,'daily'); $('daily-rows').replaceChildren();
  if (daily) {
    text('daily-scope',`${daily.report_day} UTC · accepted event time · start inclusive, end exclusive${daily.truncated ? ' · PARTIAL DEVICE LIST' : ''} · queried ${utc(data.panels.daily.generated_at)}`);
    for (const d of daily.devices || []) addRow('daily-rows',[d.device_id,d.accepted_heartbeat_count,pct(d.min_battery_percent),pct(d.average_battery_percent),pct(d.max_battery_percent)]);
    if (!daily.devices?.length) addRow('daily-rows',['No registered devices returned','—','—','—','—']);
  } else text('daily-scope','Daily metrics unavailable');
}
function schedule() { clearTimeout(timer); if ($('auto').checked && !document.hidden && $('time-mode').value === 'live') timer = setTimeout(refresh,15000); }
async function refresh() {
  if (busy) return;
  if (!$('day').value || ($('time-mode').value === 'fixed' && !$('cutoff').value)) { text('refresh-status','Choose a valid date and historical cutoff.'); return; }
  busy = true; clearTimeout(timer); $('refresh').disabled = true;
  text('refresh-status',hasSnapshot ? 'Refreshing data… Displayed values belong to the previous refresh.' : 'Loading device and pipeline data from AWS…');
  if (!hasSnapshot) empty('devices','Loading registered devices…');
  const q = new URLSearchParams({report_day:$('day').value, silence_minutes:$('threshold').value});
  if ($('time-mode').value === 'fixed') q.set('as_of',$('cutoff').value+'Z');
  const controller = new AbortController(), timeout = setTimeout(()=>controller.abort(),70000);
  try {
    const response = await fetch('/api/snapshot?'+q,{headers:{'X-Dashboard-Token':token},signal:controller.signal,cache:'no-store'});
    const data = await response.json(); if (!response.ok) throw new Error(data.error || 'Refresh failed');
    render(data); hasSnapshot = true; const failures = Object.values(data.panels).filter(p=>!p.ok).length;
    text('refresh-status',`Last refresh ${utc(data.fetched_at)}${failures ? ` · ${failures} unavailable panel(s)` : ' · All four queries returned'}`);
  } catch (error) {
    const detail = hasSnapshot ? 'Previously displayed data is stale.' : 'No data has loaded yet. Keep the dashboard terminal running and check it for errors.';
    text('refresh-status',`Refresh failed. ${detail}`);
    $('banner').hidden=false; text('banner',`${error.name==='AbortError' ? 'Refresh timed out' : error.message}. ${detail}`);
    if (!hasSnapshot) empty('devices','Device data is unavailable. See the message above; this does not mean the devices were removed.');
    $('auto').checked=false;
  } finally { clearTimeout(timeout); busy=false; $('refresh').disabled=false; schedule(); }
}
$('day').value = new Date().toISOString().slice(0,10);
$('refresh').addEventListener('click',refresh);
$('time-mode').addEventListener('change',()=>{ const fixed=$('time-mode').value==='fixed'; $('cutoff-label').hidden=!fixed; if(fixed) $('auto').checked=false; $('auto').disabled=fixed; schedule(); text('refresh-status','Settings changed. Refresh to apply.'); });
for (const id of ['threshold','day','cutoff']) $(id).addEventListener('change',()=>text('refresh-status','Settings changed. Refresh to apply.'));
$('auto').addEventListener('change',schedule);
document.addEventListener('visibilitychange',schedule);
$('silence-example').addEventListener('click',()=>{ $('time-mode').value='fixed'; $('time-mode').dispatchEvent(new Event('change')); $('cutoff').value='2026-09-20T03:29:55'; $('day').value='2026-09-20'; $('threshold').value='1'; refresh(); });

// Sidebar navigation: keep click selection stable through smooth scrolling;
// return to section tracking as soon as the user scrolls or drags manually.
const sectionLinks = [...document.querySelectorAll('aside nav a[href^="#"]')];
const sectionEntries = sectionLinks.map(link => ({link, target: document.getElementById(link.hash.slice(1))})).filter(entry => entry.target);
const reducedMotion = window.matchMedia('(prefers-reduced-motion: reduce)');
let selectedByLink = null, navFrame = 0;

function activateSection(id) {
  for (const {link, target} of sectionEntries) {
    const active = target.id === id;
    link.classList.toggle('active', active);
    if (active) link.setAttribute('aria-current', 'location');
    else link.removeAttribute('aria-current');
  }
}

function trackSection() {
  navFrame = 0;
  if (selectedByLink || !sectionEntries.length) return;
  const top = window.scrollY;
  const maxScroll = Math.max(0, document.documentElement.scrollHeight - window.innerHeight);
  let current = sectionEntries[0];
  for (const entry of sectionEntries) {
    if (entry.target.getBoundingClientRect().top <= 100) current = entry;
  }
  // The final section can be too short to reach the top of the viewport.
  if (maxScroll > 0 && top >= maxScroll - 2) current = sectionEntries[sectionEntries.length - 1];
  activateSection(current.target.id);
}

function queueSectionTracking() {
  if (!navFrame) navFrame = requestAnimationFrame(trackSection);
}

function goToSection(entry, addHistory = false, keyboard = false) {
  selectedByLink = entry.target.id;
  activateSection(selectedByLink);
  if (addHistory && window.location.hash !== entry.link.hash) history.pushState(null, '', entry.link.hash);
  const destination = entry.target.id === 'overview' ? 0 : Math.max(0, window.scrollY + entry.target.getBoundingClientRect().top - 24);
  window.scrollTo({top: destination, behavior: reducedMotion.matches ? 'instant' : 'smooth'});
  if (keyboard) {
    entry.target.setAttribute('tabindex', '-1');
    entry.target.focus({preventScroll: true});
  }
}

for (const entry of sectionEntries) entry.link.addEventListener('click', event => {
  if (event.button !== 0 || event.ctrlKey || event.metaKey || event.shiftKey || event.altKey) return;
  event.preventDefault();
  goToSection(entry, true, event.detail === 0);
});

function resumeSectionTracking() { selectedByLink = null; queueSectionTracking(); }
window.addEventListener('scroll', queueSectionTracking, {passive: true});
window.addEventListener('wheel', resumeSectionTracking, {passive: true});
window.addEventListener('touchstart', resumeSectionTracking, {passive: true});
window.addEventListener('pointerdown', event => { if (!event.target.closest('aside nav')) resumeSectionTracking(); }, {passive: true});
window.addEventListener('keydown', event => {
  if (event.target.closest('input, select, textarea, [contenteditable="true"]')) return;
  if (['ArrowUp','ArrowDown','PageUp','PageDown','Home','End',' '].includes(event.key)) resumeSectionTracking();
});
window.addEventListener('resize', resumeSectionTracking);
window.addEventListener('hashchange', () => {
  const entry = sectionEntries.find(item => item.link.hash === window.location.hash);
  if (entry) goToSection(entry);
  else resumeSectionTracking();
});
new ResizeObserver(queueSectionTracking).observe($('overview'));
const initialSection = sectionEntries.find(entry => entry.link.hash === window.location.hash);
if (initialSection) goToSection(initialSection);
else queueSectionTracking();

// Browser reloads clear the DOM; fetch the stored device list on every page load.
// Continuous polling stays opt-in through the existing checkbox.
refresh();
