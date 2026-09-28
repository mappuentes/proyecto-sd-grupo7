// Run with: node Node-Red/test-globe.cjs (no extra dependencies).
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const flows = JSON.parse(fs.readFileSync(`${__dirname}/flows.json`));
const normalize = new Function('msg', 'node', 'Buffer', flows.find(n => n.id === 'globe-normalize').func);
const sample = { norad_id: '25544', name: 'ISS', lat: 40, lon: -3, alt_km: 420, velocity_kms: 7.65, ts: new Date().toISOString() };
const run = payload => normalize({ payload }, { warn() {} }, Buffer);
for (const payload of [sample, JSON.stringify(sample), Buffer.from(JSON.stringify(sample)), { value: JSON.stringify(sample) }]) {
  assert.deepEqual(JSON.parse(run(payload).payload), sample);
}
for (const payload of [null, 'bad JSON', {}, { ...sample, alt_km: -1 }, { ...sample, lat: 91 },
  { ...sample, lon: 181 }, { ...sample, ts: 'invalid' }, { ...sample, norad_id: undefined }, { ...sample, lat: '40' }]) {
  assert.equal(run(payload), null);
}

// Exercise the actual page script: rendering scale, stream updates and stale data.
const html = fs.readFileSync(`${__dirname}/public/globe.html`, 'utf8');
function element() {
  return {
    clientWidth: 800, clientHeight: 600, value: '', children: [], attributes: {}, events: {},
    addEventListener(type, callback) { this.events[type] = callback; },
    setAttribute(key, value) { this.attributes[key] = value; },
    append(...children) { for (const child of children) { child.parent = this; this.children.push(child); } },
    remove() { this.parent.children = this.parent.children.filter(child => child !== this); }
  };
}
const elements = Object.fromEntries([...html.matchAll(/id="([^"]+)"/g)].map(match => [match[1], element()]));
const calls = {};
const fakeGlobe = new Proxy({}, { get: (_, method) => value => { calls[method] = value; return fakeGlobe; } });
let ws, timer;
const context = {
  document: { getElementById: id => elements[id], createElement: element },
  window: { addEventListener() {} }, location: { protocol: 'http:', host: 'localhost:1880' },
  Globe: function () { return fakeGlobe; },
  WebSocket: class { static OPEN = 1; constructor(url) { assert.equal(url, 'ws://localhost:1880/ws/globe'); this.readyState = 1; ws = this; } },
  setInterval: callback => { timer = callback; }, setTimeout() {}, console
};
vm.runInNewContext(html.match(/<script>([\s\S]*?)<\/script>/)[1], context);
assert.equal(calls.labelAltitude(sample), 420 / 6371);
assert.equal(calls.labelText, 'name');
assert.equal(calls.labelsTransitionDuration, 0);
ws.onopen();
const send = value => ws.onmessage({ data: JSON.stringify(value) });
send(sample);
assert.equal(calls.labelsData.length, 1);
assert.equal(calls.labelsData[0].norad_id, '25544');
send({ ...sample, lat: 20, ts: new Date(Date.now() - 5000).toISOString() });
assert.equal(calls.labelsData[0].lat, 40, 'older events must not replace newer positions');
send({ ...sample, norad_id: '99999', ts: new Date(Date.now() - 60000).toISOString() });
send(null);
ws.onmessage({ data: 'invalid' });
assert.equal(calls.labelsData.length, 1);
const tooltip = calls.labelLabel({ ...sample, name: '<img src=x onerror=alert(1)>' });
assert.ok(tooltip.textContent.includes('<img'), 'names must be rendered as text');
assert.equal(tooltip.innerHTML, undefined);
// Selection, visibility and search exercise the same handlers used by the browser.
calls.onLabelClick(sample);
assert.equal(elements['sat-name'].textContent, 'ISS');
assert.equal(elements['sat-speed'].textContent, '7.65 km/s');
assert.equal(calls.pointOfView.lat, sample.lat);
assert.equal(calls.labelColor(sample), '#ffc66d');
const row = elements['satellite-list'].children[0];
const checkbox = row.children[0];
checkbox.checked = false; checkbox.events.change();
assert.equal(calls.labelsData.length, 0);
assert.equal(elements.details.hidden, true);
send({ ...sample, ts: new Date().toISOString() });
assert.equal(calls.labelsData.length, 0, 'incoming positions must preserve visibility');
row.children[1].events.click();
assert.equal(calls.labelsData.length, 1, 'selecting an entry reveals it');
elements.search.value = 'no-such-satellite'; elements.search.events.input();
assert.equal(row.hidden, true);
assert.equal(calls.labelsData.length, 1, 'search only filters the list');
elements.search.value = '25544'; elements.search.events.input();
assert.equal(row.hidden, false);
elements['hide-all'].events.click();
send({ ...sample, norad_id: '48274', name: 'TIANHE' });
assert.equal(calls.labelsData.length, 0, 'none must also hide new arrivals');
elements['show-all'].events.click();
assert.equal(calls.labelsData.length, 2, 'all must include satellites outside the search');
assert.equal(elements.total.textContent, 2);
assert.equal(JSON.parse(run({ ...sample, velocity_kms: 'bad' }).payload).velocity_kms, null);
vm.runInNewContext('for (const sat of satellites.values()) sat.ts = new Date(Date.now() - 60000).toISOString()', context);
timer();
assert.equal(calls.labelsData.length, 0, 'stale markers must disappear');
assert.equal(elements['satellite-list'].children.length, 0);
console.log('Globe checks passed: selection, filtering, visibility, Kafka payloads, validation, altitude scale, ordering and stale markers.');
