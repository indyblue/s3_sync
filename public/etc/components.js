/* global h, useState, preact */
/* eslint-disable no-unused-vars */
function dirs({ dirs = [], onClick }) {
  return h('div', null,
    dirs.map(d => h('button', {
      onClick: onClick.bind(null, d),
    }, d.name)),
  );
}

function cFiles({ files }) {
  const [state, setState] = useState(1);
  if (state <= 0) setState(1);
  const rx = new RegExp('^((?:[^/]*/){' + state + '}).*');
  const ffiles = files.sort((a, b) => a.key < b.key ? -1 : 1)
    .reduce((a, f) => {
      const pref = f.key.replace(rx, '$1');
      let g = a.find(x => x.key === pref);
      if (!g) {
        if (pref === f.key) g = f;
        else g = { key: pref, files: [] };
        a.push(g);
      }
      if (g.files) g.files.push(f);
      return a;
    }, []);
  return h('div', null,
    h('button', { onClick: e => setState(x => x + 1) }, '+'),
    h('span', null, state),
    h('button', { onClick: e => setState(x => x - 1) }, '-'),
    h('table', null,
      h('tr', null,
        'name,location,size,count'.split(',').map(x => h('th', null, x)),
      ),
      ffiles.map(file => h(cFile, { file })),
    ),
  );
}

function cFile({ file, p }) {
  const [exp, setExp] = useState(false);
  if (!file.eq) file.eq = file.files.reduce((a, x) => Math.max(a, x.eq), 0);
  if (!file.size) file.size = file.files.reduce((a, f) => a + parseInt(f.size), 0);
  return h(preact.Fragment, null,
    h('tr', null,
      h('td', null,
        file.files ? h('button', { onClick: e => setExp(x => !x) }, exp ? '-' : '+') : null,
        p,
        file.key,
      ),
      h('td', null, file.eq === 1 ? 'local' : file.eq > 1 ? 'both' : 's3'),
      h('td', null, csn(file.size)),
      h('td', null, file.files ? file.files.length : 0),
    ),
    exp ? file.files.map(x => h(cFile, { file: x, p: ' -- ' })) : null,
  );
}
function csn(n) { return n.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ','); }

function stats({ stats, fetch: { length, done } }) {
  return h('table', null,
    length && h('tr', null, ['fetch', csn(length), done && 'done'].map(x => h('td', null, x))),
    Object.keys(stats).map(x => h(statrow, { name: x, val: stats[x] })),
  );
}

function statrow({ name, val: { len, size } }) {
  if (len > 0) {
    return h('tr', null,
      h('td', null, name),
      h('td', null, csn(len)),
      h('td', null, csn(size)),
    );
  }
}
