/* global WebSocket, preact, dirs, stats, cFiles */
/* eslint-disable no-unused-vars */
const { h, render, useState, useReducer, useEffect, update } = preact;
function ws(jh) {
  console.log('ws://' + window.location.host + '/');
  const ws = new WebSocket('ws://' + window.location.host + '/');
  ws.onopen = function() {
    jh.addConn(ws);
    ws.onmessage = e => jh.ondata(e);
  };
  ws.onclose = function() { jh.remConn(ws); };
  return jh;
}

function handler(state, { a, o } = {}) {
  if (!state) {
    return {
      files: [],
      dirs: [],
      stats: {},
      fetch: {},
    };
  }
  const newState = update(state, a, o);
  // console.log(newState);
  return newState;
}
render(h(p => {
  const jh = window.jsonWsHandler;
  const [state, _reduce] = useReducer(handler, handler());
  const reduce = (a, o) => _reduce({ a, o: o || {} });
  useEffect(() => {
    ws(jh).addCbJson('init', x => reduce('$merge', x))
      .addCbJson(/.*/i, function(p, e) {
        if ('file_q'.split('.').indexOf(e) >= 0) return;
        console.log(e, p);
      })
      .addCbJson('file_q', f => reduce('files.$push', [f]))
      .addCbJson('file_unused', f => reduce('files.$push', f.files))
    // .addCbJson('file_end', f => reduce('remove', f))
      .addCbJson('stats', x => reduce('stats.$set', x))
      .addCbJson('fetch_remote', x => reduce('fetch.$set', x))
      .addCbJson('end', x => console.log('end', x));
  }, []);

  return h('div', null,
    h(dirs, {
      ...state,
      onClick: (d, e) => {
        reduce('files.$set', []);
        reduce('stats.$set', {});
        reduce('fetch.$set', {});
        jh.sendJson(d.name, d);
      },
    }),
    h(stats, { ...state }),
    h(cFiles, { ...state }),
  );
}), document.body);
