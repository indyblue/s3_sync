const s3 = require('./s3-sync')
  , web = require('template.das')
  , flag = x => ~process.argv.indexOf('-' + x)
  , filters = [/\bnode_modules\b/, /^data$/, /~$/, /\.swp$/]
  , sync = (s, d) => s3.sync(s, d, filters)
  , dirs = [
    {
      c: 't1', name: 'temp1: Pious Reflections',
      args: ['/home/user/0das/1/Alphonsianum/Pious Reflections/', 'das-junk']
    }, {
      c: 't2', name: 'temp2: Preparation for Death',
      args: ['/home/user/0das/1/Alphonsianum/Preparation for Death/', 'das-junk']
    }, {
      c: 't3', name: 'temp3: Gildersleeve',
      args: ['/home/user/0das/pdf/Latin/Gildersleeve/', 'das-junk']
    }, {
      c: 't4', name: 'temp4: dictionaries',
      args: ['/home/user/0das/pdf/Latin/dictionaries/', 'das-junk']
    }, {
      c: '1', name: 'archive: ~/0das/1/',
      args: ['/home/user/0das/1/', 'das-1-docs']
    }, {
      c: 'p', name: 'archive: ~/0das/pdf/',
      args: ['/home/user/0das/pdf/', 'das-pdf']
    }, {
      c: 'w', name: 'archive: ~/www/',
      args: ['/home/user/www/', 'das-www']
    }
  ];


async function main() {
  let fn = () => console.log('choose action: u/d/s');
  if (flag('u')) {
    s3.consoleEmitters();
    fn = sync;
    s3.simpleS3Queue();
  }
  else if (flag('d')) fn = s3.download;
  else if (flag('s')) fn = s3.status;
  else {
    const srv = web.new();
    srv
      .addHandler(true, web.files)
      .addHandler(true, web.modFiles)
      .start();

    s3Socket(srv);
  }

  for (let d of dirs) {
    if (flag(d.c)) {
      console.log('starting ', d.name);
      await fn.apply(null, d.args)
    }
  }
  s3.printQueue();
  return true;
}
main();

function s3Socket(srv) {
  /************************************************************************/
  // websocket event handlers:
  srv.jh.cbConn = ws => srv.jh.sendJson('init',
    { events: s3.ev, dirs }, ws);
  srv.jh
    .addCbJson(/.*/i, function (p, e) {
      console.log('json msg', e, p);
      srv.jh.sendJson('ack', { txt: 'message received', obj: p }, this);
    });
  let inProgress = false;
  for (var d of dirs) srv.jh.addCbJson(d.name, async o => {
    if (inProgress) console.log('*** reject, another sync is in progress', d.name);
    inProgress = true;
    await sync.apply(null, o.args);
    inProgress = false;
  });

  /************************************************************************/
  // websocket s3 sends:
  s3.on(s3.ev.file_q, f =>
    srv.jh.sendJson(s3.ev.file_q, f));
  s3.on(s3.ev.stats, f =>
    srv.jh.sendJson(s3.ev.stats, f));
  s3.on(s3.ev.get_remote, f =>
    srv.jh.sendJson(s3.ev.get_remote, f));
  s3.on(s3.ev.file_unused, f =>
    srv.jh.sendJson(s3.ev.file_unused, f));
  s3.on(s3.ev.end, f =>
    srv.jh.sendJson(s3.ev.end, f));

  /************************************************************************/

}