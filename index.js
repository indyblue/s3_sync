const s3 = require('./s3-sync')
  , { exec } = require('child_process')
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
      c: 't5', name: 'super temp',
      args: ['/home/user/0das/temp/das-junk/', 'das-junk']
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
let srv, gchrome;

async function main() {
  let fn = () => console.log('choose action: u/d/s');
  let fnDone = () => { };
  if (flag('u')) {
    s3.consoleEmitters();
    fn = sync;
    s3.simpleS3Queue();
    fnDone = () => { process.exit(); };
  }
  else if (flag('d')) fn = s3.download;
  else if (flag('s')) fn = s3.status;
  else {
    srv = web.new();
    await srv
      .addHandler(/tmp\/(.*)/, web.modFiles(1))
      .addHandler(true, web.files)
      .start();
    //or xdg-open?
    if (flag('g')) {
      gchrome = exec(`google-chrome --app=http://${srv.host}:${srv.port}/ux.html`);
      gchrome.stdout.pipe(process.stdout);
      gchrome.stderr.pipe(process.stderr);
    }
    s3Socket(srv);
  }

  for (let d of dirs) {
    if (flag(d.c)) {
      console.log('starting ', d.name);
      await fn.apply(null, d.args)
    }
  }
  s3.printQueue();
  fnDone();
  return true;
}
main()
  .catch(e => console.log(e));

function s3Socket(srv) {
  /************************************************************************/
  // websocket event handlers:
  srv.jh.cbConn = ws => srv.jh.sendJson('init',
    { events: s3.ev, dirs }, ws);
  srv.jh
    // .addCbJson(/.*/i, function (p, e) {
    //   console.log('json msg', e, p);
    //   srv.jh.sendJson('ack', { txt: 'message received', obj: p }, this);
    // })
    .addCbJson('actionQueue', (p, e) =>
      s3.actionQueue(p, true));

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
  s3.on(s3.ev.file_start, f =>
    f.size > 1 << 20 ? srv.jh.sendJson(s3.ev.file_start, f) : null);
  s3.on(s3.ev.file_end, f =>
    srv.jh.sendJson(s3.ev.file_end, f));

  /************************************************************************/

}

process.stdin.setEncoding('utf8');
process.stdin.resume();
process.stdin.on('data', d => {
  d = d.trim();
  console.log(d, d === 'q');
  if (d === 'q') process.exit();
});

const onExit = () => {
  if (srv) {
    console.log('stopping server', srv.port);
    srv.server.close();
  }
};
process.on('exit', onExit);
process.on('SIGTERM', () => process.exit(1));
process.on('SIGINT', () => process.exit(1));

