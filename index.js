#!/usr/bin/env node
const s3 = require('./s3-sync');
const { exec } = require('child_process');
const web = require('template.das');
const flag = (x, p = '') => {
  if (x instanceof RegExp) return process.argv.find(a => x.test(a));
  return ~process.argv.indexOf('-' + p + x);
};
const filters = [
  /emr_casepacer_deploy.bak.gz/,
  /\bnode_modules\b/,
  /\.yarn\/cache\b/,
  /^data$/, /~$/, /\.swp$/,
  /^node.cp.cpdata$/, /^node.cp.enc/,
  /^node.temp/,
  /^node.doc-offline.out/,
  /^node.doc-offline.queue/,
  /^node.das.brev_etc.divinum-officium/,
  /^node.das.brev_etc.jgabc/,
  /\.next\b/,
  /android.app.build/,
  /^root.br.h/,
  /^root.texdown.node.epub-out/,
  /^(dotnet|node|test)\/.*\/(bin|obj)\//,
  /android\/app\/build/,
  /The Talmud Unmasked english parallel\/p\//,
  /2_casepacer/,
  /caldav.collection-root/,
  /^quickjs.quickjs.lib.*\.a$/,
  /^quickjs.(ssimple|dbg.ssimple.s|mvp)/,
  /^quickjs.quickjs.(qjsc?|libquickjs.a|run-test262$)$/,
  /^quickjs.quickjs.(examples|\.obj|\.git)/,
  /tex-latex\/gtemp\//,
  /Barrister\/logs\//,
  /Barrister.ClientApp.build\//,
  /schedule_calendar.TradCal.caldav./,
  /^node.das.s3_sync.cache/,
];
const sync = (s, d) => s3.sync(s, d, filters);
const dirs = [
  {
    c: 't1',
    name: 'temp1: Pious Reflections',
    args: ['/home/user/0das/1/Alphonsianum/Pious Reflections/', 'das-junk'],
  }, {
    c: 't2',
    name: 'temp2: Preparation for Death',
    args: ['/home/user/0das/1/Alphonsianum/Preparation for Death/', 'das-junk'],
  }, {
    c: 't3',
    name: 'temp3: Gildersleeve',
    args: ['/home/user/0das/pdf/Latin/Gildersleeve/', 'das-junk'],
  }, {
    c: 't4',
    name: 'temp4: dictionaries',
    args: ['/home/user/0das/pdf/Latin/dictionaries/', 'das-junk'],
  }, {
    c: 't5',
    name: 'super temp',
    args: ['/home/user/0das/temp/das-junk/', 'das-junk'],
  }, {
    c: '1',
    name: 'archive: ~/0das/1/',
    args: ['/home/user/0das/1/', 'das-1-docs'],
  }, {
    c: 'p',
    name: 'archive: ~/0das/pdf/',
    args: ['/home/user/0das/pdf/', 'das-pdf'],
  }, {
    c: 'w',
    name: 'archive: ~/www/',
    args: ['/home/user/www/', 'das-www'],
  }, {
    c: 'z',
    name: 'archive: ~/0das/zprogs/',
    args: ['/home/user/0das/zprogs/', 'das-zprogs'],
  }, {
    c: 'f',
    name: 'archive: ~/Videos/fssr/',
    args: ['/home/user/Videos/fssr/', 'das-fssr'],
  },
];
let srv, browser;

async function main() {
  let fn = () => console.log('choose action: u/d/s');
  let fnDone = () => { };
  let limit = 0;
  if (flag('0')) {
    console.log('limited bandwidth mode...');
    filters.push(/^node.cp/);
    limit = 1;
  }
  if (flag(/^-u/)) {
    s3.consoleEmitters();
    fn = sync;
    s3.simpleS3Queue(limit);
    fnDone = () => { process.exit(); };
  } else if (flag('d')) fn = s3.download;
  else if (flag('s')) fn = s3.status;
  else {
    srv = web.new({ port: 14032 });
    await srv
      .addHandler(/tmp\/(.*)/, web.modFiles(1))
      .addHandler(true, web.custFiles('public'))
      .start();
    // or xdg-open?
    if (flag('g')) {
      browser = exec(`chromium --new-window http://${srv.host}:${srv.port}/ux.html`);
      // browser = exec(`firefox --new-window http://${srv.host}:${srv.port}/preact.html`);
      browser.stdout.pipe(process.stdout);
      browser.stderr.pipe(process.stderr);
    }
    s3Socket(srv);
  }

  for (const d of dirs) {
    if (flag(d.c, 'u')) {
      console.log('starting ', d.name);
      await fn.apply(null, d.args);
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
  for (const d of dirs) {
    srv.jh.addCbJson(d.name, async o => {
      if (inProgress) console.log('*** reject, another sync is in progress', d.name);
      inProgress = true;
      await sync.apply(null, o.args);
      inProgress = false;
    });
  }

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
