const { S3 } = require('aws-sdk'),
  $path = require('path'),
  pj = $path.join,
  fs = require('./fs-promise'),
  EventEmitter = require('events'),
  emitter = new EventEmitter(),
  ev = {
    end: 'end',
    file_all: 'file_all',
    file_comp_done: 'file_comp_done',
    file_dq: 'file_dq',
    file_end: 'file_end',
    file_eq: 'file_eq',
    file_list_done: 'file_list_done',
    file_q: 'file_q',
    file_skip: 'file_skip',
    file_start: 'file_start',
    file_unused: 'file_unused',
    file_del: 'file_del',
    get_remote: 'fetch_remote',
    progress: 'progress',
    stats: 'stats',
  },
  sleep = ms => new Promise(res => setTimeout(res, ms)),
  psmb = 8,
  logger = {
    log: console.log,
    write: x => { process.stdout.write(x); },
    _buf: '',
    queue: x => logger._buf += x + '\n',
    pq: () => console.log(logger._buf)
  };

const s3 = new S3();
// comma separated number
const csn = n => n.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ',');

// file filter check - string(contains), regexp(test), function(return true)
const chkFilter = (stat, filters) => {
  if (!filters || !Array.isArray(filters)) return false;
  for (let f of filters) {
    if (f instanceof RegExp && f.test(stat.key)) return true;
    else if (typeof f === 'string' && stat.key.indexOf(f) >= 0) return true;
    else if (typeof f === 'function' && f(stat.key) === true) return true;
  }
  return false;
};

async function getRemote(bucket, lpath) {
  logger.write(`loading ${bucket}:`);
  const files = new Map();
  function addR(arr) {
    for (q of arr) {
      //if (/-/.test(q.ETag)) logger.log(q.Key, q.Size, q.ETag);
      files.set(q.Key, {
        key: q.Key, size: q.Size, bucket,
        etag: q.ETag.replace(/"/g, ''),
        path: pj(lpath, q.Key)
      });
    }
    emitter.emit(ev.get_remote, { length: files.size, done: false });
  }
  var objs = await s3.listObjects({
    Bucket: bucket,
    MaxKeys: 1000
  }).promise();
  let np = objs;
  addR(np.Contents);
  logger.write('.');
  while (np.$response.hasNextPage()) {
    np = await np.$response.nextPage().promise();
    addR(np.Contents);
    logger.write('.');
  }
  logger.write('done\n');
  emitter.emit(ev.get_remote, { length: files.size, done: true });
  return files;
}

async function s3Equal(lstat, s3map) {
  if (!s3map.has(lstat.key)) { return lstat.eq = 1; }
  let rstat = s3map.get(lstat.key);
  rstat.used = true;
  lstat.s3 = rstat;
  if (lstat.size !== rstat.size) { return lstat.eq = 2; }
  let letag = await lstat.etag(psmb);
  if (letag !== rstat.etag) { return lstat.eq = 3; }
  else return lstat.eq = 0;
}

async function sync(path, bucket, filters) {
  statReset();
  const dt0 = Date.now();
  const s3config = { bucket: bucket, dry: false };

  s3config.map = await getRemote(s3config.bucket, path);

  let aprs = [];
  emitter.emit(ev.progress, { status: 'start', path });
  const objs = await fs.deepstats('', path, stats => {
    for (let s of stats) {
      if (s.isDir || !s.isFile || s.size === 0
        || chkFilter(s, filters)) {
        emitter.emit(ev.file_skip, s);
        continue;
      }
      emitter.emit(ev.file_all, s);
      s.bucket = bucket;
      let pr = s3Equal(s, s3config.map).then(status => {
        if (status === 0) emitter.emit(ev.file_eq, s);
        else emitter.emit(ev.file_q, s);
      });
      aprs.push(pr);
    }
  });
  emitter.emit(ev.file_list_done, path);
  await Promise.all(aprs);
  emitter.emit(ev.file_comp_done, path);

  let unused = Array.from(s3config.map.values()).filter(x => !x.used),
    unusedcnt = unused.length;
  emitter.emit(ev.file_unused, { path, cnt: unusedcnt, files: unused.slice() });

  let retval = true;
  if (finishQueue) retval = await finishQueue(unused, path);

  const dt1 = Date.now();
  emitter.emit(ev.end, { dt0, dt1, path });

  return retval;
}

/******************************************************************************/
// event handlers - console especially
const statPrint = s => csn(s.len) + '/' + csn(s.size);
let statReset, statObject;
function StatsEvents() {
  let all, done, skip, eq, q, active, del
    , lastStat = 0, pendStat = false;
  const
    emitStat = () => {
      if (!lastStat) {
        emitter.emit(ev.stats, statObject);
        pendStat = false;
        lastStat++;
        setTimeout(() => { lastStat = 0; if (pendStat) emitStat(); }, 200);
      } else pendStat = true;
    }
    , stat = o => Object.assign({ len: 0, size: 0 }, o),
    arem = (a, o) => {
      let i = a.indexOf(o);
      if (i >= 0) a.splice(i, 1);
    },
    op = (s, files, o = 1) => {
      if (!Array.isArray(files)) files = [files];
      o = o < 0 ? -1 : 1;
      for (let f of files) {
        s.len += o;
        s.size += o * f.size;
        if ('files' in s) {
          if (o < 0) arem(s.files, f);
          else s.files.push(f);
        }
      }
      emitStat();
    }, sp = statPrint;
  statReset = () => {
    all = stat(); done = stat(); skip = stat(); eq = stat();
    q = stat(); active = stat({ files: [] }); del = stat({ files: [] });
    statObject = { all, done, skip, eq, q, active, del };
  };
  statReset();
  emitter.on(ev.file_all, f => op(all, f));
  emitter.on(ev.file_skip, f => op(skip, f));
  emitter.on(ev.file_eq, f => op(eq, f));
  emitter.on(ev.file_q, f => op(q, f));
  emitter.on(ev.file_dq, f => op(q, f, -1));
  //emitter.on(ev.file_start, f => op(done, f));
  emitter.on(ev.file_end, f => op(done, f));
  emitter.on(ev.file_list_done, path => {
    emitter.emit(ev.progress, {
      status: `file list done ${sp(all)}`,
      path
    });
  });
  emitter.on(ev.file_comp_done, path => {
    emitter.emit(ev.progress, { status: 'hash/compare done', path });
  });

  emitter.on(ev.file_unused, ({ path, delcnt, files }) => {
    op(del, files);
    emitter.emit(ev.progress, { status: 'delete', path, delcnt, files });
  });
  emitter.on(ev.end, ({ dt0, dt1, path }) => {
    logger.queue(`*** stats: ${path}`);
    logger.queue(`    time elapsed: ${dt1 - dt0}`);
    logger.queue(`    files checked: ${sp(all)}`);
    logger.queue(`    files uploaded: ${sp(done)}`);
    logger.queue(`    files unchanged: ${sp(eq)}`);
    logger.queue(`    files skipped: ${sp(skip)}`);
    logger.queue(`    files deleted: ${sp(del)}`);
  });
}
StatsEvents();

function consoleEmitters() {
  emitter.on(ev.progress, obj => {
    logger.log(`*** ${obj.status}: ${obj.path}`);
  });

  emitter.on(ev.file_start, file => logger.log(
    statPrint(statObject.done), 'q', statPrint(statObject.q),
    'begin', file.key, file.size, 'code', file.eq));
  emitter.on(ev.file_end, file => logger.log(
    statPrint(statObject.done), 'q', statPrint(statObject.q),
    'end', file.key, file.size, 'time', Date.now() - file.dt0));
}

/******************************************************************************/
let finishQueue = null;
function simpleS3Queue() {
  let upcnt = 0, queue = [], compDone = false,
    handleQ = s => {
      if (s) queue.push(s);
      if (upcnt < 20 && queue.length) {
        let up = queue.pop();
        emitter.emit(ev.file_dq, up);
        upcnt++;
        qprs.push(s3Upload(up));
      }
    }, qprs = [];;
  emitter.on(ev.file_comp_done, x => compDone = true);
  emitter.on(ev.file_q, s => handleQ(s));
  emitter.on(ev.file_end, s => {
    upcnt--;
    handleQ();
  });
  finishQueue = async (unused, path) => {
    while (!compDone || upcnt || queue.length) {
      await Promise.all(qprs);
    };

    await s3delete(unused, path);
    return true;
  }
}

function s3Upload(stat) {
  stat.dt0 = Date.now();
  emitter.emit(ev.file_start, stat);

  let pr = s3.upload({
    Bucket: stat.bucket,
    Key: stat.key,
    Body: fs.createReadStream(stat.path),
  }, { partSize: psmb * 1024 * 1024 }).promise();
  pr = pr.then(x => {
    emitter.emit(ev.file_end, stat);
  });
  return pr;
}

async function s3delete(dels, path) {
  while (dels.length) {
    let dbatch = dels.splice(0, 40);
    let dprs = dbatch.map(x => s3.deleteObject({
      Bucket: x.bucket,
      Key: x.key
    }).promise());
    await Promise.all(dprs);
    emitter.emit(ev.file_del, { status: 'batch', path, files: dbatch });
    logger.write('.');
  }
}


/******************************************************************************/

/******************************************************************************/
// status stuff
async function getVersions(bucket) {
  logger.write(`loading ${bucket}:`);
  const files = new Map();
  function addR(arr, isDel) {
    for (q of arr) {
      if (isDel) q.Delete = true;
      delete q.Owner;
      delete q.StorageClass;
      if (files.has(q.Key)) {
        let val = files.get(q.Key);
        val.objs.push(q);
      } else {
        files.set(q.Key, { key: q.Key, objs: [q] });
      }
    }
  }
  var objs = await s3.listObjectVersions({
    Bucket: bucket,
    MaxKeys: 1000
  }).promise();
  let np = objs;
  addR(np.DeleteMarkers, 1);
  addR(np.Versions);
  logger.write('.');
  while (np.$response.hasNextPage()) {
    np = await np.$response.nextPage().promise();
    addR(np.DeleteMarkers, 1);
    addR(np.Versions);
    logger.write('.');
  }
  logger.write('done\n');
  return files;
}

async function status(path, bucket) {
  const dt0 = Date.now();
  const s3config = { bucket: bucket, dry: false };

  s3config.map = await getVersions(s3config.bucket);
  let multi = Array.from(s3config.map.values()).filter(x => x.objs.length > 1);
  console.log(multi.length);
  console.log(JSON.stringify(multi, null, 2));
}
/******************************************************************************/

module.exports = {
  sync, status,
  printQueue: logger.pq,
  on: (name, ...a) => emitter.on(name, ...a),
  once: (name, ...a) => emitter.once(name, ...a),
  consoleEmitters, simpleS3Queue, ev
};