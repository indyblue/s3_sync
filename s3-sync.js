const { S3 } = require('aws-sdk'),
  fs = require('./fs-promise'),
  EventEmitter = require('events'),
  emitter = new EventEmitter(),
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

async function getRemote(bucket, prefix) {
  logger.write(`loading ${bucket}[${prefix}]:`);
  const files = new Map();
  function addR(arr) {
    for (q of arr) {
      //if (/-/.test(q.ETag)) logger.log(q.Key, q.Size, q.ETag);
      files.set(q.Key, { key: q.Key, size: q.Size, etag: q.ETag.replace(/"/g, '') });
    }
  }
  var objs = await s3.listObjects({
    Bucket: bucket,
    Prefix: prefix,
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
  return files;
}

async function getVersions(bucket, prefix) {
  logger.write(`loading ${bucket}[${prefix}]:`);
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
    Prefix: prefix,
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

async function s3Equal(lstat, s3map) {
  if (!s3map.has(lstat.key)) return 1;
  let rstat = s3map.get(lstat.key);
  rstat.used = true;
  if (lstat.size !== rstat.size) return 2;
  let letag = await lstat.etag(psmb);
  if (letag !== rstat.etag) {
    //console.log(lstat.key, letag, rstat.etag);
    return 3;
  }
  else return 0;
}

let qcnt = 0, queue = [],
  tryq = (item) => {
    if (item) queue.push(item);
    if (qcnt < 20 && queue.length) {
      let obj = queue.pop();
      emitter.emit('file-dq', obj.file);
      let pr = obj.fn();
      qprs.push(pr);
    }
  },
  qprs = [];
async function s3Action(lstat, s3config) {
  lstat.eq = await s3Equal(lstat, s3config.map);
  if (lstat.eq === 0) {
    emitter.emit('file-eq', lstat);
    return;
  }

  if (s3config.dry) {
    emitter.emit('file-end', lstat);
    return;
  }
  let fnup = () => {
    qcnt++;
    let dt0 = Date.now();
    emitter.emit('file-start', lstat);

    let pr = s3.upload({
      Bucket: s3config.bucket,
      Key: lstat.key,
      Body: fs.createReadStream(lstat.path),
    }, { partSize: psmb * 1024 * 1024 }).promise();
    pr = pr.then(x => {
      emitter.emit('file-end', lstat);
      qcnt--;
      tryq();
    });
    pr.lstat = lstat;
    lstat.dt0 = dt0;
    return pr;
  };
  emitter.emit('file-q', lstat);
  tryq({ file: lstat, fn: fnup });
}

const chkFilter = (stat, filters) => {
  if (!filters || !Array.isArray(filters)) return false;
  for (let f of filters) {
    if (f instanceof RegExp && f.test(stat.key)) return true;
    else if (typeof f === 'string' && stat.key.indexOf(f) >= 0) return true;
    else if (typeof f === 'function' && f(stat.key) === true) return true;
  }
  return false;
};
const csn = n => n.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ',');
async function sync(path, bucket, prefix, filters) {
  statReset();
  const dt0 = Date.now();
  const s3config = { bucket: bucket, dry: false };

  s3config.map = await getRemote(s3config.bucket, prefix);

  let aprs = [];
  emitter.emit('progress', { status: 'start', path });
  const objs = await fs.deepstats('', path, stats => {
    for (let s of stats) {
      if (s.isDir || !s.isFile || s.size === 0
        || chkFilter(s, filters)) {
        emitter.emit('file-skip', s);
        continue;
      }
      emitter.emit('file-all', s);

      aprs.push(s3Action(s, s3config));
    }
  });
  emitter.emit('file-list-done', path);
  await Promise.all(aprs);
  emitter.emit('progress', { status: 'hash/compare done', path });
  while (queue.length || qcnt) {
    await Promise.all(qprs);
  }
  emitter.emit('progress', { status: 'uploads done', path });
  let dels = Array.from(s3config.map.values()).filter(x => !x.used),
    delcnt = dels.length;
  emitter.emit('file-del', { path, delcnt, files: dels.slice() });
  if (s3config.dry) console.log(dels.map(x => x.key).join('\n'));
  while (!s3config.dry && dels.length) {
    let dbatch = dels.splice(0, 40);
    let dprs = dbatch.map(x => s3.deleteObject({
      Bucket: s3config.bucket,
      Key: x.key
    }).promise());
    await Promise.all(dprs);
    emitter.emit('delete', { status: 'batch', path, files: dbatch });
    logger.write('.');
  }
  //console.log(JSON.stringify(dels, null, 2));
  emitter.emit('progress', { status: 'delete done', path });

  const dt1 = Date.now();
  emitter.emit('end', { dt0, dt1, path });

  return true;
}

async function status(path, bucket, prefix) {
  const dt0 = Date.now();
  const s3config = { bucket: bucket, dry: false };

  s3config.map = await getVersions(s3config.bucket, prefix);
  let multi = Array.from(s3config.map.values()).filter(x => x.objs.length > 1);
  console.log(multi.length);
  console.log(JSON.stringify(multi, null, 2));
}

const statPrint = s => csn(s.len) + '/' + csn(s.size);
let statReset, statObject;
function StatsEvents() {
  let all, done, skip, eq, q, active, del;
  const stat = o => Object.assign({ len: 0, size: 0 }, o),
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
      emitter.emit('stats', statObject);
    }, sp = statPrint;
  statReset = () => {
    all = stat(); done = stat(); skip = stat(); eq = stat();
    q = stat(); active = stat({ files: [] }); del = stat({ files: [] });
    statObject = { all, done, skip, eq, q, active, del };
  };
  statReset();
  emitter.on('file-all', f => op(all, f));
  emitter.on('file-skip', f => op(skip, f));
  emitter.on('file-eq', f => op(eq, f));
  emitter.on('file-q', f => op(q, f));
  emitter.on('file-dq', f => op(q, f, -1));
  //emitter.on('file-start', f => op(done, f));
  emitter.on('file-end', f => op(done, f));
  emitter.on('file-list-done', path => {
    emitter.emit('progress', {
      status: `file list done ${sp(all)}`,
      path
    });
  });
  emitter.on('file-del', ({ path, delcnt, files }) => {
    op(del, files);
    emitter.emit('progress', { status: 'begin delete', path, delcnt, files });
  });
  emitter.on('end', ({ dt0, dt1, path }) => {
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
  emitter.on('progress', obj => {
    logger.log(`*** ${obj.status}: ${obj.path}`);
  });

  emitter.on('file-start', file => logger.log(
    statPrint(statObject.done), 'q', statPrint(statObject.q),
    'begin', file.key, file.size, 'code', file.eq));
  emitter.on('file-end', file => logger.log(
    statPrint(statObject.done), 'q', statPrint(statObject.q),
    'end', file.key, file.size, 'time', Date.now() - file.dt0));
}

module.exports = {
  sync, status,
  printQueue: logger.pq,
  on: (name, ...a) => emitter.on(name, ...a),
  once: (name, ...a) => emitter.once(name, ...a),
  consoleEmitters
};