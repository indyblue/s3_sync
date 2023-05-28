/* eslint-disable no-unused-vars, promise/param-names */
/* eslint-disable no-return-assign */
const { S3 } = require('aws-sdk');
const s3 = new S3();

const { S3: S3v3 } = require('@aws-sdk/client-s3');
const s3v3 = new S3v3({ region: 'us-east-2' });

const $path = require('path');
const pj = $path.join;
const fs = require('./fs-promise');
const { psb } = require('./etag');
const EventEmitter = require('events');
const emitter = new EventEmitter();
const ev = {
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
};
const sleep = ms => new Promise(res => setTimeout(res, ms));
const logger = {
  log: console.log,
  write: x => { process.stdout.write(x); },
  _buf: '',
  queue: x => logger._buf += x + '\n',
  pq: () => console.log(logger._buf),
};

// comma separated number
const csn = n => n?.toString()?.replace(/\B(?=(\d{3})+(?!\d))/g, ',');

async function getRemote(bucket, lpath) {
  logger.write(`loading ${bucket}:`);

  const cpath = `cache/${bucket}.json`;
  const cfiles = await fs.safeReadObj(cpath);
  const ckeys = Object.keys(cfiles).sort();
  const ci = 5;
  const cint = Math.max(Math.floor(ckeys.length / ci), 900);
  const seeds = ckeys.filter((_, i) => i && i % cint === 0);
  seeds.unshift(null);

  const files = new Map();
  function addR(arr, si) {
    if (!arr?.length) {
      return true;
    }
    let end = false;
    let i = 0;
    const nseed = seeds[si + 1];
    for (const q of arr) {
      if ((nseed && q.Key > nseed)
        || files.has(q.Key)) {
        end = true;
        break;
      }
      // if (/-/.test(q.ETag)) logger.log(q.Key, q.Size, q.ETag);
      files.set(q.Key, {
        key: q.Key,
        size: q.Size,
        bucket,
        etag: q.ETag.replace(/"/g, ''),
        path: pj(lpath, q.Key),
        isS3: true,
        eq: -1,
      });
      i++;
    }
    emitter.emit(ev.get_remote, { length: files.size, done: false });
    return end;
  }
  console.time('files');
  console.log(seeds);
  const prs = seeds.map(async (x, i) => {
    let resp;
    do {
      const ContinuationToken = resp?.NextContinuationToken;
      const StartAfter = x && !ContinuationToken ? x : undefined;
      resp = await s3v3.listObjectsV2({
        Bucket: bucket,
        StartAfter,
        ContinuationToken,
      });
      if (addR(resp.Contents, i)) break;
      logger.write('.');
    } while (resp.NextContinuationToken);
  });
  await Promise.all(prs);

  logger.write('done\n');
  await fs.writeFileObj(cpath, files);
  emitter.emit(ev.get_remote, { length: files.size, done: true });
  console.timeEnd('files');
  return files;
}

async function s3Equal(lstat, s3map) {
  if (!s3map.has(lstat.key)) { return lstat.eq = 1; }
  const rstat = s3map.get(lstat.key);
  rstat.used = true;
  lstat.s3 = rstat;
  if (lstat.size !== rstat.size) { return lstat.eq = 2; }
  const letag = await lstat.etag();
  if (letag !== rstat.etag) { return lstat.eq = 3; } else return lstat.eq = 0;
}

async function sync(path, bucket, filters) {
  statReset();
  const dt0 = Date.now();
  const s3config = { bucket, dry: false };

  s3config.map = await getRemote(s3config.bucket, path);

  emitter.emit(ev.progress, { status: 'start', path });
  const objs = await fs.deepstats('', path, filters, async stats => {
    const rv = [];
    for (const s of stats) {
      if (s.isDir || !s.isFile || s.size === 0) {
        // emitter.emit(ev.file_skip, s);
        continue;
      }
      rv.push(s);
      emitter.emit(ev.file_all, s);
      s.bucket = bucket;
      const st = await s3Equal(s, s3config.map);
      if (st === 0) emitter.emit(ev.file_eq, s);
      else emitter.emit(ev.file_q, s);
    }
    return rv;
  });
  emitter.emit(ev.file_list_done, path);
  emitter.emit(ev.file_comp_done, path);

  const unused = Array.from(s3config.map.values()).filter(x => !x.used);
  const unusedcnt = unused.length;
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
  let all; let done; let skip; let eq; let q; let active; let del
    ; let lastStat = 0; let pendStat = false;
  const
    emitStat = () => {
      if (!lastStat) {
        emitter.emit(ev.stats, statObject);
        pendStat = false;
        lastStat++;
        setTimeout(() => { lastStat = 0; if (pendStat) emitStat(); }, 200);
      } else pendStat = true;
    };
  const stat = o => Object.assign({ len: 0, size: 0 }, o);
  const arem = (a, o) => {
    const i = a.indexOf(o);
    if (i >= 0) a.splice(i, 1);
  };
  const op = (s, files, o = 1) => {
    if (!Array.isArray(files)) files = [files];
    o = o < 0 ? -1 : 1;
    for (const f of files) {
      s.len += o;
      s.size += o * f.size;
      if ('files' in s) {
        if (o < 0) arem(s.files, f);
        else s.files.push(f);
      }
    }
    emitStat();
  }; const sp = statPrint;
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
  // emitter.on(ev.file_start, f => op(done, f));
  emitter.on(ev.file_end, f => op(done, f));
  emitter.on(ev.file_list_done, path => {
    emitter.emit(ev.progress, {
      status: `file list done ${sp(all)}`,
      path,
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
function simpleS3Queue(limit) {
  let upcnt = 0; const queue = []; let compDone = false;
  const handleQ = s => {
    if (s) queue.push(s);
    if (upcnt < 20 && queue.length) {
      const up = queue.pop();
      emitter.emit(ev.file_dq, up);
      upcnt++;
      qprs.push(s3Upload(up));
    }
  };
  const qprs = [];
  emitter.on(ev.file_comp_done, x => compDone = true);
  emitter.on(ev.file_q, s => handleQ(s));
  emitter.on(ev.file_end, s => {
    upcnt--;
    handleQ();
  });
  finishQueue = async (unused, path) => {
    /* eslint-disable-next-line no-unmodified-loop-condition */
    while (!compDone || upcnt || queue.length) {
      await Promise.all(qprs);
    };

    if (limit) return true;
    const dels = unused;
    while (dels.length) {
      const dbatch = dels.splice(0, 40);
      const dprs = dbatch.map(x => s3Delete(x));
      await Promise.all(dprs);
      emitter.emit(ev.file_del, { status: 'batch', files: dbatch });
      logger.write('.');
    }
    return true;
  };
}

const s3Upload = stat => {
  stat.dt0 = Date.now();
  emitter.emit(ev.file_start, stat);

  const s3up = s3.upload({
    Bucket: stat.bucket,
    Key: stat.key,
    Body: fs.createReadStream(stat.path),
  }, { partSize: psb });
  const pr = s3up.promise().then(x => {
    emitter.emit(ev.file_end, stat);
  });
  pr.stat = stat;
  pr.abort = () => s3up.abort();
  return pr;
};

const s3Download = stat => new Promise((resolve, reject) => {
  stat.dt0 = Date.now();
  emitter.emit(ev.file_start, stat);

  fs.ensureFileSync(stat.path);
  const fileStream = fs.createWriteStream(stat.path);

  const s3stream = s3.getObject({
    Bucket: stat.bucket,
    Key: stat.key,
  }).createReadStream();
  s3stream.on('error', e => reject(e));
  s3stream.on('finish', () => {
    emitter.emit(ev.file_end, stat);
    resolve(stat);
  });
  s3stream.pipe(fileStream);
});

const s3Delete = stat => s3.deleteObject({
  Bucket: stat.bucket,
  Key: stat.key,
}).promise()
  .then(x => emitter.emit(ev.file_end, stat));

const localDelete = async stat => {
  fs.unlinkSync(stat.path);
  emitter.emit(ev.file_end, stat);
};

const takeAction = async (stat, withDels = false) => {
  if (!stat.action) return;
  if (stat.isLocal) {
    if (stat.action === 1) { return s3Upload(stat); } else if (stat.action === -1) {
      if (stat.s3) { return s3Download(stat); } else if (withDels) { return localDelete(stat); }
    }
  } else if (stat.isS3) {
    /* eslint-disable-next-line brace-style */
    if (stat.action === -1) { return s3Download(stat); }
    else if (stat.action === 1 && withDels) { return s3Delete(stat); }
  }
};

let acnt = 0;
const aq = []; const aprs = []; const amax = 5; // 20;
const enq = (s, withDels) => {
  if (s) {
    aq.push(s);
    // emitter.emit(ev.file_q, s);
  }
  if (acnt < amax && aq.length) {
    const up = aq.shift();
    // emitter.emit(ev.file_dq, up);
    acnt++;
    const pr = takeAction(up, withDels).then(x => {
      acnt--; enq(null, withDels);
    });
    aprs.push(pr);
  };
};
const actionQueue = async (arr, withDels) => {
  if (arr instanceof Array) arr.map(s => enq(s, withDels));
  else enq(arr, withDels);
  await Promise.all(aprs);
};

/******************************************************************************/

/******************************************************************************/
// status stuff
async function getVersions(bucket) {
  logger.write(`loading ${bucket}:`);
  const files = new Map();
  function addR(arr, isDel) {
    for (const q of arr) {
      if (isDel) q.Delete = true;
      delete q.Owner;
      delete q.StorageClass;
      if (files.has(q.Key)) {
        const val = files.get(q.Key);
        val.objs.push(q);
      } else {
        files.set(q.Key, { key: q.Key, objs: [q] });
      }
    }
  }
  const objs = await s3.listObjectVersions({
    Bucket: bucket,
    MaxKeys: 1000,
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
  const s3config = { bucket, dry: false };

  s3config.map = await getVersions(s3config.bucket);
  const multi = Array.from(s3config.map.values()).filter(x => x.objs.length > 1);
  console.log(multi.length);
  console.log(JSON.stringify(multi, null, 2));
}
/******************************************************************************/

module.exports = {
  sync,
  status,
  printQueue: logger.pq,
  on: (name, ...a) => emitter.on(name, ...a),
  once: (name, ...a) => emitter.once(name, ...a),
  consoleEmitters,
  simpleS3Queue,
  ev,
  actionQueue,
};
