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
  if (lstat.isDir) return 0;
  if (!lstat.isFile) return 0;
  if (lstat.key === 'data') return 0;
  if (lstat.key.indexOf('node_modules') >= 0) return 0;
  if (lstat.size === 0) return 0;

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

let qcnt = 0, qsize = 0,
  queue = [],
  tryq = (item) => {
    if (item) queue.push(item);
    if (qcnt < 20 && queue.length) {
      let fn = queue.pop();
      let pr = fn();
      qprs.push(pr);
    }
  },
  qprs = [];
async function s3Action(lstat, s3config, cb0, cbThen) {
  lstat.eq = await s3Equal(lstat, s3config.map);
  if (lstat.eq === 0) return true;

  if (s3config.dry) console.log('dry', lstat.key, lstat.size);
  else if (lstat.size === 0) return true; //console.log('zero', lstat.key, lstat.size);
  else {
    qsize += lstat.size;
    let fnup = () => {
      qcnt++; qsize -= lstat.size;
      let dt0 = Date.now();
      if (typeof cb0 === 'function') cb0();

      let pr = s3.upload({
        Bucket: s3config.bucket,
        Key: lstat.key,
        Body: fs.createReadStream(lstat.path),
      }, { partSize: psmb * 1024 * 1024 }).promise();
      if (typeof cbThen === 'function') pr = pr.then(cbThen).then(x => {
        qcnt--;
        tryq();
      });
      pr.lstat = lstat;
      lstat.dt0 = dt0;
      return pr;
    };
    tryq(fnup);
  }
}

const csn = n => n.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ',');
async function sync(path, bucket, prefix) {
  const dt0 = Date.now();
  const s3config = { bucket: bucket, dry: false };

  s3config.map = await getRemote(s3config.bucket, prefix);

  let ttl = 0, compl = 0, aprs = [], upcnt = 0, filecnt = 0, filesize = 0;
  emitter.emit('progress', { status: 'start', path });
  const objs = await fs.deepstats('', path, stats => {
    for (let s of stats) {
      if (s.isFile) { filecnt++; filesize += s.size; }
      aprs.push(s3Action(s, s3config, x => {
        upcnt++; ttl += s.size;
        emitter.emit('afile-s', { status: 'begin', file: s, compl, ttl, queue: { size: qsize, len: queue.length } });
        console.log(csn(compl), '/', csn(ttl), 'q', csn(qsize), queue.length, 'begin', s.key, s.size, 'code', s.eq);
      }, x => {
        compl += s.size;
        emitter.emit('afile-e', { status: 'end', file: s, compl, ttl });
        console.log(csn(compl), '/', csn(ttl), 'end', s.key, s.size, 'time', Date.now() - s.dt0);
        //console.log(JSON.stringify(process.memoryUsage()));
      }));
    }
  });
  emitter.emit('progress', { status: 'file list done', path, filecnt, filesize });
  await Promise.all(aprs);
  emitter.emit('progress', { status: 'hash/compare done', path });
  while (queue.length || qcnt) {
    await Promise.all(qprs);
  }
  emitter.emit('progress', { status: 'uploads done', path });
  let dels = Array.from(s3config.map.values()).filter(x => !x.used),
    delcnt = dels.length;
  emitter.emit('progress', { status: 'begin delete', path, delcnt, files: dels.slice() });
  while (dels.length) {
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
  logger.queue(`*** stats: ${path}`);
  logger.queue(`    time elapsed: ${dt1 - dt0}`);
  logger.queue(`    files checked: ${csn(filecnt)} / ${csn(filesize)}`);
  logger.queue(`    files uploaded: ${csn(upcnt)} / ${csn(compl)}`);
  logger.queue(`    files deleted: ${csn(delcnt)}`);
  //await sleep(2000);
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

function consoleEmitters() {
  emitter.on('progress', obj => {
    let addl = '';
    //console.log('progress event', obj);
    switch (obj.status) {
      case 'file list done':
        addl = ` (${obj.filecnt}, ${csn(obj.filesize)})`;
        break;
      case 'begin delete':
        addl = ` (${obj.delcnt})`;
        break;
    }
    logger.log(`*** ${obj.status}${addl}: ${obj.path}`);
  });

  emitter.on('afile-s', o => logger.log(csn(o.compl), '/', csn(o.ttl),
    'q', csn(o.queue.size), o.queue.len,
    'begin', o.file.key, o.file.size, 'code', o.file.eq));
  emitter.on('afile-e', o => logger.log(csn(o.compl), '/', csn(o.ttl),
    'end', o.file.key, o.file.size, 'time', Date.now() - o.file.dt0));
}

module.exports = {
  sync, status,
  printQueue: logger.pq,
  on: (name, ...a) => emitter.on(name, ...a),
  once: (name, ...a) => emitter.once(name, ...a),
  consoleEmitters
};