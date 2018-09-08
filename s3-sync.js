const { S3 } = require('aws-sdk'),
  fs = require('./fs-promise'),
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

async function s3Equal(lstat, s3map) {
  if (lstat.isDir) return 0;
  if (!lstat.isFile) return 0;
  if (lstat.key === 'data') return 0;
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
  logger.log(`*** start checking files: ${path}`);
  const objs = await fs.deepstats('', path, stats => {
    for (let s of stats) {
      if (s.isFile) { filecnt++; filesize += s.size; }
      aprs.push(s3Action(s, s3config, x => {
        upcnt++; ttl += s.size;
        console.log(csn(compl), '/', csn(ttl), 'q', csn(qsize), queue.length, 'begin', s.key, s.size, 'code', s.eq);
      }, x => {
        compl += s.size;
        console.log(csn(compl), '/', csn(ttl), 'end', s.key, s.size, 'time', Date.now() - s.dt0);
        //console.log(JSON.stringify(process.memoryUsage()));
      }));
    }
  });
  logger.log(`*** done building file list (${filecnt}, ${csn(filesize)}): ${path}`);
  await Promise.all(aprs);
  logger.log(`*** done hashing/comparing files: ${path}`);
  while (queue.length || qcnt) {
    await Promise.all(qprs);
  }
  logger.log(`*** done with all uploads: ${path}`);
  logger.log(`*** deleting unused from s3: ${path}`);
  let dels = Array.from(s3config.map.values()).filter(x => !x.used),
    delcnt = dels.length;
  logger.write(dels.length.toString());
  while (dels.length) {
    let dprs = dels.splice(0, 40).map(x => s3.deleteObject({
      Bucket: s3config.bucket,
      Key: x.key
    }).promise());
    await Promise.all(dprs);
    logger.write('.');
  }
  //console.log(JSON.stringify(dels, null, 2));
  logger.log(`*** done deleting: ${path}`);

  const dt1 = Date.now();
  logger.queue(`*** stats: ${path}`);
  logger.queue(`    time elapsed: ${dt1 - dt0}`);
  logger.queue(`    files checked: ${csn(filecnt)} / ${csn(filesize)}`);
  logger.queue(`    files uploaded: ${csn(upcnt)} / ${csn(compl)}`);
  logger.queue(`    files deleted: ${csn(delcnt)}`);
  //await sleep(2000);
  return true;
}

sync.printQueue = logger.pq;
module.exports = sync;