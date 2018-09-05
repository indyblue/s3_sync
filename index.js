const { S3 } = require('aws-sdk'),
  fso = require('fs'),
  $path = require('path'),
  pj = $path.join,
  util = require('util'),
  etag = require('./etag'),
  fs = {
    readdir: util.promisify(fso.readdir),
    lstat: util.promisify(fso.lstat),
    estat: (f, p) => fs.lstat(p || f).then(x => Object.assign({
      key: f,
      path: p || f,
      isDir: x.isDirectory(),
      isFile: x.isFile(),
      etag: (psmb) => etag(p || f, psmb)
    }, x)),
    readstats: async (dir, root) => {
      let keys = await fs.readdir(pj(root, dir));
      let prs = keys.map(k => fs.estat(pj(dir, k), pj(root, dir, k)));
      let stats = await Promise.all(prs);
      return stats;
    },
    deepstats: async (dir, root, cbData) => {
      const stats = [], prs = [], hasCb = typeof cbData === 'function';
      let sdir = await fs.readstats(dir, root);
      if (hasCb) cbData(sdir);
      else stats.push(...sdir);
      for (s of sdir) {
        if (s.isDir) prs.push(fs.deepstats(s.key, root, cbData));
      }
      var results = await Promise.all(prs);
      if (!hasCb) for (let r of results) stats.push(...r);
      return stats;
    }
  };

const s3 = new S3();

async function getRemote(bucket, prefix) {
  const files = new Map();
  function addR(arr) {
    for (q of arr) {
      //if (/-/.test(q.ETag)) console.log(q.Key, q.Size, q.ETag);
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
  while (np.$response.hasNextPage()) {
    np = await np.$response.nextPage().promise();
    addR(np.Contents);
  }
  var q = 1;
  return files;
}


async function s3Equal(lstat, s3map) {
  if (lstat.isDir) return 0;
  if (lstat.key === 'data') return 0;

  if (!s3map.has(lstat.key)) return 1;
  let rstat = s3map.get(lstat.key);
  if (lstat.size !== rstat.size) return 2;
  let letag5 = await lstat.etag(5);
  let letag8 = await lstat.etag(8);
  if (letag5 !== rstat.etag && letag8 !== rstat.etag) {
    console.log(lstat.key, letag5, letag8, rstat.etag);
    return 3;
  }
  else return 0;
}
async function s3Action(lstat, s3config, cb0, cbThen) {
  let eq = await s3Equal(lstat, s3config.map);
  if (eq === 0) return true;

  let dt0 = Date.now();
  if (s3config.dry) console.log('dry', lstat.key, lstat.size);
  if (lstat.size === 0) console.log('zero', lstat.key, lstat.size);
  else {
    if (typeof cb0 === 'function') cb0(eq);

    let pr = s3.upload({
      Bucket: s3config.bucket,
      Key: lstat.key,
      Body: fso.createReadStream(lstat.path),
    }, { partSize: 8 * 1024 * 1024 }).promise();
    if (typeof cbThen === 'function') pr = pr.then(cbThen);
    pr.lstat = lstat;
    lstat.dt0 = dt0;
    return pr;
  }
}

const bp = f => pj(lpath, f);
const csn = n => n.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ',');
async function main() {
  const s3config = { bucket: 'das-1-docs', dry: false },
    lpath = '/home/user/0das/1/',
    prs = [];

  s3config.map = await getRemote(s3config.bucket, '');

  let ttl = 0, compl = 0;
  const objs = await fs.deepstats('', lpath, stats => {
    for (let s of stats) prs.push(s3Action(s, s3config, x => {
      ttl += s.size;
      console.log(csn(compl), '/', csn(ttl), 'begin', s.key, s.size, 'code', x);
    }, x => {
      compl += s.size;
      console.log(csn(compl), '/', csn(ttl), 'end', s.key, s.size, 'time', Date.now() - s.dt0);
    }));
  });
  return await Promise.all(prs);
}
main();
