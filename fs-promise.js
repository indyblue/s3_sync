const fso = require('fs');
const { ensureFileSync } = require('fs-extra');
const $path = require('path');
const pj = $path.join;
const util = require('util');
const etag = require('./etag');
const fs = {
  readdir: util.promisify(fso.readdir),
  lstat: util.promisify(fso.lstat),
  estat: (f, p) => fs.lstat(p || f).then(x => Object.assign({
    key: f,
    path: p || f,
    isDir: x.isDirectory(),
    isFile: x.isFile(),
    isLocal: true,
    etag: (psmb) => etag(p || f, psmb)
  }, x)),
  readstats: async (dir, root) => {
    let keys = await fs.readdir(pj(root, dir));
    let prs = keys.map(k => fs.estat(pj(dir, k), pj(root, dir, k)));
    let stats = await Promise.all(prs);
    return stats;
  },
  deepstats: async (dir, root, cbData) => {
    const stats = []; const prs = []; const hasCb = typeof cbData === 'function';
    let sdir = await fs.readstats(dir, root);
    if (hasCb) cbData(sdir);
    else stats.push(...sdir);
    for (let s of sdir) {
      if (s.isDir) prs.push(fs.deepstats(s.key, root, cbData));
    }
    var results = await Promise.all(prs);
    if (!hasCb) for (let r of results) stats.push(...r);
    return stats;
  },
  createReadStream: fso.createReadStream,
  createWriteStream: fso.createWriteStream,
  unlinkSync: fso.unlinkSync,
  ensureFileSync
};

module.exports = fs;
