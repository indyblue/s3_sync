const fso = require('fs');
const { ensureFileSync } = require('fs-extra');
const $path = require('path');
const pj = $path.join;
const util = require('util');
const etag = require('./etag');

// file filter check - string(contains), regexp(test), function(return true)
const chkFilter = (stat, filters) => {
  if (!filters || !Array.isArray(filters)) return false;
  for (const f of filters) {
    if (f instanceof RegExp && f.test(stat.key)) return true;
    else if (typeof f === 'string' && stat.key.indexOf(f) >= 0) return true;
    else if (typeof f === 'function' && f(stat.key) === true) return true;
  }
  return false;
};

const etagCache = async s => {
  const { key, path } = s;
  const cs = fs.cache?.[key];
  if (!s.isFile) return '';
  if (!cs?.etag || s.size !== cs.size || s.mtimeMs !== cs.mtimeMs) {
    s.etag = await etag(path || key);
  } else s.etag = cs.etag;
  return s.etag;
};

const fs = {
  readdir: util.promisify(fso.readdir),
  lstat: util.promisify(fso.lstat),
  estat: (f, p) => fs.lstat(p || f).then(x => ({
    key: f,
    path: p || f,
    isDir: x.isDirectory(),
    isFile: x.isFile(),
    isSymbolicLink: x.isSymbolicLink(),
    isLocal: true,
    etag: function() { return etagCache(this); },
    size: x.size,
    mtimeMs: x.mtimeMs,
  })),
  readstats: async (dir, root) => {
    const keys = await fs.readdir(pj(root, dir));
    const prs = keys.map(k => fs.estat(pj(dir, k), pj(root, dir, k)));
    const stats = await Promise.all(prs);
    return stats;
  },
  deepstats: async (dir, root, filters, cbData) => {
    console.time('local');
    console.log('loading local files...');
    const cpath = `cache/${root.replace(/\W+/g, '_')}.json`;
    fs.cache = await fs.safeReadObj(cpath);

    const stats = await fs._deepstats(dir, root, filters, cbData);

    await fs.writeFileObj(cpath, Object.fromEntries(
      stats.map(x => [
        x.key,
        { etag: x.etag, size: x.size, mtimeMs: x.mtimeMs },
      ]),
    ));
    console.timeEnd('local');
    return stats;
  },
  _deepstats: async (dir, root, filters, cbData) => {
    const prs = [];
    const fulldir = await fs.readstats(dir, root);
    const sdir = fulldir.filter(s => !chkFilter(s, filters));
    const stats = await cbData?.(sdir) || sdir;
    for (const s of sdir) {
      if (s.isDir) prs.push(fs._deepstats(s.key, root, filters, cbData));
    }
    const results = await Promise.all(prs);
    for (const r of results) stats.push(...r);
    return stats;
  },
  createReadStream: fso.createReadStream,
  createWriteStream: fso.createWriteStream,
  unlinkSync: fso.unlinkSync,
  ensureFileSync,
  safeReadObj: async (x, d = {}, f = 'utf8') => {
    try {
      const txt = await fso.promises.readFile(x, f);
      return JSON.parse(txt);
    } catch { }
    return d;
  },
  writeFileObj: async (x, c, f = 'utf8') => {
    if (c instanceof Map) c = Object.fromEntries([...c.entries()]);
    if (typeof c !== 'string') c = JSON.stringify(c, null, 2);
    await fso.promises.writeFile(x, c, f);
  },
};

module.exports = fs;
