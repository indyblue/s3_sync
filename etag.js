/* eslint-disable no-unused-vars */
const crypto = require('crypto');
const fs = require('fs');
const psmb = 8;
const psb = 8 * 1024 * 1024;
const etag = fname => new Promise((resolve, reject) => {
  const dt0 = Date.now();
  const hashes = []; let pos = 0; let chunk;
  let hash = crypto.createHash('md5');
  const rs = fs.createReadStream(fname);

  rs.on('data', x => {
    let tail = null; const rnd = psb - (pos % psb); const len = x.length;
    if (rnd <= len) {
      tail = x.slice(rnd);
      x = x.slice(0, rnd);
    }
    hash.update(x);
    if (tail) {
      hashes.push(hash.digest('hex'));
      hash = crypto.createHash('md5');
      hash.update(tail);
    }
    pos += len;
  });
  rs.on('end', x => {
    rs.close();
    const trash = crypto.createHash('md5').digest('hex');
    const last = hash.digest('hex');
    if (last !== trash) hashes.push(last);
    // hashes.map(x => console.log(x));
    if (hashes.length === 1) return resolve(hashes[0]);

    let final = crypto.createHash('md5');
    hashes.map(x2 => final.update(Buffer.from(x2, 'hex')));
    final = final.digest('hex') + '-' + hashes.length;
    // let ref = '8421d09ee84c4de6ddfc27d1c795d5f4-5'
    // console.log(final, ref === final);
    // console.log('duration', Date.now() - dt0);
    resolve(final);
  });
});

etag.psmb = psmb;
etag.psb = psb;

module.exports = etag;
