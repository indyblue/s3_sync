/* eslint-disable no-unused-vars */
const crypto = require('crypto');
const fs = require('fs');

const etag = (fname, psmb) => new Promise((resolve, reject) => {
  const dt0 = Date.now();
  let ps = (psmb || 8) * 1024 * 1024; let hashes = []; let pos = 0; let chunk;
  let hash = crypto.createHash('md5');
  let rs = fs.createReadStream(fname);

  rs.on('data', x => {
    let tail = null; let rnd = ps - (pos % ps); let len = x.length;
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
    let trash = crypto.createHash('md5').digest('hex');
    let last = hash.digest('hex');
    if (last !== trash) hashes.push(last);
    // hashes.map(x => console.log(x));
    if (hashes.length === 1) return resolve(hashes[0]);

    let final = crypto.createHash('md5');
    hashes.map(x => final.update(Buffer.from(x, 'hex')));
    final = final.digest('hex') + '-' + hashes.length;
    // let ref = '8421d09ee84c4de6ddfc27d1c795d5f4-5'
    // console.log(final, ref === final);
    // console.log('duration', Date.now() - dt0);
    resolve(final);
  });
});

module.exports = etag;
