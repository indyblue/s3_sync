const s3 = require('./s3-sync'),
  flag = x => ~process.argv.indexOf('-' + x);

async function main() {
  s3.consoleEmitters();
  let fn = () => console.log('choose action: u/d/s');
  if (flag('u')) fn = s3.sync;
  else if (flag('d')) fn = s3.download;
  else if (flag('s')) fn = s3.status;
  else { fn(); return; }
  if (flag('t1')) await fn('/home/user/0das/1/Alphonsianum/Pious Reflections/', 'das-junk', '');
  if (flag('t2')) await fn('/home/user/0das/1/Alphonsianum/Preparation for Death/', 'das-junk', '');
  if (flag('t3')) await fn('/home/user/0das/pdf/Latin/Gildersleeve/', 'das-junk', '');
  if (flag('t4')) await fn('/home/user/0das/pdf/Latin/dictionaries/', 'das-junk', '');
  if (flag('1')) await fn('/home/user/0das/1/', 'das-1-docs', '');
  if (flag('p')) await fn('/home/user/0das/pdf/', 'das-pdf', '');
  if (flag('w')) await fn('/home/user/www/', 'das-www', '');
  s3.printQueue();
  return true;
}
main();