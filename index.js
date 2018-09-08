const sync = require('./s3-sync'),
  flag = x => ~process.argv.indexOf('-' + x);

async function main() {
  if (flag('t1')) await sync('/home/user/0das/1/Alphonsianum/Pious Reflections/', 'das-junk', '');
  if (flag('t2')) await sync('/home/user/0das/1/Alphonsianum/Preparation for Death/', 'das-junk', '');
  if (flag('1')) await sync('/home/user/0das/1/', 'das-1-docs', '');
  if (flag('p')) await sync('/home/user/0das/pdf/', 'das-pdf', '');
  sync.printQueue();
  return true;
}
main();