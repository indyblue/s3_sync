#aws cli method
cd /home/user/0das/1;
aws s3 sync . s3://das-1-docs/ --exclude 'data' --delete --dryrun

#mount/unmount via ssh (sshfs)
sshfs user@asus:. asus
sudo umount asus