 mkfifo /mount/to/save/data/files/fifo_file_name
 gzip -c </mount/to/save/data/files/fifo_file_name > /mount/to/save/data/files/file_name.del.gz &
 db2 'connect to dbname'
 db2 "export to /mount/to/save/data/files/fifo_file_name of del select <list of columns>
  FROM myschema.mytable
  where  <condition >
  db2 terminate 
  rm -f  /mount/to/save/data/files/fifo_file_name || true 
  
snowsql -q ' use schema myschemaINSF;
  begin;
  put file:///mount/to/save/data/files/file_name.del.gz @SF_STAGE auto_compress=false;
  truncate table
if exists myschemaINSF.mytable;
  copy into myschemaINSF.mytable 
from @SF_STAGE/file_name.del.gz;
  show transactions;
 Commit;
 ' -c SF_profile_NAME -o echo=true -o output_file=/tmp/fifo_file_name.out

if [ $? -eq 0 ];
  echo "errors"
fi
rm -vf /mount/to/save/data/files/file_name.del.gz || true

 
