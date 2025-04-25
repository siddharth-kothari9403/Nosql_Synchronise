mongosh mongodb://myuser:mypassword@localhost:27017
use student_course_grades;
db.grades.find({student_id: 'SID1132'});


psql -U myuser -d postgres
\c student_course_grades;
select * from grades where student_id='SID1310';


beeline -u 'jdbc:hive2://localhost:10000/'
SET hive.support.concurrency = true;
SET hive.txn.manager = org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
SET hive.enforce.bucketing = true;
SET hive.exec.dynamic.partition.mode = nonstrict;
SET hive.create.as.acid=true;
SET hive.compactor.worker.threads=1;
SET hive.compactor.initiator.on=true;
select * from grades where student_id="SID1033";