--truncate temp table 
truncate opfl_temp_db.temp_proteam_flt_history;
-- truncate table 
truncate opfl_appl_db.proteam_flt;
--Update s3 to _history table copy query
copy opfl_temp_db.temp_proteam_flt_history from's3://cbs-udh-datalake-stg-s3-staging/network_operations/uflifo/historical/proteam_flt_historical/' 
IAM_ROLE 'arn:aws:iam::033187372905:role/cbs-RedShift-ServiceLinked-Role' format as parquet COMPUPDATE OFF;

--Update _history to Target table Insert query

insert into opfl_appl_db.proteam_flt
SELECT 
cast(notif_dtm as TIMESTAMP),
carrier_cd,
flt_txt,
flt_num,
cast(sch_dprt_dtl as DATE),
orig_arpt_cd,
dest_arpt_cd,
dvrt_txt,
cnxl_ind,
fst_notif,
cast(proc_compl_dtm as TIMESTAMP),
agt_compl,
cust_cntct_cd,
pax_cnt,
mp_cnt,
compl_cd,
acars_txt,
rsn_cd,
rsn_txt,
email_cd,
ez_cd,
stat_cd,
ready_ind,
flt_mnfst_ind,
wx_ind,
notes_txt,
cast(create_dtm as TIMESTAMP),
create_by,
cast(update_dtm as TIMESTAMP),
update_by,
cast(load_dtm as TIMESTAMP) 
from opfl_temp_db.temp_proteam_flt_history;