
-- concat(substr(sttl_dt,0,4),'-',substr(sttl_dt,5,2)) 2020-10 按月分组

insert overwrite table  BDP_DW.TSYH2_result_months partition (dt)
SELECT
concat(substr(datas,0,4),substr(datas,6,2)) as datas, -- 202010
prov_name,
CARRIER_CHN_NM,
cntry_chn_nm,
yuyin_yonghu_xiezhuan_out_c,
yuyin_yonghu_out_count,
yuyin_huadan_xiezhuan_out_c,
yuyin_huadanout_count,
daunx_yonghu_xiezhuan_out_c,
duanx_yonghu_out_count,
daunx_huadan_xiezhuan_out_c,
duanx_huadanout_count,
shuju_yonghu_xiezhuan_out_c,
shuju_yonghu_out_count,
shuju_huadan_xiezhuan_out_c,
shuju_huadanout_count,
datas as dt
from(select
concat(substr(sttl_dt,0,4),'-',substr(sttl_dt,5,2)) as datas, -- 2020-10
prov.prov_name as prov_name,
CARRIER_CHN_NM,
cntry_chn_nm,
count(distinct case when serv_cd in(11,12) and imsi like '460076%'  then IMSI end ) as yuyin_yonghu_xiezhuan_out_c,
count(distinct case when serv_cd in(11,12) then IMSI end ) as yuyin_yonghu_out_count,
count( case when  serv_cd in(11,12) and imsi like '460076%'  then IMSI end ) as yuyin_huadan_xiezhuan_out_c,
count( case when  serv_cd in(11,12) then IMSI end) as yuyin_huadanout_count,

count(distinct case when serv_cd in(21,22) and imsi like '460076%'  then IMSI end ) as daunx_yonghu_xiezhuan_out_c,
count(distinct case when serv_cd in(21,22) then IMSI end ) as duanx_yonghu_out_count,
count( case when  serv_cd in(21,22) and imsi like '460076%'  then IMSI end ) as daunx_huadan_xiezhuan_out_c,
count( case when  serv_cd in(21,22) then IMSI end) as duanx_huadanout_count,

count(distinct case when serv_cd='x' and imsi like '460076%'  then IMSI end ) as shuju_yonghu_xiezhuan_out_c,
count(distinct case when serv_cd='x' then IMSI end ) as shuju_yonghu_out_count,
count( case when  serv_cd='x' and imsi like '460076%'  then IMSI end ) as shuju_huadan_xiezhuan_out_c,
count( case when  serv_cd='x' then IMSI end) as shuju_huadanout_count
from

(select
sttl_dt,
HM_PROV_CD as PROV_ID,
VSTD_CARRIER_CD,   -- 原来的运营商id 
cntry_chn_nm,
serv_cd,
IMSI
from
bdp_dwd.DWD_GSM_IOGSMDBA_CDR_GSM
where sttl_dt >= '20200101' and sttl_dt <= '20200131'
union all

select
sttl_dt,
HM_PROV_CD as PROV_ID,
VSTD_CARRIER_CD,
cntry_chn_nm,
serv_cd,
IMSI
from 
bdp_dwd.DWD_GSM_IODUALDBA_CDR_GSM
where sttl_dt >= '20200101' and sttl_dt <= '20200131'
union all

select
call_start_dt as sttl_dt,
PROV_ID,
PARTNER_CARRIER_CD as VSTD_CARRIER_CD,   -- 运营商
CNTRY_CHN_NM as cntry_chn_nm,
'x',
IMSI
from
bdp_dwd.DWD_GGSN_NRTDBA_GP_CDR
 where call_start_dt >='20200101' and call_start_dt <= '20200131'
)tmp1 join bdp_dw.dim_idd_mi_szn_province prov
on tmp1.prov_id=prov.prov_code
join (select  CARRIER_CD,CARRIER_CHN_NM
from bdp_dw.DIM_GSM_MCBDBA_CARRIERS_BASE
group by CARRIER_CD,CARRIER_CHN_NM)tmp2
on tmp1.VSTD_CARRIER_CD=tmp2.CARRIER_CD
group by concat(substr(sttl_dt,0,4),'-',substr(sttl_dt,5,2)),prov_name,CARRIER_CHN_NM,cntry_chn_nm



-----
union all

select
concat(substr(sttl_dt,0,4),'-',substr(sttl_dt,5,2)) as datas,
prov.prov_name as prov_name,
CARRIER_CHN_NM,
cntry_chn_nm,
count(distinct case when serv_cd in(11,12) and imsi like '460076%'  then IMSI end ) as yuyin_yonghu_xiezhuan_out_c,
count(distinct case when serv_cd in(11,12) then IMSI end ) as yuyin_yonghu_out_count,
count( case when  serv_cd in(11,12) and imsi like '460076%'  then IMSI end ) as yuyin_huadan_xiezhuan_out_c,
count( case when  serv_cd in(11,12) then IMSI end) as yuyin_huadanout_count,

count(distinct case when serv_cd in(21,22) and imsi like '460076%'  then IMSI end ) as daunx_yonghu_xiezhuan_out_c,
count(distinct case when serv_cd in(21,22) then IMSI end ) as duanx_yonghu_out_count,
count( case when  serv_cd in(21,22) and imsi like '460076%'  then IMSI end ) as daunx_huadan_xiezhuan_out_c,
count( case when  serv_cd in(21,22) then IMSI end) as duanx_huadanout_count,

count(distinct case when serv_cd='x' and imsi like '460076%'  then IMSI end ) as shuju_yonghu_xiezhuan_out_c,
count(distinct case when serv_cd='x' then IMSI end ) as shuju_yonghu_out_count,
count( case when  serv_cd='x' and imsi like '460076%'  then IMSI end ) as shuju_huadan_xiezhuan_out_c,
count( case when  serv_cd='x' then IMSI end) as shuju_huadanout_count
from
(select
sttl_dt,
HM_PROV_CD as PROV_ID,
'全部' as CARRIER_CHN_NM,
'全部' as cntry_chn_nm,
serv_cd,
IMSI
from
bdp_dwd.DWD_GSM_IOGSMDBA_CDR_GSM
where sttl_dt >= '20200101' and sttl_dt <= '20200131'
union all

select
sttl_dt,
HM_PROV_CD as PROV_ID,
'全部' as CARRIER_CHN_NM,
'全部' as cntry_chn_nm,
serv_cd,
IMSI
from 
bdp_dwd.DWD_GSM_IODUALDBA_CDR_GSM
 where sttl_dt >= '20200101' and sttl_dt <= '20200131'
union all

select
call_start_dt as sttl_dt,
PROV_ID,
'全部' as CARRIER_CHN_NM,
'全部' as cntry_chn_nm,
'x',
IMSI
from
bdp_dwd.DWD_GGSN_NRTDBA_GP_CDR
where call_start_dt >='20200101' and call_start_dt <= '20200131'
)tmp1 join bdp_dw.dim_idd_mi_szn_province prov
on tmp1.prov_id=prov.prov_code
group by concat(substr(sttl_dt,0,4),'-',substr(sttl_dt,5,2)),prov_name,CARRIER_CHN_NM,cntry_chn_nm

union ALL
-- 运营商全部
select
concat(substr(sttl_dt,0,4),'-',substr(sttl_dt,5,2)) as datas,
prov.prov_name as prov_name,
CARRIER_CHN_NM,
cntry_chn_nm,
count(distinct case when serv_cd in(11,12) and imsi like '460076%'  then IMSI end ) as yuyin_yonghu_xiezhuan_out_c,
count(distinct case when serv_cd in(11,12) then IMSI end ) as yuyin_yonghu_out_count,
count( case when  serv_cd in(11,12) and imsi like '460076%'  then IMSI end ) as yuyin_huadan_xiezhuan_out_c,
count( case when  serv_cd in(11,12) then IMSI end) as yuyin_huadanout_count,

count(distinct case when serv_cd in(21,22) and imsi like '460076%'  then IMSI end ) as daunx_yonghu_xiezhuan_out_c,
count(distinct case when serv_cd in(21,22) then IMSI end ) as duanx_yonghu_out_count,
count( case when  serv_cd in(21,22) and imsi like '460076%'  then IMSI end ) as daunx_huadan_xiezhuan_out_c,
count( case when  serv_cd in(21,22) then IMSI end) as duanx_huadanout_count,

count(distinct case when serv_cd='x' and imsi like '460076%'  then IMSI end ) as shuju_yonghu_xiezhuan_out_c,
count(distinct case when serv_cd='x' then IMSI end ) as shuju_yonghu_out_count,
count( case when  serv_cd='x' and imsi like '460076%'  then IMSI end ) as shuju_huadan_xiezhuan_out_c,
count( case when  serv_cd='x' then IMSI end) as shuju_huadanout_count
from
(select
sttl_dt,
HM_PROV_CD as PROV_ID,
'全部' as CARRIER_CHN_NM,  -- 运营商全部
cntry_chn_nm,
serv_cd,
IMSI
from
bdp_dwd.DWD_GSM_IOGSMDBA_CDR_GSM
where sttl_dt >= '20200101' and sttl_dt <= '20200131'
union all

select
sttl_dt,
HM_PROV_CD as PROV_ID,
'全部' as CARRIER_CHN_NM, 
cntry_chn_nm,
serv_cd,
IMSI
from 
bdp_dwd.DWD_GSM_IODUALDBA_CDR_GSM
where sttl_dt >= '20200101' and sttl_dt <= '20200131'
union all

select
call_start_dt as sttl_dt,
PROV_ID,
'全部' as CARRIER_CHN_NM, 
CNTRY_CHN_NM as cntry_chn_nm,
'x',
IMSI
from
bdp_dwd.DWD_GGSN_NRTDBA_GP_CDR
 where call_start_dt >='20200101' and call_start_dt <= '20200131'
)tmp1 join bdp_dw.dim_idd_mi_szn_province prov
on tmp1.prov_id=prov.prov_code
group by concat(substr(sttl_dt,0,4),'-',substr(sttl_dt,5,2)),prov_name,CARRIER_CHN_NM,cntry_chn_nm
)m