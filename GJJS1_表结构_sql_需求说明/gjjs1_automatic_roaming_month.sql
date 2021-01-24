--GJJS1_自动漫游_月结算波

with  lai as (  -- 来访业务
select
'来访' as yewu_line,
yewu_type,
prov_cd,
sum,
datas
from
(SELECT
'语音' as yewu_type,
prov_cd,
sum(PROV_GET_CHG_RMB - PROV_GET_SMS_CHG_RMB) as sum,
concat(substr(month,0,4),'-',substr(month,5,2)) as datas
from bdp_dwd.DWD_GSM_IIGSMDBA_STTL_MONTHLY
where month='202011' and BUSINESS_TYPE=0
group by month,prov_cd,concat(substr(month,0,4),'-',substr(month,5,2)) 
union all
select
'数据' as yewu_type,
prov_cd,
SUM(PROV_GET_CHG_RMB) as sum,
concat(substr(month,0,4),'-',substr(month,5,2)) as datas
from bdp_dwd.DWD_GSM_IIGSMDBA_STTL_MONTHLY
where BUSINESS_TYPE=1 and  month='202011'
group by month,prov_cd,concat(substr(month,0,4),'-',substr(month,5,2))
union all
SELECT
'短信' as yewu_type,
prov_cd,
SUM(PROV_GET_SMS_CHG_RMB) as sum,
concat(substr(month,0,4),'-',substr(month,5,2)) as datas
from bdp_dwd.DWD_GSM_IIGSMDBA_STTL_MONTHLY
where month='202011'
group by month,prov_cd,concat(substr(month,0,4),'-',substr(month,5,2))
union all
SELECT
'总金额' as yewu_type,
prov_cd,
SUM(PROV_GET_CHG_RMB) as sum,
concat(substr(month,0,4),'-',substr(month,5,2)) as datas
from bdp_dwd.DWD_GSM_IIGSMDBA_STTL_MONTHLY
where month='202011'
group by month,prov_cd,concat(substr(month,0,4),'-',substr(month,5,2)))tmp1
),

qu as (   -- 出访业务
select
'出访' as yewu_line,
yewu_type,
prov_cd,
sum,
datas
from(
SELECT
'语音' as yewu_type,
prov_cd,
SUM(DOM_PROV_PAY_CHG_RMB-DOM_PROV_PAY_SMS_CHG_RMB) as sum,
concat(substr(month,0,4),'-',substr(month,5,2)) as datas
from bdp_dwd.DWD_GSM_IIGSMDBA_STTL_MONTHLY
where month='202011' and BUSINESS_TYPE=0
group by month,prov_cd,concat(substr(month,0,4),'-',substr(month,5,2)) 
union all
select
'数据' as yewu_type,
cast(gp.prov_id as bigint) as prov_cd,
(gp_lfee+ggsn_lfee) as sum,
gp.datas as datas
from(
select
prov_id,
ROUND(SUM(LFEE) / 100000, 2)  as gp_lfee,
concat(substr(sttl_mn,0,4),'-',substr(sttl_mn,5,2)) as datas
from bdp_dwd.dwd_boss_ioggsndba_gp_pgw_monthly where sttl_mn='202011' 
group by prov_id,concat(substr(sttl_mn,0,4),'-',substr(sttl_mn,5,2)))gp
join
(
select
prov_id,
ROUND(SUM(LFEE), 2) as ggsn_lfee,
concat(substr(sttl_mn,0,4),'-',substr(sttl_mn,5,2)) as datas
from bdp_dwd.dwd_boss_ioggsndba_imsi_ggsn_monthly where sttl_mn='202011' 
group by prov_id,concat(substr(sttl_mn,0,4),'-',substr(sttl_mn,5,2))
)ggsn 
on ggsn.datas=gp.datas and ggsn.prov_id=gp.prov_id     -- 这边省份可能会重复，出问题加上运营商的条件
union all
SELECT
'短信' as yewu_type,
prov_cd,
SUM(DOM_PROV_PAY_SMS_CHG_RMB) as sum,
concat(substr(month,0,4),'-',substr(month,5,2)) as datas
from bdp_dwd.DWD_GSM_IIGSMDBA_STTL_MONTHLY
where month='202011'
group by month,prov_cd,concat(substr(month,0,4),'-',substr(month,5,2)) 
union all
select
'总金额' as yewu_type,
gsm.prov_cd as prov_cd,
(gsm_lfee+gp_lfee+ggsn_lfee) as sum,
gsm.datas as datas
from
(
select
prov_cd,
SUM(DOM_PROV_PAY_CHG_RMB) as gsm_lfee,
concat(substr(month,0,4),'-',substr(month,5,2)) as datas
from bdp_dwd.DWD_GSM_IIGSMDBA_STTL_MONTHLY 
where month='202011' and BUSINESS_TYPE=0 
group by month,prov_cd,concat(substr(month,0,4),'-',substr(month,5,2)) 
)gsm
join
(
select
prov_id,
ROUND(SUM(LFEE) / 100000, 2)  as gp_lfee,
concat(substr(sttl_mn,0,4),'-',substr(sttl_mn,5,2)) as datas
from bdp_dwd.dwd_boss_ioggsndba_gp_pgw_monthly 
where sttl_mn='202011' 
group by prov_id,concat(substr(sttl_mn,0,4),'-',substr(sttl_mn,5,2))
)gp 
on gp.prov_id=gsm.prov_cd and gp.datas=gsm.datas   -- 这边省份可能会重复，出问题加上运营商的条件
join
(
select
prov_id,
SUM(LFEE)  as ggsn_lfee,
concat(substr(sttl_mn,0,4),'-',substr(sttl_mn,5,2)) as datas
from bdp_dwd.dwd_boss_ioggsndba_imsi_ggsn_monthly 
where sttl_mn='202011' 
group by prov_id,concat(substr(sttl_mn,0,4),'-',substr(sttl_mn,5,2))
)ggsn 
on gp.prov_id=ggsn.prov_id and gp.datas=ggsn.datas)tmp2   -- 这边省份可能会重复，出问题加上运营商的条件
)



insert overwrite table  GJJS1_result_months partition (sttl_dt)
select 
a.yewu_line,
a.yewu_type,
a.prov_nm,
from_unixtime(unix_timestamp(a.datas,'yyyy-MM'),'yyyyMM') as datas,
a.sum,
nvl(c.sum,0) as last_years_sum,
nvl(b.sum,0) as last_sum,
a.datas as sttl_dt
from
( --
select
yewu_line,
yewu_type,
prov.prov_cd,
prov.prov_nm,
sum,
datas
from
(
select * from lai
union all
select * from qu)tmp
join 
bdp_dw.DIM_INR_PUBDBA_PROVINCE prov 
on tmp.prov_cd=prov.prov_cd)a
left join(
select * from GJJS1_result_months
)b 
on substr(add_months(concat(a.datas,'-01'),-1),0,7) = b.sttl_dt and  
b.yewu_line=a.yewu_line and 
b.yewu_type=a.yewu_type and b.prov_nm=a.prov_nm
left join(
select * from GJJS1_result_months
)c
on substr(add_months(concat(a.datas,'-01'),-12),0,7) = c.sttl_dt
and  b.yewu_line=a.yewu_line and b.yewu_type=a.yewu_type and b.prov_nm=a.prov_nm