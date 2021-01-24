-- GJJS1_漫游伴侣_月结算

with  lai as (
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
SUM(PROV_GET_CHG_RMB)-SUM(PROV_GET_SMS_CHG_RMB) as sum,
concat(substr(month,0,4),'-',substr(month,5,2)) as datas
from bdp_dwd.DWD_GSM_IIDUALDBA_STTL_MONTHLY
where month='202011' and BIZ_PKG=10
group by month,prov_cd,concat(substr(month,0,4),'-',substr(month,5,2)) 
union all
SELECT
'短信' as yewu_type,
prov_cd,
SUM(PROV_GET_SMS_CHG_RMB) as sum,
concat(substr(month,0,4),'-',substr(month,5,2)) as datas
from bdp_dwd.DWD_GSM_IIDUALDBA_STTL_MONTHLY
where month='202011' and BIZ_PKG=10
group by month,prov_cd,concat(substr(month,0,4),'-',substr(month,5,2)) 
union all
SELECT
'总金额' as yewu_type,
prov_cd,
SUM(PROV_GET_CHG_RMB) as sum,
concat(substr(month,0,4),'-',substr(month,5,2)) as datas
from bdp_dwd.DWD_GSM_IIDUALDBA_STTL_MONTHLY
where month='202011' and BIZ_PKG=10
group by month,prov_cd,concat(substr(month,0,4),'-',substr(month,5,2))
)tmp1
),
qu as (
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
SUM(DOM_PROV_PAY_CHG_RMB)-SUM(DOM_PROV_PAY_SMS_CHG_RMB) as sum,
concat(substr(month,0,4),'-',substr(month,5,2)) as datas
from bdp_dwd.DWD_GSM_IIDUALDBA_STTL_MONTHLY
where month='202011' and BIZ_PKG=10
group by month,prov_cd,concat(substr(month,0,4),'-',substr(month,5,2)) 
union all
SELECT
'短信' as yewu_type,
prov_cd,
SUM(DOM_PROV_PAY_SMS_CHG_RMB) as sum,
concat(substr(month,0,4),'-',substr(month,5,2)) as datas
from bdp_dwd.DWD_GSM_IIDUALDBA_STTL_MONTHLY
where month='202011' and BIZ_PKG=10
group by month,prov_cd,concat(substr(month,0,4),'-',substr(month,5,2)) 
union all
SELECT
'总金额' as yewu_type,
prov_cd,
SUM(DOM_PROV_PAY_CHG_RMB) as sum,
concat(substr(month,0,4),'-',substr(month,5,2)) as datas
from bdp_dwd.DWD_GSM_IIDUALDBA_STTL_MONTHLY
where month='202011' and BIZ_PKG=10
group by month,prov_cd,concat(substr(month,0,4),'-',substr(month,5,2))
)tmp2
)



insert overwrite table  GJJS1_roaming_p_result_months partition (sttl_dt)
select 
a.yewu_line,
a.yewu_type,
a.prov_nm,
from_unixtime(unix_timestamp(a.datas,'yyyy-MM'),'yyyyMM') as datas,
a.sum,
nvl(c.sum,0) as last_years_sum,
nvl(b.sum,0) as last_sum,
a.datas as sttl_dt
from( --
select
yewu_line,
yewu_type,
prov.prov_cd,
prov.prov_nm,
sum,
datas
from(
select * from lai
union all
select * from qu)tmp
join 
bdp_dw.DIM_INR_PUBDBA_PROVINCE prov on tmp.prov_cd=prov.prov_cd)a
left join(
select * from GJJS1_roaming_p_result_months
)b 
on substr(add_months(concat(a.datas,'-01'),-1),0,7) = b.sttl_dt 
and  b.yewu_line=a.yewu_line and b.yewu_type=a.yewu_type and b.prov_nm=a.prov_nm
left join(
select * from GJJS1_roaming_p_result_months
)c
on substr(add_months(concat(a.datas,'-01'),-12),0,7) = c.sttl_dt
and  b.yewu_line=a.yewu_line and b.yewu_type=a.yewu_type and b.prov_nm=a.prov_nm