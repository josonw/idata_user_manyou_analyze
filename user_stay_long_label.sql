insert overwrite table  bdp_dw.user_stay_long_label
-- 110：
select
* 
from(select
msisdn,
national_code,
case when 
national_code in('中国香港','中国澳门','中国台湾') and  count(msisdn)=1   -- 总出行次数
and sum(stay_long)/count(msisdn)>=1 and sum(stay_long)/count(msisdn)<=5 then '110' 
when
count(msisdn)=1 and  sum(stay_long)/count(msisdn)>=1 and sum(stay_long)/count(msisdn)<=5 then '110'
when 
count(msisdn)=1 and  sum(stay_long)/count(msisdn)>=6 and sum(stay_long)/count(msisdn)<=15  then '110' end as label,
dw_dat_dt
from bdp_dwa.DWA_IMSI_TIME_CONSUMPTION_MONTHLY
where dw_dat_dt='202006' and msisdn is not null
group by 
dw_dat_dt,
national_code,  -- 国家地区
msisdn )a where  label is not null


union all
-- 低频短期主/被动型动型
select
* 
from(select
msisdn,
national_code,
case when 
national_code in('中国香港','中国澳门','中国台湾') and  count(msisdn)=2   -- 总出行次数
and sum(voice_mo)>0.0 and sum(sms_mo)>0 and sum(data_mb)>1.0
and sum(stay_long)/count(msisdn)>=1 and sum(stay_long)/count(msisdn)<=5 then '111' 
when
national_code in('中国香港','中国澳门','中国台湾') and  count(msisdn)=2   -- 总出行次数
and sum(voice_mo)=0.0 and sum(sms_mo)=0 and sum(data_mb)<1.0
and sum(stay_long)/count(msisdn)>=1 and sum(stay_long)/count(msisdn)<=5 then '112' 
when 
count(msisdn)=2
and sum(voice_mo)>0.0 and sum(sms_mo)>0 and sum(data_mb)>1.0
and  sum(stay_long)/count(msisdn)>=1 and sum(stay_long)/count(msisdn)<=5 then '111'
when 
count(msisdn)=2 
and sum(voice_mo)=0.0 and sum(sms_mo)=0 and sum(data_mb)<1.0
and  sum(stay_long)/count(msisdn)>=6 and sum(stay_long)/count(msisdn)<=15  then '112' end as label,
dw_dat_dt
from bdp_dwa.DWA_IMSI_TIME_CONSUMPTION_MONTHLY
where dw_dat_dt='202006' and msisdn is not null
group by 
dw_dat_dt,
national_code,  -- 国家地区
msisdn )a where  label is not null

-- 210
union all

select
* 
from(select
msisdn,
national_code,
case when 
national_code in('中国香港','中国澳门','中国台湾') and  count(msisdn)>=3 and count(msisdn)<=5   -- 总出行次数
and sum(stay_long)/count(msisdn)>=1 and sum(stay_long)/count(msisdn)<=5 then '210' 
when
count(msisdn)>=3 and count(msisdn)<=5  
and  sum(stay_long)/count(msisdn)>=1 and sum(stay_long)/count(msisdn)<=5 then '210'
when 
count(msisdn)>=3 and count(msisdn)<=5  
and  sum(stay_long)/count(msisdn)>=6 and sum(stay_long)/count(msisdn)<=15  then '210' end as label,
dw_dat_dt
from bdp_dwa.DWA_IMSI_TIME_CONSUMPTION_MONTHLY
where dw_dat_dt='202006' and msisdn is not null
group by 
dw_dat_dt,
national_code,  -- 国家地区
msisdn )a where  label is not null

-- 频繁往返

union all
select
* 
from(select
msisdn,
national_code,
case when 
national_code in('中国香港','中国澳门','中国台湾') and  count(msisdn)>=6   -- 总出行次数
and sum(stay_long)/count(msisdn)>=1 and sum(stay_long)/count(msisdn)<=5 then '320' 
when
national_code in('中国香港','中国澳门','中国台湾') and  count(msisdn)>=6   -- 总出行次数
and sum(stay_long)/count(msisdn)>=6 and sum(stay_long)/count(msisdn)<=15 then '320'
when
national_code in('中国香港','中国澳门','中国台湾') and  count(msisdn)>=6   -- 总出行次数
and sum(stay_long)/count(msisdn)>=16 and sum(stay_long)/count(msisdn)<=31 then '320' 
when --
count(msisdn)>=6
and sum(stay_long)/count(msisdn)>=1 and sum(stay_long)/count(msisdn)<=5 then '320' 
when 
count(msisdn)>=6
and sum(stay_long)/count(msisdn)>=6 and sum(stay_long)/count(msisdn)<=15 then '320' 
when 
count(msisdn)>=6
and sum(stay_long)/count(msisdn)>=16 and sum(stay_long)/count(msisdn)<=31 then '320' 
when
count(msisdn)>=6
and  sum(stay_long)/count(msisdn)>=32 and sum(stay_long)/count(msisdn)<=46  then '320' end as label,
dw_dat_dt
from bdp_dwa.DWA_IMSI_TIME_CONSUMPTION_MONTHLY
where dw_dat_dt='202006' and msisdn is not null
group by 
dw_dat_dt,
national_code,  -- 国家地区
msisdn )a where  label is not null

-- 220
union all

select
* 
from(select
msisdn,
national_code,
case when 
national_code in('中国香港','中国澳门','中国台湾') and  count(msisdn)=1  -- 总出行次数
and sum(stay_long)/count(msisdn)>=6 and sum(stay_long)/count(msisdn)<=15 then '220' 
when
national_code in('中国香港','中国澳门','中国台湾') and  count(msisdn)=1
and  sum(stay_long)/count(msisdn)>=16 and sum(stay_long)/count(msisdn)<=31 then '220'
when
national_code in('中国香港','中国澳门','中国台湾') and  count(msisdn)=2  -- 总出行次数
and sum(stay_long)/count(msisdn)>=6 and sum(stay_long)/count(msisdn)<=15 then '220' 
when
national_code in('中国香港','中国澳门','中国台湾') and  count(msisdn)=2
and  sum(stay_long)/count(msisdn)>=16 and sum(stay_long)/count(msisdn)<=31 then '220'
when
national_code in('中国香港','中国澳门','中国台湾') and  count(msisdn)>=3 and count(msisdn)<=5  -- 总出行次数
and sum(stay_long)/count(msisdn)>=6 and sum(stay_long)/count(msisdn)<=15 then '220' 
when
national_code in('中国香港','中国澳门','中国台湾') and  count(msisdn)>=3 and count(msisdn)<=5
and  sum(stay_long)/count(msisdn)>=16 and sum(stay_long)/count(msisdn)<=31 then '220'
when --
count(msisdn)=1  -- 总出行次数
and sum(stay_long)/count(msisdn)>=16 and sum(stay_long)/count(msisdn)<=31 then '220' 
when
count(msisdn)=1
and  sum(stay_long)/count(msisdn)>=32 and sum(stay_long)/count(msisdn)<=46 then '220'
when
count(msisdn)=2  -- 总出行次数
and sum(stay_long)/count(msisdn)>=16 and sum(stay_long)/count(msisdn)<=31 then '220' 
when
count(msisdn)=2
and  sum(stay_long)/count(msisdn)>=32 and sum(stay_long)/count(msisdn)<=46 then '220'
when
count(msisdn)>=3 and count(msisdn)<=5
and  sum(stay_long)/count(msisdn)>=16 and sum(stay_long)/count(msisdn)<=31 then '220'
when 
count(msisdn)>=3 and count(msisdn)<=5  
and  sum(stay_long)/count(msisdn)>=32 and sum(stay_long)/count(msisdn)<=46  then '220' end as label,
dw_dat_dt
from bdp_dwa.DWA_IMSI_TIME_CONSUMPTION_MONTHLY
where dw_dat_dt='202006' and msisdn is not null
group by 
dw_dat_dt,
national_code,  -- 国家地区
msisdn )a where  label is not null

union all
-- 030

select
* 
from(select
msisdn,
national_code,
case when 
national_code in('中国香港','中国澳门','中国台湾') and  count(msisdn)=1   -- 总出行次数
and sum(stay_long)/count(msisdn)>=31  then '030' 
when
national_code in('中国香港','中国澳门','中国台湾') and  count(msisdn)=2   -- 总出行次数
and sum(stay_long)/count(msisdn)>=31  then '030' 
when
national_code in('中国香港','中国澳门','中国台湾') and  count(msisdn)>=3 and count(msisdn)<=5   -- 总出行次数
and sum(stay_long)/count(msisdn)>=31  then '030' 
when
national_code in('中国香港','中国澳门','中国台湾') and  count(msisdn)>=6  -- 总出行次数
and sum(stay_long)/count(msisdn)>=31  then '030' 
when --
count(msisdn)=1   -- 总出行次数
and sum(stay_long)/count(msisdn)>=47  then '030' 
when 
count(msisdn)=2   -- 总出行次数
and sum(stay_long)/count(msisdn)>=47  then '030'
when 
count(msisdn)>=3 and count(msisdn)<=5  -- 总出行次数
and sum(stay_long)/count(msisdn)>=47  then '030'
when
count(msisdn)>=6
and sum(stay_long)/count(msisdn)>=47 then '030' end as label,
dw_dat_dt
from bdp_dwa.DWA_IMSI_TIME_CONSUMPTION_MONTHLY
where dw_dat_dt='202006' and msisdn is not null
group by 
dw_dat_dt,
national_code,  -- 国家地区
msisdn )a where  label is not null