-- date_sub(next_day(sttl_dt,'MO'),7) 下个星期一的时间-7就是这个星期一的时间
-- concat(substr((substr(a.week,0,4) -1),0,4),substr(a.week,5)) = c.week    201932 202032  201932周与202032周

insert overwrite table  BDP_DW.GMLF1_RESULT_WEEKS partition (sttl_dt)
select d.* FROM
(
select 
a.week,a.yewu_line,a.yewu_type,a.prov_nm,a.national_code,a.carrier_name_chs,
from_unixtime(unix_timestamp(a.datas,'yyyy-MM-dd'),'yyyyMMdd') as datas,
a.user_count,
nvl(c.user_count,0) as last_years_user,
nvl(b.user_count,0) as last_user,
a.huadan_count,
nvl(c.huadan_count,0) as last_years_huadan,
nvl(b.huadan_count,0) as last_huadan,
a.yewuliang_count,
nvl(c.yewuliang_count,0) as last_years_yewuliang,
nvl(b.yewuliang_count,0) as last_yewuliang,
a.datas as sttl_dt
from
(
select k.week,k.week1 as datas,k.yewu_line,k.yewu_type,k.prov_nm,k.national_code,k.carrier_name_chs,
sum(k.user_count) as user_count,
sum(k.huadan_count) as huadan_count,
sum(k.yewuliang_count) as yewuliang_count
from
(
select concat(substr(date_sub(next_day(sttl_dt,'MO'),4),0,4),weekofyear(sttl_dt)) as week,
date_sub(next_day(sttl_dt,'MO'),7) as week1,yewu_line,yewu_type,prov_nm,national_code,carrier_name_chs,datas,user_count,huadan_count,yewuliang_count
from 
BDP_DW.GMLF1_result_days where sttl_dt >= '20201001' and  sttl_dt <= '20201231'
) k
group by k.week,k.week1,   -- 按照一个礼拜的数据进行分组
k.yewu_line,k.yewu_type,
k.prov_nm,k.national_code,
k.carrier_name_chs
) a
left join
(select * from  BDP_DW.GMLF1_RESULT_WEEKS ) b
on date_add(a.datas,-7) = b.sttl_dt    -- 上个礼拜的环比数据
and a.yewu_type = b.yewu_type and a.prov_nm = b.prov_nm  and a.national_code = b.national_code and a.carrier_name_chs = b.carrier_name_chs
left join
(select * from  BDP_DW.GMLF1_RESULT_WEEKS ) c
on concat(substr((substr(a.week,0,4) -1),0,4),substr(a.week,5)) = c.week   -- 去年同一个礼拜数据
and a.yewu_type = c.yewu_type and a.prov_nm = c.prov_nm  and a.national_code = c.national_code and a.carrier_name_chs = c.carrier_name_chs
) d
