insert overwrite table  BDP_DW.GMLF1_RESULT_DAYS partition (sttl_dt)
select d.* FROM
(
select 
'国际来访' as yewu_line,a.yewu_type,a.prov_nm,a.national_code,a.carrier_name_chs,from_unixtime(unix_timestamp(a.datas,'yyyy-MM-dd'),'yyyyMMdd') as datas,a.user_count,
nvl(c.user_count,0) as last_years_user,nvl(b.user_count,0) as last_user,
a.huadan_count,nvl(c.huadan_count,0) as last_years_huadan,nvl(b.huadan_count,0) as last_huadan,
a.yewuliang_count,nvl(c.yewuliang_count,0) as last_years_yewuliang,nvl(b.yewuliang_count,0) as last_yewuliang,
a.datas as sttl_dt
from  -- 正常
(
select 
   yewu_type,
   prov_nm,
   national_code,
   carrier_name_chs,
   concat(substr(sttl_dt,0,4),'-',substr(sttl_dt,5,2),'-',substr(sttl_dt,7,2)) as datas,
   count(distinct imsi) as user_count,--累计用户数
   count(imsi) as huadan_count, ---话单量
   (case when yewu_type = '语音' then round(sum(call_duration/(60*60)),2)
   when yewu_type = '数据' then round(sum((data_up+data_down)/(1024*1024*1024)),2)
   else count(imsi) end
   ) as yewuliang_count
   from bdp_ods.dwd_guoman_come where sttl_dt >= '20201001' and  sttl_dt <= '20201231'
   group by yewu_type,prov_nm,national_code,carrier_name_chs,concat(substr(sttl_dt,0,4),'-',substr(sttl_dt,5,2),'-',substr(sttl_dt,7,2))
   union all
    select 
   '全部' as yewu_type,
   prov_nm,
   national_code,
   carrier_name_chs,
   concat(substr(sttl_dt,0,4),'-',substr(sttl_dt,5,2),'-',substr(sttl_dt,7,2)) as datas,
   count(distinct imsi) as user_count,
   sum(no_of_calls) as huadan_count,
   cast(0 as double) as yewuliang_count
   from bdp_ods.dwd_guoman_come where sttl_dt >= '20201001' and  sttl_dt <= '20201231'
   group by prov_nm,national_code,carrier_name_chs,concat(substr(sttl_dt,0,4),'-',substr(sttl_dt,5,2),'-',substr(sttl_dt,7,2))
   union all
    select 
   yewu_type,
   prov_nm,
   national_code,
   '全部' as carrier_name_chs,
   concat(substr(sttl_dt,0,4),'-',substr(sttl_dt,5,2),'-',substr(sttl_dt,7,2)) as datas,
   count(distinct imsi) as user_count,
   sum(no_of_calls) as huadan_count,
   cast(0 as double) as yewuliang_count
   from bdp_ods.dwd_guoman_come where  sttl_dt >= '20201001' and  sttl_dt <= '20201231'
   group by prov_nm,national_code,yewu_type,concat(substr(sttl_dt,0,4),'-',substr(sttl_dt,5,2),'-',substr(sttl_dt,7,2))
    union all
    select 
   yewu_type,
   prov_nm,
   '全部' as national_code,
   carrier_name_chs,
   concat(substr(sttl_dt,0,4),'-',substr(sttl_dt,5,2),'-',substr(sttl_dt,7,2)) as datas,
   count(distinct imsi) as user_count,
   sum(no_of_calls) as huadan_count,
   cast(0 as double) as yewuliang_count
   from bdp_ods.dwd_guoman_come where sttl_dt >= '20201001' and  sttl_dt <= '20201231'
   group by prov_nm,yewu_type,carrier_name_chs,concat(substr(sttl_dt,0,4),'-',substr(sttl_dt,5,2),'-',substr(sttl_dt,7,2))
      ) a
left join
(select * from  BDP_DW.GMLF1_RESULT_DAYS ) b
on date_add(a.datas,-1) = b.sttl_dt 
and a.yewu_type = b.yewu_type and a.prov_nm = b.prov_nm and a.national_code = b.national_code and a.carrier_name_chs = b.carrier_name_chs
left join
(select * from  BDP_DW.GMLF1_RESULT_DAYS) c
on concat((year(a.datas) - 1),substr(a.datas,5,6)) = c.sttl_dt
and a.yewu_type = c.yewu_type and a.prov_nm = c.prov_nm and a.national_code = c.national_code and a.carrier_name_chs = c.carrier_name_chs
) d