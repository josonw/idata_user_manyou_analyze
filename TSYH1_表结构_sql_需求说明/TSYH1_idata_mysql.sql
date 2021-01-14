-- TSYH1_result_months_all1 一级渠道
select
datas,prov_name,product_name,product_type,channel,
sum(jihuo_count) as jihuo_count,
sum(dinggou_count) as dinggou_count,
sum(tuiding_count) as tuiding_count,
sum(yonghu_count) as yonghu_count
from(select
datas,prov_name,product_name,product_type,
ifnull(case when substring(channel,-1)=3 then channel when substring(channel,-1)=2 then channel 
when substring(channel,-1)=1 then channel end,'全部') as channel,
jihuo_count,
dinggou_count,
tuiding_count,
yonghu_count
from
(
select
datas,
prov_name,
product_name,
product_type,
yiji_channel  as channel,
jihuo_count,
dinggou_count,
tuiding_count,
yonghu_count
from $pagevar(table_str,time_type,TSYH1_result_months_all1) where
prov_name:prov_nm and
$pagevar(product_type_filter,product_type,1=1)  and
$pagevar(product_name_filter,product_name,1=1) and
$pagevar(yiji_channel_filter,yiji_channel,1=2) and
datas:start_time and
datas:end_time

union all

select
datas,
prov_name,
product_name,
product_type,
erji_channel as channel,
jihuo_count,
dinggou_count,
tuiding_count,
yonghu_count
from $pagevar(table_str,time_type,TSYH1_result_months_all1) where
prov_name:prov_nm and
$pagevar(product_type_filter,product_type,1=1)  and
$pagevar(product_name_filter,product_name,1=1) and
$pagevar(erji_channel_filter,erji_channel,1=2) and
datas:start_time and
datas:end_time

union all

select
datas,
prov_name,
product_name,
product_type,
sanji_channel as channel,
jihuo_count,
dinggou_count,
tuiding_count,
yonghu_count
from $pagevar(table_str,time_type,TSYH1_result_months_all1) where
prov_name:prov_nm and
$pagevar(product_type_filter,product_type,1=1)  and
$pagevar(product_name_filter,product_name,1=1) and
$pagevar(sanji_channel_filter,sanji_channel,1=2) and
datas:start_time and
datas:end_time order by substring(channel,-1) desc)m
group by datas,prov_name,product_name,product_type,
jihuo_count,
dinggou_count,
tuiding_count,
yonghu_count)mm
group by 
datas,prov_name,product_name,product_type,channel
