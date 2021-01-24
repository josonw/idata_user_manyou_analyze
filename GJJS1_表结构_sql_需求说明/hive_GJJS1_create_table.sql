create table IF NOT EXISTS GJJS1_RESULT_MONTHS(
  YEWU_LINE string,
  YEWU_TYPE string,
  PROV_NM string,
  DATAS string,
  SUM decimal (21, 2),
  LAST_YEARS_SUM decimal (21, 2),
  LAST_MONTH_SUM decimal (21, 2)
) comment 'created by dacp create table function' partitioned by (STTL_DT string) 
row format delimited fields terminated by '' stored as ORCFILE