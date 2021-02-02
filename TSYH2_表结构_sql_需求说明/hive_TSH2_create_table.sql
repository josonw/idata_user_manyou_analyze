create table IF NOT EXISTS TSYH2_RESULT_MONTHS(
  DATAS string,
  PROV_NAME string,
  VSTD_CARRIER_ID string,
  CNTRY_CHN_NM string,
  YUYIN_YONGHU_XIEZHUAN_OUT_C string,
  YUYIN_YONGHU_OUT_COUNT string,
  YUYIN_HUADAN_XIEZHUAN_OUT_C string,
  YUYIN_HUADANOUT_COUNT string,
  DAUNX_YONGHU_XIEZHUAN_OUT_C string,
  DUANX_YONGHU_OUT_COUNT string,
  DAUNX_HUADAN_XIEZHUAN_OUT_C string,
  DUANX_HUADANOUT_COUNT string,
  SHUJU_YONGHU_XIEZHUAN_OUT_C string,
  SHUJU_YONGHU_OUT_COUNT string,
  SHUJU_HUADAN_XIEZHUAN_OUT_C string,
  SHUJU_HUADANOUT_COUNT string
) comment 'created by dacp create table function' partitioned by (DT string) row format delimited fields terminated by '' stored as ORCFILE