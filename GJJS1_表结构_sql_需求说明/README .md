





# GJJS1：月结算波动分析：自动漫游/一卡多号/漫游伴侣

l    业务简介

国际漫游出访：国外运营商将国移动用户话单文件传输给国际业务运营心的计费系统，由国际业务运营心的计费系统进行分拣、校验、批价再下发给各省公司，省公司根据收到的国内口话单和用户进行收费。
 国际漫游来访：省公司采集国外用户话单，以国内口话单形式上发国际业务运营心的计费系统，由国际业务运营心的计费系统进行处理，并分发至各国外运营商。

结算数据源：来访，省上发语音短信和流量话单；出访，IT心下发语音短信和流量话单。

l    统计口径

下述涉及月结算波动分析业务统计口径，主要包括：

 

|              |                                                              |                                                              |
| ------------ | ------------------------------------------------------------ | ------------------------------------------------------------ |
| **结算金额** | **来访**                                                     | **出访（不含税）**                                           |
| 语音         | DWD_GSM_IIGSMDBA_STTL_MONTHLY表，BUSINESS_TYPE=0,SUM(T.PROV_GET_CHG_RMB) - SUM(T.PROV_GET_SMS_CHG_RMB) | DWD_GSM_IIGSMDBA_STTL_MONTHLY表，BUSINESS_TYPE=0,SUM(DOM_PROV_PAY_CHG_RMB)-SUM(DOM_PROV_PAY_SMS_CHG_RMB) |
| 数据         | DWD_GSM_IIGSMDBA_STTL_MONTHLY表，BUSINESS_TYPE=1,SUM(T.PROV_GET_CHG_RMB) | IOGGSNDBA.GP_PGW_MONTHLY表，ROUND(SUM(LFEE) /   100000, 2)+IOGGSNDBA.IMSI_GGSN_MONTHLY表SUM(LFEE) |
| 短信         | DWD_GSM_IIGSMDBA_STTL_MONTHLY表，SUM(T.PROV_GET_SMS_CHG_RMB) | DWD_GSM_IIGSMDBA_STTL_MONTHLY表，SUM(DOM_PROV_PAY_SMS_CHG_RMB) |
| 总金额       | DWD_GSM_IIGSMDBA_STTL_MONTHLY表，SUM(T.PROV_GET_CHG_RMB)     | DWD_GSM_IIGSMDBA_STTL_MONTHLY表，BUSINESS_TYPE=0,SUM(DOM_PROV_PAY_CHG_RMB+IOGGSNDBA.GP_PGW_MONTHLY表ROUND(SUM(LFEE) / 100000, 2)+IOGGSNDBA.IMSI_GGSN_MONTHLY表SUM(LFEE) |
| **一卡多号** |                                                              |                                                              |
| 语音         | DWD_GSM_IIDUALDBA_STTL_MONTHLY表，BIZ_PKG=8,SUM(T.PROV_GET_CHG_RMB)-SUM(T.PROV_GET_SMS_CHG_RMB) | DWD_GSM_IIDUALDBA_STTL_MONTHLY表，BIZ_PKG=8,SUM(DOM_PROV_PAY_CHG_RMB)-SUM(DOM_PROV_PAY_SMS_CHG_RMB) |
| 数据         | 无                                                           | 无                                                           |
| 短信         | DWD_GSM_IIDUALDBA_STTL_MONTHLY表，BIZ_PKG=8,SUM(T.PROV_GET_SMS_CHG_RMB) | DWD_GSM_IIDUALDBA_STTL_MONTHLY表，BIZ_PKG=8,SUM(DOM_PROV_PAY_SMS_CHG_RMB) |
| 总金额       | DWD_GSM_IIDUALDBA_STTL_MONTHLY表，BIZ_PKG=8,SUM(T.PROV_GET_CHG_RMB) | DWD_GSM_IIDUALDBA_STTL_MONTHLY表，BIZ_PKG=8,SUM(DOM_PROV_PAY_CHG_RMB) |
| **漫游伴侣** |                                                              |                                                              |
| 语音         | DWD_GSM_IIDUALDBA_STTL_MONTHLY表，BIZ_PKG=10,SUM(T.PROV_GET_CHG_RMB)-SUM(T.PROV_GET_SMS_CHG_RMB) | DWD_GSM_IIDUALDBA_STTL_MONTHLY表，BIZ_PKG=10,SUM(DOM_PROV_PAY_CHG_RMB)-SUM(DOM_PROV_PAY_SMS_CHG_RMB) |
| 数据         | 无                                                           | 无                                                           |
| 短信         | DWD_GSM_IIDUALDBA_STTL_MONTHLY表，BIZ_PKG=10,SUM(T.PROV_GET_SMS_CHG_RMB) | DWD_GSM_IIDUALDBA_STTL_MONTHLY表，BIZ_PKG=10,SUM(DOM_PROV_PAY_SMS_CHG_RMB) |
| 总金额       | DWD_GSM_IIDUALDBA_STTL_MONTHLY表，BIZ_PKG=10,SUM(T.PROV_GET_CHG_RMB) | DWD_GSM_IIDUALDBA_STTL_MONTHLY表，BIZ_PKG=10,SUM(DOM_PROV_PAY_CHG_RMB) |

  

##  需求分析：

总的：需要求出环比、和同比的  数据

hive 和mysql 不建立最后的结果百分比，hive表给出 本期、上期、去年同期的数据，数据同步到mysql 取数   x/x

hive的表设计，sum 本期的金额，是用的中间表就是本期要求的金额，left join 结果表，用nvl来取上期和去年同期，在第一次的时候可以把nvl的数据全补零把每个时间的金额sum补齐，最后在选择一个时间点开始取上期、去年同期的数据

a.sum,
nvl(c.sum,0) as last_years_sum,
nvl(b.sum,0) as last_sum,

on substr(add_months(concat(a.datas,'-01'),-1),0,7) = b.sttl_dt 

on substr(add_months(concat(a.datas,'-01'),-12),0,7) = c.sttl_dt

自动漫游/一卡多号/漫游伴侣 

实现一

三个的业务条件不一样所以，这边大的方向是分开求，每个业务建一个表

每个业务分别有来访和出访，这边用 with  xx as(),来分别求来访和出访然后union all 

想法二、

三个业务如果为了省略存储空间，可以建一张总的宽表，把三个表建成临时表分别打上各自业务的类型作为区分type(自动漫游/一卡多号/漫游伴侣 ),然后表三个的临时表给合并起来， 最后插入到总的宽表中，最后可以删除上面的临时表的数据

| 程序名称                              | hive结果表                    |
| ------------------------------------- | ----------------------------- |
| gjjs1_automatic_roaming_month（自动） | GJJS1_result_months           |
| GJJS1_roaming_p_result_months（一卡） | GJJS1_roaming_p_result_months |
| GJJS1_cardm_num_result_months（漫游） | GJJS1_cardm_num_result_months |

## 问题

有的数据类型比如数据、总金额，要求的是两个表的金额的和需要两个表join 然后把数据金额相加；

这边可能会出现关联的条件不是唯一的值，导致数据的重复

## 需求展示

下述以自动漫游为例，一卡多号和漫游伴侣相同，但无数据金额。

l  查询条件

时间：YYYYMM-YYYYMM

省代码：NNN

漫游方向：来访、出访

​       业务类型：语音、短信、数据、全部

​       波动分析类型：结算金额，结算金额同比，结算金额环比

​      展示类型：表格，图形