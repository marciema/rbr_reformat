DROP TABLE IF EXISTS app_risk.app_risk.cb_arrival01_gpv_fpt_loading_gpvband;
DROP TABLE IF EXISTS app_risk.app_risk.chargeback_arrival_quarterly_loading_gpvband;

CREATE OR REPLACE TABLE app_risk.app_risk.cb_arrival01_gpv_fpt_loading_gpvband AS 
with
cb as
(select 
 cb.user_token
,cb.payment_token
,date_trunc(month, cb.payment_created_at) as month
,DATE_TRUNC(QUARTER, cb.payment_created_at) AS quarter
,cb.currency_code
,DATEDIFF(DAY, LAST_DAY(payment_created_at, MONTH), chargeback_date::DATE) AS days_since_month_end
,CASE WHEN DATEDIFF(DAY, LAST_DAY(payment_created_at, MONTH), chargeback_date::DATE) <= 15 THEN '<=15'
  WHEN DATEDIFF(DAY, LAST_DAY(payment_created_at, MONTH), chargeback_date::DATE) <= 30 THEN '15-30'
  WHEN DATEDIFF(DAY, LAST_DAY(payment_created_at, MONTH), chargeback_date::DATE) <= 60 THEN '30-60'
  WHEN DATEDIFF(DAY, LAST_DAY(payment_created_at, MONTH), chargeback_date::DATE) <= 90 THEN '60-90'
  WHEN DATEDIFF(DAY, LAST_DAY(payment_created_at, MONTH), chargeback_date::DATE) <= 180 THEN '90-180'
  ELSE '180+' END AS days_group_since_month_end
 ,CASE WHEN DATEDIFF(DAY, LAST_DAY(payment_created_at, MONTH), chargeback_date::DATE) <= 15 THEN 1
  WHEN DATEDIFF(DAY, LAST_DAY(payment_created_at, MONTH), chargeback_date::DATE) <= 30 THEN 2
  WHEN DATEDIFF(DAY, LAST_DAY(payment_created_at, MONTH), chargeback_date::DATE) <= 60 THEN 3
  WHEN DATEDIFF(DAY, LAST_DAY(payment_created_at, MONTH), chargeback_date::DATE) <= 90 THEN 4
  WHEN DATEDIFF(DAY, LAST_DAY(payment_created_at, MONTH), chargeback_date::DATE) <= 180 THEN 5
  ELSE 6 END AS days_group_cd_since_month_end
,DATEDIFF(DAY, LAST_DAY(payment_created_at, QUARTER), chargeback_date::DATE) AS days_since_quarter_end
,CASE WHEN DATEDIFF(DAY, LAST_DAY(payment_created_at, QUARTER), chargeback_date::DATE) <= 15 THEN '<=15'
  WHEN DATEDIFF(DAY, LAST_DAY(payment_created_at, QUARTER), chargeback_date::DATE) <= 30 THEN '15-30'
  WHEN DATEDIFF(DAY, LAST_DAY(payment_created_at, QUARTER), chargeback_date::DATE) <= 60 THEN '30-60'
  WHEN DATEDIFF(DAY, LAST_DAY(payment_created_at, QUARTER), chargeback_date::DATE) <= 90 THEN '60-90'
  WHEN DATEDIFF(DAY, LAST_DAY(payment_created_at, QUARTER), chargeback_date::DATE) <= 180 THEN '90-180'
  ELSE '180+' END AS days_group_since_quarter_end
 ,CASE WHEN DATEDIFF(DAY, LAST_DAY(payment_created_at, QUARTER), chargeback_date::DATE) <= 15 THEN 1
  WHEN DATEDIFF(DAY, LAST_DAY(payment_created_at, QUARTER), chargeback_date::DATE) <= 30 THEN 2
  WHEN DATEDIFF(DAY, LAST_DAY(payment_created_at, QUARTER), chargeback_date::DATE) <= 60 THEN 3
  WHEN DATEDIFF(DAY, LAST_DAY(payment_created_at, QUARTER), chargeback_date::DATE) <= 90 THEN 4
  WHEN DATEDIFF(DAY, LAST_DAY(payment_created_at, QUARTER), chargeback_date::DATE) <= 180 THEN 5
  ELSE 6 END AS days_group_cd_since_quarter_end
 ,reason_code_type
 ,taxonomy_category_name
 ,chargeback_cents
 ,loss_cents
 ,loss_cents_upper_bound
from app_risk.app_risk.chargebacks cb
WHERE 
 --reason_code_type = 'credit'
--WHERE taxonomy_category_name = 'Credit Risk' and
 cb.user_token NOT IN ('F68JPSGH39ZJB', '6H30GW8FWXB14')
AND DATE_TRUNC(MONTH, payment_created_at) >= '2019-01-01'
)
   select 
    cb.user_token AS unit_token
  , cb.month
  , cb.quarter
  , cb.currency_Code
  , cb.days_since_month_end
  , cb.days_group_since_month_end
  , cb.days_group_cd_since_month_end
  , cb.days_since_quarter_end
  , cb.days_group_since_quarter_end
  , cb.days_group_cd_since_quarter_end
  , cb.reason_code_type
  , cb.taxonomy_category_name
  , SUM (chargeback_cents) AS chargeback_cents
  , SUM (loss_cents) AS loss_cents
  , SUM (loss_cents_upper_bound) AS loss_cents_upper_bound
  , SUM (CASE WHEN days_group_cd_since_month_end<=1 THEN chargeback_cents ELSE 0 END) AS exposure_cents_monthly_15
  , SUM (CASE WHEN  days_group_cd_since_month_end<=2 THEN chargeback_cents ELSE 0 END) AS exposure_cents_monthly_30
  , SUM (CASE WHEN  days_group_cd_since_month_end<=3 THEN chargeback_cents ELSE 0 END) AS exposure_cents_monthly_60
  , SUM (CASE WHEN  days_group_cd_since_month_end<=4 THEN chargeback_cents ELSE 0 END) AS exposure_cents_monthly_90
  , SUM (CASE WHEN  days_group_cd_since_month_end<=5 THEN chargeback_cents ELSE 0 END) AS exposure_cents_monthly_180
  
  , SUM (CASE WHEN  days_group_cd_since_month_end<=1 THEN loss_cents ELSE 0 END) AS loss_cents_monthly_15
  , SUM (CASE WHEN  days_group_cd_since_month_end<=2 THEN loss_cents ELSE 0 END) AS loss_cents_monthly_30
  , SUM (CASE WHEN  days_group_cd_since_month_end<=3 THEN loss_cents ELSE 0 END) AS loss_cents_monthly_60
  , SUM (CASE WHEN  days_group_cd_since_month_end<=4 THEN loss_cents ELSE 0 END) AS loss_cents_monthly_90
  , SUM (CASE WHEN  days_group_cd_since_month_end<=5 THEN loss_cents ELSE 0 END) AS loss_cents_monthly_180
  
  , SUM (CASE WHEN  days_group_cd_since_month_end<=1 THEN loss_cents_upper_bound ELSE 0 END) AS loss_cents_upper_bound_monthly_15
  , SUM (CASE WHEN  days_group_cd_since_month_end<=2 THEN loss_cents_upper_bound ELSE 0 END) AS loss_cents_upper_bound_monthly_30
  , SUM (CASE WHEN  days_group_cd_since_month_end<=3 THEN loss_cents_upper_bound ELSE 0 END) AS loss_cents_upper_bound_monthly_60
  , SUM (CASE WHEN  days_group_cd_since_month_end<=4 THEN loss_cents_upper_bound ELSE 0 END) AS loss_cents_upper_bound_monthly_90
  , SUM (CASE WHEN  days_group_cd_since_month_end<=5 THEN loss_cents_upper_bound ELSE 0 END) AS loss_cents_upper_bound_monthly_180
  
  , SUM (CASE WHEN  days_group_cd_since_quarter_end<=1 THEN chargeback_cents ELSE 0 END) AS exposure_cents_quarterly_15
  , SUM (CASE WHEN  days_group_cd_since_quarter_end<=2 THEN chargeback_cents ELSE 0 END) AS exposure_cents_quarterly_30
  , SUM (CASE WHEN  days_group_cd_since_quarter_end<=3 THEN chargeback_cents ELSE 0 END) AS exposure_cents_quarterly_60
  , SUM (CASE WHEN  days_group_cd_since_quarter_end<=4 THEN chargeback_cents ELSE 0 END) AS exposure_cents_quarterly_90
  , SUM (CASE WHEN  days_group_cd_since_quarter_end<=5 THEN chargeback_cents ELSE 0 END) AS exposure_cents_quarterly_180
  
  , SUM (CASE WHEN  days_group_cd_since_quarter_end<=1 THEN loss_cents ELSE 0 END) AS loss_cents_quarterly_15
  , SUM (CASE WHEN  days_group_cd_since_quarter_end<=2 THEN loss_cents ELSE 0 END) AS loss_cents_quarterly_30
  , SUM (CASE WHEN  days_group_cd_since_quarter_end<=3 THEN loss_cents ELSE 0 END) AS loss_cents_quarterly_60
  , SUM (CASE WHEN  days_group_cd_since_quarter_end<=4 THEN loss_cents ELSE 0 END) AS loss_cents_quarterly_90
  , SUM (CASE WHEN  days_group_cd_since_quarter_end<=5 THEN loss_cents ELSE 0 END) AS loss_cents_quarterly_180
  
  , SUM (CASE WHEN  days_group_cd_since_quarter_end<=1 THEN loss_cents_upper_bound ELSE 0 END) AS loss_cents_upper_bound_quarterly_15
  , SUM (CASE WHEN  days_group_cd_since_quarter_end<=2 THEN loss_cents_upper_bound ELSE 0 END) AS loss_cents_upper_bound_quarterly_30
  , SUM (CASE WHEN  days_group_cd_since_quarter_end<=3 THEN loss_cents_upper_bound ELSE 0 END) AS loss_cents_upper_bound_quarterly_60
  , SUM (CASE WHEN  days_group_cd_since_quarter_end<=4 THEN loss_cents_upper_bound ELSE 0 END) AS loss_cents_upper_bound_quarterly_90
  , SUM (CASE WHEN  days_group_cd_since_quarter_end<=5 THEN loss_cents_upper_bound ELSE 0 END) AS loss_cents_upper_bound_quarterly_180
   from cb
   GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12
   ;

create or replace table app_risk.app_risk.acct_gpvband_m as   
select 
distinct dps1.unit_token
,dps1.month
,dps1.currency_code
,sum(CASE WHEN DATEDIFF(month, dps2.month, dps1.month) between 0 and 11 then dps2.monthly_gpv_dllr
     else 0 end) as annual_gpv
,sum(CASE WHEN DATEDIFF(month, dps2.month, dps1.month) between 0 and 2 then dps2.monthly_gpv_dllr
     else 0 end) *4 as annualized_qtly_gpv
,case when annual_gpv> annualized_qtly_gpv then annual_gpv
else annualized_qtly_gpv end as gpv_vf
,case when gpv_vf > 500000 then '3.>500k'
when gpv_vf >125000 then '2.125-500k'
else '1.<125k' end as gpv_band_m
from (select 
      unit_token
      ,date_trunc(month, payment_trx_recognized_date) as month
      ,currency_code
      ,sum(gpv_payment_amount_base_unit/100) as monthly_gpv_dllr
      from app_bi.pentagon.aggregate_seller_daily_payment_summary
      group by 1,2,3) dps1
left join (select 
           unit_token
           ,date_trunc(month, payment_trx_recognized_date) as month
           ,currency_code
           ,sum(gpv_payment_amount_base_unit/100) as monthly_gpv_dllr
           from app_bi.pentagon.aggregate_seller_daily_payment_summary
           group by 1,2,3) dps2
on dps1.unit_token = dps2.unit_token
and dps1.currency_code = dps2.currency_code
and dps1.month >= '2019-01-01' 
AND dps1.month <= CURRENT_DATE()
group by 1,2,3
;

create or replace table app_risk.app_risk.acct_gpvband_q as
select distinct unit_token
      ,date_trunc(quarter, payment_trx_recognized_date) as quarter
      ,currency_code
      ,sum(gpv_payment_amount_base_unit/100)*4 as annulized_quarterly_gpv_dllr
      ,case when annulized_quarterly_gpv_dllr > 500000 then '3.>500k'
        when annulized_quarterly_gpv_dllr >125000 then '2.125-500k'
        else '1.<125k' end as gpv_band_q
      from app_bi.pentagon.aggregate_seller_daily_payment_summary
      WHERE payment_trx_recognized_date >= '2019-01-01'
      and payment_trx_recognized_date <= current_date()
      group by 1,2,3
;

create or replace table app_risk.app_risk.gpv_m_gpvband as 
SELECT 
DATE_TRUNC(MONTH, payment_trx_recognized_date) AS pmt_month, 
vf.currency_Code, 
gpv_band_m,
SUM (gpv_payment_amount_base_unit) AS monthly_gpv_cents 
FROM app_bi.pentagon.aggregate_seller_daily_payment_summary vf
left join (select distinct unit_token, month, currency_code, gpv_band_m from app_risk.app_risk.acct_gpvband_m) gpvband --to get gpv band for each seller in each month and currency code
on vf.unit_token = gpvband.unit_token
and DATE_TRUNC(MONTH, payment_trx_recognized_date) = gpvband.month
and vf.currency_code = gpvband.currency_code
 WHERE DATE_TRUNC(MONTH, payment_trx_recognized_date) >= '2019-01-01' AND DATE_TRUNC(MONTH, payment_trx_recognized_date) <= CURRENT_DATE()
 AND vf.unit_token NOT IN ('F68JPSGH39ZJB', '6H30GW8FWXB14') 
 GROUP BY pmt_month, vf.currency_Code, gpv_band_m
;

create or replace table app_risk.app_risk.gpv_q_gpvband as 
SELECT distinct
gpvband.quarter as pmt_quarter,
vf.currency_Code, 
gpv_band_q,
SUM (gpv_payment_amount_base_unit) AS quarterly_gpv_cents 
FROM (select distinct unit_token, quarter, currency_code, gpv_band_q from app_risk.app_risk.acct_gpvband_q) gpvband --to get gpv band for each seller in each quarter and currency code
left join app_bi.pentagon.aggregate_seller_daily_payment_summary vf 
on vf.unit_token = gpvband.unit_token
and DATE_TRUNC(quarter, payment_trx_recognized_date) = gpvband.quarter
and vf.currency_code = gpvband.currency_code
 WHERE DATE_TRUNC(MONTH, payment_trx_recognized_date) >= '2019-01-01' AND DATE_TRUNC(MONTH, payment_trx_recognized_date) <= '2021-09-01'
 AND vf.unit_token NOT IN ('F68JPSGH39ZJB', '6H30GW8FWXB14') 
 GROUP BY quarter, vf.currency_Code, gpv_band_q
;

 CREATE OR REPLACE TABLE app_risk.app_risk.chargeback_arrival_quarterly_loading_gpvband AS   
 select
    drv.month
  , drv.quarter
  , drv.currency_Code
  , nvl(gpv_m.gpv_band_m,'1.<125k') as gpv_band_m
  , nvl(gpv_q.gpv_band_q,'2.<125k') as gpv_band_q
  , days_since_month_end
  , days_group_since_month_end
  , days_group_cd_since_month_end
  , days_since_quarter_end
  , days_group_since_quarter_end
  , days_group_cd_since_quarter_end  
  , reason_code_type
  , taxonomy_category_name
  , MAX (monthly_gpv_cents) AS monthly_gpv_cents
  , MAX (quarterly_gpv_cents) AS quarterly_gpv_cents
  , SUM (chargeback_cents) AS chargeback_cents
  , SUM (loss_cents) AS loss_cents
  , SUM (loss_cents_upper_bound) AS loss_cents_upper_bound
  , SUM (exposure_cents_monthly_15  ) AS exposure_cents_monthly_15
  , SUM (exposure_cents_monthly_30  ) AS exposure_cents_monthly_30
  , SUM (exposure_cents_monthly_60  ) AS exposure_cents_monthly_60
  , SUM (exposure_cents_monthly_90  ) AS exposure_cents_monthly_90
  , SUM (exposure_cents_monthly_180  ) AS exposure_cents_monthly_180
  
  , SUM (loss_cents_monthly_15  ) AS loss_cents_monthly_15
  , SUM (loss_cents_monthly_30  ) AS loss_cents_monthly_30
  , SUM (loss_cents_monthly_60  ) AS loss_cents_monthly_60
  , SUM (loss_cents_monthly_90  ) AS loss_cents_monthly_90
  , SUM (loss_cents_monthly_180  ) AS loss_cents_monthly_180
  
  , SUM (loss_cents_upper_bound_monthly_15   ) AS loss_cents_upper_bound_monthly_15
  , SUM (loss_cents_upper_bound_monthly_30   ) AS loss_cents_upper_bound_monthly_30
  , SUM (loss_cents_upper_bound_monthly_60   ) AS loss_cents_upper_bound_monthly_60
  , SUM (loss_cents_upper_bound_monthly_90   ) AS loss_cents_upper_bound_monthly_90
  , SUM (loss_cents_upper_bound_monthly_180  ) AS loss_cents_upper_bound_monthly_180
  
  , SUM (exposure_cents_quarterly_15  ) AS exposure_cents_quarterly_15
  , SUM (exposure_cents_quarterly_30  ) AS exposure_cents_quarterly_30
  , SUM (exposure_cents_quarterly_60  ) AS exposure_cents_quarterly_60
  , SUM (exposure_cents_quarterly_90  ) AS exposure_cents_quarterly_90
  , SUM (exposure_cents_quarterly_180  ) AS exposure_cents_quarterly_180
  
  , SUM (loss_cents_quarterly_15  ) AS loss_cents_quarterly_15
  , SUM (loss_cents_quarterly_30  ) AS loss_cents_quarterly_30
  , SUM (loss_cents_quarterly_60  ) AS loss_cents_quarterly_60
  , SUM (loss_cents_quarterly_90  ) AS loss_cents_quarterly_90
  , SUM (loss_cents_quarterly_180  ) AS loss_cents_quarterly_180
  
  , SUM (loss_cents_upper_bound_quarterly_15   ) AS loss_cents_upper_bound_quarterly_15
  , SUM (loss_cents_upper_bound_quarterly_30   ) AS loss_cents_upper_bound_quarterly_30
  , SUM (loss_cents_upper_bound_quarterly_60   ) AS loss_cents_upper_bound_quarterly_60
  , SUM (loss_cents_upper_bound_quarterly_90   ) AS loss_cents_upper_bound_quarterly_90
  , SUM (loss_cents_upper_bound_quarterly_180  ) AS loss_cents_upper_bound_quarterly_180
FROM app_risk.app_risk.cb_arrival01_gpv_fpt_loading_gpvband drv
left join app_risk.app_risk.acct_gpvband_m acct_gpv_m
on drv.unit_token = acct_gpv_m.unit_token and drv.month = acct_gpv_m.month
left join app_risk.app_risk.acct_gpvband_q acct_gpv_q
on drv.unit_token = acct_gpv_q.unit_token and drv.quarter = acct_gpv_q.quarter
LEFT OUTER JOIN app_risk.app_risk.gpv_m_gpvband gpv_m 
ON drv.month = gpv_m.pmt_month AND drv.currency_Code = gpv_m.currency_Code and acct_gpv_m.gpv_band_m = gpv_m.gpv_band_m
LEFT OUTER JOIN app_risk.app_risk.gpv_q_gpvband gpv_q 
ON drv.quarter = gpv_q.pmt_quarter AND drv.currency_Code = gpv_q.currency_Code and acct_gpv_q.gpv_band_q = gpv_q.gpv_band_q
GROUP BY
   drv.month
  , drv.quarter
  , drv.currency_Code
  , gpv_m.gpv_band_m
  , gpv_q.gpv_band_q
  , days_since_month_end
  , days_group_since_month_end
  , days_group_cd_since_month_end
  , days_since_quarter_end
  , days_group_since_quarter_end
  , days_group_cd_since_quarter_end  
  , reason_code_type
  , taxonomy_category_name
;


CREATE TABLE IF NOT EXISTS app_risk.app_risk.chargeback_arrival_quarterly_gpvband LIKE app_risk.app_risk.chargeback_arrival_quarterly_loading_gpvband;
ALTER TABLE app_risk.app_risk.chargeback_arrival_quarterly_gpvband SWAP WITH app_risk.app_risk.chargeback_arrival_quarterly_loading_gpvband;
