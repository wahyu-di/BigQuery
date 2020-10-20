with
fd as (
  select
    filter1
    , timestamp_add(filter1, interval -1 day) as filter2
  from 
    (
      select 
        --timestamp('2019-04-30 17:00:00') as filter1
        timestamp('2020-10-14') as filter1
    )

)
, op as (
  select
    order_id
    , payment_source
  from
    --`prod-datarangers.galaxy_stg.order__payment`
    `datamart-finance.staging.v_order__payment` 
  where
    payment_flag = 1
    and payment_id = 1
    and payment_timestamp >= (select filter2 from fd)
)
, oc as (
  select
    order_id
    , datetime(payment_timestamp, 'Asia/Jakarta') as payment_timestamp
    , account_id
    , cs_id
    , reseller_id
    , reseller_type
  from
    --`prod-datarangers.galaxy_stg.order__cart`
    `datamart-finance.staging.v_order__cart` 
  where
    payment_status = 'paid'
    and reseller_type not in ('txtravel')
    and payment_timestamp >= (select filter1 from fd)
)
, ocdf as (
  select
    order_id
    , order_detail_id
    , order_name
    , order_name_detail
    , selling_price
    , selling_currency
    , customer_currency
    , customer_price
    , reseller_sub_price_idr
    , order_detail_status
  from
    --`prod-datarangers.galaxy_stg.order__cart_detail`
    `datamart-finance.staging.v_order__cart_detail` 
  where
    order_type = 'flight'
    and order_detail_status in ('active','refund','refunded','recheck','hide_by_cust')
    and created_timestamp >= (select filter2 from fd)
)
, ocdp as (
  select
    order_id
    , order_detail_id
    , order_type
    , order_name
    , selling_price
  from
    --`prod-datarangers.galaxy_stg.order__cart_detail`
    `datamart-finance.staging.v_order__cart_detail` 
  where
    created_timestamp >= (select filter2 from fd)
    and order_detail_status in ('active','refund','refunded','recheck')
)
, ocfp as (
  select
    order_detail_id
    ,SUM(IF(lower(title) = 'mr', 1, 0)) total_man_adult
    ,SUM(IF(lower(title) = 'mrs' OR lower(title) = 'ms', 1, 0)) total_woman_adult
    ,SUM(IF(lower(title) = 'mstr' AND lower(type)='child', 1, 0)) total_boy_child
    ,SUM(IF(lower(title) = 'miss' AND lower(type)='child', 1, 0)) total_girl_child
    ,SUM(IF(lower(title) = 'mstr' AND lower(type)='infant', 1, 0)) total_boy_infant
    ,SUM(IF(lower(title) = 'miss' AND lower(type)='infant', 1, 0)) total_girl_infant
    ,string_agg(ticket_number) as ticket_number
    ,string_agg(distinct substr(ticket_number,0,3)) as airlines_code
    ,string_agg(distinct substr(ticket_number,3,length(ticket_number))) as ticket_number_code
  from
    --`prod-datarangers.galaxy_stg.order__cart_flight_passenger` 
    `datamart-finance.staging.v_order__cart_flight_passenger` 
  group by order_detail_id
)
, ocf as (
  select
    order_detail_id
    , price_currency
    , price_nta
    , balance_due
    , count_adult
    , count_child
    , count_infant
    , base_fare_adult
    , base_fare_child
    , base_fare_infant
    , booking_code
    , ticket_class
    , sub_price_IDR
    , baggage_fee
    , account as account_code
    , departure_city
    , arrival_city
    , datetime(departure_time,'Asia/Jakarta') as departure_time
    , datetime(arrival_time,'Asia/Jakarta') as arrival_time
    , flight_number
    , airlines_name
    , airlines_master_id
    , vendor
    , ticket_status
  from
    --`prod-datarangers.galaxy_stg.order__cart_flight` 
    `datamart-finance.staging.v_order__cart_flight` 
  where
    ticket_status = 'issued'
    and departure_time >= (select filter2 from fd)
)
, ocfc as (
  select
    *
  from
    (
      select
        order_detail_id
        , total
        , row_number() over(partition by order_detail_id order by processed_dttm desc, updated_timestamp desc) as rn
      from
        --`prod-datarangers.galaxy_stg.order__cart_flight_comission`
        `datamart-finance.staging.v_order__cart_flight_comission` 
      where
        created_timestamp >= (select filter2 from fd)
    )
  where rn = 1
)
, ma_ori as (
  select
    distinct
    account_id
    , lower(replace(replace(account_username,'"',''),'\\','')) as account_username
    , account_last_login as accountlastlogin
    , processed_dttm
  from
    --`prod-datarangers.galaxy_stg.member__account`
    `datamart-finance.staging.v_member__account` 
  union distinct
  select
    distinct
    accountid as account_id
    , lower(replace(replace(accountusername,'"',''),'\\','')) as account_username
    , accountlastlogin
    , processed_dttm
  from
    --`prod-datarangers.galaxy_stg.members_account_admin` 
    `datamart-finance.staging.v_members_account_admin` 
  union distinct
  select
    distinct
    accountid as account_id
    , lower(replace(replace(accountusername,'"',''),'\\','')) as account_username
    , accountlastlogin
    , processed_dttm
  from
    --`prod-datarangers.galaxy_stg.members_account_b2c` 
    `datamart-finance.staging.v_members_account_b2c` 
  union distinct
  select
    distinct
    accountid as account_id
    , lower(replace(replace(accountusername,'"',''),'\\','')) as account_username
    , accountlastlogin
    , processed_dttm
  from
    --`prod-datarangers.galaxy_stg.members_account_b2b` 
    `datamart-finance.staging.v_members_account_b2b` 
)
, ma as (
  select 
    * 
  from (
    select
      *
      , row_number() over(partition by account_id order by processed_dttm desc, accountlastlogin desc) as rn
    from
      ma_ori
  ) 
  where rn = 1
)
/*, mp as (
  select
    *
  from
    (
      select 
        account_id
        , account_first_name
        , account_last_name
        , row_number() over(partition by account_id order by processed_dttm desc, account_profile_modified desc) as rn 
      from 
        `prod-datarangers.galaxy_stg.member__profile` 
      where 
        is_primary = 1
    )
  where rn = 1
)*/
, ca as (
  select
    distinct
    lower(EmailAccessB2C) as account_username
    , 'Yes' as corporate_flag
  from
    --`prod-datarangers.galaxy_stg.corporate_account` 
    `datamart-finance.staging.v_corporate_account` 
)
, fan as (
  select
    account_code
    , account_name
  from
    --`prod-datarangers.galaxy_stg.mapping_flight_account_name_finance` 
    `datamart-finance.staging.v_mapping_flight_account_name_finance` 
)
, ocfs as (
  select 
    order_detail_id
    , json_extract_scalar(fij,'$.flexi') as is_flexi
  from 
    `datamart-finance.staging.v_order__cart_flight_segment`
  left join
      unnest (json_extract_array(flight_info_json)) as fij
  where
    departure_time >= (select filter1 from fd)
    and flight_date >= (select date_add((select date(filter1) from fd), interval 1 day))
)
, fact as (
  select
    max(oc.payment_timestamp) as payment_timestamp
    , oc.order_id
    , ocdf.order_detail_id
    , max(ocdf.selling_price) as selling_price
    , string_agg(distinct ocdf.selling_currency) as selling_currency
    , replace(
            string_agg(distinct case
              when ocdp.order_type = 'promocode' then ocdp.order_name
              else null
              end
            )
          ,'Promo Code : ','') as promocode_name
    , sum(
        case 
          when ocdp.order_type = 'promocode' then ABS(ocdp.selling_price)
          else 0
        end
      )  as promocode_value
    , max(ocdf.customer_price) as customer_price
    , max(ocf.price_nta) as net_price
    , max(ocf.balance_due) as balance_due
    , string_agg(distinct ocf.price_currency) as price_currency
    , string_agg(distinct ocdf.customer_currency) as customer_currency
    , max(ocf.count_adult) as count_adult
    , max(ocf.count_child) as count_child
    , max(ocf.count_infant) as count_infant
    , max(ocf.count_adult * if(ocf.base_fare_adult is null, 0, ocf.base_fare_adult) +
      ocf.count_child * if(ocf.base_fare_child is null, 0, ocf.base_fare_child) +
      ocf.count_infant * if(ocf.base_fare_infant is null, 0, ocf.base_fare_infant)) as  total_base_fare
    , string_agg(distinct ocf.booking_code) as booking_code
    , string_agg(distinct ocf.ticket_class) as ticket_class
    , max(distinct ocf.sub_price_idr) as sub_price_idr
    , max(distinct ocf.baggage_fee) as baggage_fee
    , max(distinct ocdf.reseller_sub_price_idr) as reseller_sub_price_idr
    , string_agg(distinct op.payment_source) as payment_source
    , string_agg(distinct ocdf.order_name) as order_name
    , string_agg(distinct ocdf.order_name_detail) as order_name_detail
    , max(distinct oc.account_id) as account_id
    , string_agg(distinct ma.account_username) as account_username
    --, string_agg(distinct coalesce( concat(mp.account_first_name, ' ', ifnull(mp.account_last_name,'')), ma.account_username)) as customer_name
    , string_agg(distinct ocf.ticket_status) as ticket_status
    , string_agg(distinct ocf.account_code) as account_code
    , string_agg(distinct ocf.departure_city) as departure_city 
    , max(ocf.departure_time) as departure_time
    , string_agg(distinct ocf.arrival_city) as arrival_city
    , max(ocf.arrival_time) as arrival_time
    , string_agg(distinct ocf.flight_number) as flight_number
    , string_agg(distinct ocf.airlines_name) as airlines_name
    , max(ocf.airlines_master_id) as airlines_master_id
    , string_agg(distinct 
        case 
          when ocf.vendor = 'sa' then 'Sabre' 
          else 'Native' 
        end) as vendor_name
    , string_agg(distinct 
        case 
--           when ocf.vendor = 'sa' and lower(account) like '%mayflower%' then 'MayFlower' 
--           when ocf.vendor = 'sa' and lower(account) like '%hnhsabre%' then 'Hong Ngoc Ha Co Ltd' 
--           when ocf.vendor = 'sa' and lower(account) like '%holidaytoursabre%' then 'Holiday Tour & Travel' 
--           when ocf.vendor = 'sa' and lower(account) like '%gmtoursabre%' then 'GM Tour' 
--           when ocf.vendor = 'sa' and lower(account) like '%ctmsabre%' then 'CTM' 
--           when ocf.vendor = 'sa' and lower(account) like '%aviasabre%' then 'AviaTour' 
--           when ocf.vendor = 'sa' and lower(account) like '%tidesquare%' then 'Tidesquare' 
          when ocf.vendor = 'sa' then coalesce(fan.account_name,'Sabre')
          else 'Native' 
        end
        ) as pcc_account_name
    , case 
        when string_agg(distinct ocf.vendor) != 'na' then string_agg(distinct ocfp.airlines_code)
        else null
      end as airlines_code
    , case
        when max(oc.cs_id) > 0 then 'yes'
        else 'no'
      end as order_by_cs_flag
  , string_agg(distinct case
        when ca.corporate_flag is not null then 'B2B Corporate'
        when 
          oc.reseller_type in ('none','online_marketing','native_apps')
          and ca.corporate_flag is null
            then 'B2C'
        when oc.reseller_type in ('none','online_marketing','native_apps') then 'B2C'
        when oc.reseller_type in ('tiket_agent','txtravel','agent','affiliate') then 'B2B Offline'
        when oc.reseller_type in ('reseller','widget') then 'B2B Online'
      end) as customer_type
    , max(ocfp.total_man_adult) as total_man_adult
    , max(ocfp.total_woman_adult) as total_woman_adult
    , max(ocfp.total_boy_child) as total_boy_child
    , max(ocfp.total_girl_child) as total_girl_child
    , max(ocfp.total_boy_infant) as total_boy_infant
    , max(ocfp.total_girl_infant) as total_girl_infant
    , max(ocfc.total) as total_comission
    , string_agg(distinct ocdf.order_detail_status) as order_detail_status
    , string_agg(distinct ocfp.ticket_number) as ticket_number
    /*, case 
		when string_agg(distinct ocf.vendor) = 'sa' then string_agg(distinct ocfp.ticket_number)
        when string_agg(distinct ocf.vendor) != 'na' then string_agg(distinct ocfp.ticket_number_code)
        else string_agg(distinct ocfp.ticket_number)
      end as ticket_number*/
    , string_agg(distinct ocfs.is_flexi) as is_flexi
  from
    op
    join oc using (order_id)
    join ocdf using (order_id)
    join ocf on ocf.order_detail_id = ocdf.order_detail_id
    left join ocdp using (order_id)
    left join ma using (account_id)
    --left join mp using (account_id)
    left join ca using (account_username)
    left join fan using (account_code)
    left join ocfp on ocdf.order_detail_id = ocfp.order_detail_id
    left join ocfc on ocdf.order_detail_id = ocfc.order_detail_id
    left join ocfs on ocdf.order_detail_id = ocfs.order_detail_id
  group by
    oc.order_id
    , ocdf.order_detail_id
)

select 
  order_id
  , order_detail_id
  , departure_city
  , departure_time
  , arrival_city
  , arrival_time
  , airlines_name
  , replace(flight_number,' ','-') as flight_number
  , booking_code
  , ticket_class as class
  , payment_timestamp
  , payment_source as payment_method
  --, customer_name
  , safe_cast(total_base_fare as float64) as total_base_fare
  , safe_cast(total_man_adult as float64) as adult_male
  , safe_cast(total_woman_adult as float64) as adult_female
  , safe_cast(total_boy_child as float64) as child_boy
  , safe_cast(total_girl_child as float64) as child_girl
  , safe_cast(total_boy_infant as float64) as infant_boy
  , safe_cast(total_girl_infant as float64) as infant_girl
  , safe_cast(customer_price + sub_price_idr as float64) as gross_revenue
  , safe_cast(customer_price as float64) as customer_price
  , customer_currency
  , safe_cast(
      case 
        when total_comission = 0 or total_comission is null then customer_price + sub_price_idr + reseller_sub_price_idr - balance_due
        else total_comission
      end 
    as float64) as commission 
  , safe_cast(
    case
      when vendor_name='Sabre' and total_comission > 0 then net_price-(round(total_comission,0))
      when airlines_master_id IN(20392404,24307475) and total_comission > 0 then net_price-(round(total_comission,0))
      else net_price 
      end
    as float64) as pay_to_business
  , safe_cast(balance_due as float64) as balance_due
  , safe_cast(sub_price_idr as float64) as sub_price_idr
  , safe_cast(reseller_sub_price_idr as float64) as reseller_sub_price_idr
  , promocode_name
  , safe_cast(round(safe_divide(customer_price*promocode_value,sum(customer_price) over(partition by order_id)),2) as float64) as promocode_value
  , safe_cast(baggage_fee as float64) as baggage_fee
  , account_code
  , customer_type
  , order_by_cs_flag
  , order_detail_status
  , vendor_name
  , pcc_account_name
  , ticket_number
  , airlines_code
  , is_flexi
from 
  fact --where order_id in(184284016, 184285621, 182429611)