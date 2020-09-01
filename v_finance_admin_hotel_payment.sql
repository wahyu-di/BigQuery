with
fd as (
  select
    filter1
    , timestamp_add(filter1, interval -1 day) as filter2
    , timestamp_add(filter1, interval 1 day) as filter3
  from
  (
    select
      timestamp('2019-12-30 17:00:00') as filter1
  )
)
, oc as (
  select
    distinct
    order_id
    , datetime(payment_timestamp, 'Asia/Jakarta') as payment_timestamp
  from
    `datamart-finance.staging.v_order__cart` 
  where
    payment_status = 'paid'
    and payment_timestamp >= (select filter1 from fd)
)
, ocd as (
    select
      distinct
      order_id
      , order_detail_id
      -- , order_type
      -- , order_detail_status
      , selling_currency
      , selling_price
      , customer_currency
      , order_name
      , order_name_detail
    from
      `datamart-finance.staging.v_order__cart_detail`
    where
      order_detail_status in ('active', 'refund', 'refunded','hide_by_cust')
      and order_type in ('tixhotel')
      and created_timestamp >= (select filter2 from fd)
)
, op as (
  select
    distinct
    order_id
    , payment_amount
    , payment_source
  from
    `datamart-finance.staging.v_order__payment` 
  where
    payment_flag = 1
    and payment_id = 1
    and payment_timestamp >= (select filter2 from fd)
)
, oth as (
  select
    order_id
    , string_agg(distinct hotel_itineraryNumber) as hotel_itineraryNumber
    , sum(totalwithtax) as totalwithtax 
    , date(min(booking_checkindate),'Asia/Jakarta') as booking_checkindate
    , date(max(booking_checkoutdate),'Asia/Jakarta') as booking_checkoutdate
    , string_agg(distinct room_source) as room_source
    , max(safe_cast(hotel_itinerarynumber as int64)) as itinerary_id
    , max(safe_cast(hotel_id as int64)) as hotel_id
    , sum(nett_price) as nett_price
    , max(markup_percentage) as markup_percentage
    , max(vendor_incentive) as vendor_incentive
    , string_agg(distinct payment_type) as payment_type
  from
    `datamart-finance.staging.v_order__tixhotel` 
  where
    created_timestamp >= (select filter2 from fd)
  group by
    order_id
)
, hb as (
  select
    id as itinerary_id
    , max(customer_price) as customer_price
    , string_agg(distinct customer_currency) as customer_currency
    , string_agg(distinct selling_currency) as selling_currency
    , max(currency_exchange_rate) as currency_exchange_rate 
    -- , string_agg(distinct user_lang) as user_lang
    -- , string_agg(distinct room_name) as room_name
    , max(hotel_id) as master_id
  from
    `datamart-finance.staging.v_hotel_bookings` 
  where
    updated_date >= (select filter2 from fd)
  group by
    itinerary_id
)
, hbd as (
  select
    itinerary_id
    , date(min(checkin_date),'Asia/Jakarta') as checkin_date
    , date_add(date(max(checkin_date),'Asia/Jakarta'), interval 1 day) as checkout_date
--     , max(id) as room_id
    , sum(total_net_rate_price) as total_net_rate_price
  from
    `datamart-finance.staging.v_hotel_booking_details` 
  group by
    itinerary_id 
)
, bpr as (
  select
    business_id as master_id
    , string_agg(distinct business_name) as business_name
    , string_agg(distinct account_type) as account_type
    , string_agg(distinct account_info) as account_info
    , string_agg(distinct account_number) as account_number
    , string_agg(distinct account_owner) as account_owner
  from
    `datamart-finance.staging.v_business__profile` 
  group by 
    business_id
)
, ma as (
  select
    distinct
    account_id
    , account_username
  from
    `datamart-finance.staging.v_member__account`
)
, mrb as (
  select
    mrb.business_id as master_id
    , string_agg(distinct ma.account_username) as account_username
  from
   `datamart-finance.staging.v_member__rel_business` mrb
    left join ma using (account_id)
  where
    mrb.role_id in ('5','6','7')
  group by 
    mrb.business_id
    
)
, ro as (
  select
    distinct
    case
      when refundtype = 'SPECIAL_REFUND' then refundoptorderdetailid
      else referencedetailid 
    end as order_detail_id
    , 'refund' as refund_flag
  from  
    `datamart-finance.staging.v_tix_refund_refund_order` 
  where
    refundrequestdatetime >= (select filter2 from fd)
)
, ro_old as (
  select
    distinct
    case
      when refund_source = 'special_refund' then cast(refund_opt_order_detail_id as int64)
      else refund_order_detail_id 
    end as order_detail_id
    , 'refund' as refund_flag
  from  
    `datamart-finance.staging.v_order__refund`  
  where
    refund_request_datetime  >= (select filter2 from fd)
)
, orf as (
  select * from ro
  union distinct
  select * from ro_old
)
, bp as (
  select
    order_detail_id
    , string_agg(distinct payment_status) as payment_status
    , datetime(max(payment_datetime), 'Asia/Jakarta') as finance_payment_datetime
    , sum(payment_amount) as finance_payment_amount
  from
    `datamart-finance.staging.v_business__payment`  
  where 
    payment_datetime >= (select filter2 from fd)
  group by
    order_detail_id
)
, hps as (
  select
    itinerary_id
    , string_agg(distinct payment_status) as payment_status
    , datetime(max(updated_date), 'Asia/Jakarta') as finance_payment_datetime
    , sum(amount) as finance_payment_amount
  from
  `datamart-finance.staging.v_hotel_payments`  
  where 
    created_date >= (select filter2 from fd)
    and payment_status = 'PAID'
  group by
    itinerary_id
)
, hp as (
  select
    distinct
    itinerary_id
    , payment_plan
    , payment_method
  from
    --`prod-datarangers.galaxy_stg.hotel_payments` 
    `datamart-finance.staging.v_hotel_payments` 
)
, hpi as (
  select
    hotel_id as master_id
    , string_agg(distinct payment_method) as payment_method
  from
    `datamart-finance.staging.v_hotel_payment_informations`
  group by
    hotel_id
)
, hpt as (
  select
    distinct
    business_id as master_id
    , 'deposit' as deposit_flag
  from
    --`datamart-finance.staging.v_hotel__payment_type` 
    `datamart-finance.staging.v_hotel__payment_type`  
  where 
    status = 'active'
)
, hmm as (
  select
    distinct
    safe_cast(new_hotel_id as int64) as master_id
    , country
  from
    --`prod-datarangers.galaxy_dwh.hotel_mapping_master`
    `datamart-finance.staging.v_hotel_mapping_master`  
)
, 
oar as (
select 
  distinct(new_order_id) as order_id
  , order_id as old_order_id
  , total_customer_price
  , new_total_customer_price
from `datamart-finance.staging.v_order__automatic_rebooking` 
where rebook_status='SUCCESS'
)
, fact as (
  select
    oc.order_id
    , ocd.order_detail_id
    , oar.old_order_id
    , oar.new_total_customer_price-oar.total_customer_price as difference
    , oar.total_customer_price
    , oc.payment_timestamp
    , op.payment_amount
    , case 
        when oth.room_source <> 'TIKET' then oth.totalwithtax
        else hb.customer_price
      end as customer_price
    , case 
        when oth.room_source <> 'TIKET' then ocd.selling_currency
        else hb.selling_currency
      end as selling_currency
    , ocd.selling_price as selling_price
    , case 
        when oth.room_source <> 'TIKET' then ocd.customer_currency
        else hb.customer_currency
      end as customer_currency
    , case 
        when oth.room_source <> 'TIKET' then "IDR"
        else hb.selling_currency
      end as invoice_currency
    , case 
        when oth.room_source <> 'TIKET' then oth.nett_price
        else hbd.total_net_rate_price
      end as sum_net_price
    , hb.currency_exchange_rate as currency_exchange_rate
    , ocd.order_name
    , ocd.order_name_detail
    , case 
        when oth.room_source <> 'TIKET' then oth.booking_checkindate
        else hbd.checkin_date
      end as checkin_date
    , case 
        when oth.room_source <> 'TIKET' then oth.booking_checkoutdate
        else hbd.checkout_date
      end as checkout_date
    , oth.room_source
    , op.payment_source
    , case
        when hp.payment_plan = 'CHECKIN' then hbd.checkin_date
        when hp.payment_plan = 'ONE_DAY_BEFORE_CHECKIN' then date_add(hbd.checkin_date, interval -1 day)
        when hp.payment_plan = 'TWO_DAYS_BEFORE_CHECKIN' then date_add(hbd.checkin_date, interval -2 day)
      end as disbursement_date
    , mrb.account_username as contact_person
    , orf.refund_flag
    , oth.hotel_itineraryNumber as itinerary_id
    , oth.markup_percentage as hotelbeds_markup
    , UPPER(coalesce(bp.payment_status, hps.payment_status)) as tiket_to_hotel_payment_status
    , coalesce(bp.finance_payment_datetime, hps.finance_payment_datetime) as finance_payment_datetime
    , cast(coalesce(bp.finance_payment_amount,hps.finance_payment_amount) as int64) as finance_payment_amount
    , if(coalesce(hp.payment_method, hpi.payment_method) in ('VIRTUAL_CREDIT'), 'credit card', 'non credit card') as cc_tiket_authorize
    , coalesce(hpt.deposit_flag,'non deposit') as deposit_flag
    , bpr.account_type
    , bpr.account_info
    , bpr.account_number
    , bpr.account_owner
    , hmm.country
    , oth.vendor_incentive
    , oth.payment_type as payment_type_to_agoda
  from
    oc
    join ocd using (order_id)
    join op using (order_id)
    left join oth using (order_Id)
    left join hb using (itinerary_id)
    left join hbd using (itinerary_id)
--     left join hr using (room_id)
    left join bpr using (master_id)
    left join mrb using (master_id)
    left join orf using (order_detail_id)
    left join hp using (itinerary_id)
    left join hpi using (master_id)
    left join bp using (order_detail_id)
    left join hps using (itinerary_id)
    left join hpt using (master_id)
    left join hmm using (master_id)
    left join oar using (order_id)
)

select
  order_id
  , order_detail_id
  , payment_timestamp
  , case 
      when difference is null then 'No'
      else "Yes"
      end as is_rebooking_flag 
  , old_order_id
  , difference as difference_amount
  , case
      when difference > 0 then 'Addittional Price'
      when difference < 0 then 'Cashback'
      when difference = 0 then 'No Difference'
      else null
    end as rebooking_status
  , total_customer_price as old_total_customer_price
  --, old_nett_price
  , cast(payment_amount as float64) as payment_amount
  , cast(customer_price as float64) as customer_price
  , customer_currency
  , invoice_currency
  , cast(sum_net_price as float64) as total_invoice
  , cast(sum_net_price as float64) as payment_invoice
  , safe_cast(currency_exchange_rate as float64) as kurs
  , order_name
  , order_name_detail
  , checkin_date
  , checkout_date
  , payment_source
  , contact_person
  , room_source
  , itinerary_id
  , cc_tiket_authorize 
  , deposit_flag 
  , coalesce(refund_flag, 'Not Refund') as refund_flag
  , disbursement_date
  , account_type
  , account_info
  , account_number
  , account_owner
  , coalesce(tiket_to_hotel_payment_status, 'NOT PAID') as tiket_to_hotel_payment_status
  , finance_payment_datetime
  , finance_payment_amount
  , cast(vendor_incentive as int64) as vendor_incentive
  , payment_type_to_agoda
  , country
from
  fact --where order_id in( 104888234,104892368, 104897536, 104901842, 104888250)