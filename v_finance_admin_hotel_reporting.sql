WITH
  oc AS (
  SELECT
    order_id,
    payment_timestamp,
    customer_currency,
    CASE
      WHEN cs_id > 0 THEN 'CS'
      WHEN reseller_id > 0
    AND reseller_type IN ('tiket_agent',
      'txtravel') THEN 'B2B OFF'
      WHEN reseller_id > 0 AND reseller_type NOT IN ('online_marketing',  'native_apps',  'tiket_agent',  'txtravel') THEN 'B2B ON'
      ELSE 'B2C'
    END AS sell_type,
    reseller_id,
    reseller_type,
    total_customer_price,
    cs_id
  FROM
    `datamart-finance.staging.v_order__cart` 
  WHERE
    --payment_timestamp >= '2017-12-31 17:00:00'
    payment_timestamp >= '2020-09-10 17:00:00'
    --AND payment_timestamp < '2020-09-30 17:00:00'
    AND payment_status = 'paid' ),
  ocd AS (
  SELECT
    order_id,
    order_detail_id,
    order_type,
    order_name,
    order_name_detail,
    customer_price,
    selling_price,
    order_detail_status
  FROM
    `datamart-finance.staging.v_order__cart_detail` 
  WHERE
    order_detail_status IN ('active',
      'refund',
      'refunded','hide_by_cust')
    --AND created_timestamp >= '2017-12-29 17:00:00'
     AND created_timestamp >= '2020-09-10 17:00:00'
  ),
  ocdh AS (
  SELECT
    order_id,
    order_detail_id,
    order_name,
    order_type,
    comission,
    order_detail_status
  FROM
    `datamart-finance.staging.v_order__cart_detail` 
  WHERE
    order_type IN ('hotel',
      'tixhotel')
    AND order_detail_status IN ('active',
      'refund',
      'refunded','hide_by_cust')
    --AND created_timestamp >= '2017-12-29 17:00:00'
    AND created_timestamp >= '2020-09-10 17:00:00'
  ),
  op AS(
  SELECT
    order_id
  FROM
    `datamart-finance.staging.v_order__payment` 
  WHERE
    payment_flag = 1
    AND payment_id = 1
    --AND payment_timestamp >= '2017-12-29 17:00:00'
    AND payment_timestamp >= '2020-09-10 17:00:00'
  ),
  och AS (
  SELECT
    order_detail_id,
    checkin_date,
    rooms,
    room_id,
    customer_price,
    net_rate_price,
    sell_rate_price,
    sub_price_idr,
    net_rate_currency,
    order_detail_status
  FROM
    `datamart-finance.staging.v_order__cart_hotel` 
  WHERE
    --last_update >= '2017-12-29 17:00:00'
    last_update >= '2020-09-10 17:00:00'
    AND order_detail_status IN ('active',
      'refund',
      'refunded','hide_by_cust')),
  ott AS (
  SELECT
    order_id,
    booking_checkindate,
    booking_checkoutdate,
    booking_room,
    room_source,
    totalwithtax,
    booking_night,
    totalwithtax/booking_room/booking_night AS room_price,
    nett_price,
    subsidy_price,
    provinceName,
    countryName,
    hotel_itinerarynumber
  FROM
    `datamart-finance.staging.v_order__tixhotel` 
  WHERE
    created_timestamp >= '2017-12-29 17:00:00'),
 hb_new AS (
  SELECT
    distinct
    id as itinerary_id,
    commission,
    hotel_id as master_id,
    booking_status
  FROM
    `datamart-finance.staging.v_hotel_bookings` 
  WHERE
    updated_date >= '2017-12-29 17:00:00'
    AND booking_status IN ('issued',
      'refund')),
  /*hb_old AS (
  SELECT
    itinerary_id,
    commission,
    master_id,
    booking_status
  FROM
    `prod-datarangers.galaxy_stg.hotel__booking`
  WHERE
    created_timestamp >= '2017-12-29 17:00:00'
    AND booking_status IN ('issued',
      'refund')
    AND itinerary_id not in (select itinerary_id from hb_new)
  ),*/
  hb as (
    select * from hb_new
    --union distinct
    --select * from hb_old
  ),
  hbd_new AS (
  SELECT
    itinerary_id,
    net_rate_currency,
    MAX( IF( rn = 1, customer_price,0)) as customer_price,
    SUM( sub_price_IDR) sub_price_IDR,
    SUM( total_net_rate_price) total_net_rate_price,
    SUM( total_sell_rate_price) total_sell_rate_price,
    SUM( total_customer_price) total_customer_price
  FROM(
    SELECT
      itinerary_id,
      net_rate_currency,
      customer_price,
      case
        when created_date < '2019-06-19 17:00:00' then (subsidy_price*rooms)
        else subsidy_price
      end as sub_price_IDR,
      total_net_rate_price,
      total_sell_rate_price,
      case
        when created_date < '2019-06-19 17:00:00' then total_customer_price-(subsidy_price*rooms)
        when created_date < '2019-07-03 15:23:41' then total_customer_price-subsidy_price
        else total_customer_price
      end as total_customer_price,
      row_number() over(partition by itinerary_id order by checkin_date asc) as rn
    FROM
      `datamart-finance.staging.v_hotel_booking_details` 
    WHERE
      checkin_date >= '2017-12-29 17:00:00'
    order by itinerary_id, checkin_date asc
  ) table1
  GROUP BY
    itinerary_id,
    net_rate_currency 
  ),
  /*hbd_old AS (
  SELECT
    itinerary_id,
    net_rate_currency,
    MAX( IF( rn = 1, customer_price,0)) as customer_price,
    SUM( sub_price_IDR) sub_price_IDR,
    SUM( total_net_rate_price) total_net_rate_price,
    SUM( total_sell_rate_price) total_sell_rate_price,
    SUM( total_customer_price ) total_customer_price
  FROM(
    SELECT
      itinerary_id,
      room_id,
      net_rate_currency,
      customer_price,
      sub_price_IDR * rooms as sub_price_IDR,
      total_net_rate_price,
      total_sell_rate_price,
      total_customer_price - (sub_price_idr * rooms) as total_customer_price,
      row_number() over(partition by itinerary_id order by checkin_date asc) as rn
    FROM
      --`prod-datarangers.galaxy_stg.hotel__booking_detail`
      `datamart-finance.staging.v_hotel_
    WHERE
      booking_detail_status='active'
      AND checkin_date >= '2017-12-29 17:00:00'
    order by itinerary_id, checkin_date asc
  ) table1
  GROUP BY
    itinerary_id,
    room_id,
    net_rate_currency 
  ),*/
  hbd as (
    select * from hbd_new
    --union distinct
    --select * from hbd_old
  ),
  bp AS (
  SELECT
    business_id,
    business_name,
    business_province
  FROM
    `datamart-finance.staging.v_business__profile` ),
  hr AS (
  SELECT
    room_id,
    ext_source,
    master_id
  FROM
    `datamart-finance.staging.v_hotel__room` ),
  ap AS (
  SELECT
    province_id,
    province_name,
    province_country_id
  FROM
    `datamart-finance.staging.v_address__province` ),
  ac AS (
  SELECT
    country_id,
    country_name
  FROM
    `datamart-finance.staging.v_address__country` ),
  bpay as (
    select
      order_detail_id
      , upper(payment_status) as payment_status
    from
      `datamart-finance.staging.v_business__payment` 
  )
, hps as (
  select
    itinerary_id
    , string_agg(distinct payment_status) as payment_status
  from
    `datamart-finance.staging.v_hotel_payments` 
  where 
    created_date >= '2017-12-30 17:00:00'
    and payment_status = 'PAID'
  group by
    itinerary_id
)
,   -- @wahyu 17 September 2020
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
  SELECT
  ocdh.order_id,
  ocdh.order_detail_id,
  oar.old_order_id,
  oar.new_total_customer_price-oar.total_customer_price as difference,
  oar.total_customer_price,
  Datetime ( oc.payment_timestamp, 
  'Asia/Jakarta') AS payment_timestamp,
  ocdh.order_type AS category,
  oc.customer_currency AS currency,
  STRING_AGG(distinct hb.booking_status) as booking_status,
  STRING_AGG(distinct ocdh.order_name) AS hotel_name,
  STRING_AGG(distinct ott.hotel_itineraryNumber) as itin_number,
  STRING_AGG(
    CASE
      WHEN ocd.order_type = 'payment' THEN ocd.order_name
      ELSE NULL END) AS payment_source,
  CASE
    WHEN ocdh.order_type = 'hotel' THEN DATE(datetime(MIN(och.checkin_date),  'Asia/Jakarta'))
    WHEN ocdh.order_type = 'tixhotel' THEN DATE(datetime(MIN(ott.booking_checkinDate),
      'Asia/Jakarta'))
    ELSE NULL
  END AS check_in,
  CASE
    WHEN ocdh.order_type = 'hotel' THEN DATE_ADD(DATE(datetime(MAX(och.checkin_date),  'Asia/Jakarta')),INTERVAL 1 day)
    WHEN ocdh.order_type = 'tixhotel' THEN DATE(datetime(MAX(ott.booking_checkOutDate),
      'Asia/Jakarta'))
    ELSE NULL
  END AS check_out,
  CASE
    WHEN ocdh.order_type = 'hotel' THEN MAX(och.rooms)
    WHEN ocdh.order_type = 'tixhotel' THEN MAX(ott.booking_room)
    ELSE NULL
  END AS rooms,
  CASE
    WHEN ocdh.order_type = 'hotel' THEN MAX(och.customer_price)
    WHEN ocdh.order_type = 'tixhotel'
  AND STRING_AGG(DISTINCT ott.room_source)='TIKET' THEN MAX(hbd.customer_price)
    WHEN ocdh.order_type = 'tixhotel' AND STRING_AGG(DISTINCT ott.room_source)<>'TIKET' THEN MAX(ott.totalWithTax)
    ELSE 0
  END AS room_price,
  CASE
    WHEN ocdh.order_type = 'hotel' THEN MAX(och.customer_price * och.rooms)
    WHEN ocdh.order_type = 'tixhotel'
  AND STRING_AGG(DISTINCT ott.room_source)='TIKET' THEN MAX(hbd.total_customer_price)
    WHEN ocdh.order_type = 'tixhotel' AND STRING_AGG(DISTINCT ott.room_source)<>'TIKET' THEN MAX(ott.totalWithTax)
    ELSE 0
  END AS base_price,
  CASE
    WHEN ocdh.order_type = 'hotel' THEN COUNT(DISTINCT(och.checkin_date))
    WHEN ocdh.order_type = 'tixhotel' THEN MAX(ott.booking_night)
    ELSE NULL
  END AS duration,
  SUM(
    CASE
      WHEN ocd.order_type = 'giftcard' THEN ocd.selling_price
      ELSE 0
    END ) AS giftcard,
  SUM(
    CASE
      WHEN ocd.order_type = 'promocode' THEN ocd.selling_price
      ELSE 0
    END ) AS promocode,
  SUM(
    CASE
      WHEN ocd.order_type = 'tiketpoint' THEN ocd.selling_price
      ELSE 0 END) AS tiketpoint,
  SUM(
    CASE
      WHEN ocd.order_type = 'payment' THEN ocd.selling_price
      ELSE 0
    END ) AS payment_charge,
  MAX(oc.total_customer_price) AS customer_price,
  CASE
    WHEN ocdh.order_type='hotel' THEN SUM(IF(ocdh.order_detail_id = ocd.order_detail_id, och.customer_price * rooms, 0)) 
    WHEN ocdh.order_type='tixhotel' AND STRING_AGG(DISTINCT ott.room_source)='TIKET' THEN MAX(hbd.total_customer_price)
    WHEN ocdh.order_type='tixhotel' AND STRING_AGG(DISTINCT ott.room_source)<>'TIKET' THEN MAX(ott.totalWithTax)
    ELSE 0 
    END AS hotel_price, 
  CASE
    WHEN ocdh.order_type='hotel' THEN ROUND(SUM(IF(och.sell_rate_price=0,0,och.customer_price * och.rooms * och.net_rate_price / och.sell_rate_price)),0)
    WHEN ocdh.order_type='tixhotel'
  AND STRING_AGG(DISTINCT ott.room_source)='TIKET' THEN ROUND(MAX(hbd.total_net_rate_price),0)
    WHEN ocdh.order_type='tixhotel' AND STRING_AGG(DISTINCT ott.room_source)<>'TIKET' THEN MAX(IF(ott.nett_price>0,  ott.nett_price,  ott.totalWithTax))
    ELSE 0
  END AS cogs_hotel,
  CASE
    WHEN ocdh.order_type='hotel' THEN SUM(IF(ocdh.order_detail_id = ocd.order_detail_id,  och.sub_price_idr,  0))
    WHEN ocdh.order_type='tixhotel'
  AND STRING_AGG(DISTINCT ott.room_source)='TIKET' THEN MAX(hbd.sub_price_IDR)
    WHEN ocdh.order_type='tixhotel' AND STRING_AGG(DISTINCT ott.room_source)<>'TIKET' THEN MAX(IF(ott.subsidy_price IS NULL,  0,  ott.subsidy_price))
    ELSE 0
  END AS subsidy_hotel,
  CASE
    WHEN ocdh.order_type='hotel' THEN MAX(ocdh.comission)
    WHEN ocdh.order_type='tixhotel'
  AND STRING_AGG(DISTINCT ott.room_source)='TIKET' THEN MAX(hb.commission)
    WHEN ocdh.order_type='tixhotel' AND STRING_AGG(DISTINCT ott.room_source)<>'TIKET' THEN MAX(ocdh.comission)
    ELSE 0
  END AS comission_percentage,
  CASE
    WHEN STRING_AGG(ocdh.order_detail_status) IN ('refund', 'refunded') THEN 'Yes'
    ELSE NULL
  END AS refund,
  IF( MAX(oc.reseller_id) = 0,
    'TiketCom Web',
    STRING_AGG(DISTINCT bp.business_name)) AS source,
  CASE
    WHEN ocdh.order_type='hotel' THEN STRING_AGG(DISTINCT ap.province_name)
    WHEN ocdh.order_type='tixhotel' THEN STRING_AGG(DISTINCT ott.provinceName)
    ELSE NULL
  END AS hotel_province,
  CASE
    WHEN ocdh.order_type='hotel' THEN STRING_AGG(DISTINCT ac.country_name)
    WHEN ocdh.order_type='tixhotel' THEN STRING_AGG(DISTINCT ott.countryName)
    ELSE NULL
  END AS hotel_country,
  (CASE
      WHEN MAX(oc.cs_id) > 0 THEN 'CS'
      WHEN MAX(oc.reseller_id) > 0
    AND STRING_AGG(DISTINCT oc.reseller_type) IN ('tiket_agent',
      'txtravel') THEN 'B2B OFF'
      WHEN MAX(oc.reseller_id) > 0 AND STRING_AGG(DISTINCT oc.reseller_type) NOT IN ('online_marketing', 'native_apps', 'tiket_agent', 'txtravel') THEN 'B2B ON'
      ELSE 'B2C'
    END )AS customer_type,
  CASE
    WHEN ocdh.order_type='hotel' THEN MAX(hr.ext_source)
    WHEN ocdh.order_type='tixhotel' THEN LOWER(IF( STRING_AGG(DISTINCT ott.room_source)='TIKET',
      'native',
      STRING_AGG(DISTINCT ott.room_source)))
    ELSE NULL
  END AS ext_source,
  CASE
    WHEN ocdh.order_type='hotel' THEN MAX(hr.master_id)
    WHEN ocdh.order_type='tixhotel' THEN MAX(hb.master_id)
    ELSE NULL
  END AS hotel_id,
  CASE
    WHEN ocdh.order_type = 'hotel' THEN MAX(och.net_rate_currency)
    WHEN ocdh.order_type = 'tixhotel' THEN MAX(hbd.net_rate_currency)
    ELSE NULL
  END AS net_currency,
  CASE
    WHEN ocdh.order_type = 'hotel' THEN SUM(och.net_rate_price*rooms)
    WHEN ocdh.order_type = 'tixhotel' THEN MAX(hbd.total_net_rate_price)
    ELSE 0
  END AS total_net_rate,
  CASE
    WHEN ocdh.order_type = 'hotel' THEN SUM(sell_rate_price*rooms)
    WHEN ocdh.order_type = 'tixhotel' THEN MAX(hbd.total_sell_rate_price)
    ELSE 0
  END AS total_sell_rate,
  STRING_AGG(distinct coalesce(bpay.payment_status,hps.payment_status)) as tiket_payment_status
  , string_agg(distinct ott.hotel_itinerarynumber) as itinerary_id
FROM
  oc
JOIN
  op
ON
  oc.order_id = op.order_id
JOIN
  ocdh
ON
  oc.order_id = ocdh.order_id
JOIN
  ocd
ON
  oc.order_id = ocd.order_id
LEFT JOIN
  och
ON
  ocdh.order_detail_id = och.order_detail_id
  AND ocdh.order_detail_id = ocd.order_detail_id
LEFT JOIN
  ott
ON
  ott.order_id = ocdh.order_id
LEFT JOIN
  hb
ON
  CAST(hb.itinerary_id AS string) = ott.hotel_itineraryNumber
LEFT JOIN
  hbd
ON
  CAST(hbd.itinerary_id AS string) = ott.hotel_itineraryNumber
LEFT JOIN
  bp
ON
  oc.reseller_id = bp.business_id
LEFT JOIN
  hr
ON
  och.room_id = hr.room_id
LEFT JOIN
  ap
ON
  bp.business_province = ap.province_id
LEFT JOIN
  ac
ON
  ap.province_country_id = ac.country_id
LEFT JOIN
  bpay
on bpay.order_detail_id = ocdh.order_detail_id
LEFT JOIN
  hps
on safe_cast(hps.itinerary_id as string) = ott.hotel_itinerarynumber 
left join oar on  oar.order_id=oc.order_id
GROUP BY
  1,
  2,
  3,
  4,
  5,
  6,
  7,
  8
)

select 
  order_id,
  order_detail_id,
  itinerary_id,
  hotel_id,
  hotel_name,
  hotel_province,
  hotel_country,
  ext_source,
  payment_timestamp,
  payment_source,
  check_in,
  check_out,
  duration,
  rooms,
  SAFE_CAST(room_price as float64) as room_price,
  SAFE_CAST(hotel_price as float64) as hotel_price,
  SAFE_CAST(subsidy_hotel as float64) as subsidy_hotel,
  SAFE_CAST(hotel_price + subsidy_hotel as float64) as revenue,
  SAFE_CAST(cogs_hotel as float64) as cogs_hotel,
  SAFE_CAST(comission_percentage as float64) as comission_percentage,
  SAFE_CAST(subsidy_hotel - ( cogs_hotel - hotel_price ) as float64) as comission,
  SAFE_CAST(ABS(giftcard) as float64) as giftcard,
  SAFE_CAST(ABS(promocode) as float64) as promocode,
  SAFE_CAST(ABS(tiketpoint) as float64) as tiketpoint,
  SAFE_CAST(payment_charge as float64) as payment_charge,
  SAFE_CAST(customer_price as float64) as customer_price,
  currency,
  refund,
  customer_type,
  source,
  net_currency,
  SAFE_CAST(total_net_rate as float64) as total_net_rate,
  SAFE_CAST(total_sell_rate as float64) as total_sell_rate,
  tiket_payment_status
  , case 
      when difference is null then 'No'
      else "Yes"
      end as is_rebooking_flag 
  , old_order_id
  , safe_cast( difference as float64 ) as difference_amount
  --, difference as difference_amount
  , case
      when difference > 0 then 'Addittional Price'
      when difference < 0 then 'Cashback'
      when difference = 0 then 'No Difference'
      else null
    end as rebooking_status
  , safe_cast( total_customer_price  as float64 ) as old_total_customer_price
from 
  fact 
where  
  order_id in( 105479533,105479604, 105481091 ) 
  /*category = 'hotel' 
    or 
  (category = 'tixhotel' and itin_number is not null)
  and */
  