with
fd as (
  select
    filter1
    , timestamp_add(filter1, interval -1 day) as filter2
  from 
    (
      select 
        timestamp('2020-10-01') as filter1
    )
)
, oc as (
  select 
    distinct order_id
    , payment_timestamp
    , total_customer_price
  from
  `datamart-finance.staging.v_order__cart` 
  WHERE payment_status = 'paid'
  AND payment_timestamp>= '2020-10-19'  --(select filter1 from fd)
)
, ocd as (
    select
      distinct  order_id
      , order_detail_id
      , order_name_detail
      , customer_currency
      , customer_price
      , order_detail_status
    from
    `datamart-finance.staging.v_order__cart_detail`
    where order_detail_status = 'active'
    and order_type='insurance'
    and created_timestamp >= '2020-10-19' --(select filter1 from fd)
)
, oci as (
    select 
      order_detail_id
      , parent_id
      , customer_currency
      , commission_total
      , issue_code
      , json_extract_scalar(param_json,'$.total_trip.') as total_trip
      , json_extract_scalar(param_json,'$.trip_type.') as trip_type
    from `datamart-finance.staging.v_order__cart_insurance`
    where issue_code like 'tiketflightxfactor%'
)
, ocip as (
    --SELECT order_passenger_id, order_detail_id FROM `datamart-finance.staging.v_order__cart_insurance_pax`  order__cart_insurance_pax
		--UNION ALL
		select 
      order_detail_id 
      , COUNT(order_passenger_id) AS insuranced_pax
    from `datamart-finance.staging.v_order__cart_cancel_insurance_pax` 
    group by 1
)
, ocf as (
    select 
    order_detail_id
    , ticket_status
    , price_total
    , count_adult
    , count_child
    , count_infant
    , sum (count_adult+count_child+count_infant) as total_pax
  from
    `datamart-finance.staging.v_order__cart_flight`
  where departure_time>= (select filter1 from fd)
  group by 1,2,3,4,5,6
)
, fact as ( 
    SELECT 
      oc.order_id
      , ocd.order_name_detail as rincian_pemesanan_pesawat
      , oc.payment_timestamp as tanggal_pembayaran	
      , oc.total_customer_price as total_transaksi
      ,	ocf.price_total as harga_pesawat
      , ocd.order_detail_id as id_asuransi_tiket
      , oci.issue_code as id_asuransi_Cermati 
      , CONCAT(REPLACE (ocd.customer_currency,'IDR', 'RP. '), ocd.customer_price ) as harga_asuransi
      --, ocd.customer_currency as ocd_customer_currency
      --, ocd.customer_price
      , oci.commission_total as Komisi
      , oci.customer_currency as oci_customer_currency
      , oci.trip_type
      , oci.total_trip
      , ocf.ticket_status
      , ocip.insuranced_pax
      , ocf.total_pax
      , ocf.count_adult
      , ocf.count_child
		FROM oc
		LEFT JOIN ocd using (order_id)
		LEFT JOIN oci using (order_detail_id) --ON ocd.order_detail_id=oci.order_detail_id
		LEFT JOIN ocip using (order_detail_id) --ON oci.order_detail_id=ocip.order_detail_id
		LEFT JOIN ocf ON oci.parent_id=ocf.order_detail_id
    --GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17
		--ORDER BY oc.payment_timestamp
)
select * from fact --where order_id in (106294532, 106290653, 106296191, 106295600, 106264757)
