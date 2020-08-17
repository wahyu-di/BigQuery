with oce as (
  select
    order_detail_id
    , max(order_tiket_number) as quantity_event
    , string_agg(tiket_barcode order by order_detail_id desc, order_tiket_number desc) as ticket_number_event
  from
    `datamart-finance.staging.v_order__cart_event` 
  group by
    order_detail_id
 ), decm as (
  select
    distinct
    detail_id as detail_event_id
    , business_id
    , event_name
    , event_type
    , ext_source
    , event_category
    , supplier_id
    , sellprice
    , sellprice_netto
    , sellprice_adult 
    , sellprice_child 
    , sellprice_infant 
    , sellprice_senior
    , fee_in_price
    , tiket_comission 
    , tax_percent_in_price 
  from
    `datamart-finance.staging.v_detail__event_connect_ms` decm
)
, event_order as /* use this because commission in oecm is not rounded, but floor-ed, example order id 104001549 */
(
  select
    order_id
    , round(sum(commission)) as commission
  from
    (
      select 
        _id
        , safe_cast(coreOrderId as int64) order_id
        , pt.commissionInCents.numberLong/100 commission
        , rank() over(partition by coreorderId, pt.code order by lastModifiedDate desc) rownum
      from 
        `datamart-finance.staging.v_events_v2_order__order_l2` o
        left join unnest (priceTierQuantities) pt
        left join unnest(tickets) tic on /*to get the same pricetierquantities code as the tickets*/
      lower(tic.priceTierCode) = lower(pt.code)
    )
  where
    rownum = 1
  group by 
    1
), oecm as (
  select
    distinct
    order_id
    , order_detail_id
    , event_type
    , ext_source
    , business_id
    , supplier_id
    , event_name
    , event_category
    , is_tiketflexi
    /* , case
      when tiket_comission > 100 then 0
       else round(safe_divide((((qty_adult * sellprice_adult) + (qty_child * sellprice_child) + (qty_infant * sellprice_infant) + (qty_senior * sellprice_senior)) - fee_in_price) * tiket_comission, (100+tax_percent_in_price))) 
       end as commission_event*/
     /*, safe_cast(0 as float64) as commission_event /* requested by Anggi Anggara at 25 April 2020 - Paulus, event commission not calculated in workday*/
     , case 
        when is_tiketflexi = 1 and event_category = 'HOTEL' then coalesce(eo.commission,(coalesce(sellprice,0) - coalesce(sellprice_netto,0))*qty_all)
        else 0
       end as commission_event/* requested by Anggi Anggara at 08 Jul 2020 - Paulus, count commission for tiket flexi */
  from
    `datamart-finance.staging.v_order__event_connect_ms`
    left join decm using (detail_event_id)
    left join event_order eo using (order_id) 
)
, fact_event as (
  select
    order_detail_id
    , quantity_event
    , ticket_number_event
    , 0 as subsidy_event
    , 0 as upselling_event
    , commission_event
    , ext_source as ext_source_event
    , case
        when oecm.is_tiketflexi = 1 and event_category = 'HOTEL' then 'Hotel'
        when oecm.event_name like ('Airport Transfer%') then 'Car'
        when oecm.event_name like ('Tix-Spot Airport Lounge%') then 'Others'
        when lower(oecm.event_name) like ('%railink%') then 'Train'
		    when lower(oecm.event_name) like ('%sewa mobil%') then 'Car' --TTD car 
        when oecm.event_type in ('D') then 'Attraction'
        when oecm.event_type in ('E') then 'Activity'
        when oecm.event_type not in ('D','E') then 'Event'
      end as product_category_event
    , case
        when length(business_id) = 0 then '(blank)'
        when business_id is null then '(null)'
        else business_id
      end as product_provider_event
    , case
        when lower(oecm.event_name) like ('%railink%') then 'VR-00000026'
        when length(supplier_id) = 0 then '(blank)'
        when supplier_id is null then '(null)'
        --else coalesce(msatl.workday_supplier_reference_id,supplier_id)
      end  as supplier_event
    , case
        when oecm.is_tiketflexi = 1 and event_category = 'HOTEL' then 'Hotel_Voucher'
        when oecm.event_name like ('Airport Transfer%') then 'Shuttle'
		    when oecm.event_name like ('Sewa Mobil%') then 'Shuttle' --TTD car 
        when oecm.event_name like ('Tix-Spot Airport Lounge%') then 'Lounge'
        else 'Ticket'
      end as revenue_category_event
  from 
    oce
    inner join oecm using (order_detail_id)
    /*left join master_supplier_airport_transfer_and_lounge msatl 
      on msatl.workday_supplier_name = case
                                        when oecm.event_name like ('Airport Transfer%') then 'Airport Transfer'
                                        when oecm.event_name like ('Tix-Spot Airport Lounge%') then 'Tix-Sport Airport Lounge' 
                                      end*/
)

select * from fact_event where order_detail_id=180659274 