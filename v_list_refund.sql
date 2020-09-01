select 
    * except (rn)
  from
    (
      select
         order_id
         , order_detail_id
         , DATE(invoice_date) AS invoice_date
         , total_line_amount * currency_conversion as SI_amount
         , supplier_reference_id
         , product_provider 
         , product_category
         , deposit_flag
         , invoice_currency
        , row_number() over(partition by order_id, order_detail_id, spend_category order by processed_timestamp desc) as rn
      from
        `datamart-finance.datasource_workday.supplier_invoice_raw`
      where 
        order_detail_id in (128413446)
    )
  where rn = 1