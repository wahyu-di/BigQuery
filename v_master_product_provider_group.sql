with 
  mpp_old as (
    select 
      coalesce(concat('"',safe_cast(product_provider_id as string),'"'),'""') as Organization_Reference_ID
      , coalesce(concat('"',safe_cast(product_provider_name as string),'"'),'""') as Product_Provider_Name
      , coalesce(concat('"',safe_cast(product_category as string),'"'),'""') as Product_Provider_Hierarchy
      , coalesce(concat('"',safe_cast(product_category as string),'"'),'""') as Product_Category
      , processed_date 
    from `datamart-finance.datasource_workday.v_master_product_provider` 
  )
  , mpp_new as (
    select * from `datamart-finance.datamart_edp.v_master_product_provider` 
  )
  , group_mpp as (
    select * from mpp_old
    union all
    select * from mpp_new
  )
  
select * from group_mpp