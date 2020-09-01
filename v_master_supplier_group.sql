with 
 ms_old as (
    select 
      coalesce(concat('"', safe_cast(supplier_id as string),'"'),'""') as Supplier_Reference_ID
      , coalesce(concat('"', safe_cast(supplier_name as string),'"'),'""') as Supplier_Name
      , coalesce(concat('"', safe_cast(product_category as string),'"'),'""') as Supplier_Category_ID
      , coalesce(concat('"', safe_cast("" as string),'"'),'""') as Supplier_Group_ID
      , coalesce(concat('"', safe_cast("" as string),'"'),'""') as Worktag_Product_Provider_Org_Ref_ID
      , coalesce(concat('"', safe_cast(product_category as string),'"'),'""') as Worktag_Product_Category_Ref_ID
      , coalesce(concat('"', safe_cast("" as string),'"'),'""') as Supplier_Default_Currency
      , coalesce(concat('"', safe_cast("Immediate" as string),'"'),'""') as Payment_Terms
      , coalesce(concat('"', safe_cast("Deposit_Deduction" as string),'"'),'""') as Accepted_Payment_Types_1
      , coalesce(concat('"', safe_cast("Credit_Card" as string),'"'),'""') as Accepted_Payment_Types_2
      , coalesce(concat('"', safe_cast("PG_In_Transit" as string),'"'),'""') as Accepted_Payment_Types_3
      , coalesce(concat('"', safe_cast("TT" as string),'"'),'""') as Accepted_Payment_Types_4
      , coalesce(concat('"', safe_cast("" as string),'"'),'""') as Accepted_Payment_Types_5
      , coalesce(concat('"', safe_cast(payment_type as string),'"'),'""') as Default_Payment_Type
      , coalesce(concat('"', safe_cast("" as string),'"'),'""') as Tax_Default_Tax_Code_ID
      , coalesce(concat('"', safe_cast("" as string),'"'),'""') as Tax_Default_Withholding_Tax_Code_ID
      , coalesce(concat('"', safe_cast("" as string),'"'),'""') as Tax_ID_NPWP
      , coalesce(concat('"', safe_cast("" as string),'"'),'""') as Tax_ID_Type
      , coalesce(concat('"', safe_cast("" as string),'"'),'""') as Transaction_Tax_YN
      , coalesce(concat('"', safe_cast("" as string),'"'),'""') as Primary_Tax_YN
      , coalesce(concat('"', safe_cast("2020-01-01" as string),'"'),'""') as Address_Effective_Date
      , coalesce(concat('"', safe_cast("" as string),'"'),'""') as Address_Country_Code
      , coalesce(concat('"', safe_cast(address_name as string),'"'),'""') as Address_Line_1
      , coalesce(concat('"', safe_cast("" as string),'"'),'""') as Address_Line_2
      , coalesce(concat('"', safe_cast("" as string),'"'),'""') as Address_City_Subdivision_2
      , coalesce(concat('"', safe_cast("" as string),'"'),'""') as Address_City_Subdivision_1
      , coalesce(concat('"', safe_cast(city_name as string),'"'),'""') as Address_City
      , coalesce(concat('"', safe_cast("" as string),'"'),'""') as Address_Region_Subdivision_2
      , coalesce(concat('"', safe_cast("" as string),'"'),'""') as Address_Region_Subdivision_1
      , coalesce(concat('"', safe_cast("" as string),'"'),'""') as Address_Region_Code
      , coalesce(concat('"', safe_cast("" as string),'"'),'""') as Address_Postal_Code
      , coalesce(concat('"', safe_cast("" as string),'"'),'""') as Supplier_Bank_Country
      , coalesce(concat('"', safe_cast("" as string),'"'),'""') as Supplier_Bank_Currency
      , coalesce(concat('"', safe_cast("" as string),'"'),'""') as Supplier_Bank_Account_Nickname
      , coalesce(concat('"', safe_cast("" as string),'"'),'""') as Supplier_Bank_Account_Type
      , coalesce(concat('"', safe_cast(hotel_bank_name as string),'"'),'""') as Supplier_Bank_Name
      , coalesce(concat('"', safe_cast("XXX" as string),'"'),'""') as Supplier_Bank_ID_Routing_Number
      , coalesce(concat('"', safe_cast(""  as string),'"'),'""') as Supplier_Bank_Branch_ID
      , coalesce(concat('"', safe_cast(hotel_bank_branch as string),'"'),'""') as Supplier_Bank_Branch_Name
      , coalesce(concat('"', safe_cast(hotel_account_number as string),'"'),'""') as Supplier_Bank_Account_Number
      , coalesce(concat('"', safe_cast(hotel_account_holder_name as string),'"'),'""') as Supplier_Bank_Account_Name
      , coalesce(concat('"', safe_cast("" as string),'"'),'""') as Supplier_Bank_BIC_SWIFT_Code
      , date(current_timestamp(),'Asia/Jakarta') as processed_date 
    from `datamart-finance.datasource_workday.v_master_supplier` 
 )
 , ms_new as (
    select * from `datamart_edp.v_master_supplier` 
 )
 , ms_group as (
    select * from ms_old
    union all
    select * from ms_new
 )
 
select * from ms_group
 