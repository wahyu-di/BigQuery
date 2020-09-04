with
-- b2b_corporate - start --
mbc as (
select 
    * 
  from 
    `datamart-finance.datasource_workday.master_b2b_corporate`
)
, vc as (
select
  distinct(workday_business_id) as Customer_Reference_ID
  , REPLACE(company_name,'\n','') as Customer_Name
  , 'B2B_Corporate' as Customer_Category_ID
  ,case
        when Due_Date = 7 then 'NET_7'
        when due_date = 14 then 'NET_14'
        when due_date = 30 then 'NET_30'
        when due_date = 45 then 'NET_45'
        when due_date = 0 then 'NET_14'
        when due_date is null then 'NET_14'
      end as Payment_Terms_ID
  , 'Manual' as Default_Payment_Type_ID
  , 'IDR' as Credit_Limit_Currency
  , credit_limit as Credit_Limit_Amount
  , case
        when npwp <> '' then 'TAX_CODE-6-1'
        when npwp = 'null' then ''
        else ''
        end as Tax_Default_Tax_Code
  , ifnull(replace(npwp,',','.'),'') as Tax_ID_NPWP
  , case 
        when npwp <> '' then 'IDN-NPWP'
        when npwp = 'null' then ''
        else ''
        end as Tax_ID_Type
  ,case 
        when npwp <> '' then 'Y'
        when npwp = 'null' then ''
        else ''
        end as Transaction_Tax_YN
  ,case 
        when npwp <> '' then 'Y'
        when npwp = 'null' then ''
        else ''
        end as Primary_Tax_YN
  ,case
        when address<>'' then '2020-01-01'
        when address='null' then ''
        else ''
        end as Address_Effective_Date
  , case
        when address<>'' then 'ID'
        when address='null' then ''
        else ''
        end as Address_Country_Code
  , REPLACE(address,'\n','') as Address_Line_1
  , '' as Address_Line_2
  , '' as Address_City_Subdivision_2
  , '' as Address_City_Subdivision_1
  , '' as Address_City
  , '' as Address_Region_Subdivision_2
  , '' as Address_Region_Subdivision_1
  , '' as Address_Region_Code
  , '' as Address_Postal_Code
from
  `datamart-finance.staging.v_corporate_account` 
where
  workday_business_id is not null
),
b2b_corp as (
select * EXCEPT(rn)
from (
select 
  vc.*
  , ROW_NUMBER() OVER(PARTITION BY customer_reference_id ORDER BY customer_reference_id) AS rn
  , date(current_timestamp(), 'Asia/Jakarta') as processed_date
from 
  vc 
  left join mbc on vc.customer_reference_id = mbc.business_id 
   where mbc.business_id is null 
   )
  WHERE rn = 1  
  -- b2b corporate - end --
),
-- b2b online and offline - start
fd as (
  select
    filter1
    , timestamp_add(filter1, interval -1 day) as filter2
     ,timestamp_add(filter1, interval 3 day) as filter3 
  from
  (
    select
    --timestamp('2020-08-13 17:00:00') as filter1 #testing
    timestamp_add(timestamp(date(current_timestamp(), 'Asia/Jakarta')), interval -79 hour) as filter1
  )
)
, oc as (
  select
    distinct
    case
      when reseller_type in ('tiket_agent','txtravel','agent','affiliate') then reseller_id
      when reseller_type in ('reseller','widget') then reseller_id
      else null
    end as Customer_Reference_ID
    , case
        when reseller_type in ('tiket_agent','txtravel','agent','affiliate') then 'B2B_Offline'
        when reseller_type in ('reseller','widget') then 'B2B_Online'
        else null
      end as Customer_Category_ID
  from
    `datamart-finance.staging.v_order__cart`
  where
    payment_status = 'paid'
    and payment_timestamp >= (select filter1 from fd)
    and payment_timestamp < (select filter3 from fd)
)
, bp as (
  select
    business_id as Customer_Reference_ID
    , business_name as Customer_Name
    , 'NET_14' as payment_terms_id
    , 'Manual' as default_payment_type_id
    , 'IDR' as credit_limit_currency
    , '' as credit_limit_amount
    , 'TAX_CODE-6-1' as tax_default_tax_code
    , '' as tax_id_npwp
    , '' as tax_id_type -- if npwp true IDN-NPWP
    , '' as transaction_tax_yn -- if npwp true Y
    , '' as primary_tax_yn -- if npwp true Y
    , case
        when business_address1<>'' then '2020-01-01'
        when business_address1='null' then ''
        else ''
        end  as Address_Effective_Date
    , case
        when business_address1<>'' then 'ID'
        when business_address1='null' then ''
        else ''
        end as Address_Country_Code
    , REPLACE(REPLACE(business_address1,'\r',''),'\n','') as address_line_1
    , case
        when business_address1<>'' then business_address2
        when business_address1='null' then ''
        else ''
        end as address_line_2
    , '' as address_city_subdivision_2
    , '' as address_city_subdivision_1
    , ifnull(case
      when business_country = 'Indonesia' then 'ID'
      when business_country = 'Singapore' then 'SG'
      when business_country = 'Malaysia' then 'MY'
      when business_country = 'Thailand' then 'TH'
      when business_country = 'Vietnam' then 'VN'
      when business_country = 'United Kingdom' then 'GB'
      when business_country = 'Australia' then 'AU'
      when business_country = 'Philippines' then 'PH'
      when business_country = 'United States of America' then 'US'
      when business_country = 'Canada' then 'CA'
      when business_country = 'Afghanistan' then 'AF'
      when business_country = 'Albania' then 'AL'
      when business_country = 'Algeria' then 'DZ'
      when business_country = 'American Samoa' then 'AS'
      when business_country = 'Andorra' then 'AD'
      when business_country = 'Angola' then 'AO'
      when business_country = 'Anguilla' then 'AI'
      when business_country = 'Antarctica' then 'AQ'
      when business_country = 'Antigua and Barbuda' then 'AG'
      when business_country = 'Argentina' then 'AR'
      when business_country = 'Armenia' then 'AM'
      when business_country = 'Aruba' then 'AW'
      when business_country = 'Austria' then 'AT'
      when business_country = 'Azerbaijan' then 'AZ'
      when business_country = 'Bahamas' then 'BS'
      when business_country = 'Bahrain' then 'BH'
      when business_country = 'Bangladesh' then 'BD'
      when business_country = 'Barbados' then 'BB'
      when business_country = 'Belarus' then 'BY'
      when business_country = 'Belgium' then 'BE'
      when business_country = 'Belize' then 'BZ'
      when business_country = 'Benin' then 'BJ'
      when business_country = 'Bermuda' then 'BM'
      when business_country = 'Bhutan' then 'BT'
      when business_country = 'Bolivia' then 'BO'
      when business_country = 'Bosnia and Herzegovina' then 'BA'
      when business_country = 'Botswana' then 'BW'
      when business_country = 'Bouvet Island' then 'BV'
      when business_country = 'Brazil' then 'BR'
      when business_country = 'British Indian Ocean Territory' then 'IO'
      when business_country = 'Virgin Islands, British' then 'VG'
      when business_country = 'Brunei Darussalam' then 'BN'
      when business_country = 'Bulgaria' then 'BG'
      when business_country = 'Burkina Faso' then 'BF'
      when business_country = 'Burundi' then 'BI'
      when business_country = 'Cambodia' then 'KH'
      when business_country = 'Cameroon' then 'CM'
      when business_country = 'Cape Verde' then 'CV'
      when business_country = 'Cayman Islands' then 'KY'
      when business_country = 'Central African Republic' then 'CF'
      when business_country = 'Chad' then 'TD'
      when business_country = 'Chile' then 'CL'
      when business_country = 'China' then 'CN'
      when business_country = 'Christmas Island' then 'CX'
      when business_country = 'Cocos (Keeling) Islands' then 'CC'
      when business_country = 'Colombia' then 'CO'
      when business_country = 'Comoros' then 'KM'
      when business_country = 'Congo (Kinshasa)' then 'CD'
      when business_country = 'Congo (Brazzaville)' then 'CG'
      when business_country = 'Cook Islands' then 'CK'
      when business_country = 'Costa Rica' then 'CR'
      when business_country = 'Côte d\'Ivoire' then 'CI'
      when business_country = 'Croatia' then 'HR'
      when business_country = 'Cuba' then 'CU'
      when business_country = 'Cyprus' then 'CY'
      when business_country = 'Czech Republic' then 'CZ'
      when business_country = 'Denmark' then 'DK'
      when business_country = 'Djibouti' then 'DJ'
      when business_country = 'Dominica' then 'DM'
      when business_country = 'Dominican Republic' then 'DO'
      when business_country = 'Ecuador' then 'EC'
      when business_country = 'Egypt' then 'EG'
      when business_country = 'El Salvador' then 'SV'
      when business_country = 'Equatorial Guinea' then 'GQ'
      when business_country = 'Eritrea' then 'ER'
      when business_country = 'Estonia' then 'EE'
      when business_country = 'Ethiopia' then 'ET'
      when business_country = 'Falkland Islands' then 'FK'
      when business_country = 'Faroe Islands' then 'FO'
      when business_country = 'Fiji' then 'FJ'
      when business_country = 'Finland' then 'FI'
      when business_country = 'France' then 'FR'
      when business_country = 'French Guiana' then 'GF'
      when business_country = 'French Polynesia' then 'PF'
      when business_country = 'French Southern Lands' then 'TF'
      when business_country = 'Gabon' then 'GA'
      when business_country = 'Gambia' then 'GM'
      when business_country = 'Georgia' then 'GE'
      when business_country = 'Germany' then 'DE'
      when business_country = 'Ghana' then 'GH'
      when business_country = 'Gibraltar' then 'GI'
      when business_country = 'Greece' then 'GR'
      when business_country = 'Greenland' then 'GL'
      when business_country = 'Grenada' then 'GD'
      when business_country = 'Guadeloupe' then 'GP'
      when business_country = 'Guam' then 'GU'
      when business_country = 'Guatemala' then 'GT'
      when business_country = 'Guernsey' then 'GG'
      when business_country = 'Guinea' then 'GN'
      when business_country = 'Guinea-Bissau' then 'GW'
      when business_country = 'Guyana' then 'GY'
      when business_country = 'Haiti' then 'HT'
      when business_country = 'Heard and McDonald Islands' then 'HM'
      when business_country = 'Vatican City' then 'VA'
      when business_country = 'Honduras' then 'HN'
      when business_country = 'Hong Kong' then 'HK'
      when business_country = 'Hungary' then 'HU'
      when business_country = 'Iceland' then 'IS'
      when business_country = 'India' then 'IN'
      when business_country = 'Iran' then 'IR'
      when business_country = 'Iraq' then 'IQ'
      when business_country = 'Ireland' then 'IE'
      when business_country = 'Isle of Man' then 'IM'
      when business_country = 'Israel' then 'IL'
      when business_country = 'Italy' then 'IT'
      when business_country = 'Jamaica' then 'JM'
      when business_country = 'Japan' then 'JP'
      when business_country = 'Jersey' then 'JE'
      when business_country = 'Jordan' then 'JO'
      when business_country = 'Kazakhstan' then 'KZ'
      when business_country = 'Kenya' then 'KE'
      when business_country = 'Kiribati' then 'KI'
      when business_country = 'Korea, North' then 'KP'
      when business_country = 'Korea, South' then 'KR'
      when business_country = 'Kuwait' then 'KW'
      when business_country = 'Kyrgyzstan' then 'KG'
      when business_country = 'Laos' then 'LA'
      when business_country = 'Latvia' then 'LV'
      when business_country = 'Lebanon' then 'LB'
      when business_country = 'Lesotho' then 'LS'
      when business_country = 'Liberia' then 'LR'
      when business_country = 'Libya' then 'LY'
      when business_country = 'Liechtenstein' then 'LI'
      when business_country = 'Lithuania' then 'LT'
      when business_country = 'Luxembourg' then 'LU'
      when business_country = 'Macau' then 'MO'
      when business_country = 'Macedonia' then 'MK'
      when business_country = 'Madagascar' then 'MG'
      when business_country = 'Malawi' then 'MW'
      when business_country = 'Maldives' then 'MV'
      when business_country = 'Mali' then 'ML'
      when business_country = 'Malta' then 'MT'
      when business_country = 'Marshall Islands' then 'MH'
      when business_country = 'Martinique' then 'MQ'
      when business_country = 'Mauritania' then 'MR'
      when business_country = 'Mauritius' then 'MU'
      when business_country = 'Mayotte' then 'YT'
      when business_country = 'Mexico' then 'MX'
      when business_country = 'Micronesia' then 'FM'
      when business_country = 'Moldova' then 'MD'
      when business_country = 'Monaco' then 'MC'
      when business_country = 'Mongolia' then 'MN'
      when business_country = 'Montenegro' then 'ME'
      when business_country = 'Montserrat' then 'MS'
      when business_country = 'Morocco' then 'MA'
      when business_country = 'Mozambique' then 'MZ'
      when business_country = 'Myanmar' then 'MM'
      when business_country = 'Namibia' then 'NA'
      when business_country = 'Nauru' then 'NR'
      when business_country = 'Nepal' then 'NP'
      when business_country = 'Netherlands' then 'NL'
      when business_country = 'Netherlands Antilles' then 'AN'
      when business_country = 'New Caledonia' then 'NC'
      when business_country = 'New Zealand' then 'NZ'
      when business_country = 'Nicaragua' then 'NI'
      when business_country = 'Niger' then 'NE'
      when business_country = 'Nigeria' then 'NG'
      when business_country = 'Niue' then 'NU'
      when business_country = 'Norfolk Island' then 'NF'
      when business_country = 'Northern Mariana Islands' then 'MP'
      when business_country = 'Norway' then 'NO'
      when business_country = 'Oman' then 'OM'
      when business_country = 'Pakistan' then 'PK'
      when business_country = 'Palau' then 'PW'
      when business_country = 'Palestine' then 'PS'
      when business_country = 'Panama' then 'PA'
      when business_country = 'Papua New Guinea' then 'PG'
      when business_country = 'Paraguay' then 'PY'
      when business_country = 'Peru' then 'PE'
      when business_country = 'Pitcairn' then 'PN'
      when business_country = 'Poland' then 'PL'
      when business_country = 'Portugal' then 'PT'
      when business_country = 'Puerto Rico' then 'PR'
      when business_country = 'Qatar' then 'QA'
      when business_country = 'Reunion' then 'RE'
      when business_country = 'Romania' then 'RO'
      when business_country = 'Russian Federation' then 'RU'
      when business_country = 'Rwanda' then 'RW'
      when business_country = 'Saint Barthélemy' then 'BL'
      when business_country = 'Saint Helena' then 'SH'
      when business_country = 'Saint Kitts and Nevis' then 'KN'
      when business_country = 'Saint Lucia' then 'LC'
      when business_country = 'Saint Martin (French part)' then 'MF'
      when business_country = 'Saint Pierre and Miquelon' then 'PM'
      when business_country = 'Saint Vincent and the Grenadines' then 'VC'
      when business_country = 'Samoa' then 'WS'
      when business_country = 'San Marino' then 'SM'
      when business_country = 'Sao Tome and Principe' then 'ST'
      when business_country = 'Saudi Arabia' then 'SA'
      when business_country = 'Senegal' then 'SN'
      when business_country = 'Serbia' then 'RS'
      when business_country = 'Seychelles' then 'SC'
      when business_country = 'Sierra Leone' then 'SL'
      when business_country = 'Slovakia' then 'SK'
      when business_country = 'Slovenia' then 'SI'
      when business_country = 'Solomon Islands' then 'SB'
      when business_country = 'Somalia' then 'SO'
      when business_country = 'South Africa' then 'ZA'
      when business_country = 'South Georgia and South Sandwich Islands' then 'GS'
      when business_country = 'Spain' then 'ES'
      when business_country = 'Sri Lanka' then 'LK'
      when business_country = 'Sudan' then 'SD'
      when business_country = 'Suriname' then 'SR'
      when business_country = 'Svalbard and Jan Mayen Islands' then 'SJ'
      when business_country = 'Swaziland' then 'SZ'
      when business_country = 'Sweden' then 'SE'
      when business_country = 'Switzerland' then 'CH'
      when business_country = 'Syria' then 'SY'
      when business_country = 'Taiwan' then 'TW'
      when business_country = 'Tajikistan' then 'TJ'
      when business_country = 'Tanzania' then 'TZ'
      when business_country = 'Timor-Leste' then 'TL'
      when business_country = 'Togo' then 'TG'
      when business_country = 'Tokelau' then 'TK'
      when business_country = 'Tonga' then 'TO'
      when business_country = 'Trinidad and Tobago' then 'TT'
      when business_country = 'Tunisia' then 'TN'
      when business_country = 'Turkey' then 'TR'
      when business_country = 'Turkmenistan' then 'TM'
      when business_country = 'Turks and Caicos Islands' then 'TC'
      when business_country = 'Tuvalu' then 'TV'
      when business_country = 'Uganda' then 'UG'
      when business_country = 'Ukraine' then 'UA'
      when business_country = 'United Arab Emirates' then 'AE'
      when business_country = 'United States Minor Outlying Islands' then 'UM'
      when business_country = 'Virgin Islands, U.S.' then 'VI'
      when business_country = 'Uruguay' then 'UY'
      when business_country = 'Uzbekistan' then 'UZ'
      when business_country = 'Vanuatu' then 'VU'
      when business_country = 'Venezuela' then 'VE'
      when business_country = 'Wallis and Futuna Islands' then 'WF'
      when business_country = 'Western Sahara' then 'EH'
      when business_country = 'Yemen' then 'YE'
      when business_country = 'Zambia' then 'ZM'
      when business_country = 'Zimbabwe' then 'ZW'
      when business_country = 'Åland' then 'AX'
      else business_country
      end, '') address_city
    , '' as address_region_subdivision_2
    , '' as address_region_subdivision_1
    , '' as address_region_code
    , case
        when business_address1<>'' then business_zipcode
        when business_address1='null' then ''
        else ''
        end as address_postal_code
  from
    `datamart-finance.staging.v_business__profile` 
)
, fact as (
select
 *
from
  oc
  left join bp using (Customer_Reference_ID)
where
   Customer_Reference_ID is not null
)
, ms as (
  select 
    * 
  from 
    `datamart-finance.datasource_workday.master_b2b_online_and_offline`
),
b2b_online as (
select 
  fact.*
  , date(current_timestamp(), 'Asia/Jakarta') as processed_date 
from 
  fact 
  left join ms on fact.Customer_Reference_ID = ms.business_id 
where 
  ms.business_id is null
),
b2b_group as
(
select 
  coalesce(concat('"',safe_cast(Customer_Reference_ID as string),'"'),'""') as Customer_Reference_ID
  , coalesce(concat('"', safe_cast(Customer_Name as string),'"'), '""') as Customer_Name
  , coalesce(concat('"', safe_cast( Customer_Category_ID as string),'"'), '""') as Customer_Category_ID
  , coalesce(concat('"', safe_cast( Payment_Terms_ID as string),'"'), '""') as Payment_Terms_ID
  , coalesce(concat('"', safe_cast( Default_Payment_Type_ID as string),'"'), '""') as Default_Payment_Type_ID
  , coalesce(concat('"', safe_cast( Credit_Limit_Currency as string),'"'), '""') as Credit_Limit_Currency
  , coalesce(concat('"', safe_cast( Credit_Limit_Amount as string),'"'), '""') as Credit_Limit_Amount
  , coalesce(concat('"', safe_cast( Tax_Default_Tax_Code as string),'"'), '""') as Tax_Default_Tax_Code
  , coalesce(concat('"', safe_cast( Tax_ID_NPWP as string),'"'), '""') as Tax_ID_NPWP
  , coalesce(concat('"', safe_cast( Tax_ID_Type as string),'"'), '""') as Tax_ID_Type
  , coalesce(concat('"', safe_cast( Transaction_Tax_YN as string),'"'), '""') as Transaction_Tax_YN
  , coalesce(concat('"', safe_cast( Primary_Tax_YN as string),'"'), '""') as Primary_Tax_YN
  , coalesce(concat('"', safe_cast( Address_Effective_Date as string),'"'), '""') as Address_Effective_Date
  , coalesce(concat('"', safe_cast( Address_Country_Code as string),'"'), '""') as Address_Country_Code
  , coalesce(concat('"', safe_cast( Address_Line_1 as string),'"'), '""') as Address_Line_1
  , coalesce(concat('"', safe_cast( Address_Line_2 as string),'"'), '""') as Address_Line_2
  , coalesce(concat('"', safe_cast( Address_City_Subdivision_2 as string),'"'), '""') as Address_City_Subdivision_2
  , coalesce(concat('"', safe_cast( Address_City_Subdivision_1 as string),'"'), '""') as Address_City_Subdivision_1
  , coalesce(concat('"', safe_cast( Address_City as string),'"'), '""') as Address_City
  , coalesce(concat('"', safe_cast( Address_Region_Subdivision_2 as string),'"'), '""') as Address_Region_Subdivision_2
  , coalesce(concat('"', safe_cast( Address_Region_Subdivision_1 as string),'"'), '""') as Address_Region_Subdivision_1
  , coalesce(concat('"', safe_cast( Address_Region_Code as string),'"'), '""') as Address_Region_Code
  , coalesce(concat('"', safe_cast( Address_Postal_Code as string),'"'), '""') as Address_Postal_Code
  , coalesce(concat('"', safe_cast( processed_date as string),'"'), '""') as processed_date
from b2b_corp
UNION ALL
select 
  coalesce(concat('"',safe_cast(Customer_Reference_ID as string),'"'),'""') as Customer_Reference_ID
  , coalesce(concat('"', safe_cast(Customer_Name as string),'"'), '""') as Customer_Name
  , coalesce(concat('"', safe_cast( Customer_Category_ID as string),'"'), '""') as Customer_Category_ID
  , coalesce(concat('"', safe_cast( Payment_Terms_ID as string),'"'), '""') as Payment_Terms_ID
  , coalesce(concat('"', safe_cast( Default_Payment_Type_ID as string),'"'), '""') as Default_Payment_Type_ID
  , coalesce(concat('"', safe_cast( Credit_Limit_Currency as string),'"'), '""') as Credit_Limit_Currency
  , coalesce(concat('"', safe_cast( Credit_Limit_Amount as string),'"'), '""') as Credit_Limit_Amount
  , coalesce(concat('"', safe_cast( Tax_Default_Tax_Code as string),'"'), '""') as Tax_Default_Tax_Code
  , coalesce(concat('"', safe_cast( Tax_ID_NPWP as string),'"'), '""') as Tax_ID_NPWP
  , coalesce(concat('"', safe_cast( Tax_ID_Type as string),'"'), '""') as Tax_ID_Type
  , coalesce(concat('"', safe_cast( Transaction_Tax_YN as string),'"'), '""') as Transaction_Tax_YN
  , coalesce(concat('"', safe_cast( Primary_Tax_YN as string),'"'), '""') as Primary_Tax_YN
  , coalesce(concat('"', safe_cast( Address_Effective_Date as string),'"'), '""') as Address_Effective_Date
  , coalesce(concat('"', safe_cast( Address_Country_Code as string),'"'), '""') as Address_Country_Code
  , coalesce(concat('"', safe_cast( Address_Line_1 as string),'"'), '""') as Address_Line_1
  , coalesce(concat('"', safe_cast( Address_Line_2 as string),'"'), '""') as Address_Line_2
  , coalesce(concat('"', safe_cast( Address_City_Subdivision_2 as string),'"'), '""') as Address_City_Subdivision_2
  , coalesce(concat('"', safe_cast( Address_City_Subdivision_1 as string),'"'), '""') as Address_City_Subdivision_1
  , coalesce(concat('"', safe_cast( Address_City as string),'"'), '""') as Address_City
  , coalesce(concat('"', safe_cast( Address_Region_Subdivision_2 as string),'"'), '""') as Address_Region_Subdivision_2
  , coalesce(concat('"', safe_cast( Address_Region_Subdivision_1 as string),'"'), '""') as Address_Region_Subdivision_1
  , coalesce(concat('"', safe_cast( Address_Region_Code as string),'"'), '""') as Address_Region_Code
  , coalesce(concat('"', safe_cast( Address_Postal_Code as string),'"'), '""') as Address_Postal_Code
  , coalesce(concat('"', safe_cast( processed_date as string),'"'), '""') as processed_date
from b2b_online
)
select * from b2b_group