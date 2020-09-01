with
fd as (
  select
    filter1
    , timestamp_add(filter1, interval -1 day) as filter2
     ,timestamp_add(filter1, interval 1 day) as filter3 
  from
  (
    select
     timestamp_add(timestamp(date(current_timestamp(), 'Asia/Jakarta')), interval -79 hour) as filter1
     --timestamp('2020-07-27 17:00:00') as filter1
  )
)
, oc as (
  select
    order_id
    , datetime(payment_timestamp, 'Asia/Jakarta') as payment_datetime
  from
    `datamart-finance.staging.v_order__cart`
  where
    payment_status = 'paid'
    and payment_timestamp >= (select filter1 from fd)
    and payment_timestamp < (select filter3 from fd) 
  group by
    order_id
    , payment_timestamp
)
, ocd as (
  select
    order_id
    , order_detail_id
    , order_type
    , order_name_detail
  from
    `datamart-finance.staging.v_order__cart_detail`
  where
    created_timestamp >= (select filter2 from fd)
    and created_timestamp < (select filter3 from fd)
    and order_type in ('event','car','tixhotel')
    and order_detail_status in ('active','refund','refunded','hide_by_cust')
  group by
    1,2,3,4
)
, oecm as (
  select
    order_id
    , order_detail_id
    , detail_event_id
  from
    `datamart-finance.staging.v_order__event_connect_ms`
  group by
    1,2,3
)
, decm as (
  select
    detail_id as detail_event_id
    , string_agg(distinct supplier_id) as supplier_id
    , string_agg(distinct supplier_name) as supplier_name
    , case
        when string_agg(distinct event_type) in ('D') then 'Attraction'
        when string_agg(distinct event_type) in ('E') then 'Activity'
        when string_agg(distinct event_type) not in ('D','E') then 'Event'
      end as product_category
    , string_agg(distinct ext_source) as ext_source_event
  from
    `datamart-finance.staging.v_detail__event_connect_ms` 
  group by
    1
)
, occar as (
  select
    distinct
    order_detail_id
    , replace(split(split(log_data,'business_id":')[safe_offset(1)],',"')[safe_offset(0)],'"','') as supplier_id
    , replace(split(split(log_data,'business_name":')[safe_offset(1)],',"')[safe_offset(0)],'"','') as supplier_name
    , 'Car' as product_category
  from
    `datamart-finance.staging.v_order__cart_car`
  where
    lastupdate >= (select filter2 from fd)
    and lastupdate < (select filter3 from fd)
)
, hb as (
  select
    safe_cast(id as string) as hotel_itinerarynumber
    , hotel_id as hotel_id_hb
  from
    `datamart-finance.staging.v_hotel_bookings`
)
, hcc as (
  select
    distinct
    _id as city_id
    , string_agg(distinct coalesce(cityName_name, cityName_nameAlias)) as city_name
  from
    `datamart-finance.staging.v_hotel_core_city_flat` 
  where
    cityName_lang = 'en'
    and name_lang = 'en'
  group by
    _id
)
, hcct as (
  select
    distinct
    _id as country_id
    , string_agg(distinct replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(CASE
      WHEN REGEXP_CONTAINS(LOWER(name_name), r"[àáâäåæçèéêëìíîïòóôöøùúûüÿœ]") THEN
        REGEXP_REPLACE(
          REGEXP_REPLACE(
            REGEXP_REPLACE(
              REGEXP_REPLACE(
                REGEXP_REPLACE(
                  REGEXP_REPLACE(
                    REGEXP_REPLACE(
                      REGEXP_REPLACE(
                        REGEXP_REPLACE(
                          REGEXP_REPLACE(
                  REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(name_name, 'œ', 'ce'), 'ÿ', 'y'), 'ç', 'c'), 'æ', 'ae'),'Œ', 'CE'), 'Ÿ', 'Y'), 'Ç', 'C'), 'Æ', 'AE'),
                          r"[ùúûü]", 'u'),
                        r"[òóôöø]", 'o'),
                      r"[ìíîï]", 'i'),
                    r"[èéêë]", 'e'),
                  r"[àáâäå]", 'a'),
                r"[ÙÚÛÜ]", 'U'),
              r"[ÒÓÔÖØ]", 'O'),
            r"[ÌÍÎÏ]", 'I'),
          r"[ÈÉÊË]", 'E'),
        r"[ÀÁÂÄÅ]", 'A')
      ELSE
        (name_name)
      END,'©',''),'®',''),'™',''),'°',''),'–','-'),'‘',"'"),'’',"'"),'“','"'),'”','"'),'¬†','')) as country_name
  from
    `datamart-finance.staging.v_hotel_core_country_flat` 
  where
    name_lang = 'en'
    and countryName_lang = 'en'
  group by
    _id
)
, banks as (
  select
    id as bank_id
    , string_agg(distinct name) as bank_name
  from
    `datamart-finance.staging.v_banks`
  group by
    1
)
, hpi as (
  select
    distinct
    hotel_id as id
    , bank_name
    , bank_branch
    , account_number
    , account_holder_name
    , swift_code
  from
    `datamart-finance.staging.v_hotel_payment_informations` 
    left join banks using (bank_id)
)
, hpt as (
  select
    distinct
    business_id as hotel_id
    , type
  from
    `datamart-finance.staging.v_hotel__payment_type` 
  where
    status = 'active'
)
, htls as (
  select
    id as hotel_id_hb
    , string_agg(distinct coalesce(name,alias)) as hotel_name_hb
    , string_agg(distinct address) as hotel_address_hb
    , string_agg(distinct city_name) as hotel_city_hb
    , string_agg(distinct country_name) as hotel_country_hb
    , string_agg(distinct bank_name) as hotel_bank_name
    , string_agg(distinct bank_branch) as hotel_bank_branch
    , string_agg(distinct account_number) as hotel_account_number
    , string_agg(distinct account_holder_name) as hotel_account_holder_name
    , string_agg(distinct swift_code) as hotel_swift_code
    , string_agg(distinct postal_code) as hotel_postal_code
  from
    `datamart-finance.staging.v_hotels`
    left join hcc using (city_id)
    left join hcct using (country_id)
    left join hpi using (id)
  where
    active_status >= 0
  group by
    1
)
, oth as (
  select
    order_id
    , string_agg(distinct safe_cast(hb.hotel_id_hb as string)) as supplier_id
    , string_agg(distinct htls.hotel_name_hb) as supplier_name
    , string_agg(distinct htls.hotel_address_hb) as address_name
    , string_agg(distinct htls.hotel_city_hb) as city_name
    , string_agg(distinct htls.hotel_country_hb) as country_name
    , string_agg(distinct room_source) as room_source
    , string_agg(distinct hotel_bank_name) as Supplier_Bank_Name
    , string_agg(distinct hotel_bank_branch) as Supplier_Bank_Branch_Name
    , string_agg(distinct hotel_account_number) as Supplier_Bank_Account_Number
    , string_agg(distinct hotel_account_holder_name) as Supplier_Bank_Account_Name
    , string_agg(distinct hotel_swift_code) as Supplier_Bank_BIC_SWIFT_Code
    , string_agg(distinct hotel_postal_code) as Address_Postal_Code
    , 'Hotel' as product_category
  from
    `datamart-finance.staging.v_order__tixhotel` oth
    left join hb using (hotel_itinerarynumber)
    left join htls using (hotel_id_hb)
  where
    created_timestamp >= (select filter2 from fd)
    and created_timestamp < (select filter3 from fd)
    and room_source = 'TIKET'
  group by
    1
)
, combine as (
  select
    distinct
    coalesce(decm.supplier_id, occar.supplier_id, oth.supplier_id) as Supplier_Reference_ID
    , replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(CASE
      WHEN REGEXP_CONTAINS(LOWER(trim(coalesce(decm.supplier_name, occar.supplier_name, oth.supplier_name))), r"[àáâäåæçèéêëìíîïòóôöøùúûüÿœ]") THEN
        REGEXP_REPLACE(
          REGEXP_REPLACE(
            REGEXP_REPLACE(
              REGEXP_REPLACE(
                REGEXP_REPLACE(
                  REGEXP_REPLACE(
                    REGEXP_REPLACE(
                      REGEXP_REPLACE(
                        REGEXP_REPLACE(
                          REGEXP_REPLACE(
                  REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(trim(coalesce(decm.supplier_name, occar.supplier_name, oth.supplier_name)), 'œ', 'ce'), 'ÿ', 'y'), 'ç', 'c'), 'æ', 'ae'),'Œ', 'CE'), 'Ÿ', 'Y'), 'Ç', 'C'), 'Æ', 'AE'),
                          r"[ùúûü]", 'u'),
                        r"[òóôöø]", 'o'),
                      r"[ìíîï]", 'i'),
                    r"[èéêë]", 'e'),
                  r"[àáâäå]", 'a'),
                r"[ÙÚÛÜ]", 'U'),
              r"[ÒÓÔÖØ]", 'O'),
            r"[ÌÍÎÏ]", 'I'),
          r"[ÈÉÊË]", 'E'),
        r"[ÀÁÂÄÅ]", 'A')
      ELSE
        trim(coalesce(decm.supplier_name, occar.supplier_name, oth.supplier_name))
      END,'©',''),'®',''),'™',''),'°',''),'–','-'),'‘',"'"),'’',"'"),'“','"'),'”','"'),'¬†','') AS Supplier_Name
    , coalesce(decm.product_category, occar.product_category, oth.product_category) as Supplier_Category_ID
    , '' Supplier_Group_ID
    , '' Worktag_Product_Provider_Org_Ref_ID
    , coalesce(decm.product_category, occar.product_category, oth.product_category) as Worktag_Product_Category_Ref_ID
    , '' Supplier_Default_Currency
    , 'Immediate' Payment_Terms
    , 'Deposit_Deduction' Accepted_Payment_Types_1
    , 'Credit_Card' Accepted_Payment_Types_2
    , 'PG_In_Transit' Accepted_Payment_Types_3
    , 'TT' Accepted_Payment_Types_4
    , '' Accepted_Payment_Types_5
    , case 
        when string_agg(distinct ocd.order_type) = 'car' then 
          case 
            when string_agg(distinct order_name_detail) like '%EXTRA%' or string_agg(distinct order_name_detail) like '%BEST PRICE%' then 'Deposit_Deduction'
            else 'TT'
          end
        when string_agg(distinct ocd.order_type) = 'event' then
          case
            when string_agg(distinct ext_source_event) = 'BE_MY_GUEST' then 'Deposit_Deduction'
            else 'TT'
          end 
        when string_agg(distinct ocd.order_type) = 'tixhotel' then 
          case
            when string_agg(distinct hpt.type) = 'deposit' then 'Deposit_Deduction'
            when string_agg(distinct hpt.type) = 'creditcard' then 'Credit_Card'
            else 'TT'
          end
      end as Default_Payment_Type
    , '' Tax_Default_Tax_Code_ID
    , '' Tax_Default_Withholding_Tax_Code_ID
    , '' Tax_ID_NPWP
    , '' Tax_ID_Type
    , '' Transaction_Tax_YN
    , '' Primary_Tax_YN
    , format_date("%Y-%m-%d", date(payment_datetime)) Address_Effective_Date
    , ifnull(case
      when country_name = 'Indonesia' then 'ID'
      when country_name = 'Singapore' then 'SG'
      when country_name = 'Malaysia' then 'MY'
      when country_name = 'Thailand' then 'TH'
      when country_name = 'Vietnam' then 'VN'
      when country_name = 'United Kingdom' then 'GB'
      when country_name = 'Australia' then 'AU'
      when country_name = 'Philippines' then 'PH'
      when country_name = 'United States of America' then 'US'
      when country_name = 'Canada' then 'CA'
      when country_name = 'Afghanistan' then 'AF'
      when country_name = 'Albania' then 'AL'
      when country_name = 'Algeria' then 'DZ'
      when country_name = 'American Samoa' then 'AS'
      when country_name = 'Andorra' then 'AD'
      when country_name = 'Angola' then 'AO'
      when country_name = 'Anguilla' then 'AI'
      when country_name = 'Antarctica' then 'AQ'
      when country_name = 'Antigua and Barbuda' then 'AG'
      when country_name = 'Argentina' then 'AR'
      when country_name = 'Armenia' then 'AM'
      when country_name = 'Aruba' then 'AW'
      when country_name = 'Austria' then 'AT'
      when country_name = 'Azerbaijan' then 'AZ'
      when country_name = 'Bahamas' then 'BS'
      when country_name = 'Bahrain' then 'BH'
      when country_name = 'Bangladesh' then 'BD'
      when country_name = 'Barbados' then 'BB'
      when country_name = 'Belarus' then 'BY'
      when country_name = 'Belgium' then 'BE'
      when country_name = 'Belize' then 'BZ'
      when country_name = 'Benin' then 'BJ'
      when country_name = 'Bermuda' then 'BM'
      when country_name = 'Bhutan' then 'BT'
      when country_name = 'Bolivia' then 'BO'
      when country_name = 'Bosnia and Herzegovina' then 'BA'
      when country_name = 'Botswana' then 'BW'
      when country_name = 'Bouvet Island' then 'BV'
      when country_name = 'Brazil' then 'BR'
      when country_name = 'British Indian Ocean Territory' then 'IO'
      when country_name = 'Virgin Islands, British' then 'VG'
      when country_name = 'Brunei Darussalam' then 'BN'
      when country_name = 'Bulgaria' then 'BG'
      when country_name = 'Burkina Faso' then 'BF'
      when country_name = 'Burundi' then 'BI'
      when country_name = 'Cambodia' then 'KH'
      when country_name = 'Cameroon' then 'CM'
      when country_name = 'Cape Verde' then 'CV'
      when country_name = 'Cayman Islands' then 'KY'
      when country_name = 'Central African Republic' then 'CF'
      when country_name = 'Chad' then 'TD'
      when country_name = 'Chile' then 'CL'
      when country_name = 'China' then 'CN'
      when country_name = 'Christmas Island' then 'CX'
      when country_name = 'Cocos (Keeling) Islands' then 'CC'
      when country_name = 'Colombia' then 'CO'
      when country_name = 'Comoros' then 'KM'
      when country_name = 'Congo (Kinshasa)' then 'CD'
      when country_name = 'Congo (Brazzaville)' then 'CG'
      when country_name = 'Cook Islands' then 'CK'
      when country_name = 'Costa Rica' then 'CR'
      when country_name = 'Côte d\'Ivoire' then 'CI'
      when country_name = 'Croatia' then 'HR'
      when country_name = 'Cuba' then 'CU'
      when country_name = 'Cyprus' then 'CY'
      when country_name = 'Czech Republic' then 'CZ'
      when country_name = 'Denmark' then 'DK'
      when country_name = 'Djibouti' then 'DJ'
      when country_name = 'Dominica' then 'DM'
      when country_name = 'Dominican Republic' then 'DO'
      when country_name = 'Ecuador' then 'EC'
      when country_name = 'Egypt' then 'EG'
      when country_name = 'El Salvador' then 'SV'
      when country_name = 'Equatorial Guinea' then 'GQ'
      when country_name = 'Eritrea' then 'ER'
      when country_name = 'Estonia' then 'EE'
      when country_name = 'Ethiopia' then 'ET'
      when country_name = 'Falkland Islands' then 'FK'
      when country_name = 'Faroe Islands' then 'FO'
      when country_name = 'Fiji' then 'FJ'
      when country_name = 'Finland' then 'FI'
      when country_name = 'France' then 'FR'
      when country_name = 'French Guiana' then 'GF'
      when country_name = 'French Polynesia' then 'PF'
      when country_name = 'French Southern Lands' then 'TF'
      when country_name = 'Gabon' then 'GA'
      when country_name = 'Gambia' then 'GM'
      when country_name = 'Georgia' then 'GE'
      when country_name = 'Germany' then 'DE'
      when country_name = 'Ghana' then 'GH'
      when country_name = 'Gibraltar' then 'GI'
      when country_name = 'Greece' then 'GR'
      when country_name = 'Greenland' then 'GL'
      when country_name = 'Grenada' then 'GD'
      when country_name = 'Guadeloupe' then 'GP'
      when country_name = 'Guam' then 'GU'
      when country_name = 'Guatemala' then 'GT'
      when country_name = 'Guernsey' then 'GG'
      when country_name = 'Guinea' then 'GN'
      when country_name = 'Guinea-Bissau' then 'GW'
      when country_name = 'Guyana' then 'GY'
      when country_name = 'Haiti' then 'HT'
      when country_name = 'Heard and McDonald Islands' then 'HM'
      when country_name = 'Vatican City' then 'VA'
      when country_name = 'Honduras' then 'HN'
      when country_name = 'Hong Kong' then 'HK'
      when country_name = 'Hungary' then 'HU'
      when country_name = 'Iceland' then 'IS'
      when country_name = 'India' then 'IN'
      when country_name = 'Iran' then 'IR'
      when country_name = 'Iraq' then 'IQ'
      when country_name = 'Ireland' then 'IE'
      when country_name = 'Isle of Man' then 'IM'
      when country_name = 'Israel' then 'IL'
      when country_name = 'Italy' then 'IT'
      when country_name = 'Jamaica' then 'JM'
      when country_name = 'Japan' then 'JP'
      when country_name = 'Jersey' then 'JE'
      when country_name = 'Jordan' then 'JO'
      when country_name = 'Kazakhstan' then 'KZ'
      when country_name = 'Kenya' then 'KE'
      when country_name = 'Kiribati' then 'KI'
      when country_name = 'Korea, North' then 'KP'
      when country_name = 'Korea, South' then 'KR'
      when country_name = 'Kuwait' then 'KW'
      when country_name = 'Kyrgyzstan' then 'KG'
      when country_name = 'Laos' then 'LA'
      when country_name = 'Latvia' then 'LV'
      when country_name = 'Lebanon' then 'LB'
      when country_name = 'Lesotho' then 'LS'
      when country_name = 'Liberia' then 'LR'
      when country_name = 'Libya' then 'LY'
      when country_name = 'Liechtenstein' then 'LI'
      when country_name = 'Lithuania' then 'LT'
      when country_name = 'Luxembourg' then 'LU'
      when country_name = 'Macau' then 'MO'
      when country_name = 'Macedonia' then 'MK'
      when country_name = 'Madagascar' then 'MG'
      when country_name = 'Malawi' then 'MW'
      when country_name = 'Maldives' then 'MV'
      when country_name = 'Mali' then 'ML'
      when country_name = 'Malta' then 'MT'
      when country_name = 'Marshall Islands' then 'MH'
      when country_name = 'Martinique' then 'MQ'
      when country_name = 'Mauritania' then 'MR'
      when country_name = 'Mauritius' then 'MU'
      when country_name = 'Mayotte' then 'YT'
      when country_name = 'Mexico' then 'MX'
      when country_name = 'Micronesia' then 'FM'
      when country_name = 'Moldova' then 'MD'
      when country_name = 'Monaco' then 'MC'
      when country_name = 'Mongolia' then 'MN'
      when country_name = 'Montenegro' then 'ME'
      when country_name = 'Montserrat' then 'MS'
      when country_name = 'Morocco' then 'MA'
      when country_name = 'Mozambique' then 'MZ'
      when country_name = 'Myanmar' then 'MM'
      when country_name = 'Namibia' then 'NA'
      when country_name = 'Nauru' then 'NR'
      when country_name = 'Nepal' then 'NP'
      when country_name = 'Netherlands' then 'NL'
      when country_name = 'Netherlands Antilles' then 'AN'
      when country_name = 'New Caledonia' then 'NC'
      when country_name = 'New Zealand' then 'NZ'
      when country_name = 'Nicaragua' then 'NI'
      when country_name = 'Niger' then 'NE'
      when country_name = 'Nigeria' then 'NG'
      when country_name = 'Niue' then 'NU'
      when country_name = 'Norfolk Island' then 'NF'
      when country_name = 'Northern Mariana Islands' then 'MP'
      when country_name = 'Norway' then 'NO'
      when country_name = 'Oman' then 'OM'
      when country_name = 'Pakistan' then 'PK'
      when country_name = 'Palau' then 'PW'
      when country_name = 'Palestine' then 'PS'
      when country_name = 'Panama' then 'PA'
      when country_name = 'Papua New Guinea' then 'PG'
      when country_name = 'Paraguay' then 'PY'
      when country_name = 'Peru' then 'PE'
      when country_name = 'Pitcairn' then 'PN'
      when country_name = 'Poland' then 'PL'
      when country_name = 'Portugal' then 'PT'
      when country_name = 'Puerto Rico' then 'PR'
      when country_name = 'Qatar' then 'QA'
      when country_name = 'Reunion' then 'RE'
      when country_name = 'Romania' then 'RO'
      when country_name = 'Russian Federation' then 'RU'
      when country_name = 'Rwanda' then 'RW'
      when country_name = 'Saint Barthélemy' then 'BL'
      when country_name = 'Saint Helena' then 'SH'
      when country_name = 'Saint Kitts and Nevis' then 'KN'
      when country_name = 'Saint Lucia' then 'LC'
      when country_name = 'Saint Martin (French part)' then 'MF'
      when country_name = 'Saint Pierre and Miquelon' then 'PM'
      when country_name = 'Saint Vincent and the Grenadines' then 'VC'
      when country_name = 'Samoa' then 'WS'
      when country_name = 'San Marino' then 'SM'
      when country_name = 'Sao Tome and Principe' then 'ST'
      when country_name = 'Saudi Arabia' then 'SA'
      when country_name = 'Senegal' then 'SN'
      when country_name = 'Serbia' then 'RS'
      when country_name = 'Seychelles' then 'SC'
      when country_name = 'Sierra Leone' then 'SL'
      when country_name = 'Slovakia' then 'SK'
      when country_name = 'Slovenia' then 'SI'
      when country_name = 'Solomon Islands' then 'SB'
      when country_name = 'Somalia' then 'SO'
      when country_name = 'South Africa' then 'ZA'
      when country_name = 'South Georgia and South Sandwich Islands' then 'GS'
      when country_name = 'Spain' then 'ES'
      when country_name = 'Sri Lanka' then 'LK'
      when country_name = 'Sudan' then 'SD'
      when country_name = 'Suriname' then 'SR'
      when country_name = 'Svalbard and Jan Mayen Islands' then 'SJ'
      when country_name = 'Swaziland' then 'SZ'
      when country_name = 'Sweden' then 'SE'
      when country_name = 'Switzerland' then 'CH'
      when country_name = 'Syria' then 'SY'
      when country_name = 'Taiwan' then 'TW'
      when country_name = 'Tajikistan' then 'TJ'
      when country_name = 'Tanzania' then 'TZ'
      when country_name = 'Timor-Leste' then 'TL'
      when country_name = 'Togo' then 'TG'
      when country_name = 'Tokelau' then 'TK'
      when country_name = 'Tonga' then 'TO'
      when country_name = 'Trinidad and Tobago' then 'TT'
      when country_name = 'Tunisia' then 'TN'
      when country_name = 'Turkey' then 'TR'
      when country_name = 'Turkmenistan' then 'TM'
      when country_name = 'Turks and Caicos Islands' then 'TC'
      when country_name = 'Tuvalu' then 'TV'
      when country_name = 'Uganda' then 'UG'
      when country_name = 'Ukraine' then 'UA'
      when country_name = 'United Arab Emirates' then 'AE'
      when country_name = 'United States Minor Outlying Islands' then 'UM'
      when country_name = 'Virgin Islands, U.S.' then 'VI'
      when country_name = 'Uruguay' then 'UY'
      when country_name = 'Uzbekistan' then 'UZ'
      when country_name = 'Vanuatu' then 'VU'
      when country_name = 'Venezuela' then 'VE'
      when country_name = 'Wallis and Futuna Islands' then 'WF'
      when country_name = 'Western Sahara' then 'EH'
      when country_name = 'Yemen' then 'YE'
      when country_name = 'Zambia' then 'ZM'
      when country_name = 'Zimbabwe' then 'ZW'
      when country_name = 'Åland' then 'AX'
      else country_name
      end, '') Address_Country_Code
    , coalesce(REPLACE(address_name,'\n','')) as Address_Line_1
    , '' Address_Line_2
    , '' Address_City_Subdivision_2
    , '' Address_City_Subdivision_1
    , ifnull(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(CASE
      WHEN REGEXP_CONTAINS(LOWER(city_name), r"[àáâäåæçèéêëìíîïòóôöøùúûüÿœ]") THEN
        REGEXP_REPLACE(
          REGEXP_REPLACE(
            REGEXP_REPLACE(
              REGEXP_REPLACE(
                REGEXP_REPLACE(
                  REGEXP_REPLACE(
                    REGEXP_REPLACE(
                      REGEXP_REPLACE(
                        REGEXP_REPLACE(
                          REGEXP_REPLACE(
                  REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(city_name, 'œ', 'ce'), 'ÿ', 'y'), 'ç', 'c'), 'æ', 'ae'),'Œ', 'CE'), 'Ÿ', 'Y'), 'Ç', 'C'), 'Æ', 'AE'),
                          r"[ùúûü]", 'u'),
                        r"[òóôöø]", 'o'),
                      r"[ìíîï]", 'i'),
                    r"[èéêë]", 'e'),
                  r"[àáâäå]", 'a'),
                r"[ÙÚÛÜ]", 'U'),
              r"[ÒÓÔÖØ]", 'O'),
            r"[ÌÍÎÏ]", 'I'),
          r"[ÈÉÊË]", 'E'),
        r"[ÀÁÂÄÅ]", 'A')
      ELSE
        (city_name)
      END,'©',''),'®',''),'™',''),'°',''),'–','-'),'‘',"'"),'’',"'"),'“','"'),'”','"'),'¬†',''), '') AS Address_City
    , '' Address_Region_Subdivision_2
    , '' Address_Region_Subdivision_1
    , '' Address_Region_Code
    , ifnull(Address_Postal_Code, '') Address_Postal_Code
    , case when length(trim(Supplier_Bank_Account_Number)) > 1 then (ifnull(case
      when country_name = 'Indonesia' then 'ID'
      when country_name = 'Singapore' then 'SG'
      when country_name = 'Malaysia' then 'MY'
      when country_name = 'Thailand' then 'TH'
      when country_name = 'Vietnam' then 'VN'
      when country_name = 'United Kingdom' then 'GB'
      when country_name = 'Australia' then 'AU'
      when country_name = 'Philippines' then 'PH'
      when country_name = 'United States of America' then 'US'
      when country_name = 'Canada' then 'CA'
      when country_name = 'Afghanistan' then 'AF'
      when country_name = 'Albania' then 'AL'
      when country_name = 'Algeria' then 'DZ'
      when country_name = 'American Samoa' then 'AS'
      when country_name = 'Andorra' then 'AD'
      when country_name = 'Angola' then 'AO'
      when country_name = 'Anguilla' then 'AI'
      when country_name = 'Antarctica' then 'AQ'
      when country_name = 'Antigua and Barbuda' then 'AG'
      when country_name = 'Argentina' then 'AR'
      when country_name = 'Armenia' then 'AM'
      when country_name = 'Aruba' then 'AW'
      when country_name = 'Austria' then 'AT'
      when country_name = 'Azerbaijan' then 'AZ'
      when country_name = 'Bahamas' then 'BS'
      when country_name = 'Bahrain' then 'BH'
      when country_name = 'Bangladesh' then 'BD'
      when country_name = 'Barbados' then 'BB'
      when country_name = 'Belarus' then 'BY'
      when country_name = 'Belgium' then 'BE'
      when country_name = 'Belize' then 'BZ'
      when country_name = 'Benin' then 'BJ'
      when country_name = 'Bermuda' then 'BM'
      when country_name = 'Bhutan' then 'BT'
      when country_name = 'Bolivia' then 'BO'
      when country_name = 'Bosnia and Herzegovina' then 'BA'
      when country_name = 'Botswana' then 'BW'
      when country_name = 'Bouvet Island' then 'BV'
      when country_name = 'Brazil' then 'BR'
      when country_name = 'British Indian Ocean Territory' then 'IO'
      when country_name = 'Virgin Islands, British' then 'VG'
      when country_name = 'Brunei Darussalam' then 'BN'
      when country_name = 'Bulgaria' then 'BG'
      when country_name = 'Burkina Faso' then 'BF'
      when country_name = 'Burundi' then 'BI'
      when country_name = 'Cambodia' then 'KH'
      when country_name = 'Cameroon' then 'CM'
      when country_name = 'Cape Verde' then 'CV'
      when country_name = 'Cayman Islands' then 'KY'
      when country_name = 'Central African Republic' then 'CF'
      when country_name = 'Chad' then 'TD'
      when country_name = 'Chile' then 'CL'
      when country_name = 'China' then 'CN'
      when country_name = 'Christmas Island' then 'CX'
      when country_name = 'Cocos (Keeling) Islands' then 'CC'
      when country_name = 'Colombia' then 'CO'
      when country_name = 'Comoros' then 'KM'
      when country_name = 'Congo (Kinshasa)' then 'CD'
      when country_name = 'Congo (Brazzaville)' then 'CG'
      when country_name = 'Cook Islands' then 'CK'
      when country_name = 'Costa Rica' then 'CR'
      when country_name = 'Côte d\'Ivoire' then 'CI'
      when country_name = 'Croatia' then 'HR'
      when country_name = 'Cuba' then 'CU'
      when country_name = 'Cyprus' then 'CY'
      when country_name = 'Czech Republic' then 'CZ'
      when country_name = 'Denmark' then 'DK'
      when country_name = 'Djibouti' then 'DJ'
      when country_name = 'Dominica' then 'DM'
      when country_name = 'Dominican Republic' then 'DO'
      when country_name = 'Ecuador' then 'EC'
      when country_name = 'Egypt' then 'EG'
      when country_name = 'El Salvador' then 'SV'
      when country_name = 'Equatorial Guinea' then 'GQ'
      when country_name = 'Eritrea' then 'ER'
      when country_name = 'Estonia' then 'EE'
      when country_name = 'Ethiopia' then 'ET'
      when country_name = 'Falkland Islands' then 'FK'
      when country_name = 'Faroe Islands' then 'FO'
      when country_name = 'Fiji' then 'FJ'
      when country_name = 'Finland' then 'FI'
      when country_name = 'France' then 'FR'
      when country_name = 'French Guiana' then 'GF'
      when country_name = 'French Polynesia' then 'PF'
      when country_name = 'French Southern Lands' then 'TF'
      when country_name = 'Gabon' then 'GA'
      when country_name = 'Gambia' then 'GM'
      when country_name = 'Georgia' then 'GE'
      when country_name = 'Germany' then 'DE'
      when country_name = 'Ghana' then 'GH'
      when country_name = 'Gibraltar' then 'GI'
      when country_name = 'Greece' then 'GR'
      when country_name = 'Greenland' then 'GL'
      when country_name = 'Grenada' then 'GD'
      when country_name = 'Guadeloupe' then 'GP'
      when country_name = 'Guam' then 'GU'
      when country_name = 'Guatemala' then 'GT'
      when country_name = 'Guernsey' then 'GG'
      when country_name = 'Guinea' then 'GN'
      when country_name = 'Guinea-Bissau' then 'GW'
      when country_name = 'Guyana' then 'GY'
      when country_name = 'Haiti' then 'HT'
      when country_name = 'Heard and McDonald Islands' then 'HM'
      when country_name = 'Vatican City' then 'VA'
      when country_name = 'Honduras' then 'HN'
      when country_name = 'Hong Kong' then 'HK'
      when country_name = 'Hungary' then 'HU'
      when country_name = 'Iceland' then 'IS'
      when country_name = 'India' then 'IN'
      when country_name = 'Iran' then 'IR'
      when country_name = 'Iraq' then 'IQ'
      when country_name = 'Ireland' then 'IE'
      when country_name = 'Isle of Man' then 'IM'
      when country_name = 'Israel' then 'IL'
      when country_name = 'Italy' then 'IT'
      when country_name = 'Jamaica' then 'JM'
      when country_name = 'Japan' then 'JP'
      when country_name = 'Jersey' then 'JE'
      when country_name = 'Jordan' then 'JO'
      when country_name = 'Kazakhstan' then 'KZ'
      when country_name = 'Kenya' then 'KE'
      when country_name = 'Kiribati' then 'KI'
      when country_name = 'Korea, North' then 'KP'
      when country_name = 'Korea, South' then 'KR'
      when country_name = 'Kuwait' then 'KW'
      when country_name = 'Kyrgyzstan' then 'KG'
      when country_name = 'Laos' then 'LA'
      when country_name = 'Latvia' then 'LV'
      when country_name = 'Lebanon' then 'LB'
      when country_name = 'Lesotho' then 'LS'
      when country_name = 'Liberia' then 'LR'
      when country_name = 'Libya' then 'LY'
      when country_name = 'Liechtenstein' then 'LI'
      when country_name = 'Lithuania' then 'LT'
      when country_name = 'Luxembourg' then 'LU'
      when country_name = 'Macau' then 'MO'
      when country_name = 'Macedonia' then 'MK'
      when country_name = 'Madagascar' then 'MG'
      when country_name = 'Malawi' then 'MW'
      when country_name = 'Maldives' then 'MV'
      when country_name = 'Mali' then 'ML'
      when country_name = 'Malta' then 'MT'
      when country_name = 'Marshall Islands' then 'MH'
      when country_name = 'Martinique' then 'MQ'
      when country_name = 'Mauritania' then 'MR'
      when country_name = 'Mauritius' then 'MU'
      when country_name = 'Mayotte' then 'YT'
      when country_name = 'Mexico' then 'MX'
      when country_name = 'Micronesia' then 'FM'
      when country_name = 'Moldova' then 'MD'
      when country_name = 'Monaco' then 'MC'
      when country_name = 'Mongolia' then 'MN'
      when country_name = 'Montenegro' then 'ME'
      when country_name = 'Montserrat' then 'MS'
      when country_name = 'Morocco' then 'MA'
      when country_name = 'Mozambique' then 'MZ'
      when country_name = 'Myanmar' then 'MM'
      when country_name = 'Namibia' then 'NA'
      when country_name = 'Nauru' then 'NR'
      when country_name = 'Nepal' then 'NP'
      when country_name = 'Netherlands' then 'NL'
      when country_name = 'Netherlands Antilles' then 'AN'
      when country_name = 'New Caledonia' then 'NC'
      when country_name = 'New Zealand' then 'NZ'
      when country_name = 'Nicaragua' then 'NI'
      when country_name = 'Niger' then 'NE'
      when country_name = 'Nigeria' then 'NG'
      when country_name = 'Niue' then 'NU'
      when country_name = 'Norfolk Island' then 'NF'
      when country_name = 'Northern Mariana Islands' then 'MP'
      when country_name = 'Norway' then 'NO'
      when country_name = 'Oman' then 'OM'
      when country_name = 'Pakistan' then 'PK'
      when country_name = 'Palau' then 'PW'
      when country_name = 'Palestine' then 'PS'
      when country_name = 'Panama' then 'PA'
      when country_name = 'Papua New Guinea' then 'PG'
      when country_name = 'Paraguay' then 'PY'
      when country_name = 'Peru' then 'PE'
      when country_name = 'Pitcairn' then 'PN'
      when country_name = 'Poland' then 'PL'
      when country_name = 'Portugal' then 'PT'
      when country_name = 'Puerto Rico' then 'PR'
      when country_name = 'Qatar' then 'QA'
      when country_name = 'Reunion' then 'RE'
      when country_name = 'Romania' then 'RO'
      when country_name = 'Russian Federation' then 'RU'
      when country_name = 'Rwanda' then 'RW'
      when country_name = 'Saint Barthélemy' then 'BL'
      when country_name = 'Saint Helena' then 'SH'
      when country_name = 'Saint Kitts and Nevis' then 'KN'
      when country_name = 'Saint Lucia' then 'LC'
      when country_name = 'Saint Martin (French part)' then 'MF'
      when country_name = 'Saint Pierre and Miquelon' then 'PM'
      when country_name = 'Saint Vincent and the Grenadines' then 'VC'
      when country_name = 'Samoa' then 'WS'
      when country_name = 'San Marino' then 'SM'
      when country_name = 'Sao Tome and Principe' then 'ST'
      when country_name = 'Saudi Arabia' then 'SA'
      when country_name = 'Senegal' then 'SN'
      when country_name = 'Serbia' then 'RS'
      when country_name = 'Seychelles' then 'SC'
      when country_name = 'Sierra Leone' then 'SL'
      when country_name = 'Slovakia' then 'SK'
      when country_name = 'Slovenia' then 'SI'
      when country_name = 'Solomon Islands' then 'SB'
      when country_name = 'Somalia' then 'SO'
      when country_name = 'South Africa' then 'ZA'
      when country_name = 'South Georgia and South Sandwich Islands' then 'GS'
      when country_name = 'Spain' then 'ES'
      when country_name = 'Sri Lanka' then 'LK'
      when country_name = 'Sudan' then 'SD'
      when country_name = 'Suriname' then 'SR'
      when country_name = 'Svalbard and Jan Mayen Islands' then 'SJ'
      when country_name = 'Swaziland' then 'SZ'
      when country_name = 'Sweden' then 'SE'
      when country_name = 'Switzerland' then 'CH'
      when country_name = 'Syria' then 'SY'
      when country_name = 'Taiwan' then 'TW'
      when country_name = 'Tajikistan' then 'TJ'
      when country_name = 'Tanzania' then 'TZ'
      when country_name = 'Timor-Leste' then 'TL'
      when country_name = 'Togo' then 'TG'
      when country_name = 'Tokelau' then 'TK'
      when country_name = 'Tonga' then 'TO'
      when country_name = 'Trinidad and Tobago' then 'TT'
      when country_name = 'Tunisia' then 'TN'
      when country_name = 'Turkey' then 'TR'
      when country_name = 'Turkmenistan' then 'TM'
      when country_name = 'Turks and Caicos Islands' then 'TC'
      when country_name = 'Tuvalu' then 'TV'
      when country_name = 'Uganda' then 'UG'
      when country_name = 'Ukraine' then 'UA'
      when country_name = 'United Arab Emirates' then 'AE'
      when country_name = 'United States Minor Outlying Islands' then 'UM'
      when country_name = 'Virgin Islands, U.S.' then 'VI'
      when country_name = 'Uruguay' then 'UY'
      when country_name = 'Uzbekistan' then 'UZ'
      when country_name = 'Vanuatu' then 'VU'
      when country_name = 'Venezuela' then 'VE'
      when country_name = 'Wallis and Futuna Islands' then 'WF'
      when country_name = 'Western Sahara' then 'EH'
      when country_name = 'Yemen' then 'YE'
      when country_name = 'Zambia' then 'ZM'
      when country_name = 'Zimbabwe' then 'ZW'
      when country_name = 'Åland' then 'AX'
      else country_name
      end, '')) else '' end Supplier_Bank_Country
    , '' Supplier_Bank_Currency
    , '' Supplier_Bank_Account_Nickname
    , case when length(trim(Supplier_Bank_Account_Number)) > 1
      then 'SA' else '' end Supplier_Bank_Account_Type
    , case when length(trim(Supplier_Bank_Account_Number)) > 1
      then ifnull(Supplier_Bank_Name,'') else '' end Supplier_Bank_Name
    , case when length(trim(Supplier_Bank_Account_Number)) > 1 
      then 'XXX' else '' end Supplier_Bank_ID_Routing_Number
    , '' Supplier_Bank_Branch_ID
    --, room_source
    , case when length(trim(Supplier_Bank_Account_Number)) > 1
      then Supplier_Bank_Branch_Name else '' end Supplier_Bank_Branch_Name
    , case when length(trim(Supplier_Bank_Account_Number)) > 1
      then Supplier_Bank_Account_Number else '' end Supplier_Bank_Account_Number
    , case when length(trim(Supplier_Bank_Account_Number)) > 1
      then Supplier_Bank_Account_Name else '' end Supplier_Bank_Account_Name
    , case when length(trim(Supplier_Bank_Account_Number)) > 1
      then ifnull(Supplier_Bank_BIC_SWIFT_Code, '') else '' end Supplier_Bank_BIC_SWIFT_Code
    , max(payment_datetime) as max_payment_datetime
  from
    oc
    inner join ocd using (order_id)
    left join oth using (order_id)
    left join oecm using (order_detail_id)
    left join decm using (detail_event_id)
    left join occar using (order_detail_id)
    left join hpt on safe_cast(hpt.hotel_id as string) = oth.supplier_id
  where
    coalesce(decm.product_category, occar.product_category, oth.product_category) is not null
  group by 1,2,3,4,5,6,7,8,9,10,11,12,13,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42
)
, add_row_number as (
  select
    *
    , row_number() over(partition by Supplier_Reference_ID order by max_payment_datetime desc) as rn
  from
    combine
)
, fact as (
  select
    * except (max_payment_datetime,rn)
  from
    add_row_number
  where
    rn = 1
    and (
      Supplier_Reference_ID is not null
      and length(Supplier_Reference_ID) > 0
      and Supplier_Reference_ID != '-'
      and Supplier_Reference_ID != '0'
    )
)
, ms as (
  select 
    * 
  from 
    `datamart-finance.datasource_workday.master_supplier`
)

select 
  --, fact.Row
  coalesce(concat('"', safe_cast(fact.Supplier_Reference_ID as string),'"'),'""') as Supplier_Reference_ID
  , coalesce(concat('"', safe_cast(fact.Supplier_Name as string),'"'),'""') as Supplier_Name
  , coalesce(concat('"', safe_cast(fact.Supplier_Category_ID as string),'"'),'""') as Supplier_Category_ID
  , coalesce(concat('"', safe_cast(fact.Supplier_Group_ID as string),'"'),'""') as Supplier_Group_ID
  , coalesce(concat('"', safe_cast(fact.Worktag_Product_Provider_Org_Ref_ID as string),'"'),'""') as Worktag_Product_Provider_Org_Ref_ID
  , coalesce(concat('"', safe_cast(fact.Worktag_Product_Category_Ref_ID as string),'"'),'""') as Worktag_Product_Category_Ref_ID
  , coalesce(concat('"', safe_cast(fact.Supplier_Default_Currency as string),'"'),'""') as Supplier_Default_Currency
  , coalesce(concat('"', safe_cast(fact.Payment_Terms as string),'"'),'""') as Payment_Terms
  , coalesce(concat('"', safe_cast(fact.Accepted_Payment_Types_1 as string),'"'),'""') as Accepted_Payment_Types_1
  , coalesce(concat('"', safe_cast(fact.Accepted_Payment_Types_2 as string),'"'),'""') as Accepted_Payment_Types_2
  , coalesce(concat('"', safe_cast(fact.Accepted_Payment_Types_3 as string),'"'),'""') as Accepted_Payment_Types_3
  , coalesce(concat('"', safe_cast(fact.Accepted_Payment_Types_4 as string),'"'),'""') as Accepted_Payment_Types_4
  , coalesce(concat('"', safe_cast(fact.Accepted_Payment_Types_5 as string),'"'),'""') as Accepted_Payment_Types_5
  , coalesce(concat('"', safe_cast(fact.Default_Payment_Type as string),'"'),'""') as Default_Payment_Type
  , coalesce(concat('"', safe_cast(fact.Tax_Default_Tax_Code_ID as string),'"'),'""') as Tax_Default_Tax_Code_ID
  , coalesce(concat('"', safe_cast(fact.Tax_Default_Withholding_Tax_Code_ID as string),'"'),'""') as Tax_Default_Withholding_Tax_Code_ID
  , coalesce(concat('"', safe_cast(fact.Tax_ID_NPWP as string),'"'),'""') as Tax_ID_NPWP
  , coalesce(concat('"', safe_cast(fact.Tax_ID_Type as string),'"'),'""') as Tax_ID_Type
  , coalesce(concat('"', safe_cast(fact.Transaction_Tax_YN as string),'"'),'""') as Transaction_Tax_YN
  , coalesce(concat('"', safe_cast(fact.Primary_Tax_YN as string),'"'),'""') as Primary_Tax_YN
  , coalesce(concat('"', safe_cast(fact.Address_Effective_Date as string),'"'),'""') as Address_Effective_Date
  , coalesce(concat('"', safe_cast(fact.Address_Country_Code as string),'"'),'""') as Address_Country_Code
  , coalesce(concat('"', safe_cast(fact.Address_Line_1 as string),'"'),'""') as Address_Line_1
  , coalesce(concat('"', safe_cast(fact.Address_Line_2 as string),'"'),'""') as Address_Line_2
  , coalesce(concat('"', safe_cast(fact.Address_City_Subdivision_2 as string),'"'),'""') as Address_City_Subdivision_2
  , coalesce(concat('"', safe_cast(fact.Address_City_Subdivision_1 as string),'"'),'""') as Address_City_Subdivision_1
  , coalesce(concat('"', safe_cast(fact.Address_City as string),'"'),'""') as Address_City
  , coalesce(concat('"', safe_cast(fact.Address_Region_Subdivision_2 as string),'"'),'""') as Address_Region_Subdivision_2
  , coalesce(concat('"', safe_cast(fact.Address_Region_Subdivision_1 as string),'"'),'""') as Address_Region_Subdivision_1
  , coalesce(concat('"', safe_cast(fact.Address_Region_Code as string),'"'),'""') as Address_Region_Code
  , coalesce(concat('"', safe_cast(fact.Address_Postal_Code as string),'"'),'""') as Address_Postal_Code
  , coalesce(concat('"', safe_cast(fact.Supplier_Bank_Country as string),'"'),'""') as Supplier_Bank_Country
  , coalesce(concat('"', safe_cast(fact.Supplier_Bank_Currency as string),'"'),'""') as Supplier_Bank_Currency
  , coalesce(concat('"', safe_cast(fact.Supplier_Bank_Account_Nickname as string),'"'),'""') as Supplier_Bank_Account_Nickname
  , coalesce(concat('"', safe_cast(fact.Supplier_Bank_Account_Type as string),'"'),'""') as Supplier_Bank_Account_Type
  , coalesce(concat('"', safe_cast(fact.Supplier_Bank_Name as string),'"'),'""') as Supplier_Bank_Name
  , coalesce(concat('"', safe_cast(fact.Supplier_Bank_ID_Routing_Number as string),'"'),'""') as Supplier_Bank_ID_Routing_Number
  , coalesce(concat('"', safe_cast(fact.Supplier_Bank_Branch_ID as string),'"'),'""') as Supplier_Bank_Branch_ID
  , coalesce(concat('"', safe_cast(fact.Supplier_Bank_Branch_Name as string),'"'),'""') as Supplier_Bank_Branch_Name
  , coalesce(concat('"', safe_cast(fact.Supplier_Bank_Account_Number as string),'"'),'""') as Supplier_Bank_Account_Number
  , coalesce(concat('"', safe_cast(fact.Supplier_Bank_Account_Name as string),'"'),'""') as Supplier_Bank_Account_Name
  , coalesce(concat('"', safe_cast(fact.Supplier_Bank_BIC_SWIFT_Code as string),'"'),'""') as Supplier_Bank_BIC_SWIFT_Code
  , date(current_timestamp(),'Asia/Jakarta') as processed_date 
from 
  fact 
  left join ms on fact.Supplier_Reference_ID = ms.supplier_id 
--where 
--ms.supplier_id is null
--ms.supplier_id = '105831'
--ms.supplier_id is null