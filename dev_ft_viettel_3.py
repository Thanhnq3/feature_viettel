with invoice as (
  select iv1.col129 as partition_time, iv1.col29 as mst_buyer
      , iv1.col30 as transport
      , iv1.col79 as payment
      , iv1.col33 as buyer_address --- check lai gia tri dia chi
      , sum(case 
            -- HĐ dieu chinh tien
              when iv1.col11 = 2 then coalesce(iv1.col55,0) + coalesce(iv2.col55,0) -- total amount with vat frn
            -- HĐ thay the
              when iv1.col11 = 3 then coalesce(iv2.col55,0) -- total amount with vat frn
              else  coalesce(iv1.col55,0)
              end) as total_sales
      , count(distinct iv1.col32) as number_of_buyer -- dem buyer
      , count(distinct iv1.col72) as number_of_invoice -- dem so hoa don
      , count(distinct iv1.col94) as number_of_sold_item -- dem thong tin parse tu jso col 94
      , count(distinct iv1.col49) as total_discount -- settlement discount amount
      , count(distinct iv1.col47) as total_tax_amt -- check lai xem lay thue nao (tieu thu dac biet, thue truoc/sau VAT..)
      , count(distinct iv1.col72) over(partition by iv1.col30) as count_transport
      , count(distinct iv1.col72) over(partition by iv1.col79) as count_payment
      , count(distinct iv1.col72) over(partition by iv1.col33) as count_buyer_address
  from user_workbench_rmd_priv.hanhtth8.viettel_invoice_sample iv1
  left join user_workbench_rmd_priv.hanhtth8.viettel_invoice_sample iv2
      on iv1.col129 = iv2.col129 -- partition date
        and iv1.col72 = iv2.col1 -- join invoice_id tim hoa don goc
  -- left join thong_tin_dn dc on iv1.partition = dc.partition and iv1.mst = dc.mst -- join bang thong tin DN
  where 1=1
    -- and dc.col56 = 1 -- loc KH DN 
    and iv1.col9 = 1  -- loc HD da pha hanhlọc HĐ đã phát hành
  group by all
)
, dataset as (
  select *
      , row_number() over(partition by transport order by count_transport desc) as rn_transport
      , row_number() over(partition by payment order by count_payment desc) as rn_payment
      , row_number() over(partition by buyer_address order by count_buyer_address desc) as rn_buyer_address
  from invoice
)
select partition_time, mst_buyer
    , sum(total_sales) as total_sales
    , sum(number_of_buyer) as number_of_buyer
    , sum(number_of_invoice) as number_of_invoice
    , sum(number_of_sold_item) as number_of_sold_item
    , sum(total_sales)/sum(number_of_buyer) as total_sales_per_number_buyer
    , sum(total_sales)/sum(number_of_invoice) as total_sales_per_number_invoice
    , sum(total_sales)/sum(number_of_sold_item) as total_sales_per_number_sold_item
    , sum(number_of_sold_item)/sum(number_of_invoice) as number_sold_item_per_number_invoice
    , sum(number_of_sold_item)/sum(number_of_buyer) as number_sold_item_per_number_buyer
    , sum(total_discount) as total_discount
    , sum(total_discount)/sum(total_sales) as discount_per_sales
    , count(distinct transport) as count_transport
    , max(case when rn_transport = 1 then transport end) as most_freq_transport
    , count(distinct payment) as count_payment
    , max(case when rn_payment = 1 then payment end) as most_freq_payment
    , count(distinct buyer_address) as count_buyer_address
    , max(case when rn_buyer_address = 1 then buyer_address end) as most_freq_buyer_address
    , count(distinct count_buyer_address) as count_buyer_address
from dataset
group by all