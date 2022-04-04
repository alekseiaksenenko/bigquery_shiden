with data as (
    select
        *
         ,cast(json_extract_scalar(data_result, "$.data.block_num") as numeric) as block_num
    from
        `p2p-data-warehouse.raw_data.all_jsons`
    where 1=1
      and chain_name = 'shiden'
      and event_name = 'load_blocks'
    )

   ,data2 as (
    select
        *
         ,lag(block_num) over (order by block_num) as lag_block_num
    from data
    where 1=1
      and block_num is not null
    order by
        block_num desc
       )

select
    count(*)
    -- *
from
    data2
where 1=1
  and lag_block_num + 1 != block_num