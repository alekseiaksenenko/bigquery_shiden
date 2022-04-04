select
    *
     ,cast(json_extract_scalar(data_result, "$.data.block_num") as numeric) as block_num
from
    `p2p-data-warehouse.raw_data.all_jsons`
where 1=1
  and chain_name = 'shiden'
  and event_name = 'load_blocks'
order by
    block_num desc

