-- delete from `p2p-data-warehouse.raw_data.all_jsons`
-- where load_guid in (
select count(*) from (
     select
         load_guid
     from (
          select
              load_guid
               ,row_number() over (partition by block_num order by load_timest) as row_num
          from (
               select
                   load_guid
                    ,load_timest
                    ,cast(json_extract_scalar(data_result, "$.data.block_num") as numeric) as block_num
               from
                   `p2p-data-warehouse.raw_data.all_jsons`
               where 1=1
                 and chain_name = 'shiden'
                 and event_name = 'load_blocks'
               ) data
          where 1=1
            and block_num is not null
          ) data2
     where 1=1
       and row_num > 1
    )

