delete from `p2p-data-warehouse.raw_data.all_jsons`
where load_guid in (
    select load_guid
    from `p2p-data-warehouse.raw_data.all_jsons`
    where 1 = 1
      and chain_name = 'shiden'
      and event_name = 'load_blocks'
      and json_extract_scalar(data_result, "$.message") = 'API rate limit exceeded'
    )