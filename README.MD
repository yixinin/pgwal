### postgre config
- wal_level = logic
- max_replication_slots >= 1
  
* Notice: if publication or slot no longer use, do not forget to clean it.


### REPLICA IDENTITY
- default
  ``` sql
  alter table [tablename] replica identity default;
  -- no replica message if table has no pk
  -- no old doc, and no null value if not changed on update
  -- only pk value on delete
  ```
- full
  ``` sql
  alter table [tablename] replica identity full;
  -- full doc
  ```