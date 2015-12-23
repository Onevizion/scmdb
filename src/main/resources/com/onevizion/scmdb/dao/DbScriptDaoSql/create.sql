insert into db_script (
  name,
  file_hash,
  text,
  ts,
  output,
  type,
  status)
values (
  :name,
  :fileHash,
  :text,
  :ts,
  :output,
  :type,
  :status
)