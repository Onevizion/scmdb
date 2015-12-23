update db_script set
  name = :name,
  file_hash = :fileHash,
  text = :text,
  ts = :ts,
  output = :output,
  type = :type,
  status = :status
where db_script_id = :dbScriptId