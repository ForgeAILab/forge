UPDATE repo
SET work_mode = 'direct_merge'
WHERE work_mode <> 'direct_merge';
