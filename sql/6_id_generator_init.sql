UPDATE id_generator SET id=100000000001;

DELETE FROM user_sessions WHERE deleted_at IS NOT NULL;
ALTER TABLE user_sessions DROP COLUMN deleted_at;
ALTER TABLE user_sessions ADD UNIQUE INDEX uniq (session_id, user_id);

DELETE FROM user_one_time_tokens WHERE deleted_at IS NOT NULL;
ALTER TABLE user_one_time_tokens DROP COLUMN deleted_at;
ALTER TABLE user_one_time_tokens ADD UNIQUE INDEX uniq (token, user_id);
