USE `isucon`;

DROP FUNCTION IF EXISTS check_idx;
CREATE FUNCTION check_idx (user_id BIGINT)
RETURNS BOOLEAN DETERMINISTIC
RETURN user_id % 4 = (CAST(SUBSTRING(@@hostname, 3, 1) AS SIGNED) - 2);

DELETE FROM users WHERE not check_idx(id);
DELETE FROM user_decks WHERE not check_idx(user_id);
DELETE FROM user_devices WHERE not check_idx(user_id);
DELETE FROM user_login_bonuses WHERE not check_idx(user_id);
DELETE FROM user_present_all_received_history WHERE not check_idx(user_id);
DELETE FROM user_presents WHERE not check_idx(user_id);
DELETE FROM user_presents_deleted WHERE not check_idx(user_id);
DELETE FROM user_items WHERE not check_idx(user_id);
DELETE FROM user_cards WHERE not check_idx(user_id);
DELETE FROM user_sessions WHERE not check_idx(user_id);
DELETE FROM user_one_time_tokens WHERE not check_idx(user_id);
