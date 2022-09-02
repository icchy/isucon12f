USE `isucon`;

DROP TABLE IF EXISTS user_presents_deleted;

CREATE TABLE user_presents_deleted (
  `id` bigint NOT NULL,
  `user_id` bigint NOT NULL comment 'ユーザID',
  `sent_at` bigint NOT NULL comment 'プレゼント送付日時',
  `item_type` int(1) NOT NULL comment 'アイテム種別',
  `item_id` int NOT NULL comment 'アイテムID',
  `amount` int NOT NULL comment 'アイテム数',
  `present_message` varchar(255) comment 'プレゼントメッセージ',
  `created_at` bigint NOT NULL,
  `updated_at`bigint NOT NULL,
  `deleted_at` bigint default NULL,
  PRIMARY KEY (`id`),
  INDEX userid_idx (`user_id`)
);

INSERT INTO user_presents_deleted
(id, user_id, sent_at, item_type, item_id, amount, present_message, created_at, updated_at, deleted_at)
SELECT * FROM user_presents WHERE deleted_at IS NOT NULL;

DELETE FROM user_presents WHERE deleted_at IS NOT NULL;