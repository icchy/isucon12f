create table if not exists user_presents_exists
(
    id              bigint       not null
        primary key,
    user_id         bigint       not null comment 'ユーザID',
    sent_at         bigint       not null comment 'プレゼント送付日時',
    item_type       int          not null comment 'アイテム種別',
    item_id         int          not null comment 'アイテムID',
    amount          int          not null comment 'アイテム数',
    present_message varchar(255) null comment 'プレゼントメッセージ',
    created_at      bigint       not null,
    updated_at      bigint       not null
);
create table if not exists user_presents_deleted
(
    id              bigint       not null
        primary key,
    user_id         bigint       not null comment 'ユーザID',
    sent_at         bigint       not null comment 'プレゼント送付日時',
    item_type       int          not null comment 'アイテム種別',
    item_id         int          not null comment 'アイテムID',
    amount          int          not null comment 'アイテム数',
    present_message varchar(255) null comment 'プレゼントメッセージ',
    created_at      bigint       not null,
    updated_at      bigint       not null,
    deleted_at      bigint       not null
);

INSERT INTO user_presents_exists
(id, user_id, sent_at, item_type, item_id, amount, present_message, created_at, updated_at)
SELECT
    id, user_id, sent_at, item_type, item_id, amount, present_message, created_at, updated_at
FROM user_presents
WHERE deleted_at IS NULL;

INSERT INTO user_presents_deleted
(id, user_id, sent_at, item_type, item_id, amount, present_message, created_at, updated_at, deleted_at)
SELECT
    *
FROM user_presents
WHERE deleted_at IS NOT NULL;

DROP TABLE user_presents;
create view user_presents as
select `user_presents_deleted`.`id`              AS `id`,
       `user_presents_deleted`.`user_id`         AS `user_id`,
       `user_presents_deleted`.`sent_at`         AS `sent_at`,
       `user_presents_deleted`.`item_type`       AS `item_type`,
       `user_presents_deleted`.`item_id`         AS `item_id`,
       `user_presents_deleted`.`amount`          AS `amount`,
       `user_presents_deleted`.`present_message` AS `present_message`,
       `user_presents_deleted`.`created_at`      AS `created_at`,
       `user_presents_deleted`.`updated_at`      AS `updated_at`,
       `user_presents_deleted`.`deleted_at`      AS `deleted_at`
from `user_presents_deleted`
union all
select `user_presents_exists`.`id`              AS `id`,
       `user_presents_exists`.`user_id`         AS `user_id`,
       `user_presents_exists`.`sent_at`         AS `sent_at`,
       `user_presents_exists`.`item_type`       AS `item_type`,
       `user_presents_exists`.`item_id`         AS `item_id`,
       `user_presents_exists`.`amount`          AS `amount`,
       `user_presents_exists`.`present_message` AS `present_message`,
       `user_presents_exists`.`created_at`      AS `created_at`,
       `user_presents_exists`.`updated_at`      AS `updated_at`,
       NULL                                              AS `NULL`
from `user_presents_exists`;

create table if not exists user_presents_tmp
(
    id              bigint       not null
        primary key,
    user_id         bigint       not null comment 'ユーザID',
    sent_at         bigint       not null comment 'プレゼント送付日時',
    item_type       int          not null comment 'アイテム種別',
    item_id         int          not null comment 'アイテムID',
    amount          int          not null comment 'アイテム数',
    present_message varchar(255) null comment 'プレゼントメッセージ',
    created_at      bigint       not null,
    updated_at      bigint       not null
);
