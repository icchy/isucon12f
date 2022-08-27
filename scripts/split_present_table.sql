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
