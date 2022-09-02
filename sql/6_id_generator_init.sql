DROP TABLE IF EXISTS id_generator;

CREATE TABLE `id_generator` (
  `id` bigint NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

INSERT INTO id_generator VALUES(100000000001);

DROP TABLE IF EXISTS uid_generator;

CREATE TABLE `uid_generator` (
  `id` bigint NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

INSERT INTO uid_generator VALUES(100000000001);
