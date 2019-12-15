CREATE TABLE `itembuycount` (
  `id` mediumint NOT NULL auto_increment,
  `itemId` bigint(255) NOT NULL,
  `buyCount` bigint(11) DEFAULT NULL,
  `createDate` timestamp NULL DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci