/*
 Navicat Premium Dump SQL

 Source Server         : MySQL9.6 on docker(192.168.247.162)
 Source Server Type    : MySQL
 Source Server Version : 90600 (9.6.0)
 Source Host           : 192.168.247.162:3306
 Source Schema         : moz_flink_test

 Target Server Type    : MySQL
 Target Server Version : 90600 (9.6.0)
 File Encoding         : 65001

 Date: 04/07/2026 17:56:37
*/

SET NAMES utf8mb4;
SET FOREIGN_KEY_CHECKS = 0;

-- ----------------------------
-- Table structure for recharge_info
-- ----------------------------
DROP TABLE IF EXISTS `recharge_info`;
CREATE TABLE `recharge_info`  (
  `id` bigint NOT NULL AUTO_INCREMENT COMMENT 'id',
  `user_id` bigint NOT NULL COMMENT 'FK -> user_info.user_id',
  `price` int NOT NULL COMMENT '充值金额',
  `action_time` datetime NULL DEFAULT NULL COMMENT '发生时间',
  `pay_method` tinyint NOT NULL COMMENT '支付方式',
  `remark` varchar(128) CHARACTER SET utf8mb3 COLLATE utf8mb3_bin NULL DEFAULT NULL COMMENT '备注信息',
  PRIMARY KEY (`id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 9 CHARACTER SET = utf8mb3 COLLATE = utf8mb3_bin COMMENT = 'recharge_info' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Records of recharge_info
-- ----------------------------
INSERT INTO `recharge_info` VALUES (1, 1, 380, '2026-01-12 23:18:06', 2, '点卡充值');
INSERT INTO `recharge_info` VALUES (2, 2, 500, '2026-01-14 15:33:14', 3, '点卡充值');
INSERT INTO `recharge_info` VALUES (3, 1, 250, '2026-02-04 12:39:20', 2, '点卡充值');
INSERT INTO `recharge_info` VALUES (4, 3, 700, '2026-02-20 09:16:16', 1, '点卡充值');
INSERT INTO `recharge_info` VALUES (5, 2, 600, '2026-03-10 15:08:14', 3, '点卡充值');
INSERT INTO `recharge_info` VALUES (6, 3, 470, '2026-04-21 17:03:15', 1, '月卡充值');
INSERT INTO `recharge_info` VALUES (7, 3, 200, '2026-04-29 13:30:14', 1, '点卡充值');
INSERT INTO `recharge_info` VALUES (8, 1, 150, '2026-05-05 17:50:42', 2, '月卡充值');

SET FOREIGN_KEY_CHECKS = 1;
