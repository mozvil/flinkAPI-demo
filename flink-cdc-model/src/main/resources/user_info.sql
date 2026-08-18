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

 Date: 04/07/2026 22:34:30
*/

SET NAMES utf8mb4;
SET FOREIGN_KEY_CHECKS = 0;

-- ----------------------------
-- Table structure for user_info
-- ----------------------------
DROP TABLE IF EXISTS `user_info`;
CREATE TABLE `user_info`  (
  `user_id` bigint NOT NULL AUTO_INCREMENT COMMENT 'id',
  `username` varchar(255) CHARACTER SET utf8mb3 COLLATE utf8mb3_bin NOT NULL COMMENT '用户名',
  `phone_num` varchar(128) CHARACTER SET utf8mb3 COLLATE utf8mb3_bin NULL DEFAULT NULL COMMENT '手机号',
  `login_time` datetime NOT NULL COMMENT '最后一次登录时间',
  `status` tinyint NULL DEFAULT NULL COMMENT '状态',
  PRIMARY KEY (`user_id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 5 CHARACTER SET = utf8mb3 COLLATE = utf8mb3_bin COMMENT = 'user_info' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Records of user_info
-- ----------------------------
INSERT INTO `user_info` VALUES (1, 'Ashiley Cole', '13500240913', '2026-06-29 20:15:48', 1);
INSERT INTO `user_info` VALUES (2, 'Bomton Sarge', '19201923738', '2026-06-30 13:31:36', 1);
INSERT INTO `user_info` VALUES (3, 'Martin Hergersis', '17350912262', '2026-07-01 04:17:02', 1);
INSERT INTO `user_info` VALUES (4, 'Sharaton Hong', '15271903308', '2026-07-02 18:02:43', 1);

SET FOREIGN_KEY_CHECKS = 1;
