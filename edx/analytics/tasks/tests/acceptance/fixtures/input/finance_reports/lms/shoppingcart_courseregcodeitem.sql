--
-- Table structure for table `shoppingcart_courseregcodeitem`
--

DROP TABLE IF EXISTS `shoppingcart_courseregcodeitem`;
CREATE TABLE `shoppingcart_courseregcodeitem` (
  `orderitem_ptr_id` int(11) NOT NULL,
  `course_id` varchar(128) NOT NULL,
  `mode` varchar(50) NOT NULL,
  PRIMARY KEY (`orderitem_ptr_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

