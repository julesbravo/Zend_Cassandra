<?php
/**
 * Zend Framework
 *
 * LICENSE
 *
 * This source file is subject to the new BSD license that is bundled
 * with this package in the file LICENSE.txt.
 * It is also available through the world-wide-web at this URL:
 * http://framework.zend.com/license/new-bsd
 * If you did not receive a copy of the license and are unable to
 * obtain it through the world-wide-web, please send an email
 * to license@zend.com so we can send you a copy immediately.
 *
 * @category   Zend
 * @package    Zend_UUID
 * @copyright  Copyright (c) 2005-2009 Zend Technologies USA Inc. (http://www.zend.com)
 * @license    http://framework.zend.com/license/new-bsd     New BSD License
 */

/**
 * A representation of a version 1 UUID, based on code orginally by Wing Lian.
 *
 * @category   Zend
 * @package    Zend_UUID
 * @copyright  Copyright (c) 2005-2009 Zend Technologies USA Inc. (http://www.zend.com)
 * @license    http://framework.zend.com/license/new-bsd     New BSD License
 * @version    0.1
 */
class Zend_UUID
{
	protected $_binary;
	protected $_hex;
	
	/**
	 * Represents a UUID based upon a given hexadecimal string, binary data, or generated 
	 * using the current time, node (MAC Address), and clock sequence.
	 * 
	 * @param string hex
	 * @param binary binary
	 * @param binary $node
	 * @param binary $clockSeq
	 */
	public function __construct($hex=NULL, $binary=NULL, $node=NULL, $clockSeq=NULL) {
		if(!is_null($hex))
		{
			$this->_hex = $hex;
			$this->_binary = pack("H*", $this->_hex);
		}
		elseif(!is_null($binary))
		{
			$this->_binary = $binary;
			$unpacked = unpack("H*hex", $this->_binary);
			$this->_hex = $unpacked['hex'];
		}
		else
		{
			// 0x01b21dd213814000 is the number of 100-ns intervals between the
			// UUID epoch 1582-10-15 00:00:00 and the Unix epoch 1970-01-01 00:00:00.
			$timestamp = microtime(true)*10000000 + 0x01b21dd213814000;
			
			$clockSeq = (is_null($clockSeq)) ? mt_rand(0, 0x3FFF) : ($clockSeq & 0x3FFF);
			
			$this->_binary = pack("NnnnnN", $timestamp & 0xFFFFFFFF, ($timestamp >> 32) & 0xFFFF, (($timestamp >> 48) & 0x0FFF) | 0x1000, ($clockSeq & 0x3FFF) | 0x8000, (is_null($node) ? mt_rand(0, 0xFFFF) : $node >> 32), (is_null($node) ? mt_rand(0, 0xFFFFFFFF) : ($node & 0xFFFFFFFF)) );
			$this->_hex = unpack("H*hex", $this->_binary);
		}
	}
	
	public function getBinary()
	{
		return $this->_binary;
	}
	
	public function getHex()
	{
		return $this->_hex; 
	}
}