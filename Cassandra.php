<?php
/**
 * A TSocket patch is recommended for this library. See the following link:
 * @link http://issues.apache.org/jira/browse/THRIFT-347
 */

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
 * @package    Zend_Cassandra
 * @copyright  Copyright (c) 2005-2009 Zend Technologies USA Inc. (http://www.zend.com)
 * @license    http://framework.zend.com/license/new-bsd     New BSD License
 */

/**
 * A basic wrapper to the thrift function calls for Cassandra
 *
 * TODO: Description
 *
 * @category   Zend
 * @package    Zend_Cassandra
 * @copyright  Copyright (c) 2005-2009 Zend Technologies USA Inc. (http://www.zend.com)
 * @license    http://framework.zend.com/license/new-bsd     New BSD License
 * @version    0.1
 */
class Zend_Cassandra
{
	protected $_server;
	protected $_port;
	protected $_keyspace;
	protected $_transport;
	protected $_client;
	
	public function __construct($host, $port, $keyspace)
	{
		$this->_keyspace = $keyspace;
			
		$socket = new TSocket($host, $port);
		$this->_transport = new TBufferedTransport($socket, 1024, 1024);
		$protocol = new TBinaryProtocol($this->_transport);
		$this->_client = new CassandraClient($protocol);
		$this->_transport->open();
	}
	
	/**
	 * Set the currently used cassandra keyspace
	 * 
	 * @param $keyspace
	 */
	public function setKeyspace($keyspace)
	{
		$this->_keyspace = (string) $keyspace;
	}
	
	/**
	 * Get a cassandra string property
	 * 
	 * @param string $property
	 */
	public function getCount($columnFamily, $key, $consistency = cassandra_ConsistencyLevel::ONE)
	{
		$columnParent = $this->buildColumnParent($columnFamily); 
		
		return $this->_client->get_count($this->_keyspace, $key, $columnParent, $consistency);
	}
	
	/**
	 * Inserts a row into the cassandra database.
	 * 
	 * @param string $columnFamily
	 * @param string $key
	 * @param array $data
	 * @param integer $consistency
	 */
	public function insert($columnFamily, $key, $data, $consistency = cassandra_ConsistencyLevel::ZERO)
	{
		$columns = array();
		
		$time = microtime(true);
		
		foreach($data as $column_name => $val)
		{	
			$column_parent = new cassandra_ColumnOrSuperColumn();
			if(!is_array($val))
			{
				$column = new cassandra_Column();
				$column->timestamp = $time;
				$column->name = $column_name;
				$column->value = (!is_null($val)?$val:'');

				$column_parent->column = $column;
			}
			else
			{
				$super_column = new cassandra_SuperColumn();
				$super_column->name = $column_name;
				$super_column->columns = array();

				foreach($val as $sub_column_name => $sub_val)
				{
					$column = new cassandra_Column();
					$column->timestamp = $time;
					$column->name = $sub_column_name;
					$column->value = (!is_null($sub_val)?$sub_val:'');
					
					$super_column->columns[] = $column;
				}
				
				$column_parent->super_column = $super_column;
			}	
				
			$columns[] = $column_parent;
		}
		
		$mutation[$columnFamily] = $columns;
		
		$this->_client->batch_insert($this->_keyspace, $key, $mutation, $consistency);
	}
	
	/**
	 * Deletes a piece of data from the Cassandra database
	 * 
	 * @param string $columnFamily
	 * @param string $columnName
	 * @param string $superColumnName
	 * @param integer $consistency
	 */
	public function delete($columnFamily, $key, $columnName, $superColumnName = NULL, $consistency = cassandra_ConsistencyLevel::ONE)
	{
		$columnPath = $this->buildColumnPath($columnFamily, $columnName, $superColumnName);
		$time = microtime(true);
		$this->_client->remove($this->_keyspace, $key, $columnPath, $time, $consistency);
	}

	/**
	 * Fetches a single row from the cassandra database
	 * 
	 * @param string $columnFamily
	 * @param string $key
	 * @param integer $consistency
	 * @return array
	 */
	public function fetchRow($columnFamily, $key, $start='', $finish='', $reversed = FALSE, $count = 100, $consistency = cassandra_ConsistencyLevel::ONE)
	{
		$columnParent = $this->buildColumnParent($columnFamily);

		$predicate = $this->buildPredicate($start, $finish, $reversed, $count);

		return $this->_client->get_slice($this->_keyspace, $key, $columnParent, $predicate, $consistency);
	}
	
	/**
	 * Fetches multiple rows from the cassandra database based on a list of keys.
	 * 
	 * @param string $columnFamily
	 * @param array $keys
	 * @param integer $consistency
	 * @return array
	 */
	public function fetchRows($columnFamily, $keys, $consistency = cassandra_ConsistencyLevel::ONE)
	{
		$columnParent = $this->buildColumnParent($columnFamily);
		$predicate = $this->buildPredicate();
		return $this->_client->multiget_slice($this->_keyspace, $keys, $columnParent, $predicate, $consistency);
	}
	
	/**
	 * Fetches multiple rows from the cassandra database based on range values for key.
	 * 
	 * @param string $columnFamily
	 * @param string $startKey
	 * @param string $endKey
	 * @param integer $rowCount
	 * @param integer $consistency
	 * @return array
	 */
	public function fetchRowsByRange($columnFamily, $startKey='', $endKey='', $rowCount = 100, $reversed=FALSE, $consistency = cassandra_ConsistencyLevel::ONE)
	{
		$columnParent = $this->buildColumnParent($columnFamily);
		$predicate = $this->buildPredicate();
		$results = $this->_client->get_range_slice($this->_keyspace, $columnParent, $predicate, $startKey, $endKey, $rowCount, $consistency);
		return $results;
	}
	
	/**
	 * Wrapper to fetchRowsByRange to fetch all data in a column family.
	 * 
	 * @param string $columnFamily
	 * @param string $rowCount
	 * @param int $consistency
	 * @return array
	 */
	public function fetchAll($columnFamily, $rowCount=100, $reversed=FALSE, $consistency = cassandra_ConsistencyLevel::ONE)
	{
		return $this->fetchRowsByRange($columnFamily, '', '', $rowCount, $reversed, $consistency);
	}
	
	/**
	 * Fetches a single column from the cassandra database based on key and column.
	 * 
	 * @param string $columnFamily
	 * @param string $key
	 * @param string $columnName
	 * @param integer $consistency
	 * @return mixed
	 */
	public function fetchCol($columnFamily, $key, $columnName, $consistency = cassandra_ConsistencyLevel::ONE)
	{
		$columnPath = $this->buildColumnPath($columnFamily, $columnName);
		return $this->_client->get($this->_keyspace, $key, $columnPath, $consistency);
	}
	
	/**
	 * Builds a Column Parent for fetches
	 * @param string $columnFamily
	 * @param string $superColumnName
	 * @return cassandra_ColumnParent
	 */
	public function buildColumnParent($columnFamily, $superColumnName=NULL)
	{
		if(empty($columnFamily))
		{
			throw new Exception('Column Family must be defined in the ColumnParent');
		}
		
		$columnParent = new cassandra_ColumnParent();
		$columnParent->column_family = $columnFamily;
		$columnParent->super_column = $superColumnName;
		
		return $columnParent;
	}
	
	/**
	 * Builds a Column Path
	 * @param string $columnFamily
	 * @param string $columnName
	 * @param string $superColumnName
	 * @return cassandra_ColumnPath
	 */
	public function buildColumnPath($columnFamily, $columnName, $superColumnName = NULL)
	{
		if(empty($columnFamily))
		{
			throw new Exception('Column Family must be defined in the Column Parent');
		}
		
		if(empty($columnName))
		{
			throw new Exception('Column Name must be defined in the Column Parent');
		}
		
		$columnPath = new cassandra_ColumnPath();
		$columnPath->column_family = $columnFamily;
		$columnPath->column = $columnName;
		$columnPath->super_column = $superColumnName;
		
		return $columnPath;
	}
	
	/**
	 * Builds a Slice Predicate for fetches.
	 * 
	 * @param string $start
	 * @param string $finish
	 * @return cassandra_SlicePredicate
	 */
	public function buildPredicate($start='', $finish='', $reversed = FALSE, $count = 100)
	{
		$sliceRange = new cassandra_SliceRange();
		// get all columns
		$sliceRange->start = $start;
		$sliceRange->finish = $finish;
		$sliceRange->reversed = $reversed;
		$sliceRange->count = $count;
		
		$predicate = new cassandra_SlicePredicate();
		$predicate->slice_range = $sliceRange;
		
		return $predicate;
	}
	
	public function __destruct()
	{
		$this->_transport->close();
	} 
}
