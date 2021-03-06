<?hh

//Adapted from MongoFill
/**
 * A unique identifier created for database objects. If an object is inserted
 * into the database without an _id field, an _id field will be added to it
 * with a MongoId instance as its value. If the data has a naturally occuring
 * unique field (say, a username or timestamp) it is fine to use this as the
 * _id field instead, and it will not be replaced with a MongoId.   Instances
 * of the MongoId class fulfill the role that autoincrementing does in a
 * relational database: to provide a unique key if the data does not natually
 * have one. Autoincrementing does not work well with a sharded database, as
 * it is impossible to find what the next number should be quickly. This class
 * fulfills the constraints of quickly generating a value that is unique
 * across shards.   Each MongoId is 12 bytes (making its string form 24
 * hexidecimal characters). The first four bytes are a timestamp, the next
 * three are a hash of the client machine's hostname, the next two are the two
 * least significant bytes of the process id running the script, and the last
 * three bytes are an incrementing value.   MongoIds are
 * serializable/unserializable. Their serialized form is similar to their
 * string form:
 */
class MongoId
{
	
	private $id;
	private $timestamp;
	private $hostname;
	private $pid;
	private $inc;
	static private $refInc = null;

	/**
	* Creates a new id
	*
	* @param string $id - A string to use as the id. Must be 24
	* hexidecimal characters. If an invalid string is passed to this
	* constructor, the constructor will ignore it and create a new id
	* value.
	*
	* @return - Returns a new id.
	*/
	public function __construct($id = null)
	{
		$this->hostname = self::getHostname();
		if ($id === null) {
			$id = $this->generateId();
		} else if (self::isValid($id)) {
			$this->disassembleId($id);
		} else {
			throw new MongoException('Invalid object ID', 19);
		}
		$this->{'$id'} = $id;
		$this->id = $id;
	}

	private function generateId()
	{
		if (null === self::$refInc) {
			 self::$refInc = (int) mt_rand(0, pow(2, 24));
		}
		
		$this->timestamp = time();
		$this->inc = self::$refInc++;
		$this->pid = getmypid();

		if ($this->pid > 32768) {
				$this->pid = 65536 - $this->pid;
		}

		return $this->assembleId();
	}

	private function assembleId()
	{
		$hash = unpack('a3hash', md5($this->hostname, true))['hash'];
		$i1 = ($this->inc) & 255;
		$i2 = ($this->inc >> 8) & 255;
		$i3 = ($this->inc >> 16) & 255;
		$binId = pack(
			'Na3vC3',
			$this->timestamp,
			$hash,
			$this->pid,
			$i3, $i2, $i1
		);
 
		return bin2hex($binId);
	}

	private function disassembleId($id)
	{
		$vars = unpack('Nts/C3m/vpid/C3i', hex2bin($id));
		$this->timestamp = $vars['ts'];
		$this->pid = $vars['pid'];
		$this->inc = $vars['i3'] | ($vars['i2'] << 8) | ($vars['i1'] << 16);
	}

	/**
	* Gets the hostname being used for this machine's ids
	*
	* @return string - Returns the hostname.
	*/
	public static function getHostname()
	{
		return gethostname();
	}

	/**
	* Gets the number of seconds since the epoch that this id was created
	*
	* @return int - Returns the number of seconds since the epoch that
	* this id was created. There are only four bytes of timestamp stored,
	* so MongoDate is a better choice for storing exact or wide-ranging
	* times.
	*/
	public function getTimestamp()
	{
		return $this->timestamp;
	}

	/**
	* Gets the process ID
	*
	* @return int - Returns the PID of the MongoId.
	*/
	public function getPid()
	{
		return $this->pid;
	}

	/**
	* Gets the incremented value to create this id
	*
	* @return int - Returns the incremented value used to create this
	* MongoId.
	*/
	public function getInc()
	{
		return $this->inc;
	}

	/**
	* Check if a value is a valid ObjectId
	*
	* @param mixed $value - The value to check for validity.
	*
	* @return bool - Returns TRUE if value is a MongoId instance or a
	* string consisting of exactly 24 hexadecimal characters; otherwise,
	* FALSE is returned.
	*/
	public static function isValid($id)
	{
		return preg_match('/[0-9a-fA-F]{24}/', $id);
	}

	/**
	* Create a dummy MongoId
	*
	* @param array $props - Theoretically, an array of properties used to
	* create the new id. However, as MongoId instances have no properties,
	* this is not used.
	*
	* @return MongoId - A new id with the value
	* "000000000000000000000000".
	*/
	public static function __set_state($props)
	{
		$id = new self('000000000000000000000000');
		foreach($props as $propName => $value) {
			$id->{$propName} = $value;
		}
		$id->id = $id->assembleId();
		return $id;
	}

	/**
	* Returns a hexidecimal representation of this id
	*
	* @return string - This id.
	*/
	public function __toString()
	{
		return (string)$this->id;
	}
}