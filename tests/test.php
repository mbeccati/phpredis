<?php

// phpunit is such a pain to install, we're going with pure-PHP here.
class TestSuite {
	public static $errors = array();
	public static $warnings = array();

	protected function assertFalse($bool) {
		$this->assertTrue(!$bool);
	}

	protected function assertTrue($bool) {
		if($bool)
			return;

		$bt = debug_backtrace(false);
		self::$errors []= sprintf("Assertion failed: %s:%d (%s)\n",
			$bt[0]["file"], $bt[0]["line"], $bt[1]["function"]);
	}

    protected function assertLess($a, $b) {
        if($a < $b) 
            return;

        $bt = debug_backtrace(false);
        self::$errors[] = sprintf("Assertion failed (%s >= %s): %s: %d (%s\n",
            print_r($a, true), print_r($b, true),
            $bt[0]["file"], $bt[0]["line"], $bt[1]["function"]);
    }

	protected function assertEquals($a, $b) {
		if($a === $b)
			return;

		$bt = debug_backtrace(false);
		self::$errors []= sprintf("Assertion failed (%s !== %s): %s:%d (%s)\n",
			print_r($a, true), print_r($b, true),
			$bt[0]["file"], $bt[0]["line"], $bt[1]["function"]);
	}

	protected function markTestSkipped($msg='') {
		$bt = debug_backtrace(false);
		self::$warnings []= sprintf("Skipped test: %s:%d (%s) %s\n",
			$bt[0]["file"], $bt[0]["line"], $bt[1]["function"], $msg);

		throw new Exception($msg);
	}

	public static function run($className, $str_limit) {
        $rc = new ReflectionClass($className);
		$methods = $rc->GetMethods(ReflectionMethod::IS_PUBLIC);
        $i_limit = -1;
        $i_idx = 0;
        $boo_printed = false;
        $arr_ran_methods = Array();

        if ($str_limit && is_numeric($str_limit)) {
            echo "Limiting to $str_limit tests!\n";
            $i_limit = (integer)$str_limit;
        } else if ($str_limit) {
            echo "Limiting to tests with the substring: '$str_limit'\n";
        }

		foreach($methods as $m) {
			$name = $m->name;
			if(substr($name, 0, 4) !== 'test')
				continue;

            /* If TestRedis.php was envoked with an argument, do a simple
             * match against the routine.  Useful to limit to one test */
            if ($i_limit == -1 && $str_limit && strpos(strtolower($name),strtolower($str_limit))===false)
                continue;

            if ($i_limit > -1 && $i_idx++ >= $i_limit) {
                continue;
            }

            $arr_ran_methods[] = $name;

			$count = count($className::$errors);
			$rt = new $className;
			try {
var_dump($name);
				$rt->setUp();
				$rt->$name();
				echo ($count === count($className::$errors)) ? "." : "F";
			} catch (Exception $e) {
				if ($e instanceof RedisException) {
					$className::$errors[] = "Uncaught exception '".$e->getMessage()."' ($name)\n";
					echo 'F';
				} else {
					echo 'S';
				}
            }
		}
		echo "\n";
		echo implode('', $className::$warnings);

        echo " --- tests run ---\n";
        echo implode("\n", $arr_ran_methods) . "\n" ;
        echo " --- fin ---\n";

		if(empty($className::$errors)) {
			echo "All tests passed.\n";
			return 0;
		}

		echo implode('', $className::$errors);
		return 1;
	}
}

?>
