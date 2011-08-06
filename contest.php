<?php
class contest {
  //connection handle
  private $conn;
  //database name
  private $dbname;
  //user id
  private $userid;
  //user password
  private $password = "";
  
  private $time_start;
  private $time_end;
  private $execution_time;
  
  //do not modify this function!
  function __construct($dbname = "", $userid = "") {
    $this->dbname = $dbname;
    $this->userid = $userid;
  } 

  //do not modify this function!
  public function __destruct() {
    if(is_resource($this->conn)) {
      cubrid_disconnect($this->conn);
    }
  }

  //do not modify this function!
  private function connect() {
    $this->conn = cubrid_connect("localhost", 30000, $this->dbname, "public", $this->password);
  }  
  
  //do not modify this function!
  private function display_results() {
    $sql = "select most_duplicated_value, total_occurrences, execution_time from results where userid = '".$this->userid."'";
    $result = cubrid_query($sql);
    if (!$result) {
      die(cubrid_error());
    }

    while ($row = cubrid_fetch_assoc($result)) {
      echo "Most duplicated value: [".$row["most_duplicated_value"]."], having ".$row["total_occurrences"]." occurrences.".PHP_EOL;
      echo "Elapsed time: ".($row["execution_time"]/1000)." sec.".PHP_EOL;
    }
    cubrid_free_result($result);
  }  
  
  public function run() {
    $this->connect();
    if(!$this->conn) {
      echo "Error connecting to the database!".PHP_EOL;
      return false;
    }
    
    //set auto-commit OFF
    if(function_exists("cubrid_set_autocommit")) {
      cubrid_set_autocommit($this->conn, CUBRID_AUTOCOMMIT_FALSE);
    }
    
    //start timing
    $this->time_start = microtime(true);

    ///////////////////////////////////////////////////////////////////////////////////////////
    // Here below you will need to write the code to solve the contest problem
    ///////////////////////////////////////////////////////////////////////////////////////////

      set_time_limit(0);
    /*
     * Step 1. Definition of checking rules
     *
     * Different field types should be tested in different conditions.
     * Create a group of rules for checking ($rules).
     */

    // Rules for fields checking
    $rules = array(
        'int' =>
            array(
                'select' => 'TO_CHAR(%s)',
                'where' => '%s < 0'
            ),
        'float' =>
            array(
                'select' => 'TO_CHAR(%s)',
                'where' => ''
            ),
        'string' =>
            array(
                'select' => '%s',
                'where' => ''
            ),
        'datetime' =>
            array(
                'select' => 'TO_CHAR(%s)',
                'where' => ''
            )
    );

    //Associating $rules ids with sql data type names
    $assoc_array = array(

        /* numeric types*/
        'SHORT'     => 'int',
        'SMALLINT'  => 'int',
        'INT'       => 'int',
        'INTEGER'   => 'int',
        'BIGINT'    => 'int',

         /* float types */
        'FLOAT'     => 'float',
        'REAL'      => 'float',
        'DOUBLE'    => 'float',
        'DOUBLE PRECISION' => 'float',
        'MONETARY'  => 'float',
        'NUMERIC'   => 'float',
        'DECIMAL'   => 'float',

        /* string types */
        'CHAR'      => 'string',
        'VARCHAR'   => 'string',
        'CHAR VARYING' => 'string',
        'STRING'    => 'string',

         /* datetime types */
        'DATE'      => 'datetime',
        'TIMESTAMP' => 'datetime',
        'DATETIME'  => 'datetime',
        'TIME'      => 'datetime'
    );

    $columns_array = array ();

    //Get a list of tables
    if($table_list = cubrid_schema( $this->conn , CUBRID_SCH_CLASS ) ){

        /*
        * Step 2. Getting field names and field types for generating sql-queries
        */

        //Searching only user tables in schema (`results` table is ignored)
        foreach( $table_list as $table ){
            /*
            * $table['TYPE']:
            * 0:system table;
            * 1:view;
            * 2:table;
            */
            if(
                ( isset ($table['TYPE']) ) && ( isset ($table['NAME']) )&&
                ( $table['TYPE'] == 2 ) && ( $table['NAME'] != 'results' )
            ){

                //Empty SQL request (with "LIMIT 0") for getting columns of current table
                $result = cubrid_execute( $this->conn , "select * from {$table['NAME']} limit 0;");

                // Getting column name and types for current table
                $column_names = cubrid_column_names($result);
                $column_types = cubrid_column_types($result);

                if( is_array( $column_names ) && is_array( $column_types ) &&
                    count( $column_names ) > 0 && count( $column_names ) == count( $column_types ) ){

                    for ($i = 0; $i < count($column_names); $i++){
                        $column =  $column_types[$i];                      // ex. varchar(255)
                        $column = preg_replace('/\((.*)\)/', '', $column); // deleting brackets (ex. varchar)

                        // Adding this column name and column type into table array
                        $columns_array[] =array(
                                            'tableName' => $table[ 'NAME' ],      // table name
                                            'columnName'=> $column_names[ $i ],   // field name
                                            'columnType'=> strtoupper( $column )   // field type in $assoc_array style
                                                                                // (ex. FLOAT, CHAR)
                        );
                    }
                }
            }
        }
        unset ($table_list);

        /*
         * Step 3. Generating and executing main part of sql-queries
         */

        // Cubrid sql resources array
        $results = array();

        // Flags array - default: <b>true</b>; if column is fully checked then <b>false</b>
        $column_enable = array();

        // Template to quickly create items of main array
        $template_array = array();

        foreach ($columns_array as $id => $column){
            $name  = $column[ 'columnName' ];
            $table = $column[ 'tableName' ];
            $type  = $column[ 'columnType' ];

            // SELECT part of request (based on $rules)
            $select = str_replace('%s', "`$name`", $rules[ $assoc_array[$type] ]['select']);

            // WHERE part of request (based on $rules)
            if( $rules[ $assoc_array[$type] ]['where'] != ''){
                $where = " WHERE " . str_replace('%s', "`$name`", $rules[ $assoc_array[$type] ]['where']);
            }else{
                $where = '';
            }

            // sql request for each column in database
            $sql = "SELECT $select, count(*) FROM `$table`$where GROUP BY `$name` ORDER BY 2 DESC;";

            // Writing result of execute sql-request
            $results[ $id ] = cubrid_execute( $this->conn, $sql );
            $column_enable[ $id ] = true;
            $template_array[$id] = null;
        }

        /*
         * This sql requests return tables with pair ("value", count) for each column (for all tables).
         * This pairs will be sorted by count.
         *
         * ex. of resulted tables:
         *  value | count   |   value | count
         *    xyz | 156     |    -456 | 6
         *     zz | 15      |     1,5 | 5
         *       ...        |        ...
         *
         * In next step we will work with this sorted pairs ("value", count).
         * We can work with them using $result - array of cubrid resource
         */

        /*
         * Step 4. Calculating top of values which occurs the most times
         */

        /*
         *  Top contains 2 nice array: $count_array and $main_array
         */

        /*
         * Count of occurrences array
         * Ex.
         * array(
         *      'xyz' => 6,
         *      '-21' => 2
         * )
         *
         */
        $count_array = array();

        /*
         * The main array of values in columns
         * Ex. we have columns
         *          `field1` | `field2` | `field3`
         *           12    |   xyz  |   xyz
         *          -21    |    0   |   xyz
         *          -17    |   zzz  |   zzz
         *                     ...
         *
         *
         * After sql-queries in previous step we have next tables:
         *      `field1`      |     `field2`      |     `field3`
         *  `value` | `count` | `value` | `count` | `value` | `count`
         *    -21   | 1       |     xyz | 1       |     xyz | 2
         *    -17   | 1       |     zzz | 1       |     zzz | 1
         *         ...        |        ...        |        ...
         *
         * Main array for this example will be
         * array(
         *      'xyz' => (null, 1, 2),    // count of elements in sub-array equals the count of columns
         *      '-21' => (1, null, null), // each number in sub-array equals count of current value in this column (null if not found)
         *      '-17' => (1, null, null),
         *      'zzz' => (null, 1, 1)
         * )
         **/

        $main_array = array();

        // while values not ended in columns
        while(array_search(true, $column_enable) !== false){
            // temporary array which contain current counts of values
            $line = array();

            // current record in each column
            foreach($results as $id => $column){
                if($column_enable[$id]){
                    if(!$row = cubrid_fetch_row($column)){
                        //Column was ended
                        $column_enable[$id] = false;
                        continue;
                    }
                    // in  $row we have array (0 => "value", 1 => count);

                    $value = $row[0];
                    $count = (int) $row[1];

                    /*
                     * only for non-numeric, string values
                     */
                    if(($value !== null) && ($value !== false) && ($value != '')
                       && !preg_match('/^[\d]+$/', $value) && (strlen( $value ) < 256)){

                        if(!isset($main_array[$value])){
                            // create item in main array and in count array
                            $main_array[$value] = $template_array;
                            $count_array[$value] = 0;
                        }

                        $count_array[$value] += $count;
                        $main_array[$value][$id] = $count;

                    }
                    $line[$id] = $count;
                }else{
                    $line[$id] = null;
                }
            }

            // Total of counts in this line
            $line_sum = array_sum($line);

            // sorting count array
            arsort($count_array);

            // go to the first element of count array
            $first_value_count = reset($count_array);
            $first_value = key($count_array);

            // <magic>
            /*
             * We calculate 'available sum' for each value.
             * This number, which can potentially be achieved by the current value.
             * This number is needed to understand whether the value to continue to compete.
             */

            foreach($main_array as $value => $columns){
                if($value != $first_value){

                    // calculating 'available sum'
                    $available_sum = 0;
                    foreach($columns as $id => $counts){
                        if($column_enable[$id] && $counts === null){
                            $available_sum += $line[$id];
                        }
                    }

                    if($available_sum + $count_array[$value] - $first_value_count < 0){
                        // current value is getting out from our Top (main and count array)
                        unset($count_array[$value]);
                        unset($main_array[$value]);
                    }
                }
            }
            // </magic>

            // Last element of count array
            end($count_array);

            if( current( $count_array ) > $line_sum ){
                /*
                 * In $line_sum we saved max potentially sum which values from next row can be achieved.
                 * We compare this number with the last element of Top.
                 * When we got here, then there is no point in continuing.
                 * The values of next records can not reach the top. Never.
                 */
                break;
            }
        }

        /*
         * Good. Dirty work is done. We have now Top with 2 arrays ($count_array and $main_array).
         * But we are not sure that all counts from other columns were in the top, because previous while loop can be stopped!
         * We can check it, it is simple.
         */

        /*
         * Step 5. Search for missing counts
         */

        /*
         * This is array with values that must be checked.
         * But we have to look what we have already counted.
         */
        $query = array();

        foreach($main_array as $value => $columns){

            foreach($columns as $id => $column){

                if($column === null && $column_enable[$id]){

                    $type = $columns_array[$id][ 'columnType' ];

                    if(isset($assoc_array [$type]) && isset($rules[$assoc_array [$type]])){

                        if($assoc_array [$type] == 'int'){
                            if(filter_var($value, FILTER_VALIDATE_INT))
                                $query[$id][] = $value;

                        }elseif($assoc_array [$type] == 'float'){

                            if(filter_var($value, FILTER_VALIDATE_FLOAT))
                                $query[$id][] = $value;

                        }else{
                            $query[$id][] = cubrid_real_escape_string( $value );
                        }
                    }

                }
            }
        }

        // All right. Make SQL-requests.
        foreach($query as $id => $values){

            $name = $columns_array[$id][ 'columnName' ];
            $table = $columns_array[$id][ 'tableName' ];
            $type = $columns_array[$id][ 'columnType' ];

            $select = str_replace('%s', "`$name`", $rules[ $assoc_array[$type] ]['select']);

            if($assoc_array [$type] == 'int'){
                $where = " WHERE `$name` IN (" . implode (', ', $values) . ")";
            }elseif($assoc_array [$type] == 'string'){
                $where = " WHERE `$name` IN ('" . implode ("', '", $values) . "')";
            }else{
                $where = " WHERE TO_CHAR(`$name`) IN ('" . implode ("', '", $values) . "')";
            }
            $sql = "SELECT $select, count(*) FROM `$table`$where GROUP BY `$name`;";

            $result = cubrid_execute($this->conn, $sql);

            while($row = cubrid_fetch_row($result)) {
                $value = $row[0];
                $count = (int) $row[1];

                // Sum with existing TOP
                if(isset($main_array[$value])){
                    $count_array[$value] += $count;
                    $main_array[$value][$id] = $count;
                }
            }
        }

        /*
         * Step 6. Final.
         */
        //Sorting
        arsort($count_array);
        $count = current($count_array);
        $value = cubrid_real_escape_string( key($count_array) );


        // Writting into result table
        cubrid_execute($this->conn, "INSERT INTO `results` (`userid`, `most_duplicated_value`, `total_occurrences`)
            values ('" . $this->userid . "', '$value', $count)");


    }
          
    ///////////////////////////////////////////////////////////////////////////////////////////
    // !!!do not modify the code from here below!!!
    ///////////////////////////////////////////////////////////////////////////////////////////
    
    //end timing
    $this->time_end = microtime(true);
    $this->execution_time = ($this->time_end - $this->time_start)*1000; //get microseconds
    
    // Update execution time
    $sqlUpdateExecutionTime = "update results set execution_time = ".$this->execution_time." where userid = '".$this->userid."'";
    $result = cubrid_execute($this->conn, $sqlUpdateExecutionTime);
    cubrid_close_request($result);
    cubrid_commit($this->conn);

    echo PHP_EOL;
    echo "===========================================================".PHP_EOL;
    echo "User Id: ".$this->userid.PHP_EOL;
    $this->display_results();
    echo "===========================================================".PHP_EOL;
    
    return true;
  }
}  
?>
