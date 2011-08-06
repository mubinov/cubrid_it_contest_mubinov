<?php
  if (!function_exists('cubrid_connect')) {
    echo "CUBRID API is not detected!" . "<br />";
    echo "Please check your CUBRID PHP API instalation." . "<br />";
    exit(1);
  }
  
  require_once("contest.php");
  
  // !!! Make sure you update the value with your own user id, 
  // the same one you used to register for the contest !!!
  $userid = "mubinov.com";
  
  //database name
  $dbname = "contest"; //do not change this value

  //Do not modify the rest of the code below
  echo "Program started...".PHP_EOL;

  $contest2 = new contest($dbname, $userid);
  if(!$contest2->run()) {
    echo "Program did not complete successfully!".PHP_EOL;
  }
?>
