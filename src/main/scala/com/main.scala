package com
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import java.sql.DriverManager
import java.sql.{Connection,DriverManager}


object main {

  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "C:\\hadoop")
    val spark = SparkSession
      .builder
      .appName("hello hive")
      .config("spark.master", "local")
      .enableHiveSupport()
      .getOrCreate()
    Logger.getLogger("org").setLevel(Level.OFF)
    println("created spark session")

    var dfload = spark.read.csv("hdfs://localhost:9000/demo/storm_deaths.csv")
    var dfdamages = spark.read.csv("hdfs://localhost:9000/demo/storm_damge.csv")
    dfload.createOrReplaceTempView("hdfsdeath")
    dfdamages.createOrReplaceTempView("hdfsdamage")


    spark.sql("DROP table IF EXISTS deaths")
    //spark.sql("CREATE TABLE test2 (id INT, name STRING, age INT) STORED AS ORC;");
    spark.sql("create table IF NOT EXISTS deaths (Rank INT, Location STRING, Year INT, Category INT, Deaths INT, Name String) row format delimited fields terminated by ','")
    spark.sql("LOAD DATA LOCAL INPATH 'C:/Users/zmgre/Desktop/project0all/storm_deaths.csv' INTO TABLE deaths")

    spark.sql("DROP table IF EXISTS damages")
    //spark.sql("CREATE TABLE test2 (id INT, name STRING, age INT) STORED AS ORC;");
    spark.sql("create table IF NOT EXISTS damages (Rank INT, Name STRING, Location String, Year Int, Category INT, Damages INT) row format delimited fields terminated by ','")
    spark.sql("LOAD DATA LOCAL INPATH 'C:/Users/zmgre/Desktop/project0all/storm_damge.csv' INTO TABLE damages")

    spark.sql("DROP table IF EXISTS chance")
    //spark.sql("CREATE TABLE test2 (id INT, name STRING, age INT) STORED AS ORC;");
    spark.sql("create table IF NOT EXISTS chance (Rank INT, State STRING, Category1 int, Category2 Int, Category3 INT, Category4 INT, Category5 INT, All INT, Major Int, percentOfAll String, percentOfMajor String, AB String, Location String) row format delimited fields terminated by ','")
    spark.sql("LOAD DATA LOCAL INPATH 'C:/Users/zmgre/Desktop/project0all/storm_chance.csv' INTO TABLE chance")

    /************************************USING HADOOP*******************************/
    def catVsDeath(): Unit ={
        spark.sql("SELECT _c3 AS Category, SUM(_c4) AS Total_Deaths, COUNT(_c3) AS Amount_Of_Storms, ROUND(AVG(_c4),1) as Avg_Deaths FROM hdfsdeath GROUP BY _c3 ORDER BY _c3 ASC LIMIT 5").show()

      }

    def catVsDamages() {
      spark.sql("SELECT _c4 AS Category, SUM(_c5) AS Total_Damages, COUNT(_c4) AS Amount_Of_Storms, ROUND(AVG(_c5),1) as Avg_Damages FROM hdfsdamage GROUP BY _c4 ORDER BY _c4 ASC LIMIT 5").show()

    }
/************************************************USES HIVE***************************************************************/
    def deathVsDamages(): Unit ={
       spark.sql("SELECT ROUND(AVG(Damages),0) AS AVG_Damages, COUNT(Damages) AS Amount_Of_Storms, ROUND(AVG(Deaths),0) as AVGDeaths FROM (SELECT damages.Damages AS Damages, deaths.Deaths AS Deaths, damages.Name, damages.Rank AS damageRank, deaths.Rank As deathRank FROM damages INNER JOIN deaths ON damages.Name = deaths.Name) GROUP BY Damages > 9250 HAVING Amount_Of_Storms > 0").show()

      spark.sql("SELECT ROUND(AVG(Deaths),0) AS AVG_Deaths, COUNT(Deaths) AS Amount_Of_Storms, ROUND(AVG(Damages),0) as AVGDamage FROM (SELECT damages.Damages AS Damages, deaths.Deaths AS Deaths, damages.Name, damages.Rank AS damageRank, deaths.Rank As deathRank FROM damages INNER JOIN deaths ON damages.Name = deaths.Name) GROUP BY Deaths > 74 HAVING Amount_Of_Storms > 0").show()

    }
    def chance(): Unit ={
      spark.sql("SELECT AB, State FROM chance").show()
      var state = ""
      do {

        println("Type the state abbreviation you would like to look at\nType quit to Exit: ")
        state = scala.io.StdIn.readLine()
        if (state != "quit") {

          spark.sql("SELECT SUM(totalDEATHS) FROM (SELECT Deaths AS totalDeaths FROM deaths WHERE Location LIKE '%" + state + "%')").show()
          spark.sql("SELECT SUM(totalDamages) FROM (SELECT Damages AS totalDamages FROM damages WHERE Location LIKE '%" + state + "%')").show()
          spark.sql("SELECT * FROM chance WHERE AB LIKE '%"+state+"%'").show()

        }
      }while(state != "quit")

    }

    def locVsCat(){
      spark.sql("SELECT COUNT(Location), Location, SUM(All) FROM chance GROUP BY Location").show()
      spark.sql("SELECT COUNT(Location), Location, SUM(Category3+Category4+Category5) AS CAT3AndUp FROM chance GROUP BY Location").show()
      spark.sql("SELECT COUNT(Location), Location, SUM(Category4+Category5) AS CAT4AndUp FROM chance GROUP BY Location").show()
    }

    def catVsTime(): Unit ={
      spark.sql("SELECT COUNT(_c3) AS amountStorms, SUM(_c4) AS DeathsPost1925, ROUND(AVG(_c3),1) AS after1925 FROM hdfsdeath WHERE _c2 >= 1925").show()
      spark.sql("SELECT COUNT(_c3) AS amountStorms, SUM(_c4) AS DeathsPre1925, ROUND(AVG(_c3),1) AS before1925 FROM hdfsdeath WHERE _c2 < 1925").show()

      /*var yearPick = ""
      do {
        println("Pick a year to divide the database: \nType quit to Exit:  ")
        yearPick = scala.io.StdIn.readLine()
        if(yearPick != "quit") {
          spark.sql("SELECT COUNT(_c3) AS amountStorms, SUM(_c4) AS DeathsPost1925, ROUND(AVG(_c3),1) AS after1925 FROM hdfsdeath WHERE _c2 >= '" + yearPick + "'").show()
          spark.sql("SELECT COUNT(_c3) AS amountStorms, SUM(_c4) AS DeathsPre1925, ROUND(AVG(_c3),1) AS before1925 FROM hdfsdeath WHERE _c2 < '" + yearPick + "'").show()
        }
      }while(yearPick != "quit")*/
    }


/*******************************************************************************************************/
choice()
def choice() {
  println("Press 1 to create an account\nPress 2 to sign in ")
  val choice = scala.io.StdIn.readInt()
  if (choice == 1) {
    createAccount()
  }else if (choice == 2) {
    signIn()
  }else{
    println("Not an option")
  }
}
  def createAccount(){
    val url = "jdbc:mysql://localhost:3306/users"
    val driver = "com.mysql.jdbc.Driver"
    val username = "root"
    val password = "INSERTPSW"
    var connection:Connection = null
    try {
      Class.forName(driver)
      connection = DriverManager.getConnection(url, username, password)
      val statement = connection.createStatement
      println("Create a username: ")
      val u = scala.io.StdIn.readLine()
      println("Create a password: ")
      val p = scala.io.StdIn.readLine()
      statement.executeUpdate("INSERT INTO stormaccount (username, psw) VALUE ('"+u+"','"+p+"')")
      //val rs = statement.executeQuery("SELECT * FROM stormaccount")
      /*while (rs.next) {
        val id = rs.getString("id")
        val host = rs.getString("username")
        val user = rs.getString("psw")
        println(id, host, user)
      }*/
    } catch {
      case e: Exception => e.printStackTrace
    }
  connection.close
  }

  def signIn(){
  val url = "jdbc:mysql://localhost:3306/users"
  val driver = "com.mysql.jdbc.Driver"
  val username = "root"
  val password = "INSERTPSW"
  var connection:Connection = null
  try {
    Class.forName(driver)
    connection = DriverManager.getConnection(url, username, password)
    val statement = connection.createStatement

    println("Enter your username: ")
    val u = scala.io.StdIn.readLine()
    println("Enter your password: ")
    val p = scala.io.StdIn.readLine()


    var continue = 1
    while(continue == 1) {
      val account = statement.executeQuery("SELECT * FROM stormaccount WHERE username = '"+u+"' AND psw = '"+p+"'")
      if (account.next != false) {
        val id = account.getString("id")
        //println(id)
        if (id == "1") {
          println("Press 1 to delete an account\nPress 2 to sign out: ")
          val userpick = scala.io.StdIn.readInt()
          if (userpick == 1) {
            val rs = statement.executeQuery("SELECT * FROM stormaccount")
            while (rs.next) {
              val id = rs.getString("id")
              val host = rs.getString("username")
              val user = rs.getString("psw")
              print(id, host, user)

            }
            println("Which ID do you want to delete: ")
            var del = scala.io.StdIn.readInt()
            while (del == 1) {
              println("Can not delete Admin")
              println("Which ID do you want to delete: ")
              del = scala.io.StdIn.readInt()
            }
            statement.executeUpdate("Delete FROM stormaccount WHERE id = '" + del + "'")
          }else if(userpick == 2){
            choice()
          }
        } else {

          println("Press 1 to edit your account\nPress 2 to see the data: ")
          val userpick = scala.io.StdIn.readInt()

          /** ********************************************** */
          if (userpick == 1) {
            println("Press 1 to change username\nPress 2 to change password\nPress 3 to do both: ")
            val upb = scala.io.StdIn.readInt()
            if (upb == 1) {
              println("Type your new username")
              val newuser = scala.io.StdIn.readLine()
              statement.executeUpdate("UPDATE stormaccount SET username = '" + newuser + "' WHERE id = '" + id + "'")
            } else if (upb == 2) {
              println("Type your new password")
              val newpsw = scala.io.StdIn.readLine()
              statement.executeUpdate("UPDATE stormaccount SET psw = '" + newpsw + "' WHERE id = '" + id + "'")
            } else if (upb == 3) {
              println("Type your new username")
              val newuser = scala.io.StdIn.readLine()
              statement.executeUpdate("UPDATE stormaccount SET username = '" + newuser + "' WHERE id = '" + id + "'")
              println("Type your new password")
              val newpsw = scala.io.StdIn.readLine()
              statement.executeUpdate("UPDATE stormaccount SET psw = '" + newpsw + "' WHERE id = '" + id + "'")
            }

            /** ****************************************** */
          }else if(userpick == 2){
            dataSelect()
          }
        }
      } else {
        println("incorrect password and/or username")
        choice()
      }
      println("Press 1 to continue\nPress 2 to quit: ")
      continue = scala.io.StdIn.readInt()
    }

  } catch {
    case e: Exception => e.printStackTrace
  }
  connection.close
  }
  def dataSelect(): Unit ={
    println("Press 1 to see the relationship between the Category and the amount of deaths\nPress 2 to see the relationship between Category and Cost of damages ")
    println("Press 3 to see the relationship between the cost of Damages and the amount of deaths\nPress 4 to see the Damages and Deaths compared to State")
    println("Press 5 to see the relationship between the Location and Category\nPress 6 to see the deaths compared to time:")
    val dataChoice = scala.io.StdIn.readInt()
    if(dataChoice == 1){
      catVsDeath()
    }else if(dataChoice == 2) {
      catVsDamages()
    }else if(dataChoice == 3) {
      deathVsDamages()
    }else if(dataChoice == 4) {
      chance()
    }else if(dataChoice == 5) {
      locVsCat()
    }else if(dataChoice == 6) {
      catVsTime()
    }
  }

  spark.close()
  }
}
