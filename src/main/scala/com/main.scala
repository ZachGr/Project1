package com
//package tests
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import scala.io.StdIn.readLine
import java.sql.DriverManager
import java.sql.{Connection,DriverManager}


object main {

  def main(args: Array[String]): Unit = {


    // create a spark session
    // for Windows
    System.setProperty("hadoop.home.dir", "C:\\hadoop")
    val spark = SparkSession
      .builder
      .appName("hello hive")
      .config("spark.master", "local")
      //.config("spark.driver.allowMultipleContexts","true")
      .enableHiveSupport()
      .getOrCreate()
    //Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("org").setLevel(Level.OFF)
    println("created spark session")

    var dfload = spark.read.csv("hdfs://localhost:9000/demo/storm_deaths.csv")
    var dfdamages = spark.read.csv("hdfs://localhost:9000/demo/storm_damge.csv")
    dfload.createOrReplaceTempView("hdfsdeath")
    dfdamages.createOrReplaceTempView("hdfsdamage")

    //spark.sql("CREATE TABLE test2 (id INT, name STRING, age INT) STORED AS ORC;");
    //spark.sql("DROP table IF EXISTS users")
    //spark.sql("create table IF NOT EXISTS users (ID INT, username String, password String) STORED AS ORC")
    //spark.sql("INSERT INTO users VALUES (1, 'zach', 'yo')")
    //spark.sql("INSERT INTO users VALUES (2, 'green', 'yo')")
    //spark.sql("SELECT * FROM users").show()
    //spark.sql("DELETE FROM users WHERE username = 'zach'")
    //spark.sql("SELECT * FROM users").show()
    //spark.sql("LOAD DATA LOCAL INPATH 'C:/Users/zmgre/Desktop/project0all/storm_deaths.csv' INTO TABLE deaths")

    spark.sql("DROP table IF EXISTS deaths")
    //spark.sql("CREATE TABLE test2 (id INT, name STRING, age INT) STORED AS ORC;");
    spark.sql("create table IF NOT EXISTS deaths (Rank INT, Location STRING, Year INT, Category INT, Deaths INT, Name String) row format delimited fields terminated by ','")
    spark.sql("LOAD DATA LOCAL INPATH 'C:/Users/zmgre/Desktop/project0all/storm_deaths.csv' INTO TABLE deaths")

    spark.sql("DROP table IF EXISTS damages")
    //spark.sql("CREATE TABLE test2 (id INT, name STRING, age INT) STORED AS ORC;");
    spark.sql("create table IF NOT EXISTS damages (Rank INT, Name STRING, Location String, Year Int, Category INT, Damages INT) row format delimited fields terminated by ','")
    spark.sql("LOAD DATA LOCAL INPATH 'C:/Users/zmgre/Desktop/project0all/storm_damge.csv' INTO TABLE damages")

      //spark.sql("SELECT damages.Damages, deaths.Deaths, damages.Name, damages.Rank AS damageRank, deaths.Rank As deathRank FROM damages INNER JOIN deaths ON damages.Name = deaths.Name ORDER BY Damages DESC").show()
      //spark.sql("SELECT * FROM deaths WHERE _c5 != 'null'").show()
    //val catTest = 1
    //spark.sql("SELECT SUM(_c5) AS Damages, COUNT(_c4) AS TS, ROUND(AVG(_c5),1) as avg FROM hdfsdamage WHERE _c4 = '"+catTest+"'").show()
      /*spark.sql("SELECT SUM(_c4) AS Deaths, COUNT(_c3) AS TS, ROUND(AVG(_c4),1) as avg FROM hdfsdeath WHERE _c3 = 'TS'").show()
      spark.sql("SELECT SUM(_c4) AS Deaths, COUNT(_c3) AS Cat1, ROUND(AVG(_c4),1) as avg FROM hdfsdeath WHERE _c3 = 1").show()
      spark.sql("SELECT SUM(_c4) AS Deaths, COUNT(_c3) AS Cat2, ROUND(AVG(_c4),1) as avg FROM hdfsdeath WHERE _c3 = 2").show()
      spark.sql("SELECT SUM(_c4) AS Deaths, COUNT(_c3) AS Cat3, ROUND(AVG(_c4),1) as avg FROM hdfsdeath WHERE _c3 = 3").show()
      spark.sql("SELECT SUM(_c4) AS Deaths, COUNT(_c3) AS Cat4, ROUND(AVG(_c4),1) as avg FROM hdfsdeath WHERE _c3 = 4").show()
      spark.sql("SELECT SUM(_c4) AS Deaths, COUNT(_c3) AS Cat5, ROUND(AVG(_c4),1) as avg FROM hdfsdeath WHERE _c3 = 5").show()*/
    /************************************USING HADOOP*******************************/
    def catVsDeath(): Unit ={
        spark.sql("SELECT SUM(_c4) AS Deaths, COUNT(_c3) AS TS, ROUND(AVG(_c4),1) as avg FROM hdfsdeath WHERE _c3 = 'TS'").show()
        spark.sql("SELECT SUM(_c4) AS Deaths, COUNT(_c3) AS Cat1, ROUND(AVG(_c4),1) as avg FROM hdfsdeath WHERE _c3 = 1").show()
        spark.sql("SELECT SUM(_c4) AS Deaths, COUNT(_c3) AS Cat2, ROUND(AVG(_c4),1) as avg FROM hdfsdeath WHERE _c3 = 2").show()
        spark.sql("SELECT SUM(_c4) AS Deaths, COUNT(_c3) AS Cat3, ROUND(AVG(_c4),1) as avg FROM hdfsdeath WHERE _c3 = 3").show()
        spark.sql("SELECT SUM(_c4) AS Deaths, COUNT(_c3) AS Cat4, ROUND(AVG(_c4),1) as avg FROM hdfsdeath WHERE _c3 = 4").show()
        spark.sql("SELECT SUM(_c4) AS Deaths, COUNT(_c3) AS Cat5, ROUND(AVG(_c4),1) as avg FROM hdfsdeath WHERE _c3 = 5").show()
      }
      /*spark.sql("SELECT COUNT(_c3) AS amountStorms, SUM(_c4) AS DeathsPost1925, ROUND(AVG(_c3),1) AS after1925 FROM deaths WHERE _c2 >= 1925").show()
      spark.sql("SELECT COUNT(_c3) AS amountStorms, SUM(_c4) AS DeathsPre1925, ROUND(AVG(_c3),1) AS before1925 FROM deaths WHERE _c2 < 1925").show()

      //spark.sql("SELECT COUNT(_c3) AS underFour FROM deaths WHERE _c3 < 4").show()
      //spark.sql("SELECT COUNT(_c3) AS fourAbove FROM deaths WHERE _c3 >= 4").show()*/
    def catVsDamages() {
      spark.sql("SELECT SUM(_c5) AS Damages, COUNT(_c4) AS TS, ROUND(AVG(_c5),1) as avg FROM hdfsdamage WHERE _c4 = 'TS'").show()
      spark.sql("SELECT SUM(_c5) AS Damages, COUNT(_c4) AS Cat1, ROUND(AVG(_c5),1) as avg FROM hdfsdamage WHERE _c4 = 1").show()
      spark.sql("SELECT SUM(_c5) AS Damages, COUNT(_c4) AS Cat2, ROUND(AVG(_c5),1) as avg FROM hdfsdamage WHERE _c4 = 2").show()
      spark.sql("SELECT SUM(_c5) AS Damages, COUNT(_c4) AS Cat3, ROUND(AVG(_c5),1) as avg FROM hdfsdamage WHERE _c4 = 3").show()
      spark.sql("SELECT SUM(_c5) AS Damages, COUNT(_c3) AS Cat4, ROUND(AVG(_c5),1) as avg FROM hdfsdamage WHERE _c4 = 4").show()
      spark.sql("SELECT SUM(_c5) AS Damages, COUNT(_c3) AS Cat5, ROUND(AVG(_c5),1) as avg FROM hdfsdamage WHERE _c4 = 5").show()
    }
/************************************************USES HIVE***************************************************************/
    def deathVsDamages(): Unit ={
      spark.sql("SELECT damages.Damages, deaths.Deaths, damages.Name, damages.Rank AS damageRank, deaths.Rank As deathRank FROM damages INNER JOIN deaths ON damages.Name = deaths.Name ORDER BY Damages DESC").show()
    }

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
      val password = "Hardrock1972"
      var connection:Connection = null
      try {
        Class.forName(driver)
        connection = DriverManager.getConnection(url, username, password)
        val statement = connection.createStatement
        //statement.executeUpdate("Delete FROM psw WHERE accountid = 9")
        //statement.executeUpdate("Delete FROM account WHERE id = 9")
        println("Create a username: ")
        val u = scala.io.StdIn.readLine()
        println("Create a password: ")
        val p = scala.io.StdIn.readLine()
        statement.executeUpdate("INSERT INTO stormaccount (username, psw) VALUE ('"+u+"','"+p+"')")
        //val test = 1
        //val rs = statement.executeQuery("SELECT id FROM stormaccount WHERE id = 1")
        //statement.executeUpdate("UPDATE stormaccount SET username = 'testing' WHERE id = 1")
        //val id = 3
        val rs = statement.executeQuery("SELECT * FROM stormaccount")
        //spark.sql("SELECT * FROM stormaccount").show()
        //val result = rs.getString("username")
        //val test = rs.getString("id")
        while (rs.next) {
          val id = rs.getString("id")
          val host = rs.getString("username")
          val user = rs.getString("psw")
          println(id, host, user)
        }
      } catch {
        case e: Exception => e.printStackTrace
      }
      connection.close
    }

    def signIn(){
      val url = "jdbc:mysql://localhost:3306/users"
      val driver = "com.mysql.jdbc.Driver"
      val username = "root"
      val password = "Hardrock1972"
      var connection:Connection = null
      try {
        Class.forName(driver)
        connection = DriverManager.getConnection(url, username, password)
        val statement = connection.createStatement

        println("Enter your username: ")
        val u = scala.io.StdIn.readLine()
        println("Enter your password: ")
        val p = scala.io.StdIn.readLine()
        //val account = statement.executeQuery("SELECT * FROM stormaccount WHERE username = '"+u+"' AND psw = '"+p+"'")

        var continue = 1
        while(continue == 1) {
          val account = statement.executeQuery("SELECT * FROM stormaccount WHERE username = '"+u+"' AND psw = '"+p+"'")
          if (account.next != false) {
            val id = account.getString("id")
            println(id)
            if (id == "1") {
              println("Press 1 to delete an account\nPress 2 to do something else: ")
              val userpick = scala.io.StdIn.readInt()
              if (userpick == 1) {
                val rs = statement.executeQuery("SELECT * FROM stormaccount")
                while (rs.next) {
                  val id = rs.getString("id")
                  val host = rs.getString("username")
                  val user = rs.getString("psw")
                  print(id, host, user)

                  //println("%s, %s, %s".format(id, host,user))
                }
                println("Which ID do you want to delete: ")
                var del = scala.io.StdIn.readInt()
                while (del == 1) {
                  println("Can not delete Admin")
                  println("Which ID do you want to delete: ")
                  del = scala.io.StdIn.readInt()
                }
                statement.executeUpdate("Delete FROM stormaccount WHERE id = '" + del + "'")
              }
            } else {
              //println(id)
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
      println("Press 1 to see the relationship between the Category and the amount of deaths\nPress 2 to see the relationship between Category and Cost of damages: ")
      println("Press 3 to see the relationship between the cost of Damages and the amount of deaths")
      val dataChoice = scala.io.StdIn.readInt()
      if(dataChoice == 1){
        catVsDeath()
      }else if(dataChoice == 2) {
        catVsDamages()
      }else if(dataChoice == 3) {
        deathVsDamages()
      }
    }

    spark.close()
  }
}
