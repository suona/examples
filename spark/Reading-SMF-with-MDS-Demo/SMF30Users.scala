package com.ibm.zos.smf.SparkSMF
object SMF30Users {
/* Sample Scala code for accessing SMF Data using    
 * Spark on Z and MDS     
 *                                                    
 *  LICENSED MATERIALS - PROPERTY OF IBM              
 *  5655-OD1 COPYRIGHT IBM CORP. 2017           
 *                                                    
 *  STATUS = HSPK120                                  
 */   
 import org.apache.spark.SparkContext
 import org.apache.spark.SparkConf
 import org.apache.spark.sql.SparkSession
 import org.apache.spark.sql.DataFrame
 import com.rs.jdbc.dv.DvDriver
  
 var dsn = ""
 var pwd = ""
 var usr = ""
 var spark = SparkSession.builder().getOrCreate()
      val table = "SMF_03000_SMF30ID" //virtual table name

      val driverClassName = "com.rs.jdbc.dv.DvDriver"   
      
      
      def main(args: Array[String]): Unit = {
    

   if(args.length == 1)
   { 
     println("Reading JSON data from " + args(0))
     val source = args(0)
     val df = spark.read.json(source)
     
    getTopUsers(df,10).show(false)
   }
   
   else if( args.length == 3){
   println("Reading from DataSet")  
   this.dsn = args(0) 
   this.usr = args(1)
   this.pwd = args(2)   
      
    val dbtable = table + "__" + this.dsn;    
    
     
     val df = loadDataSet()
     
    
    
    getTopUsers(df,10).show(false)
     
   }
     else if (args.length ==2 || args.length == 0){
     println("Error: Please enter argument in one of the following forms:")
     println("1: path/To/Json/RDD")
     println("2: DataSetName, UserID, Password (where the dataset name is seperated with \"_\" instead of \".\") ")
     
   }
   else {
     var i = 3
     while(i < args.length){
     println("Error: Unrecognized additional parameter: "+ args(i))
     i += 1
     }
     println("Error: Please enter argument in one of the following forms:")
     println("1: path/To/Json/RDD")
     println("2: DataSetName, UserID, Password (where the dataset name is seperated with \"_\" instead of \".\") ")
   } 
      
      
    
    
  }
  
 
 def loadDataSet() : DataFrame ={
    val url = "jdbc:rs:dv://your.mds.sys.com;DBTY=DVS;CompressionType=UNCOMPRESSED;" 
    val dbtable = table + "__" + this.dsn;    
   val df = spark.read
      .format("jdbc")
      .option("driver", "com.rs.jdbc.dv.DvDriver")
      .option("url", url )
      .option("dbtable" , dbtable)
      .option("user",usr)//add user credentials
      .option ("password", pwd)
      .load() 
      println("succesfully created dataframe");
   return df
 }
 
  
   
    
 
   
    /**
    * returns a number (num) of the most commonly occuring RACF userids for a 
    * given smf30 dataset (df)
    */
   def getTopUsers(df: DataFrame , num: Int ): DataFrame = {
     val res = df.groupBy("SMF30RUD").count()
     return res.orderBy(res.col("count").desc).limit(num)
   }
   
}