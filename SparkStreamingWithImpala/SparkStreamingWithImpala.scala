package etu.spark.streaming
import org.apache.spark._
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.sql.SQLContext
import org.apache.spark.storage.StorageLevel
import scala.util.matching.Regex
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.types.StructType
import org.apache.spark.sql.catalyst.types.StructField
import org.apache.spark.sql.catalyst.types.StringType
import org.apache.spark.sql.hive.HiveContext
import java.util.Locale
import etu.impala.ImpalaClient
import collection.JavaConversions._
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator
import org.apache.hadoop.fs._
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.conf._
object SparkStreamingWithImpala extends serializable{
  
  val dateTimeRegex="[0-9]{1,2}/[a-zA-z]{3}/[0-9]{4}:[0-9]{1,2}:[0-9]{1,2}:[0-9]{1,2}".r
  val dateSdt = new java.text.SimpleDateFormat("yyyy-MM-dd")
  val timeSdt = new java.text.SimpleDateFormat("HH:mm:ss")
  val dateTimdSdt =new java.text.SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss",Locale.ENGLISH)
  val nginxLogRegex ="^([\\d.]+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"(.+?)\" (\\d{3}) (\\d+) \"([^\"]*)\" \"([^\"]+)\"".r
  val schemaString = "ip,time,cid,act,cat,uid,pid,plist,eruid,pry,oid,keywords,eturec,url,hostname,agent" 
  case class Logs(ip:String,time:String,cid:String,act:String,cat:String,uid:String,pid:String,plist:String,eruid:String,pry:String,oid:String,keywords:String,eturec:String,url:String,hostname:String,agent:String) 
 def main(args:Array[String]){
	
		if(args.length<2){
		  System.err.println("Usage: SparkStreamingWithImpala <hostname> <port>")
 		  System.exit(1)
		}
		val fileSystem = FileSystem.get(new Configuration)		

		//initalthe the config, and declare a spark context with the config
		val conf=new SparkConf().setAppName("SparkStreamingWithImpala")
		val sc=new SparkContext(conf)
		
		//declare the streamingcontext with sparkcontext, and the input data will be divided into batches in every 5 seconds
		val ssc=new StreamingContext(sc,Seconds(5))
		//receive the input data by listening the network socket on the spefic hostname and port , and the input data will be store in the memory
		val lines=ssc.socketTextStream(args(0), args(1).toInt, StorageLevel.MEMORY_ONLY)
		
		val logger = Logger.getRootLogger
		val erlogger = Logger.getLogger("SparkStreamingWithImpala")		

		//declare the sqlcontext with sparkcontext
		val sqc=new SQLContext(sc)	
		import sqc._		
		
		//according the the content of variable "schemaString" to generate the table schema
		val schema= StructType(schemaString.split(",").map(a=>StructField(a,StringType,true)))

		//parsing the input date, and keep the cache the result
		val parsed= lines.map(parserLog(_)).map(x=>Logs(x.get("ip").getOrElse(""),x.get("time").getOrElse(""),x.get("cid").getOrElse(""),x.get("act").getOrElse(""),x.get("cat").getOrElse(""),x.get("uid").getOrElse(""),x.get("pid").getOrElse(""),x.get("plist").getOrElse(""),x.get("eruid").getOrElse(""),x.get("pry").getOrElse(""),x.get("oid").getOrElse(""),x.get("keywords").getOrElse(""),x.get("eturec").getOrElse(""),x.get("url").getOrElse(""),x.get("hostname").getOrElse(""),x.get("agent").getOrElse(""))).cache
		//for every input data, writing the parsing result into Impala
		//parsed.foreachRDD(x => { try{ x.collect.foreach(insertIntoHiveTable(_))}catch{case e:Exception=>logger.error(e)}})		
		parsed.foreachRDD(x=>{
		if(x.count>0){
			val str=x.map(y=> y.ip+","+y.time+","+y.cid+","+y.act+","+y.cat+","+y.uid+","+y.pid+","+y.plist+","+y.eruid+","+y.pry+","+y.oid+","+y.keywords+","+y.eturec+","+y.url+","+y.hostname+","+y.agent)	
			//output the content of RDD to hdfs, and using the "load data" command to load the file into impala
			str.saveAsTextFile("/tmp/spark/SparkStreamingWithImpala")
			fileSystem.setPermission(new Path("/tmp/spark/SparkStreamingWithImpala"),new FsPermission("777"))
			insertIntoHiveTable("/tmp/spark/SparkStreamingWithImpala")
			//after loading process, remove the temp directory
			fileSystem.delete(new Path("/tmp/spark/SparkStreamingWithImpala"),true)
		}})
		//process the data in the latest 30 seconds, and scan it every 10 seconds
		
		   parsed.window(Seconds(30),Seconds(10)).foreachRDD(rdd=>{
		   //apply the data with table schema, and register the data into sparksql with table name
		   rdd.registerTempTable("logs")
		   //query the table with specfic sql command, and output the result through logger
	           val results = sqc.sql("SELECT count(*)  FROM logs").collect
		   results.foreach( x => erlogger.info("Record counts in TABLE logs:"+x))
		})
		
		ssc.start()
		ssc.awaitTermination()
	}

	//parsing each of the log raw data to a map object
	def parserLog(line: String): Map[String,String] ={
          var parsed =  Map[String,String]()

          val res=(dateTimeRegex findAllIn line).mkString(" ")
          val dateTime=dateTimdSdt.parse(res)
          val date=dateSdt.format(dateTime)
          val time=timeSdt.format(dateTime)

          val nginxLogRegex(p1,p2,p3,p4,p5,p6,p7,p8,p9)=line

          parsed+=("ip"->p1)
          parsed+=("time"->(date+time))

          val request = p5.split(" ")
          val reqParms=request(1).split(";")
          val act = reqParms(0).substring(1,reqParms(0).length-1)
          parsed+=("act"->act)

	  val URL= new java.net.URL(p8)
          parsed+=("hostname"->URL.getHost)
          parsed+=("url"->p8)
          parsed+=("agent"->p9)

          for(i<-1 until reqParms.length){
               val reqParm = reqParms(i).split("=")
               if(reqParm.length == 2){
                    if(reqParm(0)=="eturec"){
                         if(reqParm(1)=="1"){
                              parsed+=("eturec"->"true")
                         }else{
                              parsed+=("eturec"->"false")
                         }

                    }else{
                         parsed+=(reqParm(0) -> reqParm(1))
                    } 
               } else{
                     if(reqParm(0)=="eturec"){
                              parsed+=("eturec"->"false")
                    }else{
                         parsed+=(reqParm(0) -> "")
                    }
               }
          }

          return parsed
      }
	//calling the impala client to load the content in specific directory into Impala
	def insertIntoHiveTable(path:String){
		val sql="LOAD DATA INPATH \'"+path+"\' INTO TABLE Logs"		
		val client=new ImpalaClient("127.0.0.1","21050")
		client.executeSql(sql)
	}
	
}
