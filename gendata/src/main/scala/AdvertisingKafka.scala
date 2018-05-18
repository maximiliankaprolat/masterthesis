package spark.benchmark

import java.util
import org.apache.spark.SparkConf

import org.json.JSONObject
import kafka.serializer.StringDecoder
import org.apache.kafka.clients.producer._

import org.sedis._
import redis.clients.jedis._
import scala.collection.Iterator
import org.apache.spark.rdd.RDD

import compat.Platform.currentTime

import java.util.Properties
import java.util.UUID
import scala.io.Source._
import scala.io.Source
import scala.util.Random
import java.io.FileNotFoundException

object KafkaAdvertisingProducer
{

	// Verwendete Arrays
	var TypeDefTmp: Array[String] = Array.empty[String]
	var TypeDef_CountTmp: Array[Int] = Array.empty[Int]
	var Page_Ids: Array[String] = Array.empty[String]
	var User_Ids: Array[String] = Array.empty[String]
	var ads: Array[String] = Array.empty[String]
	val ad_types = Array("banner", "modal", "sponsored-search", "mail", "mobile")
	val event_types = Array("view", "click", "purchase")
	
	// Parameter 
	val num_campaigns = 100
	var duration = 120
	var throughput = 50
	var confisnull = true

	// Erzeugung von einem Array mit UUIDs
	def MakeIDs(n: Int): Array[String] = 
	{
		val tmparray = new Array[String](n)

		val x = 0
		for(x <- 0 until n)
		{
			tmparray(x) = UUID.randomUUID().toString()
		}

		return tmparray
	}

	//Config laden
	def LoadConfFile()
	{
		val x = 0

		try
		{
			val lines = Source.fromFile("target/conf")
			// Entferne leere Zeilen aus der Config
			TypeDefTmp = lines.getLines.filter(!_.isEmpty()).toArray

			// Erzeuge Array mit richtige Anzahl
			TypeDef_CountTmp = new Array[Int](TypeDefTmp.length)

			
			if(TypeDefTmp.length == 0)
			{
				println("No params available...")
			}
			else
			{
				confisnull = false
				println("Load params from conf file...")

				// Splitte Eintrag aus der Config in Typ und Laenge auf
				for(x <- 0 until TypeDefTmp.length)
				{
					TypeDef_CountTmp(x) = TypeDefTmp(x).split(" ")(1).toInt
					TypeDefTmp(x) = TypeDefTmp(x).split(" ")(0)
				}   
			}
		}
		catch
		{
			case e: Exception => println("Something goes wrong while loading the conf file...")
		}
	}
  
	def randomString(length: Int) :String = 
	{
		val sb = new StringBuilder
		
		// Erzeuge Alphabet mit Zeichen von a-z und A-Z
		val chars = ('a' to 'z') ++ ('A' to 'Z')

		// Generiere neuen String mithilfe des Arrays
		for (i <- 0 until length)
		{
			val randomNum = scala.util.Random.nextInt(chars.length)
			sb.append(chars(randomNum))
		}
		sb.toString
	}

	def Make_Kafka_Event(t: Long) : String = 
	{
		val del = ","
		val ran = Random
		var kafkamsg = ""

		// Struktur
		// {"user_id":(random_UUID),"page_id":(random_UUID),"ad_id":(rand ads),"ad_type":(rand ad-types),"event_type":(rand event-types),"event_time":(time),"ip_address":"1.2.3.4"}
		kafkamsg += "{"
		kafkamsg += "\"user_id\": \"" + User_Ids(ran.nextInt(100)) + "\", "
		kafkamsg += "\"page_id\": \"" + Page_Ids(ran.nextInt(100)) + "\", "
		kafkamsg += "\"ad_id\": \"" + ads(ran.nextInt(num_campaigns * 10)) + "\", "
		kafkamsg += "\"ad_type\": \"" + ad_types(ran.nextInt(5)) + "\", "
		kafkamsg += "\"event_type\": \"" + event_types(ran.nextInt(3)) + "\", "
		kafkamsg += "\"event_time\": \"" + t + "\"" + ", "
		kafkamsg += "\"ip_address\": \"1.2.3.4\""
      
		// Wenn Parameter vorhanden
		if(confisnull == false)
		{
			kafkamsg += ", "
			
			for(x <- 0 until TypeDefTmp.length)
			{
				// Erzeuge String mit der Funktion: randomString
				if(TypeDefTmp(x) == "String")
				{
				  kafkamsg += "\"" + x + "\": \"" + randomString(TypeDef_CountTmp(x)) + "\""
				}
				
				// Erzeuge Int mit Java-Random-Gen
				if(TypeDefTmp(x) == "Int")
				{
				  kafkamsg += "\"" + x + "\": \"" + ran.nextInt(TypeDef_CountTmp(x)) + "\""
				}
			
				// Fuege delimiter ein
				if(x < TypeDefTmp.length - 1)
				{
				  kafkamsg += del + " "
				}
			}
		}

		kafkamsg += "}"
		//println(kafkamsg)
		return kafkamsg
	}

	def CreateRadomString()
	{
		
		// Umrechnung der Zeit in Nanosekunden
		val start_time = System.currentTimeMillis * 1000000

		// Schreibe Settings fuer Kafka-Producer
		val settings = new Properties()
		settings.put("bootstrap.servers", "localhost:9092")
		settings.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
		settings.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

		val producer = new KafkaProducer[String, String](settings)

		// Standardtopic des Yahoo Streaming Benchmark
		val topic="ad-events"

		val period: Long = (1000000000/throughput)
		var timelist: Array[Long] = new Array[Long](duration * throughput)
	
		// Ausgabe der Duration + Throughput in der Konsole
		println("\n\n\n" + duration + " " + throughput)

		// Berechne Zeitpunkt fuer jedes Event
		for(i <- 0 until duration * throughput)
		{
			timelist(i) = start_time + (period * i)
		}

		
		for(interval <- 0 until duration * throughput)
		{
			var cur = System.currentTimeMillis
			var t: Long = timelist(interval)/1000000

			// Wenn der Zeitpunkt des aktuellen Events noch in der Zukunft liegt warte...
			if(t > cur)
			{
				Thread.sleep(t - cur)
			}

			// Erzeuge neues Event und sende es an Kafka
			producer.send(new ProducerRecord(topic, "key1", Make_Kafka_Event(t)))
		}
	}

	def SetupRedis()
	{
		var i, j = 0

		// Erzeuge Kampagnen fuer den Test
		var Campaign_Ids: Array[String] = MakeIDs(num_campaigns)
		val jedis = new Jedis("localhost")
		
		// Loesche Inhalt innerhalb der Datenbank, falls vorhanden
		jedis.flushAll()

		// Schreibe alle Kampagnen in den Schluessel campaigns fuer spaetere Analyse
		while(i < num_campaigns)
		{
			jedis.sadd("campaigns", Campaign_Ids(i))
			i += 1
		}

		i = 0

		// Schreibe Reklame-Kampagnen-Referenzen
		while(i < num_campaigns * 10)
		{
			while(j < num_campaigns)
			{
				jedis.set(ads(i), Campaign_Ids(j))
				i += 1
				j += 1
			}

			j = 0
		}
	}

	def main(args: Array[String])
	{
		// Wenn keine Parameter gegeben, dann nutze Standardwerte
		if(args.length == 0)
		{
			println("\n\n\nNo param are given!\n\n\n")
		}
		else
		{
			// Setze duration neu
			if(args(0) == "-t")
			{
				println("Time: " + args(1))
				duration = args(1).toInt
			}
			// Setze Durchsatz neu
			if(args(2) == "-l")
			{
				println("Load: " + args(3))
				throughput = args(3).toInt
			}
		}

		println("Start load... --> " + System.currentTimeMillis)
		
		// Erzeuge Ad_Ids
		ads = MakeIDs(num_campaigns * 10)
		// Erzeuge Page_Ids
		Page_Ids = MakeIDs(num_campaigns)
		// Erzeuge User_Ids
		User_Ids = MakeIDs(num_campaigns)
		// Lade Config
		LoadConfFile()
		// Bereite Datenbank vor
		SetupRedis()
		// Schreibe und sende Events an Kafka
		CreateRadomString()
		println("Stop load...bye! --> " + System.currentTimeMillis)
	}
}
