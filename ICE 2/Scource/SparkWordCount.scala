

import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by Mayanka on 09-Sep-15.
  */
object SparkWordCount {

  def main(args: Array[String]) {

    System.setProperty("hadoop.home.dir","F:\\winutils");

    val sparkConf = new SparkConf().setAppName("SparkWordCount").setMaster("local[*]")

    val sc=new SparkContext(sparkConf)

    val input=sc.textFile("input")

    val wc=input.flatMap(line=>{line.split(" ")}).distinct()
    val wc1=wc.map(word=>(word.charAt(0),word)).cache()


    val output=wc1.groupBy(word => word._1).mapValues(_.map(_._2).mkString("(",",",")"))

    output.saveAsTextFile("output4")

    val o=output.collect()

    var s:String="Words:group by first letter \n"
    o.foreach{case(firstletter,word)=>{

      s+=firstletter+" : "+word+"\n"

    }}
    print(s)


/*

    //val map = Map(keyValuePairs : _*)
    val i1=input.map(fields => fields.split("\t")(0) -> 1).cache() //this will create  a key  value pair consisting of userid and 1 (userid,1)

    val ou2=i1.reduceByKey(_+_) //it will reduce the key value pair by key so getting as the count of the user as review a item.

    val ou3=ou2.filter(x =>(x._2 > 25)) //it will provide a filter to get only user that have reviewed more than 25 items.
    ou3.saveAsTextFile("output3")
   // val i=input.map(word=>(word.split("  ")(0),1)).cache()

   */
/*
    val wc=input.flatMap(line=>{line.split(" ")}).map(word=>(word,1)).cache()

    val output=wc.reduceByKey(_+_)

    output.saveAsTextFile("output1")

    val o=output.collect()

    var s:String="Words:Count \n"
    o.foreach{case(word,count)=>{

      s+=word+" : "+count+"\n"

    }}
*/
  }

}
