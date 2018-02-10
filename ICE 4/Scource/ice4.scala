import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object ice4 {


  def main(args: Array[String]): Unit = {



    System.setProperty("hadoop.home.dir", "F:\\winutils");
    val sparkConf = new SparkConf().setAppName("ice4").setMaster("local[*]")

    val sc = new SparkContext(sparkConf)

    // Turn off Info Logger for Consolexxx
    Logger.getLogger("org").setLevel(Level.OFF);
    Logger.getLogger("akka").setLevel(Level.OFF);
    // Load and parse the data



//val predictionLabels=(1,0,1,1,0,0,1,1,0,1,0,0,0,0,1,1,1,0,0,0);
    val predictionLabels=sc.parallelize(Seq((0.0,1.0),(1.0,1.0),(0.0,0.0),(1.0,1.0),(1.0,0.0),(0.0,0.0),(0.0,0.0),(1.0,1.0),(0.0,1.0),(0.0,0.0)));

val accuracy = 1.0 * predictionLabels.filter(x => x._1 == x._2).count() / 10
   println(accuracy)
    //ModelEvaluation.evaluateModel(pRDD)
    ModelEvaluation.evaluateModel(predictionLabels)
// woman 0 a-yes 5 a-no 5  p-yes 6
    // man 1

    /*
var tp=4.0
var tn=3.0
var fp=2.0
var fn=1.0
var t=predictionLabels.count().toDouble
    println("accuracy:",(tp+tn)/t)

      println(" Misclassification Rate:",(fp+fn)/t)
  //  True Positive Rate: tp/a-yes
    println("True Positive Rate:",tp/5)
    // False Positive Rate: fp/a-no
    println(" False Positive Rate:",fp/5)
// Specificity: tn/a-no
    println("Specificity:",tn/5)
    // Precision: tp/p-yes
    println(" Precision:",tp/6)
    //Prevalence:a-yes/t
    println("Prevalence:",5/t)
  */
  }

}
