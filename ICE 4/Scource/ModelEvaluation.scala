import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.rdd.RDD

/**
 * Created by Mayanka on 14-Jul-15.
 */
object ModelEvaluation {
  def evaluateModel(predictionAndLabels: RDD[(Double, Double)]) = {
    val metrics = new MulticlassMetrics(predictionAndLabels)
    val cfMatrix = metrics.confusionMatrix
    println(" |=================== Confusion matrix ==========================")
    println(cfMatrix)
    println(metrics.fMeasure)
    var tp=cfMatrix(0,0)
    var tn=cfMatrix(0,1)
    var fp=cfMatrix(1,0)
    var fn=cfMatrix(1,1)
    var t=predictionAndLabels.count().toDouble
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
//
//    printf(
//      s"""
//         |=================== Confusion matrix ==========================
//         |          | %-15s                     %-15s
//         |----------+----------------------------------------------------
//         |Actual = 0| %-15f                     %-15f
//         |Actual = 1| %-15f                     %-15f
//         |===============================================================
//         """.stripMargin, "Predicted = 0", "Predicted = 1",
//      cfMatrix.apply(0, 0), cfMatrix.apply(0, 1), cfMatrix.apply(1, 0), cfMatrix.apply(1, 1))
//
//    println("\nACCURACY " + ((cfMatrix(0,0) + cfMatrix(1,1))/(cfMatrix(0,0) + cfMatrix(0,1) + cfMatrix(1,0) + cfMatrix(1,1))))


    //cfMatrix.toArray

//    val fpr = metrics.falsePositiveRate(0)
//    val tpr = metrics.truePositiveRate(0)
//
//    println(
//      s"""
//         |False positive rate = $fpr
//          |True positive rate = $tpr
//     """.stripMargin)
//
//    val m = new BinaryClassificationMetrics(predictionAndLabels)
//    println("PR " + m.areaUnderPR())
//    println("AUC " + m.areaUnderROC())
  }
}
