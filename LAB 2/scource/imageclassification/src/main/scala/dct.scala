
  import java.nio.file.{Files, Paths}
  import IPApp._
  import org.apache.spark.mllib.clustering.KMeansModel
  import org.apache.spark.mllib.linalg.Vectors
  import org.apache.spark.mllib.regression.LabeledPoint
  import org.apache.spark.{SparkConf, SparkContext}
  import org.apache.spark.mllib.tree.DecisionTree
  import org.apache.spark.mllib.tree.model.DecisionTreeModel
  import org.apache.spark.mllib.util.MLUtils

  object dct {

    def generateDecissionTreeModel(sc: SparkContext): Unit = {
      // Load and parse the data file.

      if (Files.exists(Paths.get(IPSettings.DECESSION_TREE_FOREST_PATH))) {
        println(s"${IPSettings.DECESSION_TREE_FOREST_PATH} exists, skipping Decession tree model formation..")
        return
      }

      val data = sc.textFile(IPSettings.HISTOGRAM_PATH)
      import java.nio.file.{Files, Paths}
      val parsedData = data.map { line =>
        val parts = line.split(',')
        LabeledPoint(parts(0).toDouble, Vectors.dense(parts(1).split(' ').map(_.toDouble)))
      }

      // Split data into training (70%) and test (30%).
      val splits = parsedData.randomSplit(Array(0.7, 0.3), seed = 11L)
      print("splits size  = " + splits.size)
      val trainingData = splits(0)
      val testData = splits(1)


      // Train a DecisionTree model.
      //  Empty categoricalFeaturesInfo indicates all features are continuous.
      val numClasses = 4
      val categoricalFeaturesInfo = Map[Int, Int]()
      val impurity = "gini"
      val maxDepth = 5
      val maxBins = 32

      val model = DecisionTree.trainClassifier(trainingData, numClasses, categoricalFeaturesInfo,
        impurity, maxDepth, maxBins)

      // Evaluate model on test instances and compute test error
      val labelAndPreds = testData.map { point =>
        val prediction = model.predict(point.features)
        (point.label, prediction)
      }
      val testErr = labelAndPreds.filter(r => r._1 != r._2).count().toDouble / testData.count()
      println("Test Error = " + testErr)
      println("Learned classification tree model:\n" + model.toDebugString)

      // Save and load model
      model.save(sc, IPSettings.DECESSION_TREE_FOREST_PATH)
      val sameModel = DecisionTreeModel.load(sc, IPSettings.DECESSION_TREE_FOREST_PATH)
    }


    def main(args: Array[String]) {

      System.setProperty("hadoop.home.dir","F:\\winutils");

      val conf = new SparkConf()
        .setAppName(s"IPApp")
        .setMaster("local[*]")
        .set("spark.executor.memory", "6g")
        .set("spark.driver.memory", "6g")
      val sparkConf = new SparkConf().setAppName("SparkWordCount").setMaster("local[*]")

      val sc=new SparkContext(sparkConf)



      /**
        * From the labeled Histograms a Decission Tree Model is created
        */
      generateDecissionTreeModel(sc)

      //    testImageClassification(sc)

      val testImages = sc.wholeTextFiles(s"${IPSettings.TEST_INPUT_DIR}/*/*.jpg")
      val testImagesArray = testImages.collect()
      var predictionLabels = List[String]()
      testImagesArray.foreach(f => {
        println(f._1)
        val splitStr = f._1.split("file:/")
        val predictedClass: Double = classifyDCImage(sc, splitStr(1))
        val segments = f._1.split("/")
        val cat = segments(segments.length - 2)
        val GivenClass = IMAGE_CATEGORIES.indexOf(cat)
        println(s"Predicting test image : " + cat + " as " + IMAGE_CATEGORIES(predictedClass.toInt))
        predictionLabels = predictedClass + ";" + GivenClass :: predictionLabels
      })

      val pLArray = predictionLabels.toArray

      predictionLabels.foreach(f => {
        val ff = f.split(";")
        println(ff(0), ff(1))
      })
      val predictionLabelsRDD = sc.parallelize(pLArray)


      val pRDD = predictionLabelsRDD.map(f => {
        val ff = f.split(";")
        (ff(0).toDouble, ff(1).toDouble)
      })
      val accuracy = 1.0 * pRDD.filter(x => x._1 == x._2).count() / testImages.count

      println(accuracy)
      ModelEvaluation.evaluateModel(pRDD)


    }
    def classifyDCImage(sc: SparkContext, path: String): Double = {

      val model = KMeansModel.load(sc, IPSettings.KMEANS_PATH)
      val vocabulary = ImageUtils.vectorsToMat(model.clusterCenters)

      val desc = ImageUtils.bowDescriptors(path, vocabulary)

      val histogram = ImageUtils.matToVector(desc)

      println("--Histogram size : " + histogram.size)

      val nbModel = DecisionTreeModel.load(sc, IPSettings.DECESSION_TREE_FOREST_PATH)
      //println(nbModel.labels.mkString(" "))

      val p = nbModel.predict(histogram)
      //    println(s"Predicting test image : " + IMAGE_CATEGORIES(p.toInt))

      p
    }
  }


