package aws_prep


object SaltFactorySample extends SparkSessionManager{

    def main(args: Array[String]) : Unit = {
        implicit  val sparkSession = startSession("Aws_salt_prep")
    }
  
}
