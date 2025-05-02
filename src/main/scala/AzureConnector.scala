//AzureConnector.scala
import org.apache.spark.sql.SparkSession

object AzureConnector {
  def configure(spark: SparkSession): Unit = {
    spark.conf.set("fs.azure.account.key.tripdata126.blob.core.windows.net", "VzN7DC7a5p8h2jhEtP43IqdEh/m+RZ5PaDiaTRVtATdeqdxraCNoiHC7KqE3Gso5Qe4vw16gh7mE+AStu4PFvQ==")
  }
}
