import org.apache.spark.sql.functions._

def processTags(
  tag_names: Seq[String],
  target_storage_name: String,
  target_container_name: String,
  target_storage_key: String,
  target_file_path: String,
  source_database: String,
  source_table: String
): Unit = {

  val blobStorage = s"$target_storage_name.blob.core.windows.net"
  val absPath = s"wasbs://$target_container_name@$blobStorage/$target_file_path"

  // Set Spark configuration for Azure storage
  spark.conf.set(s"fs.azure.account.key.$blobStorage", target_storage_key)

  // Load the Parquet data
  var df = spark.read.format("parquet").load(absPath)

  // Filter the DataFrame for the specified tags
  val df_specified_tags = df.filter(df("tag_name").isin(tag_names: _*))

  // Process each tag
  tag_names.foreach { tag_name =>

    try {
      // Filter the DataFrame by the tag name
      val df_filtered = df_specified_tags.filter(df_specified_tags("tag_name") === tag_name)
      val target_count = df_filtered.count()

      println(s"$tag_name")

      // Check if filtered DataFrame is empty
      if (target_count == 0) {
        println(s"        No data in target found for this tag.")
      } else {
        // Find the minimum value of ts_utc in the target path
        val min_ts_utc = df_filtered.agg(min("ts_utc")).first().get(0)

        println(s"        Minimum tag time:       $min_ts_utc")
        println(s"        Target count:           $target_count")

        // Convert tag_names to a comma-separated string for SQL IN clause
        val tag_names_sql = s"'$tag_name'"

        // Execute the SQL query using Spark SQL for the source table
        val query = s"""
          SELECT tag_name, COUNT(*)
          FROM ${source_database}.${source_table}
          WHERE tag_name IN ($tag_names_sql) AND ts_utc >= "$min_ts_utc"
          GROUP BY tag_name
        """
        val result = spark.sql(query)

        if (result.isEmpty) {
          println(s"        No data in source found for this tag.")
        } else {
          val source_count = result.collect().head.getLong(1) // Get the count from the result
          println(s"        Source count:           $source_count")

          // Assert that the counts are equal
          assert(target_count == source_count, s"❌ Counts do not match")
          println(s"        ✅ Counts match")
        }
        println("--------------------------------------------------------------------------------------------------------")
      }

    } catch {
      // Catch any exception that occurs during processing and log it
      case e: Exception =>
        println(s"Error processing tag $tag_name: ${e.getMessage}")
        // Optionally, log the full stack trace for debugging purposes
        e.printStackTrace()
    }
  }
}
