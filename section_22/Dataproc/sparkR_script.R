# Load SparkR library
library(SparkR)

# Initialize Spark session
sparkR.session(appName = "SparkR SQL Example")

# Function to read data from GCS and perform SQL operations
main <- function(gcs_path) {
  # Read CSV file from GCS
  df <- read.df(gcs_path, source = "csv", header = "true", inferSchema = "true")

  # Create a temporary view
  createOrReplaceTempView(df, "data")

  # Execute SQL query
  result <- sql("SELECT * FROM data WHERE age > 30")

  # Show the result
  showDF(result)

  # Stop the Spark session
  sparkR.session.stop()
}

# Check for command line arguments
args <- commandArgs(trailingOnly = TRUE)
if (length(args) != 1) {
  stop("Usage: sparkR_script.R <gcs_path>")
}

# Pass the GCS path to the main function
gcs_path <- args[1]
main(gcs_path)
