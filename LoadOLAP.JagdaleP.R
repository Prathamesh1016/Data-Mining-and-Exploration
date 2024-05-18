# PART 2: Creating Star/Snowflake Schema: Transforming the normalized schema into a denormalized schema suitable for Online Analytical Processing (OLAP) and populating an analytical database in MySQL.

# Loading required packages
library(RMySQL)
library(RSQLite)
library(sqldf)
library(tidyr)
options(sqldf.driver = "SQLite")

# Connect to SQLite Database
sqlite_conn <- dbConnect(RSQLite::SQLite(), dbname = "practicum2.db")

# Check SQLite connection
if (!dbIsValid(sqlite_conn)) {
  stop("Failed to connect to SQLite database.")
} else {
  cat("Connected to SQLite database successfully.\n")
}

# Connect to MySQL Database
mysql_conn <- dbConnect(
  RMySQL::MySQL(),
  dbname = "practicum2",
  host = "database-2.cr84qqy6ksk7.us-east-2.rds.amazonaws.com",
  port = 3306,
  user = "admin",
  password = "practicum2"
)

# Check MySQL connection
if (dbIsValid(mysql_conn)) {
  cat("Connection to MySQL database successful!\n")
} else {
  stop("Failed to connect to the MySQL database.\n")
}

# Drop existing tables
drop_tables <- c("product_facts", "rep_facts")
lapply(drop_tables, function(table) {
  tryCatch(
    dbRemoveTable(mysql_conn, table),
    error = function(e) {}
  )
})

# Transfer data from SQLite to MySQL
sqlite_tables <- dbListTables(sqlite_conn)
lapply(sqlite_tables, function(table) {
  data <- dbReadTable(sqlite_conn, table)
  dbWriteTable(mysql_conn, table, data, overwrite = TRUE)
  cat(sprintf("Data in MySQL table %s:\n", table))
  print(dbGetQuery(mysql_conn, sprintf("SELECT * FROM %s LIMIT 5;", table)))
})

create_product_facts <- function() {
  dbExecute(mysql_conn, "
    CREATE TABLE IF NOT EXISTS product_facts (
      productID INT,
      productName VARCHAR(255),
      month INT,
      year INT,
      quarter INT,
      territory VARCHAR(255),
      totalAmountSold INT,
      totalUnitsSold INT,
      PRIMARY KEY (productID, month, year, quarter, territory)
    );
  ")
  
  dbExecute(mysql_conn, "
    INSERT INTO product_facts
    SELECT
      p.productID,
      MAX(p.productName) AS productName,
      MONTH(s.saledate) AS month,
      YEAR(s.saledate) AS year,
      QUARTER(s.saledate) AS quarter,
      r.territory,
      SUM(s.total) AS totalAmountSold,
      SUM(s.qty) AS totalUnitsSold
    FROM Sales s
    JOIN Products p ON s.productID = p.productID
    JOIN Reps r ON s.repID = r.repID
    WHERE s.saledate IS NOT NULL
    GROUP BY p.productID, YEAR(s.saledate), QUARTER(s.saledate), r.territory;
  ")
  
  cat("Data in product_facts table:\n")
  print(dbGetQuery(mysql_conn, "SELECT * FROM product_facts LIMIT 5;"))
}

create_rep_facts <- function() {
  dbExecute(mysql_conn, "
    CREATE TABLE IF NOT EXISTS rep_facts (
      repID VARCHAR(255),
      repName VARCHAR(255),
      month INT,
      year INT,
      quarter INT,
      productID INT,
      totalSold DECIMAL(10,2),
      averageSold DECIMAL(10,2),
      PRIMARY KEY (repID, year, quarter, month, productID)
    );
  ")
  
  dbExecute(mysql_conn, "
    INSERT INTO rep_facts (repID, repName, month, year, quarter, productID, totalSold, averageSold)
    SELECT
      r.repID,
      MAX(CONCAT(r.firstname, ' ', r.lastname)) AS repName,
      MONTH(s.saledate) AS month,
      YEAR(s.saledate) AS year,
      QUARTER(s.saledate) AS quarter,
      s.productID,
      SUM(s.total) AS totalSold,
      AVG(s.total) AS averageSold
    FROM Sales s
    JOIN Reps r ON s.repID = r.repID
    WHERE s.saledate IS NOT NULL
    GROUP BY r.repID, YEAR(s.saledate), QUARTER(s.saledate), s.productID;
  ")
  
  cat("Data in rep_facts table:\n")
  print(dbGetQuery(mysql_conn, "SELECT * FROM rep_facts LIMIT 50;"))
}


main <- function() {
  create_product_facts()
  create_rep_facts()
}
main()

# Query 1: Total sold for each quarter of 2021 for 'Alaraphosol'
query1 <- "
  SELECT
    pf.quarter,
    SUM(pf.totalAmountSold) AS total_sold
  FROM product_facts pf
  JOIN Products p ON pf.productID = p.productID
  WHERE p.productName = 'Alaraphosol' AND pf.year = 2021
  GROUP BY pf.quarter;
"
cat("Query 1 result:\n")
print(dbGetQuery(mysql_conn, query1))

