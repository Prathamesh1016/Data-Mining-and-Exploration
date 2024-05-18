# Load required packages
load_packages <- function() {
  library(XML)
  library(RSQLite)
  library(DBI)
}

# Set up database connection
establish_db_connection <- function(db_location) {
  con <- dbConnect(RSQLite::SQLite(), dbname = db_location)
  if (!DBI::dbIsValid(con)) {
    stop("Database connection is not valid.")
  } else {
    cat("Database connection successfully established!\n")
  }
  return(con)
}

# Drop existing tables
drop_tables <- function(con, tables) {
  lapply(tables, function(table) {
    dbExecute(con, paste0("DROP TABLE IF EXISTS ", table))
  })
}

# Create tables
create_tables <- function(con) {
  dbExecute(con, "CREATE TABLE Products (
                  productID INTEGER PRIMARY KEY AUTOINCREMENT,
                  productName TEXT NOT NULL)")
  
  dbExecute(con, "CREATE TABLE Reps (
                  repID INTEGER PRIMARY KEY,
                  firstName TEXT,
                  lastName TEXT,
                  territory TEXT,
                  commission REAL)")
  
  dbExecute(con, "CREATE TABLE Customers (
                  customerID INTEGER PRIMARY KEY AUTOINCREMENT,
                  customerName TEXT,
                  country TEXT)")
  
  dbExecute(con, "CREATE TABLE Sales (
                  saleID INTEGER PRIMARY KEY AUTOINCREMENT,
                  txnID INTEGER,
                  productID INTEGER,
                  repID VARCHAR(255),
                  customerID INTEGER,
                  saleDate DATE,
                  qty INTEGER,
                  total REAL,
                  FOREIGN KEY(productID) REFERENCES Products(productID),
                  FOREIGN KEY(customerID) REFERENCES Customers(customerID))")
}

# Insert representative data
insert_rep_data <- function(con, xml_directory) {
  rep_files <- list.files(xml_directory, pattern = "pharmaReps.*\\.xml$", full.names = TRUE, ignore.case = TRUE)
  lapply(rep_files, function(file_path) {
    xml_data <- xmlParse(file_path)
    rep_nodes <- getNodeSet(xml_data, "//rep")
    
    lapply(rep_nodes, function(rep_node) {
      repID <- xmlGetAttr(rep_node, "rID")
      repID <- substr(repID, 2, nchar(repID))
      firstName <- xmlValue(rep_node[["name"]][["first"]])
      lastName <- xmlValue(rep_node[["name"]][["sur"]])
      territory <- xmlValue(rep_node[["territory"]])
      commission <- xmlValue(rep_node[["commission"]])
      
      lastName <- ifelse(is.na(lastName), "NULL", sprintf("'%s'", lastName))
      commission <- ifelse(is.na(as.numeric(commission)), "NULL", commission)
      
      firstName <- sprintf("'%s'", firstName)
      territory <- sprintf("'%s'", territory)
      repID <- sprintf("'%s'", repID)
      
      query <- sprintf("INSERT INTO Reps (repID, firstName, lastName, territory, commission) VALUES (%s, %s, %s, %s, %s)",
                       repID, firstName, lastName, territory, commission)
      
      tryCatch({
        dbExecute(con, query)
      }, error = function(e) {
        cat("Failed to insert for repID:", repID, "- Error:", e$message, "\n")
      })
    })
  })
}

# Insert or update product
insert_or_update_product <- function(con, productName) {
  query <- sprintf("SELECT productID FROM Products WHERE productName = '%s'", productName)
  existingID <- dbGetQuery(con, query)$productID
  
  if (length(existingID) == 0) {
    dbExecute(con, sprintf("INSERT INTO Products (productName) VALUES ('%s')", productName))
    productID <- dbGetQuery(con, "SELECT last_insert_rowid() AS lastID")$lastID
  } else {
    productID <- existingID
  }
  
  return(productID)
}

# Insert or update customer
insert_or_update_customer <- function(con, customerName, country) {
  escapedCustomerName <- gsub("'", "''", customerName, fixed = TRUE)
  
  query <- sprintf("SELECT customerID FROM Customers WHERE customerName = '%s' AND country = '%s'",
                   escapedCustomerName, country)
  existingID <- dbGetQuery(con, query)$customerID
  
  if (length(existingID) == 0) {
    insertQuery <- sprintf("INSERT INTO Customers (customerName, country) VALUES ('%s', '%s')",
                           escapedCustomerName, country)
    dbExecute(con, insertQuery)
    customerID <- dbGetQuery(con, "SELECT last_insert_rowid() AS lastID")$lastID
  } else {
    customerID <- existingID[1]
  }
  
  return(customerID)
}

# Insert sale
insert_sale <- function(con, txnID, productID, repID, customerID, saleDate, qty, total) {
  query <- sprintf("INSERT INTO Sales (txnID, productID, repID, customerID, saleDate, qty, total) 
                    VALUES (%s, %s, '%s', %s, '%s', %s, %s)",
                   txnID, productID, repID, customerID, saleDate, qty, total)
  tryCatch({
    dbExecute(con, query)
  }, error = function(e) {
    cat("Failed to insert sale:", query, "\nError:", e$message, "\n")
  })
}

# Insert transaction data
insert_transaction_data <- function(con, xml_directory) {
  txn_files <- list.files(xml_directory, pattern = "pharmaSalesTxn.*\\.xml$", full.names = TRUE, ignore.case = TRUE)
  lapply(txn_files, function(file_path) {
    xml_data <- xmlParse(file_path)
    txn_nodes <- getNodeSet(xml_data, "//txn")
    
    lapply(txn_nodes, function(txn_node) {
      txnID <- xmlGetAttr(txn_node, "txnID")
      repID <- xmlGetAttr(txn_node, "repID")
      customerName <- xmlValue(txn_node[["customer"]])
      country <- xmlValue(txn_node[["country"]])
      saleDate <- xmlValue(txn_node[["sale"]][["date"]])
      productName <- xmlValue(txn_node[["sale"]][["product"]])
      qty <- xmlValue(txn_node[["sale"]][["qty"]])
      total <- xmlValue(txn_node[["sale"]][["total"]])
      
      convertDate <- as.Date(saleDate, format="%m/%d/%Y")
      
      productID <- insert_or_update_product(con, productName)
      customerID <- insert_or_update_customer(con, customerName, country)
      
      insert_sale(con, txnID, productID, repID, customerID, convertDate, qty, total)
    })
  })
}

# Fetch and display sample records
fetch_sample_records <- function(con, sampleSize = 10) {
  query1 <- sprintf("SELECT * FROM Products LIMIT %d", sampleSize)
  sample_products <- dbGetQuery(con, query1)
  print(sample_products)
  
  query2 <- sprintf("SELECT * FROM Reps LIMIT %d", sampleSize)
  sample_reps <- dbGetQuery(con, query2)
  print(sample_reps)
  
  query3 <- sprintf("SELECT * FROM Customers LIMIT %d", sampleSize)
  sample_customers <- dbGetQuery(con, query3)
  print(sample_customers)
  
  query4 <- sprintf("SELECT * FROM Sales LIMIT %d", sampleSize)
  sample_sales <- dbGetQuery(con, query4)
  print(sample_sales)
}

# Main execution
main <- function() {
  load_packages()
  db_location <- "pharma.db"
  con <- establish_db_connection(db_location)
  
  tables <- c("Sales", "Customers", "Reps", "Products")
  drop_tables(con, tables)
  create_tables(con)
  
  insert_rep_data(con, "txn-xml")
  insert_transaction_data(con, "txn-xml")
  fetch_sample_records(con)
  
  dbDisconnect(con)
}

main()