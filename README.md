# 🚀 **Efficient Ways to Load Large Datasets into SQL Server** 🗄️

Handling large datasets can be challenging, especially when transferring them to a SQL Server hosted on-premises. This project demonstrates **three efficient ways** to upload datasets larger than **3 GB** from a local machine to an on-premises SQL Server. Whether you're dealing with performance bottlenecks or want to explore different approaches, these methods will help you move data quickly and effectively. 💾

---

## ⚡ Aparklyr + dbplyr 🌟

For large datasets, this approach leverages Apache Spark to distribute data processing, making it ideal for handling massive files (3 GB+). dbplyr allows you to write dplyr-like code that translates directly into SQL commands, all executed within the Spark environment. Here's a quick 

## 🏎️ Approach 3: data.table + ODBC + DBI 💨

When speed is crucial, the combination of data.table and DBI delivers blazing performance. data.table is incredibly fast for in-memory operations, and DBI ensures a smooth and secure connection to your SQL Server.

## 🔍 Why These Approaches?

Scalability: Handle datasets over 3 GB seamlessly.
Flexibility: Choose the approach that best suits your data structure and workflow.
Efficiency: Leverage R's ecosystem for both in-memory and distributed data processing.

## 📚 Getting Started

1. Install the required packages:

```r
install.packages(c("sparklyr", "dplyr", "dbplyr", "DBI", "odbc", "data.table", "janitor"))
```

2. Clone this repository (SSH):

```r
git clone git@github.com:RoldanRamon/kms_BigData_LoadDataTo_SQLServer.git
```

3. Ensure you have Java installed for sparklyr:

Install Java for Windows
Install Java for Linux
Install Java for Mac

4. Run one of the provided scripts to see the methods in action!

## 🏗️ Project Structure

```r
📁 root/
├── 📄 README.md
├── 📂 scripts/
│   ├── odbc_dplyr.R        # Approach 1: ODBC + dplyr
│   ├── sparklyr_dbplyr.R   # Approach 2: Sparklyr + dbplyr
│   └── data_table_dbi.R    # Approach 3: data.table + DBI
│   ├── initial_data_read.py  # Python script used to read the data initially
│   └── create_tables.sql   # SQL script to create tables with correct data types
└── 📂 data/
    └── large-dataset.csv   # Example dataset (replace with your own)
```

### 🐍 Python Script (initial_data_read.py)
This Python script is used to read the large dataset before it's passed to R for further processing and loading into SQL Server.

### 🛢️ SQL Script (create_tables.sql)
This SQL script is used to create the necessary tables in SQL Server with the correct primitive data types before loading the dataset.

## 📞 Support
Feel free to raise an issue if you run into any problems or have questions. You can also reach me on LinkedIn or via email.

## 🎉 Contributions
Contributions are welcome! If you'd like to improve the scripts or add new methods, feel free to open a pull request.