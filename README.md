# Mini-SQL
An SQL Engine for running basic SQL queries using CLI where data is stored in the form of .csv files

### How to run
* **Method 1 :** 
   * `./setup.sh or sh setup.sh`
   * `./run.sh`
   * "Enter queries"

* **Method 2 :** 
   * `./setup.sh or sh setup.sh`
   * `source venv/bin/activate`
   * `python run.py "<Enter Right query syntax with semicolon at the end>"`

### Some Examples
*  `python run.py "select * from table1;"`
*  `python run.py "select table1.A, table2.B from table1, table2 where table1.B = table2.B;"`

