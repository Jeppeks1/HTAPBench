# HTAPBench

HTAPBench is derived from from OLTPBenchmark framework @ https://github.com/oltpbenchmark/oltpbench

The Hybrid Transactional and Analytical Processing Benchmark is targeted at assessing engines capable of delivering mixed workloads composed of OLTP transactions and OLAP business queries without resorting to ETL.

There are a few requirements to run HTAPBench:
1. You need to have JAVA distribution (> 1.7) installed on the machine running HTAPBench.
2. You need to have installed the JDBC driver for the database you want to test.

# A. Build HTAPBench:

If the complex HTAP workload is desired, clone the GitHub repository called "ComplexHTAP". Otherwise, use the master branch for a normal HTAP workload. Then run:

```bash
	mvn clean compile package
```

# B. Configure HTAPBench:
Clone and adjust the example configuration file in config/htapb_config_postgres.xml to you test case.

Before you continue ensure that:
- The database engine you wish to test is installed and that you can reach it from the machine running HTAPBench.
- That the database engine to be tested is configured with the required memory and that the max_clients allowance is enough for you setup.
- In the database engine to be tested, create a test database e.g., htapb.
- In the database engine to be tested, create a user/password and grant all privileges to your test database.

# C. Run Tests
Before running any tests ensure that the configuration stage was successfully completed. 

Then run:
```bash
java -cp .:target/htapbench-0.95-jar-with-dependencies.jar:/usr/share/java/mysql-connector-java-8.0.15.jar pt.haslab.htapbench.core.HTAPBench -b htapb -c config/htapb_config_mysql.xml --mode execute --workload htap --sequence true --strategy fixed
```

The `--mode` option allows the user to control which stages of the benchmark phase are run. The options are:
* Configure: Installs the relevant schema in the database
* Generate: Generates the csv files for the given target TPS specified in the configuration file
* Populate: Attempts to fill the database with the data from the csv files
* Execute: Executes the actual workload against the database

The `--sequence` option gives the user control over which of the above stages are execute. If `--sequence true`, all the previous modes are performed in sequence. For example, the user can specify `--mode populate` and `--sequence true`, in order to install the schema in the database, generate the required csv files and populate the database with the data, but the actual execution is skipped. 

This is very useful when performing multiple tests in a row. The `--overwrite true` option can be set to forcefully drop an existing database, allowing the user to run the next experiment with just a single command. If the `--overwrite false` option is used, but an existing database contains data, the benchmark will ignore the option and continue as normal.

# Publications
If you are using this benchmark for your papers or for your work, please cite the paper:

HTAPBench: Hybrid Transactional and Analytical Processing Benchmark 
Fábio Coelho, João Paulo, Ricardo Vilaça, José Pereira, Rui Oliveira
Proceedings of the 8th ACM/SPEC on International Conference on Performance Engineering

BibTex:
```bash
@inproceedings{Coelho:2017:HHT:3030207.3030228,
 author = {Coelho, F\'{a}bio and Paulo, Jo\~{a}o and Vila\c{c}a, Ricardo and Pereira, Jos{\'e} and Oliveira, Rui},
 title = {HTAPBench: Hybrid Transactional and Analytical Processing Benchmark},
 booktitle = {Proceedings of the 8th ACM/SPEC on International Conference on Performance Engineering},
 series = {ICPE '17},
 year = {2017},
 isbn = {978-1-4503-4404-3},
 location = {L'Aquila, Italy},
 pages = {293--304},
 numpages = {12},
 url = {http://doi.acm.org/10.1145/3030207.3030228},
 doi = {10.1145/3030207.3030228},
 acmid = {3030228},
 publisher = {ACM},
 address = {New York, NY, USA},
 keywords = {benchmarking, htap, olap, oltp},
} 
```



