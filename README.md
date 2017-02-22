pydistcp
==================================

A python WebHDFS/HTTPFS based tool for inter/intra-cluster data copying. This tool is very suitable for multiple mid or small size files cross-clusters copy. Compared to the normal distcp which adds a lot of overhead time for subtimitting the map-reduce job then waiting for YARN to schedule it...,  pydistcp uses webhdfs to stream the data from source cluster datanodes directly to destination cluster datanodes using multiple parallel threads. 

When transferring few huge files, the normal distcp may be faster, but when transferring lot of small, midsize or relatively big file,  pydistcp provides a very good performance.

```bash
  $ pydistcp -f -s staging -d prod /data/outgoing /data/incoming --threads=10 --part-size=131072
  27.1%   [ pending: 32 | transferring: 6 | complete: 4 ]
```

```json
Job Status:
{
  "Size Failed": 0,
  "Size Copied": 257721641,
  "Source Path": "/data/t100",
  "Size Expected": 257721641,
  "Files Expected": 42,
  "Files Failed": 0,
  "Destination Path": "/data/t200",
  "Start Time": "2017-02-22 17:39:29",
  "Files Skipped": 0,
  "Size Deleted": 0,
  "End Time": "2017-02-22 17:39:50",
  "Files Copied": 42,
  "Files Deleted": 0,
  "Duration": 20.756325006484985,
  "Outcome": "Successful",
  "Size Skipped": 0
}
```

Pydistcp uses [ pywhdfs ](https://github.com/yassineazzouz/pywhdfs) for establishing connections with WEBHDFS/HTTPFS source and destination clusters.

Features
--------

* Pydistcp is based on pywhdfs project to establish WebHDFS and HTTPFS connections with source and destination clusters,
  so all clusters configurations supported in  pywhdfs are also supported in pydistcp:
   - Support both secure (Kerberos,Token) and insecure clusters
   - Supports HA cluster and handle namenode failover
   - Supports HDFS federation with multiple nameservices and mount points.
* Supports data copy between secure and insecure clusters
* Supports data copy between clusters using different kerberos realms using token authentication
* Supports data copy between encrypted and non encrypted clusters
* Json format clusters configuration.
* Perform concurent multithreaded data copy.


Getting started
---------------

```bash
  $ easy_install pydistcp
```


Configuration
---------------

Pydistcp share the same json configuration file used by [ pywhdfs ](https://github.com/yassineazzouz/pywhdfs).
Please refer to the project readme file for details about the json configuration schema.

USAGE
-------

There are multiple arguments you can use to alter the way the copy works, or to inhence the performace of the job depending on the size of the server you use.
Use the help argument to display the full list of supported parameters:

```bash
  $ pydistcp --help
  pydistcp: A python Web HDFS based tool for inter/intra-cluster data copying.

  Usage:
    pydistcp [-fp] [--no-checksum] [--silent] (-s CLUSTER -d CLUSTER) [-v...] [--part-size=PART_SIZE] [--threads=THREADS] SRC_PATH DEST_PATH
    pydistcp (--version | -h)

  Options:
    --version                     Show version and exit.
    -h --help                     Show help and exit.
    -s CLUSTER --src=CLUSTER      Alias of source namenode to connect to (valid only with dist).
    -d CLUSTER --dest=CLUSTER     Alias of destination namenode to connect to (valid only with dist).
    -v --verbose                  Enable log output. Can be specified multiple times to increase verbosity each time.
    --no-checksum                 Disable checksum check prior to file transfer. This will force overwrite.
    --silent                      Don't display progress status.
    -f --force                    Allow overwriting any existing files.
    -p --preserve                 Preserve file attributes.
    --threads=THREADS             Number of threads to use for parallelization.
                                  zero limits the concurency to the maximim concurrent threads
                                  supported by the cluster. [default: 0]
    --part-size=PART_SIZE         Interval in bytes by which the files will be copied
                                  needs to be a Powers of 2. [default: 65536]

  Examples:
    pydistcp -s prod -d preprod -v /tmp/src /tmp/dest
```

All cluster connection parameters will be fetched from the json configuration file. 


benchmarks
------------

Below some benchmarks showing the impact of data size on the copy performance using pydistcp :


| File Count | Data Size | Time |
| ---------- | --------- | ------- |
|     2379   |   11.4 G  |  4m39.069s |
|     242    |  25.9 G   |  5m39.348s |
|     869    |  116.9 G  |  25m53.231s |
|     42     |  545.8 M  |  0m19.946s |

Note that all test cases are executed with 10 concurent threads on a machine having 6 cores and supporting up to 12 threads and no files
are skipped during the copy. Both the source and destination clusters are secured with kerberos and use ssl to encrypt transferred data.


Contributing
------------

Feedback and Pull requests are very welcome!
