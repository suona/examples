# Client Retention Demo
<!-- TOC depthFrom:1 depthTo:6 withLinks:1 updateOnSave:1 orderedList:0 -->

- [Client Retention Demo](#client-retention-demo)
	- [Purpose](#purpose)
	- [Overview](#overview)
	- [Contents](#contents)
	- [Dependencies](#dependencies)
	- [Demo Setup](#demo-setup)
		- [z/OS Data](#zos-data)
			- [Setup Interactive-Insights-Workbench (I2W) Environment](#setup-interactive-insights-workbench-i2w-environment)
			- [Setup MongoDB](#setup-mongodb)
			- [Setup Scala-Workbench Environment](#setup-scala-workbench-environment)
	- [Verification Test](#verification-test)
		- [client\_retention\_demo.ipynb](#clientretentiondemoipynb)
		- [client\_explore.ipynb](#clientexploreipynb)
		- [churn\_business\_value.ipynb](#churnbusinessvalueipynb)
	- [Loading the CRDEMO Data on z/OS](#loading-the-crdemo-data-on-zos)
		- [1.) Download TRSBIN from Github](#1-download-trsbin-from-github)
		- [2.) Upload TRSBIN package to an MVS dataset](#2-upload-trsbin-package-to-an-mvs-dataset)
		- [3.) Unterse the TRS Dataset](#3-unterse-the-trs-dataset)
		- [4.) Restore the Logical Dump](#4-restore-the-logical-dump)
		- [5.) Ensure You Restored Successfully](#5-ensure-you-restored-successfully)

<!-- /TOC -->
## Purpose
This project serves as an end-to-end demonstration and integration verification test (IVT) for the Spark on z/OS [reference architecture](https://ibm.box.com/shared/static/xm05xl372hkbmmj4eu9fhoq0kplytzp3.png).

The project uses imitation financial data for a retail bank scenario. The retail bank is assumed to have two data sources for their system of records:

* Client Profile Data (VSAM)
* Client Transaction History (DB2)

The bank would use the Scala Workbench to distill these data sources into a desired data set for use by data explorers who would use the Interactive Insights Workbench to perform downstream analytics.

## Overview

To perform an IVT of a Spark on z/OS deployment one would do the following:

1. Install and configure the [IBM z/OS Platform for Apache Spark](http://www-03.ibm.com/systems/z/os/zos/apache-spark.html).
2. Prime the z/OS data sources with sample demo data.
3. Install the Scala Workbench
4. Install the Interactive Insights Workbench (I2W) and MongoDB
5. Run a Scala Notebook to prime the MongoDB
6. Run a I2W Notebook to visualize downstream analytics.

## Contents

* z System Data Package
	* Sample Client Profile Data (VSAM)
	* Sample Client Transaction History (DB2)
	* Preload scripts
* Notebooks
	* Sample Scala Notebook that performs data munging on DB2 and VSAM data and writes results to MongoDB.
	* Sample Python Notebooks that analyzes data in MongoDB.
	* Sample Python Notebook that uses [Dato](https://dato.com) to provide a churn analysis on the data in MongoDB. <font color="red">*Pending contribution from Dato*</font>.

## Dependencies
The client retention demo requires the following:

* Docker Toolbox
* z/OS Host (z/System)
 * [IBM z/OS Platform for Apache Spark](http://www-03.ibm.com/systems/z/os/zos/apache-spark.html) with <font color="blue">PTF UI36538</font>
 * VSAM
 * DB2
 * [IBM JAVA 8 sdk for 64-bit System z](http://www.ibm.com/developerworks/java/jdk/linux/download.html)
* Interactive Insights Workbench (Docker)
* MongoDB (Docker)
* Scala Workbench (Docker)

## Demo Setup

### z/OS Data
Prepare VSAM and DB2 data sources with sample demo data. Go into the ```data/zos/``` directory and follow the README in that directory.

#### Setup Interactive-Insights-Workbench (I2W) Environment
Download [I2W](https://github.com/zos-spark/interactive-insights-workbench) and following the [Quickstart](https://github.com/zos-spark/interactive-insights-workbench#quickstart) instructions.

#### Setup MongoDB
If you haven't already, Download [I2W](https://github.com/zos-spark/interactive-insights-workbench) and follow the [Sample Database](https://github.com/zos-spark/interactive-insights-workbench#sample-database) instructions.

#### Setup Scala-Workbench Environment
Download the [Scala-Workbench](https://github.com/zos-spark/scala-workbench) and follow the setup instructions.

## Verification Test
Once the setup steps listed above have been completed, you can verify the setup using the following scripts:

1. On the Scala-Workbench run the ```client_retention_demo.ipynb``` notebook.
2. On I2W run the ```client_explore.ipynb``` and ```churn_business_value.ipynb``` notebooks.

#### client\_retention\_demo.ipynb
The ```client_retention_demo.ipynb``` will use [IBM z/OS Platform for Apache Spark](http://www-03.ibm.com/systems/z/os/zos/apache-spark.html) to access data stored in a DB2 table and in a VSAM data set.  It will then calculate some aggregate statistics, then offload the results to MongoDB.

#### client\_explore.ipynb
The ```client_explore.ipynb``` will read from MongoDB, and create several interactive exploritory widgets.

#### churn\_business\_value.ipynb
The ```churn_business_value.ipynb``` will read from MongoDB, and create several interactive widgets that show business value of target groups.


## Loading the CRDEMO Data on z/OS

Contents of this $$README are contained in: ```<InstallHLQ>.CRDEMO.JCL($$README)```

Below is the list of files and instructions needed to restore the sample
VSAM KSDS & DB2 table for the Spark Customer Retention demo (CRDEMO):

These notes are intended for use by the z/OS and DB2 System Programmers!

### 1.) Download TRSBIN from Github
Download the following [TRSBIN](https://github.com/zODSP/examples/releases/download/pre-release/CRDEMO.DUMP.TRSBIN.20160406) package from GitHub to your workstattion:

### 2.) Upload TRSBIN package to an MVS dataset
In this section we will upload the TRSBIN package to an MVS dataset using FTP w/quote site

```
$ FTP <yourMVShost>
Userid:   <yourusername>
Password: <yourpassword>
$ bin
$ quote site RECFM=FB LRECL=1024 BLKSIZE=20480 CYL PRI=200 SEC=20
```

For SMS managed storage, use the following:

```
$ quote site STORCLAS=<SMS_STORCLAS> MGMTCLAS=<SMS_MGMTCLAS>
```

For Non-SMS managed storage, use the following:

```
$ quote site UNIT=3390 VOLUME=??????
```

Put the package into your user directory

```
$ cd '<userid>'
$ put <GitHub_package_name> CRDEMO.DUMP.TRSBIN
```

### 3.) Unterse the TRS Dataset
We will use the following JCL template to unterse the TRS dataset:

* Sample JCL located in ```<InstallHLQ>.CRDEMO.JCL(UNTRDEMO)```
* Change JOBCARD as appropriate for your system installation
* Change <userid> for your TSO userid on installation system
  * For SMS-managed target, use STORCLAS= DD card
  * For non SMS-managed target, use UNIT/VOL=SER= DD card

**UNTRDEMO JCL sample**

```
//UNTRDEMO JOB CLASS=A,MSGCLASS=H,NOTIFY=&SYSUID,REGION=0M
//UNTERSE EXEC PGM=TRSMAIN,PARM='UNPACK'
//SYSPRINT  DD SYSOUT=*,DCB=(LRECL=133,BLKSIZE=12901,RECFM=FBA)
//INFILE    DD DSN=<userid>.CRDEMO.DUMP.TRSBIN,DISP=SHR
//OUTFILE   DD DSN=<userid>.CRDEMO.DUMP,DISP=(NEW,CATLG),
//             STORCLAS=????????,
//*            UNIT=3390,VOL=SER=??????,
//             SPACE=(CYL,(500,50))
```


### 4.) Restore the Logical Dump
We will use the following JCL template to restore the logical dump:

* Sample JCL located in ```<InstallHLQ>.CRDEMO.JCL(RESTDEMO)```
* Change JOBCARD as appropriate for your system installation
* Change <userid> for your TSO userid on installation system
* Change <InstallHLQ> to 2-qual HLQ appropriate on your system
  * For SMS-managed target, use STORCLAS() keyword
  * For non SMS-managed target, use following ADRDSSU keywords
     - BYPASSACS(*)
     - NULLSTORCLAS
     - NULLMGMTCLAS
     - OUTDYNAM()

**Notes: The DSS logical dump was created from datasets under**


### WORKLOAD.ZSPARK (2 qualifiers) for the source libraries

You must do one of the following:

* RESTORE with RENAMEU(<InstallHLQ>)
                     -> Datasets will be under ```<InstallHLQ>.ZSPARK.CRDEMO.**```
* RESTORE with 2 HLQs on RENAMEU (as in example below)
                     -> Datasets will be under ```<InstallHLQ>.CRDEMO.**```
* Individually RENAME each library to add/remove
                     a qualifier specific to your installation
                     -> List of individual datasets contained within dump:
  * ```WORKLOAD.ZSPARK.CRDEMO.DB.CLIENTS.SORTED```
  * ```WORKLOAD.ZSPARK.CRDEMO.DB.SPPAYTS.SYSPUNCH```
  * ```WORKLOAD.ZSPARK.CRDEMO.DB.SPPAYTS.SYSREC```
  * ```WORKLOAD.ZSPARK.CRDEMO.DB2.DDL```
  * ```WORKLOAD.ZSPARK.CRDEMO.DB2.JCL```
  * ```WORKLOAD.ZSPARK.CRDEMO.DCLGEN```
  * ```WORKLOAD.ZSPARK.CRDEMO.JCL```
  * ```WORKLOAD.ZSPARK.CRDEMO.XLS.ZIPBIN```


**RESTDEMO JCL sample**

```
//RESTDEMO JOB ,,CLASS=A,MSGCLASS=H,NOTIFY=&SYSUID,REGION=0M
//*
//RESTORE  EXEC PGM=ADRDSSU
//SYSPRINT DD SYSOUT=*
//DDIN     DD DSN=<userid>.CRDEMO.DUMP,DISP=SHR
//SYSIN    DD *
  RESTORE DATASET(                            -
          INCLUDE(**))                        -
          RENAMEU(WORKLOAD.ZSPARK.**,         -
                  <InstallHLQ>.**)            -
    CATALOG                                   -
    NULLMGMTCLAS                              -
    STORCLAS(????????)                        -
    INDD(DDIN)
//
    BYPASSACS(*)                              -
    NULLSTORCLAS                              -
    NULLMGMTCLAS                              -
    OUTDYNAM(??????)                          -
```

### 5.) Ensure You Restored Successfully
After RESTDEMO Restore job completes, you should have the following:

* Install JCL:      ```<InstallHLQ>.CRDEMO.JCL```
* COBOL COPYBOOK:  ```<InstallHLQ>.CRDEMO.DCLGEN```
* DB2 DDL/JCL:      ```<InstallHLQ>.CRDEMO.DB2.*```
* DB2 UNLOADs:      ```<InstallHLQ>.CRDEMO.DB.*```
* XLS WinZip:       ```<InstallHLQ>.CRDEMO.XLS.ZIPBIN```

**Continue w/Steps #6 thru #15 contained in the full version of the
README packaged within the dump in:** ```<InstallHLQ>.CRDEMO.JCL($$README)```
