# SYSLOG Filtering with Apache Spark

This spark application allows a system programmer to filter
z/OS SYSLOG data based on one or more user-supplied type codes of a message
identifier. For example, a system programmer may want to find all message IDs ending
with 'A' which require an action. How
to runIf
the SYSLOG data has been copied down to z/OS UNIX, a sample invocation is:

```
cat "//'LOGWTR.LOGTS.LOG16296.DATA'" | iconv -f IBM-1047 -t ISO8859-1 \
	| spark-submit --class "com.ibm.log.LogFilteringTypeCode" --master local[4] \
	/u/myuser/LogFiltering-0.0.1-SNAPSHOT-jar-with-dependencies.jar A
```

The
"cat" command reads the data from the specified dataset
LOGWTR.LOGTS.LOG16296.DATA and sends it to the iconv command through the shell
pipe ("|") so it can be converted from EBCDIC (IBM-1047) to ASCII
(ISO8859-1) before sending to our Spark application, which is invoked with
spark-submit. Spark expects the data in ASCII because it's compiled as ASCII.
The application is run with the letter 'A' as a parameter so that action
messages will be displayed.For
more information  
[What
is SYSLOG?](https://www.ibm.com/support/knowledgecenter/zosbasics/com.ibm.zos.zproblem/uslogs.htm) [SYSLOG
(HCL) format](http://publibfp.dhe.ibm.com/cgi-bin/bookmgr/BOOKS/a3208541/70.1.1?SHELF=&DT=20110522083237&CASE=)[z/OS
message format](https://www.ibm.com/support/knowledgecenter/SSLTBW_2.1.0/com.ibm.zos.v2r1.ieam100/msgfmt.htm)

