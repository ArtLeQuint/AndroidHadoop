set JAVA_HOME=%1
set HADOOP_CLASSPATH=%JAVA_HOME%\lib\tools.jar
set HADOOP_HOME=%~dp0hadoop-2.8.0
set Path=%Path%;%HADOOP_HOME%\bin;%JAVA_HOME%\bin

rd /s /q output

call hadoop jar bin/Q1/Q1.jar Q1 data/input output/Q1

call hadoop jar bin/Q2/Q2.jar Q2 data/input output/Q2

call hadoop jar bin/Q3/Q3.jar Q3 data/input output/Q3
