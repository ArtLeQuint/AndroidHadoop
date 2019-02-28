set JAVA_HOME=%1
set HADOOP_CLASSPATH=%JAVA_HOME%\lib\tools.jar
set HADOOP_HOME=%~dp0hadoop-2.8.0
set Path=%Path%;%HADOOP_HOME%\bin;%JAVA_HOME%\bin

rd /s /q bin
md bin\Q1
md bin\Q2
md bin\Q3

call hadoop com.sun.tools.javac.Main -d bin/Q1 src/Q1.java

jar -cvf bin/Q1/Q1.jar -C bin/Q1/ .

call hadoop com.sun.tools.javac.Main -d bin/Q2 src/Q2.java

jar -cvf bin/Q2/Q2.jar -C bin/Q2/ .

call hadoop com.sun.tools.javac.Main -d bin/Q3 src/Q3.java

jar -cvf bin/Q3/Q3.jar -C bin/Q3/ .


