# AndroidHadoop

AndroidHadoop, is a Hadoop (Java) based project that answers 3 questions for the inputs in input.7z

## Important

We modified the input in order to eliminate "monsters", while reading with Excel, caused by ',' in quotes, replacing them by '.' then we replaced ',' separators by ';' for convenience purpose, that's why names with ',' or other attributes will have '.' instead

## Data store

"Original" data store can be found here : [DataStore](https://www.kaggle.com/lava18/google-play-store-apps#googleplaystore.csv)
You might also find there many informations on the DataStore used

## Questions

Which are the categories that has the most installations, by targeted public ?
Which is the genre recipe that works the best ?
In paid application "universe" which price range is the best, ranked by installations ?

## Getting Started

These instructions will get you a copy of the project up and running on your local machine for development and testing purposes
Clone this project, then read "Use" section:

### Prerequisites

- [Hadoop](https://hadoop.apache.org/)

- [Java](https://www.java.com/)

### Use

#### Test example SNCF (don't forget to unzip inputs files) (used for dev)

$ mkdir sncf_classes

$ hadoop com.sun.tools.javac.Main -d sncf_classes Exemple_SNCF.java

$ jar  -cvf exemple_sncf.jar -C sncf_classes/ .

$ hadoop jar exemple_sncf.jar Exemple_SNCF inputSNCF output

#### Test question 1

$ mkdir q1_classes
$ hadoop com.sun.tools.javac.Main -d q1_classes q1.java
$ jar  -cvf q1.jar -C q1_classes/ .
$ hadoop jar q1.jar q1 input outputq1

#### Test question 2

$ mkdir q2_classes
$ hadoop com.sun.tools.javac.Main -d q2_classes q2.java
$ jar  -cvf q2.jar -C q2_classes/ .
$ hadoop jar q2.jar q2 input outputq2

#### Test question 3

$ mkdir q3_classes
$ hadoop com.sun.tools.javac.Main -d q3_classes q3.java
$ jar  -cvf q3.jar -C q3_classes/ .
$ hadoop jar q3.jar q3 input outputq3

#### Results

Results can be found in the output directory you choosed in the last command.

## Authors

**DELRUE Arthur** - *Initial work* - [ArtLeQuint](https://github.com/ArtLeQuint).
**FARAUX Sylvein** - *Initial work*.

## License

This project is licensed under the GNU Lesser General Public License v3.0 - see the [LICENSE.md](LICENSE.md) file for details.