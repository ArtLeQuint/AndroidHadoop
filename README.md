# AndroidHadoop

AndroidHadoop, is a Hadoop (Java) based project that answers 3 questions for the input in data folder.

## Important

We modified the input in order to eliminate "monsters", while reading with Excel, caused by ',' in quotes, replacing them by '.', that's why names with ',' or other attributes will have '.' instead.

We also replaced ',' separators by ';' for convenience purpose.

## Data store

"Original" data store can be found here and in data folder : [DataStore](https://www.kaggle.com/lava18/google-play-store-apps#googleplaystore.csv)

You might also find there many informations on the DataStore used

## Questions

Q1 : Which are the categories that has the most installations, by targeted public ?

Q2 : Which is the genre recipe that works the best ?

Q3 : In paid application "universe" which price range is the best, ranked by installations ?

## Getting Started

These instructions will get you a copy of the project up and running on your local machine for development and testing purposes
Clone this project, then read "Use" section:

### Prerequisites

- [Hadoop](https://hadoop.apache.org/)

- [Java](https://www.java.com/)

### Use

#### Using .bat

You can use BuildAndroidHadoop.bat [java_path] to build the project, then use AndroidHadoop.bat [java_path] to generate results.

[java_path] is necessary because you might have to move it, currently hadoop doesn't work if there is a space in your java, hadoop or in this repo path...
If your computer is already configured and the JAVA_HOME environment variable is known, you can simply run "BuildAndroidHadoop.bat %JAVA_HOME%" and then "AndroidHadoop.bat %JAVA_HOME%"

#### Results

Results can be found in the output/Q1-final, output/Q2-final and output/Q3 directory, its the part-r-00000 file, you can easily analyze it with any csv explorer.

## Authors

**DELRUE Arthur** - *Initial work* - [ArtLeQuint](https://github.com/ArtLeQuint).

**FARAUX Sylvein** - *Initial work* - [SylveinFARAUX](https://github.com/SylveinFARAUX).

## License

This project is licensed under the GNU Lesser General Public License v3.0 - see the [LICENSE.md](LICENSE.md) file for details.
