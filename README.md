# Movie Recommendation System

## Background
With the rapid development of the network information age, the demand of movies viewing for people is more and more. However, the number of movies is so massive that people usually don't know how to choose a good movie. Personalized recommendation system is very helpful and useful to recommend high rating movies to users based on the personal preference.
Apache Sparkis an open-source cluster-computing framework which has been applied to many applications of big data projects. Because the processing speed of Spark is very fast and Spark’s capabilities are accessible with a set of rich APIs, all designed specifically for interacting quickly and easily with data at scale. Besides, Spark supports a range of programming languages, including Java, Python, and Scala. In our project, we chose the Python so we need to learn about the pySpark. Flask is a web framework written in Python and it's  easy to learn and simple to use.

## How to run the code
1. Set up environment first : Python 3.6.5 + Java 8.0+ Spark2.4.4 + Pyspark + Flask; 
2. Enter the project directory; 
3. Run fetch_online_data.py to fetch the data online (You can pass this step since data is already downloaded) 
4. Run ( flask run ) in the terminal to start the web server;
5. Type http://127.0.0.1:5000/ in Chrome, and everything will be super intuitive for you. Now you can start to rate some movies and get recommdations!!!
