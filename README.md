# Modeling high-frequency limit order book dynamics with support vector machines

Based on paper [Modeling high-frequency limit order book dynamics with support vector machines](https://raw.github.com/ezhulenev/scala-openbook/master/assets/Modeling-high-frequency-limit-order-book-dynamics-with-support-vector-machines.pdf)

## Spark Testing Example Application

Source code for blog posts:

- [Stock Price Prediction With Big Data and Machine Learning](http://eugenezhulenev.com/blog/2014/11/14/stock-price-prediction-with-big-data-and-machine-learning/)

## Testing

By default tests are running with Spark in local mode

    sbt test

## Building

In the root directory run:

    sbt assembly

The application fat jars will be placed in:
  - `target/scala-2.10/order-book-dynamics.jar`


## Running

First you need to run `assembly` in sbt and then run java cmd

    java -Dspark.master=spark://spark-host:7777 order-book-dynamics.jar