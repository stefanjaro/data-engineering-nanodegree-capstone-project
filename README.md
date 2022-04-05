# US Immigration and Climate Change - A Udacity Capstone Project for the Data Engineering Nanodegree

# Project Summary

The purpose of this project is to extract and transform US foreign immigration, demographic, and temperature data and load it into a data lake for use by a team of climate scientists who are interested in exploring the relationships between climate change, foreign travel, and the demographic composition of cities and states.

It's important to note that the goal of the project is to not run the analyses, but to provide the fundamental data required for them. 

It is entirely possible that the process documented below may need to be changed if the scientists discover new patterns in the data that had to be taken into account during the ETL process. Many assumptions had to be made during this initial run.

# Scope of the Project

## Scope

The scope of this project is to provide a team of climate scientists access to analysis-ready data that would allow them to primarily determine if US cities and states that have seen greater changes in temperature over the years were subject to relatively more or less foreign citizen immigration.

Furthermore, our climate scientists would also like to determine if there is a correlation between climate change and:
* The types of transportation used
* The broad reasons for immigration
* The demographics of a city and/or state
* The demographics of the travellers (resident country, gender, and age)
* The airline used by foreign travellers

## Technology

This project will make use of AWS S3 Buckets, AWS EMR, PySpark, and Airflow.

![architecture](./diagrams-and-visuals/Architecture.png)

* The raw data that requires transformation will be held on an **AWS S3 Bucket**.
* The analysis-ready datasets will be stored in a data lake held within another **AWS S3 Bucket**. 
    * We're using a data lake instead of a database like Redshift to provide the climate scientists greater flexibility over how they query their data given the somewhat loose relationships between the various datasets.
    * We're choosing AWS S3 due to its ease of use and low cost.
* Since we're dealing with large datasets, an **AWS EMR Cluster** along with **PySpark** will be used to clean and aggregate the data before it is passed into the data lake. 
    * We're using AWS EMR since we'll need access to a distributed network of computers to crunch our data. We can also easily scale our EMR instance by adding more nodes if we need additional computing power. We can shut our EMR instance down each time we're done processing the data, so we're not spending money on idle resources.
    * We're using PySpark (or Python's wrapper for Spark) to distribute the job of cleaning and aggregating the data to each of the nodes in our distributed network of computers. Our data operations will be memory-intensive and PySpark makes more efficient use of memory compared to other big data technologies like Hadoop MapReduce.
* Since we'll be running a host of operations on our data - reading, cleaning, transforming, writing, etc., - we'll need to use a tool to monitor the progress of each of these tasks. The tool we've chosen for this us **Airflow** which allows us to easily define the tasks within a data pipeline and monitor their execution.

## Data

We have 4 datasets provided by Udacity:

* **I-94 Immigration Data**: This data was gathered by the US National Tourism and Trade Office. An I-94 is a form issued to all foreign visitors to the United States at the country's ports of entry. The data spans the year 2016 and contains (but is not limited to): 
    * The traveller's personal details such as their resident country, gender, age, and birth year.
    * Their travel details such as their mode of travel, their airline and flight number, departing country, their port of entry, their date of arrival, the final date before which they must depart the US, their arrival status, and their departure status.
    * Their visa details such as the type of their visa, their purpose in the country, and the department of state that issued their visa.
* **US City Demographics**: This data was published by OpenDataSoft. It contains demographic information for 2,891 cities across the United States. The dataset is comprised of numerical information such as median age, gender-based population figures, average house-hold size, number of veterans, number of foreign-born residents, and a breakdown by race.
* **Airport Codes**: This data was published on DataHub.io. It contains, primarily codified, information on 55,075 transportation ports (i.e., airports, seaports, heliports, and baloonports) around the world. In addition to the name of the port and codes for the countries, regions, municipalities, and continents it belongs to, it also contains its elevation level, location coordinates, and type of port.
* **Global Land Temperature by City**: This data was published by the non-profit, Berkeley Earth. It contains time-series data of the average temperature observed in 3,448 cities across 159 countries spanning the time period 1743 to 2013.

# Data Exploration

An iPython Notebook was used to explore the data (or sample data) and can be found in the following repository location: `/supplementary-scripts/data-exploration.ipynb`. 

## Findings





