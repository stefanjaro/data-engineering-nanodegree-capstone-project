# Udacity Capstone Project: US Immigration, Demographics, and Climate Change

This is a Capstone Project for the Udacity Data Engineering Nanodegree.

# Project Summary

The purpose of this project is to extract and transform US foreign immigration, demographic, and temperature data and load it into a relational database for use by a team of climate scientists who are interested in exploring the relationships between climate change, foreign air travel, and the demographic composition of cities and states.

It's important to note that the goal of the project is to not run the analyses, but to provide the fundamental data required for them. 

It is entirely possible that the process documented below may need to be changed if the scientists discover new patterns in the data that had to be taken into account during the ETL process. Many assumptions had to be made during this initial run.

# Scope of the Project

## Scope

The scope of this project is to provide a team of climate scientists access to analysis-ready data that would allow them to primarily determine if US cities and states that have seen greater changes in temperature over the years were subject to relatively more or less foreign citizen immigration by air.

Furthermore, our climate scientists would also like to determine if there is a correlation between climate change and:
* The broad reasons for immigration (business, leisure)
* The demographics of a city and/or state
* The demographics of the travellers (resident country, gender, and age category)
* The number and variety of airports in a city and/or state

Our climate scientists have a very good knowledge of SQL and want a flexible and tightly-defined data model that they can easily run aggregations and joins on.

## Technology

This project will make use of an AWS S3 Bucket, AWS EMR, PySpark, AWS Redshift, and Airflow.

![architecture](./diagrams-and-visuals/Architecture.png)

* The raw data that requires transformation will be held on an **AWS S3 Bucket**.
* The analysis-ready datasets will be stored in a relational database held using **AWS Redshift**.
    * We're using a relational database since we're going to model our data into a star schema with several fact tables. To explore relationships between various facts, the scientists will want to easily be able to aggregate the data in varying ways and join the aggregations as they see fit.
    * While a data lake using S3 buckets would have been an inexpensive option, Redshift will allow the scientists to interact with the data using a language they're bound to be more familiar with - SQL - and scale their operations across a cluster of computers.
* Since we're dealing with large datasets, an **AWS EMR Cluster** along with **PySpark** will be used to clean and aggregate the data before it is passed into the relational database. 
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

The findings below contain both observations and ideas on how the data should be cleaned or transformed.

### I-94 Immigration Data

* A lot of the data is codified and must be expanded using the reference data provided in the `I94_SAS_Labels_Description.SAS` file. Codified data includes `i94cit`, `i94res`, `i94port`, and `i94mode` among others.
* According to the reference data in the `I94_SAS_Labels_Description.SAS` file, some of the codes are invalid or should not be used. For key fields like `i94port`, these must be removed from the dataset so eventual analyses aren't based off erroneous data.
* The `arrdate` or arrival date is stored as an SAS numeric date. This means it indicates the number of days either before (negative values) or after (positive values) the 1st of January, 1960.
* There are fields that are codified but have no reference data in the `I94_SAS_Labels_Description.SAS` file. The fields include (but are not limited to): `occup`, `entdepa`, `entdepd`, and `entdepu`.
* Less than 0.01% of records in the sample dataset do not have an `i94mode` value. But more than 99% of records do not have an `occup` value, `insnum` value or an `entdepu` value. The nulls are unlikely missing values and more than likely have a meaning. Nevertheless, these aren't necessary for our project's analytics use cases.
* However, 13.38% of records do not have a `gender` value. These will need to be filled in. Since we can't infer the gender, we could fill it in with a "U" for "Unknown".
* The parquet file-based sample dataset provided alone was very large. We cannot clean and aggregate this data on a single machine.

### Airport Codes Data

* The dataset contains non-US ports as well which will need to be filtered out.
* The state of the US port is included in the `iso_region` column following a hyphen. It'll need to be split out. There are no records without `iso_region` values.
* The majority of medium and large airports have `iata_code` values but only around 12% of small airports have IATA codes. This is to be expected since only major airports would have an IATA code.
* The `municipality` field refers to the city in which the port is located.

### US City Demographics

* Very few records (16) in the dataset contain nulls.
* The dataset is relatively clean but we have aggregated values such as `Median Age` and `Average Household Size` that would be difficult to roll-up to a state-level. We may need to calculate total number of households and then divide by the total population. We could do the same for `Median Age` using it as a proxy for average age.
* The city-wise data has been largely duplicated due to the presence of the population by race numbers. Except for `Count`, the data isn't segmented by race so it simply repeats for each race value. We'll need to split the race figures out and de-duplicate the remaining data.

### Global Land Temperature by City

* This data contains non-US records as well. These will need to be filtered out.
* Unfortunately there's no field for US state, so we'll need to combine this with another source to be able to aggregate to a state-level if needed.
* It's unlikely that our climate scientists need data all the way back from the 1700s. We've only got immigration and demographic data for 2016, so we could only extract temperatures observed in 2016. But the dataset only contains observations until 2013. Furthermore, to understand patterns over time we'll need multi-year data. We could perhaps limit the data to only those ports (or cities) or states included in the I-94 Immigration Data.

# The Data Model

## Conceptual Data Model

The data model will follow a Star Schema with 5 Fact Tables and 3 Dimension Tables:

![data-model](./diagrams-and-visuals/DataModel.png)

The **Immigration Facts** table is a fact table that consists of:
* *city_id*: The unique identifier for a US city
* *departing_country_id*: The unique identifier for the country of departure
* *resident_country_id*: The unique identifier for the country of residence
* *purpose_of_visit_id*: The unique identifier for the purpose of the visit (i.e., business, leisure, etc.)
* *age_category_id*: The unique identifier for the age bracket the travellers belong to
* *gender_id*: The unique identifier for the gender the travellers identify as
* *arrival_month*: The month the travellers arrived to the US
* *count_of_travellers*: The number of travellers that fall into each of the unique combinations of categories above

The **Airport Facts** table is a fact table that consists of:
* *city_id*: The unique identifier for a US city
* *type_of_airport*: Whether an airport is small, medium, large, or otherwise
* *number_of_airports*: The number of airports by type and city

The **General Population Facts** table is a fact table that consists of:
* *city_id*: The unique identifier for a US city
* *total_population*: The total population of the city
* *male_population*: The number of men in the city
* *female_population*: The number of women in the city
* *veteran_population*: The number of war veterans in the city
* *foreign_population*: The number of foreign residents in the city
* *median_age*: The median age of the city's population
* *average_household_size*: The average size of the city's households

The **Race Population Facts** table is a fact table that consists of:
* *city_id*: The unique identifier for a US city
* *race*: A specific ethnicity or race
* *population*: The number of a specific ethnicity or race that resides within the city

The **Temperature Facts** table is a fact table that consists of:
* *city_id*: The unique identifier for a US city
* *timestamp*: The date an average temperature was recorded
* *average_temperature*: The average temperature recorded

The **City Dim** table is a dimension table that consists of:
* *city_id*: The unique identifier for a US city
* *city_name*: The full name of a city
* *state_code*: The 2-letter code for the state the city belongs to
* *lattitude*: The locational lattitude of the city
* *longitude*: The locational longitude of the city

The **Country Dim** table is a dimension table that consists of:
* *country_id*: The unique identifier for a country
* *country_name*: The name of the country
* *region*: The region of the world it belongs to
* *global_region*: Whether it belongs to the global north or south

The **Time Dim** table is a dimension table that consists of:
* *timestamp*: The full timestamp containing the month, year, and day value
* *day*: The day component of the timestamp
* *month*: The month component of the timestamp
* *year*: The year component of the timestamp
* *day_of_week*: An indicator from 1 to 7 signifying the day of the week a timestamp falls into
* *month_year*: A combination of the month component and year component

## Structure of the Data Pipeline

TBD