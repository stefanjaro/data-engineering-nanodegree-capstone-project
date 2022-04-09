# Udacity Capstone Project: US Immigration, Demographics, and Climate Change

This is a Capstone Project for the Udacity Data Engineering Nanodegree.

# Project Summary

The purpose of this project is to extract and transform US foreign immigration, demographic, and temperature data and load it into a relational database for use by a team of climate scientists who are interested in exploring the relationships between climate change, foreign travel, and the demographic composition of all US states.

It's important to note that the goal of the project is to not run the analyses, but to provide the fundamental data required for the comparisons. 

# Scope of the Project

## Scope

The scope of this project is to provide a team of climate scientists access to analysis-ready and pre-aggregated data that would allow them to: 
1. Identify US states with the greatest changes in yearly temperature over the decades (using either minimums, maximums, averages, or medians)
2. Investigate the following characteristics of each of these US states:
    * The daily volumes of various types of travellers in terms of mode of travel, travel purpose, gender, citizenship region
    * The composition of a state's population in terms of gender, race, citizenship, veteran status, and average household size
    * The variety and number of air transportation ports

Our climate scientists only have a working knowledge of SQL and thus they'd like a relational database that they can easily query without a significant number of joins or aggregations.

## Technology

This project will make use of an AWS S3 Bucket, AWS EMR, PySpark, AWS Redshift, and Airflow.

![architecture](./diagrams-and-visuals/Architecture.png)

* The raw data that requires transformation will be held on an **AWS S3 Bucket**.
* The analysis-ready datasets will be stored in a relational database held using **AWS Redshift**.
    * We're using a relational database since we're going to model our data into a star schema with several fact tables containing pre-aggregated metrics.
    * While a data lake using S3 buckets would have been an inexpensive option, Redshift will allow the scientists to interact with the data using a language they're more familiar with - SQL - and scale their operations across a cluster of computers (if needed).
* Since we're dealing with large datasets, an **AWS EMR Cluster** along with **PySpark** will be used to clean and aggregate the data before it is passed into the relational database. 
    * We're using AWS EMR since we'll need access to a distributed network of computers to crunch our data. We can also easily scale our EMR instance by adding more nodes if we need additional computing power. We can shut our EMR instance down each time we're done processing the data, so we're not spending money on idle resources.
    * We're using PySpark (or Python's wrapper for Spark) to distribute the job of cleaning and aggregating the data to each of the nodes in our distributed network of computers. Our data operations will be memory-intensive and PySpark makes more efficient use of memory compared to other big data technologies like Hadoop MapReduce.
* Since we'll be running a host of operations on our data - reading, cleaning, transforming, writing, etc., - we'll need to use a tool to monitor the progress of each of these tasks. The tool we've chosen for this us **Airflow** which allows us to easily define the tasks within a data pipeline and monitor their execution.

## Data

We have 4 datasets provided by Udacity:

* **I-94 Immigration Data**: This data was gathered by the US National Tourism and Trade Office. An I-94 is a form issued to all foreign visitors to the United States at the country's ports of entry. The data spans the year 2016 and contains (but is not limited to): 
    * The traveller's personal details such as their country of citizenship, resident country, gender, age, and birth year.
    * Their travel details such as their mode of travel, their airline and flight number, their port of entry, their date of arrival, the final date before which they must depart the US, their arrival status, and their departure status.
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
* The city-wise data has been largely duplicated due to the presence of the population by race numbers. Except for `Count`, the data isn't segmented by race so it simply repeats for each race value. We'll need to split the race figures out and de-duplicate the remaining data before rolling it up to a state-level.
* The table doesn't contain demographic information on every city in each state. As a result, our state-level aggregations will be understated.

### Global Land Temperature by City

* This data contains non-US records as well. These will need to be filtered out.
* Unfortunately there's no field for US state, so we'll need to combine this with another source to be able to aggregate to a state-level (we could use the city and state columns in the Demographics data).
* The dataset only contains observations from the late 1700s until 2013. We don't have observations in 2016 which is the reference time period for all the other datasets we're working with. Nevertheless, we can generate yearly summary statistics with what we've got.

### Cross-File Observations

* Unfortunately `i94port` in the Immigration dataset does not correspond to either `local_code` or `iata_code` in the Airport Codes dataset. This effectively means that we would need to spend a significant amount of time manually mapping the `i94port` values to `municipality` values in the Airport Codes dataset (which in turn can be used to map both datasets, at a city level, to the Temperature data and Demographic data). However, since we're only interested in state-level aggregations, this is beyond the scope of the project.

# The Data Model

## Conceptual Data Model

The data model will follow a Star Schema with 4 Fact Tables and 3 Dimension Tables:

![data-model](./diagrams-and-visuals/DataModel.png)

The **fact_immigration** table is a fact table that consists of:
* *state_id*: The two-letter identifier for a US state
* *arrival_date*: The date the travellers arrived to the US
* *profile_id*: The identifier for a unique combination of gender, age category, and citizenship region 
* *all_travellers*: The number of travellers that arrived to the state (on a particular day and fitting a particular profile)
* *land_travellers*: The number of travellers that arrived by land
* *sea_travellers*: The number of travellers that arrived by sea
* *air_travellers*: The number of travellers that arrived by air
* *business_travellers*: The number of travellers that arrived for business purposes
* *pleasure_travellers*: The number of travellers that arrived for holiday or leisure purposes
* *student_travellers*: The number of travellers that arrived for the prupose of study

The **fact_ports** table is a fact table that consists of:
* *state_id*: The two-letter identifier for a US state
* *port_type*: The type of air transportion port (e.g. small/medium/large airport, balloonport, etc.)
* *num_of_ports*: The number of air transportation port by type and state

The **fact_demographics** table is a fact table that consists of:
* *state_id*: The two-letter identifier for a US state
* *total_pop*: The total population of the state (for major cities only)
* *male_pop*: The number of men in the state (for major cities only)
* *female_pop*: The number of women in the state (for major cities only)
* *veteran_pop*: The number of war veterans in the state (for major cities only)
* *foreign_pop*: The number of foreign residents in the state (for major cities only)
* *hispanic_pop*: The number of Hispanics or Latin Americans in the state (for major cities only)
* *white_pop*: The number of Caucasians in the state (for major cities only)
* *asian_pop*: The number of Asians in the state (for major cities only)
* *black_pop*: The number of black or African Americans in the state (for major cities only)
* *native_pop*: The number of American Indians or Native Alaskans in the state (for major cities only)
* *avg_hh_size*: The average size of the state's households (for major cities only)
* *total_hh*: The number of households in a state (for major cities only)

The **fact_temperature** table is a fact table that consists of:
* *state_id*: The two-letter identifier for a US state
* *year*: The year of the observations
* *observation_count*: The number of observations recorded
* *num_of_cities*: The number of cities the observations were recorded at
* *min_temp*: The minimum temperature recorded
* *max_temp*: The maximum temperature recorded
* *avg_temp*: The mean temperature recorded
* *median_temp*: The median temperature recorded

The **dim_state** table is a dimension table that consists of:
* *state_id*: The two-letter identifier for a US state
* *state_name*: The full name of a state

The **dim_datetime** table is a dimension table that consists of:
* *timestamp*: The full timestamp containing the month, year, and day value
* *day*: The day component of the timestamp
* *month*: The month component of the timestamp
* *year*: The year component of the timestamp
* *month_year*: A combination of the month component and year component
* *day_of_week*: An indicator from 1 to 7 signifying the day of the week a timestamp falls into

The **dim_traveller_profile** table is a dimension table that consists of:
* *profile_id*: The identifier for each profile
* *gender*: The gender a traveller identifies as either male, female, or other
* *age_category*: The age category the traveller falls into (as of 2016)
* *citizen_region*: The particular continent in the world the traveller is a citizen of (i.e., Asia, Africa, Oceania, etc.)
* *citizen_global_region*: Whether the traveller belongs to the global north or the global south

## Structure of the Data Pipeline

TBD