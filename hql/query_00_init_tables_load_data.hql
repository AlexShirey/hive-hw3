--create database
CREATE DATABASE IF NOT EXISTS ${db}
	COMMENT "The database for hive_hw3, bid + city + UTF";

USE ${db};

--create external table city_ext to get the data from, textformat
CREATE EXTERNAL TABLE IF NOT EXISTS city_ext
	(id INT, name STRING)
	ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
	STORED AS TEXTFILE
	LOCATION "${city_data_dir}"
	TBLPROPERTIES ('creator'="${creator}", 'date'="${date}");

--create a working table city to work with, orc format
CREATE TABLE IF NOT EXISTS city
	(id INT, name STRING)
	ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
	STORED AS ORC 
	TBLPROPERTIES ('creator'="${creator}", 'date'="${date}");

--fill table with data
FROM city_ext
INSERT OVERWRITE TABLE city SELECT *;

--create external table bid_ext to get the data from, textformat
CREATE EXTERNAL TABLE IF NOT EXISTS bid_ext
	(BidID STRING, TS STRING, LogType CHAR(1), iPinYouID STRING, UserAgent STRING, IP STRING, RegionID INT,
	CityID INT, AdExchange STRING, Domain STRING, URL STRING, AnonymousURL STRING, AdSlotID STRING, AdSlotWidth INT,
	AdSlotHeight INT, AdSlotVisibility STRING, AdSlotFormat STRING, AdSlotFloorPrice INT, CreativeID STRING, BiddingPrice INT,
	PayingPrice INT, LandingPageURL STRING, AdvertiserID STRING, UserProfileIDs STRING)
	ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
	STORED AS TEXTFILE
	LOCATION "${bid_data_dir}"
	TBLPROPERTIES ('creator'="${creator}", 'date'="${date}");

--create a working table bid to work with, orc format
CREATE TABLE IF NOT EXISTS bid
	(BidID STRING, TS STRING, LogType CHAR(1), iPinYouID STRING, UserAgent STRING, IP STRING, RegionID INT,
	CityID INT, AdExchange STRING, Domain STRING, URL STRING, AnonymousURL STRING, AdSlotID STRING, AdSlotWidth INT,
	AdSlotHeight INT, AdSlotVisibility STRING, AdSlotFormat STRING, AdSlotFloorPrice INT, CreativeID STRING, BiddingPrice INT,
	PayingPrice INT, LandingPageURL STRING, AdvertiserID STRING, UserProfileIDs STRING)
	ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
	STORED AS ORC 
	TBLPROPERTIES ('creator'="${creator}", 'date'="${date}");

--fill table with data
FROM bid_ext
INSERT OVERWRITE TABLE bid SELECT *;