############################
# read and clean .csv#
############################

set opt.fetch false

DEFINE CSVLoader org.apache.pig.piggybank.storage.CSVLoader();
load_2009 = LOAD 'airlineDelayAnalysis/2009.csv' using CSVLoader() AS (FL_DATE:chararray, OP_CARRIER:chararray,	OP_CARRIER_FL_NUM:chararray, ORIGIN:chararray, DEST:chararray, CRS_DEP_TIME:chararray, DEP_TIME:chararray, DEP_DELAY:int, TAXI_OUT:int, WHEELS_OFF:chararray, WHEELS_ON:chararray, TAXI_IN:int, CRS_ARR_TIME:chararray, ARR_TIME:chararray, ARR_DELAY:int, CANCELLED:int, CANCELLATION_CODE:int, DIVERTED:int, CRS_ELAPSED_TIME:int, ACTUAL_ELAPSED_TIME:int, AIR_TIME:int, DISTANCE:int, CARRIER_DELAY:int, WEATHER_DELAY:int, NAS_DELAY:int, SECURITY_DELAY:int, LATE_AIRCRAFT_DELAY:int);

load_2010 = LOAD 'airlineDelayAnalysis/2010.csv' using CSVLoader() AS (FL_DATE:chararray, OP_CARRIER:chararray,	OP_CARRIER_FL_NUM:chararray, ORIGIN:chararray, DEST:chararray, CRS_DEP_TIME:chararray, DEP_TIME:chararray, DEP_DELAY:int, TAXI_OUT:int, WHEELS_OFF:chararray, WHEELS_ON:chararray, TAXI_IN:int, CRS_ARR_TIME:chararray, ARR_TIME:chararray, ARR_DELAY:int, CANCELLED:int, CANCELLATION_CODE:int, DIVERTED:int, CRS_ELAPSED_TIME:int, ACTUAL_ELAPSED_TIME:int, AIR_TIME:int, DISTANCE:int, CARRIER_DELAY:int, WEATHER_DELAY:int, NAS_DELAY:int, SECURITY_DELAY:int, LATE_AIRCRAFT_DELAY:int);

load_2011 = LOAD 'airlineDelayAnalysis/2011.csv' using CSVLoader() AS (FL_DATE:chararray, OP_CARRIER:chararray,	OP_CARRIER_FL_NUM:chararray, ORIGIN:chararray, DEST:chararray, CRS_DEP_TIME:chararray, DEP_TIME:chararray, DEP_DELAY:int, TAXI_OUT:int, WHEELS_OFF:chararray, WHEELS_ON:chararray, TAXI_IN:int, CRS_ARR_TIME:chararray, ARR_TIME:chararray, ARR_DELAY:int, CANCELLED:int, CANCELLATION_CODE:int,	DIVERTED:int, CRS_ELAPSED_TIME:int, ACTUAL_ELAPSED_TIME:int, AIR_TIME:int, DISTANCE:int, CARRIER_DELAY:int, WEATHER_DELAY:int, NAS_DELAY:int, SECURITY_DELAY:int, LATE_AIRCRAFT_DELAY:int);

load_2012 = LOAD 'airlineDelayAnalysis/2012.csv' using CSVLoader() AS (FL_DATE:chararray, OP_CARRIER:chararray,	OP_CARRIER_FL_NUM:chararray, ORIGIN:chararray, DEST:chararray, CRS_DEP_TIME:chararray, DEP_TIME:chararray, DEP_DELAY:int, TAXI_OUT:int, WHEELS_OFF:chararray, WHEELS_ON:chararray, TAXI_IN:int, CRS_ARR_TIME:chararray, ARR_TIME:chararray, ARR_DELAY:int, CANCELLED:int, CANCELLATION_CODE:int,	DIVERTED:int, CRS_ELAPSED_TIME:int, ACTUAL_ELAPSED_TIME:int, AIR_TIME:int, DISTANCE:int, CARRIER_DELAY:int, WEATHER_DELAY:int, NAS_DELAY:int, SECURITY_DELAY:int, LATE_AIRCRAFT_DELAY:int);

load_2013 = LOAD 'airlineDelayAnalysis/2013.csv' using CSVLoader() AS (FL_DATE:chararray, OP_CARRIER:chararray,	OP_CARRIER_FL_NUM:chararray, ORIGIN:chararray, DEST:chararray, CRS_DEP_TIME:chararray, DEP_TIME:chararray, DEP_DELAY:int, TAXI_OUT:int, WHEELS_OFF:chararray, WHEELS_ON:chararray, TAXI_IN:int, CRS_ARR_TIME:chararray, ARR_TIME:chararray, ARR_DELAY:int, CANCELLED:int, CANCELLATION_CODE:int,	DIVERTED:int, CRS_ELAPSED_TIME:int, ACTUAL_ELAPSED_TIME:int, AIR_TIME:int, DISTANCE:int, CARRIER_DELAY:int, WEATHER_DELAY:int, NAS_DELAY:int, SECURITY_DELAY:int, LATE_AIRCRAFT_DELAY:int);

load_2014 = LOAD 'airlineDelayAnalysis/2014.csv' using CSVLoader() AS (FL_DATE:chararray, OP_CARRIER:chararray,	OP_CARRIER_FL_NUM:chararray, ORIGIN:chararray, DEST:chararray, CRS_DEP_TIME:chararray, DEP_TIME:chararray, DEP_DELAY:int, TAXI_OUT:int, WHEELS_OFF:chararray, WHEELS_ON:chararray, TAXI_IN:int, CRS_ARR_TIME:chararray, ARR_TIME:chararray, ARR_DELAY:int, CANCELLED:int, CANCELLATION_CODE:int,	DIVERTED:int, CRS_ELAPSED_TIME:int, ACTUAL_ELAPSED_TIME:int, AIR_TIME:int, DISTANCE:int, CARRIER_DELAY:int, WEATHER_DELAY:int, NAS_DELAY:int, SECURITY_DELAY:int, LATE_AIRCRAFT_DELAY:int);

load_2015 = LOAD 'airlineDelayAnalysis/2015.csv' using CSVLoader() AS (FL_DATE:chararray, OP_CARRIER:chararray,	OP_CARRIER_FL_NUM:chararray, ORIGIN:chararray, DEST:chararray, CRS_DEP_TIME:chararray, DEP_TIME:chararray, DEP_DELAY:int, TAXI_OUT:int, WHEELS_OFF:chararray, WHEELS_ON:chararray, TAXI_IN:int, CRS_ARR_TIME:chararray, ARR_TIME:chararray, ARR_DELAY:int, CANCELLED:int, CANCELLATION_CODE:int,	DIVERTED:int, CRS_ELAPSED_TIME:int, ACTUAL_ELAPSED_TIME:int, AIR_TIME:int, DISTANCE:int, CARRIER_DELAY:int, WEATHER_DELAY:int, NAS_DELAY:int, SECURITY_DELAY:int, LATE_AIRCRAFT_DELAY:int);

load_2016 = LOAD 'airlineDelayAnalysis/2016.csv' using CSVLoader() AS (FL_DATE:chararray, OP_CARRIER:chararray,	OP_CARRIER_FL_NUM:chararray, ORIGIN:chararray, DEST:chararray, CRS_DEP_TIME:chararray, DEP_TIME:chararray, DEP_DELAY:int, TAXI_OUT:int, WHEELS_OFF:chararray, WHEELS_ON:chararray, TAXI_IN:int, CRS_ARR_TIME:chararray, ARR_TIME:chararray, ARR_DELAY:int, CANCELLED:int, CANCELLATION_CODE:int,	DIVERTED:int, CRS_ELAPSED_TIME:int, ACTUAL_ELAPSED_TIME:int, AIR_TIME:int, DISTANCE:int, CARRIER_DELAY:int, WEATHER_DELAY:int, NAS_DELAY:int, SECURITY_DELAY:int, LATE_AIRCRAFT_DELAY:int);

load_2017 = LOAD 'airlineDelayAnalysis/2017.csv' using CSVLoader() AS (FL_DATE:chararray, OP_CARRIER:chararray,	OP_CARRIER_FL_NUM:chararray, ORIGIN:chararray, DEST:chararray, CRS_DEP_TIME:chararray, DEP_TIME:chararray, DEP_DELAY:int, TAXI_OUT:int, WHEELS_OFF:chararray, WHEELS_ON:chararray, TAXI_IN:int, CRS_ARR_TIME:chararray, ARR_TIME:chararray, ARR_DELAY:int, CANCELLED:int, CANCELLATION_CODE:int,	DIVERTED:int, CRS_ELAPSED_TIME:int, ACTUAL_ELAPSED_TIME:int, AIR_TIME:int, DISTANCE:int, CARRIER_DELAY:int, WEATHER_DELAY:int, NAS_DELAY:int, SECURITY_DELAY:int, LATE_AIRCRAFT_DELAY:int);

load_2018 = LOAD 'airlineDelayAnalysis/2018.csv' using CSVLoader() AS (FL_DATE:chararray, OP_CARRIER:chararray,	OP_CARRIER_FL_NUM:chararray, ORIGIN:chararray, DEST:chararray, CRS_DEP_TIME:chararray, DEP_TIME:chararray, DEP_DELAY:int, TAXI_OUT:int, WHEELS_OFF:chararray, WHEELS_ON:chararray, TAXI_IN:int, CRS_ARR_TIME:chararray, ARR_TIME:chararray, ARR_DELAY:int, CANCELLED:int, CANCELLATION_CODE:int,	DIVERTED:int, CRS_ELAPSED_TIME:int, ACTUAL_ELAPSED_TIME:int, AIR_TIME:int, DISTANCE:int, CARRIER_DELAY:int, WEATHER_DELAY:int, NAS_DELAY:int, SECURITY_DELAY:int, LATE_AIRCRAFT_DELAY:int);

load_2019 = LOAD 'airlineDelayAnalysis/2019.csv' using CSVLoader() AS (FL_DATE:chararray, OP_CARRIER:chararray,	OP_CARRIER_FL_NUM:chararray, ORIGIN:chararray, DEST:chararray, CRS_DEP_TIME:chararray, DEP_TIME:chararray, DEP_DELAY:int, TAXI_OUT:int, WHEELS_OFF:chararray, WHEELS_ON:chararray, TAXI_IN:int, CRS_ARR_TIME:chararray, ARR_TIME:chararray, ARR_DELAY:int, CANCELLED:int, CANCELLATION_CODE:int,	DIVERTED:int, CRS_ELAPSED_TIME:int, ACTUAL_ELAPSED_TIME:int, AIR_TIME:int, DISTANCE:int, CARRIER_DELAY:int, WEATHER_DELAY:int, NAS_DELAY:int, SECURITY_DELAY:int, LATE_AIRCRAFT_DELAY:int);

# remove csv headings
year_2009 = FILTER load_2009 BY (SUBSTRING(FL_DATE, 0, 7) != 'FL_DATE') AND (SUBSTRING(OP_CARRIER, 0, 10) != 'OP_CARRIER') AND (SUBSTRING(OP_CARRIER_FL_NUM, 0, 19) != 'OP_CARRIER_FL_NUM') AND (SUBSTRING(ORIGIN, 0, 6) != 'ORIGIN');

year_2010 = FILTER load_2010 BY (SUBSTRING(FL_DATE, 0, 7) != 'FL_DATE') AND (SUBSTRING(OP_CARRIER, 0, 10) != 'OP_CARRIER') AND (SUBSTRING(OP_CARRIER_FL_NUM, 0, 19) != 'OP_CARRIER_FL_NUM') AND (SUBSTRING(ORIGIN, 0, 6) != 'ORIGIN');

year_2011 = FILTER load_2011 BY (SUBSTRING(FL_DATE, 0, 7) != 'FL_DATE') AND (SUBSTRING(OP_CARRIER, 0, 10) != 'OP_CARRIER') AND (SUBSTRING(OP_CARRIER_FL_NUM, 0, 19) != 'OP_CARRIER_FL_NUM') AND (SUBSTRING(ORIGIN, 0, 6) != 'ORIGIN');

year_2012 = FILTER load_2012 BY (SUBSTRING(FL_DATE, 0, 7) != 'FL_DATE') AND (SUBSTRING(OP_CARRIER, 0, 10) != 'OP_CARRIER') AND (SUBSTRING(OP_CARRIER_FL_NUM, 0, 19) != 'OP_CARRIER_FL_NUM') AND (SUBSTRING(ORIGIN, 0, 6) != 'ORIGIN');

year_2013 = FILTER load_2013 BY (SUBSTRING(FL_DATE, 0, 7) != 'FL_DATE') AND (SUBSTRING(OP_CARRIER, 0, 10) != 'OP_CARRIER') AND (SUBSTRING(OP_CARRIER_FL_NUM, 0, 19) != 'OP_CARRIER_FL_NUM') AND (SUBSTRING(ORIGIN, 0, 6) != 'ORIGIN');

year_2014 = FILTER load_2014 BY (SUBSTRING(FL_DATE, 0, 7) != 'FL_DATE') AND (SUBSTRING(OP_CARRIER, 0, 10) != 'OP_CARRIER') AND (SUBSTRING(OP_CARRIER_FL_NUM, 0, 19) != 'OP_CARRIER_FL_NUM') AND (SUBSTRING(ORIGIN, 0, 6) != 'ORIGIN');

year_2015 = FILTER load_2015 BY (SUBSTRING(FL_DATE, 0, 7) != 'FL_DATE') AND (SUBSTRING(OP_CARRIER, 0, 10) != 'OP_CARRIER') AND (SUBSTRING(OP_CARRIER_FL_NUM, 0, 19) != 'OP_CARRIER_FL_NUM') AND (SUBSTRING(ORIGIN, 0, 6) != 'ORIGIN');

year_2016 = FILTER load_2016 BY (SUBSTRING(FL_DATE, 0, 7) != 'FL_DATE') AND (SUBSTRING(OP_CARRIER, 0, 10) != 'OP_CARRIER') AND (SUBSTRING(OP_CARRIER_FL_NUM, 0, 19) != 'OP_CARRIER_FL_NUM') AND (SUBSTRING(ORIGIN, 0, 6) != 'ORIGIN');

year_2017 = FILTER load_2017 BY (SUBSTRING(FL_DATE, 0, 7) != 'FL_DATE') AND (SUBSTRING(OP_CARRIER, 0, 10) != 'OP_CARRIER') AND (SUBSTRING(OP_CARRIER_FL_NUM, 0, 19) != 'OP_CARRIER_FL_NUM') AND (SUBSTRING(ORIGIN, 0, 6) != 'ORIGIN');

year_2018 = FILTER load_2018 BY (SUBSTRING(FL_DATE, 0, 7) != 'FL_DATE') AND (SUBSTRING(OP_CARRIER, 0, 10) != 'OP_CARRIER') AND (SUBSTRING(OP_CARRIER_FL_NUM, 0, 19) != 'OP_CARRIER_FL_NUM') AND (SUBSTRING(ORIGIN, 0, 6) != 'ORIGIN');

year_2019 = FILTER load_2019 BY (SUBSTRING(FL_DATE, 0, 7) != 'FL_DATE') AND (SUBSTRING(OP_CARRIER, 0, 10) != 'OP_CARRIER') AND (SUBSTRING(OP_CARRIER_FL_NUM, 0, 19) != 'OP_CARRIER_FL_NUM') AND (SUBSTRING(ORIGIN, 0, 6) != 'ORIGIN');

data = UNION year_2009, year_2010, year_2011, year_2012, year_2013, year_2014, year_2015, year_2016, year_2017, year_2018, year_2019; 

STORE data INTO 'airlineDelayData.csv' using PigStorage(','); 