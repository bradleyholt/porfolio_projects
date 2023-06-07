/* Data used in the following project was collected from data.gov on 28 Jan 2023. The data 
consists of three tables 'crashes', 'persons', and 'vehicles' containing features of April 2016 
and later motor vehicle accidents in New York City (NYC) for collisions where someone is injured 
or killed, or where there is at least $1000 worth of damage. Data was collected by police 
officers on form MV-104AN and input into NYPD's Finest Online Records Management System (FORMS).

The data was verified for public use and terms of use located at https://www.nyc.gov/home/terms-of-use.page 
will be followed. Additionally, a preliminary check was conducted to ensure no personally
identifiable information (PII) was included in the data.*/

/* This section focuses on database schema, table setup, data transformation, and other tasks
related to data cleaning. While some data exploration will be conducted, the overall objective 
is to ensure the data is free of errors, is not redundant and is realiable. This process will 
ensure that data extracted for analysis is accurate, complete, and consise. */

/*Creates table 'crashes' labeling each column with names from .csv file*/
CREATE TABLE IF NOT EXISTS crashes
(CRASH_DATE text, CRASH_TIME text, BOROUGH text,
 ZIP_CODE text, LATITUDE text, LONGITUDE text, LOCATION text, ON_STREET_NAME text,
 CROSS_STREET_NAME text, OFF_STREET_NAME text, NUMBER_OF_PERSONS_INJURED text, 
 NUMBER_OF_PERSONS_KILLED text,	NUMBER_OF_PEDESTRIANS_INJURED text,	NUMBER_OF_PEDESTRIANS_KILLED text,
 NUMBER_OF_CYCLIST_INJURED text, NUMBER_OF_CYCLIST_KILLED text,	NUMBER_OF_MOTORIST_INJURED text,
 NUMBER_OF_MOTORIST_KILLED text, CONTRIBUTING_FACTOR_VEHICLE_1 text, CONTRIBUTING_FACTOR_VEHICLE_2 text,
 CONTRIBUTING_FACTOR_VEHICLE_3 text, CONTRIBUTING_FACTOR_VEHICLE_4 text, CONTRIBUTING_FACTOR_VEHICLE_5 text,
 COLLISION_ID text,	VEHICLE_TYPE_CODE_1 text, VEHICLE_TYPE_CODE_2 text,	VEHICLE_TYPE_CODE_3 text,	
 VEHICLE_TYPE_CODE_4 text, VEHICLE_TYPE_CODE_5 text
);

/*Imports data from .csv file into the newly created table*/ 
COPY crashes FROM 'C:\Users\Public\Documents\crashes.csv' WITH (FORMAT csv)
;

/*Deletes header row that was imported.*/
DELETE FROM crashes
WHERE crash_date = 'CRASH_DATE';

/*Importing process was also completed for the vehicles and persons dataset*/

CREATE TABLE IF NOT EXISTS vehicles
(UNIQUE_ID text, COLLISION_ID text, CRASH_DATE text, CRASH_TIME text, VEHICLE_ID text, STATE_REGISTRATION text,
 VEHICLE_TYPE text,	VEHICLE_MAKE text, VEHICLE_MODEL text, VEHICLE_YEAR text, TRAVEL_DIRECTION text,
 VEHICLE_OCCUPANTS text, DRIVER_SEX text, DRIVER_LICENSE_STATUS text, DRIVER_LICENSE_JURISDICTION text,
 PRE_CRASH text, POINT_OF_IMPACT text, VEHICLE_DAMAGE text,	VEHICLE_DAMAGE_1 text, VEHICLE_DAMAGE_2 text,
 VEHICLE_DAMAGE_3 text,	PUBLIC_PROPERTY_DAMAGE text, PUBLIC_PROPERTY_DAMAGE_TYPE text, 
 CONTRIBUTING_FACTOR_1 text, CONTRIBUTING_FACTOR_2 text
);

/*Imports data from .csv file into the newly created table*/ 
COPY vehicles FROM 'C:\Users\Public\Documents\vehicles.csv' WITH (FORMAT csv)
;

/*Deletes header row that was imported.*/
DELETE FROM vehicles
WHERE crash_date = 'CRASH_DATE';

/* Selects all columns in crashes table limited to 10 to begin data cleaning process */
SELECT * FROM crashes
LIMIT 100
;


/* Confirming uniqueness of column crashes.collision_id(TEXT) by subtracting total rows from the 
count of unique collision_id; Result: 0 */
SELECT COUNT(DISTINCT(collision_id))-COUNT(*)
FROM crashes
;


/* Alters column crashes.collision_id to integer to ensure proper usage as a primary key */
ALTER TABLE IF EXISTS crashes
	ALTER COLUMN collision_id 
	TYPE INTEGER
		USING(collision_id::INTEGER)
;
		

/* Adds a primary key constraint to crashes.collision_id column to maintain data integrity */
ALTER TABLE IF EXISTS crashes
	ADD CONSTRAINT crashes_pk 
	PRIMARY KEY (collision_id)
;
	

/* Alters column persons.collision_id to integer to ensure proper usage as a foreign key */
ALTER TABLE IF EXISTS persons
	ALTER COLUMN collision_id 
	TYPE INTEGER
		USING(collision_id::INTEGER)
;
		
/* Adds a foreign key constraint to persons.collision_id column which references the primary key 
in crashes table */
ALTER TABLE IF EXISTS persons
	ADD CONSTRAINT persons_fk 
	FOREIGN KEY (collision_id)
	REFERENCES crashes(collision_id)
;
	
	
/* Alters column vehicles.collision_id to integer to ensure proper usage as a foreign key */
ALTER TABLE IF EXISTS vehicles
	ALTER COLUMN collision_id 
	TYPE INTEGER
		USING(collision_id::INTEGER)
;
		
/* Adds a foreign key constraint to vehicles.collision_id column which references the primary key 
in crashes table */
ALTER TABLE IF EXISTS vehicles
	ADD CONSTRAINT vehicles_fk 
	FOREIGN KEY (collision_id)
	REFERENCES crashes(collision_id)
;
	
	
/* Add column 'new_date' to the table to prepare to transform crash_date from text format. */
ALTER TABLE IF EXISTS crashes
	ADD COLUMN new_date DATE
;


/* Checks count of total rows in crashes table compare to count of crash_date column to check
for NULL values Result: total_count = 1964130, count = 1964130 */
SELECT COUNT(*) as total_count,
	   (SELECT COUNT(crash_date)
	    FROM crashes)
FROM crashes
;


/* Updates column 'new_date' with values from crash_date column converted to date format */
UPDATE crashes
	SET new_date = crash_date :: DATE
;


/* Selects both crash_date and new_date columns to visually verify that correct data 
transformation occured */
SELECT crash_date, new_date
FROM crashes
LIMIT 100
;

/* Adds column 'new_date' to the table */	
ALTER TABLE IF EXISTS crashes
	ADD COLUMN new_time TIME
;


/* Checks count of total rows in crashes table compare to count of crash_time column to check
for NULL values; Result: total_count = 1964130, count = 1964130 */
SELECT COUNT(*) as total_count,
	   (SELECT COUNT(crash_time)
	    FROM crashes)
FROM crashes
;


/* Update column 'new_time' with values from crash_date column converted to date format */
UPDATE crashes
	SET new_time = TO_TIMESTAMP(crash_time, 'HH24:MI')::TIME
;


/* Selects both crash_time and new_time columns to visually verify that correct data 
transformation occured */
SELECT crash_time, new_time
FROM crashes
LIMIT 100
;


/* Permenantly removes crash_date from crashes table */
ALTER TABLE crashes
	DROP COLUMN crash_date
;
	

/* Permenantly removes crash_time from crashes table */
ALTER TABLE crashes
	DROP COLUMN crash_time
;
	

/* Renames new_date column to crash_date */
ALTER TABLE crashes
	RENAME COLUMN new_date to crash_date
;
	

/* Renames new_date column to crash_date */
ALTER TABLE crashes
	RENAME COLUMN new_time to crash_time
;
	

/* Adds new column crash_timestamp to crashes table */
ALTER TABLE IF EXISTS crashes
	ADD COLUMN crash_timestamp TIMESTAMP
;


/* A future improvement to be noted is that the original 'crash_date' and 'crash_time'
columns in text format can be converted the same way without the need to convert to date and time 
datatypes first. Unless requirements include seperate date and time columns, this will be more 
efficient. */

/* Concatenates 'crash_date' and 'crash_time' columns into new 'crash_timestamp' column and converts
data type to TIMESTAMP */
UPDATE crashes
	SET crash_timestamp = CONCAT(crash_date, ' ', crash_time)::TIMESTAMP
;
	

/* Confirming accurate data visually in new crash_timestamp column */
SELECT crash_date, crash_time, crash_timestamp
FROM crashes
LIMIT 50
;


/* Permenantly removes crash_date and crash_time from crashes table */
ALTER TABLE crashes
	DROP COLUMN crash_date, 
	DROP COLUMN crash_time
;
	
	
/* Returns list of unique values in borough column */
SELECT DISTINCT(borough)
FROM crashes
;

	
/* Returns the count of borough(NOT NULL), count of rows, and perect of borough column with data 
input. All data was cast as numeric to allow return of decimals and rounded to the 100ths.
Approximately 69% of the borough column had a value input.*/
SELECT COUNT(borough)::NUMERIC AS borough_count, 
	   COUNT(*)::NUMERIC AS total_count, 
	   ROUND(COUNT(borough) / COUNT(*)::NUMERIC *100, 2) AS pct_input  
FROM crashes
;


/* Fills null values in 'borough' column with value 'Unspecified' to assist with future EDA.*/
UPDATE crashes
	SET borough = 
		COALESCE(borough, 'Unspecified')
;
		
/* After examining the zip_code column it was found that one or more values were empty strings.
This needed corrected in order to convert the column to a integer data type.*/

/* Trims whitespace from all values in the 'zip_code' column. */
UPDATE crashes
	   SET zip_code = TRIM(zip_code)
;

/* Sets empty string values to NULL in zip_code column.*/
UPDATE crashes
	   SET zip_code = NULL
		   WHERE zip_code=''
;
		   
/* Coverts zip_code column from text data to integer data as it is a more appropriate data type for
the feature.*/		   
ALTER TABLE IF EXISTS crashes
	ALTER COLUMN zip_code 
			     TYPE INTEGER
			      	  USING (zip_code::INTEGER)
;
	
/* Returns the count of zip_code(NOT NULL), count of total rows, and perect of zip_code column 
with data input. All data was cast as numeric to allow return of decimals and rounded to the 100ths.
Approximately 69% of the zip_code column had a value input*/
SELECT COUNT(zip_code::NUMERIC) AS zip_count, 
	   COUNT(*)::NUMERIC AS total_count, 
	   ROUND(COUNT(zip_code)::NUMERIC/ COUNT(*)::NUMERIC *100, 2) AS pct_input  
FROM crashes
;

/* Selects counts of null values in location, longitude, and latitude columns.
Results 225502 rows have null values in location, latitude, and longitude columns*/
SELECT COUNT(*) AS location_null,
	   (SELECT COUNT(*)
	    FROM crashes
	    WHERE longitude IS NULL) AS long_null,
	   (SELECT COUNT(*)
		FROM crashes
	    WHERE latitude IS NULL) AS lat_null
FROM crashes
WHERE location IS NULL
;


/* Selects count of crashes group by borough and on_street_name. Inital run of the query and examination of the results
showed duplication of many borough/on_street_name combinations most likely caused by whitespace. Updating the column by 
trimming whitespace corrected the issue.*/
SELECT borough, on_street_name, COUNT(on_street_name) as crash_count
FROM crashes
GROUP BY borough, on_street_name
ORDER BY crash_count DESC
;

/* Trims whitespace in on_street_name.*/
UPDATE crashes
	SET on_street_name = TRIM(on_street_name)
;
	
/* Trims whitespace in cross_street_name.*/
UPDATE crashes	
	SET cross_street_name = TRIM(cross_street_name)
;
	
/* Trims whitespace in off_street_name.*/
UPDATE crashes	
	SET off_street_name = TRIM(off_street_name)
;


/* Selects counts of null values in on_street_name, cross_street_name, and off_street_name columns.
Results: 409658 rows have null values in on_street_name with 79% values inputed, 
         727223 nulls in cross_street_name with 63% values inputed, 
		 1646843 nulls in off_street_name with 16% values inputed */
SELECT COUNT(*) AS on_null,
	   (SELECT COUNT(*)
	    FROM crashes
	    WHERE cross_street_name IS NULL) AS cross_null,
	   (SELECT COUNT(*)
		FROM crashes
	    WHERE off_street_name IS NULL) AS off_null,
	   (SELECT ROUND(COUNT(on_street_name) / COUNT(*)::NUMERIC *100, 2)
		FROM crashes) AS on_pct_input,
	   (SELECT ROUND(COUNT(cross_street_name) / COUNT(*)::NUMERIC *100, 2)
		FROM crashes) AS cross_pct_input,
	   (SELECT ROUND(COUNT(off_street_name) / COUNT(*)::NUMERIC *100, 2)
		FROM crashes) AS off_pct_input  
FROM crashes
WHERE on_street_name IS NULL
;

/* Returns rows where number_of_persons_injured is NULL. Investigation revealed 18 rows were NULL however had number of
injuries input in specified type of entity columns*/
SELECT *
from crashes
WHERE number_of_persons_injured IS NULL
;

/* Sets number of injuries from specific entity columns where number_of_persons_injured is NULL */
UPDATE crashes
	   SET number_of_persons_injured = (CASE WHEN number_of_pedestrians_injured::NUMERIC > 0 THEN number_of_pedestrians_injured::NUMERIC
				 					  WHEN number_of_cyclist_injured::NUMERIC > 0 THEN number_of_cyclist_injured::NUMERIC
				 					  WHEN number_of_motorist_injured::NUMERIC > 0 THEN number_of_motorist_injured::NUMERIC
				 					  ELSE 0 END)
	   WHERE number_of_persons_injured IS NULL
;	

/* Coverts number_of_persons_injured column from text data to integer data as it is a more appropriate data type for
the feature.*/		   
ALTER TABLE IF EXISTS crashes
	ALTER COLUMN number_of_persons_injured
			     TYPE INTEGER
			      	  USING (number_of_persons_injured::INTEGER)
;
					  
/* Returns rows where number_of_persons_injured is NULL. Investigation revealed 31 rows were NULL however had number of
deaths input in specified type of entity columns. */
SELECT *
from crashes
WHERE number_of_persons_killed IS NULL
;

/* Sets number of deaths from specific entity columns where number_of_persons_killed is NULL */
UPDATE crashes
	   SET number_of_persons_killed = (CASE WHEN number_of_pedestrians_killed::NUMERIC > 0 THEN number_of_pedestrians_killed::NUMERIC
				 					  WHEN number_of_cyclist_killed::NUMERIC > 0 THEN number_of_cyclist_killed::NUMERIC
				 					  WHEN number_of_motorist_killed::NUMERIC > 0 THEN number_of_motorist_killed::NUMERIC
				 					  ELSE 0 END)
	   WHERE number_of_persons_killed IS NULL
;
	  
/* Coverts number_of_persons_killed column from text data to integer data as it is a more appropriate data type for
the feature.*/		   
ALTER TABLE IF EXISTS crashes
	ALTER COLUMN number_of_persons_killed
			     TYPE INTEGER
			      	  USING (number_of_persons_killed::INTEGER)
;
					  
/* Coverts number_of_pedestrians_injured column from text data to integer data as it is a more appropriate data type for
the feature.*/		   
ALTER TABLE IF EXISTS crashes
	ALTER COLUMN number_of_pedestrians_injured
			     TYPE INTEGER
			      	  USING (number_of_pedestrians_injured::INTEGER)
;
					  
/* Coverts number_of_pedestrians_killed column from text data to integer data as it is a more appropriate data type for
the feature.*/		   
ALTER TABLE IF EXISTS crashes
	ALTER COLUMN number_of_pedestrians_killed
			     TYPE INTEGER
			      	  USING (number_of_pedestrians_killed::INTEGER)
;
					  
/* Coverts number_of_cyclist_injured column from text data to integer data as it is a more appropriate data type for
the feature.*/		   
ALTER TABLE IF EXISTS crashes
	ALTER COLUMN number_of_cyclist_injured
			     TYPE INTEGER
			      	  USING (number_of_cyclist_injured::INTEGER)
;
					  
/* Coverts number_of_cyclist_killed column from text data to integer data as it is a more appropriate data type for
the feature.*/		   
ALTER TABLE IF EXISTS crashes
	ALTER COLUMN number_of_cyclist_killed
			     TYPE INTEGER
			      	  USING (number_of_cyclist_killed::INTEGER)
;
					  
/* Coverts number_of_motorist_injured column from text data to integer data as it is a more appropriate data type for
the feature.*/		   
ALTER TABLE IF EXISTS crashes
	ALTER COLUMN number_of_motorist_injured
			     TYPE INTEGER
			      	  USING (number_of_motorist_injured::INTEGER)
;
					  
/* Coverts number_of_motorist_killed column from text data to integer data as it is a more appropriate data type for
the feature.*/		   
ALTER TABLE IF EXISTS crashes
	ALTER COLUMN number_of_motorist_killed
			     TYPE INTEGER
			      	  USING (number_of_motorist_killed::INTEGER)
;
					  
/* Selects distinct values in the contributing_factor_vehicle_1 column. Investigation revealed some values were mispelled 
or have slight variations of the same factor creating uneccessary additional factors. e.g: Illness spelled 'Illnes' 
The values 1 & 80 are also listed as factors and according to the New York State POLICE CRASH REPORT SUBMISSION INSTRUCTIONS
there is no code specified for these values, but there are less than ~120 entries with these values.*/

SELECT DISTINCT contributing_factor_vehicle_1
FROM crashes
;

/* Corrects spelling of value 'Illnes' to 'Illness' */
UPDATE crashes
	   SET contributing_factor_vehicle_1 = 'Illness'
	   WHERE contributing_factor_vehicle_1 = 'Illnes'
;

/* Corrects capitalization variation of value 'Cell Phone(hand-held)' */
UPDATE crashes
	   SET contributing_factor_vehicle_1 = 'Cell Phone (hand-held)'
	   WHERE contributing_factor_vehicle_1 = 'Cell Phone (hand-Held)'
;
	   
/* According to NY State POLICE CRASH REPORT SUBMISSION INSTRUCTIONS All crashes must have at least one apparent 
contributing factor - human, vehicular and/or environmental. However, 6037 crashes have no contributing factors listed.*/
SELECT COUNT(*) 
FROM crashes
 	 WHERE contributing_factor_vehicle_1 IS NULL
	 AND contributing_factor_vehicle_2 IS NULL
	 AND contributing_factor_vehicle_3 IS NULL
	 AND contributing_factor_vehicle_4 IS NULL
	 AND contributing_factor_vehicle_5 IS NULL
;

/* Selects distinct values from contributing_factor_vehicle_2. Investigation found the same results as in 
contibuting_factor_vehicle_1.*/
SELECT DISTINCT contributing_factor_vehicle_2
FROM crashes
;
	 
/* Corrects spelling of value 'Illnes' to 'Illness' */
UPDATE crashes
	   SET contributing_factor_vehicle_2 = 'Illness'
	   WHERE contributing_factor_vehicle_2 = 'Illnes'
;

/* Corrects capitalization variation of value 'Cell Phone(hand-held)' */
UPDATE crashes
	   SET contributing_factor_vehicle_2 = 'Cell Phone (hand-held)'
	   WHERE contributing_factor_vehicle_2 = 'Cell Phone (hand-Held)'
;

/* Selects distinct values from contributing_factor_vehicle_3. Investigation found the same results as in 
contibuting_factor_vehicle_1.*/
SELECT DISTINCT contributing_factor_vehicle_3
FROM crashes
;

/* Corrects spelling of value 'Illnes' to 'Illness' */
UPDATE crashes
	   SET contributing_factor_vehicle_3 = 'Illness'
	   WHERE contributing_factor_vehicle_3 = 'Illnes'
;

/* Corrects capitalization variation of value 'Cell Phone(hand-held)' */
UPDATE crashes
	   SET contributing_factor_vehicle_3 = 'Cell Phone (hand-held)'
	   WHERE contributing_factor_vehicle_3 = 'Cell Phone (hand-Held)'
;
	   
/* Selects distinct values from contributing_factor_vehicle_4. Investigation found the same results as in 
contibuting_factor_vehicle_1.*/
SELECT DISTINCT contributing_factor_vehicle_4
FROM crashes
;

/* Corrects spelling of value 'Illnes' to 'Illness' */
UPDATE crashes
	   SET contributing_factor_vehicle_4 = 'Illness'
	   WHERE contributing_factor_vehicle_4 = 'Illnes'
;

/* Corrects capitalization variation of value 'Cell Phone(hand-held)' */
UPDATE crashes
	   SET contributing_factor_vehicle_4 = 'Cell Phone (hand-held)'
	   WHERE contributing_factor_vehicle_4 = 'Cell Phone (hand-Held)'
;
	   
/* Selects distinct values from contributing_factor_vehicle_5.*/
SELECT DISTINCT contributing_factor_vehicle_5
FROM crashes
;


/* Select and counts the number of each type of vehicle (trimmed and uppercased) recorded. */
SELECT TRIM(UPPER(vehicle_type_code_1)) as upper_vehicle_type_code, 
	   COUNT(vehicle_type_code_1) AS count_type	   
FROM crashes
GROUP BY upper_vehicle_type_code
ORDER BY count_type
;

/* Data inputed into the database was not adequately validated to ensure accurate reporting.
This resulted in hundreds of different inputs with unclear vehicle types, multiple values that
are the same with different spellings, and/or indisinct categories. To assist with data cleaning
a selection of vehicle types will be changed to 'OTHER' by identifying the vehicle types with 
a small number count and a total sum of the resulting types equaling less than 1% of the total
count of the database to minimize negative impacts to future data analysis results*/

/* Selects the total sum of counts of vehicle types with less than a count of 50
Result: 4644 */
WITH crash_count AS
	   (SELECT TRIM(UPPER(vehicle_type_code_1)) as upper_vehicle_type_code, 
	   		  COUNT(vehicle_type_code_1) AS count_type
       FROM crashes
       GROUP BY upper_vehicle_type_code
       HAVING COUNT(vehicle_type_code_1) < 90
       )

SELECT SUM(count_type)
FROM crash_count
;

/* Trims and capitalizes all characters in vehicle_type_code_1 column */
UPDATE crashes
	   SET vehicle_type_code_1 = TRIM(UPPER(vehicle_type_code_1)
);
	   
/* Next step update vehicle_type_code_1 to OTHER when total count is less than 90 */
UPDATE crashes
SET vehicle_type_code_1 = 'OTHER'
WHERE vehicle_type_code_1 IN (
    SELECT vehicle_type_code_1
    FROM (
        SELECT vehicle_type_code_1, COUNT(*) AS count_type
        FROM crashes
        GROUP BY vehicle_type_code_1
    ) AS counts
    WHERE counts.count_type <= 90
);

/* Selects counts of all vehicle_type_codes to insure proper operation of above code and continue data cleaning process */
SELECT vehicle_type_code_1, COUNT(*) as type_count
FROM crashes
GROUP BY vehicle_type_code_1
ORDER BY type_count DESC
;

/* Changes rows with similar ambulance alias to AMBULANCE */
UPDATE crashes
SET vehicle_type_code_1 = 'AMBULANCE'
	WHERE vehicle_type_code_1 IN ('AMBU', 'AMBUL')
;

/* Changes rows with similar e-bike alias to E-BIKE */
UPDATE crashes
SET vehicle_type_code_1 = 'E-BIKE'
	WHERE vehicle_type_code_1 IN ('E-BIK')
;

/* Changes rows with similar e-scooter alias to E-SCOOTER */
UPDATE crashes
SET vehicle_type_code_1 = 'E-SCOOTER'
	WHERE vehicle_type_code_1 IN ('E-SCO')
;

/* Changes rows with similar fire truck alias to fire_truck */
UPDATE crashes
SET vehicle_type_code_1 = 'FIRE TRUCK'
	WHERE vehicle_type_code_1 IN ('FIRE')
;

/* Changes rows with similar STATION WAGON/SPORT UTILITY VEHICLE alias to STATION WAGON/SPORT UTILITY VEHICLE */
UPDATE crashes
SET vehicle_type_code_1 = 'STATION WAGON/SPORT UTILITY VEHICLE'
	WHERE vehicle_type_code_1 IN ('SPORT UTILITY / STATION WAGON')
;

/* Changes rows with similar tractor truck alias to TRACTOR TRUCK */
UPDATE crashes
SET vehicle_type_code_1 = 'TRACTOR TRUCK'
	WHERE vehicle_type_code_1 IN ('TRACT', 'TRACTOR TRUCK DIESEL', 'TRACTOR TRUCK GASOLINE')
;

/* Changes rows with similar bicycle alias to BICYCLE */
UPDATE crashes
SET vehicle_type_code_1 = 'BICYCLE'
	WHERE vehicle_type_code_1 IN ('BIKE')
;

/* Changes rows with similar motorcycle alias to MOTORCYCLE */
UPDATE crashes
SET vehicle_type_code_1 = 'MOTORCYCLE'
	WHERE vehicle_type_code_1 IN ('MOTORBIKE')
;

/* Changes rows with similar pick-up alias to PICK-UP TRUCK */
UPDATE crashes
SET vehicle_type_code_1 = 'PICK-UP TRUCK'
	WHERE vehicle_type_code_1 IN ('PK', 'TRUCK')
;

/* Changes rows with similar sedan alias to SEDAN */
UPDATE crashes
SET vehicle_type_code_1 = 'SEDAN'
	WHERE vehicle_type_code_1 IN ('4 DR SEDAN', '3 DOOR', '2 DR SEDAN')
;

/* Changes rows with similar sedan alias to SEDAN */
UPDATE crashes
SET vehicle_type_code_1 = 'MOPED'
	WHERE vehicle_type_code_1 IN ('MOTORSCOOTER')
;


/* Returns list of vehicle_type_code_1, total count of each type, and percentage of total crashes */
SELECT vehicle_type_code_1, 
	   COUNT(*) as type_count,
	   ROUND(COUNT(*)::NUMERIC/(SELECT COUNT(*)::NUMERIC
											FROM crashes)*100, 2) AS pct_of_total
FROM crashes
GROUP BY vehicle_type_code_1
ORDER BY type_count DESC
;

/* Update column 'new_time' with values from crash_date column converted to date format */
UPDATE vehicles
	SET crash_time = TO_TIMESTAMP(crash_time, 'HH24:MI')::TIME
;

/* Adds new column crash_timestamp to vehicles table */
ALTER TABLE IF EXISTS vehicles
	ADD COLUMN crash_timestamp TIMESTAMP
;

/* Concatenates 'crash_date' and 'crash_time' columns into new 'crash_timestamp' column and converts
data type to TIMESTAMP */
UPDATE vehicles
	SET crash_timestamp = CONCAT(crash_date, ' ', crash_time)::TIMESTAMP
;

/* Permenantly removes crash_date from vehicles table */
ALTER TABLE vehicles
	DROP COLUMN crash_date,
	DROP COLUMN crash_time
;

/* Selects distinct values from the state_registration for inspection. Some foreign country codes are included in the 
values, but no unusual values were found from 83 distinct values*/
SELECT DISTINCT(state_registration), COUNT(*) 
FROM vehicles
GROUP BY state_registration 
ORDER BY state_registration DESC;
;

/* Selects distinct values from the vehicle_type for inspection.*/
SELECT DISTINCT(vehicle_type), COUNT(*) 
FROM vehicles
GROUP BY vehicle_type
ORDER BY vehicle_type DESC;
;

/* Select and counts the number of each type of vehicle (trimmed and uppercased) recorded. */
SELECT TRIM(UPPER(vehicle_type)) as upper_vehicle_type, 
	   COUNT(*) AS count_type	   
FROM vehicles
GROUP BY upper_vehicle_type
ORDER BY count_type
;

/* To clean and transform the vehicle_type column all previous transformations from the vehicle_type_code columns will are
reused to conduct the same tranformation. Afterwards the remaining values will be reassessed for update. */

/* Trims and capitalizes all characters in vehicle_type column */
UPDATE vehicles
	   SET vehicle_type = TRIM(UPPER(vehicle_type)
);

/* Changes rows with similar ambulance alias to AMBULANCE */
UPDATE vehicles
SET vehicle_type = 'AMBULANCE'
	WHERE vehicle_type IN ('AMBU', 'AMBUL', 'AMBIANCE', 'AMBULAMCE', 'AMBULANCE"', 'AMBULCANCE', 'AMDU', 'AMUBL', 
						   'AMUBULANCE', 'AMULA', 'AMULANCE', 'ANBUL', 'ALMBULANCE', 'AMABU', 'AMBUKANCE', 'ABULANCE',
						  'AMBULANCE''')
;

/* Changes rows with similar e-bike alias to E-BIKE */
UPDATE vehicles
SET vehicle_type = 'E-BIKE'
	WHERE vehicle_type IN ('E-BIK')
;

/* Changes rows with similar e-scooter alias to E-SCOOTER */
UPDATE vehicles
SET vehicle_type = 'E-SCOOTER'
	WHERE vehicle_type IN ('E-SCO')
;

/* Changes rows with similar fire truck alias to fire_truck */
UPDATE vehicles
SET vehicle_type = 'FIRE TRUCK'
	WHERE vehicle_type IN ('FIRE')
;

/* Changes rows with similar STATION WAGON/SPORT UTILITY VEHICLE alias to STATION WAGON/SPORT UTILITY VEHICLE */
UPDATE vehicles
SET vehicle_type = 'STATION WAGON/SPORT UTILITY VEHICLE'
	WHERE vehicle_type IN ('SPORT UTILITY / STATION WAGON')
;

/* Changes rows with similar tractor truck alias to TRACTOR TRUCK */
UPDATE vehicles
SET vehicle_type = 'TRACTOR TRUCK'
	WHERE vehicle_type IN ('TRACT', 'TRACTOR TRUCK DIESEL', 'TRACTOR TRUCK GASOLINE')
;

/* Changes rows with similar bicycle alias to BICYCLE */
UPDATE vehicles
SET vehicle_type = 'BICYCLE'
	WHERE vehicle_type IN ('BIKE')
;


/* Changes rows with similar motorcycle alias to MOTORCYCLE */
UPDATE vehicles
SET vehicle_type = 'MOTORCYCLE'
	WHERE vehicle_type IN ('MOTORBIKE')
;

/* Changes rows with similar pick-up alias to PICK-UP TRUCK */
UPDATE vehicles
SET vehicle_type = 'PICK-UP TRUCK'
	WHERE vehicle_type IN ('PK', 'TRUCK')
;

/* Changes rows with similar sedan alias to SEDAN */
UPDATE vehicles
SET vehicle_type = 'SEDAN'
	WHERE vehicle_type IN ('4 DR SEDAN', '3 DOOR', '2 DR SEDAN')
;

/* Changes rows with similar sedan alias to SEDAN */
UPDATE vehicles
SET vehicle_type = 'MOPED'
	WHERE vehicle_type IN ('MOTORSCOOTER')
;

/* Changes rows with similar unknown alias to UNKNOWN */
UPDATE vehicles
SET vehicle_type = 'UNKNOWN'
	WHERE vehicle_type IN ('UNK FEMALE', 'UNK T', 'UNK,', 'UNK.', 'UNKL', 'UNKWN', 'UNLNO', 'UNNKO')
;

/* Select and counts the number of each type of vehicle (trimmed and uppercased) recorded. */
SELECT DISTINCT(vehicle_type), 
	   COUNT(*) AS count_type	   
FROM vehicles
GROUP BY vehicle_type
ORDER BY count_type
;

/* Next step update vehicle_type to OTHER when total count is less than 90 */
UPDATE vehicles
SET vehicle_type = 'OTHER'
WHERE vehicle_type IN (
    SELECT vehicle_type
    FROM (
        SELECT vehicle_type, COUNT(*) AS count_type
        FROM vehicles
        GROUP BY vehicle_type
    ) AS counts
    WHERE counts.count_type <= 90
);


/* Due to the cleaning process, the vehicle_type column of the vehicles table may not have a match in the 
vehicle_type_code of the crashes table. After checking the high count vehicle types with the query below,
only a very small amount of rows did not have a match and will not significantly impact further analysis.*/
SELECT c.collision_id, vehicle_type, vehicle_type_code_1, vehicle_type_code_2, vehicle_type_code_3, 
vehicle_type_code_4, vehicle_type_code_5
FROM crashes as c
LEFT JOIN vehicles as v
ON v.collision_id = c.collision_id
WHERE vehicle_type = 'SEDAN' AND 'SEDAN' NOT IN(vehicle_type_code_1, vehicle_type_code_2, 
												vehicle_type_code_3, vehicle_type_code_4, 
												vehicle_type_code_5)
ORDER BY c.collision_id
;

/* Select count of distinct of vehicle makes to begin cleaning of vehicle_make column. 7951 different
of vehicle_makes are present with many different values of the same brand or ambiguous makes.*/
SELECT DISTINCT(TRIM(UPPER(vehicle_make))) as vehicle_make, 
       COUNT(*) as vehicle_make_count
FROM vehicles
GROUP BY vehicle_make
ORDER BY vehicle_make_count DESC
;

/* Standardizes vehicle make values by trimming white space and capitalizing all letters */
UPDATE vehicles
SET vehicle_make = (TRIM(UPPER(vehicle_make)))
;

/* Find all values of vehicle makes containing 'TOYT'*/
SELECT vehicle_make, 
	   COUNT(*) as vehicle_make_count
FROM vehicles
	WHERE vehicle_make LIKE('%TOYT%')
GROUP BY vehicle_make
ORDER BY vehicle_make_count DESC
;

/* Update table by using above query to set return values to 'TOYOTA' */
UPDATE vehicles
SET vehicle_make = 'TOYOTA'
	WHERE vehicle_make LIKE('%TOYT%')
;


/* Find all values of vehicle makes containing 'HOND'*/
SELECT vehicle_make, 
	   COUNT(*) as vehicle_make_count
FROM vehicles
	WHERE vehicle_make LIKE('%HOND%')
GROUP BY vehicle_make
ORDER BY vehicle_make_count DESC
;

/* Update table by using above query to set return values to 'HONDA' */
UPDATE vehicles
SET vehicle_make = 'HONDA'
	WHERE vehicle_make LIKE('%HOND%')
;

/* Find all values of vehicle makes containing 'NISS'*/
SELECT vehicle_make, 
       COUNT(*) as vehicle_make_count
FROM vehicles
	WHERE vehicle_make LIKE('%NISS%')
GROUP BY vehicle_make
ORDER BY vehicle_make_count DESC
;

/* Update table by using above query to set return values to 'NISSAN' */
UPDATE vehicles
SET vehicle_make = 'NISSAN'
	WHERE vehicle_make LIKE('%NISS%')
;

/* Find all values of vehicle makes containing 'FORD'*/
SELECT vehicle_make, COUNT(*) as vehicle_make_count
FROM vehicles
	 WHERE vehicle_make LIKE('%FORD%')
GROUP BY vehicle_make
ORDER BY vehicle_make_count DESC
;

/* Update table by using above query to set return values to 'FORD' */
UPDATE vehicles
SET vehicle_make = 'FORD'
	WHERE vehicle_make LIKE('%FORD%')
;

/* Find all values of vehicle makes containing 'CHEV'*/
SELECT vehicle_make, 
       COUNT(*) as vehicle_make_count
FROM vehicles
	 WHERE vehicle_make LIKE('%CHEV%')
GROUP BY vehicle_make
ORDER BY vehicle_make_count DESC
;

/* Update table by using above query to set return values to 'CHEVROLET' */
UPDATE vehicles
SET vehicle_make = 'CHEVROLET'
	WHERE vehicle_make LIKE('%CHEV%')
	OR vehicle_make = 'CHREVOLET'
	OR vehicle_make = 'CHREVROLET'
	OR vehicle_make = 'CHERVOLET'
;

/* Find all values of vehicle makes containing 'HYUN'*/
SELECT vehicle_make, 
       COUNT(*) as vehicle_make_count
FROM vehicles
	 WHERE vehicle_make LIKE('%HYUN%')
GROUP BY vehicle_make
ORDER BY vehicle_make_count DESC
;

/* Update table by using above query to set return values to 'HYUNDAI' */
UPDATE vehicles
SET vehicle_make = 'HYUNDAI'
	WHERE vehicle_make LIKE('%HYUN%')
;

/* Find all values of vehicle makes containing 'BMW'*/
SELECT vehicle_make, 
       COUNT(*) as vehicle_make_count
FROM vehicles
	 WHERE vehicle_make LIKE('%BMW%')
GROUP BY vehicle_make
ORDER BY vehicle_make_count DESC
;

/* Update table by using above query to set return values to 'BMW' */
UPDATE vehicles
SET vehicle_make = 'BMW'
	WHERE vehicle_make LIKE('%BMW%')
;

/* Find all values of vehicle makes containing 'MERZ' 'MERCEDES' OR 'BENZ'*/
SELECT vehicle_make, 
       COUNT(*) as vehicle_make_count
FROM vehicles
	 WHERE vehicle_make LIKE('%MERZ%')
	       OR vehicle_make LIKE ('%MERCEDES%')
		   OR vehicle_make LIKE ('%BENZ%')
GROUP BY vehicle_make
ORDER BY vehicle_make_count DESC
;

/* Update table by using above query to set return values to 'MERCEDES' */
UPDATE vehicles
SET vehicle_make = 'MERCEDES'
	WHERE vehicle_make LIKE('%MERZ%')
	       OR vehicle_make LIKE ('%MERCEDES%')
		   OR vehicle_make LIKE ('%BENZ%')
;

/* Find all values of vehicle makes containing 'JEE'*/
SELECT vehicle_make, 
       COUNT(*) as vehicle_make_count
FROM vehicles
	 WHERE vehicle_make LIKE('%JEE%')
GROUP BY vehicle_make
ORDER BY vehicle_make_count DESC
;

/* Update table by using above query to set return values to 'JEEP' */
UPDATE vehicles
SET vehicle_make = 'JEEP'
	WHERE vehicle_make LIKE('%JEE%')
;

/* Find all values of vehicle makes containing 'DOD', 'RAM', but not like 'RAMB' (to exclude
'rambler' matches*/
SELECT vehicle_make, 
       COUNT(*) as vehicle_make_count
FROM vehicles
	 WHERE vehicle_make LIKE('%DOD%')
	       OR vehicle_make LIKE('RAM%')
		   AND vehicle_make NOT LIKE('%RAMB%')
GROUP BY vehicle_make
ORDER BY vehicle_make_count DESC
;

/* Update table by using above query to set return values to 'DODGE' */
UPDATE vehicles
SET vehicle_make = 'DODGE'
	WHERE vehicle_make LIKE('%DOD%')
	       OR vehicle_make LIKE('RAM%')
		   AND vehicle_make NOT LIKE('%RAMB%')
;

/* Find all values of vehicle makes containing 'LEX' or 'LEXUS'*/
SELECT vehicle_make, 
       COUNT(*) as vehicle_make_count
FROM vehicles
	 WHERE vehicle_make LIKE('%LEXS%')
	 OR vehicle_make LIKE ('LEXUS')
GROUP BY vehicle_make
ORDER BY vehicle_make_count DESC
;

/* Update table by using above query to set return values to 'LEXUS' */
UPDATE vehicles
SET vehicle_make = 'LEXUS'
	WHERE vehicle_make LIKE('%LEXS%')
	 OR vehicle_make LIKE ('LEXUS')
;

/* Find all values of vehicle makes containing 'ACU'*/
SELECT vehicle_make, 
       COUNT(*) as vehicle_make_count
FROM vehicles
	 WHERE vehicle_make LIKE('%ACU%')
GROUP BY vehicle_make
ORDER BY vehicle_make_count DESC
;

/* Update table by using above query to set return values to 'ACURA' */
UPDATE vehicles
SET vehicle_make = 'ACURA'
	WHERE vehicle_make LIKE('%ACU%')
;

/* Find all values of vehicle makes containing 'INF'*/
SELECT vehicle_make, 
       COUNT(*) as vehicle_make_count
FROM vehicles
	 WHERE vehicle_make LIKE('%INF%')
GROUP BY vehicle_make
ORDER BY vehicle_make_count DESC
;

/* Update table by using above query to set return values to 'ACURA' */
UPDATE vehicles
SET vehicle_make = 'INFINITY'
	WHERE vehicle_make LIKE('%INF%')
;

/* Find all values of vehicle makes containing 'VOLK'*/
SELECT vehicle_make, 
       COUNT(*) as vehicle_make_count
FROM vehicles
	 WHERE vehicle_make LIKE('%VOLK%')
GROUP BY vehicle_make
ORDER BY vehicle_make_count DESC
;

/* Update table by using above query to set return values to 'VOLKSWAGON' */
UPDATE vehicles
SET vehicle_make = 'VOLKSWAGON'
	WHERE vehicle_make LIKE('%VOLK%')
;

/* Find all values of vehicle makes containing 'CHR' but not 'CHRIS'*/
SELECT vehicle_make, 
       COUNT(*) as vehicle_make_count
FROM vehicles
	 WHERE vehicle_make LIKE('%CHR%')
	       AND vehicle_make NOT LIKE ('%CHRIS%')
GROUP BY vehicle_make
ORDER BY vehicle_make_count DESC
;

/* Update table by using above query to set return values to 'CHRYSLER' */
UPDATE vehicles
SET vehicle_make = 'CHRYSLER'
	WHERE vehicle_make LIKE('%CHR%')
	       AND vehicle_make NOT LIKE ('%CHRIS%')
;

/* Find all values of vehicle makes containing 'SUB'*/
SELECT vehicle_make, 
       COUNT(*) as vehicle_make_count
FROM vehicles
	 WHERE vehicle_make LIKE('%SUBA%')
	 AND vehicle_make NOT LIKE ('JIANGSUBAODIAO')
GROUP BY vehicle_make
ORDER BY vehicle_make_count DESC
;

/* Update table by using above query to set return values to 'SUBARU' */
UPDATE vehicles
SET vehicle_make = 'SUBARU'
	WHERE vehicle_make LIKE('%SUBA%')
	       AND vehicle_make NOT LIKE ('JIANGSUBAODIAO')
;

/* Find all values of vehicle makes containing 'KIA'*/
SELECT vehicle_make, 
       COUNT(*) as vehicle_make_count
FROM vehicles
	 WHERE vehicle_make LIKE('%KIA%')
	 AND vehicle_make NOT LIKE ('ANAKIA')
GROUP BY vehicle_make
ORDER BY vehicle_make_count DESC
;

/* Update table by using above query to set return values to 'KIA' */
UPDATE vehicles
SET vehicle_make = 'KIA'
	WHERE vehicle_make LIKE('%KIA%')
	       AND vehicle_make NOT LIKE ('ANAKIA')
;

/* Find all values of vehicle makes containing 'GMC'*/
SELECT vehicle_make, 
       COUNT(*) as vehicle_make_count
FROM vehicles
	 WHERE vehicle_make LIKE('%GMC%')
GROUP BY vehicle_make
ORDER BY vehicle_make_count DESC
;

/* Update table by using above query to set return values to 'KIA' */
UPDATE vehicles
SET vehicle_make = 'GMC'
	WHERE vehicle_make LIKE('%GMC%')
;

/* Find all values of vehicle makes containing 'AUDI'*/
SELECT vehicle_make, 
       COUNT(*) as vehicle_make_count
FROM vehicles
	 WHERE vehicle_make LIKE('%AUDI%')
GROUP BY vehicle_make
ORDER BY vehicle_make_count DESC
;

/* Update table by using above query to set return values to 'KIA' */
UPDATE vehicles
SET vehicle_make = 'AUDI'
	WHERE vehicle_make LIKE('%AUDI%')
;

/* Find all values of vehicle makes containing 'MAZ'*/
SELECT vehicle_make, 
       COUNT(*) as vehicle_make_count
FROM vehicles
	 WHERE vehicle_make LIKE('%MAZ%')
GROUP BY vehicle_make
ORDER BY vehicle_make_count DESC
;

/* Update table by using above query to set return values to 'MAZDA' */
UPDATE vehicles
SET vehicle_make = 'MAZDA'
	WHERE vehicle_make LIKE('%MAZ%')
;

/* Find all values of vehicle makes containing 'LINC'*/
SELECT vehicle_make, 
       COUNT(*) as vehicle_make_count
FROM vehicles
	 WHERE vehicle_make LIKE('%LINC%')
GROUP BY vehicle_make
ORDER BY vehicle_make_count DESC
;

/* Update table by using above query to set return values to 'LINCOLN' */
UPDATE vehicles
SET vehicle_make = 'LINCOLN'
	WHERE vehicle_make LIKE('%LINC%')
;

/* Find all values of vehicle makes containing 'FREI' or 'FRHT'*/
SELECT vehicle_make, 
       COUNT(*) as vehicle_make_count
FROM vehicles
	 WHERE vehicle_make LIKE('%FREI%')
	       OR vehicle_make LIKE('%FRHT%')
GROUP BY vehicle_make
ORDER BY vehicle_make_count DESC
;

/* Update table by using above query to set return values to 'FREIGHTLINER' */
UPDATE vehicles
SET vehicle_make = 'FREIGHTLINER'
	WHERE vehicle_make LIKE('%FREI%')
		  OR vehicle_make LIKE('%FRHT%')  
;

/* Find all values of vehicle makes containing 'FREI' or 'FRHT'*/
SELECT vehicle_make, 
       COUNT(*) as vehicle_make_count
FROM vehicles
	 WHERE vehicle_make LIKE('%MACK%')
GROUP BY vehicle_make
ORDER BY vehicle_make_count DESC
;

/* Update table by using above query to set return values to 'MACK' */
UPDATE vehicles
SET vehicle_make = 'MACK'
	WHERE vehicle_make LIKE('%MACK%')
;

/* Find all values of vehicle makes containing 'CADI'*/
SELECT vehicle_make, 
       COUNT(*) as vehicle_make_count
FROM vehicles
	 WHERE vehicle_make LIKE('%CADI%')
	  AND vehicle_make NOT LIKE('%CASCADIA%')
GROUP BY vehicle_make
ORDER BY vehicle_make_count DESC
;

/* Update table by using above query to set return values to 'CADILLAC' */
UPDATE vehicles
SET vehicle_make = 'CADILLAC'
	WHERE vehicle_make LIKE('%CADI%')
	AND vehicle_make NOT LIKE('%CASCADIA%')
;

/* Find all values of vehicle makes containing 'MITS'*/
SELECT vehicle_make, 
       COUNT(*) as vehicle_make_count
FROM vehicles
	 WHERE vehicle_make LIKE('%MITS%')
GROUP BY vehicle_make
ORDER BY vehicle_make_count DESC
;

/* Update table by using above query to set return values to 'CADILLAC' */
UPDATE vehicles
SET vehicle_make = 'MITSUBISHI'
	WHERE vehicle_make LIKE('%MITS%')
;

/* Find all values of vehicle makes containing 'INTL'*/
SELECT vehicle_make, 
       COUNT(*) as vehicle_make_count
FROM vehicles
	 WHERE vehicle_make LIKE('%INTL%')
	 OR vehicle_make LIKE ('%INTER%')
	 AND vehicle_make NOT LIKE('INTERSTATE')
	  AND vehicle_make NOT LIKE('SPRINTER')
	   AND vehicle_make NOT LIKE('NASSAU')
	   AND vehicle_make NOT LIKE('%INTERCEPTOR%')
GROUP BY vehicle_make
ORDER BY vehicle_make_count DESC
;

/* Update table by using above query to set return values to 'INTERNATIONAL' */
UPDATE vehicles
SET vehicle_make = 'INTERNATIONAL'
	WHERE vehicle_make LIKE('%INTL%')
	 OR vehicle_make LIKE ('%INTER%')
	 AND vehicle_make NOT LIKE('INTERSTATE')
	  AND vehicle_make NOT LIKE('SPRINTER')
	   AND vehicle_make NOT LIKE('NASSAU')
	   AND vehicle_make NOT LIKE('%INTERCEPTOR%')
;

/* Find filtered values of vehicle makes containing 'VOL'*/
SELECT vehicle_make, 
       COUNT(*) as vehicle_make_count
FROM vehicles
	 WHERE vehicle_make LIKE('%VOLV%')
	 AND vehicle_make NOT LIKE ('CITY%')
GROUP BY vehicle_make
ORDER BY vehicle_make_count DESC
;

/* Update table by using above query to set return values to 'VOLVO' */
UPDATE vehicles
SET vehicle_make = 'VOLVO'
	WHERE vehicle_make LIKE('%VOLV%')
	 AND vehicle_make NOT LIKE ('CITY%')
;

/* Find filtered values of vehicle makes containing 'LNDR' or 'LAND R'*/
SELECT vehicle_make, 
       COUNT(*) as vehicle_make_count
FROM vehicles
	 WHERE vehicle_make LIKE('%LNDR%')
	 OR vehicle_make LIKE ('%LAND R%')
GROUP BY vehicle_make
ORDER BY vehicle_make_count DESC
;


/* Update table by using above query to set return values to 'LAND ROVER' */
UPDATE vehicles
SET vehicle_make = 'LAND ROVER'
	WHERE vehicle_make LIKE('%LNDR%')
	 OR vehicle_make LIKE ('%LAND R%')
;

/* Find filtered values of vehicle makes containing 'HIN'*/
SELECT vehicle_make, 
       COUNT(*) as vehicle_make_count
FROM vehicles
	 WHERE vehicle_make LIKE('%HIN%')
	 AND vehicle_make NOT LIKE ('%MACHINE%')
	 AND vehicle_make NOT LIKE ('%CHINA%')
GROUP BY vehicle_make
ORDER BY vehicle_make_count DESC
;

/* Update table by using above query to set return values to 'HINO' */
UPDATE vehicles
SET vehicle_make = 'HINO'
	 WHERE vehicle_make LIKE('%HIN%')
	 AND vehicle_make NOT LIKE ('%MACHINE%')
	 AND vehicle_make NOT LIKE ('%CHINA%')
;

/* Find filtered values of vehicle makes containing 'BUIC'*/
SELECT vehicle_make, 
       COUNT(*) as vehicle_make_count
FROM vehicles
	 WHERE vehicle_make LIKE('%BUIC%')
GROUP BY vehicle_make
ORDER BY vehicle_make_count DESC
;

/* Update table by using above query to set return values to 'BUICK' */
UPDATE vehicles
SET vehicle_make = 'BUICK'
	 WHERE vehicle_make LIKE('%BUIC%')
;

/* Find filtered values of vehicle makes containing 'MERC'*/
SELECT vehicle_make, 
       COUNT(*) as vehicle_make_count
FROM vehicles
	 WHERE vehicle_make LIKE('%MERC%')
	 AND vehicle_make NOT LIKE('%COMMERCIAL%')
GROUP BY vehicle_make
ORDER BY vehicle_make_count DESC
;

/* Update table by using above query to set return values to 'MERCURY' */
UPDATE vehicles
SET vehicle_make = 'MERCURY'
	 WHERE vehicle_make LIKE('%MERC%')
	 AND vehicle_make NOT LIKE('%COMMERCIAL%')
;

/* Find filtered values of vehicle makes containing 'ISU'*/
SELECT vehicle_make, 
       COUNT(*) as vehicle_make_count
FROM vehicles
	 WHERE vehicle_make LIKE('%ISU%')
GROUP BY vehicle_make
ORDER BY vehicle_make_count DESC
;

/* Update table by using above query to set return values to 'ISUZU' */
UPDATE vehicles
SET vehicle_make = 'ISUZU'
	 WHERE vehicle_make LIKE('%ISU%')
;

/* Find filtered values of vehicle makes containing 'KW' or 'KEN'*/
SELECT vehicle_make, 
       COUNT(*) as vehicle_make_count
FROM vehicles
	 WHERE vehicle_make LIKE('%KW%')
	 OR vehicle_make LIKE('%KEN%')
	 AND vehicle_make NOT LIKE('%KENTUCKY%')
GROUP BY vehicle_make
ORDER BY vehicle_make_count DESC
;

/* Update table by using above query to set return values to 'KENWORTH' */
UPDATE vehicles
SET vehicle_make = 'KENWORTH'
	 WHERE vehicle_make LIKE('%KW%')
	 OR vehicle_make LIKE('%KEN%')
	 AND vehicle_make NOT LIKE('%KENTUCKY%')
;

/* Find filtered values of vehicle makes containing 'PTRB' or 'PET'*/
SELECT vehicle_make, 
       COUNT(*) as vehicle_make_count
FROM vehicles
	 WHERE vehicle_make LIKE('%PTRB%')
	 OR vehicle_make LIKE('%PET%')
GROUP BY vehicle_make
ORDER BY vehicle_make_count DESC
;

/* Update table by using above query to set return values to 'PETERBILT' */
UPDATE vehicles
SET vehicle_make = 'PETERBILT'
	 WHERE vehicle_make LIKE('%PTRB%')
	 OR vehicle_make LIKE('%PET%')
;

/* Find filtered values of vehicle makes containing 'PORS'*/
SELECT vehicle_make, 
       COUNT(*) as vehicle_make_count
FROM vehicles
	 WHERE vehicle_make LIKE('%PORS%')
GROUP BY vehicle_make
ORDER BY vehicle_make_count DESC
;

/* Update table by using above query to set return values to 'PORSCHE' */
UPDATE vehicles
SET vehicle_make = 'PORSCHE'
	 WHERE vehicle_make LIKE('%PORS%')
;

/* Find filtered values of vehicle makes containing 'MNNI' or 'MINI'*/
SELECT vehicle_make, 
       COUNT(*) as vehicle_make_count
FROM vehicles
	 WHERE vehicle_make LIKE('%MNNI%')
	 OR vehicle_make LIKE('MINI%')
	 AND vehicle_make NOT LIKE('%VAN%')
	 AND vehicle_make NOT LIKE('%BUS%')
	 AND vehicle_make NOT LIKE('MINIMOTORS%')
GROUP BY vehicle_make
ORDER BY vehicle_make_count DESC
;

/* Update table by using above query to set return values to 'MINI' */
UPDATE vehicles
SET vehicle_make = 'MINI'
	 WHERE vehicle_make LIKE('%MNNI%')
	 OR vehicle_make LIKE('MINI%')
	 AND vehicle_make NOT LIKE('%VAN%')
	 AND vehicle_make NOT LIKE('%BUS%')
	 AND vehicle_make NOT LIKE('MINIMOTORS%')
;


/* Find filtered values of vehicle makes containing 'STRN' OR 'SATURN'*/
SELECT vehicle_make, 
       COUNT(*) as vehicle_make_count
FROM vehicles
	 WHERE vehicle_make LIKE('%STRN%')
	 OR vehicle_make LIKE('%SATURN%')
GROUP BY vehicle_make
ORDER BY vehicle_make_count DESC
;

/* Update table by using above query to set return values to 'SATURN' */
UPDATE vehicles
SET vehicle_make = 'SATURN'
	 WHERE vehicle_make LIKE('%STRN%')
	 OR vehicle_make LIKE('%SATURN%')
;


/* Find filtered values of vehicle makes containing 'PONT'*/
SELECT vehicle_make, 
       COUNT(*) as vehicle_make_count
FROM vehicles
	 WHERE vehicle_make LIKE('%PONT%')
GROUP BY vehicle_make
ORDER BY vehicle_make_count DESC
;

/* Update table by using above query to set return values to 'PONTIAC' */
UPDATE vehicles
SET vehicle_make = 'PONTIAC'
	 WHERE vehicle_make LIKE('%PONT%')
;

/* Find filtered values of vehicle makes containing 'JAG'*/
SELECT vehicle_make, 
       COUNT(*) as vehicle_make_count
FROM vehicles
	 WHERE vehicle_make LIKE('%JAG%')
GROUP BY vehicle_make
ORDER BY vehicle_make_count DESC
;

/* Update table by using above query to set return values to 'JAGUAR' */
UPDATE vehicles
SET vehicle_make = 'JAGUAR'
	 WHERE vehicle_make LIKE('%JAG%')
	 AND vehicle_make NOT LIKE ('%XMVA%')
;

/* Find filtered values of vehicle makes containing 'YAMA'*/
SELECT vehicle_make, 
       COUNT(*) as vehicle_make_count
FROM vehicles
	 WHERE vehicle_make LIKE('%YAMA%')
	 AND vehicle_make NOT LIKE('%YAMASAKI%')
	 AND vehicle_make NOT LIKE('%YAMASKI%')
	 AND vehicle_make NOT LIKE('YAMAN')
GROUP BY vehicle_make
ORDER BY vehicle_make_count DESC
;

/* Update table by using above query to set return values to 'YAMAHA' */
UPDATE vehicles
SET vehicle_make = 'YAMAHA'
	 WHERE vehicle_make LIKE('%YAMA%')
	 AND vehicle_make NOT LIKE('%YAMASAKI%')
	 AND vehicle_make NOT LIKE('%YAMASKI%')
	 AND vehicle_make NOT LIKE('YAMAN')
;

/* Find filtered values of vehicle makes containing 'TESL'*/
SELECT vehicle_make, 
       COUNT(*) as vehicle_make_count
FROM vehicles
	 WHERE vehicle_make LIKE('%TESL%')
GROUP BY vehicle_make
ORDER BY vehicle_make_count DESC
;

/* Update table by using above query to set return values to 'TESLA' */
UPDATE vehicles
SET vehicle_make = 'TESLA'
	 WHERE vehicle_make LIKE('%TESL%')
;

/* Find filtered values of vehicle makes containing 'SUZI'*/
SELECT vehicle_make, 
       COUNT(*) as vehicle_make_count
FROM vehicles
	 WHERE vehicle_make LIKE('%SUZI%')
	 OR vehicle_make LIKE('%SUZU%')
	 AND vehicle_make NOT LIKE('ISUZU')
GROUP BY vehicle_make
ORDER BY vehicle_make_count DESC
;

/* Update table by using above query to set return values to 'SUZUKI' */
UPDATE vehicles
SET vehicle_make = 'SUZUKI'
	  WHERE vehicle_make LIKE('%SUZI%')
	  OR vehicle_make LIKE('%SUZU%')
	  AND vehicle_make NOT LIKE('ISUZU')
;

/* Find filtered values of vehicle makes containing 'SMRT'*/
SELECT vehicle_make, 
       COUNT(*) as vehicle_make_count
FROM vehicles
	 WHERE vehicle_make LIKE('%SMART%')
	 OR vehicle_make LIKE('%SMRT%')
GROUP BY vehicle_make
ORDER BY vehicle_make_count DESC
;

/* Update table by using above query to set return values to 'SMART' */
UPDATE vehicles
SET vehicle_make = 'SMART'
	   WHERE vehicle_make LIKE('%SMART%')
	   OR vehicle_make LIKE('%SMRT%')
;

/* Find filtered values of vehicle makes containing 'HD'*/
SELECT vehicle_make, 
       COUNT(*) as vehicle_make_count
FROM vehicles
	 WHERE vehicle_make LIKE('%HARL%')
	 OR vehicle_make LIKE('%HD%')
GROUP BY vehicle_make
ORDER BY vehicle_make_count DESC
;

/* Update table by using above query to set return values to 'HARLEY DAVIDSON' */
UPDATE vehicles
SET vehicle_make = 'HARLEY DAVIDSON'
	   WHERE vehicle_make LIKE('%HARL%')
	   OR vehicle_make LIKE('%HD%')
;

/* Find filtered values of vehicle makes containing 'HD'*/
SELECT vehicle_make, 
       COUNT(*) as vehicle_make_count
FROM vehicles
	 WHERE vehicle_make LIKE('%KAWK%')
	 OR vehicle_make LIKE('%KAWA%')
GROUP BY vehicle_make
ORDER BY vehicle_make_count DESC
;

/* Update table by using above query to set return values to 'KAWASAKI' */
UPDATE vehicles
SET vehicle_make = 'KAWASAKI'
	   WHERE vehicle_make LIKE('%KAWK%')
	   OR vehicle_make LIKE('%KAWA%')
;

/* Find filtered values of vehicle makes containing 'FIAT'*/
SELECT vehicle_make, 
       COUNT(*) as vehicle_make_count
FROM vehicles
	 WHERE vehicle_make LIKE('%FIAT%')
GROUP BY vehicle_make
ORDER BY vehicle_make_count DESC
;

/* Update table by using above query to set return values to 'FIAT' */
UPDATE vehicles
SET vehicle_make = 'FIAT'
	   WHERE vehicle_make LIKE('%FIAT%')
;

/* Find filtered values of vehicle makes containing 'FIAT'*/
SELECT vehicle_make, 
       COUNT(*) as vehicle_make_count
FROM vehicles
	 WHERE vehicle_make LIKE('%NOVA%')
GROUP BY vehicle_make
ORDER BY vehicle_make_count DESC
;

/* Update table by using above query to set return values to 'NOVA' */
UPDATE vehicles
SET vehicle_make = 'NOVA'
	   WHERE vehicle_make LIKE('%NOVA%')
;

/* Find filtered values of vehicle makes containing 'UNK'*/
SELECT vehicle_make, 
       COUNT(*) as vehicle_make_count
FROM vehicles
	 WHERE vehicle_make LIKE('%UNK%')
GROUP BY vehicle_make
ORDER BY vehicle_make_count DESC
;

/* Update table by using above query to set return values to 'NOVA' */
UPDATE vehicles
SET vehicle_make = 'UNKNOWN'
	   WHERE vehicle_make LIKE('%UNK%')
;

/* Find filtered values of vehicle makes containing 'BlUE BIRD'*/
SELECT vehicle_make, 
       COUNT(*) as vehicle_make_count
FROM vehicles
	 WHERE vehicle_make LIKE('%BLUE BIRD%')
	 OR vehicle_make LIKE('%BLUEB%')
	 OR vehicle_make LIKE ('%BLUI%')
	 AND vehicle_make NOT LIKE ('%NEW FLYER%')
GROUP BY vehicle_make
ORDER BY vehicle_make_count DESC
;

/* Update table by using above query to set return values to 'BLUEBIRD' */
UPDATE vehicles
SET vehicle_make = 'BLUEBIRD'
	   WHERE vehicle_make LIKE('%BLUE BIRD%')
	   OR vehicle_make LIKE('%BLUEB%')
	   OR vehicle_make LIKE ('%BLUI%')
	   AND vehicle_make NOT LIKE ('%NEW FLYER%')
;

/* Find filtered values of vehicle makes containing 'MCIN'*/
SELECT vehicle_make, 
       COUNT(*) as vehicle_make_count
FROM vehicles
	 WHERE vehicle_make LIKE('%MCIN%')
GROUP BY vehicle_make
ORDER BY vehicle_make_count DESC
;

/* Update table by using above query to set return values to 'MOTORCOACH IND' */
UPDATE vehicles
SET vehicle_make = 'MOTORCOACH IND'
	   WHERE vehicle_make LIKE('%MCIN%')
;

/* Find filtered values of vehicle makes containing 'RAMB'*/
SELECT vehicle_make, 
       COUNT(*) as vehicle_make_count
FROM vehicles
	 WHERE vehicle_make LIKE('%RAMB%')
GROUP BY vehicle_make
ORDER BY vehicle_make_count DESC
;

/* Update table by using above query to set return values to 'RAMBLER' */
UPDATE vehicles
SET vehicle_make = 'RAMBLER'
	   WHERE vehicle_make LIKE('%RAMB%')
;

/* Find filtered values of vehicle makes containing 'MASE'*/
SELECT vehicle_make, 
       COUNT(*) as vehicle_make_count
FROM vehicles
	 WHERE vehicle_make LIKE('%MASE%')
GROUP BY vehicle_make
ORDER BY vehicle_make_count DESC
;

/* Update table by using above query to set return values to 'MASERATI' */
UPDATE vehicles
SET vehicle_make = 'MASERATI'
	   WHERE vehicle_make LIKE('%MASE%')
;

/* Find filtered values of vehicle makes containing 'SAA'*/
SELECT vehicle_make, 
       COUNT(*) as vehicle_make_count
FROM vehicles
	 WHERE vehicle_make LIKE('%SAA%')
GROUP BY vehicle_make
ORDER BY vehicle_make_count DESC
;

/* Update table by using above query to set return values to 'SAAB' */
UPDATE vehicles
SET vehicle_make = 'SAAB'
	   WHERE vehicle_make LIKE('%SAA%')
;

/* Find filtered values of vehicle makes containing 'STARC'*/
SELECT vehicle_make, 
       COUNT(*) as vehicle_make_count
FROM vehicles
	 WHERE vehicle_make LIKE('%STARC%')
GROUP BY vehicle_make
ORDER BY vehicle_make_count DESC
;

/* Update table by using above query to set return values to 'STARCRAFT' */
UPDATE vehicles
SET vehicle_make = 'STARCRAFT'
	   WHERE vehicle_make LIKE('%STARC%')
;

/* Find filtered values of vehicle makes containing 'HUMM'*/
SELECT vehicle_make, 
       COUNT(*) as vehicle_make_count
FROM vehicles
	 WHERE vehicle_make LIKE('%HUMM%')
GROUP BY vehicle_make
ORDER BY vehicle_make_count DESC
;

/* Update table by using above query to set return values to 'HUMMER' */
UPDATE vehicles
SET vehicle_make = 'HUMMER'
	   WHERE vehicle_make LIKE('%HUMM%')
;

/* Find filtered values of vehicle makes containing 'SCIO'*/
SELECT vehicle_make, 
       COUNT(*) as vehicle_make_count
FROM vehicles
	 WHERE vehicle_make LIKE('%SCIO%')
GROUP BY vehicle_make
ORDER BY vehicle_make_count DESC
;

/* Update table by using above query to set return values to 'SCION' */
UPDATE vehicles
SET vehicle_make = 'SCION'
	   WHERE vehicle_make LIKE('%SCIO%')
;

/* Find filtered values of vehicle makes containing 'ORION'*/
SELECT vehicle_make, 
       COUNT(*) as vehicle_make_count
FROM vehicles
	 WHERE vehicle_make LIKE('%ORION%')
GROUP BY vehicle_make
ORDER BY vehicle_make_count DESC
;

/* Update table by using above query to set return values to 'ORION' */
UPDATE vehicles
SET vehicle_make = 'ORION'
	   WHERE vehicle_make LIKE('%ORION%')
;

/* Find filtered values of vehicle makes containing 'NEW FLYER'*/
SELECT vehicle_make, 
       COUNT(*) as vehicle_make_count
FROM vehicles
	 WHERE vehicle_make LIKE('%NEW FLYER%')
GROUP BY vehicle_make
ORDER BY vehicle_make_count DESC
;

/* Update table by using above query to set return values to 'NEW FLYER' */
UPDATE vehicles
SET vehicle_make = 'NEW FLYER'
	   WHERE vehicle_make LIKE('%NEW FLYER%')
	   OR vehicle_make = 'NEWFL'
;

/* Find filtered values of vehicle makes containing 'THMS'*/
SELECT vehicle_make, 
       COUNT(*) as vehicle_make_count
FROM vehicles
	 WHERE vehicle_make LIKE('%THMS%')
	 OR vehicle_make LIKE('%THOM%')
GROUP BY vehicle_make
ORDER BY vehicle_make_count DESC
;

/* Update table by using above query to set return values to 'NEW FLYER' */
UPDATE vehicles
SET vehicle_make = 'THOMAS'
	   WHERE vehicle_make LIKE('%THMS%')
	   OR vehicle_make LIKE('%THOM%')
;

/* Find filtered values of vehicle makes containing 'OLDS'*/
SELECT vehicle_make, 
       COUNT(*) as vehicle_make_count
FROM vehicles
	 WHERE vehicle_make LIKE('%OLDS%')
	 AND vehicle_make NOT LIKE('ALEXANDER%')
GROUP BY vehicle_make
ORDER BY vehicle_make_count DESC
;

/* Update table by using above query to set return values to 'OLDSMOBILE' */
UPDATE vehicles
SET vehicle_make = 'OLDSMOBILE'
	  WHERE vehicle_make LIKE('%OLDS%')
	  AND vehicle_make NOT LIKE('ALEXANDER%')
;

/* Find filtered values of vehicle makes containing 'PREVO'*/
SELECT vehicle_make, 
       COUNT(*) as vehicle_make_count
FROM vehicles
	 WHERE vehicle_make LIKE('%PREVO%')
GROUP BY vehicle_make
ORDER BY vehicle_make_count DESC
;

/* Update table by using above query to set return values to 'PREVO' */
UPDATE vehicles
SET vehicle_make = 'PREVOST'
	  WHERE vehicle_make LIKE('%PREVO%')
;

/* Find filtered values of vehicle makes containing 'WESTERN STAR'*/
SELECT vehicle_make, 
       COUNT(*) as vehicle_make_count
FROM vehicles
	 WHERE vehicle_make LIKE('%WSTR%')
	 OR vehicle_make LIKE('%WESTER%')
GROUP BY vehicle_make
ORDER BY vehicle_make_count DESC
;

/* Update table by using above query to set return values to 'WESTERN STAR'*/
UPDATE vehicles
SET vehicle_make = 'WESTERN STAR'
	  WHERE vehicle_make LIKE('%WSTR%')
	  OR vehicle_make LIKE('%WESTER%')
;

/* Next step update vehicle_make to OTHER when total count is less than 641 */
UPDATE vehicles
SET vehicle_make = 'OTHER'
WHERE vehicle_make IN (
    SELECT vehicle_make
    FROM (
        SELECT vehicle_make, COUNT(*) AS count_type
        FROM vehicles
        GROUP BY vehicle_make
    ) AS counts
    WHERE counts.count_type <= 641
);

/* Final tally of grouped vehicle_make. */
SELECT DISTINCT(vehicle_make) as vehicle_make, 
       COUNT(*) as vehicle_make_count
FROM vehicles
GROUP BY vehicle_make
ORDER BY vehicle_make_count DESC
;


/* Conducted an inspection of the vehicle_model by grouping distinct vehicle_model by count. After 
inspection it was found most models are missing and the ones inputed are too indistinguible to be 
used in analysis.*/
SELECT DISTINCT(vehicle_model) as vehicle_model, 
       COUNT(*) as vehicle_model_count
FROM vehicles
GROUP BY vehicle_model
ORDER BY vehicle_model_count DESC
;

/* The vehicle_year was group by count to inspect entries, many 
entries found are obviously incorrect as they are years much later 
than 2022/2023. */
SELECT DISTINCT(vehicle_year), 
       COUNT(*) AS vehicle_year_count
FROM vehicles
GROUP BY vehicle_year
ORDER BY vehicle_year;

/* Alters column vehicles.vehicle_year to integer in order to make corrections to the inputs easier.*/
ALTER TABLE IF EXISTS vehicles
	ALTER COLUMN vehicle_year 
	TYPE INTEGER
		USING(vehicle_year::INTEGER)
;

/* Updates vehicle_year to NULL where vehicle years are between 2023 and 20064.*/
UPDATE vehicles
SET vehicle_year = NULL
WHERE vehicle_year 
      BETWEEN 2024 AND 20064;

/* Updates vehicle_year to NULL where vehicle years are between 1000 and 1900.*/
UPDATE vehicles
SET vehicle_year = NULL
WHERE vehicle_year 
      BETWEEN 1000 AND 1900;
	  
/* Reformats the vehicle_year column back to text format.*/
ALTER TABLE IF EXISTS vehicles
	ALTER COLUMN vehicle_year 
	TYPE TEXT
		USING(vehicle_year::TEXT)
;

/* Groups counts of distinct travel_direction to inspect the column. Minor corrections will be needed to
to column to ensure uniformity of data. */ 
SELECT DISTINCT(travel_direction), 
       COUNT(*) AS travel_direction_count
FROM vehicles
GROUP BY travel_direction
ORDER BY travel_direction;

/* Uppercases all text within the column */
UPDATE vehicles
SET travel_direction = UPPER(travel_direction)
;

/* Updates data to read 'EAST' when input as 'E' */
UPDATE vehicles
SET travel_direction = 'EAST'
	WHERE travel_direction = 'E'
;

/* Updates data to read 'NORTH' when input as 'N' */
UPDATE vehicles
SET travel_direction = 'NORTH'
	WHERE travel_direction = 'N'
;

/* Updates data to read 'WEST' when input as 'W' */
UPDATE vehicles
SET travel_direction = 'WEST'
	WHERE travel_direction = 'W'
;

/* Updates data to read 'SOUTH' when input as 'S' */
UPDATE vehicles
SET travel_direction = 'SOUTH'
	WHERE travel_direction = 'S'
;

/* Updates data to read 'UNKNOWN' when input as 'U' */
UPDATE vehicles
SET travel_direction = 'UNKNOWN'
	WHERE travel_direction = 'U'
;

/* Updates data to NULL when input as '-' */
UPDATE vehicles
SET travel_direction = NULL
	WHERE travel_direction = '-'
;

/* Final check of data in travel_direction column */
SELECT DISTINCT(travel_direction), 
       COUNT(*) AS travel_direction_count
FROM vehicles
GROUP BY travel_direction
ORDER BY travel_direction;

/* Conduct an inital inspection of driver_sex values. No inconsistent inputs found.  */
SELECT DISTINCT(driver_sex), 
       COUNT(*) AS sex_count
FROM vehicles
GROUP BY driver_sex
ORDER BY sex_count;

/* Conduct an inital inspection of driver_license_status values. */
SELECT DISTINCT(driver_license_status), 
       COUNT(*) AS license_count
FROM vehicles
GROUP BY driver_license_status
ORDER BY license_count;

/* Trims and uppercases all values within the driver_license_status column */
UPDATE vehicles
	SET driver_license_status = TRIM(UPPER(driver_license_status))
;
/* Conduct an inital inspection of driver_license_jurisdiction values. */
SELECT DISTINCT(driver_license_jurisdiction), 
COUNT(*) AS license_count
FROM vehicles
GROUP BY driver_license_jurisdiction
ORDER BY license_count
;

/* Selects the rows with data inputed with errors present.*/
SELECT driver_license_jurisdiction, 
       COUNT(*) as license_count
FROM vehicles
	 WHERE driver_license_jurisdiction LIKE('__''')
	 		OR driver_license_jurisdiction LIKE('%,%')
GROUP BY driver_license_jurisdiction
ORDER BY license_count DESC
;

/* Sets driver_license_jurisdiction to PA to remove input error. */
UPDATE vehicles
	SET driver_license_jurisdiction = 'PA'
		WHERE driver_license_jurisdiction LIKE('__''')
;

/* Sets driver_license_jurisdiction to NULL to remove input as there was only one. */
UPDATE vehicles
	SET driver_license_jurisdiction = NULL
		WHERE driver_license_jurisdiction LIKE('%,%')
;

/* Conduct an inital inspection of pre_crash values. All values seem  to be standardized */
SELECT DISTINCT(pre_crash), 
       COUNT(*) AS pre_crash_count
FROM vehicles
GROUP BY pre_crash
ORDER BY pre_crash_count
;

/* Trims and uppercases all values within the pre_crash column */
UPDATE vehicles
	SET pre_crash = TRIM(UPPER(pre_crash))
;

/* Selects rows with values 'OTHER*' */
SELECT pre_crash,
	   COUNT(*)
FROM vehicles
WHERE pre_crash = 'OTHER*'
GROUP BY pre_crash
;

/* Sets values to 'OTHER' to remove astrisk from inputed 'OTHER*' values */
UPDATE vehicles
	SET pre_crash = 'OTHER'
		WHERE pre_crash = 'OTHER*'
;

/* Conduct an inital inspection of point_of_impact values. All values seem to be standardized*/
SELECT DISTINCT(point_of_impact), 
       COUNT(*) AS impact_count
FROM vehicles
GROUP BY point_of_impact
ORDER BY impact_count;

/* Trims and uppercases all values within the point_of_impact column */
UPDATE vehicles
	SET point_of_impact = TRIM(UPPER(point_of_impact))