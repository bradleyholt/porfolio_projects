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


/* Selects all columns in crashes table limited to 10 to begin data cleaning process */
SELECT * FROM crashes
LIMIT 100;


/* Confirming uniqueness of column crashes.collision_id(TEXT) by subtracting total rows from the 
count of unique collision_id; Result: 0 */
SELECT COUNT(DISTINCT(collision_id))-COUNT(*)
FROM crashes;


/* Alters column crashes.collision_id to integer to ensure proper usage as a primary key */
ALTER TABLE IF EXISTS crashes
	ALTER COLUMN collision_id 
	TYPE INTEGER
		USING(collision_id::INTEGER);
		

/* Adds a primary key constraint to crashes.collision_id column to maintain data integrity */
ALTER TABLE IF EXISTS crashes
	ADD CONSTRAINT crashes_pk 
	PRIMARY KEY (collision_id);
	

/* Alters column persons.collision_id to integer to ensure proper usage as a foreign key */
ALTER TABLE IF EXISTS persons
	ALTER COLUMN collision_id 
	TYPE INTEGER
		USING(collision_id::INTEGER);
		
/* Adds a foreign key constraint to persons.collision_id column which references the primary key 
in crashes table */
ALTER TABLE IF EXISTS persons
	ADD CONSTRAINT persons_fk 
	FOREIGN KEY (collision_id)
	REFERENCES crashes(collision_id);
	
	
/* Alters column vehicles.collision_id to integer to ensure proper usage as a foreign key */
ALTER TABLE IF EXISTS vehicles
	ALTER COLUMN collision_id 
	TYPE INTEGER
		USING(collision_id::INTEGER);
		

/* Adds a foreign key constraint to vehicles.collision_id column which references the primary key 
in crashes table */
ALTER TABLE IF EXISTS vehicles
	ADD CONSTRAINT vehicles_fk 
	FOREIGN KEY (collision_id)
	REFERENCES crashes(collision_id);
	
	
/* Add column 'new_date' to the table to prepare to transform crash_date from text format. */
ALTER TABLE IF EXISTS crashes
	ADD COLUMN new_date DATE;


/* Checks count of total rows in crashes table compare to count of crash_date column to check
for NULL values Result: total_count = 1964130, count = 1964130 */
SELECT COUNT(*) as total_count,
	   (SELECT COUNT(crash_date)
	    FROM crashes)
FROM crashes;


/* Updates column 'new_date' with values from crash_date column converted to date format */
UPDATE crashes
	SET new_date = crash_date :: DATE;


/* Selects both crash_date and new_date columns to visually verify that correct data 
transformation occured */
SELECT crash_date, new_date
FROM crashes
LIMIT 100;

/* Adds column 'new_date' to the table */	
ALTER TABLE IF EXISTS crashes
	ADD COLUMN new_time TIME;


/* Checks count of total rows in crashes table compare to count of crash_time column to check
for NULL values; Result: total_count = 1964130, count = 1964130 */
SELECT COUNT(*) as total_count,
	   (SELECT COUNT(crash_time)
	    FROM crashes)
FROM crashes;


/* Update column 'new_time' with values from crash_date column converted to date format */
UPDATE crashes
	SET new_time = TO_TIMESTAMP(crash_time, 'HH24:MI')::TIME;


/* Selects both crash_time and new_time columns to visually verify that correct data 
transformation occured */
SELECT crash_time, new_time
FROM crashes
LIMIT 100;


/* Permenantly removes crash_date from crashes table */
ALTER TABLE crashes
	DROP COLUMN crash_date;
	

/* Permenantly removes crash_time from crashes table */
ALTER TABLE crashes
	DROP COLUMN crash_time;
	

/* Renames new_date column to crash_date */
ALTER TABLE crashes
	RENAME COLUMN new_date to crash_date;
	

/* Renames new_date column to crash_date */
ALTER TABLE crashes
	RENAME COLUMN new_time to crash_time;
	

/* Adds new column crash_timestamp to crashes table */
ALTER TABLE IF EXISTS crashes
	ADD COLUMN crash_timestamp TIMESTAMP;


/* A future improvement to be noted is that the original 'crash_date' and 'crash_time'
columns in text format can be converted the same way without the need to convert to date and time 
datatypes first. Unless requirements include seperate date and time columns, this will be more 
efficient. */

/* Concatenates 'crash_date' and 'crash_time' columns into new 'crash_timestamp' column and converts
data type to TIMESTAMP */
UPDATE crashes
	SET crash_timestamp = CONCAT(crash_date, ' ', crash_time)::TIMESTAMP;
	

/* Confirming accurate data visually in new crash_timestamp column */
SELECT crash_date, crash_time, crash_timestamp
FROM crashes
LIMIT 50;


/* Permenantly removes crash_date and crash_time from crashes table */
ALTER TABLE crashes
	DROP COLUMN crash_date, 
	DROP COLUMN crash_time;
	
	
/* Returns list of unique values in borough column */
SELECT DISTINCT(borough)
FROM crashes;

	
/* Returns the count of borough(NOT NULL), count of rows, and perect of borough column with data 
input. All data was cast as numeric to allow return of decimals and rounded to the 100ths.
Approximately 69% of the borough column had a value input.*/
SELECT COUNT(borough)::NUMERIC AS borough_count, 
	   COUNT(*)::NUMERIC AS total_count, 
	   ROUND(COUNT(borough) / COUNT(*)::NUMERIC *100, 2) AS pct_input  
FROM crashes;


/* Fills null values in 'borough' column with value 'Unspecified' to assist with future EDA.*/
UPDATE crashes
	SET borough = 
		COALESCE(borough, 'Unspecified');
		
/* After examining the zip_code column it was found that one or more values were empty strings.
This needed corrected in order to convert the column to a integer data type.*/

/* Trims whitespace from all values in the 'zip_code' column. */
UPDATE crashes
	   SET zip_code = TRIM(zip_code)

/* Sets empty string values to NULL in zip_code column.*/
UPDATE crashes
	   SET zip_code = NULL
		   WHERE zip_code=''
		   
/* Coverts zip_code column from text data to integer data as it is a more appropriate data type for
the feature.*/		   
ALTER TABLE IF EXISTS crashes
	ALTER COLUMN zip_code 
			     TYPE INTEGER
			      	  USING (zip_code::INTEGER);
	
/* Returns the count of zip_code(NOT NULL), count of total rows, and perect of zip_code column 
with data input. All data was cast as numeric to allow return of decimals and rounded to the 100ths.
Approximately 69% of the zip_code column had a value input*/
SELECT COUNT(zip_code::NUMERIC) AS zip_count, 
	   COUNT(*)::NUMERIC AS total_count, 
	   ROUND(COUNT(zip_code)::NUMERIC/ COUNT(*)::NUMERIC *100, 2) AS pct_input  
FROM crashes;

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
WHERE location IS NULL;


/* Selects count of crashes group by borough and on_street_name. Inital run of the query and examination of the results
showed duplication of many borough/on_street_name combinations most likely caused by whitespace. Updating the column by 
trimming whitespace corrected the issue.*/
SELECT borough, on_street_name, COUNT(on_street_name) as crash_count
FROM crashes
GROUP BY borough, on_street_name
ORDER BY crash_count DESC;

/* Trims whitespace in on_street_name.*/
UPDATE crashes
	SET on_street_name = TRIM(on_street_name);
	
/* Trims whitespace in cross_street_name.*/
UPDATE crashes	
	SET cross_street_name = TRIM(cross_street_name);
	
/* Trims whitespace in off_street_name.*/
UPDATE crashes	
	SET off_street_name = TRIM(off_street_name);


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
WHERE on_street_name IS NULL;

/* Returns rows where number_of_persons_injured is NULL. Investigation revealed 18 rows were NULL however had number of
injuries input in specified type of entity columns*/
SELECT *
from crashes
WHERE number_of_persons_injured IS NULL;

/* Sets number of injuries from specific entity columns where number_of_persons_injured is NULL */
UPDATE crashes
	   SET number_of_persons_injured = (CASE WHEN number_of_pedestrians_injured::NUMERIC > 0 THEN number_of_pedestrians_injured::NUMERIC
				 					  WHEN number_of_cyclist_injured::NUMERIC > 0 THEN number_of_cyclist_injured::NUMERIC
				 					  WHEN number_of_motorist_injured::NUMERIC > 0 THEN number_of_motorist_injured::NUMERIC
				 					  ELSE 0 END)
	   WHERE number_of_persons_injured IS NULL;	

/* Coverts number_of_persons_injured column from text data to integer data as it is a more appropriate data type for
the feature.*/		   
ALTER TABLE IF EXISTS crashes
	ALTER COLUMN number_of_persons_injured
			     TYPE INTEGER
			      	  USING (number_of_persons_injured::INTEGER);
					  
/* Returns rows where number_of_persons_injured is NULL. Investigation revealed 31 rows were NULL however had number of
deaths input in specified type of entity columns. */
SELECT *
from crashes
WHERE number_of_persons_killed IS NULL;

/* Sets number of deaths from specific entity columns where number_of_persons_killed is NULL */
UPDATE crashes
	   SET number_of_persons_killed = (CASE WHEN number_of_pedestrians_killed::NUMERIC > 0 THEN number_of_pedestrians_killed::NUMERIC
				 					  WHEN number_of_cyclist_killed::NUMERIC > 0 THEN number_of_cyclist_killed::NUMERIC
				 					  WHEN number_of_motorist_killed::NUMERIC > 0 THEN number_of_motorist_killed::NUMERIC
				 					  ELSE 0 END)
	   WHERE number_of_persons_killed IS NULL;
	  
/* Coverts number_of_persons_killed column from text data to integer data as it is a more appropriate data type for
the feature.*/		   
ALTER TABLE IF EXISTS crashes
	ALTER COLUMN number_of_persons_killed
			     TYPE INTEGER
			      	  USING (number_of_persons_killed::INTEGER);
					  
/* Coverts number_of_pedestrians_injured column from text data to integer data as it is a more appropriate data type for
the feature.*/		   
ALTER TABLE IF EXISTS crashes
	ALTER COLUMN number_of_pedestrians_injured
			     TYPE INTEGER
			      	  USING (number_of_pedestrians_injured::INTEGER);
					  
/* Coverts number_of_pedestrians_killed column from text data to integer data as it is a more appropriate data type for
the feature.*/		   
ALTER TABLE IF EXISTS crashes
	ALTER COLUMN number_of_pedestrians_killed
			     TYPE INTEGER
			      	  USING (number_of_pedestrians_killed::INTEGER);
					  
/* Coverts number_of_cyclist_injured column from text data to integer data as it is a more appropriate data type for
the feature.*/		   
ALTER TABLE IF EXISTS crashes
	ALTER COLUMN number_of_cyclist_injured
			     TYPE INTEGER
			      	  USING (number_of_cyclist_injured::INTEGER);
					  
/* Coverts number_of_cyclist_killed column from text data to integer data as it is a more appropriate data type for
the feature.*/		   
ALTER TABLE IF EXISTS crashes
	ALTER COLUMN number_of_cyclist_killed
			     TYPE INTEGER
			      	  USING (number_of_cyclist_killed::INTEGER);
					  
/* Coverts number_of_motorist_injured column from text data to integer data as it is a more appropriate data type for
the feature.*/		   
ALTER TABLE IF EXISTS crashes
	ALTER COLUMN number_of_motorist_injured
			     TYPE INTEGER
			      	  USING (number_of_motorist_injured::INTEGER);
					  
/* Coverts number_of_motorist_killed column from text data to integer data as it is a more appropriate data type for
the feature.*/		   
ALTER TABLE IF EXISTS crashes
	ALTER COLUMN number_of_motorist_killed
			     TYPE INTEGER
			      	  USING (number_of_motorist_killed::INTEGER);
					  
/* Selects distinct values in the contributing_factor_vehicle_1 column. Investigation revealed some values were mispelled 
or have slight variations of the same factor creating uneccessary additional factors. e.g: Illness spelled 'Illnes' 
The values 1 & 80 are also listed as factors and according to the New York State POLICE CRASH REPORT SUBMISSION INSTRUCTIONS
there is no code specified for these values, but there are less than ~120 entries with these values.*/

SELECT DISTINCT contributing_factor_vehicle_1
FROM crashes;

/* Corrects spelling of value 'Illnes' to 'Illness' */
UPDATE crashes
	   SET contributing_factor_vehicle_1 = 'Illness'
	   WHERE contributing_factor_vehicle_1 = 'Illnes';

/* Corrects capitalization variation of value 'Cell Phone(hand-held)' */
UPDATE crashes
	   SET contributing_factor_vehicle_1 = 'Cell Phone (hand-held)'
	   WHERE contributing_factor_vehicle_1 = 'Cell Phone (hand-Held)';
	   
/* According to NY State POLICE CRASH REPORT SUBMISSION INSTRUCTIONS All crashes must have at least one apparent 
contributing factor - human, vehicular and/or environmental. However, 6037 crashes have no contributing factors listed.*/
SELECT COUNT(*) 
FROM crashes
 	 WHERE contributing_factor_vehicle_1 IS NULL
	 AND contributing_factor_vehicle_2 IS NULL
	 AND contributing_factor_vehicle_3 IS NULL
	 AND contributing_factor_vehicle_4 IS NULL
	 AND contributing_factor_vehicle_5 IS NULL;

/* Selects distinct values from contributing_factor_vehicle_2. Investigation found the same results as in 
contibuting_factor_vehicle_1.*/
SELECT DISTINCT contributing_factor_vehicle_2
FROM crashes;
	 
/* Corrects spelling of value 'Illnes' to 'Illness' */
UPDATE crashes
	   SET contributing_factor_vehicle_2 = 'Illness'
	   WHERE contributing_factor_vehicle_2 = 'Illnes';

/* Corrects capitalization variation of value 'Cell Phone(hand-held)' */
UPDATE crashes
	   SET contributing_factor_vehicle_2 = 'Cell Phone (hand-held)'
	   WHERE contributing_factor_vehicle_2 = 'Cell Phone (hand-Held)';

/* Selects distinct values from contributing_factor_vehicle_3. Investigation found the same results as in 
contibuting_factor_vehicle_1.*/
SELECT DISTINCT contributing_factor_vehicle_3
FROM crashes;

/* Corrects spelling of value 'Illnes' to 'Illness' */
UPDATE crashes
	   SET contributing_factor_vehicle_3 = 'Illness'
	   WHERE contributing_factor_vehicle_3 = 'Illnes';

/* Corrects capitalization variation of value 'Cell Phone(hand-held)' */
UPDATE crashes
	   SET contributing_factor_vehicle_3 = 'Cell Phone (hand-held)'
	   WHERE contributing_factor_vehicle_3 = 'Cell Phone (hand-Held)';
	   
/* Selects distinct values from contributing_factor_vehicle_4. Investigation found the same results as in 
contibuting_factor_vehicle_1.*/
SELECT DISTINCT contributing_factor_vehicle_4
FROM crashes;

/* Corrects spelling of value 'Illnes' to 'Illness' */
UPDATE crashes
	   SET contributing_factor_vehicle_4 = 'Illness'
	   WHERE contributing_factor_vehicle_4 = 'Illnes';

/* Corrects capitalization variation of value 'Cell Phone(hand-held)' */
UPDATE crashes
	   SET contributing_factor_vehicle_4 = 'Cell Phone (hand-held)'
	   WHERE contributing_factor_vehicle_4 = 'Cell Phone (hand-Held)';
	   
/* Selects distinct values from contributing_factor_vehicle_5.*/
SELECT DISTINCT contributing_factor_vehicle_5
FROM crashes;

SELECT *
FROM vehicles
LIMIT 25;

/* Select and counts the number of each type of vehicle (trimmed and uppercased) recorded. */

SELECT TRIM(UPPER(vehicle_type_code_1)) as upper_vehicle_type_code, 
	   COUNT(vehicle_type_code_1) AS count_type	   
FROM crashes
GROUP BY upper_vehicle_type_code
ORDER BY count_type

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
       HAVING COUNT(vehicle_type_code_1) < 90);

SELECT SUM(count_type)
FROM crash_count;

/* Trims and capitalizes all characters in vehicle_type_code_1 column */
UPDATE crashes
	   SET vehicle_type_code_1 = TRIM(UPPER(vehicle_type_code_1));
	   
/* Next step update vehicle_type_code_1 to OTHER when total count is less than 90 */
UPDATE crashes 
		SET vehicle_type_code_1 = 'OTHER'
				WHERE (SELECT COUNT(vehicle_type_code_1)
       			FROM crashes
				GROUP BY vehicle_type_code_1
      			) < 90;
				
SELECT vehicle_type_code_1, COUNT(vehicle_type_code_1) AS count_type
FROM crashes
GROUP BY vehicle_type_code_1
HAVING COUNT(vehicle_type_code_1) < 90;
