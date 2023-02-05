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


SELECT DISTINCT(on_street_name)
FROM crashes
WHERE on_street_name LIKE '[%0123456789]';
