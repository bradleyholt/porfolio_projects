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
ADD COLUMN crash_timestamp TIME;


/* NOT WORKING */
ALTER TABLE IF EXISTS crashes
SET crash_timestamp = (crash_date || crash_time)::TIME;
