-- Selects all columns in crashes table limited to 10 to begin data cleaning process
SELECT * FROM crashes
LIMIT 10;


-- Confirming uniqueness of column by subracting total rows from the count of unique collision_id
SELECT COUNT(DISTINCT(collision_id))-COUNT(*)
FROM crashes;


-- Altering column crashes.collision_id to integer to ensure proper usage as a primary key
ALTER TABLE IF EXISTS crashes
	ALTER COLUMN collision_id 
	TYPE INTEGER
		USING(collision_id::INTEGER);
		

-- Alter crashes.collision_id column to primary_key to maintain data integrity
ALTER TABLE IF EXISTS crashes
	ADD CONSTRAINT crashes_pk 
	PRIMARY KEY (collision_id);
	

-- Repeating altering column collision_id to integer to ensure proper usage as a foreign key persons and vehicles tables
ALTER TABLE IF EXISTS persons
	ALTER COLUMN collision_id 
	TYPE INTEGER
		USING(collision_id::INTEGER);
		
				
-- Alter persons.person_id column to primary_key to maintain data integrity
ALTER TABLE IF EXISTS crashes
	ADD CONSTRAINT crashes_pk 
	PRIMARY KEY (collision_id);		
	
	
-- Repeating altering column collision_id to integer to ensure proper usage as a foreign key persons and vehicle tables
ALTER TABLE IF EXISTS vehicles
	ALTER COLUMN collision_id 
	TYPE INTEGER
		USING(collision_id::INTEGER);
		

-- Alter vehicles.collision_id column to foriegn_key referencing primary key in crashes table
ALTER TABLE IF EXISTS vehicles
	ADD CONSTRAINT vehicles_fk 
	FOREIGN KEY (collision_id)
	REFERENCES crashes(collision_id);
	
	
SELECT *
FROM vehicles
LIMIT 10;
	
