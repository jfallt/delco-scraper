import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import yaml

# Read creds
with open("creds.yaml", 'r') as stream:
    data_loaded = yaml.safe_load(stream)

database = data_loaded.get('database')
user = data_loaded.get('user')
password = data_loaded.get('password')
host = data_loaded.get('host')

def create_connection():
    return psycopg2.connect(
            user=user, password=password, host=host, database=database)

def execute_multiple_queries(commands):
    conn = None
    try:
        conn = create_connection()
        cur = conn.cursor()
        for command in commands:
            cur.execute(commands)
        cur.close()
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
    

def create_database():
    con = psycopg2.connect(user=user, password=password, host=host)
    con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cursor = con.cursor()
    name_Database = "delco_real_estate_sales"
    sqlCreateDatabase = "create database "+name_Database+";"
    cursor.execute(sqlCreateDatabase)

def create_tables():
    commands = (
        """
        CREATE TABLE sales_upload (
            parcel_id VARCHAR(255) NOT NULL,
            tax_map_id VARCHAR(255) NOT NULL,
            owner_name VARCHAR(255),
            property_address VARCHAR(255),
            sales_date DATE,
            sales_amount VARCHAR(255),
            land_use_description VARCHAR(255)
            )
        """,
        """
        CREATE TABLE dim_parcel (
            ID SERIAL PRIMARY KEY,
            PARCEL_ID VARCHAR(11),
            TAX_MAP_ID VARCHAR(14),
            PROPERTY_ADDRESS VARCHAR(255),
            LAND_USE_TYPE CHAR(1),
            LAND_USE_SUBTYPE VARCHAR(255),
            LAST_MODIFIED_DATE TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            UNIQUE (PARCEL_ID)
        )
        """,
        """
        CREATE TABLE fact_sales (
            ID INT,
            SALES_DATE DATE,
			SALES_AMOUNT MONEY,
            OWNER_NAME VARCHAR(255),
        UNIQUE (id, sales_date)
		CONSTRAINT fk_parcel_id
      	FOREIGN KEY(id) 
	  	REFERENCES dim_parcel(id)
	    )
        """,
        """
        CREATE TABLE sales_upload_amount_errors (
            parcel_id VARCHAR(255) NOT NULL,
            tax_map_id VARCHAR(255) NOT NULL,
            owner_name VARCHAR(255),
            property_address VARCHAR(255),
            sales_date DATE,
            sales_amount VARCHAR(255),
            land_use_description VARCHAR(255)
            )
        """,
        """
        CREATE TABLE fact_parcel_details (
            ID INT,
            card VARCHAR(255),
			class VARCHAR(255),
			grade VARCHAR(255),
			cdu VARCHAR(255),
			style VARCHAR(255),
			acres float,
			year_built_effective_year VARCHAR(255),
			remodeled_year VARCHAR(255), 
			base_area VARCHAR(255),
			finished_bsmt_area VARCHAR(255),
			number_of_stories int,
			exterior_wall VARCHAR(255),
			basement VARCHAR(255),
			physical_condition VARCHAR(255),
			heating VARCHAR(255),
			heat_fuel_type VARCHAR(255),
			attic_code VARCHAR(255),
			fireplaces VARCHAR(255),
			parking VARCHAR(255),
			total_rooms int,
			full_baths int,
			half_baths int,
			total_fixtures int,
			additional_fixtures int,
			bed_rooms int,
			family_room int,
			living_units int,
        	UNIQUE (id),
			CONSTRAINT fk_parcel_id
      		FOREIGN KEY(id)
	  		REFERENCES dim_parcel(id)
	    )
        """,
        """
        CREATE TABLE stg_parcel_details (
            parcel_id VARCHAR(255),
            card VARCHAR(255),
			class VARCHAR(255),
			grade VARCHAR(255),
			cdu VARCHAR(255),
			style VARCHAR(255),
			acres float,
			year_built_effective_year VARCHAR(255),
			remodeled_year VARCHAR(255), 
			base_area VARCHAR(255),
			finished_bsmt_area VARCHAR(255),
			number_of_stories int,
			exterior_wall VARCHAR(255),
			basement VARCHAR(255),
			physical_condition VARCHAR(255),
			heating VARCHAR(255),
			heat_fuel_type VARCHAR(255),
			attic_code VARCHAR(255),
			fireplaces VARCHAR(255),
			parking VARCHAR(255),
			total_rooms int,
			full_baths int,
			half_baths int,
			total_fixtures int,
			additional_fixtures int,
			bed_rooms int,
			family_room int,
			living_units int)
        """
        )

    execute_multiple_queries(commands)

def create_stored_procedures():
    commands = (
        """
        -- PROCEDURE: public.sp_populate_sales_upload_amount_errors()

        -- DROP PROCEDURE public.sp_populate_sales_upload_amount_errors();

        CREATE OR REPLACE PROCEDURE public.sp_populate_sales_upload_amount_errors(
            )
        LANGUAGE 'sql'
        AS $BODY$
        INSERT INTO sales_upload_amount_errors
        SELECT *
        FROM SALES_UPLOAD S
        WHERE SALES_AMOUNT LIKE '%...%'
        ON CONFLICT DO NOTHING
        $BODY$;
        """,
        """
        -- PROCEDURE: public.sp_populate_fact_sales()

        -- DROP PROCEDURE public.sp_populate_fact_sales();

        CREATE OR REPLACE PROCEDURE public.sp_populate_fact_sales(
            )
        LANGUAGE 'sql'
        AS $BODY$
        INSERT INTO fact_sales (ID, SALES_DATE, SALES_AMOUNT, OWNER_NAME)
        SELECT DISTINCT P.ID,
            S.SALES_DATE,
            CASE
                WHEN CAST(SALES_AMOUNT as int) = 1
                THEN NULL
                ELSE CAST(SALES_AMOUNT as money)
            END as SALES_AMOUNT,
            OWNER_NAME
        FROM SALES_UPLOAD S
        INNER JOIN DIM_PARCEL P ON P.PARCEL_ID = S.PARCEL_ID
        WHERE SALES_AMOUNT NOT LIKE '%...%'
        ON CONFLICT DO NOTHING
        $BODY$;
        """,
        """
        CREATE TABLE fact_sales (
            ID INT,
            SALES_DATE DATE,
			SALES_AMOUNT MONEY,
            OWNER_NAME VARCHAR(255),
        UNIQUE (id, sales_date)
		CONSTRAINT fk_parcel_id
      	FOREIGN KEY(id) 
	  	REFERENCES dim_parcel(id)
	    )
        """,
        """
        -- PROCEDURE: public.sp_populate_dim_parcel()

        -- DROP PROCEDURE public.sp_populate_dim_parcel();

        CREATE OR REPLACE PROCEDURE public.sp_populate_dim_parcel(
            )
        LANGUAGE 'sql'
        AS $BODY$
        INSERT INTO dim_parcel (PARCEL_ID, TAX_MAP_ID, PROPERTY_ADDRESS, LAND_USE_TYPE, LAND_USE_SUBTYPE)
        SELECT DISTINCT CAST(PARCEL_ID AS VARCHAR(11)) AS PARCEL_ID,
                    CAST(TAX_MAP_ID AS VARCHAR(14)) AS TAX_MAP_ID,
                    PROPERTY_ADDRESS,
                    CAST(SPLIT_PART(LAND_USE_DESCRIPTION,'-',1) AS CHAR(1)) AS LAND_USE_TYPE,
                    SPLIT_PART(LAND_USE_DESCRIPTION,'-', 2) AS LAND_USE_SUBTYPE
                FROM SALES_UPLOAD
        ON CONFLICT DO NOTHING
        $BODY$;
        """)

    execute_multiple_queries(commands)

if __name__ == '__main__':
    create_database()
    create_tables()
    create_stored_procedures()
