import psycopg2


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
        """)

    conn = None
    try:
        conn = psycopg2.connect(
            user='postgres', password='password', host='localhost', database='delco_real_estate_sales')
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


if __name__ == '__main__':
    create_tables()
