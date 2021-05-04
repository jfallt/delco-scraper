insert_residential_details = """INSERT INTO stg_parcel_residential_details(
    parcel_id,
    card,
    class,
    grade,
    cdu,
    style,
    acres,
    year_built_effective_year,
    remodeled_year,
    base_area,
    finished_bsmt_area,
    number_of_stories,
    exterior_wall,
    basement,
    physical_condition,
    heating,
    heat_fuel_type,
    heating_system,
    attic_code,
    fireplaces,
    parking,
    total_rooms,
    full_baths,
    half_baths,
    total_fixtures,
    additional_fixtures,
    bed_rooms,
    family_room,
    living_units) 
    VALUES (
            '{parcel_id}',
            '{card}',
            '{class_input}',
            '{grade}',
            '{cdu}',
            '{style}',
            '{acres}',
            '{year_built_effective_year}',
            '{remodeled_year}',
            '{base_area}',
            '{finished_bsmt_area}',
            '{number_of_stories}',
            '{exterior_wall}',
            '{basement}',
            '{physical_condition}',
            '{heating}',
            '{heat_fuel_type}',
            '{heating_system}',
            '{attic_code}',
            '{fireplaces}',
            '{parking}',
            '{total_rooms}',
            '{full_baths}',
            '{half_baths}',
            '{total_fixtures}',
            '{additional_fixtures}',
            '{bed_rooms}',
            '{family_room}',
            '{living_units}'
            )"""


insert_parcel_site_details = """INSERT INTO stg_parcel_site_details(
    parcel_id,
    site_location,
    legal_description,
    municipality,
    school_district,
    property_type) 
    VALUES (
            '{parcel_id}',
            '{site_location}',
            '{legal_description}',
            '{municipality}',
            '{school_district}',
            '{property_type}'
            )"""

get_missing_data = {
    'residential_details': """SELECT P.PARCEL_ID
    FROM DIM_PARCEL P
    LEFT JOIN dim_parcel_residential_details FPD ON FPD.ID = P.ID
    GROUP BY P.PARCEL_ID
    HAVING COUNT(FPD.ID) = 0
    ORDER BY P.PARCEL_ID DESC""",
    'parcel_site_details': """call public.sp_update_dim_parcel_all();
    truncate stg_parcel_site_details;
    SELECT P.PARCEL_ID
    FROM DIM_PARCEL P
	LEFT JOIN (select distinct parcel_id from stg_parcel_site_details) sd
	on sd.parcel_id = p.parcel_id
    WHERE municipality is null
	and sd.parcel_id is null
    ORDER BY P.PARCEL_ID DESC;"""
}
