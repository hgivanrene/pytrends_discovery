def trending_words():
    import os
    import pandas as pd
    from datetime import datetime
    import time
    import snowflake.connector
    from pytrends.request import TrendReq
    from pytrends.exceptions import TooManyRequestsError, ResponseError
    from dotenv import load_dotenv

    load_dotenv()

    # Config snowflake connection
    sf_account = os.getenv('ENV_SF_ACCOUNT')
    sf_user = os.getenv('ENV_SF_USER')
    sf_password = os.getenv('ENV_SF_PASSWORD')
    sf_warehouse = os.getenv('ENV_SF_WAREHOUSE')
    sf_database = os.getenv('ENV_SF_DATABASE')
    sf_schema = os.getenv('ENV_SF_SCHEMA')

    # Waiting time
    wait_time = 600

    # List of countries and its geo codes
    countries = {
        "Argentina": "AR",
        "Belice": "BZ",
        "Bahamas": "BS",
        "Colombia": "CO",
        "Brasil": "BR",
        "Chile": "CL",
        "Ecuador": "EC",
        "El Salvador": "SV",
        "Honduras": "HN",
        "Jamaica": "JM",
        "México": "MX",
        "Guatemala": "GT",
        "Nicaragua": "NI",
        "República Dominicana": "DO",
        "Venezuela": "VE",
        "Perú": "PE",
        "Costa Rica": "CR",
        "Panamá": "PA"
    }

    # List of key words to be consult
    data = pd.read_csv('pytrends/main_words.csv')
    keywords_list = data['KEY_WORDS'].tolist()
    
    ##################################################################### EXTRACT SECTION ################################################################

    for kw_list in keywords_list:
        for country, geo_code in countries.items():
            try:
                # configure pytrends
                pytrends = TrendReq(hl='en-US', tz=0, timeout=(30,30))
                pytrends.build_payload([kw_list], timeframe='today 1-m', geo=geo_code)
                data = pytrends.interest_over_time()

    ##################################################################### TRANSFORM SECTION ################################################################
                if data.empty:
                    print(f"No data available for the keyword: {kw_list} in {country}")
                else:
                    if 'isPartial' in data.columns:
                        data = data.drop(columns=['isPartial'])

                    extract_at = datetime.now()
                    data['key_word'] = kw_list
                    data['country'] = country # Add column for country
                    data['loaded_at'] = extract_at # Add column for date of the data extraction
                    data = data.rename(columns={kw_list: 'value'}) # Rename key word column to value
                    data_transformed = data.reset_index()[['date', 'key_word', 'country', 'value', 'loaded_at']]
            

    ##################################################################### LOAD SECTION #####################################################################

                    # Create snowflake connection
                    conn = snowflake.connector.connect(
                        user=sf_user,
                        password=sf_password,
                        account=sf_account,
                        warehouse=sf_warehouse,
                        database=sf_database,
                        schema=sf_schema
                    )

                    # open transaction
                    cursor = conn.cursor()

                    print(f"Inserting data for key word {kw_list} in {country}")

                    # Insert data from each row of the dataframe. Load.
                    insert_sql = f'''INSERT INTO {sf_database}.{sf_schema}.GOOGLE_TRENDS (
                                        TREND_AT,
                                        KEYWORD,
                                        VALUE,
                                        COUNTRY,
                                        LOADED_AT
                                    ) 
                                    VALUES (%s, %s, %s, %s, %s)
                                '''
                    for index, row in data_transformed.iterrows():
                        cursor.execute(insert_sql, (row['date'].isoformat(),
                                                    row['key_word'],
                                                    row['value'],
                                                    row['country'],
                                                    row['loaded_at'].isoformat()
                                                    ))

                    # Confirm and close transaction
                    conn.commit()
                    cursor.close()
                    conn.close()

                    print(f"Data successfully inserted for key word {kw_list} in {country}")

            except (TooManyRequestsError, ResponseError) as e:
                print(f"Received {type(e).__name__} for {kw_list} in {country}. Waiting for {wait_time} seconds.")
                time.sleep(wait_time)


if __name__ == '__main__':
    trending_words()