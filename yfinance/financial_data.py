def main_extraction():
    import os
    import pandas as pd
    import yfinance as yf
    import snowflake.connector
    from datetime import datetime
    from dotenv import load_dotenv

    load_dotenv()

    extract_at = datetime.now()

    # Config snowflake connection
    sf_account = os.getenv('ENV_SF_ACCOUNT')
    sf_user = os.getenv('ENV_SF_USER')
    sf_password = os.getenv('ENV_SF_PASSWORD')
    sf_warehouse = os.getenv('ENV_SF_WAREHOUSE')
    sf_database = os.getenv('ENV_SF_DATABASE')
    sf_schema = os.getenv('ENV_SF_SCHEMA')


    # List of stock symbols to be consulted
    symbols = ['IR', 'JCI', 'EMR', 'G1A.F', 'BAC', 'DKILY', 'TT', 'CARR']

    for symbol in symbols:
        # Create a stock data object. Extract
        stock = yf.Ticker(symbol)
        
        # Obtain current quote information
        stock_info = stock.info

        # Filter the desired keys. Transform
        filtered_data = {key: stock_info.get(key, None) for key in ['shortName', 'longName', 'symbol', 'website', 'longBusinessSummary',
                                                                      'industry', 'industryKey', 'industryDisp', 'sector', 'sectorKey',
                                                                      'sectorDisp', 'fullTimeEmployees', 'regularMarketPrice', 'regularMarketTime',
                                                                      'regularMarketChangePercent', 'ebitda', 'totalRevenue'
                                                                      ]}

        data = pd.DataFrame([filtered_data])
        data['extract_at'] = extract_at

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

        print(f"Inserting data for symbol {symbol}")

        # Insert data from each row of the dataframe. Load.
        insert_sql = f'''INSERT INTO PHYGITAL_PROD.BRONZE_LAYER.STOCK_MARKET_VALUES (
                            SHORTNAME,
                            LONGNAME,
                            SYMBOL,
                            WEBSITE,
                            LONGBUSINESSSUMMARY,
                            INDUSTRY,
                            INDUSTRYKEY,
                            INDUSTRYDISP,
                            SECTOR,
                            SECTORKEY,
                            SECTORDISP,
                            FULLTIMEEMPLOYEES,
                            REGULARMARKETPRICE,
                            REGULARMARKETTIME,
                            REGULARMARKETCHANGEPERCENT,
                            EBITDA,
                            TOTALREVENUE,
                            LOADED_AT
                        ) 
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                      '''
        for index, row in data.iterrows():
            cursor.execute(insert_sql, (row['shortName'],
                                        row['longName'],
                                        row['symbol'],
                                        row['website'],
                                        row['longBusinessSummary'],
                                        row['industry'],
                                        row['industryKey'],
                                        row['industryDisp'],
                                        row['sector'],
                                        row['sectorKey'],
                                        row['sectorDisp'],
                                        row['fullTimeEmployees'],
                                        row['regularMarketPrice'],
                                        row['regularMarketTime'],
                                        row['regularMarketChangePercent'],
                                        row['ebitda'],
                                        row['totalRevenue'],
                                        row['extract_at'].isoformat()
                                        ))

        # Confirm and close transaction
        conn.commit()
        cursor.close()
        conn.close()

        print(f"Data successfully inserted for symbol {symbol}")


if __name__ == '__main__':
    main_extraction()