import numpy as np
import pandas as pd
import os
from env import get_db_url


def get_zillow_data():
    filename = 'zillow.csv'
    
    if os.path.isfile(filename):
        return pd.read_csv(filename, index_col=[0])
    else:
        # read the SQL query into a dataframe
        zillow_db = pd.read_sql('''
                                SELECT *
                                FROM properties_2017
                                LEFT OUTER JOIN airconditioningtype USING (airconditioningtypeid)
                                LEFT OUTER JOIN architecturalstyletype USING (architecturalstyletypeid)
                                LEFT OUTER JOIN buildingclasstype USING (buildingclasstypeid)
                                LEFT OUTER JOIN typeconstructiontype USING (typeconstructiontypeid)
                                LEFT OUTER JOIN propertylandusetype USING (propertylandusetypeid)
                                LEFT OUTER JOIN heatingorsystemtype USING (heatingorsystemtypeid)
                                LEFT OUTER JOIN storytype USING (storytypeid)
                                LEFT OUTER JOIN unique_properties USING (parcelid)
                                JOIN predictions_2017 ON (properties_2017.parcelid = predictions_2017.parcelid)
                                WHERE latitude IS NOT NULL AND longitude IS NOT NULL;
                                ''', get_db_url('zillow'))
        
        # Write that dataframe to disk for later.  Called "caching" the data for later.
        zillow_db.to_csv(filename)
        
        return zillow_db
    