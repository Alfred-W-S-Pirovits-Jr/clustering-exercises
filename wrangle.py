import env
import acquire

def remove_columns(df, cols_to_remove):  
    df = df.drop(columns=cols_to_remove)
    return df

def handle_missing_values(df, prop_required_column = .5, prop_required_row = .5):
    threshold = int(round(prop_required_column*len(df.index),0))
    df.dropna(axis=1, thresh=threshold, inplace=True)
    threshold = int(round(prop_required_row*len(df.columns),0))
    df.dropna(axis=0, thresh=threshold, inplace=True)
    return df


def data_prep(df, cols_to_remove=[], prop_required_column=.5, prop_required_row=.5):
    df = remove_columns(df, cols_to_remove)
    df = handle_missing_values(df, prop_required_column, prop_required_row)
    return df

def wrangle_zillow(zillow):
    
    #acquire the data
    zillow = acquire.get_zillow_data()

    #Lists for single and multiple residences
    single = ['Single Family Residential', 'Condominium', 'Residential General', 'Manufactured, Modular', 
          'Prefabricated Homes', 'Mobile Home', 'Townhouse']

    multiple = ['Duplex (2 Units, Any Combination)', 
       'Planned Unit Development', 'Triplex (3 Units, Any Combination)',
       'Quadruplex (4 Units, Any Combination)', 'Cluster Home',
       'Commercial/Office/Residential Mixed Used', 'Cooperative']
    
    #Remove all multiple family properties by masking

    zillow = zillow[zillow.propertylandusedesc.isin(single) == True]

    #remove columns with too many nulls/redundant information
    zillow = data_prep(zillow, cols_to_remove=['parcelid.1', 'id.1', 'heatingorsystemtypeid', 'buildingqualitytypeid', 'propertycountylandusecode', 'propertyzoningdesc', 'propertylandusedesc', 'unitcnt', 'heatingorsystemdesc', 'calculatedbathnbr', 'fullbathcnt'], prop_required_column=.5, prop_required_row=.5)

    #remove all na values I deemed it not necessary to impute in order to keep clusterizations clean
    zillow = zillow.dropna()
    
    return zillow