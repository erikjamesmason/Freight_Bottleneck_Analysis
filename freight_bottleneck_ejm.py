import pandas as pd
import numpy as np
import datetime
import time


# Utility class to read shapefiles
class ShapeData:
    def __init__(self):
        self.data = None

    def read_shapefile(self, shp_path):
        """
        Read a shapefile into a Pandas dataframe with a 'polyline' column holding
        the geometry information. This uses the pyshp package
        Credit: https://gist.github.com/aerispaha/f098916ac041c286ae92d037ba5c37ba
        """
        import shapefile

        # read file, parse out the records and shapes
        sf = shapefile.Reader(shp_path)
        fields = [x[0] for x in sf.fields][1:]
        records = sf.records()
        shps = [s.points for s in sf.shapes()]

        # write into a dataframe
        self.data = pd.DataFrame(columns=fields, data=records)
        self.data = self.data.assign(polyline=shps)


# Utility function to compute percentiles for pandas aggregation
def percentile(n):
    def percentile_(x):
        return np.percentile(x, n)
    percentile_.__name__ = 'percentile_%s' % n
    return percentile_


# Utility function to parse the timestamp of the NPMRDS data
def extract_vals(date_str):
    # ----NPMRDS----
    date, time = date_str.split(' ')[:2]
    time_tokens = time.split(':')
    hour = int(time_tokens[0])
    minute = int(time_tokens[1])
    [year, month, day] = [int(val) for val in date.split('-')]
    day_type = datetime.datetime(year, month, day).weekday()
    ap = (hour * (60 // 5)) + minute // 5
    # ----End NPMRDS----
    return date, year, month, day, ap, day_type


# Utility function to append the parsed timestamps as columns to the dataframe
def create_columns(data, is_case_study=False):
    if is_case_study is False:
        dates, years, months, days, aps, weekday = zip(*data)
        return list(dates), list(years), list(months), list(days), list(aps), list(weekday)
    else:
        dates, years, months, days, aps, regions, weekday = zip(*data)
        return list(dates), list(years), list(months), list(days), list(aps), list(regions), list(weekday)


# Paths to shapefile and data
path_to_shapefile = r'C:\Users\ejmason\JupyterNotebooks\NPMRDS\Alaska_NPMRDS_2019_shapefile\Alaska.shp'
path_to_data = r'C:\Users\ejmason\JupyterNotebooks\NPMRDS\NPMRDS_AK_2019_TrucksOnly_Minutes'

# Read shapefile
sd = ShapeData()
sd.read_shapefile(path_to_shapefile)

# Time start of analysis run
otime1 = time.time()

# Load data and TMC files
full_df = pd.read_csv(path_to_data + r'\NPMRDS_AK_2019_TrucksOnly_Minutes.csv')
tmc = pd.read_csv(path_to_data + r'\TMC_Identification.csv')
# Create output file
f_out = open(path_to_data + 'EJM_TMC_2019_TrucksOnly.csv', 'w')

# Prepping output file (writing header)
f_out.write('tmc' + ',')
f_out.write('trans_delay' + ',')
f_out.write('unreliability' + ',')
f_out.write('pct_yr_avail' + ',')
f_out.write('length' + ',')
f_out.write('traffic_singl' + ',')
f_out.write('traffic_combi' + ',')
f_out.write('traffic_total' + ',')
f_out.write('tt_95' + ',')
f_out.write('tt_50' + ',')
f_out.write('tt_10' + ',')
f_out.write('tt_mean' + ',')
f_out.write('trans_delay_50' + '\n')

# Creating and appending additional timestamp columns to dataframe
full_df = full_df[full_df.travel_time_minutes.notnull()]
time1 = time.time()
new_mat = [extract_vals(dStr) for dStr in full_df['measurement_tstamp']]
time2 = time.time()
print('Mat Creation: ' + str(time2 - time1))
time1 = time.time()
full_df['Date'], full_df['Year'], full_df['Month'], full_df['Day'], full_df['AP'], full_df['weekday'] = create_columns(new_mat)
start_date = full_df['Date'].min()
end_date = full_df['Date'].max()
full_df['Hour'] = full_df['AP'] // 12
time2 = time.time()
print('full_df Creation: '+str(time2-time1))

# Aggregating data to TMCs and compute statistics
df_tdi = full_df.groupby('tmc_code')['travel_time_minutes'].agg([percentile(10), 'mean', 'count'])
wkdy_df = full_df[full_df['weekday'].isin([0, 1, 2, 3, 4])]
df_tti = wkdy_df.groupby('tmc_code')['travel_time_minutes'].agg([percentile(95), percentile(50)])

for index, row in tmc.iterrows():
    try:
        tmc_id = row['tmc']
        print('tmc_id')
        tmc_info_tdi = df_tdi.loc[tmc_id]
        tmc_info_tti = df_tti.loc[tmc_id]
        tmc_data_observations = tmc_info_tdi['count']
        tt10 = tmc_info_tdi['percentile_10']
        tt_mean = tmc_info_tdi['mean']
        tt95 = tmc_info_tti['percentile_95']
        tt50 = tmc_info_tti['percentile_50']

        # Determine % of data available
        pct_year_avail = 100.0 * tmc_data_observations / (365.0 * 288.0)

        # Compute transportation delay
        delay = tt_mean - tt10
        delay_50 = tt50 - tt10
        adt_total = sd.data[sd.data['Tmc'] == tmc_id]['AADT'].values[0]
        adt_single = sd.data[sd.data['Tmc'] == tmc_id]['AADT_Singl'].values[0]
        adt_combined = sd.data[sd.data['Tmc'] == tmc_id]['AADT_Combi'].values[0]
        traffic = adt_single + adt_combined
        length = tmc[tmc['tmc'] == tmc_id]['miles']
        tdi = delay * (traffic * 365.0) / (60 * length)
        tdi_50 = delay_50 * (traffic * 365.0) / (60 * length)

        # Compute unreliability
        tti = tt95 / tt50
        unreliability = tti * traffic / length

        f_out.write(tmc_id + ',')
        f_out.write('{:1.2f}'.format(tdi.values[0]) + ',')
        f_out.write('{:1.2f}'.format(unreliability.values[0]) + ',')
        f_out.write('{:1.5f}'.format(pct_year_avail) + ',')
        f_out.write('{:1.2f}'.format(length.values[0]) + ',')
        f_out.write('{:1.0f}'.format(adt_single) + ',')
        f_out.write('{:1.0f}'.format(adt_combined) + ',')
        f_out.write('{:1.0f}'.format(adt_total) + ',')
        f_out.write('{:1.2f}'.format(tt95) + ',')
        f_out.write('{:1.2f}'.format(tt50) + ',')
        f_out.write('{:1.2f}'.format(tt10) + ',')
        f_out.write('{:1.2f}'.format(tt_mean) + ',')
        f_out.write('{:1.2f}'.format(tdi_50.values[0]) + '\n')
    except KeyError as key_error:
        print(key_error)
        pass

f_out.close()

otime2 = time.time()

print('Analysis runtime: '+str(otime2-otime1))
