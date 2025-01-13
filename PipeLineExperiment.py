import gc
import numpy as np
import pandas as pd
import warnings
from tqdm import tqdm
import dask.dataframe as dd
from dask.distributed import Client

client = Client(n_workers = 1)

warnings.simplefilter(action='ignore', category=FutureWarning)

def compute_and_continue(ddf):
    ddf = ddf.compute()
    ddf = dd.from_pandas(ddf,10)
    return ddf

def downscale_and_save(df, filename):
    df_copy = df.copy()

    for col in df_copy.select_dtypes(include='float64').columns:
        df_copy[col] = df_copy[col].astype('float32')

    df_copy.to_csv(filename, index=False)

    del df_copy
    gc.collect()

#Reading in the data
train_df = dd.read_csv('data/train_baby.csv', parse_dates=['date_time'])
test_df = dd.read_csv('data/test_baby.csv', parse_dates=['date_time'])

# Creating the relevance target
train_df['relevance'] = train_df['booking_bool'] * 2 + (train_df['click_bool'] * (1 - train_df['booking_bool']))

# Extract useful features from 'date_time'
train_df['year'] = train_df['date_time'].dt.year
train_df['month'] = train_df['date_time'].dt.month
train_df['day'] = train_df['date_time'].dt.day
train_df = train_df.drop(columns=['date_time'])

test_df['year'] = test_df['date_time'].dt.year
test_df['month'] = test_df['date_time'].dt.month
test_df['day'] = test_df['date_time'].dt.day
test_df = test_df.drop(columns=['date_time'])

#Removing Outliers
num_feats_with_outliers = ['price_usd', 'comp1_rate_percent_diff', 'comp2_rate_percent_diff', 'comp3_rate_percent_diff', 'comp4_rate_percent_diff', 'comp5_rate_percent_diff', 'comp6_rate_percent_diff', 'comp7_rate_percent_diff', 'comp8_rate_percent_diff']

for feature in num_feats_with_outliers:  # Based on EDA only price_usd & compX_rate_percent_diff
    Q1 = train_df[feature].quantile(0.25)
    Q3 = train_df[feature].quantile(0.75)
    IQR = Q3 - Q1
    lower_bound = Q1 - 3 * IQR
    upper_bound = Q3 + 3 * IQR
    
    # Replace outliers with NaN
    train_df[feature].mask(~train_df[feature].between(lower_bound, upper_bound), np.nan)

#Selecting Subset of Records

# Calculate the count of missing values in each row
train_df['missing_count'] = train_df.isnull().sum(axis=1)
# Sort the dataframe by 'missing_count' in ascending order
train_df = train_df.sort_values(by='missing_count')
# Select the top x% of the rows with the least missing values
top_percentage = 0.75
cut_off = int(len(train_df) * top_percentage)
train_df = train_df.head(cut_off)
train_df = dd.from_pandas(train_df,10)

#Feature Engineering

#Mean Position
mean_positions = train_df[train_df['random_bool'] == False].groupby('prop_id')['position'].mean().rename('mean_train_position')  # Exclude records where the results order is random
train_df = train_df.join(mean_positions, on='prop_id')
test_df = test_df.join(mean_positions, on='prop_id')

train_df = train_df.compute()
test_df = compute_and_continue(test_df)

#Compute prior click/book probability
def compute_prior(df, group_field, value_field):
    # Sum and count values per group
    #DASK sums = df.groupby(group_field)[value_field].transform('sum', meta = {'sum': int})
    sums = df.groupby(group_field)[value_field].transform('sum')
    #DASK count = df.groupby(group_field)[value_field].transform('count',  meta = {'count': int})
    count = df.groupby(group_field)[value_field].transform('count')
    # Calculate leave-one-out prior
    
    prior = (sums - df[value_field]) / (count - 1)
    
    return prior

# Apply function for click and booking bool
train_df['click_prior'] = compute_prior(train_df, 'prop_id', 'click_bool')
train_df['booking_prior'] = compute_prior(train_df, 'prop_id', 'booking_bool')

# Handling cases with only one record per group
train_df = train_df.fillna({'click_prior': train_df['click_bool'].mean()})
train_df = train_df.fillna({'booking_prior': train_df['booking_bool'].mean()})

train_df = dd.from_pandas(train_df,10)

# Priors for click and booking bool from the training set
test_df['click_prior'] = test_df['prop_id'].map(train_df.groupby('prop_id')['click_bool'].mean())
test_df['booking_prior'] = test_df['prop_id'].map(train_df.groupby('prop_id')['booking_bool'].mean())

# Handling cases with only one record per group
test_df = test_df.fillna({'click_prior': train_df['click_bool'].mean()})
test_df = test_df.fillna({'booking_prior': train_df['booking_bool'].mean()})

test_df = compute_and_continue(test_df)

#Number of Previous Searches

# Number of occurences "minus the current row"
train_df['previous_searches'] = train_df.groupby('prop_id')['prop_id'].transform('count') - 1
test_df['previous_searches'] = test_df['prop_id'].map(train_df['prop_id'].value_counts() - 1).fillna(0)

train_df = compute_and_continue(train_df)
test_df = compute_and_continue(test_df)

# Aggregate number of bookings for each property and destination combination
booking_counts = train_df.groupby(['prop_id', 'srch_destination_id'])['booking_bool'].sum().reset_index()
booking_counts = booking_counts.rename(columns={'booking_bool': 'booking_count'})

# Merge this count back to the train and test datasets
train_df = train_df.merge(booking_counts, on=['prop_id', 'srch_destination_id'], how='left')
test_df = test_df.merge(booking_counts, on=['prop_id', 'srch_destination_id'], how='left')

train_df = train_df.compute()
test_df = compute_and_continue(test_df)


# Calculate the maximum difference in distance to the user within each search query
train_df['max_distance_diff'] = train_df.groupby('srch_id')['orig_destination_distance'].transform(lambda x: x.max() - x.min())

train_df = dd.from_pandas(train_df,10)

# Compute the mean of these maximum differences by property and add it back to the dataset
mean_distance = train_df.groupby('prop_id')['max_distance_diff'].mean().reset_index()
mean_distance = mean_distance.rename(columns={'max_distance_diff': 'mean_max_distance_diff'})

train_df = train_df.merge(mean_distance, on='prop_id', how='left')
test_df = test_df.merge(mean_distance, on='prop_id', how='left')

train_df = compute_and_continue(train_df)
test_df = compute_and_continue(test_df)

features_to_stat = ['visitor_hist_starrating', 'visitor_hist_adr_usd', 'prop_starrating', 'prop_review_score', 'prop_location_score1', 'prop_location_score2', 'prop_log_historical_price', 'price_usd', 'orig_destination_distance', 'srch_query_affinity_score', 'srch_length_of_stay', 'srch_booking_window', 'srch_adults_count', 'srch_children_count', 'srch_room_count']  # Perhaps change this based on LightGBM.feature_importances_
for feature in tqdm(features_to_stat):
    feature_groupby = train_df.groupby('prop_id')[feature]

    stats_mean = feature_groupby.agg(['mean']).rename(columns={'mean': f'{feature}_mean'})
    train_df = train_df.join(stats_mean, on='prop_id')
    
    stats_std = feature_groupby.agg(['std']).rename(columns={'std': f'{feature}_std'})
    train_df = train_df.join(stats_std, on='prop_id')

    train_df = compute_and_continue(train_df)

for feature in tqdm(features_to_stat):
    feature_groupby = train_df.groupby('prop_id')[feature]

    stats_mean = feature_groupby.agg(['mean']).rename(columns={'mean': f'{feature}_mean'})
    test_df= test_df.join(stats_mean, on='prop_id')
    
    stats_std = feature_groupby.agg(['std']).rename(columns={'std': f'{feature}_std'})
    test_df = test_df.join(stats_std, on='prop_id')

    test_df = compute_and_continue(test_df)


#Save Data For completeness sake
train_df = train_df.compute()
test_df = test_df.compute()

downscale_and_save(train_df, 'data/processed_train.csv')
downscale_and_save(test_df, 'data/processed_test.csv')