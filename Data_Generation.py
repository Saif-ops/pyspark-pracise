import pandas as pd

activity_data = [[1, 2, '2016-03-01', 5],
        [1, 2, '2016-05-02', 6],
        [1, 3, '2017-06-25', 1],
        [3, 1, '2016-03-02', 0],
        [3, 4, '2018-07-03', 5]]

activity = pd.DataFrame(activity_data, columns=['player_id', 'device_id', 'event_date', 'games_played']).astype({'player_id':'Int64', 'device_id':'Int64', 'event_date':'datetime64[ns]', 'games_played':'Int64'})
# Data for Trips table


# Save Trips as a Parquet file
#trips_df.to_parquet("Trips.parquet", engine="pyarrow", index=False)
#print("Trips Parquet file saved as 'Trips.parquet'.")

# Save Users as a CSV file
activity.to_csv("activity.csv", index=False)
print("Users CSV file saved as 'Users.csv'.")
