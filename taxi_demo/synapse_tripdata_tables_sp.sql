CREATE TABLE [tripdata]
( 
	[color] [nvarchar](256)  NULL,
	[pickup_date] [date]  NULL,
	[pickup_time] [datetime2](7)  NULL,
	[dropoff_time] [datetime2](7)  NULL,
	[pickup_zone_id] [int]  NULL,
	[pickup_borough] [nvarchar](256)  NULL,
	[pickup_zone_name] [nvarchar](256)  NULL,
	[dropoff_zone_id] [int]  NULL,
	[dropoff_borough] [nvarchar](256)  NULL,
	[dropoff_zone_name] [nvarchar](256)  NULL,
	[passenger_count] [int]  NULL,
	[trip_distance] [float]  NULL,
	[trip_minutes] [float]  NULL,
	[tip_amount] [float]  NULL,
	[total_amount] [float]  NULL
)
WITH
(
	DISTRIBUTION = HASH ( [pickup_zone_id] ),
	CLUSTERED COLUMNSTORE INDEX
)
GO

CREATE TABLE [tripdata_summary]
( 
	[color] [nvarchar](256)  NULL,
	[pickup_borough] [nvarchar](256)  NULL,
	[pickup_zone_name] [nvarchar](256)  NULL,
	[dropoff_borough] [nvarchar](256)  NULL,
	[dropoff_zone_name] [nvarchar](256)  NULL,
	[total_passengers] [bigint]  NULL,
	[total_distance] [float]  NULL,
	[total_minutes] [float]  NULL,
	[total_amount] [float]  NULL
)
WITH
(
	DISTRIBUTION = HASH([pickup_zone_name]),
	CLUSTERED COLUMNSTORE INDEX
)
GO

CREATE PROCEDURE trip_summary_update_sp
AS BEGIN
	MERGE INTO tripdata_summary ts USING tripdata_summary_update tsu
		ON ts.color = tsu.color AND
		ts.pickup_borough = tsu.pickup_borough AND 
		ts.pickup_zone_name = tsu.pickup_zone_name AND 
		ts.dropoff_borough = tsu.dropoff_borough AND 
		ts.dropoff_zone_name = tsu.dropoff_zone_name 
	WHEN MATCHED 
		THEN UPDATE SET 
		total_passengers = tsu.total_passengers,
		total_distance = tsu.total_distance,
		total_minutes = tsu.total_minutes,
		total_amount = tsu.total_amount
	WHEN NOT MATCHED 
		THEN INSERT ( 
			color,
			pickup_borough,
			pickup_zone_name,
			dropoff_borough,
			dropoff_zone_name,
			total_passengers,
			total_distance,
			total_minutes,
			total_amount 
		)
		VALUES ( 
			tsu.color,
			tsu.pickup_borough,
			tsu.pickup_zone_name,
			tsu.dropoff_borough,
			tsu.dropoff_zone_name,
			tsu.total_passengers,
			tsu.total_distance,
			tsu.total_minutes,
			tsu.total_amount
		);
END
