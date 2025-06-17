/*
STOPS TABLE

DANIEL BRISTOW, 01/23/2024

Updated 07/29/2024 to add Load Seq for joining to Loadleg table on a combination of Loadleg Tag and Load Seq
in a Power BI data model.

Updated 08/27/2024 to include shifts A, B, and C; and IB Load Count per shift.

Updated 03/28/2025 with Estimated Arrival data and a comparison to Planned Arrival data to forewarn DMS dock personnel if a shipment 
is running late and if so, by how many hours.

Updated 05/13/2025 to replace customer DMS with HUT (Hutchinson)

Updated 05/21/2025 to change customer code for Hutchinson to AVS

Updated the supplier_agg subquery, so that it includes suppliers that are missing a planner (Hutchinson does not yet have
planners added to the supplier table, like DMS did.  Also updated the final join to a LEFT JOIN from an INNER JOIN.

Updated 05/29/2025 to exclude IB/OB, because Hutchinson does not consolidate.
Also changed On-Time evaluation to be based on Departure times instead of Arrival times, because
only departure check calls are being entered for this customer (per Austin Cornwall's request, approved by Ryan Mann).

Updated 06/16/2025 with new stop codes for the Hutchinson plants ('AVS', 'FMS')
FOR POWER BI IMPORT
*/


--Join to Load Leg Hourly 2306 on CONCAT(Load Leg ID, cus).
WITH raw_data AS (
SELECT DISTINCT
	s.[Route] AS [Route]
	,s.[Version] AS [Version]
	,s.[Leg] AS [Leg]
	,CONCAT(s.[Route], '-', s.[Version]) AS [Route-Version]
	,CONCAT(s.[Route], '-', s.[Version], '-', s.[Leg]) AS [Route-Version-Leg]
	,s.[Loadleg_ID] AS [Loadleg ID]
	,s.[Load_ID] AS [Load ID]
	,s.[LoadLeg_Tag] AS [Loadleg Tag]
	,CONCAT(s.[LoadLeg_Tag], ' - ', s.[Stop_Seq]) AS [Loadleg Tag Stop]
	,s.[CC_Active_Status] AS [Active Status]
	,s.[Stop_Code] AS [Stop Location]
	--,MAX(CASE WHEN [Stop_Code] IN ('DMS - DET', '157187') THEN 1 ELSE 0 END) OVER (PARTITION BY s.[Loadleg_ID]) AS [Includes 157187 or DMS - DET]
	
	,CASE 
		WHEN s.[Stop_Code] IN ('10', '10A', '11', 'FMS', 'AVS') THEN 'Yes' 
		ELSE 'No'
	END AS [Is HUT Location]
	,s.[Stop_Name] AS [Stop Name]
	,s.[Stop_Seq] AS [Stop Sequence]
	,s.[Is_Pick] AS [Is Pick]
	,s.[Is_Drop] AS [Is Drop]
	,CASE
		WHEN [Is_Pick] = 0 AND [Is_Drop] = 0 THEN ''
		WHEN s.[Is_Pick] = 1 AND s.[Is_Drop] = 1 THEN 'Dunnage/Freight'
		WHEN s.[Is_Pick] = 1 AND s.[Stop_Code] IN ('10', '10A', '11') THEN 'Dunnage'
		WHEN s.[Is_Pick] = 1 THEN 'Freight'
		WHEN s.[Is_Drop] = 1 AND s.[Stop_Code] IN ('10', '10A', '11') THEN 'Freight'
		WHEN s.[Is_Drop] = 1 THEN 'Dunnage'
		WHEN [Is_Pick] IS NULL AND [Is_Drop] IS NULL THEN ''
	END AS [Dunnage/Freight]

	/* Compare planned arrival to estimated arrival */
	,TRY_CAST(s.[Dep_Time] AS DATETIME) AS [Planned Departure]
	,TRY_CAST(s.[Dep_Time] AS DATE) AS [Planned Departure Date]
	,TRY_CAST(s.[Est_Dep_Time] AS DATETIME) AS [Estimated Departure]
	,TRY_CAST(s.[Est_Dep_Time] AS DATE) AS [Estimated Departure Date]
	,TRY_CAST(s.[Est_Dep_Time] AS TIME) AS [Estimated Departure Time]
	,CAST(DATEDIFF(MINUTE, TRY_CAST(s.[Dep_Time] AS DATETIME), TRY_CAST(s.[Est_Dep_Time] AS DATETIME)) / 60.0 AS DECIMAL(5,2)) AS [Late By (Hrs)]

	--Identify day of the week.
	--Identify time of day.
	--Based on day of week and time of day, add Crew field with values A, B, or C.
	--Count the number of unique loadleg tags (excluding loads with route codes that start with DMSDL) per shift per day. (partition by day then by shift -- A, B, or C)
	--In Power BI, create a metric that takes the average count in any given context.
	--,TRY_CAST(s.[Arr_Time] AS TIME) AS [Planned Arrival Time]
	,DATENAME(WEEKDAY, s.[Arr_Time]) AS [Planned Day of Week]

	/* Add crew data */
    ,CASE 
		--WHEN s.[Route] LIKE 'DMSDL%' THEN NULL

        -- A Crew (kept the same)
        WHEN DATEPART(WEEKDAY, s.[Arr_Time]) IN (2,3,4,5) -- Mon, Tue, Wed, Thu
             AND CAST(s.[Arr_Time] AS TIME) BETWEEN '06:00' AND '16:30' THEN 'A'
        
        -- B Crew (updated)
        WHEN (DATEPART(WEEKDAY, s.[Arr_Time]) IN (3,4,5,6) -- Tue, Wed, Thu, Fri
              AND CAST(s.[Arr_Time] AS TIME) BETWEEN '18:00' AND '23:59:59')
             OR 
             (DATEPART(WEEKDAY, s.[Arr_Time]) IN (4,5,6,7) -- Wed, Thu, Fri, Sat
              AND CAST(s.[Arr_Time] AS TIME) BETWEEN '00:00' AND '04:30') THEN 'B'
        
        -- C Crew (updated)
        WHEN (DATEPART(WEEKDAY, s.[Arr_Time]) IN (6,7) -- Fri, Sat
              AND CAST(s.[Arr_Time] AS TIME) BETWEEN '06:00' AND '16:30')
             OR 
             (DATEPART(WEEKDAY, s.[Arr_Time]) IN (1,2) -- Sun, Mon
              AND CAST(s.[Arr_Time] AS TIME) BETWEEN '18:00' AND '23:59:59')
             OR 
             (DATEPART(WEEKDAY, s.[Arr_Time]) IN (2,3) -- Mon, Tue
              AND CAST(s.[Arr_Time] AS TIME) BETWEEN '00:00' AND '04:30') THEN 'C'
        
        ELSE NULL -- for any times/days not covered by the conditions
    END AS Crew

	/* Add shift data */
	,CASE
		-- A Crew
		WHEN DATEPART(WEEKDAY, s.[Arr_Time]) IN (2,3,4,5)
			 AND CAST(s.[Arr_Time] AS TIME) BETWEEN '06:00' AND '16:30'
		THEN DATEADD(HOUR, 6, DATEADD(DAY, DATEDIFF(DAY, 0, s.[Arr_Time]), 0))
		-- B Crew
		WHEN (DATEPART(WEEKDAY, s.[Arr_Time]) IN (3,4,5,6)
			  AND CAST(s.[Arr_Time] AS TIME) BETWEEN '18:00' AND '23:59:59')
		THEN DATEADD(HOUR, 18, DATEADD(DAY, DATEDIFF(DAY, 0, s.[Arr_Time]), 0))
		WHEN (DATEPART(WEEKDAY, s.[Arr_Time]) IN (4,5,6,7)
			  AND CAST(s.[Arr_Time] AS TIME) BETWEEN '00:00' AND '04:30')
		THEN DATEADD(HOUR, 18, DATEADD(DAY, DATEDIFF(DAY, 0, s.[Arr_Time]) - 1, 0))
		-- C Crew
		WHEN (DATEPART(WEEKDAY, s.[Arr_Time]) IN (6,7)
			  AND CAST(s.[Arr_Time] AS TIME) BETWEEN '06:00' AND '16:30')
		THEN DATEADD(HOUR, 6, DATEADD(DAY, DATEDIFF(DAY, 0, s.[Arr_Time]), 0))
		WHEN (DATEPART(WEEKDAY, s.[Arr_Time]) IN (1,2)
			  AND CAST(s.[Arr_Time] AS TIME) BETWEEN '18:00' AND '23:59:59')
		THEN DATEADD(HOUR, 18, DATEADD(DAY, DATEDIFF(DAY, 0, s.[Arr_Time]), 0))
		WHEN (DATEPART(WEEKDAY, s.[Arr_Time]) IN (2,3)
			  AND CAST(s.[Arr_Time] AS TIME) BETWEEN '00:00' AND '04:30')
		THEN DATEADD(HOUR, 18, DATEADD(DAY, DATEDIFF(DAY, 0, s.[Arr_Time]) - 1, 0))
		ELSE NULL
	END AS [Shift Start Time]

	,TRY_CAST(s.[Actl_Arr_Time] AS DATETIME) AS [Actual Arrival]
	,TRY_CAST(s.[Actl_Arr_Time] AS DATE) AS [Actual Arrival Date]
	,TRY_CAST(s.[Actl_Arr_Time] AS TIME) AS [Actual Arrival Time]
	,TRY_CAST(s.[Arr_Time] AS DATETIME) AS [Planned Arrival]
	,TRY_CAST(s.[Arr_Time] AS DATE) AS [Planned Arrival Date]
	,TRY_CAST(s.[Arr_Time] AS TIME) AS [Planned Arrival Time]
	--,TRY_CAST(s.[Dep_Time] AS DATETIME) AS [Planned Departure]
	--,TRY_CAST(s.[Dep_Time] AS DATE) AS [Planned Departure Date]
	,TRY_CAST(s.[Dep_Time] AS TIME) AS [Planned Departure Time]
	,TRY_CAST(s.[Actl_Dep_Time] AS DATETIME) AS [Actual Departure]
	,TRY_CAST(s.[Actl_Dep_Time] AS DATE) AS [Actual Departure Date]
	,TRY_CAST(s.[Actl_Dep_Time] AS TIME) AS [Actual Departure Time]

	,s.[Travel_Time]
	,CAST(REPLACE(s.[Distance_From_last_Stop], '.', '') AS INT) AS [Distance from Last Stop]
	,CASE 
		WHEN s.cus IN ('HUT', 'AVS', 'FMS') THEN 'HUT' 
		ELSE 'OTHER' 
	END AS [Stop Customer]
	,s.[Last_update] AS [Stops Last Updated]


FROM METRIX_LOAD_LEG_STOP_Hourly_2306 s
WHERE 
	s.cus IN ('AVS', 'HUT', 'FMS')
	AND s.[Loadleg_ID] NOT IN (		--Include only loads that deliver to a Hutchinson plant.		
		-- Find loads where ALL delivery stops are to non-Hutchinson locations and EXCLUDE them!
		SELECT s2.[Loadleg_ID]
		FROM METRIX_LOAD_LEG_STOP_Hourly_2306 s2
		WHERE s2.[Is_Drop] = 1
		AND s2.cus IN ('AVS', 'HUT', 'FMS')
		GROUP BY s2.[Loadleg_ID]
		HAVING SUM(CASE WHEN s2.[Stop_Code] IN (
			'10'
			--,'10A'
			,'11'
			,'FMS'
			,'AVS') THEN 1 ELSE 0 END) = 0
	)
), final_data AS (
SELECT
	[Route]
	,[Version]
	,[Leg]
	,[Route-Version]
	,[Route-Version-Leg]
	,[Loadleg ID]
	,[Load ID]
	,[Loadleg Tag]
	,[Loadleg Tag Stop]
	,[Active Status]
	,[Stop Location]
	--,[Includes 157187 or DMS - DET]
	,[Is HUT Location]
	,[Stop Name]
	,[Stop Sequence]
	,[Is Pick]
	,[Is Drop]
	,[Dunnage/Freight]
	--,[Planned Departure]
	--,[Planned Departure Date]
	,[Estimated Departure]
	,[Estimated Departure Date]
	,[Estimated Departure Time]
	,CASE WHEN [Late By (Hrs)] < 0.0 THEN 0.0 ELSE [Late By (Hrs)] END AS [Late By (Hrs)]
	,DATEADD(WEEK, DATEDIFF(WEEK, 0, CAST([Planned Departure Date] AS DATETIME) - 1), 0) AS [Start of Week]
	,[Planned Departure Time]
	,[Crew]
	,[Shift Start Time]
	,NULLIF(CONCAT([Crew], FORMAT([Shift Start Time], 'yyyyMMdd-HH')), '') AS [Shift Id]
	,[Planned Day of Week]
	,[Actual Arrival]
	,DATEDIFF(MINUTE, [Planned Departure], [Actual Departure]) AS [Difference]
	,DATEDIFF(DAY, [Planned Departure], GETDATE()) AS [Days Since Planned Departure]
	,CASE
		WHEN ABS(DATEDIFF(MINUTE, [Planned Departure], [Actual Departure])) IS NULL AND DATEDIFF(MINUTE, [Planned Departure], GETDATE()) <= 0 THEN 'Open'
		WHEN ABS(DATEDIFF(MINUTE, [Planned Departure], [Actual Departure])) IS NULL AND DATEDIFF(MINUTE, [Planned Departure], GETDATE()) > 0 THEN 'Missing Time'
		WHEN ABS(DATEDIFF(MINUTE, [Planned Departure], [Actual Departure])) BETWEEN 0 AND 30 THEN 'On-Time (+/- 30 Min.)'
		WHEN DATEDIFF(MINUTE, [Planned Departure], [Actual Departure]) < -30 THEN 'Early by 31+ Min.'
		WHEN ABS(DATEDIFF(MINUTE, [Planned Departure], [Actual Departure])) BETWEEN 31 AND 120 THEN 'Late by 31 to 120 Min.'
		WHEN ABS(DATEDIFF(MINUTE, [Planned Departure], [Actual Departure])) BETWEEN 121 AND 240 THEN 'Late by 2 to 4 Hrs.'
		WHEN ABS(DATEDIFF(MINUTE, [Planned Departure], [Actual Departure])) > 240 THEN 'Late by 4+ Hours'
	END AS [On-Time]
	--,CASE
	--	WHEN [Is Pick] = 0 AND [Is Drop] = 0 THEN ''
	--	WHEN [Is Pick] = 1 AND [Is Drop] = 1 THEN 'IB/OB'
	--	WHEN [Is Pick] = 1 THEN 'OB'
	--	WHEN [Is Drop] = 1 THEN 'IB'
	--	WHEN [Is Pick] IS NULL AND [Is Drop] IS NULL THEN ''
	--END AS [Inbound/Outbound]
	,[Actual Arrival Date]
	,[Actual Arrival Time]
	,[Planned Departure]
	,[Planned Departure Date]
	--,[Planned Departure Time]
	,[Actual Departure]
	,[Actual Departure Date]
	,[Actual Departure Time]
	,[Travel_Time]
	,[Distance from Last Stop]
	,[Stop Customer]
	,CONCAT([Loadleg ID], '-', [Stop Customer]) AS [Loadleg ID Customer Key]
	,[Stops Last Updated]
FROM raw_data
)
,supplier_agg AS (
	SELECT
		stops.[Loadleg ID]
		--,stops.cus
		--,stops.[Location Type]
		,STRING_AGG(stops.[Stop Location], ', ') AS [Suppliers]
		,STRING_AGG([Location Name], ', ') AS [Names]
		,STRING_AGG(stops.[Planner], ', ') AS [Planners]
	FROM (

			SELECT DISTINCT
				s.[Loadleg_ID] AS [Loadleg ID]
				--,s.cus
				,s.[Stop_Code] AS [Stop Location]
				--,l.[LocationType] AS [Location Type]
				,l.[Location_Name] AS [Location Name]
				,l.[LE] AS [Planner]
			FROM METRIX_LOAD_LEG_STOP_hourly_2306 s
			LEFT JOIN
				METRIX_LOCATION l
				ON s.[Stop_Code] = LTRIM(RTRIM(l.[Location_Code]))
			WHERE 
				s.cus IN ('AVS', 'HUT', 'FMS')
				AND (l.[LocationType] = 'Supplier' OR l.[LocationType] IS NULL)
				--AND l.[LE] <> ''
				--AND s.[LoadLeg_Tag] = '798237-1'
		) stops
	GROUP BY stops.[Loadleg ID] --,stops.cus, stops.[Location Type]
)
--,pbi_dataset AS (
SELECT
	[Route]
	,[Version]
	,[Leg]
	,[Route-Version]
	,[Route-Version-Leg]
	,f.[Loadleg ID]
	--,s.[Loadleg ID]
	,[Load ID]
	,[Loadleg Tag]
	,[Loadleg Tag Stop]
	,[Active Status]
	,[Stop Location]
	--,[Includes 157187 or DMS - DET]
	,[Is HUT Location]
	,[Stop Name]
	,[Suppliers] AS [Supplier(s)]
	,[Names] AS [Name(s)]
	,[Planners] AS [Planner(s)]
	,[Stop Sequence]
	,[Is Pick]
	,[Is Drop]
	,[Dunnage/Freight]
	,[Planned Departure]
	--,COUNT(CASE WHEN [Stop Location] = 'DMS - DET' THEN [Loadleg Tag] END) OVER(PARTITION BY [Planned Departure Date], [Crew]) AS [IB Loads Count]	
	,[Planned Departure Date]
	,[Planned Day of Week]
	,[Crew]
	,[Shift Start Time]
	,[Shift Id]
	--,COUNT(CASE WHEN [Stop Location] = 'DMS - DET' AND [Inbound/Outbound] LIKE '%IB%' THEN [Loadleg Tag] END) OVER(PARTITION BY [Shift Id]) AS [IB Loads Count]	
	,[Start of Week]
	,[Planned Departure Time]
	,[Estimated Departure]
	,[Estimated Departure Date]
	,[Estimated Departure Time]
	,CASE WHEN [Actual Departure] IS NULL THEN [Late By (Hrs)] ELSE NULL END AS [Late By (Hrs) Sort By This Field in PBI]
	,CASE WHEN [Actual Departure] IS NULL THEN CAST([Late By (Hrs)] AS VARCHAR) ELSE CAST([On-Time] AS VARCHAR) END AS [Late By (Hrs) for PBI Visual]
	,[Actual Arrival]
	,[Difference]
	,[Days Since Planned Departure]
	,[On-Time]
	--,CASE 
	--	WHEN [On-Time] = 'Missing Time' THEN 
	--		CASE
	--			WHEN [Days Since Planned Arrival] < 1 THEN 'Missed by Less Than 24 Hrs.'
	--			WHEN [Days Since Planned Arrival] BETWEEN 1 AND 2 THEN 'Missed by 1-2 Days'
	--			WHEN [Days Since Planned Arrival] BETWEEN 3 AND 4 THEN 'Missed by 3-4 Days'
	--			WHEN [Days Since Planned Arrival] BETWEEN 5 AND 6 THEN 'Missed by 5-6 Days'
	--			WHEN [Days Since Planned Arrival] >= 7 THEN 'Missed by 7+ Days'
	--		END
	--	WHEN [On-Time] = 'Open' THEN 'Open'
	--	ELSE 'Arrived'
	--END AS [Days Missed]
	--,[Inbound/Outbound]
	,[Actual Arrival Date]
	,[Actual Arrival Time]
	--,[Planned Departure]
	--,[Planned Departure Date]
	--,[Planned Departure Time]
	,[Actual Departure]
	,[Actual Departure Date]
	,[Actual Departure Time]
	,[Travel_Time]
	,[Distance from Last Stop]
	,[Stop Customer]
	,[Loadleg ID Customer Key]
	,[Stops Last Updated]
FROM final_data f
LEFT JOIN
	supplier_agg s
	ON f.[Loadleg ID] = s.[Loadleg ID]
--WHERE [Loadleg Tag] = '796709-1'
ORDER BY [Loadleg ID], [Stop Sequence]

/* Show count of missing arrival times and departure times */
--)
--SELECT
--	/* Arrival and Departure Data Entry Success Rate at Pick Stops */
--	SUM(CASE WHEN [Is Pick] = 1 THEN 1 ELSE 0 END) AS [Total Stops at Suppliers]
--	,SUM(CASE WHEN [Is Pick] = 1 AND [Actual Arrival Date] IS NULL THEN 1 ELSE 0 END) AS [Count Missing Arrival Times at Suppliers]
--	,100.0 * SUM(CASE WHEN [Is Pick] = 1 AND [Actual Arrival Date] IS NULL THEN 1 ELSE 0 END) / SUM(CASE WHEN [Is Pick] = 1 THEN 1 ELSE 0 END) AS [Missing Arr. Time % at Pick]
--	,SUM(CASE WHEN [Is Pick] = 1 AND [Actual Departure Date] IS NULL THEN 1 ELSE 0 END) AS [Count Missing Departure Times at Suppliers]
--	,100.0 * SUM(CASE WHEN [Is Pick] = 1 AND [Actual Departure Date] IS NULL THEN 1 ELSE 0 END) / 	SUM(CASE WHEN [Is Pick] = 1 THEN 1 ELSE 0 END) AS [Missing Dep. Time % at Pick]


--	/* Arrival and Departure Data Entry Success Rate at Drop Stops */
--	,SUM(CASE WHEN [Is Drop] = 1 THEN 1 ELSE 0 END) AS [Total Stops at Plants]
--	,SUM(CASE WHEN [Is Drop] = 1 AND [Actual Arrival Date] IS NULL THEN 1 ELSE 0 END) AS [Count Missing Arrival Times at Plants]
--	,100.0 * SUM(CASE WHEN [Is Drop] = 1 AND [Actual Arrival Date] IS NULL THEN 1 ELSE 0 END) / SUM(CASE WHEN [Is Drop] = 1 THEN 1 ELSE 0 END) AS [Missing Arr. Time % at Drop]
--	,SUM(CASE WHEN [Is Drop] = 1 AND [Actual Departure Date] IS NULL THEN 1 ELSE 0 END) AS [Count Missing Departure Times at Plants]
--	,100.0 * SUM(CASE WHEN [Is Drop] = 1 AND [Actual Departure Date] IS NULL THEN 1 ELSE 0 END) / SUM(CASE WHEN [Is Drop] = 1 THEN 1 ELSE 0 END) AS [Missing Dep. Time % at Drop]
--FROM pbi_dataset

/* Sample dataset for showing missing arrival and departure times to internal customers */
--)
--SELECT
--	[Loadleg Tag]
--	,[Route]
--	,[Stop Location] AS [Stop Loc]
--	,[Stop Sequence] AS [Stop Seq]
--	,[Is Pick]
--	,[Is Drop]
--	,[Planned Arrival Date] AS [Planned Arr.]
--	--,[Planned Arrival Time]
--	,[Actual Arrival Date] AS [Actual Arr.]
--	--,[Actual Arrival Time]
--	,[Planned Departure Date] AS [Planned Dep.]
--	--,[Planned Departure Time]
--	,[Actual Departure Date] AS [Actual Dep.]
--	--,[Actual Departure Time]
--	,[On-Time]
--FROM pbi_dataset