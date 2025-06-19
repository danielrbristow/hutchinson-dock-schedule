/*
DOCK SCHEDULE (HUTCHINSON)
(Stops table)

DANIEL BRISTOW, 06/16/2025

FOR POWER BI IMPORT
*/

USE TMS_venturelogistics;

WITH raw_data AS(
SELECT TOP 1000
	NULL AS [Route]
	,NULL AS [Version]
	,NULL AS [Leg]
	,NULL AS [Route-Version]
	,NULL AS [Route-Version-Leg]
	,ld.[LoadNum] AS [Loadleg ID]
	,s.[LoadId] AS [Load ID]
	,ld.[LoadNum] AS [Loadleg Tag]
	,CONCAT(ld.[LoadNum],  ' - ', s.[StopNum]) AS [Loadleg Tag Stop]
	,CASE ld.[DataValue_LoadTmsStatus] 
		WHEN 'Delivered' THEN 'Completed' 
		WHEN 'Canceled' THEN 'Inactive' 
		WHEN 'TenderExpired' THEN 'Inactive' 
		ELSE 'Active' 
	END AS [Active Status]
	,s.[LocId] AS [Stop Location]
	,CASE WHEN loc.AddrName LIKE 'Hutchinson%' THEN 'Yes' ELSE 'No' END AS [Is HUT Location]
	,loc.AddrName AS [Stop Name]
	,s.[StopNum] AS [Stop Sequence]
	,CASE s.[DataValue_StopType] WHEN 'Pickup' THEN 1 ELSE 0 END AS [Is Pick]
	,CASE s.[DataValue_StopType] WHEN 'Delivery' THEN 1 ELSE 0 END AS [Is Drop]
	,CASE
		WHEN s.[DataValue_StopType] NOT IN ('Pickup', 'Delivery') THEN ''
		--WHEN s.[Is_Pick] = 1 AND s.[Is_Drop] = 1 THEN 'Dunnage/Freight'
		WHEN s.[DataValue_StopType] = 'Pickup' AND loc.AddrName LIKE 'Hutchinson%' THEN 'Dunnage'
		WHEN s.[DataValue_StopType] = 'Pickup' THEN 'Freight'
		WHEN s.[DataValue_StopType] = 'Delivery' AND loc.AddrName LIKE 'Hutchinson%' THEN 'Freight'
		WHEN s.[DataValue_StopType] = 'Delivery' THEN 'Dunnage'
	END AS [Dunnage/Freight]
		/* Compare planned arrival to estimated arrival */
	,TRY_CAST(s.[DateTime_LateArrival] AS DATETIME) AS [Planned Departure]
	,TRY_CAST(s.[DateTime_LateArrival] AS DATE) AS [Planned Departure Date]
	,TRY_CAST(s.[DateTime_ExpectedDeparture] AS DATETIME) AS [Estimated Departure]
	,TRY_CAST(s.[DateTime_ExpectedDeparture] AS DATE) AS [Estimated Departure Date]
	,TRY_CAST(s.[DateTime_ExpectedDeparture] AS TIME) AS [Estimated Departure Time]
	,CAST(DATEDIFF(MINUTE, TRY_CAST(s.[DateTime_LateArrival] AS DATETIME), TRY_CAST(s.[DateTime_ExpectedDeparture] AS DATETIME)) / 60.0 AS DECIMAL(5,2)) AS [Late By (Hrs)]
	
	--Identify day of the week.
	,DATENAME(WEEKDAY, s.[DateTime_PlannedArrival]) AS [Planned Day of Week]

	/* Add crew data */
    ,CASE 
		--WHEN s.[Route] LIKE 'DMSDL%' THEN NULL

        -- A Crew (kept the same)
        WHEN DATEPART(WEEKDAY, s.[DateTime_PlannedArrival]) IN (2,3,4,5) -- Mon, Tue, Wed, Thu
             AND CAST(s.[DateTime_PlannedArrival] AS TIME) BETWEEN '06:00' AND '16:30' THEN 'A'
        
        -- B Crew (updated)
        WHEN (DATEPART(WEEKDAY, s.[DateTime_PlannedArrival]) IN (3,4,5,6) -- Tue, Wed, Thu, Fri
              AND CAST(s.[DateTime_PlannedArrival] AS TIME) BETWEEN '18:00' AND '23:59:59')
             OR 
             (DATEPART(WEEKDAY, s.[DateTime_PlannedArrival]) IN (4,5,6,7) -- Wed, Thu, Fri, Sat
              AND CAST(s.[DateTime_PlannedArrival] AS TIME) BETWEEN '00:00' AND '04:30') THEN 'B'
        
        -- C Crew (updated)
        WHEN (DATEPART(WEEKDAY, s.[DateTime_PlannedArrival]) IN (6,7) -- Fri, Sat
              AND CAST(s.[DateTime_PlannedArrival] AS TIME) BETWEEN '06:00' AND '16:30')
             OR 
             (DATEPART(WEEKDAY, s.[DateTime_PlannedArrival]) IN (1,2) -- Sun, Mon
              AND CAST(s.[DateTime_PlannedArrival] AS TIME) BETWEEN '18:00' AND '23:59:59')
             OR 
             (DATEPART(WEEKDAY, s.[DateTime_PlannedArrival]) IN (2,3) -- Mon, Tue
              AND CAST(s.[DateTime_PlannedArrival] AS TIME) BETWEEN '00:00' AND '04:30') THEN 'C'
        
        ELSE NULL -- for any times/days not covered by the conditions
    END AS Crew

	/* Add shift data */
	,CASE
		-- A Crew
		WHEN DATEPART(WEEKDAY, s.[DateTime_PlannedArrival]) IN (2,3,4,5)
			 AND CAST(s.[DateTime_PlannedArrival] AS TIME) BETWEEN '06:00' AND '16:30'
		THEN DATEADD(HOUR, 6, DATEADD(DAY, DATEDIFF(DAY, 0, s.[DateTime_PlannedArrival]), 0))
		-- B Crew
		WHEN (DATEPART(WEEKDAY, s.[DateTime_PlannedArrival]) IN (3,4,5,6)
			  AND CAST(s.[DateTime_PlannedArrival] AS TIME) BETWEEN '18:00' AND '23:59:59')
		THEN DATEADD(HOUR, 18, DATEADD(DAY, DATEDIFF(DAY, 0, s.[DateTime_PlannedArrival]), 0))
		WHEN (DATEPART(WEEKDAY, s.[DateTime_PlannedArrival]) IN (4,5,6,7)
			  AND CAST(s.[DateTime_PlannedArrival] AS TIME) BETWEEN '00:00' AND '04:30')
		THEN DATEADD(HOUR, 18, DATEADD(DAY, DATEDIFF(DAY, 0, s.[DateTime_PlannedArrival]) - 1, 0))
		-- C Crew
		WHEN (DATEPART(WEEKDAY, s.[DateTime_PlannedArrival]) IN (6,7)
			  AND CAST(s.[DateTime_PlannedArrival] AS TIME) BETWEEN '06:00' AND '16:30')
		THEN DATEADD(HOUR, 6, DATEADD(DAY, DATEDIFF(DAY, 0, s.[DateTime_PlannedArrival]), 0))
		WHEN (DATEPART(WEEKDAY, s.[DateTime_PlannedArrival]) IN (1,2)
			  AND CAST(s.[DateTime_PlannedArrival] AS TIME) BETWEEN '18:00' AND '23:59:59')
		THEN DATEADD(HOUR, 18, DATEADD(DAY, DATEDIFF(DAY, 0, s.[DateTime_PlannedArrival]), 0))
		WHEN (DATEPART(WEEKDAY, s.[DateTime_PlannedArrival]) IN (2,3)
			  AND CAST(s.[DateTime_PlannedArrival] AS TIME) BETWEEN '00:00' AND '04:30')
		THEN DATEADD(HOUR, 18, DATEADD(DAY, DATEDIFF(DAY, 0, s.[DateTime_PlannedArrival]) - 1, 0))
		ELSE NULL
	END AS [Shift Start Time]

	,TRY_CAST(s.[DateTime_ActualArrival] AS DATETIME) AS [Actual Arrival]
	,TRY_CAST(s.[DateTime_ActualArrival] AS DATE) AS [Actual Arrival Date]
	,TRY_CAST(s.[DateTime_ActualArrival] AS TIME) AS [Actual Arrival Time]
	,TRY_CAST(s.[DateTime_PlannedArrival] AS DATETIME) AS [Planned Arrival]
	,TRY_CAST(s.[DateTime_PlannedArrival] AS DATE) AS [Planned Arrival Date]
	,TRY_CAST(s.[DateTime_PlannedArrival] AS TIME) AS [Planned Arrival Time]
	--,TRY_CAST(s.[DateTime_LateArrival] AS DATETIME) AS [Planned Departure]
	--,TRY_CAST(s.[DateTime_LateArrival] AS DATE) AS [Planned Departure Date]
	,TRY_CAST(s.[DateTime_LateArrival] AS TIME) AS [Planned Departure Time]
	,TRY_CAST(s.[DateTime_ActualDeparture] AS DATETIME) AS [Actual Departure]
	,TRY_CAST(s.[DateTime_ActualDeparture] AS DATE) AS [Actual Departure Date]
	,TRY_CAST(s.[DateTime_ActualDeparture] AS TIME) AS [Actual Departure Time]
	
	,s.[DurationTransitTimeFromPrevStop] AS [Travel_Time]
	,CAST(s.[Dist_FromPrevStop] AS INT) AS [Distance from Last Stop]
	,clt.[TradingPartnerNum] AS [Stop Customer]
	,CONCAT(ld.[LoadNum], '-', clt.[TradingPartnerNum]) AS [Loadleg ID Customer Key]
	,s.[DateLastModified] AS [Stops Last Updated]

FROM [Load] ld
LEFT JOIN
	Vw3g_Load_ClientAndCount ld_clt
	ON ld.[LoadId] = ld_clt.[LoadId]
LEFT JOIN 
	[TradingPartner] clt		--Clients
	ON ld_clt.[TradingPartnerIdClient] = clt.[TradingPartnerID]
LEFT JOIN
	[Stop] s
	ON ld.[LoadId] = s.[LoadId]
LEFT JOIN
	[Loc] loc
	ON s.[LocId] = loc.[LocId]
WHERE 
    clt.[TradingPartnerNum] = 'HUT'
	AND ld.[DataValue_LoadTmsStatus] NOT IN ('TenderExpired', 'Canceled') --Exclude expired tenders and canceled loads.
    AND ld.[LoadId] NOT IN (
        -- Find loads where ALL delivery stops are to non-Hutchinson locations
        SELECT s2.[LoadId]
        FROM [Stop] s2
        LEFT JOIN [Loc] loc2 ON s2.[LocId] = loc2.[LocId]
        WHERE s2.[DataValue_StopType] = 'Delivery'
        GROUP BY s2.[LoadId]
        HAVING SUM(CASE WHEN loc2.AddrName LIKE '%Hutchinson%' THEN 1 ELSE 0 END) = 0
    )
	--AND ld.[LoadNum] = 'L250509-67885'
	--AND ld.[LocIdOrig] IN ('578059', '578136')
)
,final_data AS (
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
	,[Is HUT Location]
	,[Stop Name]
	,[Stop Sequence]
	,[Is Pick]
	,[Is Drop]
	,[Dunnage/Freight]
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
	,[Loadleg ID Customer Key]
	,[Stops Last Updated]
FROM raw_data
)
,supplier_agg AS (
	SELECT
		stops.[Loadleg ID]
		,STRING_AGG(stops.[Stop Location], ', ') AS [Suppliers]
		,STRING_AGG([Location Name], ', ') AS [Names]
	FROM (

			SELECT DISTINCT
				ld.[LoadNum] AS [Loadleg ID]
				,s.[LocId] AS [Stop Location]
				,loc.AddrName AS [Location Name]
			FROM [Load] ld
			LEFT JOIN
				[Stop] s
				ON ld.[LoadId] = s.[LoadId]
			LEFT JOIN
				Vw3g_Load_ClientAndCount ld_clt
				ON ld.[LoadId] = ld_clt.[LoadId]
			LEFT JOIN 
				[TradingPartner] clt		--Clients
				ON ld_clt.[TradingPartnerIdClient] = clt.[TradingPartnerID]
			LEFT JOIN
				[Loc] loc
				ON s.[LocId] = loc.[LocId]
			WHERE 
				clt.[TradingPartnerNum] = 'HUT'
				AND s.[DataValue_StopType] = 'Pickup'
		) stops
	GROUP BY stops.[Loadleg ID]
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
	,NULL AS [Planner(s)]
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
--WHERE [Loadleg Tag] = 'l250530-68567'
ORDER BY [Loadleg Tag], [Stop Sequence]



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
