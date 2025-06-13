/*
LOAD LEG DAILY TABLE

DANIEL BRISTOW, 05/08/2024
Updated 06/10/2024 with route pattern from the Load table.
Updated 06/13/2024, so that the latest load id for each load tag is included, excluding cancelled or adjusted load ids.
Updated 06/14/2024 to exclude Life Cycle Sequences 'System Cancelled (4)','ACA Cancelled (5)','ACA Adjusted (6)', and 'ACA Rejected (7)', because somehow
some still existed in the dataset after the Is_ACA flag was set to 0.

Updated 03/28/2025 to exclude load status = 'Adjusted'

Updated 05/13/2025: switched customer from DMS to HUT (Hutchinson)

Updated 05/21/2025: customer code for Hutchinson to 'AVS'

Updated 06/13/2025: Added [Acitivy] field, so the report is slicable by Hutchinson's business units.

FOR POWER BI IMPORT
*/

WITH loads_data AS (
	SELECT DISTINCT
		ll.[Loadleg_tag] AS [Loadleg Tag]
		,ld.[Load_Tag] AS [Load Tag]
		,CASE LEFT(ll.[Route], 3) WHEN 'AVS' THEN 'AVS' WHEN 'FMS' THEN 'FMS' END AS [Activity] --Business units (Anti-Vibration Systems and Fluid Management Systems)
		--ll.[Acc_Location] AS [Division]
		,ld.[Status] AS [Load Status]
		,ld.[Rptn] AS [Route Pattern]
		,ld.[Load_ID] AS [Load ID]
		,ll.[Route] AS [Route]
		,ll.[Version] AS [Version]
		,ll.[Leg] AS [Leg]
		,CONCAT(ll.[Route], '-', ll.[Version]) AS [Route-Version]
		,CONCAT(ll.[Route], '-', ll.[Version], '-', ll.[Leg]) AS [Route-Version-Leg]
		,ll.[Mode] AS [Mode]
		,ll.[Loadleg_Id] AS [Loadleg ID]
		,ll.[Trailer_1] AS [Trailer]
		,ll.[Trailer_2] AS [Trailer 2]
		,TRY_CAST(ll.[Start_Time] AS DATETIME) AS [Load Start Time]
		,TRY_CAST(ll.[End_Time] AS DATE) AS [Load End Date]
		,TRY_CAST(ll.[End_Time] AS DATETIME) AS [Load End Time]
		,TRY_CAST(ll.[Est_Start_Time] AS DATETIME) AS [Estimated Start Time]
		,ll.[Life_Cycle_Seq] AS [Life Cycle Sequence]
		,ll.[Status] AS [Status]
		,ll.[Carrier_Code] AS [Carrier Code]
		,ll.[Carrier_Name] AS [Carrier Name]
		,CONCAT(ll.[Carrier_Code], ' - ', ll.[Carrier_Name]) AS [Carrier]
		,TRY_CAST(REPLACE(ll.[Distance], '.', '') AS INT) AS [Distance]
		,ll.[Year] AS [Year]
		,ll.[Week] AS [Week]
		,ll.[Last_update] AS [Load Legs Last Updated]
	FROM METRIX_LOAD_hourly_2306 ld

	LEFT JOIN
		METRIX_LOAD_LEG_hourly_2306 ll
		ON ld.[Load_Tag] = ll.[Load_Tag]
			AND ld.[Load_ID] = ll.[Load_ID]
			AND ld.[Status] <> 'Adjusted'
	WHERE
		ld.[Status] <> 'Adjusted'
		AND ld.[cus] IN ('AVS', 'FMS')
		AND (ll.[cus] IN ('AVS', 'FMS') OR ll.[cus] IS NULL)
		AND (ll.[Life_Cycle_Seq] NOT IN (	
			'System Cancelled (4)'
			,'ACA Cancelled (5)'
			,'ACA Adjusted (6)'
			,'ACA Rejected (7)'
		) OR ll.[Life_Cycle_Seq] IS NULL)
		--AND ll.[Load_Tag] = '775704'

)
--,pbi_dataset AS (
SELECT
	[Loadleg Tag]
	--,CONCAT([Load Tag], ' - ', [Load Seq]) AS [Tag-Seq Key]						--alternate primary key for joining to STOPS table in PBI model
	--,CONCAT([Loadleg Tag], ' - ', [Load Seq]) AS [Loadleg Tag - Load Seq]		--primary key for joining to STOPS table in PBI model
	,[Load Tag]
	,[Activity]
	,[Load Status]
	,[Route Pattern]
	,[Load ID]
	,[Route]
	,COUNT([Load ID]) OVER(PARTITION BY [Route]) AS [Load Count Per Route]
	,[Version]
	,[Leg]
	,[Route-Version]
	,[Route-Version-Leg]
	,[Mode]
	,[Loadleg ID]
	,[Trailer]
	,[Trailer 2]
	,[Load Start Time]
	,[Load End Date]
	,[Load End Time]
	,[Estimated Start Time]
	,[Life Cycle Sequence]
	,[Status]
	,[Carrier Code]
	,[Carrier Name]
	,[Carrier]
	,COUNT([Load ID]) OVER(PARTITION BY [Carrier Code]) AS [Load Count Per Carrier]
	,[Distance]
	,[Year]
	,[Week]
	,[Load Legs Last Updated]
	,'ALMS' AS [TMS]
FROM loads_data
--WHERE
--	[Load Tag] <> '782825'
--	OR
--		([Load Tag] = '782825' AND [Loadleg ID] = '503838')
	--[Route-Version] = 'DMSDL46-1'
	--AND [Load Start Time] BETWEEN '2024-07-22' AND '2024-07-28'


/*
--)
--SELECT
--	COUNT(DISTINCT([Loadleg Tag])) AS [Distinct Loadleg Tags]
--	,COUNT(*) AS [All Rows]
--FROM pbi_dataset

--)
--SELECT
--	[Loadleg Tag]
--	,COUNT(*)
--FROM pbi_dataset
--GROUP BY [Loadleg Tag]
--ORDER BY 2 DESC

--)
--SELECT
--*
--FROM pbi_dataset
--WHERE [Loadleg Tag] = '782825-1'
*/