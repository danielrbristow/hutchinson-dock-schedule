/*
DOCK SCHEDULE (HUTCHINSON)

DANIEL BRISTOW, 06/13/2025
Updated 06/16/2025, trimmed the fat and limited results to orders terminating at a Hutchinson plant

FOR POWER BI IMPORT
*/

USE TMS_venturelogistics;

SELECT DISTINCT
	--Order data
	ld.[LoadNum] AS [Loadleg Tag]
	,ld.[LoadNum] AS [Load Tag]
	,CASE ld_dest.[AddrName] 
		WHEN 'Hutchinson AVS' THEN 'AVS'	-- Hutchinson AVS
		WHEN 'Hutchinson Cadillac' THEN 'AVS'	-- Hutchinson Cadillac
		WHEN 'Hutchinson FMS Wh' THEN 'FMS'	-- Hutchinson FMS
		WHEN 'Hutchinson FMS' THEN 'FMS'
		ELSE 'OTHER' 
	END AS [Activity] --Business units (Anti-Vibration Systems and Fluid Management Systems)
	,ld.[DataValue_LoadTmsStatus] AS [Load Status]
	--,ld_dest.[AddrName]
	,NULL AS [Route Pattern]
	,ld.[LoadId] AS [Load ID]
	,NULL AS [Route]
	,NULL AS [Load Count Per Route]
	,NULL AS [Version]
	,NULL AS [Leg]
	,NULL AS [Route-Version]
	,NULL AS [Route-Version-Leg]
	,tm.[TransportModeName] AS [Mode]
	,ld.[LoadId] AS [Loadleg ID]
	,NULL AS [Trailer]
	,NULL AS [Trailer2]
	,TRY_CAST(ld.[DateTime_PlannedStart] AS DATETIME) AS [Load Start Time]
	,TRY_CAST(ld.[DateTime_PlannedEnd] AS DATE) AS [Load End Date]
	,TRY_CAST(ld.[DateTime_PlannedEnd] AS DATETIME) AS [Load End Time]
	,NULL AS [Estimated Start Time]
	,ld.[DataValue_LoadTmsStatus] AS [Life Cycle Sequence]
	,'Active' AS [Status]
	,car.[TradingPartnerNum] AS [Carrier Code]
	,car.[TradingPartnerName] AS [Carrier Name]
	,CONCAT(car.[TradingPartnerNum], ' - ', car.[TradingPartnerName]) AS [Carrier]
	,NULL AS [Load Count Per Carrier]
	,ld.[Dist_Tot] AS [Distance]
	,YEAR(ld.[DateTime_PlannedStart]) AS [Year]
	,NULL AS [Week]
	,ld.[DateLastModified] AS [Load Legs Last Updated]
	,'3G' AS [TMS]

FROM [Load] ld	--Orders
LEFT JOIN
	[Loc] ld_dest
	ON ld.[LocIdDest] = ld_dest.[LocId]
--LEFT JOIN
--	[Loc] ord_dest
--	ON ord.[LocIdDest] = ord_dest.[LocId]
LEFT JOIN
	Vw3g_Load_Ord ld_ord
	ON ld.[LoadId] = ld_ord.[LoadId]
--LEFT JOIN 
--	[OrdLeg] ol			--Order legs
--	ON ord.[OrdHeaderId] = ol.[OrdHeaderId]
--LEFT JOIN 
--	[Vw3g_OrdLeg_LoadFirstLeg] fl		--Order legs, load legs join table
--	ON ol.[OrdLegId] = fl.[OrdLegId]
LEFT JOIN 
	[OrdHeader] ord		--Loads
	ON ld_ord.[OrdHeaderId] = ord.[OrdHeaderId]
LEFT JOIN 
	[TradingPartner] clt		--Clients
	ON ord.[TradingPartnerIdClient] = clt.[TradingPartnerID] 
		AND clt.[DataValue_TradingPartnerType] = 'Client'
LEFT JOIN 
	[TransportMode] tm		--Modes
	ON ld.[TransportModeId] = tm.[TransportModeId]
LEFT JOIN 
	[TradingPartner] car	--Carriers
	ON ld.[TradingPartnerIdCarrier] = car.[TradingPartnerId] 
		AND car.[DataValue_TradingPartnerType] = 'Carrier'
WHERE
	--clt.TradingPartnerNum = 'HUT' --'395116'
	ld.[DataValue_LoadTmsStatus] NOT IN ('TenderExpired', 'Canceled') --Exclude expired tenders and canceled loads.
	AND ld.[LoadId] NOT IN (
		-- Find loads where ALL delivery stops are to non-Hutchinson locations and EXCLUDE them.
		SELECT s.[LoadId]
		FROM [Stop] s
		LEFT JOIN [Loc] loc ON s.[LocId] = loc.[LocId]
		WHERE s.[DataValue_StopType] = 'Delivery'
		GROUP BY s.[LoadId]
		HAVING SUM(CASE WHEN loc.AddrName LIKE '%Hutchinson%' THEN 1 ELSE 0 END) = 0
	)
	--AND ld.[LocIdOrig] IN ('578059', '578136')
	--AND ld.[LoadNum] = 'L250609-68927'

