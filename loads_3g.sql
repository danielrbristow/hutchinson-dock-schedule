/*
DOCK SCHEDULE (HUTCHINSON)

DANIEL BRISTOW, 06/13/2025

FOR POWER BI IMPORT
*/
USE TMS_venturelogistics;

SELECT DISTINCT
	--Order data
	ld.[LoadNum] AS [Loadleg Tag]
	,ld.[LoadNum] AS [Load Tag]
	,CASE ord.LocIdBillto WHEN '578051' THEN 'AVS' WHEN '578131' THEN 'FMS' ELSE 'OTHER' END AS [Activity] --Business units (Anti-Vibration Systems and Fluid Management Systems)
	,ld.[DataValue_LoadTmsStatus] AS [Load Status]
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



	--,ord.ContactIdBillTo
	--,ord.LocIdBillTo
	--,ord.[OrdHeaderId] AS [Order Header Id]
	--,TRY_CAST(ord.[DateTime_EarlyPickup] AS DATE) AS [Order Pickup Date]
	--,ord.[DataValue_OrdTmsStatus] AS [Order Status]
	--,clt.TradingPartnerNum AS [Client]
	--,CONCAT(ordorigst.[StateCode], '-', orddestst.[StateCode]) AS [Lane]

	----Order Leg data
	--,ol.[OrdLegId] AS [Leg Number]
	--,CASE
	--	WHEN (l1.[AddrName] LIKE '%VENTURE%' OR l1.[AddrName] LIKE '%CROSSDOCK%') AND (l2.[AddrName] LIKE '%VENTURE%' OR l2.[AddrName] LIKE '%CROSSDOCK%') THEN 'TRANSFER'
	--	WHEN l1.[AddrName] LIKE '%VENTURE%' OR l1.[AddrName] LIKE '%CROSSDOCK%' THEN 'OB'
	--	WHEN l2.[AddrName] LIKE '%VENTURE%' OR l2.[AddrName] LIKE '%CROSSDOCK%' THEN 'IB'
	--	ELSE 'DIRECT'
	--END AS [Leg Direction]
	--,ol.[LegNum] AS [Leg Seq]
	--,ol.[DataValue_OrdLegTmsStatus] AS [Leg Status]

	----Load data
	--,ld.[LoadNum] AS [Load Number]
	--,ld.[LoadId] AS [Load Id]
	--,ld.[TradingPartnerIdCarrier] AS [Carrier Id]
	--,car.[TradingPartnerNum] AS [SCAC]
	--,car.[TradingPartnerName] AS [Carrier Name]
	--,ld.[CurrencyAmt_NetCostTot] AS [Load Total Cost]
	--,ld.[ProNum] AS [Pro Number]

	----More order data
	--,ord.HandlingUnitCountTot AS [Pallet Count]
	--,ord.PieceCountTot AS [Piece Count]
	--,ord.Vol_NetTot AS [Order Volume]
	--,ord.WtBase_GrossTot AS [Order Weight]
	--,ord.CurrencyAmt_NetFreightChargeTot AS [Freight Charges]
	--,ord.CurrencyAmt_NetAccessorialChargeTot AS [FSC]
	--,ord.CurrencyAmt_NetChargeTot AS [Total Charges]
	--,ord.[OrdLegCount] AS [Order Leg Count]


	----Order leg origin data
	--,ol.[LocIdOrig] AS [Leg Origin Id]
	--,l1.[AddrName] AS [Leg Origin Name]
	--,l1.[Addr1] AS [Leg Origin Address]
	--,l1.[CityName] AS [Leg Origin City]
	--,st1.[StateCode] AS [Leg Origin State]
	--,l1.[PostalCode] AS [Leg Origin Zip]

	----Order leg destination data
	--,ol.[LocIdDest] AS [Leg Destination Id]
	--,l2.[AddrName] AS [Leg Destination Name]
	--,l2.[Addr1] AS [Leg Destination Address]
	--,l2.[CityName] AS [Leg Destination City]
	--,st2.[StateCode] AS [Leg Destination State]
	--,l2.[PostalCode] AS [Leg Destination Zip]

	----Billing data
	--,i.[InvoiceNum] AS [Invoice Number]
	--,i.[CurrencyAmt_NetChargeTot] AS [Charge Total]
	----,c.[DataValue_CostType] AS [Charge Type]
	----,a.[AccCodeDesc] AS [Charge Description]
	----,CASE WHEN a.[AccCodeDesc] = 'TONU' THEN 'YES' ELSE 'NO' END AS [Is TONU]


	----Financial data (for allotting a buy amount to each order on a given load)
	--,ord.[CurrencyAmt_NetChargeTot] AS [Order Sell Amount]
	--,ISNULL(ISNULL(NULLIF(ld.[CurrencyAmt_NetCostTot], 0), f.CurrencyAmt_NetCostTot), 0) AS [Load Buy Amount]
	---- Calculate total sell amount for each load
	--,SUM(ord.[CurrencyAmt_NetChargeTot]) OVER (PARTITION BY ld.[LoadId]) AS [Total Load Sell Amount]
	---- Calculate the proportion of this order's charge to the total load charge
	--,ISNULL(ord.[CurrencyAmt_NetChargeTot] / NULLIF(SUM(ord.[CurrencyAmt_NetChargeTot]) OVER (PARTITION BY ld.[LoadId]), 0), 0) AS [Order Proportion]
	---- Allocate the load buy amount to this order based on its proportion
	--,ISNULL(ISNULL(ISNULL(NULLIF(ld.[CurrencyAmt_NetCostTot], 0), f.CurrencyAmt_NetCostTot), 0) * (ord.[CurrencyAmt_NetChargeTot] / NULLIF(SUM(ord.[CurrencyAmt_NetChargeTot]) OVER (PARTITION BY ld.[LoadId]), 0)), 0) AS [Allocated Expense]
       
	---- Assign a rank to each load for an order, prioritizing inbound and direct over outbound
	--,ROW_NUMBER() OVER (PARTITION BY ord.[OrdHeaderId] ORDER BY 
	--	CASE
	--		WHEN (l1.[AddrName] LIKE '%VENTURE%' OR l1.[AddrName] LIKE '%CROSSDOCK%') AND (l2.[AddrName] LIKE '%VENTURE%' OR l2.[AddrName] LIKE '%CROSSDOCK%') THEN 2 -- Transfer loads
	--		WHEN l2.[AddrName] LIKE '%VENTURE%' OR l2.[AddrName] LIKE '%CROSSDOCK%' THEN 1 --Inbound loads
	--		WHEN l1.[AddrName] LIKE '%VENTURE%' OR l1.[AddrName] LIKE '%CROSSDOCK%' THEN 3 --Outbound loads 
	--		ELSE 1 -- Direct loads
	--	END
	--) AS [Load Rank]
FROM [OrdHeader] ord
LEFT JOIN [InvoiceOrdHeader] ih ON ord.ordheaderid = ih.ordheaderid
LEFT JOIN [Invoice] i ON ih.invoiceid = i.invoiceid
LEFT JOIN [InvoiceCharge] c ON i.[InvoiceId] = c.[InvoiceId]
LEFT JOIN [AccessorialCode] a ON c.[AccessorialCodeId] = a.[AccessorialCodeId]
LEFT JOIN [Loc] ordorig ON ord.[LocIdOrig] = ordorig.[LocId]
LEFT JOIN [State] ordorigst ON ordorig.[StateId] = ordorigst.[StateId]
LEFT JOIN [Loc] orddest ON ord.[LocIdDest] = orddest.[LocId]
LEFT JOIN [State] orddestst ON orddest.[StateId] = orddestst.[StateId]
LEFT JOIN [OrdLeg] ol ON ord.[OrdHeaderId] = ol.[OrdHeaderId]
LEFT JOIN [TradingPartner] clt ON ord.[TradingPartnerIdClient] = clt.[TradingPartnerID] AND clt.[DataValue_TradingPartnerType] = 'Client'
LEFT JOIN [Vw3g_OrdLeg_LoadFirstLeg] fl ON ol.[OrdLegId] = fl.[OrdLegId]
LEFT JOIN [Load] ld ON fl.[LoadIdFirstLeg] = ld.[LoadId]
LEFT JOIN [TransportMode] tm ON ld.[TransportModeId] = tm.[TransportModeId]
--LEFT JOIN [FreightBill] f ON ld.[LoadId] = f.[LoadId] AND f.[DataValue_FreightBillType] = 'Original'
LEFT JOIN [TradingPartner] car ON ld.[TradingPartnerIdCarrier] = car.[TradingPartnerId] AND car.[DataValue_TradingPartnerType] = 'Carrier'
LEFT JOIN [Loc] l1 ON ol.[LocIdOrig] = l1.[LocId]
LEFT JOIN [State] st1 ON l1.[StateId] = st1.[StateId]
LEFT JOIN [Loc] l2 ON ol.[LocIdDest] = l2.[LocId]
LEFT JOIN [State] st2 ON l2.[StateId] = st2.[StateId]
WHERE
	clt.TradingPartnerNum = 'HUT' --'395116'
	AND ld.[DataValue_LoadTmsStatus] NOT IN ('TenderExpired', 'Canceled') --Exclude expired tenders and canceled loads.
	--ord.DataValue_OrdTmsStatus IN ('PartiallyPlanned')
	--AND c.[DataValue_CostType] = 'Accessorial'
	--AND a.[AccCodeDesc] = 'TONU'
--AND ord.[OrdHeaderId] = '1817824'
--AND ord.[OrdNum] = 'SH0500257'
--AND ld.[LoadNum] IN ('L240806-56880', 'L240807-56914')
--ORDER BY [DateLastModified] DESC
