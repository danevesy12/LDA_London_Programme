USE [NHSE_Sandbox_Lon_LDAProgramme]
GO

/****** Object:  StoredProcedure [dbo].[SSRS_TransformingCare_MasterDashboard_SupplementaryReport_AHC]    Script Date: 17/08/2022 21:16:14 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO




CREATE PROCEDURE [dbo].[SSRS_TransformingCare_MasterDashboard_SupplementaryReport_AHC] @ReportingDate datetime 
as	
BEGIN

  SET NOCOUNT ON;

	delete from [TransformingCare_MasterDashboard_SupplementaryReport_AHC] 

	CREATE TABLE #TransformingCare_tblMetricAHC_BackTest (
	[Region] [varchar](100) NOT NULL,
	[ICS] [varchar](100) NOT NULL,
	[ReportingMonth] [datetime] NOT NULL,
	[MetricID] int NOT NULL,
	[Value] decimal(18,5) NULL,
	[CommissioningArrangement] [varchar](20) NOT NULL,
	[PatientGroup] [varchar](25) NOT NULL,
	PatientCategory [varchar](100) NOT NULL	)

	insert into #TransformingCare_tblMetricAHC_BackTest
	select 'London' , [ICS] , ReportingMonth , MetricID , sum([value]) , [CommissioningArrangement], [PatientGroup] , [PatientCategory] from [dbo].[TransformingCare_tblMetric_AHC] A 
	inner join [dbo].[TransformingCare_LookupPracriceCode] B on a.PRACTICE_CODE = b.PRACTICE_CODE 
	group by [ICS] , ReportingMonth , MetricID, [CommissioningArrangement], [PatientGroup] , [PatientCategory]
	order by [ICS] , ReportingMonth , MetricID, [CommissioningArrangement], [PatientGroup] , [PatientCategory]

	insert into #TransformingCare_tblMetricAHC_BackTest
	select 'England' , 'England' , ReportingMonth , MetricID , sum([value]) , [CommissioningArrangement], [PatientGroup] , [PatientCategory] 
	from [dbo].[TransformingCare_tblMetric_AHC] A 
	group by  ReportingMonth , MetricID, [CommissioningArrangement], [PatientGroup] , [PatientCategory]
	order by  ReportingMonth , MetricID, [CommissioningArrangement], [PatientGroup] , [PatientCategory]
	
	insert into #TransformingCare_tblMetricAHC_BackTest
	select a.Region, A.[ICS] , a.ReportingMonth , 4 , b.[value] / a.[value]  , a.[CommissioningArrangement] ,A.[PatientGroup], a.PatientCategory 
	from #TransformingCare_tblMetricAHC_BackTest A
	inner join #TransformingCare_tblMetricAHC_BackTest B on A.[PatientGroup] = b.[PatientGroup] and A.[ICS] = b.[ICS] and A.ReportingMonth = b.ReportingMonth
	where a.MetricID = 1 and b.MetricID = 2
	
	insert into #TransformingCare_tblMetricAHC_BackTest
	select  a.Region, A.[ICS] , a.ReportingMonth , 5 , b.[value] / a.[value]  , a.[CommissioningArrangement] ,A.[PatientGroup], a.PatientCategory 
	from #TransformingCare_tblMetricAHC_BackTest A
	inner join #TransformingCare_tblMetricAHC_BackTest B on A.[PatientGroup] = b.[PatientGroup] and A.[ICS] = b.[ICS] and A.ReportingMonth = b.ReportingMonth
	where a.MetricID = 1 and b.MetricID = 3
	
	declare @SQLSubstring varchar(8000) 
	declare @SQL varchar(8000) 

	declare @LastDate Datetime 	
	
	set @LastDate = iif ( month(@ReportingDate) > 3 ,cast(year(@ReportingDate) + 1  as varchar(4))  + '-03-01' , cast( year(@ReportingDate) as varchar(4)) + '-03-01') 
	
	set @SQLSubstring = ( select  string_agg('[' +  cast(year(dateadd( month , UnitsRemoved , @LastDate )) as varchar(4)) + '-' +  cast(month(dateadd( month , UnitsRemoved , @LastDate )) as varchar(2)) + '-01] , ' , '' )   from [dbo].[TransformingCare_MasterDashboard_ColumnsMaster_Alternative] where [column_name] like 'Month%' group by [TABLE_NAME] )
   
	set @SQL = 'insert into [dbo].[TransformingCare_MasterDashboard_SupplementaryReport_AHC] 
	select  [Test].* From #TransformingCare_tblMetricAHC_BackTest pivot ( sum([Value] ) for ReportingMonth in ( ' + left(@SQLSubstring , len(@SQLSubstring) - 2 )  + ') ) as [Test] ' 

	--set  @SQL  = '  select  [Test].* From #TransformingCare_tblMetricAHC_BackTest pivot ( sum([Value] ) for ReportingMonth in ( ' + left(@SQLSubstring , len(@SQLSubstring) - 2 )  + ') ) as [Test] ' 
	exec(@SQL)  

  drop table #TransformingCare_tblMetricAHC_BackTest

  delete from [dbo].[TransformingCare_LookupDates_Alternative] 

    insert into [dbo].[TransformingCare_LookupDates_Alternative]
  select COLUMN_NAME , 
	case 
	when COLUMN_NAME like 'Month%' then    cast(  format(dateadd( month , UnitsRemoved , @LastDate) , 'MMM' )    as varchar(13)   ) + '-' + cast(  right(year(  dateadd( month , UnitsRemoved , @LastDate) ) ,2) as varchar(2)  )
	else '' 	end as [ColumnDateName]
  from [dbo].[TransformingCare_MasterDashboard_ColumnsMaster_Alternative] where UnitsRemoved is not null  



END
GO


