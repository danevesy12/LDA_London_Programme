USE [NHSE_Sandbox_Lon_LDAProgramme]
GO

/****** Object:  StoredProcedure [dbo].[SSRS_TransformingCare_MasterDashboard]    Script Date: 17/08/2022 21:13:01 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO






CREATE PROCEDURE [dbo].[SSRS_TransformingCare_MasterDashboard] @ReportingDate datetime 
as	
BEGIN

  SET NOCOUNT ON;
  
  declare @ICStable table ( dashboard varchar(50) , CommissioningArrangement varchar(50), PatientGroup varchar(50) , ICS varchar(50) , [PatientCategory] varchar(100) ) 
  declare @datetable table ( TableName varchar(50) , ColumnNameOld varchar(50), ColumnNameNew varchar(50) ) 
 
  declare @ColumnNameOld varchar(50)
  declare @ColumnNameNew varchar(50)
  declare @ColumnNameFormatted varchar(50)
 
  declare @unitsremoved int
  declare @QuaterNumber int 
  declare @FYMaster datetime
  declare @QuaterStart datetime 
  declare @QuaterCurrent varchar(13)  
  
  declare @ICS varchar(50) 
  declare @TableName varchar(50) 
  declare @CCGorSC varchar(50) 
  declare @PatientGroup varchar(50) 
  declare @PatientCategory varchar(500)
  declare @SQl varchar(8000) 
   
  delete from [NHSE_Sandbox_Lon_LDAProgramme].[dbo].[TransformingCare_MasterDashboard]  --Change

  insert into @ICStable 
  select  dashboard  ,[CommissioningArrangement] , PatientGroup , z.ICS , [PatientCategory] from [TransformingCare_MasterDashboard_Status]  --Change
  cross apply( select ICS from [NHSE_Sandbox_Lon_LDAProgramme].[dbo].[TransformingCare_tblMetric] where GroupingLevel = 1
  group by ICS ) z
  group by dashboard , [CommissioningArrangement] , PatientGroup , z.ICS , [PatientCategory]

  WHILE exists( select top(1) * from @ICStable) 
    BEGIN	
	
	set @ICS = (select top(1) ics from @ICStable)	
    set @TableName = (select top(1) dashboard from @ICStable)
    set @CCGorSC = (select top(1) [CommissioningArrangement] from @ICStable)
    set @PatientGroup = (select top(1) PatientGroup from @ICStable)
	set @PatientCategory = (select top(1) [PatientCategory] from @ICStable) 

	insert into [dbo].[TransformingCare_MasterDashboard]	
	select 	
	@ICS as [ICS] 	
	, MasterTable.OrderNumber
	,iif( MonthTwentyFive.[Value]  is null , 0 , MonthTwentyFive.[Value] ) as MonthTwentyFive
	,iif( MonthTwentyFour.[Value]  is null , 0 , MonthTwentyFour.[Value] ) as MonthTwentyFour
	,iif( MonthTwentyThree.[Value]  is null , 0 , MonthTwentyThree.[Value] ) as MonthTwentyThree
	,iif( MonthTwentyTwo.[Value]  is null , 0 , MonthTwentyTwo.[Value] ) as MonthTwentyTwo
	,iif( MonthTwentyOne.[Value]  is null , 0 , MonthTwentyOne.[Value] ) as MonthTwentyOne
	,iif( MonthTwenty.[Value]  is null , 0 , MonthTwenty.[Value] ) as MonthTwenty
	,iif( MonthNineTeen.[Value]  is null , 0 , MonthNineTeen.[Value] ) as MonthNineTeen
	,iif( MonthEightTeen.[Value]  is null , 0 , MonthEightTeen.[Value] ) as MonthEightTeen
	,iif( MonthSevenTeen.[Value]  is null , 0 , MonthSevenTeen.[Value] ) as MonthSevenTeen
	,iif( MonthSixTeen.[Value]  is null , 0 , MonthSixTeen.[Value] ) as MonthSixTeen
	,iif( Monthfifteen.[Value]  is null , 0 , Monthfifteen.[Value] ) as Monthfifteen
	,iif( MonthFourteen.[Value]  is null , 0 , MonthFourteen.[Value] ) as MonthFourteen
	,iif( Monththirteen.[Value]  is null , 0 , Monththirteen.[Value] ) as Monththirteen	
	,iif( MonthTwelve.[Value]  is null , 0 , MonthTwelve.[Value] ) as MonthTwelve	
	,iif( MonthEleven.[Value]  is null , 0 , MonthEleven.[Value] ) as MonthEleven	
	,iif( MonthTen.[Value]  is null , 0 , MonthTen.[Value] ) as MonthTen	
	,iif( MonthNine.[Value]  is null , 0 , MonthNine.[Value] ) as MonthNine
	,iif( MonthEight.[Value]  is null , 0 , MonthEight.[Value] ) as MonthEight	
	,iif( MonthSeven.[Value]  is null , 0 , MonthSeven.[Value] ) as MonthSeven	
	,iif( MonthSix.[Value]  is null , 0 , MonthSix.[Value] ) as MonthSix	
	,iif( MonthFive.[Value]  is null , 0 , MonthFive.[Value] ) as MonthFive
	,iif( MonthFour.[Value]  is null , 0 , MonthFour.[Value] ) as MonthFour
	,iif( MonthThree.[Value]  is null , 0 , MonthThree.[Value] ) as MonthThree	
	,iif( MonthTwo.[Value]  is null , 0 , MonthTwo.[Value] ) as MonthTwo	
	,iif( MonthOne.[Value]  is null , 0 , MonthOne.[Value] ) as MonthOne	
	,iif( MonthZero.[Value]  is null , 0 , MonthZero.[Value] ) as [MonthZero]		
	,iif( MonthPlusOne.[Value]  is null , 0 , MonthPlusOne.[Value] ) as MonthPlusOne
	,iif( MonthPlusTwo.[Value]  is null , 0 , MonthPlusTwo.[Value] ) as MonthPlusTwo
	,iif( MonthPlusThree.[Value]  is null , 0 , MonthPlusThree.[Value] ) as MonthPlusThree	
	,iif( MonthPlusFour.[Value]  is null , 0 , MonthPlusFour.[Value] ) as MonthPlusFour	
	,iif( MonthPlusFive.[Value]  is null , 0 , MonthPlusFive.[Value] ) as MonthPlusFive	
	,iif (  FirstQuater.FirstQuater	 is null , 0 , FirstQuater.FirstQuater			) as QuaterOne 
	,iif (  SeccondQuater.SeccondQuater is null , 0 , SeccondQuater.SeccondQuater		) as QuaterTwo	
	,iif (  ThirdQuater.ThirdQuater	 is null , 0 , ThirdQuater.ThirdQuater			) as QuaterThree	
	,iif (  FourthQuater.FourthQuater 	 is null , 0 , FourthQuater.FourthQuater 		) as QuaterFour 	
	,iif ( 	Hold_1.Hold_1						is	null , 0 , 		Hold_1.Hold_1					 		) as  Hold_1	
	,iif (	Hold_2.Hold_2						is	null , 0 , 		Hold_2.Hold_2					 		) as  Hold_2	
	,iif (  ThreeMonthRA.ThreeMonthRA	 is null , 0 , ThreeMonthRA.ThreeMonthRA		) as ThreeMonthRA	
	,iif (  TwelveMonthRA.TwelveMonthRA is null , 0 , TwelveMonthRA.TwelveMonthRA		) as TwelveMonthRA	
	,iif( FYZero.[Value]  is null , 0 , FYZero.[Value] ) as FYZero	
	,iif( FYOne.[Value]  is null , 0 , FYOne.[Value] ) as FYOne	
	,iif( FYTwo.[Value]  is null , 0 , FYTwo.[Value] ) as FYTwo	
	,iif( FYThree.[Value]  is null , 0 , FYThree.[Value] ) as FYThree
	, MasterTable.OrderNumber
	, @CCGorSC as [CommissioningArrangement]
	, @PatientGroup as [PatientGroup]
	, MasterTable.GroupID
	, @PatientCategory
	from [dbo].[TransformingCare_MasterDashboard_Status] as MasterTable  
	left join [dbo].[TransformingCare_ViewAllData] as MonthPlusFive		on MonthPlusFive.OrderNumber = MasterTable.OrderNumber			and  MonthPlusFive.ReportingMonth   = dateadd( month , 5 ,@ReportingDate )		and MonthPlusFive.ICS = @ICS	and MonthPlusFive.[CommissioningArrangement] = @CCGorSC		and MonthPlusFive.PatientGroup = @PatientGroup		and MonthPlusFive.[PatientCategory] = @PatientCategory		and  MonthPlusFive.GroupingLevel = 1	
	left join [dbo].[TransformingCare_ViewAllData] as MonthPlusFour		on MonthPlusFour.OrderNumber = MasterTable.OrderNumber			and  MonthPlusFour.ReportingMonth    = dateadd( month , 4 ,@ReportingDate )		and MonthPlusFour.ICS = @ICS	and MonthPlusFour.[CommissioningArrangement] = @CCGorSC		and MonthPlusFour.PatientGroup = @PatientGroup		and MonthPlusFour.[PatientCategory] = @PatientCategory		and  MonthPlusFour.GroupingLevel = 1 
	left join [dbo].[TransformingCare_ViewAllData] as MonthPlusThree	on MonthPlusThree.OrderNumber = MasterTable.OrderNumber			and  MonthPlusThree.ReportingMonth    = dateadd( month , 3 ,@ReportingDate )	and MonthPlusThree.ICS = @ICS	and MonthPlusThree.[CommissioningArrangement] = @CCGorSC	and MonthPlusThree.PatientGroup = @PatientGroup		and MonthPlusThree.[PatientCategory] = @PatientCategory		and  MonthPlusThree.GroupingLevel = 1 
	left join [dbo].[TransformingCare_ViewAllData] as MonthPlusTwo		on MonthPlusTwo.OrderNumber = MasterTable.OrderNumber			and  MonthPlusTwo.ReportingMonth  = dateadd( month , 2 ,@ReportingDate )		and MonthPlusTwo.ICS = @ICS		and MonthPlusTwo.[CommissioningArrangement] = @CCGorSC		and MonthPlusTwo.PatientGroup = @PatientGroup		and MonthPlusTwo.[PatientCategory] = @PatientCategory		and  MonthPlusTwo.GroupingLevel = 1 
	left join [dbo].[TransformingCare_ViewAllData] as MonthPlusOne		on MonthPlusOne.OrderNumber = MasterTable.OrderNumber			and  MonthPlusOne.ReportingMonth   = dateadd( month , 1 ,@ReportingDate )		and MonthPlusOne.ICS = @ICS		and MonthPlusOne.[CommissioningArrangement] = @CCGorSC		and MonthPlusOne.PatientGroup = @PatientGroup		and MonthPlusOne.[PatientCategory] = @PatientCategory		and  MonthPlusOne.GroupingLevel = 1 																																																																																										
	left join [dbo].[TransformingCare_ViewAllData] as MonthZero			on MonthZero.OrderNumber = MasterTable.OrderNumber				and  MonthZero.ReportingMonth   = dateadd( month , 0 ,@ReportingDate )			and MonthZero.ICS = @ICS		and MonthZero.[CommissioningArrangement] = @CCGorSC			and MonthZero.PatientGroup = @PatientGroup			and MonthZero.[PatientCategory] = @PatientCategory			and  MonthZero.GroupingLevel = 1 
	left join [dbo].[TransformingCare_ViewAllData] as MonthOne			on MonthOne.OrderNumber = MasterTable.OrderNumber				and  MonthOne.ReportingMonth    = dateadd( month , -1 ,@ReportingDate )			and MonthOne.ICS = @ICS			and MonthOne.[CommissioningArrangement] = @CCGorSC			and MonthOne.PatientGroup = @PatientGroup			and MonthOne.[PatientCategory] = @PatientCategory			and  MonthOne.GroupingLevel = 1 
	left join [dbo].[TransformingCare_ViewAllData] as MonthTwo			on MonthTwo.OrderNumber = MasterTable.OrderNumber				and  MonthTwo.ReportingMonth    = dateadd( month , -2 ,@ReportingDate )			and MonthTwo.ICS = @ICS			and MonthTwo.[CommissioningArrangement] = @CCGorSC			and MonthTwo.PatientGroup = @PatientGroup			and MonthTwo.[PatientCategory] = @PatientCategory			and  MonthTwo.GroupingLevel = 1 
	left join [dbo].[TransformingCare_ViewAllData] as MonthThree		on MonthThree.OrderNumber = MasterTable.OrderNumber				and  MonthThree.ReportingMonth  = dateadd( month , -3 ,@ReportingDate )			and MonthThree.ICS = @ICS		and MonthThree.[CommissioningArrangement] = @CCGorSC		and MonthThree.PatientGroup = @PatientGroup			and MonthThree.[PatientCategory] = @PatientCategory			and  MonthThree.GroupingLevel = 1 
	left join [dbo].[TransformingCare_ViewAllData] as MonthFour			on MonthFour.OrderNumber = MasterTable.OrderNumber				and  MonthFour.ReportingMonth   = dateadd( month , -4 ,@ReportingDate )			and MonthFour.ICS = @ICS		and MonthFour.[CommissioningArrangement] = @CCGorSC			and MonthFour.PatientGroup = @PatientGroup			and MonthFour.[PatientCategory] = @PatientCategory			and  MonthFour.GroupingLevel = 1 
	left join [dbo].[TransformingCare_ViewAllData] as MonthFive			on MonthFive.OrderNumber = MasterTable.OrderNumber				and  MonthFive.ReportingMonth   = dateadd( month , -5 ,@ReportingDate )			and MonthFive.ICS = @ICS		and MonthFive.[CommissioningArrangement] = @CCGorSC			and MonthFive.PatientGroup = @PatientGroup			and MonthFive.[PatientCategory] = @PatientCategory			and  MonthFive.GroupingLevel = 1 
	left join [dbo].[TransformingCare_ViewAllData] as MonthSix			on MonthSix.OrderNumber = MasterTable.OrderNumber				and  MonthSix.ReportingMonth    = dateadd( month , -6 ,@ReportingDate )			and MonthSix.ICS = @ICS			and MonthSix.[CommissioningArrangement] = @CCGorSC			and MonthSix.PatientGroup = @PatientGroup			and MonthSix.[PatientCategory] = @PatientCategory			and  MonthSix.GroupingLevel = 1 
	left join [dbo].[TransformingCare_ViewAllData] as MonthSeven		on MonthSeven.OrderNumber = MasterTable.OrderNumber				and  MonthSeven.ReportingMonth  = dateadd( month , -7 ,@ReportingDate )			and MonthSeven.ICS = @ICS		and MonthSeven.[CommissioningArrangement] = @CCGorSC		and MonthSeven.PatientGroup = @PatientGroup			and MonthSeven.[PatientCategory] = @PatientCategory			and  MonthSeven.GroupingLevel = 1 
	left join [dbo].[TransformingCare_ViewAllData] as MonthEight		on MonthEight.OrderNumber = MasterTable.OrderNumber				and  MonthEight.ReportingMonth  = dateadd( month , -8 ,@ReportingDate )			and MonthEight.ICS = @ICS		and MonthEight.[CommissioningArrangement] = @CCGorSC		and MonthEight.PatientGroup = @PatientGroup			and MonthEight.[PatientCategory] = @PatientCategory			and  MonthEight.GroupingLevel = 1 
	left join [dbo].[TransformingCare_ViewAllData] as MonthNine			on MonthNine.OrderNumber = MasterTable.OrderNumber				and  MonthNine.ReportingMonth   = dateadd( month , -9 ,@ReportingDate )			and MonthNine.ICS = @ICS		and MonthNine.[CommissioningArrangement] = @CCGorSC			and MonthNine.PatientGroup = @PatientGroup			and MonthNine.[PatientCategory] = @PatientCategory			and  MonthNine.GroupingLevel = 1 
	left join [dbo].[TransformingCare_ViewAllData] as MonthTen			on MonthTen.OrderNumber = MasterTable.OrderNumber				and  MonthTen.ReportingMonth    = dateadd( month , -10 ,@ReportingDate )		and MonthTen.ICS = @ICS			and MonthTen.[CommissioningArrangement] = @CCGorSC			and MonthTen.PatientGroup = @PatientGroup			and MonthTen.[PatientCategory] = @PatientCategory			and  MonthTen.GroupingLevel = 1 
	left join [dbo].[TransformingCare_ViewAllData] as MonthEleven		on MonthEleven.OrderNumber = MasterTable.OrderNumber			and  MonthEleven.ReportingMonth = dateadd( month , -11 ,@ReportingDate )		and MonthEleven.ICS = @ICS		and MonthEleven.[CommissioningArrangement] = @CCGorSC		and MonthEleven.PatientGroup = @PatientGroup		and MonthEleven.[PatientCategory] = @PatientCategory		and  MonthEleven.GroupingLevel = 1 
	left join [dbo].[TransformingCare_ViewAllData] as MonthTwelve		on MonthTwelve.OrderNumber = MasterTable.OrderNumber			and  MonthTwelve.ReportingMonth = dateadd( month , -12 ,@ReportingDate )		and MonthTwelve.ICS = @ICS		and MonthTwelve.[CommissioningArrangement] = @CCGorSC		and MonthTwelve.PatientGroup = @PatientGroup		and MonthTwelve.[PatientCategory] = @PatientCategory		and  MonthTwelve.GroupingLevel = 1 
	left join [dbo].[TransformingCare_ViewAllData] as Monththirteen		on Monththirteen.OrderNumber = MasterTable.OrderNumber			and  Monththirteen.ReportingMonth = dateadd( month , -13 ,@ReportingDate )		and Monththirteen.ICS = @ICS	and Monththirteen.[CommissioningArrangement] = @CCGorSC		and Monththirteen.PatientGroup = @PatientGroup		and Monththirteen.[PatientCategory] = @PatientCategory		and  Monththirteen.GroupingLevel = 1 
	left join [dbo].[TransformingCare_ViewAllData] as MonthFourteen		on MonthFourteen.OrderNumber = MasterTable.OrderNumber			and  MonthFourteen.ReportingMonth = dateadd( month , -14 ,@ReportingDate )		and MonthFourteen.ICS = @ICS	and MonthFourteen.[CommissioningArrangement] = @CCGorSC		and MonthFourteen.PatientGroup = @PatientGroup		and MonthFourteen.[PatientCategory] = @PatientCategory		and  MonthFourteen.GroupingLevel = 1 
	left join [dbo].[TransformingCare_ViewAllData] as Monthfifteen		on Monthfifteen.OrderNumber = MasterTable.OrderNumber			and  Monthfifteen.ReportingMonth = dateadd( month , -15 ,@ReportingDate )		and Monthfifteen.ICS = @ICS		and Monthfifteen.[CommissioningArrangement] = @CCGorSC		and Monthfifteen.PatientGroup = @PatientGroup		and Monthfifteen.[PatientCategory] = @PatientCategory		and  Monthfifteen.GroupingLevel = 1 
	left join [dbo].[TransformingCare_ViewAllData] as MonthSixTeen		on MonthSixTeen.OrderNumber = MasterTable.OrderNumber			and  MonthSixTeen.ReportingMonth = dateadd( month , -16 ,@ReportingDate )		and MonthSixTeen.ICS = @ICS		and MonthSixTeen.[CommissioningArrangement] = @CCGorSC		and MonthSixTeen.PatientGroup = @PatientGroup		and MonthSixTeen.[PatientCategory] = @PatientCategory		and  MonthSixTeen.GroupingLevel = 1 
	left join [dbo].[TransformingCare_ViewAllData] as MonthSevenTeen	on MonthSevenTeen.OrderNumber = MasterTable.OrderNumber			and  MonthSevenTeen.ReportingMonth = dateadd( month , -17 ,@ReportingDate )		and MonthSevenTeen.ICS = @ICS	and MonthSevenTeen.[CommissioningArrangement] = @CCGorSC	and MonthSevenTeen.PatientGroup = @PatientGroup		and MonthSevenTeen.[PatientCategory] = @PatientCategory		and  MonthSevenTeen.GroupingLevel = 1 
	left join [dbo].[TransformingCare_ViewAllData] as MonthEightTeen	on MonthEightTeen.OrderNumber = MasterTable.OrderNumber			and  MonthEightTeen.ReportingMonth = dateadd( month , -18 ,@ReportingDate )		and MonthEightTeen.ICS = @ICS	and MonthEightTeen.[CommissioningArrangement] = @CCGorSC	and MonthEightTeen.PatientGroup = @PatientGroup		and MonthEightTeen.[PatientCategory] = @PatientCategory		and  MonthEightTeen.GroupingLevel = 1 
	left join [dbo].[TransformingCare_ViewAllData] as MonthNineTeen		on MonthNineTeen.OrderNumber = MasterTable.OrderNumber			and  MonthNineTeen.ReportingMonth = dateadd( month , -19 ,@ReportingDate )		and MonthNineTeen.ICS = @ICS	and MonthNineTeen.[CommissioningArrangement] = @CCGorSC		and MonthNineTeen.PatientGroup = @PatientGroup		and MonthNineTeen.[PatientCategory] = @PatientCategory		and  MonthNineTeen.GroupingLevel = 1 
	left join [dbo].[TransformingCare_ViewAllData] as MonthTwenty		on MonthTwenty.OrderNumber = MasterTable.OrderNumber			and  MonthTwenty.ReportingMonth = dateadd( month , -20 ,@ReportingDate )		and MonthTwenty.ICS = @ICS		and MonthTwenty.[CommissioningArrangement] = @CCGorSC		and MonthTwenty.PatientGroup = @PatientGroup		and MonthTwenty.[PatientCategory] = @PatientCategory		and  MonthTwenty.GroupingLevel = 1 
	left join [dbo].[TransformingCare_ViewAllData] as MonthTwentyOne	on MonthTwentyOne.OrderNumber = MasterTable.OrderNumber			and  MonthTwentyOne.ReportingMonth = dateadd( month , -21 ,@ReportingDate )		and MonthTwentyOne.ICS = @ICS	and MonthTwentyOne.[CommissioningArrangement] = @CCGorSC	and MonthTwentyOne.PatientGroup = @PatientGroup		and MonthTwentyOne.[PatientCategory] = @PatientCategory		and  MonthTwentyOne.GroupingLevel = 1 
	left join [dbo].[TransformingCare_ViewAllData] as MonthTwentyTwo	on MonthTwentyTwo.OrderNumber = MasterTable.OrderNumber			and  MonthTwentyTwo.ReportingMonth = dateadd( month , -22 ,@ReportingDate )		and MonthTwentyTwo.ICS = @ICS	and MonthTwentyTwo.[CommissioningArrangement] = @CCGorSC	and MonthTwentyTwo.PatientGroup = @PatientGroup		and MonthTwentyTwo.[PatientCategory] = @PatientCategory		and  MonthTwentyTwo.GroupingLevel = 1 
	left join [dbo].[TransformingCare_ViewAllData] as MonthTwentyThree	on MonthTwentyThree.OrderNumber = MasterTable.OrderNumber		and  MonthTwentyThree.ReportingMonth = dateadd( month , -23 ,@ReportingDate )	and MonthTwentyThree.ICS = @ICS and MonthTwentyThree.[CommissioningArrangement] = @CCGorSC	and MonthTwentyThree.PatientGroup = @PatientGroup	and MonthTwentyThree.[PatientCategory] = @PatientCategory	and  MonthTwentyThree.GroupingLevel = 1 
	left join [dbo].[TransformingCare_ViewAllData] as MonthTwentyFour	on MonthTwentyFour.OrderNumber = MasterTable.OrderNumber		and  MonthTwentyFour.ReportingMonth = dateadd( month , -24 ,@ReportingDate )	and MonthTwentyFour.ICS = @ICS	and MonthTwentyFour.[CommissioningArrangement] = @CCGorSC	and MonthTwentyFour.PatientGroup = @PatientGroup	and MonthTwentyFour.[PatientCategory] = @PatientCategory	and  MonthTwentyFour.GroupingLevel = 1 
	left join [dbo].[TransformingCare_ViewAllData] as MonthTwentyFive	on MonthTwentyFive.OrderNumber = MasterTable.OrderNumber		and  MonthTwentyFive.ReportingMonth = dateadd( month , -25 ,@ReportingDate )	and MonthTwentyFive.ICS = @ICS	and MonthTwentyFive.[CommissioningArrangement] = @CCGorSC	and MonthTwentyFive.PatientGroup = @PatientGroup	and MonthTwentyFive.[PatientCategory] = @PatientCategory	and  MonthTwentyFive.GroupingLevel = 1 
	
	outer apply (   select dateadd( month , -12 ,max(QuaterStart) ) as QuaterStart
	from [dbo].[TransformingCare_LookupQuater]  where QuaterStart <= @ReportingDate  ) as Quater
	
	outer apply (  select iif (month(@ReportingDate) > 3 , cast(cast(year(@ReportingDate) as varchar(4)) + '-03-01'  as datetime )
	, cast(cast(year(@ReportingDate) -1 as varchar(4)) + '-03-01'  as datetime ))  as FY_Master     ) AS FY_Master
 
	outer apply (	  select ([value]) as FirstQuater from [dbo].[TransformingCare_ViewAllData]  
	where OrderNumber = MasterTable.OrderNumber and  ReportingMonth in (dateadd( month , 0 , quater.QuaterStart)   )  
	and ICS = @ICS and [CommissioningArrangement] = @CCGorSC and PatientGroup = @PatientGroup  and [PatientCategory] = @PatientCategory and GroupingLevel = 1 ) as FirstQuater
	
	outer apply (	  select ([value]) as SeccondQuater from [dbo].[TransformingCare_ViewAllData]  
	where OrderNumber = MasterTable.OrderNumber and  ReportingMonth in (dateadd( month , 3 , quater.QuaterStart)  )  
	and ICS = @ICS and [CommissioningArrangement] = @CCGorSC and PatientGroup = @PatientGroup and [PatientCategory] = @PatientCategory  and GroupingLevel = 1  ) as SeccondQuater
	
	outer apply (	  select ([value]) as ThirdQuater from [dbo].[TransformingCare_ViewAllData]  
	where OrderNumber = MasterTable.OrderNumber and  ReportingMonth in (dateadd( month , 6 , quater.QuaterStart) )  
	and ICS = @ICS and [CommissioningArrangement] = @CCGorSC and PatientGroup = @PatientGroup  and [PatientCategory] = @PatientCategory and GroupingLevel = 1  ) as ThirdQuater
	
	outer apply (	  select ([value]) as FourthQuater from [dbo].[TransformingCare_ViewAllData]  
	where OrderNumber = MasterTable.OrderNumber and  ReportingMonth in (dateadd( month , 9 , quater.QuaterStart)   )  
	and ICS = @ICS and [CommissioningArrangement] = @CCGorSC and PatientGroup = @PatientGroup and [PatientCategory] = @PatientCategory and GroupingLevel = 1   ) as FourthQuater
	
	outer apply (	  select sum([value])/3 as ThreeMonthRA from [dbo].[TransformingCare_ViewAllData]  
	where OrderNumber = MasterTable.OrderNumber and  ReportingMonth >= dateadd( month , -2 ,@ReportingDate )  
	and ICS = @ICS and [CommissioningArrangement] = @CCGorSC and PatientGroup = @PatientGroup and [PatientCategory] = @PatientCategory  and GroupingLevel = 1  ) as ThreeMonthRA
	
	outer apply (	  select sum([value])/12 as TwelveMonthRA from [dbo].[TransformingCare_ViewAllData]  
	where OrderNumber = MasterTable.OrderNumber and  ReportingMonth >= dateadd( month , -11 ,@ReportingDate )  
	and ICS = @ICS and [CommissioningArrangement] = @CCGorSC and PatientGroup = @PatientGroup and [PatientCategory] = @PatientCategory and GroupingLevel = 1   ) as TwelveMonthRA

	outer apply (	  select ([value]) as [value] from [dbo].[TransformingCare_ViewAllData]  
	where OrderNumber = MasterTable.OrderNumber and  ReportingMonth = dateadd( year , 0 ,FY_Master.FY_Master )  
	and ICS = @ICS and [CommissioningArrangement] = @CCGorSC and PatientGroup = @PatientGroup  and [PatientCategory] = @PatientCategory  and GroupingLevel = 1 ) as FYZero
	
	outer apply (	  select ([value]) as [value] from [dbo].[TransformingCare_ViewAllData]  
	where OrderNumber = MasterTable.OrderNumber and  ReportingMonth = dateadd( year , -1 ,FY_Master.FY_Master )  
	and ICS = @ICS and [CommissioningArrangement] = @CCGorSC and PatientGroup = @PatientGroup  and [PatientCategory] = @PatientCategory and GroupingLevel = 1  ) as FYOne
	
	outer apply (	  select ([value]) as [value] from [dbo].[TransformingCare_ViewAllData]  
	where OrderNumber = MasterTable.OrderNumber and  ReportingMonth = dateadd( year , -2 ,FY_Master.FY_Master )  
	and ICS = @ICS and [CommissioningArrangement] = @CCGorSC and PatientGroup = @PatientGroup and [PatientCategory] = @PatientCategory   and GroupingLevel = 1 ) as FYTwo
	
	outer apply (	  select ([value]) as [value] from [dbo].[TransformingCare_ViewAllData]  
	where OrderNumber = MasterTable.OrderNumber and  ReportingMonth = dateadd( year , -3 ,FY_Master.FY_Master )  
	and ICS = @ICS and [CommissioningArrangement] = @CCGorSC and PatientGroup = @PatientGroup and [PatientCategory] = @PatientCategory   and GroupingLevel = 1 ) as FYThree

	outer apply (	  select sum([value]) as Hold_1  from [dbo].[TransformingCare_ViewAllData]  
	where OrderNumber = MasterTable.OrderNumber and  ReportingMonth >= FY_Master.FY_Master  and   ReportingMonth < @ReportingDate
	and ICS = @ICS and [CommissioningArrangement] = @CCGorSC and PatientGroup = @PatientGroup  and [PatientCategory] = @PatientCategory and GroupingLevel = 1 ) as Hold_1 --Current YTD

	outer apply (	  select sum([value]) as Hold_2  from [dbo].[TransformingCare_ViewAllData]  
	where OrderNumber = MasterTable.OrderNumber and  ReportingMonth > dateadd( year , - 1 , FY_Master.FY_Master )  and   ReportingMonth <= dateadd( year , 0 , FY_Master.FY_Master ) 
	and ICS = @ICS and [CommissioningArrangement] = @CCGorSC and PatientGroup = @PatientGroup  and [PatientCategory] = @PatientCategory and GroupingLevel = 1 ) as Hold_2 --Current YTD

	where MasterTable.dashboard = @TableName and MasterTable.[status] = 1 and MasterTable.PatientCategory = @PatientCategory
	order by MasterTable.OrderNumber   
	
  delete top(1) @ICStable
  END;
    
  set @QuaterStart = (   select dateadd( month , -12 ,max(QuaterStart) ) as QuaterStart  from [dbo].[TransformingCare_LookupQuater]  where QuaterStart <= @ReportingDate  ) 
  set @FYMaster = (   select iif (month(@ReportingDate) > 3 , cast(cast(year(@ReportingDate) as varchar(4)) + '-03-01'  as datetime )  , cast(cast(year(@ReportingDate) -1 as varchar(4)) + '-03-01'  as datetime ))  ) 
  set @QuaterNumber = ( select QuaterNumber from [dbo].[TransformingCare_LookupQuater] where QuaterStart = @QuaterStart ) 

  delete from [dbo].[TransformingCare_LookupDates]
  
  insert into [dbo].[TransformingCare_LookupDates]
  select COLUMN_NAME , 
	case 
	when COLUMN_NAME like 'Month%' then    cast(  format(dateadd( month , UnitsRemoved , @reportingDate) , 'MMM' )    as varchar(13)   ) + '-' + cast(  right(year(  dateadd( month , UnitsRemoved , @reportingDate) ) ,2) as varchar(2)  )
	when COLUMN_NAME like 'Quater%' then (select FY + '-' + quater from [TransformingCare_LookupQuater] where quaternumber = (@QuaterNumber + UnitsRemoved ))
	when COLUMN_NAME like 'FY%'   then cast( year(dateadd( year  , UnitsRemoved -1 , @FYMaster)) as varchar(13))   + '/' + right(cast( year(dateadd( year  , UnitsRemoved  , @FYMaster)) as varchar(13)),2) 
	else '' 	end as [ColumnDateName]
  from [dbo].[TransformingCare_MasterDashboard_ColumnsMaster] where UnitsRemoved is not null  


 -- insert into @datetable 
 -- select a.TABLE_NAME , a.COLUMN_NAME , b.COLUMN_NAME
 -- from INFORMATION_SCHEMA.COLUMNS A 
 -- inner join [dbo].[TransformingCare_MasterDashboard_ColumnsMaster] B on a.TABLE_NAME = b.TABLE_NAME and a.ORDINAL_POSITION = b.ORDINAL_POSITION
 -- where  a.ORDINAL_POSITION <> 1  and a.ORDINAL_POSITION <> 2 and a.ORDINAL_POSITION <> 46 and a.ORDINAL_POSITION <> 47 and a.ORDINAL_POSITION <> 48 and a.ORDINAL_POSITION <> 49
 -- order by a.ORDINAL_POSITION
 
 -- WHILE exists( select top(1) * from @datetable) 
 -- BEGIN	
 -- 		set @ColumnNameOld =  (select top(1) ColumnNameOld from @datetable ) 
 -- 		set @ColumnNameNew =  (select top(1) ColumnNameNew from @datetable ) 
 -- 		set @SQl = 'exec sp_rename ' + '''' + 'TransformingCare_MasterDashboard'  + '.' + @ColumnNameOld  + '''' + ' , ' + '''' + @ColumnNameNew + '''' + ' , ''' + 'COLUMN' + ''' ;'
 -- 		exec(@SQL)
 -- 
 -- 		set @ColumnNameFormatted = @ColumnNameNew
 -- 
 -- 		set @unitsremoved = ( select unitsremoved from [dbo].[TransformingCare_MasterDashboard_ColumnsMaster] where column_name = @ColumnNameNew  ) 
 -- 		set @unitsremoved = iif ( @unitsremoved is null , 0 , @unitsremoved ) 
 -- 		set @QuaterNumber = ( select QuaterNumber from [dbo].[TransformingCare_LookupQuater] where QuaterStart = @QuaterStart ) 
 -- 				
 -- 		set @QuaterCurrent = (select FY + '-' + quater from [TransformingCare_LookupQuater] where quaternumber = (@QuaterNumber + @unitsremoved ))
 -- 
 -- 		set @ColumnNameFormatted = (  iif( @ColumnNameNew like 'Month%'  , cast( year(  dateadd( month , @unitsremoved , @reportingDate) ) as varchar(4)  ) + '-' + cast(  format(dateadd( month , @unitsremoved , @reportingDate) , 'MMM' )    as varchar(13)   ) , @ColumnNameFormatted )  ) 
 -- 		set @ColumnNameFormatted = (  iif( @ColumnNameNew like 'Quater%' , @QuaterCurrent , @ColumnNameFormatted )  ) 
 -- 		set @ColumnNameFormatted = (  iif( @ColumnNameNew like 'FY%'     , cast( year(dateadd( year  , @unitsremoved , @FYMaster)) as varchar(13))   + '/' + right(cast( year(dateadd( year  , @unitsremoved -1 , @FYMaster)) as varchar(13)),2) , @ColumnNameFormatted )  ) 
 -- 		   		
 -- 		set @SQl = 'exec sp_rename ' + '''' + 'TransformingCare_MasterDashboard'  + '.' + @ColumnNameNew  + '''' + ' , ' + '''' +  @ColumnNameFormatted + '''' + ' , ''' + 'COLUMN' + ''' ;'
 -- 		exec(@SQL)
 -- 
 -- 		delete top(1) @datetable
 -- END;




END
GO


