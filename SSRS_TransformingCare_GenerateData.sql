USE [NHSE_Sandbox_Lon_LDAProgramme]
GO

/****** Object:  StoredProcedure [dbo].[SSRS_TransformingCare_GenerateData]    Script Date: 17/08/2022 21:05:09 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO




CREATE PROCEDURE [dbo].[SSRS_TransformingCare_GenerateData] @ReportingDate datetime 
as	
BEGIN

  SET NOCOUNT ON;	

	declare @reportingDateHold datetime 
	set @reportingDateHold = @ReportingDate

	declare @datetable table ( ReportingMonth datetime ) 
	declare @datetableDischargePlan table ( ReportingMonth datetime , DischargeMonth datetime ) 
	declare @DischargeMonth datetime 	

	insert into @datetable values ( dateadd(MONTH, 0, @reportingdate) )
	insert into @datetable values ( dateadd(MONTH, -1, @reportingdate) )
	insert into @datetable values ( dateadd(MONTH, -2, @reportingdate) )
	insert into @datetable values ( dateadd(MONTH, -3, @reportingdate) )
	insert into @datetable values ( dateadd(MONTH, -4, @reportingdate) )
	insert into @datetable values ( dateadd(MONTH, -5, @reportingdate) )
	insert into @datetable values ( dateadd(MONTH, -6, @reportingdate) )	
	insert into @datetable values ( dateadd(MONTH, -7, @reportingdate) )
	insert into @datetable values ( dateadd(MONTH, -8, @reportingdate) )
	insert into @datetable values ( dateadd(MONTH, -9, @reportingdate) )
	insert into @datetable values ( dateadd(MONTH, -10, @reportingdate) )
	insert into @datetable values ( dateadd(MONTH, -11, @reportingdate) )
	insert into @datetable values ( dateadd(MONTH, -12, @reportingdate) )

	CREATE TABLE #TransformingCare_tempLiveData	(	[OriginatingCCGICSName] [nvarchar](255) NULL,	[EpisodeID] [float] NULL,	[SpellID] [varchar](9) NULL,	[UniqueID] [nvarchar](255) NULL,	[EpisodeStartDate] [datetime] NULL,	[EpisodeEndDate] [datetime] NULL,
	[AgeAtPeriodEnd] [float] NULL,	[NewAdmission] [int] NULL,	[CCGorSC] [nvarchar](255) NULL,	[TransferIn] [int] NOT NULL,	[DischargeFromHospital] [int] NULL,	[TransferOut] [int] NOT NULL ,
	[DateOfPlannedTransfer] [datetime] NULL,	[RN] [bigint] NULL,
	[OriginatingCCGRegionName] [nvarchar](255) NULL,	[SiteName] [varchar](1000) NULL,	[ProviderName] [varchar](1000) NULL,	[CommissioningArrangement] [nvarchar](255) NULL,
	[RecordClosed] [nvarchar](255) NULL,	[Transfer] [float] NULL,	[PatientCategory] [varchar](50) NULL,	[ReasonForAdmission] [varchar](50) NULL,	[SiteStatus] [varchar](11) NOT NULL,
	[AdmissionPlan] [varchar](10) NULL,	[DischargeDestination] [varchar](35) NULL,	[WardSecurityLevel] [varchar](200) NULL,	[WardType] [varchar](200) NULL,	
	[MentalHealthAct] [nvarchar](50) NULL,	[S17ExtendedLeave] [nvarchar](255) NULL,	[DateDiagnosedWithAutism] [datetime] NULL,	[DateDiagnosedWithLearningDisability] [datetime] NULL,
	[LoSStartDate] [datetime] NULL,	[AdmissionSource] [varchar](100) NULL,	[PatientCarePlan] [varchar](100) NULL	, [OAPReasonNoLocalSpecialistBed] [nvarchar](255) NULL	, [OAPReasonMoreSpecialist] [nvarchar](255) NULL
	, [OAPReasonCloserToFamily] [nvarchar](255) NULL	, [OAPReasonSchoolLocation] [nvarchar](255) NULL	, [OAPReasonNoLocalBed] [nvarchar](255) NULL	, [OAPReasonSafeguarding] [nvarchar](255) NULL
	, [OAPReasonOffendingRestrictions] [nvarchar](255) NULL	, [OAPReasonChoice] [nvarchar](255) NULL	, [OAPReasonOther] [nvarchar](255) NULL	, [OAPReasonNA] [nvarchar](255) NULL	) 

	insert into #TransformingCare_tempLiveData
	select OriginatingCCGICSName  , a.EpisodeID , a.SpellID , a.UniqueID ,a.EpisodeStartDate , a.EpisodeEndDate  
	, AgeAtPeriodEnd , A.NewAdmission , CCGorSC , a.TransferIn , a.DischargeFromHospital , a.TransferOut 
	, a.DateOfPlannedTransfer
	, ROW_NUMBER() over ( partition by a.UniqueID  order by a.EpisodeStartDate ) As RN  
	, b.OriginatingCCGRegionName  , h.SiteName , h.ProviderName  
	, iif( CCGorSC = 'CCG' , CCGorSC , iif( j.SCorPC is null , 'SC' , j.SCorPC )) as [CommissioningArrangement]
    , k.RecordClosed ,k.[Transfer] , a.[PatientCategory], a.[ReasonForAdmission]
	, iif( H.SiteRegionName = 'LONDON' , 'London Site' , 'OOA' )  as [SiteStatus] 
	,a.AdmissionPlan , a.DischargeDestination , a.WardSecurityLevel , a.WardType , a.MentalHealthAct , a.S17ExtendedLeave , a.DateDiagnosedWithAutism , a.DateDiagnosedWithLearningDisability
	, i.LoSStartDate , a.[AdmissionSource] , x.PatientCarePlan ,	  h.[OAPReasonNoLocalSpecialistBed]	, h.[OAPReasonMoreSpecialist]	, h.[OAPReasonCloserToFamily]
	, h.[OAPReasonSchoolLocation]	, h.[OAPReasonNoLocalBed]	, h.[OAPReasonSafeguarding]	, h.[OAPReasonOffendingRestrictions]	, h.[OAPReasonChoice]
	, h.[OAPReasonOther]	, h.[OAPReasonNA] 
    from [NHSE_Sandbox_LDP_Shared].[dbo].[InpatientEpisodesDetails] A 
	left join [NHSE_Sandbox_LDP_Shared].[dbo].[EpisodesCommissioners_vw] B   on a.EpisodeID = b.EpisodeID and a.UniqueID = b.UniqueID
	left join [NHSE_Sandbox_LDP_Shared].[dbo].Inpatients E on a.UniqueID = E.UniqueID and a.UniqueID = e.UniqueID
	left join [NHSE_Sandbox_LDP_Shared].[dbo].ProviderEpisodes  H on H.UniqueID = a.UniqueID and a.EpisodeID = h.EpisodeID
	left join [NHSE_Sandbox_Lon_LDAProgramme].[dbo].[TransformingCare_LookupCommissioning] J on J.[CommissioningCode] = b.SubmittingCommissioner
	left join [NHSE_Sandbox_LDP_Shared].[dbo].DataExtractHistory K on k.[UNIQUE ID] = a.UniqueID and k.EpisodeID = a.EpisodeID
	left join [NHSE_Sandbox_LDP_Shared].[dbo].InpatientSpellsv2 I on a.SpellID = I.SpellID and a.UniqueID = I.UniqueID
	left join [NHSE_Sandbox_LDP_Shared].[dbo].DischargePlanDetails X on a.EpisodeID = X.EpisodeID 	
	where OriginatingCCGRegionName = 'LONDON'

	-- insert into [dbo].[TransformingCare_Archive_FlatFile] 
	-- select a.*  , @reportingDateHold  from #TransformingCare_tempLiveData A 
	-- left join [dbo].[TransformingCare_Archive_FlatFile]  B on a.UniqueID = b.UniqueID and a.EpisodeID = b.EpisodeID and b.ReportingMonth = @reportingDateHold
	-- where a.UniqueID  is null 


	CREATE TABLE #TransformingCare_tblMasterMetric_BackTest (
	[ICS] [varchar](100) NOT NULL,
	[ReportingMonth] [datetime] NOT NULL,
	[MetricID] int NOT NULL,
	[Value] decimal(18,5) NULL,
	[CommissioningArrangement] [varchar](20) NOT NULL,
	[PatientGroup] [varchar](25) NOT NULL,
	[GroupingLevel] int NOT NULL, 
	PatientCategory [varchar](100) NOT NULL	,
	[LocationName] [varchar](500) NOT NULL) 	
	   

		WHILE exists( select top(1) * from @datetable) 
		BEGIN	
			set @reportingdate = ( select top(1) * from @datetable ) 
				  
------------------------------------------------------------------------------------------------------------------------------------------------------
--Number of Inpatients Start -------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------------------------
		 
			insert into #TransformingCare_tblMasterMetric_BackTest
			select OriginatingCCGICSName ,  @reportingdate  , 1 , count(*) as C
			, [CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) ) , 1 , PatientCategory ,'NA'
			from #TransformingCare_tempLiveData A 
			left join [NHSE_Sandbox_LDP_Shared].[dbo].[InpatientAgeBanding_vw] B on a.UniqueID = b.UniqueID and EOMONTH (@reportingdate) = TimePeriod
			where  a.EpisodeEndDate > EOMONTH(@reportingdate) and a.EpisodeStartDate <= EOMONTH(@reportingdate)
			group by OriginatingCCGICSName , [CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) ) , PatientCategory

------------------------------------------------------------------------------------------------------------------------------------------------------
--Number of Inpatients End ---------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------------------------
				
-------------------------------------------------------------------------------------------------------------------------------------------------------
---- Number of Inpatients by Site Start ---------------------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------------------------------------------------------------	
--
--			insert into #TransformingCare_tblMasterMetric_BackTest
--			select OriginatingCCGICSName ,  @reportingdate , 1 , count(*) as C
--			, [CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) ) , 2 , PatientCategory , sitename 
--			from #TransformingCare_tempLiveData A 
--			left join [NHSE_Sandbox_LDP_Shared].[dbo].[InpatientAgeBanding_vw] B on a.UniqueID = b.UniqueID and EOMONTH (@reportingdate) = TimePeriod
--			where  a.EpisodeEndDate > EOMONTH(@reportingdate) and a.EpisodeStartDate <= EOMONTH(@reportingdate)
--			group by OriginatingCCGICSName , sitename , [CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) ), PatientCategory
--
-------------------------------------------------------------------------------------------------------------------------------------------------------
---- Number of Inpatients by Site End  ----------------------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------------------------------------------------------------	
--
-------------------------------------------------------------------------------------------------------------------------------------------------------
---- Number of Inpatients by provider Start -----------------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------------------------------------------------------------
--
--			insert into #TransformingCare_tblMasterMetric_BackTest
--			select  OriginatingCCGICSName,  @reportingdate  , 1 , count(*) as C
--			, [CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) ) , 3 , PatientCategory , providerName
--			from #TransformingCare_tempLiveData A 
--			left join [NHSE_Sandbox_LDP_Shared].[dbo].[InpatientAgeBanding_vw] B on a.UniqueID = b.UniqueID and EOMONTH (@reportingdate) = TimePeriod
--			where  a.EpisodeEndDate > EOMONTH(@reportingdate) and a.EpisodeStartDate <= EOMONTH(@reportingdate)
--			group by OriginatingCCGICSName , providerName , [CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) ), PatientCategory
--			
-------------------------------------------------------------------------------------------------------------------------------------------------------
---- Number of Inpatients by provider End -------------------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------------------------------------------------------------

-----------------------------------------------------------------------------------------------------------------------------------------------------
-- Number of SRS Inpatients Start -------------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------------------------------
				 
			insert into #TransformingCare_tblMasterMetric_BackTest
			select OriginatingCCGICSName ,  @reportingdate  , 2 , count(*) as C
			, [CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) ) ,1 , PatientCategory ,'NA'
			from #TransformingCare_tempLiveData A 
			left join [NHSE_Sandbox_LDP_Shared].[dbo].[InpatientAgeBanding_vw] B on a.UniqueID = b.UniqueID and EOMONTH (@reportingdate) = TimePeriod
			where  a.EpisodeEndDate > EOMONTH(@reportingdate) and a.EpisodeStartDate <= EOMONTH(@reportingdate) and providerName = 'HERTFORDSHIRE PARTNERSHIP UNIVERSITY NHS FT'
			group by OriginatingCCGICSName , [CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) ) , PatientCategory
				
-----------------------------------------------------------------------------------------------------------------------------------------------------
-- Number of SRS Inpatients End ---------------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------------------------------

-----------------------------------------------------------------------------------------------------------------------------------------------------
-- Number of MOJ Inpatients Start -------------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------------------------------
				
		insert into #TransformingCare_tblMasterMetric_BackTest
		select OriginatingCCGICSName ,  @reportingdate  , 3 , count(*) as C
		, [CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) )  , 1 , PatientCategory,'NA'
		from #TransformingCare_tempLiveData A 
		left join [NHSE_Sandbox_LDP_Shared].[dbo].[InpatientAgeBanding_vw] B on a.UniqueID = b.UniqueID and EOMONTH (@reportingdate) = TimePeriod
		where  a.EpisodeEndDate > EOMONTH(@reportingdate) and a.EpisodeStartDate <= EOMONTH(@reportingdate) and MentalHealthAct in ( 'MHA Section 37 with 41 restrictions','MHA Section 47','MHA Section 47 with 49 restrictions' ) 
		group by OriginatingCCGICSName , [CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) ) , PatientCategory
							  
-----------------------------------------------------------------------------------------------------------------------------------------------------
-- Number of MOJ Inpatients End ---------------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------------------------------

-----------------------------------------------------------------------------------------------------------------------------------------------------
-- Number of Mental Health sections Inpatients Start ------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------------------------------
				
		insert into #TransformingCare_tblMasterMetric_BackTest
		select OriginatingCCGICSName ,  @reportingdate  , 4 , count(*) as C
		, [CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) )  , 1 , PatientCategory,'NA'
		from #TransformingCare_tempLiveData A 
		left join [NHSE_Sandbox_LDP_Shared].[dbo].[InpatientAgeBanding_vw] B on a.UniqueID = b.UniqueID and EOMONTH (@reportingdate) = TimePeriod
		where  a.EpisodeEndDate > EOMONTH(@reportingdate) and a.EpisodeStartDate <= EOMONTH(@reportingdate) and MentalHealthAct not in ( 'Not Known','Not Applicable' , 'Informal' ) 
		group by OriginatingCCGICSName , [CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) ) , PatientCategory
							  
-----------------------------------------------------------------------------------------------------------------------------------------------------
-- Number of Mental Health sections Inpatients End --------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------------------------------

-----------------------------------------------------------------------------------------------------------------------------------------------------
-- Number patients diagnosed with LD Start ----------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------------------------------
				
		insert into #TransformingCare_tblMasterMetric_BackTest
		select OriginatingCCGICSName ,  @reportingdate  , 5 , count(*) as C
		, [CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) )  , 1 , PatientCategory,'NA'
		from #TransformingCare_tempLiveData A 
		left join [NHSE_Sandbox_LDP_Shared].[dbo].[InpatientAgeBanding_vw] B on a.UniqueID = b.UniqueID and EOMONTH (@reportingdate) = TimePeriod
		where  a.DateDiagnosedWithLearningDisability > EOMONTH(@reportingdate) and a.DateDiagnosedWithLearningDisability <= EOMONTH(@reportingdate) and DateDiagnosedWithLearningDisability is not null
		group by OriginatingCCGICSName , [CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) ) , PatientCategory
							  
-----------------------------------------------------------------------------------------------------------------------------------------------------
-- Number patients diagnosed with LD End ------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------------------------------

-----------------------------------------------------------------------------------------------------------------------------------------------------
-- Number of Learning disabilty only inpatients Start -----------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------------------------------
				
		insert into #TransformingCare_tblMasterMetric_BackTest
		select OriginatingCCGICSName ,  @reportingdate  , 6, count(*) as C
		, [CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) ) , 1 , PatientCategory,'NA'
		from #TransformingCare_tempLiveData A 
		left join [NHSE_Sandbox_LDP_Shared].[dbo].[InpatientAgeBanding_vw] B on a.UniqueID = b.UniqueID and EOMONTH (@reportingdate) = TimePeriod
		where  a.EpisodeEndDate > EOMONTH(@reportingdate) and a.EpisodeStartDate <= EOMONTH(@reportingdate) and PatientCategory = 'Learning Disability Only'
		group by OriginatingCCGICSName , [CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) ) , PatientCategory
							  
-----------------------------------------------------------------------------------------------------------------------------------------------------
-- Number of Learning disabilty only inpatients End -------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------------------------------

-----------------------------------------------------------------------------------------------------------------------------------------------------
-- Number patients diagnosed with Autisum Start ----------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------------------------------
				
		insert into #TransformingCare_tblMasterMetric_BackTest
		select OriginatingCCGICSName ,  @reportingdate  , 7 , count(*) as C
		, [CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) )  , 1 , PatientCategory,'NA'
		from #TransformingCare_tempLiveData A 
		left join [NHSE_Sandbox_LDP_Shared].[dbo].[InpatientAgeBanding_vw] B on a.UniqueID = b.UniqueID and EOMONTH (@reportingdate) = TimePeriod
		where  a.DateDiagnosedWithAutism > EOMONTH(@reportingdate) and a.DateDiagnosedWithAutism <= EOMONTH(@reportingdate) and DateDiagnosedWithAutism is not null
		group by OriginatingCCGICSName , [CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) ) , PatientCategory
							  
-----------------------------------------------------------------------------------------------------------------------------------------------------
-- Number patients diagnosed with LD End ------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------------------------------

-----------------------------------------------------------------------------------------------------------------------------------------------------
-- Number of Autistic Spectrum Condition Only inpatients Start --------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------------------------------
				
		insert into #TransformingCare_tblMasterMetric_BackTest
		select OriginatingCCGICSName ,  @reportingdate  , 8 , count(*) as C
		, [CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) )  , 1 , PatientCategory,'NA'
		from #TransformingCare_tempLiveData A 
		left join [NHSE_Sandbox_LDP_Shared].[dbo].[InpatientAgeBanding_vw] B on a.UniqueID = b.UniqueID and EOMONTH (@reportingdate) = TimePeriod
		where  a.EpisodeEndDate > EOMONTH(@reportingdate) and a.EpisodeStartDate <= EOMONTH(@reportingdate) and PatientCategory = 'Autistic Spectrum Condition Only'
		group by OriginatingCCGICSName , [CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) ) , PatientCategory
							  
-----------------------------------------------------------------------------------------------------------------------------------------------------
-- Number of Autistic Spectrum Condition Only inpatients End ----------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------------------------------

-----------------------------------------------------------------------------------------------------------------------------------------------------
-- Number of Learning Disability and Autistic Spectrum inpatients Start -----------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------------------------------
				
		insert into #TransformingCare_tblMasterMetric_BackTest
		select OriginatingCCGICSName ,  @reportingdate  , 9 , count(*) as C
		, [CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) ) , 1 , PatientCategory	,'NA'
		from #TransformingCare_tempLiveData A 
		left join [NHSE_Sandbox_LDP_Shared].[dbo].[InpatientAgeBanding_vw] B on a.UniqueID = b.UniqueID and EOMONTH (@reportingdate) = TimePeriod
		where  a.EpisodeEndDate > EOMONTH(@reportingdate) and a.EpisodeStartDate <= EOMONTH(@reportingdate) and PatientCategory = 'Learning Disability and Autistic Spectrum'
		group by OriginatingCCGICSName , [CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) ) , PatientCategory
							  
-----------------------------------------------------------------------------------------------------------------------------------------------------
-- Number of Learning Disability and Autistic Spectrum inpatients End -------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------------------------------

-----------------------------------------------------------------------------------------------------------------------------------------------------
-- Population numbers (10) are a special case See script [] -----------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------------------------------

-----------------------------------------------------------------------------------------------------------------------------------------------------
-- Number of Learning Disability and Autistic Spectrum inpatients End -------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------------------------------

-----------------------------------------------------------------------------------------------------------------------------------------------------
-- Admissions/Transfers in count Start --------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------------------------------
	
		insert into #TransformingCare_tblMasterMetric_BackTest
		select OriginatingCCGICSName ,  @reportingdate  , 11, count(*) as C
		, [CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) ) , 1 , PatientCategory,'NA'
		from #TransformingCare_tempLiveData A 
		left join [NHSE_Sandbox_LDP_Shared].[dbo].[InpatientAgeBanding_vw] B on a.UniqueID = b.UniqueID and EOMONTH (@reportingdate) = TimePeriod
		where  ( A.NewAdmission = 1 or a.TransferIn = 1 ) 	and a.EpisodeStartDate >= @reportingdate and a.EpisodeStartDate <= EOMONTH(@reportingdate)
		group by OriginatingCCGICSName , [CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) ) , PatientCategory
								
-----------------------------------------------------------------------------------------------------------------------------------------------------
-- Admissions/Transfers in count End ----------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------------------------------

-----------------------------------------------------------------------------------------------------------------------------------------------------
-- Admissions count Start ---------------------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------------------------------

		insert into #TransformingCare_tblMasterMetric_BackTest
		select OriginatingCCGICSName ,  @reportingdate  , 12 , count(*) as C
		, [CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) ) , 1 , PatientCategory,'NA'
		from #TransformingCare_tempLiveData A 
		left join [NHSE_Sandbox_LDP_Shared].[dbo].[InpatientAgeBanding_vw] B on a.UniqueID = b.UniqueID and EOMONTH (@reportingdate) = TimePeriod
		where  A.NewAdmission = 1 			and a.EpisodeStartDate >= @reportingdate and a.EpisodeStartDate <= EOMONTH(@reportingdate)
		group by OriginatingCCGICSName , [CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) ) , PatientCategory
				
-----------------------------------------------------------------------------------------------------------------------------------------------------
-- Admissions count End -----------------------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------------------------------

-----------------------------------------------------------------------------------------------------------------------------------------------------
-- Transfers in count Start -------------------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------------------------------
		 
		insert into #TransformingCare_tblMasterMetric_BackTest
		select OriginatingCCGICSName ,  @reportingdate  , 13 , count(*) as C
		, [CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) )  , 1 , PatientCategory,'NA'
		from #TransformingCare_tempLiveData A 
		left join [NHSE_Sandbox_LDP_Shared].[dbo].[InpatientAgeBanding_vw] B on a.UniqueID = b.UniqueID and EOMONTH (@reportingdate) = TimePeriod
		where  a.TransferIn = 1			and a.EpisodeStartDate >= @reportingdate and a.EpisodeStartDate <= EOMONTH(@reportingdate)
		group by OriginatingCCGICSName , [CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) ) , PatientCategory
				
-----------------------------------------------------------------------------------------------------------------------------------------------------
-- Transfers in count End ---------------------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------------------------------

-----------------------------------------------------------------------------------------------------------------------------------------------------
-- Unplanned/planned Admissions count Start -----------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------------------------------

		insert into #TransformingCare_tblMasterMetric_BackTest
		select OriginatingCCGICSName ,  @reportingdate  , 14 , count(*) as C
		, [CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) ) , 1 , PatientCategory,'NA'
		from #TransformingCare_tempLiveData A 
		left join [NHSE_Sandbox_LDP_Shared].[dbo].[InpatientAgeBanding_vw] B on a.UniqueID = b.UniqueID and EOMONTH (@reportingdate) = TimePeriod
		where  A.NewAdmission = 1 		and a.EpisodeStartDate >= @reportingdate and a.EpisodeStartDate <= EOMONTH(@reportingdate) and AdmissionPlan in ( 'Unplanned' , 'Planned' ) 
		group by OriginatingCCGICSName , [CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) ) , PatientCategory
				
-----------------------------------------------------------------------------------------------------------------------------------------------------
-- Unplanned/planned Admissions count End -----------------------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------------------------------

-----------------------------------------------------------------------------------------------------------------------------------------------------
-- Unplanned Admissions count Start -----------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------------------------------

		insert into #TransformingCare_tblMasterMetric_BackTest
		select OriginatingCCGICSName ,  @reportingdate  , 15 , count(*) as C
		, [CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) ) , 1 , PatientCategory,'NA'
		from #TransformingCare_tempLiveData A 
		left join [NHSE_Sandbox_LDP_Shared].[dbo].[InpatientAgeBanding_vw] B on a.UniqueID = b.UniqueID and EOMONTH (@reportingdate) = TimePeriod
		where  A.NewAdmission = 1 		and a.EpisodeStartDate >= @reportingdate and a.EpisodeStartDate <= EOMONTH(@reportingdate) and AdmissionPlan in ( 'Unplanned') 
		group by OriginatingCCGICSName , [CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) ) , PatientCategory
				
-----------------------------------------------------------------------------------------------------------------------------------------------------
-- Unplanned Admissions count End -----------------------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------------------------------

-----------------------------------------------------------------------------------------------------------------------------------------------------
-- planned Admissions count Start -----------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------------------------------

		insert into #TransformingCare_tblMasterMetric_BackTest
		select OriginatingCCGICSName ,  @reportingdate  , 16 , count(*) as C
		, [CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) ) , 1 , PatientCategory,'NA'
		from #TransformingCare_tempLiveData A 
		left join [NHSE_Sandbox_LDP_Shared].[dbo].[InpatientAgeBanding_vw] B on a.UniqueID = b.UniqueID and EOMONTH (@reportingdate) = TimePeriod
		where  A.NewAdmission = 1 		and a.EpisodeStartDate >= @reportingdate and a.EpisodeStartDate <= EOMONTH(@reportingdate) and AdmissionPlan in (  'Planned' ) 
		group by OriginatingCCGICSName , [CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) ) , PatientCategory
				
-----------------------------------------------------------------------------------------------------------------------------------------------------
-- planned Admissions count End -----------------------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------------------------------

-----------------------------------------------------------------------------------------------------------------------------------------------------
-- Readmissions (30 days)  Start --------------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------------------------------

		insert into #TransformingCare_tblMasterMetric_BackTest
		select OriginatingCCGICSName ,  @reportingdate  , 17, count(*) as C
		, [CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) )  , 1 , PatientCategory,'NA'
		from #TransformingCare_tempLiveData A 
		left join [NHSE_Sandbox_LDP_Shared].[dbo].[InpatientAgeBanding_vw] B on a.UniqueID = b.UniqueID and EOMONTH (@reportingdate) = TimePeriod
		outer apply ( select max(spellenddate) as MX_SpellEndDate from [NHSE_Sandbox_LDP_Shared].[dbo].[Readmissions] where a.UniqueID = UniqueID and a.SpellID <> SpellID and a.EpisodeStartDate > SpellStartDate ) As C 
		outer apply ( select min(SpellStartDate) as MX_SpellStartDate from [NHSE_Sandbox_LDP_Shared].[dbo].[Readmissions] where a.UniqueID = UniqueID and a.SpellID = SpellID  ) As D
		where   a.EpisodeStartDate >= @reportingdate and a.EpisodeStartDate <= EOMONTH(@reportingdate) and MX_SpellEndDate is not null and A.NewAdmission = 1  and dateadd(day , 30 , MX_SpellEndDate)  > MX_SpellStartDate 
		group by OriginatingCCGICSName , [CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) ) , PatientCategory

-----------------------------------------------------------------------------------------------------------------------------------------------------
-- Readmissions (30 days) End -----------------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------------------------------

-----------------------------------------------------------------------------------------------------------------------------------------------------
-- Readmissions (90 days)  Start --------------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------------------------------

		insert into #TransformingCare_tblMasterMetric_BackTest
		select OriginatingCCGICSName ,  @reportingdate  , 18 , count(*) as C
		, [CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) )  , 1 , PatientCategory,'NA'
		from #TransformingCare_tempLiveData A 
		left join [NHSE_Sandbox_LDP_Shared].[dbo].[InpatientAgeBanding_vw] B on a.UniqueID = b.UniqueID and EOMONTH (@reportingdate) = TimePeriod
		outer apply ( select max(spellenddate) as MX_SpellEndDate from [NHSE_Sandbox_LDP_Shared].[dbo].[Readmissions] where a.UniqueID = UniqueID and a.SpellID <> SpellID and a.EpisodeStartDate > SpellStartDate ) As C 
		outer apply ( select min(SpellStartDate) as MX_SpellStartDate from [NHSE_Sandbox_LDP_Shared].[dbo].[Readmissions] where a.UniqueID = UniqueID and a.SpellID = SpellID  ) As D
		where   a.EpisodeStartDate >= @reportingdate and a.EpisodeStartDate <= EOMONTH(@reportingdate) and MX_SpellEndDate is not null and A.NewAdmission = 1  and dateadd(day , 90 , MX_SpellEndDate)  > MX_SpellStartDate 
		group by OriginatingCCGICSName , [CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) ) , PatientCategory

-----------------------------------------------------------------------------------------------------------------------------------------------------
-- Readmissions (90 days) End -----------------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------------------------------

-----------------------------------------------------------------------------------------------------------------------------------------------------
-- Readmissions (180 days)  Start -------------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------------------------------

		insert into #TransformingCare_tblMasterMetric_BackTest
		select OriginatingCCGICSName ,  @reportingdate  , 19 , count(*) as C
		, [CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) )  , 1 , PatientCategory,'NA'
		from #TransformingCare_tempLiveData A 
		left join [NHSE_Sandbox_LDP_Shared].[dbo].[InpatientAgeBanding_vw] B on a.UniqueID = b.UniqueID and EOMONTH (@reportingdate) = TimePeriod
		outer apply ( select max(spellenddate) as MX_SpellEndDate from [NHSE_Sandbox_LDP_Shared].[dbo].[Readmissions] where a.UniqueID = UniqueID and a.SpellID <> SpellID and a.EpisodeStartDate > SpellStartDate ) As C 
		outer apply ( select min(SpellStartDate) as MX_SpellStartDate from [NHSE_Sandbox_LDP_Shared].[dbo].[Readmissions] where a.UniqueID = UniqueID and a.SpellID = SpellID  ) As D
		where   a.EpisodeStartDate >= @reportingdate and a.EpisodeStartDate <= EOMONTH(@reportingdate) and MX_SpellEndDate is not null and A.NewAdmission = 1  and dateadd(day , 180 , MX_SpellEndDate)  > MX_SpellStartDate 
		group by OriginatingCCGICSName , [CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) ), PatientCategory
	
------------------------------------------------------------------------------------------------------------------------------------------------------
-- Readmissions (180 days) End -----------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------------------------

------------------------------------------------------------------------------------------------------------------------------------------------------
-- Readmissions (Start of programme)  Start ----------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------------------------
			
		insert into #TransformingCare_tblMasterMetric_BackTest
		select OriginatingCCGICSName ,  @reportingdate  , 20 , count(*) as C
		, [CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) )  , 1 , PatientCategory,'NA'
			from #TransformingCare_tempLiveData A 
		left join [NHSE_Sandbox_LDP_Shared].[dbo].[InpatientAgeBanding_vw] B on a.UniqueID = b.UniqueID and EOMONTH (@reportingdate) = TimePeriod
		outer apply ( select max(spellenddate) as MX_SpellEndDate from [NHSE_Sandbox_LDP_Shared].[dbo].[Readmissions] where a.UniqueID = UniqueID and a.SpellID <> SpellID and a.EpisodeStartDate > SpellStartDate ) As C 
		outer apply ( select min(SpellStartDate) as MX_SpellStartDate from [NHSE_Sandbox_LDP_Shared].[dbo].[Readmissions] where a.UniqueID = UniqueID and a.SpellID = SpellID  ) As D
		where   a.EpisodeStartDate >= @reportingdate and a.EpisodeStartDate <= EOMONTH(@reportingdate) and MX_SpellEndDate is not null and A.NewAdmission = 1  --and dateadd(day , 180 , MX_SpellEndDate)  > MX_SpellStartDate 
		group by OriginatingCCGICSName , [CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) ) , PatientCategory

------------------------------------------------------------------------------------------------------------------------------------------------------
-- Readmissions (Start of programme) End -------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------------------------

-----------------------------------------------------------------------------------------------------------------------------------------------------
-- Discharges count Start ---------------------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------------------------------
	
		insert into #TransformingCare_tblMasterMetric_BackTest
		select OriginatingCCGICSName ,  @reportingdate  , 21 , count(*) as C
		, [CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) )  , 1 , PatientCategory,'NA'
		from #TransformingCare_tempLiveData A 
		left join [NHSE_Sandbox_LDP_Shared].[dbo].[InpatientAgeBanding_vw] B on a.UniqueID = b.UniqueID and EOMONTH (@reportingdate) = TimePeriod
		where  a.DischargeFromHospital = 1		and a.episodeenddate >= @reportingdate and a.episodeenddate <= EOMONTH(@reportingdate)
		group by OriginatingCCGICSName , [CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) ) , PatientCategory
				
-----------------------------------------------------------------------------------------------------------------------------------------------------
-- Discharges count End -----------------------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------------------------------

-----------------------------------------------------------------------------------------------------------------------------------------------------
-- Number of discharged patients with a LOS greater than 5 years Start ------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------------------------------
	
		insert into #TransformingCare_tblMasterMetric_BackTest
		select OriginatingCCGICSName ,  @reportingdate  , 22 , count(*) as C
		, [CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) )  , 1 , PatientCategory,'NA'
		from #TransformingCare_tempLiveData A 
		left join [NHSE_Sandbox_LDP_Shared].[dbo].[InpatientAgeBanding_vw] B on a.UniqueID = b.UniqueID and EOMONTH (@reportingdate) = TimePeriod
		left join [NHSE_Sandbox_LDP_Shared].[dbo].[InpatientLengthOfStay] C on C.UniqueID = a.UniqueID and c.EpisodeID = a.EpisodeID and  EOMONTH (@reportingdate) = c.TimePeriod
		where a.DischargeFromHospital = 1 and a.episodeenddate >= @reportingdate and a.episodeenddate <= EOMONTH(@reportingdate)
		and EpisodeLOSBanding not in ('0-3 Months' , '3-6 Months' , '6-12 Months' , '1-2 Years' , '2-3 Years' , '3-4 Years', '4-5 Years') 
		group by OriginatingCCGICSName , [CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) ), PatientCategory
		
-----------------------------------------------------------------------------------------------------------------------------------------------------
-- Number of discharged patients with a LOS greater than 5 years End --------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------------------------------

-----------------------------------------------------------------------------------------------------------------------------------------------------
-- Total amount of days stayed by discharged patients Start -----------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------------------------------
	
		insert into #TransformingCare_tblMasterMetric_BackTest
		select OriginatingCCGICSName ,  @reportingdate  , 23 , sum(c.LOSAtDischarge) 
		, [CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) )  , 1 , PatientCategory,'NA'
		from #TransformingCare_tempLiveData A 
		left join [NHSE_Sandbox_LDP_Shared].[dbo].[InpatientAgeBanding_vw] B on a.UniqueID = b.UniqueID and EOMONTH (@reportingdate) = TimePeriod
		left join [NHSE_Sandbox_LDP_Shared].[dbo].[InpatientLengthOfStay] C on C.UniqueID = a.UniqueID and c.EpisodeID = a.EpisodeID and  EOMONTH (@reportingdate) = c.TimePeriod
		where a.DischargeFromHospital = 1 and a.episodeenddate >= @reportingdate and a.episodeenddate <= EOMONTH(@reportingdate)
		group by OriginatingCCGICSName , [CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) ), PatientCategory
		
-----------------------------------------------------------------------------------------------------------------------------------------------------
-- Total amount of days stayed by discharged patients End -------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------------------------------

-----------------------------------------------------------------------------------------------------------------------------------------------------
-- Total number of inpatients with a Discharge Plans Start -------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------------------------
			
		insert into #TransformingCare_tblMasterMetric_BackTest
		select OriginatingCCGICSName ,  @reportingdate  , 24 , count(*) as C
		, [CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) )  , 1 , PatientCategory,'NA'
		from #TransformingCare_tempLiveData A 
		left join [NHSE_Sandbox_LDP_Shared].[dbo].[InpatientAgeBanding_vw] B on a.UniqueID = b.UniqueID and EOMONTH (@reportingdate) = TimePeriod
		where   a.EpisodeEndDate > EOMONTH(@reportingdate) and a.EpisodeStartDate <= EOMONTH(@reportingdate)
		and A.DateOfPlannedTransfer is not null 
		group by OriginatingCCGICSName , [CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) ) , PatientCategory
			  	
-----------------------------------------------------------------------------------------------------------------------------------------------------
-- Total number of inpatients with a Discharge Plans End --------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------------------------------

------------------------------------------------------------------------------------------------------------------------------------------------------
-- Discharge Plans (this Month) Start ----------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------------------------
			
		insert into #TransformingCare_tblMasterMetric_BackTest
		select OriginatingCCGICSName ,  @reportingdate  , 25 , count(*) as C
		, [CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) )  , 1 , PatientCategory,'NA'
		from #TransformingCare_tempLiveData A 
		left join [NHSE_Sandbox_LDP_Shared].[dbo].[InpatientAgeBanding_vw] B on a.UniqueID = b.UniqueID and EOMONTH (@reportingdate) = TimePeriod
		where   a.EpisodeEndDate > EOMONTH(@reportingdate) and a.EpisodeStartDate <= EOMONTH(@reportingdate)
		and A.DateOfPlannedTransfer >= dateadd(MONTH, 0, @reportingdate) and A.DateOfPlannedTransfer <= EOMONTH(dateadd(MONTH, 0, @reportingdate))
		and A.DateOfPlannedTransfer is not null 
		group by OriginatingCCGICSName , [CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) ) , PatientCategory
			  	
-----------------------------------------------------------------------------------------------------------------------------------------------------
-- Discharge Plans (This Month) End -----------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------------------------------

------------------------------------------------------------------------------------------------------------------------------------------------------
-- Discharge Plans (Next Month) Start ----------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------------------------
			
		insert into #TransformingCare_tblMasterMetric_BackTest
		select OriginatingCCGICSName ,  @reportingdate  , 26 , count(*) as C
		, [CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) )  , 1 , PatientCategory,'NA'
		from #TransformingCare_tempLiveData A 
		left join [NHSE_Sandbox_LDP_Shared].[dbo].[InpatientAgeBanding_vw] B on a.UniqueID = b.UniqueID and EOMONTH (@reportingdate) = TimePeriod
		where   a.EpisodeEndDate > EOMONTH(@reportingdate) and a.EpisodeStartDate <= EOMONTH(@reportingdate)
		and A.DateOfPlannedTransfer >= dateadd(MONTH, 1, @reportingdate) and A.DateOfPlannedTransfer <= EOMONTH(dateadd(MONTH, 1, @reportingdate))
		and A.DateOfPlannedTransfer is not null 
		group by OriginatingCCGICSName , [CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) ) , PatientCategory
			  	
-----------------------------------------------------------------------------------------------------------------------------------------------------
-- Discharge Plans (Next Month) End -----------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------------------------------
					 
-----------------------------------------------------------------------------------------------------------------------------------------------------
-- Discharge Plans (in 1 to 2 months) Start ---------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------------------------------

		insert into #TransformingCare_tblMasterMetric_BackTest
		select OriginatingCCGICSName ,  @reportingdate  , 27, count(*) as C
		, [CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) )  , 1 , PatientCategory,'NA'
		from #TransformingCare_tempLiveData A 
		left join [NHSE_Sandbox_LDP_Shared].[dbo].[InpatientAgeBanding_vw] B on a.UniqueID = b.UniqueID and EOMONTH (@reportingdate) = TimePeriod
		where   a.EpisodeEndDate > EOMONTH(@reportingdate) and a.EpisodeStartDate <= EOMONTH(@reportingdate)
		and A.DateOfPlannedTransfer >= dateadd(MONTH, 2, @reportingdate) and A.DateOfPlannedTransfer <= EOMONTH(dateadd(MONTH, 2, @reportingdate))
		and A.DateOfPlannedTransfer is not null 
		group by OriginatingCCGICSName , [CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) ) , PatientCategory
					
----------------------------------------------------------------------------------------------------------------------------------------------------
-- Discharge Plans (in 1 to 2 months) End ----------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------------------------------			 

----------------------------------------------------------------------------------------------------------------------------------------------------
-- Discharge Plans (in 2 to 3 months) Start --------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------------------------------

		insert into #TransformingCare_tblMasterMetric_BackTest
		select OriginatingCCGICSName ,  @reportingdate  , 28 , count(*) as C
		, [CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) )  , 1 , PatientCategory,'NA'
		from #TransformingCare_tempLiveData A 
		left join [NHSE_Sandbox_LDP_Shared].[dbo].[InpatientAgeBanding_vw] B on a.UniqueID = b.UniqueID and EOMONTH (@reportingdate) = TimePeriod
		where   a.EpisodeEndDate > EOMONTH(@reportingdate) and a.EpisodeStartDate <= EOMONTH(@reportingdate)
		and A.DateOfPlannedTransfer >= dateadd(MONTH, 3, @reportingdate) and A.DateOfPlannedTransfer <= EOMONTH(dateadd(MONTH, 3, @reportingdate))
		and A.DateOfPlannedTransfer is not null 
		group by OriginatingCCGICSName , [CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) ) , PatientCategory
					
----------------------------------------------------------------------------------------------------------------------------------------------------
-- Discharge Plans (in 2 to 3 months) End ----------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------------------------------	

----------------------------------------------------------------------------------------------------------------------------------------------------
-- Discharge Plans Next Quater Start ---------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------------------------------

		insert into #TransformingCare_tblMasterMetric_BackTest
		select OriginatingCCGICSName ,  @reportingdate  , 29 , count(*) as C
		, [CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) )  , 1 , PatientCategory,'NA'
		from #TransformingCare_tempLiveData A 
		left join [NHSE_Sandbox_LDP_Shared].[dbo].[InpatientAgeBanding_vw] B on a.UniqueID = b.UniqueID and EOMONTH (@reportingdate) = TimePeriod
		where   a.EpisodeEndDate > EOMONTH(@reportingdate) and a.EpisodeStartDate <= EOMONTH(@reportingdate)
		and A.DateOfPlannedTransfer >= dateadd(MONTH, 1, @reportingdate) and A.DateOfPlannedTransfer <= EOMONTH(dateadd(MONTH, 3, @reportingdate))
		and A.DateOfPlannedTransfer is not null 
		group by OriginatingCCGICSName , [CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) ) , PatientCategory
					
----------------------------------------------------------------------------------------------------------------------------------------------------
-- Discharge Plans Next Quater End -----------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------------------------------	

----------------------------------------------------------------------------------------------------------------------------------------------------
-- Number of patients with no planned discharge date in the next 12 months Start -------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------------------------------

		insert into #TransformingCare_tblMasterMetric_BackTest
		select OriginatingCCGICSName ,  @reportingdate  , 30 , count(*) as C
		, [CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) )  , 1 , PatientCategory,'NA'
		from #TransformingCare_tempLiveData A 
		left join [NHSE_Sandbox_LDP_Shared].[dbo].[InpatientAgeBanding_vw] B on a.UniqueID = b.UniqueID and EOMONTH (@reportingdate) = TimePeriod
		where   a.EpisodeEndDate > EOMONTH(@reportingdate) and a.EpisodeStartDate <= EOMONTH(@reportingdate)
		and ( A.DateOfPlannedTransfer > dateadd(year, 1, @reportingdate) or  DateOfPlannedTransfer is null ) 
		and A.DateOfPlannedTransfer is not null 
		group by OriginatingCCGICSName , [CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) ) , PatientCategory
					
----------------------------------------------------------------------------------------------------------------------------------------------------
-- Number of patients with no planned discharge date in the next 12 months End ---------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------------------------------	


-----------------------------------------------------------------------------------------------------------------------------------------------------
--Number of Inpatients unable to discharge Start ----------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------------------------------

		insert into #TransformingCare_tblMasterMetric_BackTest
		select OriginatingCCGICSName ,  @reportingdate  , 31, count(*) as C
		, [CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) )  , 1 , PatientCategory,'NA'
		from #TransformingCare_tempLiveData A 
		left join [NHSE_Sandbox_LDP_Shared].[dbo].[InpatientAgeBanding_vw] B on a.UniqueID = b.UniqueID and EOMONTH (@reportingdate) = TimePeriod
		where  a.EpisodeEndDate > EOMONTH(@reportingdate) and a.EpisodeStartDate <= EOMONTH(@reportingdate) and PatientCarePlan in ( 'Currently not dischargeable' ) 
		group by OriginatingCCGICSName , [CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) ) , PatientCategory
				
-----------------------------------------------------------------------------------------------------------------------------------------------------
--Number of Inpatients unable to discharge End ------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------------------------------

-----------------------------------------------------------------------------------------------------------------------------------------------------
--Number of Inpatients unable to discharge Start ----------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------------------------------

		insert into #TransformingCare_tblMasterMetric_BackTest
		select OriginatingCCGICSName ,  @reportingdate  , 32 , count(*) as C
		, [CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) )  , 1 , PatientCategory,'NA'
		from #TransformingCare_tempLiveData A 
		left join [NHSE_Sandbox_LDP_Shared].[dbo].[InpatientAgeBanding_vw] B on a.UniqueID = b.UniqueID and EOMONTH (@reportingdate) = TimePeriod
		where  a.EpisodeEndDate > EOMONTH(@reportingdate) and a.EpisodeStartDate <= EOMONTH(@reportingdate) and PatientCarePlan in ( 'Unable to discharge'  ) 
		group by OriginatingCCGICSName , [CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) ) , PatientCategory
				
-----------------------------------------------------------------------------------------------------------------------------------------------------
--Number of Inpatients unable to discharge End ------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------------------------------

-----------------------------------------------------------------------------------------------------------------------------------------------------
-- Deaths Count Start -------------------------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------------------------------
		 
		insert into #TransformingCare_tblMasterMetric_BackTest
		select OriginatingCCGICSName ,  @reportingdate  , 33 , count(*) as C
		, [CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) )  , 1 , PatientCategory,'NA'
		from #TransformingCare_tempLiveData A 
		left join [NHSE_Sandbox_LDP_Shared].[dbo].[InpatientAgeBanding_vw] B on a.UniqueID = b.UniqueID and EOMONTH (@reportingdate) = TimePeriod
		where  ( RecordClosed = 12 or [Transfer] = 15 )		and a.episodeenddate >= @reportingdate and a.episodeenddate <= EOMONTH(@reportingdate)
		group by OriginatingCCGICSName , [CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) ) , PatientCategory
				
-----------------------------------------------------------------------------------------------------------------------------------------------------
-- Deaths count End ---------------------------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------------------------------

-----------------------------------------------------------------------------------------------------------------------------------------------------
-- Number of OOA Inpatients Start -------------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------------------------------
	
			insert into #TransformingCare_tblMasterMetric_BackTest
			select OriginatingCCGICSName ,  @reportingdate  , 34 , count(*) as C 
			, [CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) ) , 1 , PatientCategory,'NA'
			from #TransformingCare_tempLiveData A 
			left join [NHSE_Sandbox_LDP_Shared].[dbo].[InpatientAgeBanding_vw] B on a.UniqueID = b.UniqueID and EOMONTH (@reportingdate) = TimePeriod
			where  a.EpisodeEndDate > EOMONTH(@reportingdate) and a.EpisodeStartDate <= EOMONTH(@reportingdate) and SiteStatus = 'OOA' 
			group by OriginatingCCGICSName , [CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) ) , PatientCategory
			  
-----------------------------------------------------------------------------------------------------------------------------------------------------
-- Number of OOA Inpatients End ---------------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------------------------------

-----------------------------------------------------------------------------------------------------------------------------------------------------
-- Number of OOA Inpatients Reason: [OAPReasonNoLocalSpecialistBed] Start ---------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------------------------------
	
			insert into #TransformingCare_tblMasterMetric_BackTest
			select OriginatingCCGICSName ,  @reportingdate  , 35 , count(*) as C 
			, [CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) ) , 1 , PatientCategory,'NA'
			from #TransformingCare_tempLiveData A 
			left join [NHSE_Sandbox_LDP_Shared].[dbo].[InpatientAgeBanding_vw] B on a.UniqueID = b.UniqueID and EOMONTH (@reportingdate) = TimePeriod
			where  a.EpisodeEndDate > EOMONTH(@reportingdate) and a.EpisodeStartDate <= EOMONTH(@reportingdate) and SiteStatus = 'OOA' and [OAPReasonNoLocalSpecialistBed] = 'y'
			group by OriginatingCCGICSName , [CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) ) , PatientCategory
			  
-----------------------------------------------------------------------------------------------------------------------------------------------------
-- Number of OOA Inpatients Reason: [OAPReasonNoLocalSpecialistBed] End -----------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------------------------------

-----------------------------------------------------------------------------------------------------------------------------------------------------
-- Number of OOA Inpatients Reason: [OAPReasonMoreSpecialist] Start ---------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------------------------------
	
			insert into #TransformingCare_tblMasterMetric_BackTest
			select OriginatingCCGICSName ,  @reportingdate  , 36 , count(*) as C 
			, [CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) ) , 1 , PatientCategory,'NA'
			from #TransformingCare_tempLiveData A 
			left join [NHSE_Sandbox_LDP_Shared].[dbo].[InpatientAgeBanding_vw] B on a.UniqueID = b.UniqueID and EOMONTH (@reportingdate) = TimePeriod
			where  a.EpisodeEndDate > EOMONTH(@reportingdate) and a.EpisodeStartDate <= EOMONTH(@reportingdate) and SiteStatus = 'OOA' and [OAPReasonMoreSpecialist] = 'y'
			group by OriginatingCCGICSName , [CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) ) , PatientCategory
			  
-----------------------------------------------------------------------------------------------------------------------------------------------------
-- Number of OOA Inpatients Reason: [OAPReasonMoreSpecialist] End -----------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------------------------------

-----------------------------------------------------------------------------------------------------------------------------------------------------
-- Number of OOA Inpatients Reason: [OAPReasonCloserToFamily] Start ---------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------------------------------
	
			insert into #TransformingCare_tblMasterMetric_BackTest
			select OriginatingCCGICSName ,  @reportingdate  , 37 , count(*) as C 
			, [CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) ) , 1 , PatientCategory,'NA'
			from #TransformingCare_tempLiveData A 
			left join [NHSE_Sandbox_LDP_Shared].[dbo].[InpatientAgeBanding_vw] B on a.UniqueID = b.UniqueID and EOMONTH (@reportingdate) = TimePeriod
			where  a.EpisodeEndDate > EOMONTH(@reportingdate) and a.EpisodeStartDate <= EOMONTH(@reportingdate) and SiteStatus = 'OOA' and [OAPReasonCloserToFamily] = 'y'
			group by OriginatingCCGICSName , [CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) ) , PatientCategory
			  
-----------------------------------------------------------------------------------------------------------------------------------------------------
-- Number of OOA Inpatients Reason: [OAPReasonCloserToFamily] End -----------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------------------------------

-----------------------------------------------------------------------------------------------------------------------------------------------------
-- Number of OOA Inpatients Reason: [OAPReasonSchoolLocation] Start ---------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------------------------------
	
			insert into #TransformingCare_tblMasterMetric_BackTest
			select OriginatingCCGICSName ,  @reportingdate  , 38 , count(*) as C 
			, [CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) ) , 1 , PatientCategory,'NA'
			from #TransformingCare_tempLiveData A 
			left join [NHSE_Sandbox_LDP_Shared].[dbo].[InpatientAgeBanding_vw] B on a.UniqueID = b.UniqueID and EOMONTH (@reportingdate) = TimePeriod
			where  a.EpisodeEndDate > EOMONTH(@reportingdate) and a.EpisodeStartDate <= EOMONTH(@reportingdate) and SiteStatus = 'OOA' and [OAPReasonSchoolLocation] = 'y'
			group by OriginatingCCGICSName , [CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) ) , PatientCategory
			  
-----------------------------------------------------------------------------------------------------------------------------------------------------
-- Number of OOA Inpatients Reason: [OAPReasonSchoolLocation] End -----------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------------------------------

-----------------------------------------------------------------------------------------------------------------------------------------------------
-- Number of OOA Inpatients Reason: [OAPReasonNoLocalBed] Start -------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------------------------------
	
			insert into #TransformingCare_tblMasterMetric_BackTest
			select OriginatingCCGICSName ,  @reportingdate  , 39 , count(*) as C 
			, [CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) ) , 1 , PatientCategory,'NA'
			from #TransformingCare_tempLiveData A 
			left join [NHSE_Sandbox_LDP_Shared].[dbo].[InpatientAgeBanding_vw] B on a.UniqueID = b.UniqueID and EOMONTH (@reportingdate) = TimePeriod
			where  a.EpisodeEndDate > EOMONTH(@reportingdate) and a.EpisodeStartDate <= EOMONTH(@reportingdate) and SiteStatus = 'OOA' and [OAPReasonNoLocalBed] = 'y'
			group by OriginatingCCGICSName , [CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) ) , PatientCategory
			  
-----------------------------------------------------------------------------------------------------------------------------------------------------
-- Number of OOA Inpatients Reason: [OAPReasonNoLocalBed] End ---------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------------------------------

-----------------------------------------------------------------------------------------------------------------------------------------------------
-- Number of OOA Inpatients Reason: [OAPReasonSafeguarding] Start -----------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------------------------------
	
			insert into #TransformingCare_tblMasterMetric_BackTest
			select OriginatingCCGICSName ,  @reportingdate  , 40 , count(*) as C 
			, [CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) ) , 1 , PatientCategory,'NA'
			from #TransformingCare_tempLiveData A 
			left join [NHSE_Sandbox_LDP_Shared].[dbo].[InpatientAgeBanding_vw] B on a.UniqueID = b.UniqueID and EOMONTH (@reportingdate) = TimePeriod
			where  a.EpisodeEndDate > EOMONTH(@reportingdate) and a.EpisodeStartDate <= EOMONTH(@reportingdate) and SiteStatus = 'OOA' and [OAPReasonSafeguarding] = 'y'
			group by OriginatingCCGICSName , [CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) ) , PatientCategory
			  
-----------------------------------------------------------------------------------------------------------------------------------------------------
-- Number of OOA Inpatients Reason: [OAPReasonSafeguarding] End -------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------------------------------

-----------------------------------------------------------------------------------------------------------------------------------------------------
-- Number of OOA Inpatients Reason: [OAPReasonOffendingRestrictions] Start --------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------------------------------
	
			insert into #TransformingCare_tblMasterMetric_BackTest
			select OriginatingCCGICSName ,  @reportingdate  , 41 , count(*) as C 
			, [CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) ) , 1 , PatientCategory,'NA'
			from #TransformingCare_tempLiveData A 
			left join [NHSE_Sandbox_LDP_Shared].[dbo].[InpatientAgeBanding_vw] B on a.UniqueID = b.UniqueID and EOMONTH (@reportingdate) = TimePeriod
			where  a.EpisodeEndDate > EOMONTH(@reportingdate) and a.EpisodeStartDate <= EOMONTH(@reportingdate) and SiteStatus = 'OOA' and [OAPReasonOffendingRestrictions] = 'y'
			group by OriginatingCCGICSName , [CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) ) , PatientCategory
			  
-----------------------------------------------------------------------------------------------------------------------------------------------------
-- Number of OOA Inpatients Reason: [OAPReasonOffendingRestrictions] End ----------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------------------------------

-----------------------------------------------------------------------------------------------------------------------------------------------------
-- Number of OOA Inpatients Reason: [OAPReasonChoice] Start -----------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------------------------------
	
			insert into #TransformingCare_tblMasterMetric_BackTest
			select OriginatingCCGICSName ,  @reportingdate  , 42 , count(*) as C 
			, [CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) ) , 1 , PatientCategory,'NA'
			from #TransformingCare_tempLiveData A 
			left join [NHSE_Sandbox_LDP_Shared].[dbo].[InpatientAgeBanding_vw] B on a.UniqueID = b.UniqueID and EOMONTH (@reportingdate) = TimePeriod
			where  a.EpisodeEndDate > EOMONTH(@reportingdate) and a.EpisodeStartDate <= EOMONTH(@reportingdate) and SiteStatus = 'OOA' and [OAPReasonChoice] = 'y'
			group by OriginatingCCGICSName , [CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) ) , PatientCategory
			  
-----------------------------------------------------------------------------------------------------------------------------------------------------
-- Number of OOA Inpatients Reason: [OAPReasonChoice] End -------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------------------------------

-----------------------------------------------------------------------------------------------------------------------------------------------------
-- Number of OOA Inpatients Reason: [OAPReasonOther] Start ------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------------------------------
	
			insert into #TransformingCare_tblMasterMetric_BackTest
			select OriginatingCCGICSName ,  @reportingdate  , 43 , count(*) as C 
			, [CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) ) , 1 , PatientCategory,'NA'
			from #TransformingCare_tempLiveData A 
			left join [NHSE_Sandbox_LDP_Shared].[dbo].[InpatientAgeBanding_vw] B on a.UniqueID = b.UniqueID and EOMONTH (@reportingdate) = TimePeriod
			where  a.EpisodeEndDate > EOMONTH(@reportingdate) and a.EpisodeStartDate <= EOMONTH(@reportingdate) and SiteStatus = 'OOA' and [OAPReasonOther] = 'y'
			group by OriginatingCCGICSName , [CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) ) , PatientCategory
			  
-----------------------------------------------------------------------------------------------------------------------------------------------------
-- Number of OOA Inpatients Reason: [OAPReasonOther] End --------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------------------------------

-----------------------------------------------------------------------------------------------------------------------------------------------------
-- Number of OOA Inpatients Reason: [OAPReasonNA] Start ---------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------------------------------
	
			insert into #TransformingCare_tblMasterMetric_BackTest
			select OriginatingCCGICSName ,  @reportingdate  , 44 , count(*) as C 
			, [CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) ) , 1 , PatientCategory,'NA'
			from #TransformingCare_tempLiveData A 
			left join [NHSE_Sandbox_LDP_Shared].[dbo].[InpatientAgeBanding_vw] B on a.UniqueID = b.UniqueID and EOMONTH (@reportingdate) = TimePeriod
			where  a.EpisodeEndDate > EOMONTH(@reportingdate) and a.EpisodeStartDate <= EOMONTH(@reportingdate) and SiteStatus = 'OOA' and [OAPReasonNA] = 'y'
			group by OriginatingCCGICSName , [CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) ) , PatientCategory
			  
-----------------------------------------------------------------------------------------------------------------------------------------------------
-- Number of OOA Inpatients Reason: [OAPReasonNA] End -----------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------------------------------

----------------------------------------------------------------------------------------------------------------------------------------------------
-- LOS  > 5 years Start ----------------------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------------------------------
		
		insert into #TransformingCare_tblMasterMetric_BackTest
		select OriginatingCCGICSName ,  @reportingdate  , 45, count(*) as C
		, [CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) )  , 1 , PatientCategory,'NA'
		from #TransformingCare_tempLiveData A 
		left join [NHSE_Sandbox_LDP_Shared].[dbo].[InpatientAgeBanding_vw] B on a.UniqueID = b.UniqueID and EOMONTH (@reportingdate) = TimePeriod
		left join [NHSE_Sandbox_LDP_Shared].[dbo].[InpatientLengthOfStay] C on C.UniqueID = a.UniqueID and c.EpisodeID = a.EpisodeID and  EOMONTH (@reportingdate) = c.TimePeriod
		where  a.EpisodeEndDate > EOMONTH(@reportingdate) and a.EpisodeStartDate <= EOMONTH(@reportingdate)
		and SpellLOSBanding not in ('0-3 Months' , '3-6 Months' , '6-12 Months' , '1-2 Years' , '2-3 Years' , '3-4 Years', '4-5 Years') 
		group by OriginatingCCGICSName , [CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) ), PatientCategory

----------------------------------------------------------------------------------------------------------------------------------------------------
-- LOS  > 5 years End ------------------------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------------------------------

----------------------------------------------------------------------------------------------------------------------------------------------------
-- LOS > 1 year & < 5 years Start ------------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------------------------------
		insert into #TransformingCare_tblMasterMetric_BackTest
		select OriginatingCCGICSName ,  @reportingdate  ,46 , count(*) as C
		, [CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) )  , 1 , PatientCategory,'NA'
		from #TransformingCare_tempLiveData A 
		left join [NHSE_Sandbox_LDP_Shared].[dbo].[InpatientAgeBanding_vw] B on a.UniqueID = b.UniqueID and EOMONTH (@reportingdate) = TimePeriod
		left join [NHSE_Sandbox_LDP_Shared].[dbo].[InpatientLengthOfStay] C on C.UniqueID = a.UniqueID and c.EpisodeID = a.EpisodeID and  EOMONTH (@reportingdate) = c.TimePeriod
		where  a.EpisodeEndDate > EOMONTH(@reportingdate) and a.EpisodeStartDate <= EOMONTH(@reportingdate)
		and SpellLOSBanding in ('1-2 Years' , '2-3 Years' , '3-4 Years', '4-5 Years') 
		group by OriginatingCCGICSName , [CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) ), PatientCategory

----------------------------------------------------------------------------------------------------------------------------------------------------
-- LOS > 1 year & < 5 years End --------------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------------------------------
					
----------------------------------------------------------------------------------------------------------------------------------------------------
-- LOS  < 1 year Start -----------------------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------------------------------

		insert into #TransformingCare_tblMasterMetric_BackTest
		select OriginatingCCGICSName ,  @reportingdate  , 47 , count(*) as C
		, [CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) )  , 1 , PatientCategory,'NA'
		from #TransformingCare_tempLiveData A 
		left join [NHSE_Sandbox_LDP_Shared].[dbo].[InpatientAgeBanding_vw] B on a.UniqueID = b.UniqueID and EOMONTH (@reportingdate) = TimePeriod
		left join [NHSE_Sandbox_LDP_Shared].[dbo].[InpatientLengthOfStay] C on C.UniqueID = a.UniqueID and c.EpisodeID = a.EpisodeID and  EOMONTH (@reportingdate) = c.TimePeriod
		where  a.EpisodeEndDate > EOMONTH(@reportingdate) and a.EpisodeStartDate <= EOMONTH(@reportingdate)
		and SpellLOSBanding in ('0-3 Months' , '3-6 Months' , '6-12 Months') 
		group by OriginatingCCGICSName , [CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) ), PatientCategory

----------------------------------------------------------------------------------------------------------------------------------------------------
-- LOS  < 1 year Start -----------------------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------------------------------
	
-----------------------------------------------------------------------------------------------------------------------------------------------------
-- Total lengoth of stay in days of all patients Start ----------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------------------------------
				 
			insert into #TransformingCare_tblMasterMetric_BackTest
			select OriginatingCCGICSName ,  @reportingdate  , 48 , sum(c.SpellLOS) 
			, [CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) ) ,1 , PatientCategory,'NA'
			from #TransformingCare_tempLiveData A 
			left join [NHSE_Sandbox_LDP_Shared].[dbo].[InpatientAgeBanding_vw] B on a.UniqueID = b.UniqueID and EOMONTH (@reportingdate) = TimePeriod
			left join [NHSE_Sandbox_LDP_Shared].[dbo].[InpatientLengthOfStay] C on C.UniqueID = a.UniqueID and c.EpisodeID = a.EpisodeID and  EOMONTH (@reportingdate) = c.TimePeriod
			where  a.EpisodeEndDate > EOMONTH(@reportingdate) and a.EpisodeStartDate <= EOMONTH(@reportingdate) 
			group by OriginatingCCGICSName , [CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) ) , PatientCategory
				
-----------------------------------------------------------------------------------------------------------------------------------------------------
-- Total lengoth of stay in days of all patients End ------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------------------------------

-----------------------------------------------------------------------------------------------------------------------------------------------------
-- Total lengoth of stay in days of all SRS Patients Start ------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------------------------------
				 
			insert into #TransformingCare_tblMasterMetric_BackTest
			select OriginatingCCGICSName ,  @reportingdate  , 49 , sum(c.SpellLOS) 
			, [CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) ) ,1 , PatientCategory,'NA'
			from #TransformingCare_tempLiveData A 
			left join [NHSE_Sandbox_LDP_Shared].[dbo].[InpatientAgeBanding_vw] B on a.UniqueID = b.UniqueID and EOMONTH (@reportingdate) = TimePeriod
			left join [NHSE_Sandbox_LDP_Shared].[dbo].[InpatientLengthOfStay] C on C.UniqueID = a.UniqueID and c.EpisodeID = a.EpisodeID and  EOMONTH (@reportingdate) = c.TimePeriod
			where  a.EpisodeEndDate > EOMONTH(@reportingdate) and a.EpisodeStartDate <= EOMONTH(@reportingdate) and providerName = 'HERTFORDSHIRE PARTNERSHIP UNIVERSITY NHS FT'
			group by OriginatingCCGICSName , [CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) ) , PatientCategory
				
-----------------------------------------------------------------------------------------------------------------------------------------------------
-- Total lengoth of stay in days of all SRS Patients End --------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------------------------------

-----------------------------------------------------------------------------------------------------------------------------------------------------
-- Total lengoth of stay in days of all patients excl SRS Start -------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------------------------------
				 
			insert into #TransformingCare_tblMasterMetric_BackTest
			select OriginatingCCGICSName ,  @reportingdate  , 50 , sum(c.SpellLOS) 
			, [CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) ) ,1 , PatientCategory,'NA'
			from #TransformingCare_tempLiveData A 
			left join [NHSE_Sandbox_LDP_Shared].[dbo].[InpatientAgeBanding_vw] B on a.UniqueID = b.UniqueID and EOMONTH (@reportingdate) = TimePeriod
			left join [NHSE_Sandbox_LDP_Shared].[dbo].[InpatientLengthOfStay] C on C.UniqueID = a.UniqueID and c.EpisodeID = a.EpisodeID and  EOMONTH (@reportingdate) = c.TimePeriod
			where  a.EpisodeEndDate > EOMONTH(@reportingdate) and a.EpisodeStartDate <= EOMONTH(@reportingdate) and providerName <> 'HERTFORDSHIRE PARTNERSHIP UNIVERSITY NHS FT'
			group by OriginatingCCGICSName , [CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) ) , PatientCategory
				
-----------------------------------------------------------------------------------------------------------------------------------------------------
-- Total lengoth of stay in days of all patients excl SRS End ---------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------------------------------

-----------------------------------------------------------------------------------------------------------------------------------------------------
-- Total number of patients who have tipped in to > 5 years Start -----------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------------------------------
				 
			insert into #TransformingCare_tblMasterMetric_BackTest
			select OriginatingCCGICSName ,  @reportingdate  , 51 , Count(*)
			, [CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) ) ,1 , PatientCategory,'NA'
			from #TransformingCare_tempLiveData A 
			left join [NHSE_Sandbox_LDP_Shared].[dbo].[InpatientAgeBanding_vw] B on a.UniqueID = b.UniqueID and EOMONTH (@reportingdate) = TimePeriod
			left join [NHSE_Sandbox_LDP_Shared].[dbo].[InpatientLengthOfStay] C on C.UniqueID = a.UniqueID and c.EpisodeID = a.EpisodeID and  EOMONTH (@reportingdate) = c.TimePeriod
			where  a.EpisodeEndDate > EOMONTH(@reportingdate) and a.EpisodeStartDate <= EOMONTH(@reportingdate) and c.SpellLOSBanding = '5-10 Years' and (@reportingdate) < dateadd( MONTH , 61, SpellStartDate ) 
			group by OriginatingCCGICSName , [CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) ) , PatientCategory
				
-----------------------------------------------------------------------------------------------------------------------------------------------------
-- Total number of patients who have tipped in to > 5 years End -------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------------------------------

-----------------------------------------------------------------------------------------------------------------------------------------------------
-- Total number of patients who will tip in to > 5 years in 1 months time Start ---------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------------------------------
				 
			insert into #TransformingCare_tblMasterMetric_BackTest
			select OriginatingCCGICSName ,  @reportingdate  , 52 , Count(*)
			, [CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) ) ,1 , PatientCategory,'NA'
			from #TransformingCare_tempLiveData A 
			left join [NHSE_Sandbox_LDP_Shared].[dbo].[InpatientAgeBanding_vw] B on a.UniqueID = b.UniqueID and EOMONTH (@reportingdate) = TimePeriod
			left join [NHSE_Sandbox_LDP_Shared].[dbo].[InpatientLengthOfStay] C on C.UniqueID = a.UniqueID and c.EpisodeID = a.EpisodeID and  EOMONTH (@reportingdate) = c.TimePeriod
			where  a.EpisodeEndDate > EOMONTH(@reportingdate) and a.EpisodeStartDate <= EOMONTH(@reportingdate) 
			and c.SpellLOSBanding = '4-5 Years' 
			and  dateadd( MONTH , 60, SpellStartDate ) < dateadd ( month , 2 , (@reportingdate) )		-- 2 = 1 months time (4) = Q
			and  dateadd( MONTH , 60, SpellStartDate ) > dateadd ( month , 1 , (@reportingdate) )		-- 1 = 1 months time (1) = Q 
			group by OriginatingCCGICSName , [CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) ) , PatientCategory
				
-----------------------------------------------------------------------------------------------------------------------------------------------------
-- Total number of patients who will tip in to > 5 years in 1 months time End -----------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------------------------------

-----------------------------------------------------------------------------------------------------------------------------------------------------
-- Total number of patients who will tip in to > 5 years in 2 months time Start ---------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------------------------------
				 
			insert into #TransformingCare_tblMasterMetric_BackTest
			select OriginatingCCGICSName ,  @reportingdate  , 53 , Count(*)
			, [CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) ) ,1 , PatientCategory,'NA'
			from #TransformingCare_tempLiveData A 
			left join [NHSE_Sandbox_LDP_Shared].[dbo].[InpatientAgeBanding_vw] B on a.UniqueID = b.UniqueID and EOMONTH (@reportingdate) = TimePeriod
			left join [NHSE_Sandbox_LDP_Shared].[dbo].[InpatientLengthOfStay] C on C.UniqueID = a.UniqueID and c.EpisodeID = a.EpisodeID and  EOMONTH (@reportingdate) = c.TimePeriod
			where  a.EpisodeEndDate > EOMONTH(@reportingdate) and a.EpisodeStartDate <= EOMONTH(@reportingdate) 
			and c.SpellLOSBanding = '4-5 Years' 
			and  dateadd( MONTH , 60, SpellStartDate ) < dateadd ( month , 3 , (@reportingdate) )		-- 2 = 1 months time (4) = Q
			and  dateadd( MONTH , 60, SpellStartDate ) > dateadd ( month , 2 , (@reportingdate) )		-- 1 = 1 months time (1) = Q 
			group by OriginatingCCGICSName , [CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) ) , PatientCategory
				
-----------------------------------------------------------------------------------------------------------------------------------------------------
-- Total number of patients who will tip in to > 5 years in 2 months time End -----------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------------------------------

-----------------------------------------------------------------------------------------------------------------------------------------------------
-- Total number of patients who will tip in to > 5 years in 3 months time Start ---------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------------------------------
				 
			insert into #TransformingCare_tblMasterMetric_BackTest
			select OriginatingCCGICSName ,  @reportingdate  , 54 , Count(*)
			, [CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) ) ,1 , PatientCategory,'NA'
			from #TransformingCare_tempLiveData A 
			left join [NHSE_Sandbox_LDP_Shared].[dbo].[InpatientAgeBanding_vw] B on a.UniqueID = b.UniqueID and EOMONTH (@reportingdate) = TimePeriod
			left join [NHSE_Sandbox_LDP_Shared].[dbo].[InpatientLengthOfStay] C on C.UniqueID = a.UniqueID and c.EpisodeID = a.EpisodeID and  EOMONTH (@reportingdate) = c.TimePeriod
			where  a.EpisodeEndDate > EOMONTH(@reportingdate) and a.EpisodeStartDate <= EOMONTH(@reportingdate) 
			and c.SpellLOSBanding = '4-5 Years' 
			and  dateadd( MONTH , 60, SpellStartDate ) < dateadd ( month , 4 , (@reportingdate) )		-- 2 = 1 months time (4) = Q
			and  dateadd( MONTH , 60, SpellStartDate ) > dateadd ( month , 3 , (@reportingdate) )		-- 1 = 1 months time (1) = Q 
			group by OriginatingCCGICSName , [CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) ) , PatientCategory
				
-----------------------------------------------------------------------------------------------------------------------------------------------------
-- Total number of patients who will tip in to > 5 years in 3 months time End -----------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------------------------------

-----------------------------------------------------------------------------------------------------------------------------------------------------
-- Total number of patients who will tip in to > 5 years in Next Quater Start -----------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------------------------------
				 
			insert into #TransformingCare_tblMasterMetric_BackTest
			select OriginatingCCGICSName ,  @reportingdate  , 55 , Count(*) 
			, [CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) ) ,1 , PatientCategory,'NA'
			from #TransformingCare_tempLiveData A 
			left join [NHSE_Sandbox_LDP_Shared].[dbo].[InpatientAgeBanding_vw] B on a.UniqueID = b.UniqueID and EOMONTH (@reportingdate) = TimePeriod
			left join [NHSE_Sandbox_LDP_Shared].[dbo].[InpatientLengthOfStay] C on C.UniqueID = a.UniqueID and c.EpisodeID = a.EpisodeID and  EOMONTH (@reportingdate) = c.TimePeriod
			where  a.EpisodeEndDate > EOMONTH(@reportingdate) and a.EpisodeStartDate <= EOMONTH(@reportingdate) 
			and c.SpellLOSBanding = '4-5 Years' 
			and  dateadd( MONTH , 60, SpellStartDate ) < dateadd ( month , 4 , (@reportingdate) )		-- 2 = 1 months time (4) = Q
			and  dateadd( MONTH , 60, SpellStartDate ) > dateadd ( month , 1 , (@reportingdate) )		-- 1 = 1 months time (1) = Q 
			group by OriginatingCCGICSName , [CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) ) , PatientCategory
				
-----------------------------------------------------------------------------------------------------------------------------------------------------
-- Total number of patients who will tip in to > 5 years in Next Quater End -------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------------------------------

-----------------------------------------------------------------------------------------------------------------------------------------------------
-- Total number of patients who have tipped in to > 1 years Start -----------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------------------------------
				 
			insert into #TransformingCare_tblMasterMetric_BackTest
			select OriginatingCCGICSName ,  @reportingdate  , 56 , Count(*) 
			, [CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) ) ,1 , PatientCategory,'NA'
			from #TransformingCare_tempLiveData A 
			left join [NHSE_Sandbox_LDP_Shared].[dbo].[InpatientAgeBanding_vw] B on a.UniqueID = b.UniqueID and EOMONTH (@reportingdate) = TimePeriod
			left join [NHSE_Sandbox_LDP_Shared].[dbo].[InpatientLengthOfStay] C on C.UniqueID = a.UniqueID and c.EpisodeID = a.EpisodeID and  EOMONTH (@reportingdate) = c.TimePeriod
			where  a.EpisodeEndDate > EOMONTH(@reportingdate) and a.EpisodeStartDate <= EOMONTH(@reportingdate) and c.SpellLOSBanding = '1-2 Years' and (@reportingdate) < dateadd( MONTH , 13, SpellStartDate ) 
			group by OriginatingCCGICSName , [CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) ) , PatientCategory
				
-----------------------------------------------------------------------------------------------------------------------------------------------------
-- Total number of patients who have tipped in to > 1 years End -------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------------------------------

-----------------------------------------------------------------------------------------------------------------------------------------------------
-- Total number of patients who will tip in to > 1 years in 1 months time Start ---------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------------------------------
				 
			insert into #TransformingCare_tblMasterMetric_BackTest
			select OriginatingCCGICSName ,  @reportingdate  , 57 , Count(*)
			, [CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) ) ,1 , PatientCategory,'NA'
			from #TransformingCare_tempLiveData A 
			left join [NHSE_Sandbox_LDP_Shared].[dbo].[InpatientAgeBanding_vw] B on a.UniqueID = b.UniqueID and EOMONTH (@reportingdate) = TimePeriod
			left join [NHSE_Sandbox_LDP_Shared].[dbo].[InpatientLengthOfStay] C on C.UniqueID = a.UniqueID and c.EpisodeID = a.EpisodeID and  EOMONTH (@reportingdate) = c.TimePeriod
			where  a.EpisodeEndDate > EOMONTH(@reportingdate) and a.EpisodeStartDate <= EOMONTH(@reportingdate) 
			and c.SpellLOSBanding = '6-12 Months' 
			and  dateadd( MONTH , 12, SpellStartDate ) < dateadd ( month , 2 , (@reportingdate) )		-- 2 = 1 months time (4) = Q
			and  dateadd( MONTH , 12, SpellStartDate ) > dateadd ( month , 1 , (@reportingdate) )		-- 1 = 1 months time (1) = Q 
			group by OriginatingCCGICSName , [CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) ) , PatientCategory
				
-----------------------------------------------------------------------------------------------------------------------------------------------------
-- Total number of patients who will tip in to > 1 years in 1 months time End -----------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------------------------------

-----------------------------------------------------------------------------------------------------------------------------------------------------
-- Total number of patients who will tip in to > 1 years in 2 months time Start ---------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------------------------------
				 
			insert into #TransformingCare_tblMasterMetric_BackTest
			select OriginatingCCGICSName ,  @reportingdate  , 58 , Count(*)
			, [CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) ) ,1 , PatientCategory,'NA'
			from #TransformingCare_tempLiveData A 
			left join [NHSE_Sandbox_LDP_Shared].[dbo].[InpatientAgeBanding_vw] B on a.UniqueID = b.UniqueID and EOMONTH (@reportingdate) = TimePeriod
			left join [NHSE_Sandbox_LDP_Shared].[dbo].[InpatientLengthOfStay] C on C.UniqueID = a.UniqueID and c.EpisodeID = a.EpisodeID and  EOMONTH (@reportingdate) = c.TimePeriod
			where  a.EpisodeEndDate > EOMONTH(@reportingdate) and a.EpisodeStartDate <= EOMONTH(@reportingdate) 
			and c.SpellLOSBanding = '6-12 Months' 
			and  dateadd( MONTH , 12, SpellStartDate ) < dateadd ( month , 3 , (@reportingdate) )		-- 2 = 1 months time (4) = Q
			and  dateadd( MONTH , 12, SpellStartDate ) > dateadd ( month , 2 , (@reportingdate) )		-- 1 = 1 months time (1) = Q 
			group by OriginatingCCGICSName , [CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) ) , PatientCategory
				
-----------------------------------------------------------------------------------------------------------------------------------------------------
-- Total number of patients who will tip in to > 1 years in 2 months time End -----------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------------------------------

-----------------------------------------------------------------------------------------------------------------------------------------------------
-- Total number of patients who will tip in to > 1 years in 3 months time Start ---------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------------------------------
				 
			insert into #TransformingCare_tblMasterMetric_BackTest
			select OriginatingCCGICSName ,  @reportingdate  , 59 , Count(*)
			, [CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) ) ,1 , PatientCategory,'NA'
			from #TransformingCare_tempLiveData A 
			left join [NHSE_Sandbox_LDP_Shared].[dbo].[InpatientAgeBanding_vw] B on a.UniqueID = b.UniqueID and EOMONTH (@reportingdate) = TimePeriod
			left join [NHSE_Sandbox_LDP_Shared].[dbo].[InpatientLengthOfStay] C on C.UniqueID = a.UniqueID and c.EpisodeID = a.EpisodeID and  EOMONTH (@reportingdate) = c.TimePeriod
			where  a.EpisodeEndDate > EOMONTH(@reportingdate) and a.EpisodeStartDate <= EOMONTH(@reportingdate) 
			and c.SpellLOSBanding = '6-12 Months' 
			and  dateadd( MONTH , 12, SpellStartDate ) < dateadd ( month , 4 , (@reportingdate) )		-- 2 = 1 months time (4) = Q
			and  dateadd( MONTH , 12, SpellStartDate ) > dateadd ( month , 3 , (@reportingdate) )		-- 1 = 1 months time (1) = Q 
			group by OriginatingCCGICSName , [CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) ) , PatientCategory
				
-----------------------------------------------------------------------------------------------------------------------------------------------------
-- Total number of patients who will tip in to > 1 years in 3 months time End -----------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------------------------------

-----------------------------------------------------------------------------------------------------------------------------------------------------
-- Total number of patients who will tip in to > 1 years in Next Quater Start -----------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------------------------------
				 
			insert into #TransformingCare_tblMasterMetric_BackTest
			select OriginatingCCGICSName ,  @reportingdate  , 60 , Count(*) 
			, [CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) ) ,1 , PatientCategory,'NA'
			from #TransformingCare_tempLiveData A 
			left join [NHSE_Sandbox_LDP_Shared].[dbo].[InpatientAgeBanding_vw] B on a.UniqueID = b.UniqueID and EOMONTH (@reportingdate) = TimePeriod
			left join [NHSE_Sandbox_LDP_Shared].[dbo].[InpatientLengthOfStay] C on C.UniqueID = a.UniqueID and c.EpisodeID = a.EpisodeID and  EOMONTH (@reportingdate) = c.TimePeriod
			where  a.EpisodeEndDate > EOMONTH(@reportingdate) and a.EpisodeStartDate <= EOMONTH(@reportingdate) 
			and c.SpellLOSBanding = '6-12 Months' 
			and  dateadd( MONTH , 12, SpellStartDate ) < dateadd ( month , 4 , (@reportingdate) )		-- 2 = 1 months time (4) = Q
			and  dateadd( MONTH , 12, SpellStartDate ) > dateadd ( month , 1 , (@reportingdate) )		-- 1 = 1 months time (1) = Q 
			group by OriginatingCCGICSName , [CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) ) , PatientCategory
				
-----------------------------------------------------------------------------------------------------------------------------------------------------
-- Total number of patients who will tip in to > 1 years in Next Quater End -------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------------------------------

----------------------------------------------------------------------------------------------------------------------------------------------------
-- Repeat U18 CTR Denominator Start ----------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------------------------------	

		insert into #TransformingCare_tblMasterMetric_BackTest
		select   OriginatingCCGICSName , @reportingdate , 61 , count(*) as C 
		, [CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) ) , 1 , PatientCategory,'NA'
		from #TransformingCare_tempLiveData A
		left join [NHSE_Sandbox_LDP_Shared].[dbo].[InpatientAgeBanding_vw] D on a.UniqueID = D.UniqueID and a.EpisodeStartDate = D.EpisodeStartDate and d.TimePeriod = EOMONTH(@reportingdate)
		left join [NHSE_Sandbox_LDP_Shared].[dbo].[InpatientLengthOfStay] H on a.UniqueID = H.UniqueID and a.SpellID = h.SpellID and a.EpisodeID = h.EpisodeID and h.TimePeriod = EOMONTH(@reportingdate)
		outer apply ( 	select max(CTRDate) as MAX_PreCTRDate from [NHSE_Sandbox_LDP_Shared].[dbo].CleansedCTRDates 
		where a.UniqueID = UniqueID and a.SpellID = SpellID  and CTRDate > '2013-03-01' and CTRDate < @ReportingDate	group by UniqueID	)  as K 	
		where OriginatingCCGRegionName = 'LONDON'  and a.EpisodeEndDate > (EOMONTH(@reportingdate)) and a.EpisodeStartDate <= (EOMONTH(@reportingdate)) 
		and d.AgeBand = 'U18'
		and iif ( cast( cast(EOMONTH(@reportingdate) as datetime) - a.LoSStartDate   as int ) < 92 , 0 , 1  ) = 1 
		group by 	a.OriginatingCCGICSName , A.[CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) ), PatientCategory
		order by 	a.OriginatingCCGICSName , A.[CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) ), PatientCategory

----------------------------------------------------------------------------------------------------------------------------------------------------
-- Repeat U18 CTR Denominator End  -----------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------------------------------	

----------------------------------------------------------------------------------------------------------------------------------------------------
-- Repeat U18 CTR Numerator Start  -----------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------------------------------
		  
		insert into #TransformingCare_tblMasterMetric_BackTest
		select   OriginatingCCGICSName , @reportingdate , 62 , count(*) as C 
		, [CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) ) , 1 , PatientCategory,'NA'
		from #TransformingCare_tempLiveData A
		left join [NHSE_Sandbox_LDP_Shared].[dbo].[InpatientAgeBanding_vw] D on a.UniqueID = D.UniqueID and a.EpisodeStartDate = D.EpisodeStartDate and d.TimePeriod = EOMONTH(@reportingdate)
		left join [NHSE_Sandbox_LDP_Shared].[dbo].[InpatientLengthOfStay] H on a.UniqueID = H.UniqueID and a.SpellID = h.SpellID and a.EpisodeID = h.EpisodeID and h.TimePeriod = EOMONTH(@reportingdate)
		outer apply ( 	select max(CTRDate) as MAX_PreCTRDate from [NHSE_Sandbox_LDP_Shared].[dbo].CleansedCTRDates 
		where a.UniqueID = UniqueID and a.SpellID = SpellID  and CTRDate > '2013-03-01' and CTRDate < EOMONTH(@reportingdate)	group by UniqueID	)  as K 	
		where OriginatingCCGRegionName = 'LONDON'  and a.EpisodeEndDate > (EOMONTH(@reportingdate)) and a.EpisodeStartDate <= (EOMONTH(@reportingdate)) 
		and d.AgeBand = 'U18' 
		and iif (  d.AgeBand = 'U18' , iif ( k.MAX_PreCTRDate is null , 0 , iif ( cast( cast(EOMONTH(@reportingdate) as datetime) - a.LoSStartDate as int ) < 92 , 0 , iif ( EOMONTH(DATEADD(month,-3,EOMONTH(@reportingdate))) > k.MAX_PreCTRDate , 0 , 1)   ) ) , 0 ) = 1 
		group by 	a.OriginatingCCGICSName , A.[CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) ), PatientCategory
		order by 	a.OriginatingCCGICSName , A.[CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) ), PatientCategory
		  
----------------------------------------------------------------------------------------------------------------------------------------------------
-- Repeat U18 CTR Numerator End  -------------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------------------------------

----------------------------------------------------------------------------------------------------------------------------------------------------
-- Repeat None Secure CTR Denominator Start --------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------------------------------
		 
		insert into #TransformingCare_tblMasterMetric_BackTest
		select   OriginatingCCGICSName , @reportingdate , 63 , count(*) as C 
		, [CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) ) , 1 , PatientCategory,'NA'
		from #TransformingCare_tempLiveData A
		left join [NHSE_Sandbox_LDP_Shared].[dbo].[InpatientAgeBanding_vw] D on a.UniqueID = D.UniqueID and a.EpisodeStartDate = D.EpisodeStartDate and d.TimePeriod = EOMONTH(@reportingdate)
		left join [NHSE_Sandbox_LDP_Shared].[dbo].[InpatientLengthOfStay] H on a.UniqueID = H.UniqueID and a.SpellID = h.SpellID and a.EpisodeID = h.EpisodeID and h.TimePeriod = EOMONTH(@reportingdate)
		outer apply ( 	select max(CTRDate) as MAX_PreCTRDate from [NHSE_Sandbox_LDP_Shared].[dbo].CleansedCTRDates 
		where a.UniqueID = UniqueID and a.SpellID = SpellID  and CTRDate > '2013-03-01' and CTRDate < EOMONTH(@reportingdate)	group by UniqueID	)  as K 	
		where OriginatingCCGRegionName = 'LONDON'  and a.EpisodeEndDate > (EOMONTH(@reportingdate)) and a.EpisodeStartDate <= (EOMONTH(@reportingdate)) 
		and d.AgeBand <> 'U18' and  WardSecurityLevel not like '% Secure'
		and  iif (  d.AgeBand <> 'U18' and WardSecurityLevel not like '% Secure' , iif ( cast( cast(EOMONTH(@reportingdate) as datetime) - a.LoSStartDate as int ) < 183 , 0 , 1  ) , 0 ) = 1 
		group by 	a.OriginatingCCGICSName , A.[CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) ), PatientCategory
		order by 	a.OriginatingCCGICSName , A.[CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) ), PatientCategory
		  
----------------------------------------------------------------------------------------------------------------------------------------------------
-- Repeat None Secure CTR Denominator End  ---------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------------------------------

----------------------------------------------------------------------------------------------------------------------------------------------------
-- Repeat None Secure CTR Numerator Start  ---------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------------------------------
		  
		insert into #TransformingCare_tblMasterMetric_BackTest
		select   OriginatingCCGICSName , @reportingdate , 64 , count(*) as C 
		, [CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) ) , 1 , PatientCategory,'NA'
		from #TransformingCare_tempLiveData A
		left join [NHSE_Sandbox_LDP_Shared].[dbo].[InpatientAgeBanding_vw] D on a.UniqueID = D.UniqueID and a.EpisodeStartDate = D.EpisodeStartDate and d.TimePeriod = EOMONTH(@reportingdate)
		left join [NHSE_Sandbox_LDP_Shared].[dbo].[InpatientLengthOfStay] H on a.UniqueID = H.UniqueID and a.SpellID = h.SpellID and a.EpisodeID = h.EpisodeID and h.TimePeriod = EOMONTH(@reportingdate)
		outer apply ( 	select max(CTRDate) as MAX_PreCTRDate from [NHSE_Sandbox_LDP_Shared].[dbo].CleansedCTRDates 
		where a.UniqueID = UniqueID and a.SpellID = SpellID  and CTRDate > '2013-03-01' and CTRDate < EOMONTH(@reportingdate)	group by UniqueID	)  as K 	
		where OriginatingCCGRegionName = 'LONDON'  and a.EpisodeEndDate > (EOMONTH(@reportingdate)) and a.EpisodeStartDate <= (EOMONTH(@reportingdate)) 
		and d.AgeBand <> 'U18' and  WardSecurityLevel not like '% Secure'
		and iif (  d.AgeBand <> 'U18' and WardSecurityLevel not like '% Secure' , iif ( k.MAX_PreCTRDate is null , 0 , iif ( cast( cast(EOMONTH(@reportingdate) as datetime) - a.LoSStartDate as int ) < 183 , 0 , iif ( EOMONTH(DATEADD(month,-6,EOMONTH(@reportingdate))) > k.MAX_PreCTRDate , 0 , 1)   ) ) , 0 ) = 1 
		group by 	a.OriginatingCCGICSName , A.[CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) ), PatientCategory
		order by 	a.OriginatingCCGICSName , A.[CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) ), PatientCategory
		  
----------------------------------------------------------------------------------------------------------------------------------------------------
-- Repeat None Secure CTR Numerator End -----------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------------------------------
		
----------------------------------------------------------------------------------------------------------------------------------------------------
-- Repeat None Secure CTR Excl SRS Denominator Start  ----------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------------------------------
		 
		insert into #TransformingCare_tblMasterMetric_BackTest
		select   OriginatingCCGICSName , @reportingdate , 65 , count(*) as C 
		, [CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) ) , 1 , PatientCategory,'NA'
		from #TransformingCare_tempLiveData A
		left join [NHSE_Sandbox_LDP_Shared].[dbo].[InpatientAgeBanding_vw] D on a.UniqueID = D.UniqueID and a.EpisodeStartDate = D.EpisodeStartDate and d.TimePeriod = EOMONTH(@reportingdate)
		left join [NHSE_Sandbox_LDP_Shared].[dbo].[InpatientLengthOfStay] H on a.UniqueID = H.UniqueID and a.SpellID = h.SpellID and a.EpisodeID = h.EpisodeID and h.TimePeriod = EOMONTH(@reportingdate)
		outer apply ( 	select max(CTRDate) as MAX_PreCTRDate from [NHSE_Sandbox_LDP_Shared].[dbo].CleansedCTRDates 
		where a.UniqueID = UniqueID and a.SpellID = SpellID  and CTRDate > '2013-03-01' and CTRDate < EOMONTH(@reportingdate)	group by UniqueID	)  as K 	
		where OriginatingCCGRegionName = 'LONDON'  and a.EpisodeEndDate > (EOMONTH(@reportingdate)) and a.EpisodeStartDate <= (EOMONTH(@reportingdate)) 
		and d.AgeBand <> 'U18' and  WardSecurityLevel not like '% Secure' and A.providerName <> 'HERTFORDSHIRE PARTNERSHIP UNIVERSITY NHS FT' 
		and  iif (  d.AgeBand <> 'U18' and WardSecurityLevel not like '% Secure' , iif ( cast( cast(EOMONTH(@reportingdate) as datetime) - a.LoSStartDate as int ) < 183 , 0 , 1  ) , 0 ) = 1 
		group by 	a.OriginatingCCGICSName , A.[CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) ), PatientCategory
		order by 	a.OriginatingCCGICSName , A.[CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) ), PatientCategory
		  
----------------------------------------------------------------------------------------------------------------------------------------------------
-- Repeat None Secure CTR Excl SRS Denominator End -------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------------------------------

----------------------------------------------------------------------------------------------------------------------------------------------------
-- Repeat None Secure CTR Excl SRS Numerator Start  ------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------------------------------
  
		insert into #TransformingCare_tblMasterMetric_BackTest
		select   OriginatingCCGICSName , @reportingdate , 66 , count(*) as C 
		, [CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) ) , 1 , PatientCategory,'NA'
		from #TransformingCare_tempLiveData A
		left join [NHSE_Sandbox_LDP_Shared].[dbo].[InpatientAgeBanding_vw] D on a.UniqueID = D.UniqueID and a.EpisodeStartDate = D.EpisodeStartDate and d.TimePeriod = EOMONTH(@reportingdate)
		left join [NHSE_Sandbox_LDP_Shared].[dbo].[InpatientLengthOfStay] H on a.UniqueID = H.UniqueID and a.SpellID = h.SpellID and a.EpisodeID = h.EpisodeID and h.TimePeriod = EOMONTH(@reportingdate)
		outer apply ( 	select max(CTRDate) as MAX_PreCTRDate from [NHSE_Sandbox_LDP_Shared].[dbo].CleansedCTRDates 
		where a.UniqueID = UniqueID and a.SpellID = SpellID  and CTRDate > '2013-03-01' and CTRDate < EOMONTH(@reportingdate)	group by UniqueID	)  as K 	
		where OriginatingCCGRegionName = 'LONDON'  and a.EpisodeEndDate > (EOMONTH(@reportingdate)) and a.EpisodeStartDate <= (EOMONTH(@reportingdate)) 
		and d.AgeBand <> 'U18' and  WardSecurityLevel not like '% Secure' and A.providerName <> 'HERTFORDSHIRE PARTNERSHIP UNIVERSITY NHS FT' 
		and iif (  d.AgeBand <> 'U18' and WardSecurityLevel not like '% Secure' , iif ( k.MAX_PreCTRDate is null , 0 , iif ( cast( cast(EOMONTH(@reportingdate) as datetime) - a.LoSStartDate as int ) < 183 , 0 , iif ( EOMONTH(DATEADD(month,-6,EOMONTH(@reportingdate))) > k.MAX_PreCTRDate , 0 , 1)   ) ) , 0 ) = 1 
		group by 	a.OriginatingCCGICSName , A.[CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) ), PatientCategory
		order by 	a.OriginatingCCGICSName , A.[CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) ), PatientCategory
		 
----------------------------------------------------------------------------------------------------------------------------------------------------
-- Repeat None Secure CTR Excl SRS Numerator End ---------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------------------------------

----------------------------------------------------------------------------------------------------------------------------------------------------
-- Repeat None Secure CTR Denominator Start  -------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------------------------------
		  
		insert into #TransformingCare_tblMasterMetric_BackTest
		select   OriginatingCCGICSName , @reportingdate , 67 , count(*) as C 
		, [CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) ) , 1 , PatientCategory,'NA'
		from #TransformingCare_tempLiveData A
		left join [NHSE_Sandbox_LDP_Shared].[dbo].[InpatientAgeBanding_vw] D on a.UniqueID = D.UniqueID and a.EpisodeStartDate = D.EpisodeStartDate and d.TimePeriod = EOMONTH(@reportingdate)
		left join [NHSE_Sandbox_LDP_Shared].[dbo].[InpatientLengthOfStay] H on a.UniqueID = H.UniqueID and a.SpellID = h.SpellID and a.EpisodeID = h.EpisodeID and h.TimePeriod = EOMONTH(@reportingdate)
		outer apply ( 	select max(CTRDate) as MAX_PreCTRDate from [NHSE_Sandbox_LDP_Shared].[dbo].CleansedCTRDates 
		where a.UniqueID = UniqueID and a.SpellID = SpellID  and CTRDate > '2013-03-01' and CTRDate < EOMONTH(@reportingdate)	group by UniqueID	)  as K 	
		where OriginatingCCGRegionName = 'LONDON'  and a.EpisodeEndDate > (EOMONTH(@reportingdate)) and a.EpisodeStartDate <= (EOMONTH(@reportingdate)) 
		and d.AgeBand <> 'U18'  and  WardSecurityLevel like '% Secure'
		and  iif (  d.AgeBand <> 'U18' and WardSecurityLevel like '% Secure' , iif ( cast( cast(EOMONTH(@reportingdate) as datetime) - a.LoSStartDate as int ) < 366 , 0 , 1  ) , 0 ) = 1 
		group by 	a.OriginatingCCGICSName , A.[CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) ), PatientCategory
		order by 	a.OriginatingCCGICSName , A.[CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) ), PatientCategory
		  
----------------------------------------------------------------------------------------------------------------------------------------------------
-- Repeat None Secure CTR Denominator End  ---------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------------------------------

----------------------------------------------------------------------------------------------------------------------------------------------------
-- Repeat None Secure CTR Numerator Start ----------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------------------------------
		  
		insert into #TransformingCare_tblMasterMetric_BackTest
		select   OriginatingCCGICSName , @reportingdate , 68 , count(*) as C 
		, [CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) ) , 1 , PatientCategory,'NA'
		from #TransformingCare_tempLiveData A
		left join [NHSE_Sandbox_LDP_Shared].[dbo].[InpatientAgeBanding_vw] D on a.UniqueID = D.UniqueID and a.EpisodeStartDate = D.EpisodeStartDate and d.TimePeriod = EOMONTH(@reportingdate)
		left join [NHSE_Sandbox_LDP_Shared].[dbo].[InpatientLengthOfStay] H on a.UniqueID = H.UniqueID and a.SpellID = h.SpellID and a.EpisodeID = h.EpisodeID and h.TimePeriod = EOMONTH(@reportingdate)
		outer apply ( 	select max(CTRDate) as MAX_PreCTRDate from [NHSE_Sandbox_LDP_Shared].[dbo].CleansedCTRDates 
		where a.UniqueID = UniqueID and a.SpellID = SpellID  and CTRDate > '2013-03-01' and CTRDate < EOMONTH(@reportingdate)	group by UniqueID	)  as K 	
		where OriginatingCCGRegionName = 'LONDON'  and a.EpisodeEndDate > (EOMONTH(@reportingdate)) and a.EpisodeStartDate <= (EOMONTH(@reportingdate)) 
		and d.AgeBand <> 'U18'  and  WardSecurityLevel like '% Secure'
		and iif (  d.AgeBand <> 'U18' and WardSecurityLevel like '% Secure' , iif ( k.MAX_PreCTRDate is null , 0 , iif ( cast( cast(EOMONTH(@reportingdate) as datetime) - a.LoSStartDate as int ) < 366 , 0 , iif ( EOMONTH(DATEADD(year,-1,EOMONTH(@reportingdate))) > k.MAX_PreCTRDate , 0 , 1)   ) ) , 0 )  = 1 
		group by 	a.OriginatingCCGICSName , A.[CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) ), PatientCategory
		order by 	a.OriginatingCCGICSName , A.[CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) ), PatientCategory
		 
----------------------------------------------------------------------------------------------------------------------------------------------------
-- Repeat None Secure CTR Numerator End ------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------------------------------

----------------------------------------------------------------------------------------------------------------------------------------------------
-- Repeat None Secure CTR Excl SRS Denominator Start   ---------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------------------------------
		 
		insert into #TransformingCare_tblMasterMetric_BackTest
		select   OriginatingCCGICSName , @reportingdate , 69 , count(*) as C 
		, [CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) ) , 1 , PatientCategory,'NA'
		from #TransformingCare_tempLiveData A
		left join [NHSE_Sandbox_LDP_Shared].[dbo].[InpatientAgeBanding_vw] D on a.UniqueID = D.UniqueID and a.EpisodeStartDate = D.EpisodeStartDate and d.TimePeriod = EOMONTH(@reportingdate)
		left join [NHSE_Sandbox_LDP_Shared].[dbo].[InpatientLengthOfStay] H on a.UniqueID = H.UniqueID and a.SpellID = h.SpellID and a.EpisodeID = h.EpisodeID and h.TimePeriod = EOMONTH(@reportingdate)
		outer apply ( 	select max(CTRDate) as MAX_PreCTRDate from [NHSE_Sandbox_LDP_Shared].[dbo].CleansedCTRDates 
		where a.UniqueID = UniqueID and a.SpellID = SpellID  and CTRDate > '2013-03-01' and CTRDate < EOMONTH(@reportingdate)	group by UniqueID	)  as K 	
		where OriginatingCCGRegionName = 'LONDON'  and a.EpisodeEndDate > (EOMONTH(@reportingdate)) and a.EpisodeStartDate <= (EOMONTH(@reportingdate)) 
		and d.AgeBand <> 'U18'  and  WardSecurityLevel like '% Secure' and A.providerName <> 'HERTFORDSHIRE PARTNERSHIP UNIVERSITY NHS FT' 
		and  iif (  d.AgeBand <> 'U18' and WardSecurityLevel like '% Secure' , iif ( cast( cast(EOMONTH(@reportingdate) as datetime) - a.LoSStartDate as int ) < 366 , 0 , 1  ) , 0 ) = 1 
		group by 	a.OriginatingCCGICSName , A.[CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) ), PatientCategory
		order by 	a.OriginatingCCGICSName , A.[CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) ), PatientCategory
		 
----------------------------------------------------------------------------------------------------------------------------------------------------
-- Repeat None Secure CTR Excl SRS Denominator End  ------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------------------------------

----------------------------------------------------------------------------------------------------------------------------------------------------
--Repeat None Secure CTR Excl SRS Numerator Start --------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------------------------------

		insert into #TransformingCare_tblMasterMetric_BackTest
		select   OriginatingCCGICSName , @reportingdate , 70 , count(*) as C 
		, [CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) ) , 1 , PatientCategory,'NA'
		from #TransformingCare_tempLiveData A
		left join [NHSE_Sandbox_LDP_Shared].[dbo].[InpatientAgeBanding_vw] D on a.UniqueID = D.UniqueID and a.EpisodeStartDate = D.EpisodeStartDate and d.TimePeriod = EOMONTH(@reportingdate)
		left join [NHSE_Sandbox_LDP_Shared].[dbo].[InpatientLengthOfStay] H on a.UniqueID = H.UniqueID and a.SpellID = h.SpellID and a.EpisodeID = h.EpisodeID and h.TimePeriod = EOMONTH(@reportingdate)
		outer apply ( 	select max(CTRDate) as MAX_PreCTRDate from [NHSE_Sandbox_LDP_Shared].[dbo].CleansedCTRDates 
		where a.UniqueID = UniqueID and a.SpellID = SpellID  and CTRDate > '2013-03-01' and CTRDate < EOMONTH(@reportingdate)	group by UniqueID	)  as K 	
		where OriginatingCCGRegionName = 'LONDON'  and a.EpisodeEndDate > (EOMONTH(@reportingdate)) and a.EpisodeStartDate <= (EOMONTH(@reportingdate)) 
		and d.AgeBand <> 'U18'  and  WardSecurityLevel like '% Secure' and A.providerName <> 'HERTFORDSHIRE PARTNERSHIP UNIVERSITY NHS FT' 
		and iif (  d.AgeBand <> 'U18' and WardSecurityLevel like '% Secure' , iif ( k.MAX_PreCTRDate is null , 0 , iif ( cast( cast(EOMONTH(@reportingdate) as datetime) - a.LoSStartDate as int ) < 366 , 0 , iif ( EOMONTH(DATEADD(year,-1,EOMONTH(@reportingdate))) > k.MAX_PreCTRDate , 0 , 1)   ) ) , 0 )  = 1 
		group by 	a.OriginatingCCGICSName , A.[CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) ), PatientCategory
		order by 	a.OriginatingCCGICSName , A.[CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) ), PatientCategory
		 
----------------------------------------------------------------------------------------------------------------------------------------------------
-- Repeat None Secure CTR Excl SRS Numerator End  --------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------------------------------

----------------------------------------------------------------------------------------------------------------------------------------------------
--Community/admissions U18 Denominator Start  ------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------------------------------
		  
		insert into #TransformingCare_tblMasterMetric_BackTest
		select   OriginatingCCGICSName , @reportingdate , 71 , count(*) as C 
		, [CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) ) , 1 , PatientCategory,'NA'
		from #TransformingCare_tempLiveData A
		left join [NHSE_Sandbox_LDP_Shared].[dbo].[InpatientAgeBanding_vw] D on a.UniqueID = D.UniqueID and a.EpisodeStartDate = D.EpisodeStartDate and d.TimePeriod = EOMONTH(@reportingdate)
		left join [NHSE_Sandbox_LDP_Shared].[dbo].[InpatientLengthOfStay] H on a.UniqueID = H.UniqueID and a.SpellID = h.SpellID and a.EpisodeID = h.EpisodeID and h.TimePeriod = EOMONTH(@reportingdate)
		outer apply ( 	select max(CTRDate) as MAX_PostCTRDate from [NHSE_Sandbox_LDP_Shared].[dbo].CleansedCTRDates 
		where a.UniqueID = UniqueID and a.SpellID = SpellID and CTRDate > '2013-03-01' and CTRDate >= a.EpisodeStartDate and CTRDate < DATEADD(day,29 ,a.EpisodeStartDate)		group by UniqueID	)  as K 
		outer apply ( 	select max(CTRDate) as MAX_PreCTRDate from [NHSE_Sandbox_LDP_Shared].[dbo].CleansedCTRDates 
		where a.UniqueID = UniqueID  and  CTRDate > '2013-03-01' and CTRDate < a.EpisodeStartDate	and CTRType = 'Community' group by UniqueID	)  as L 
		where OriginatingCCGRegionName = 'LONDON'  and A.NewAdmission = 1 
		and a.EpisodeStartDate > DATEADD(month , -3 , EOMONTH(@reportingdate) ) and a.EpisodeStartDate <= EOMONTH(@reportingdate)
		and  iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) ) = 'U18'
		and iif ( AgeAtPeriodEnd < 18 , iif( cast( a.EpisodeEndDate - a.EpisodeStartDate as int )  < 7 and MAX_PostCTRDate is null  , 0 , 1 ) , 0  )  = 1 
		group by 	a.OriginatingCCGICSName , A.[CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) ), PatientCategory
		order by 	a.OriginatingCCGICSName , A.[CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) ), PatientCategory
		 
----------------------------------------------------------------------------------------------------------------------------------------------------
-- Community/admissions U18 Denominator End   ------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------------------------------

----------------------------------------------------------------------------------------------------------------------------------------------------
--Community/admissions U18 Pre Admission Numerator Start  ------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------------------------------
		  
		insert into #TransformingCare_tblMasterMetric_BackTest
		select   OriginatingCCGICSName , @reportingdate , 72 , count(*) as C 
		, [CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) ) , 1 , PatientCategory,'NA'
		from #TransformingCare_tempLiveData A
		left join [NHSE_Sandbox_LDP_Shared].[dbo].[InpatientAgeBanding_vw] D on a.UniqueID = D.UniqueID and a.EpisodeStartDate = D.EpisodeStartDate and d.TimePeriod = EOMONTH(@reportingdate)
		left join [NHSE_Sandbox_LDP_Shared].[dbo].[InpatientLengthOfStay] H on a.UniqueID = H.UniqueID and a.SpellID = h.SpellID and a.EpisodeID = h.EpisodeID and h.TimePeriod = EOMONTH(@reportingdate)
		outer apply ( 	select max(CTRDate) as MAX_PostCTRDate from [NHSE_Sandbox_LDP_Shared].[dbo].CleansedCTRDates 
		where a.UniqueID = UniqueID and a.SpellID = SpellID and CTRDate > '2013-03-01' and CTRDate >= a.EpisodeStartDate and CTRDate < DATEADD(day,29 ,a.EpisodeStartDate)		group by UniqueID	)  as K 
		outer apply ( 	select max(CTRDate) as MAX_PreCTRDate from [NHSE_Sandbox_LDP_Shared].[dbo].CleansedCTRDates 
		where a.UniqueID = UniqueID  and  CTRDate > '2013-03-01' and CTRDate < a.EpisodeStartDate	and CTRType = 'Community' group by UniqueID	)  as L 	
		where OriginatingCCGRegionName = 'LONDON'  and A.NewAdmission = 1 
		and a.EpisodeStartDate > DATEADD(month , -3 , EOMONTH(@reportingdate) ) and a.EpisodeStartDate <= EOMONTH(@reportingdate) 
				and  iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) ) = 'U18'
		and iif ( AgeAtPeriodEnd < 18 , iif( cast( a.EpisodeEndDate - a.EpisodeStartDate as int )  < 7 and MAX_PostCTRDate is null  , 0 , iif ( cast(a.EpisodeStartDate - L.MAX_PreCTRDate as int ) < 28 ,1 , 0 )  ) , 0  ) = 1 
		group by 	a.OriginatingCCGICSName , A.[CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) ), PatientCategory
		order by 	a.OriginatingCCGICSName , A.[CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) ), PatientCategory
		
----------------------------------------------------------------------------------------------------------------------------------------------------
-- Community/admissions U18 Pre Admission Numerator End  -------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------------------------------

----------------------------------------------------------------------------------------------------------------------------------------------------
-- Community/admissions U18 Post Admission Numerator Start  ----------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------------------------------
		   
		insert into #TransformingCare_tblMasterMetric_BackTest
		select   OriginatingCCGICSName , @reportingdate , 73 , count(*) as C 
		, [CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) ) , 1 , PatientCategory,'NA'
		from #TransformingCare_tempLiveData A
		left join [NHSE_Sandbox_LDP_Shared].[dbo].[InpatientAgeBanding_vw] D on a.UniqueID = D.UniqueID and a.EpisodeStartDate = D.EpisodeStartDate and d.TimePeriod = EOMONTH(@reportingdate)
		left join [NHSE_Sandbox_LDP_Shared].[dbo].[InpatientLengthOfStay] H on a.UniqueID = H.UniqueID and a.SpellID = h.SpellID and a.EpisodeID = h.EpisodeID and h.TimePeriod = EOMONTH(@reportingdate)
		outer apply ( 	select max(CTRDate) as MAX_PostCTRDate from [NHSE_Sandbox_LDP_Shared].[dbo].CleansedCTRDates 
		where a.UniqueID = UniqueID and a.SpellID = SpellID and CTRDate > '2013-03-01' and CTRDate >= a.EpisodeStartDate and CTRDate < DATEADD(day,29 ,a.EpisodeStartDate)		group by UniqueID	)  as K 
		outer apply ( 	select max(CTRDate) as MAX_PreCTRDate from [NHSE_Sandbox_LDP_Shared].[dbo].CleansedCTRDates 
		where a.UniqueID = UniqueID  and  CTRDate > '2013-03-01' and CTRDate < a.EpisodeStartDate	and CTRType = 'Community' group by UniqueID	)  as L 	
		where OriginatingCCGRegionName = 'LONDON'  and A.NewAdmission = 1 
		and a.EpisodeStartDate > DATEADD(month , -3 , EOMONTH(@reportingdate) ) and a.EpisodeStartDate <= EOMONTH(@reportingdate) 
				and  iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) ) = 'U18'
		and iif ( AgeAtPeriodEnd < 18 , iif( cast( a.EpisodeEndDate - a.EpisodeStartDate as int )  < 7 and MAX_PostCTRDate is null  , 0 , iif ( k.MAX_PostCTRDate is null , 0 , iif( cast (MAX_PostCTRDate - a.EpisodeStartDate as int ) <= 7 , 1 , 0 )) ) , 0  ) = 1 
		group by 	a.OriginatingCCGICSName , A.[CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) ), PatientCategory
		order by 	a.OriginatingCCGICSName , A.[CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) ), PatientCategory
		  
----------------------------------------------------------------------------------------------------------------------------------------------------
-- Community/admissions U18 Post Admission Numerator End   -----------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------------------------------

----------------------------------------------------------------------------------------------------------------------------------------------------
-- Community/admissions U18 Covid Post Admission Numerator Start  ---------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------------------------------
		
		insert into #TransformingCare_tblMasterMetric_BackTest
		select   OriginatingCCGICSName , @reportingdate , 74 , count(*) as C 
		, [CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) ) , 1 , PatientCategory,'NA'
		from #TransformingCare_tempLiveData A
		left join [NHSE_Sandbox_LDP_Shared].[dbo].[InpatientAgeBanding_vw] D on a.UniqueID = D.UniqueID and a.EpisodeStartDate = D.EpisodeStartDate and d.TimePeriod = EOMONTH(@reportingdate)
		left join [NHSE_Sandbox_LDP_Shared].[dbo].[InpatientLengthOfStay] H on a.UniqueID = H.UniqueID and a.SpellID = h.SpellID and a.EpisodeID = h.EpisodeID and h.TimePeriod = EOMONTH(@reportingdate)
		outer apply ( 	select max(CTRDate) as MAX_PostCTRDate from [NHSE_Sandbox_LDP_Shared].[dbo].CleansedCTRDates 
		where a.UniqueID = UniqueID and a.SpellID = SpellID and CTRDate > '2013-03-01' and CTRDate >= a.EpisodeStartDate and CTRDate < DATEADD(day,29 ,a.EpisodeStartDate)		group by UniqueID	)  as K 
		outer apply ( 	select max(CTRDate) as MAX_PreCTRDate from [NHSE_Sandbox_LDP_Shared].[dbo].CleansedCTRDates 
		where a.UniqueID = UniqueID  and  CTRDate > '2013-03-01' and CTRDate < a.EpisodeStartDate	and CTRType = 'Community' group by UniqueID	)  as L 	
		where OriginatingCCGRegionName = 'LONDON'  and A.NewAdmission = 1 
		and a.EpisodeStartDate > DATEADD(month , -3 , EOMONTH(@reportingdate) ) and a.EpisodeStartDate <= EOMONTH(@reportingdate)  
				and  iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) ) = 'U18'
		and iif ( AgeAtPeriodEnd < 18 , iif( cast( a.EpisodeEndDate - a.EpisodeStartDate as int )  < 7 and MAX_PostCTRDate is null  , 0 , iif ( k.MAX_PostCTRDate is null , 0 , iif( cast (MAX_PostCTRDate - a.EpisodeStartDate as int ) <= 14 , 1 , 0 )) ) , 0  ) = 1 
		group by 	a.OriginatingCCGICSName , A.[CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) ), PatientCategory
		order by 	a.OriginatingCCGICSName , A.[CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) ), PatientCategory
		  
----------------------------------------------------------------------------------------------------------------------------------------------------
-- Community/admissions U18 Covid Post Admission Numerator End  -----------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------------------------------

----------------------------------------------------------------------------------------------------------------------------------------------------
-- Community/admissions U18 Total Numerator Start  ---------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------------------------------
		
		insert into #TransformingCare_tblMasterMetric_BackTest
		select   OriginatingCCGICSName , @reportingdate , 75 , count(*) as C 
		, [CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) ) , 1 , PatientCategory,'NA'
		from #TransformingCare_tempLiveData A
		left join [NHSE_Sandbox_LDP_Shared].[dbo].[InpatientAgeBanding_vw] D on a.UniqueID = D.UniqueID and a.EpisodeStartDate = D.EpisodeStartDate and d.TimePeriod = EOMONTH(@reportingdate)
		left join [NHSE_Sandbox_LDP_Shared].[dbo].[InpatientLengthOfStay] H on a.UniqueID = H.UniqueID and a.SpellID = h.SpellID and a.EpisodeID = h.EpisodeID and h.TimePeriod = EOMONTH(@reportingdate)
		outer apply ( 	select max(CTRDate) as MAX_PostCTRDate from [NHSE_Sandbox_LDP_Shared].[dbo].CleansedCTRDates 
		where a.UniqueID = UniqueID and a.SpellID = SpellID and CTRDate > '2013-03-01' and CTRDate >= a.EpisodeStartDate and CTRDate < DATEADD(day,29 ,a.EpisodeStartDate)		group by UniqueID	)  as K 
		outer apply ( 	select max(CTRDate) as MAX_PreCTRDate from [NHSE_Sandbox_LDP_Shared].[dbo].CleansedCTRDates 
		where a.UniqueID = UniqueID  and  CTRDate > '2013-03-01' and CTRDate < a.EpisodeStartDate	and CTRType = 'Community' group by UniqueID	)  as L 	
		where OriginatingCCGRegionName = 'LONDON'  and A.NewAdmission = 1 
		and a.EpisodeStartDate > DATEADD(month , -3 , EOMONTH(@reportingdate) ) and a.EpisodeStartDate <= EOMONTH(@reportingdate) 
				and  iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) ) = 'U18'
		and (  iif ( AgeAtPeriodEnd < 18 , iif( cast( a.EpisodeEndDate - a.EpisodeStartDate as int )  < 7 and MAX_PostCTRDate is null  , 0 , iif ( cast(a.EpisodeStartDate - L.MAX_PreCTRDate as int ) < 28 ,1 , 0 )  ) , 0  ) = 1  
		or iif ( AgeAtPeriodEnd < 18 , iif( cast( a.EpisodeEndDate - a.EpisodeStartDate as int )  < 7 and MAX_PostCTRDate is null  , 0 , iif ( k.MAX_PostCTRDate is null , 0 , iif( cast (MAX_PostCTRDate - a.EpisodeStartDate as int ) <= 7 , 1 , 0 )) ) , 0  ) = 1  ) 
		group by 	a.OriginatingCCGICSName , A.[CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) ), PatientCategory
		order by 	a.OriginatingCCGICSName , A.[CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) ), PatientCategory
		  
----------------------------------------------------------------------------------------------------------------------------------------------------
-- Community/admissions U18 Total Numerator End  ---------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------------------------------

----------------------------------------------------------------------------------------------------------------------------------------------------
-- Community/admissions U18 Total Covid Numerator Start  -------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------------------------------
		
		insert into #TransformingCare_tblMasterMetric_BackTest
		select   OriginatingCCGICSName , @reportingdate , 76 , count(*) as C 
		, [CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) ) , 1 , PatientCategory,'NA'
		from #TransformingCare_tempLiveData A
		left join [NHSE_Sandbox_LDP_Shared].[dbo].[InpatientAgeBanding_vw] D on a.UniqueID = D.UniqueID and a.EpisodeStartDate = D.EpisodeStartDate and d.TimePeriod = EOMONTH(@reportingdate)
		left join [NHSE_Sandbox_LDP_Shared].[dbo].[InpatientLengthOfStay] H on a.UniqueID = H.UniqueID and a.SpellID = h.SpellID and a.EpisodeID = h.EpisodeID and h.TimePeriod = EOMONTH(@reportingdate)
		outer apply ( 	select max(CTRDate) as MAX_PostCTRDate from [NHSE_Sandbox_LDP_Shared].[dbo].CleansedCTRDates 
		where a.UniqueID = UniqueID and a.SpellID = SpellID and CTRDate > '2013-03-01' and CTRDate >= a.EpisodeStartDate and CTRDate < DATEADD(day,29 ,a.EpisodeStartDate)		group by UniqueID	)  as K 
		outer apply ( 	select max(CTRDate) as MAX_PreCTRDate from [NHSE_Sandbox_LDP_Shared].[dbo].CleansedCTRDates 
		where a.UniqueID = UniqueID  and  CTRDate > '2013-03-01' and CTRDate < a.EpisodeStartDate	and CTRType = 'Community' group by UniqueID	)  as L 	
		where OriginatingCCGRegionName = 'LONDON'  and A.NewAdmission = 1 
		and a.EpisodeStartDate > DATEADD(month , -3 , EOMONTH(@reportingdate) ) and a.EpisodeStartDate <= EOMONTH(@reportingdate) 
				and  iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) ) = 'U18'
		and (  iif ( AgeAtPeriodEnd < 18 , iif( cast( a.EpisodeEndDate - a.EpisodeStartDate as int )  < 7 and MAX_PostCTRDate is null  , 0 , iif ( cast(a.EpisodeStartDate - L.MAX_PreCTRDate as int ) < 28 ,1 , 0 )  ) , 0  ) = 1  
		or iif ( AgeAtPeriodEnd < 18 , iif( cast( a.EpisodeEndDate - a.EpisodeStartDate as int )  < 7 and MAX_PostCTRDate is null  , 0 , iif ( k.MAX_PostCTRDate is null , 0 , iif( cast (MAX_PostCTRDate - a.EpisodeStartDate as int ) <= 14 , 1 , 0 )) ) , 0  ) = 1  ) 
		group by a.OriginatingCCGICSName , A.[CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) ), PatientCategory
		order by a.OriginatingCCGICSName , A.[CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) ), PatientCategory
		  
----------------------------------------------------------------------------------------------------------------------------------------------------
-- Community/admissions U18 Total Covid Numerator End  ---------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------------------------------

----------------------------------------------------------------------------------------------------------------------------------------------------
-- Community/admissions Adults Denominator Start ---------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------------------------------

	insert into #TransformingCare_tblMasterMetric_BackTest
	select   OriginatingCCGICSName , @reportingdate , 77 , count(*) as C 
	, [CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) ) , 1 , PatientCategory,'NA'
	from #TransformingCare_tempLiveData A
	left join [NHSE_Sandbox_LDP_Shared].[dbo].[InpatientAgeBanding_vw] D on a.UniqueID = D.UniqueID and a.EpisodeStartDate = D.EpisodeStartDate and d.TimePeriod = EOMONTH(@reportingdate)
	left join [NHSE_Sandbox_LDP_Shared].[dbo].[InpatientLengthOfStay] H on a.UniqueID = H.UniqueID and a.SpellID = h.SpellID and a.EpisodeID = h.EpisodeID and h.TimePeriod = EOMONTH(@reportingdate)
	outer apply ( 	select max(CTRDate) as MAX_PostCTRDate from [NHSE_Sandbox_LDP_Shared].[dbo].CleansedCTRDates 
	where a.UniqueID = UniqueID and a.SpellID = SpellID and CTRDate > '2013-03-01' and CTRDate >= a.EpisodeStartDate and CTRDate < DATEADD(day,29 ,a.EpisodeStartDate)		group by UniqueID	)  as K 
	outer apply ( 	select max(CTRDate) as MAX_PreCTRDate from [NHSE_Sandbox_LDP_Shared].[dbo].CleansedCTRDates 
	where a.UniqueID = UniqueID  and  CTRDate > '2013-03-01' and CTRDate < a.EpisodeStartDate	and CTRType = 'Community' group by UniqueID	)  as L 
	where OriginatingCCGRegionName = 'LONDON'  and A.NewAdmission = 1 
			and  iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) ) <> 'U18'
	and a.EpisodeStartDate > DATEADD(month , -3 , cast( EOMONTH(@reportingdate) as datetime) ) and a.EpisodeStartDate <=  cast(EOMONTH(@reportingdate) as datetime) 
	and iif ( AgeAtPeriodEnd >= 18 , iif( cast( a.EpisodeEndDate - a.EpisodeStartDate as int )  < 28 and MAX_PostCTRDate is null ,0 ,1) , 0  ) = 1 
	group by 	a.OriginatingCCGICSName , A.[CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) ), PatientCategory
	order by 	a.OriginatingCCGICSName , A.[CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) ), PatientCategory
	 
----------------------------------------------------------------------------------------------------------------------------------------------------
-- Community/admissions Adults Denominator End   ---------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------------------------------

----------------------------------------------------------------------------------------------------------------------------------------------------
-- Community/admissions Adults Pre Admission Numerator Start  --------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------------------------------
   
    insert into #TransformingCare_tblMasterMetric_BackTest
	select   OriginatingCCGICSName , @reportingdate , 78 , count(*) as C 
	, [CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) ) , 1 , PatientCategory,'NA'
	from #TransformingCare_tempLiveData A
	left join [NHSE_Sandbox_LDP_Shared].[dbo].[InpatientAgeBanding_vw] D on a.UniqueID = D.UniqueID and a.EpisodeStartDate = D.EpisodeStartDate and d.TimePeriod = (@reportingdate)
	left join [NHSE_Sandbox_LDP_Shared].[dbo].[InpatientLengthOfStay] H on a.UniqueID = H.UniqueID and a.SpellID = h.SpellID and a.EpisodeID = h.EpisodeID and h.TimePeriod = (@reportingdate)
	outer apply ( 	select max(CTRDate) as MAX_PostCTRDate from [NHSE_Sandbox_LDP_Shared].[dbo].CleansedCTRDates 
	where a.UniqueID = UniqueID and a.SpellID = SpellID and CTRDate > '2013-03-01' and CTRDate >= a.EpisodeStartDate and CTRDate < DATEADD(day,29 ,a.EpisodeStartDate)		group by UniqueID	)  as K 
	outer apply ( 	select max(CTRDate) as MAX_PreCTRDate from [NHSE_Sandbox_LDP_Shared].[dbo].CleansedCTRDates 
	where a.UniqueID = UniqueID  and  CTRDate > '2013-03-01' and CTRDate < a.EpisodeStartDate	and CTRType = 'Community' group by UniqueID	)  as L 	
	where OriginatingCCGRegionName = 'LONDON'  and A.NewAdmission = 1 	and  iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) ) <> 'U18'
	and a.EpisodeStartDate > DATEADD(month , -3 , (@reportingdate) ) and a.EpisodeStartDate <= (@reportingdate)  
	and iif ( AgeAtPeriodEnd >= 18 , iif( cast( a.EpisodeEndDate - a.EpisodeStartDate as int )  < 28 and MAX_PostCTRDate is null  , 0 , iif ( cast(a.EpisodeStartDate - L.MAX_PreCTRDate as int ) < 28 ,1 , 0 )  ) , 0  ) = 1 
	group by 	a.OriginatingCCGICSName , A.[CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) ), PatientCategory
	order by 	a.OriginatingCCGICSName , A.[CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) ), PatientCategory
	  
----------------------------------------------------------------------------------------------------------------------------------------------------
-- Community/admissions Adults Pre Admission Numerator End -----------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------------------------------

----------------------------------------------------------------------------------------------------------------------------------------------------
-- Community/admissions Adults Post Admission Numerator Start --------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------------------------------
	  
	insert into #TransformingCare_tblMasterMetric_BackTest
	select   OriginatingCCGICSName , @reportingdate , 79 , count(*) as C 
	, [CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) ) , 1 , PatientCategory,'NA'
	from #TransformingCare_tempLiveData A
	left join [NHSE_Sandbox_LDP_Shared].[dbo].[InpatientAgeBanding_vw] D on a.UniqueID = D.UniqueID and a.EpisodeStartDate = D.EpisodeStartDate and d.TimePeriod = EOMONTH(@reportingdate)
	left join [NHSE_Sandbox_LDP_Shared].[dbo].[InpatientLengthOfStay] H on a.UniqueID = H.UniqueID and a.SpellID = h.SpellID and a.EpisodeID = h.EpisodeID and h.TimePeriod = EOMONTH(@reportingdate)
	outer apply ( 	select max(CTRDate) as MAX_PostCTRDate from [NHSE_Sandbox_LDP_Shared].[dbo].CleansedCTRDates 
	where a.UniqueID = UniqueID and a.SpellID = SpellID and CTRDate > '2013-03-01' and CTRDate >= a.EpisodeStartDate and CTRDate < DATEADD(day,29 ,a.EpisodeStartDate)		group by UniqueID	)  as K 
	outer apply ( 	select max(CTRDate) as MAX_PreCTRDate from [NHSE_Sandbox_LDP_Shared].[dbo].CleansedCTRDates 
	where a.UniqueID = UniqueID  and  CTRDate > '2013-03-01' and CTRDate < a.EpisodeStartDate	and CTRType = 'Community' group by UniqueID	)  as L 	
	where OriginatingCCGRegionName = 'LONDON'  and A.NewAdmission = 1 	and  iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) ) <> 'U18'
	and a.EpisodeStartDate > DATEADD(month , -3 , EOMONTH(@reportingdate) ) and a.EpisodeStartDate <= EOMONTH(@reportingdate)  
	and iif ( AgeAtPeriodEnd >= 18 , iif( cast( a.EpisodeEndDate - a.EpisodeStartDate as int )  < 28 and MAX_PostCTRDate is null , 0 , iif ( k.MAX_PostCTRDate is null , 0 , iif( cast (MAX_PostCTRDate - a.EpisodeStartDate as int ) <= 28 , 1 , 0 )) ) , 0  ) = 1 
	group by 	a.OriginatingCCGICSName , A.[CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) ), PatientCategory
	order by 	a.OriginatingCCGICSName , A.[CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) ), PatientCategory
	  
----------------------------------------------------------------------------------------------------------------------------------------------------
-- Community/admissions Adults Admission Numerator End   -------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------------------------------

----------------------------------------------------------------------------------------------------------------------------------------------------
-- Community/admissions Adults Total Numerator Start --------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------------------------------
	  
	insert into #TransformingCare_tblMasterMetric_BackTest
	select   OriginatingCCGICSName , @reportingdate , 80 , count(*) as C 
	, [CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) ) , 1 , PatientCategory,'NA'
	from #TransformingCare_tempLiveData A
	left join [NHSE_Sandbox_LDP_Shared].[dbo].[InpatientAgeBanding_vw] D on a.UniqueID = D.UniqueID and a.EpisodeStartDate = D.EpisodeStartDate and d.TimePeriod = EOMONTH(@reportingdate)
	left join [NHSE_Sandbox_LDP_Shared].[dbo].[InpatientLengthOfStay] H on a.UniqueID = H.UniqueID and a.SpellID = h.SpellID and a.EpisodeID = h.EpisodeID and h.TimePeriod = EOMONTH(@reportingdate)
	outer apply ( 	select max(CTRDate) as MAX_PostCTRDate from [NHSE_Sandbox_LDP_Shared].[dbo].CleansedCTRDates 
	where a.UniqueID = UniqueID and a.SpellID = SpellID and CTRDate > '2013-03-01' and CTRDate >= a.EpisodeStartDate and CTRDate < DATEADD(day,29 ,a.EpisodeStartDate)		group by UniqueID	)  as K 
	outer apply ( 	select max(CTRDate) as MAX_PreCTRDate from [NHSE_Sandbox_LDP_Shared].[dbo].CleansedCTRDates 
	where a.UniqueID = UniqueID  and  CTRDate > '2013-03-01' and CTRDate < a.EpisodeStartDate	and CTRType = 'Community' group by UniqueID	)  as L 	
	where OriginatingCCGRegionName = 'LONDON'  and A.NewAdmission = 1 	and  iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) ) <> 'U18'
	and a.EpisodeStartDate > DATEADD(month , -3 , EOMONTH(@reportingdate) ) and a.EpisodeStartDate <= EOMONTH(@reportingdate)  
	and  ( iif ( AgeAtPeriodEnd >= 18 , iif( cast( a.EpisodeEndDate - a.EpisodeStartDate as int )  < 28 and MAX_PostCTRDate is null , 0 , iif ( k.MAX_PostCTRDate is null , 0 , iif( cast (MAX_PostCTRDate - a.EpisodeStartDate as int ) <= 28 , 1 , 0 )) ) , 0  ) = 1 
	or iif ( AgeAtPeriodEnd >= 18 , iif( cast( a.EpisodeEndDate - a.EpisodeStartDate as int )  < 28 and MAX_PostCTRDate is null  , 0 , iif ( cast(a.EpisodeStartDate - L.MAX_PreCTRDate as int ) < 28 ,1 , 0 )  ) , 0  ) = 1 ) 
	group by 	a.OriginatingCCGICSName , A.[CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) ), PatientCategory
	order by 	a.OriginatingCCGICSName , A.[CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) ), PatientCategory
	  
----------------------------------------------------------------------------------------------------------------------------------------------------
-- Community/admissions Adults Total Numerator End   -----------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------------------------------

----------------------------------------------------------------------------------------------------------------------------------------------------
-- Community/admissions Adults Excl SRS Denominator Start  -----------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------------------------------
		  
	insert into #TransformingCare_tblMasterMetric_BackTest
	select   OriginatingCCGICSName , @reportingdate , 81 , count(*) as C 
	, [CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) ) , 1 , PatientCategory,'NA'
	from #TransformingCare_tempLiveData A
	left join [NHSE_Sandbox_LDP_Shared].[dbo].[InpatientAgeBanding_vw] D on a.UniqueID = D.UniqueID and a.EpisodeStartDate = D.EpisodeStartDate and d.TimePeriod = EOMONTH(@reportingdate)
	left join [NHSE_Sandbox_LDP_Shared].[dbo].[InpatientLengthOfStay] H on a.UniqueID = H.UniqueID and a.SpellID = h.SpellID and a.EpisodeID = h.EpisodeID and h.TimePeriod = EOMONTH(@reportingdate)
	outer apply ( 	select max(CTRDate) as MAX_PostCTRDate from [NHSE_Sandbox_LDP_Shared].[dbo].CleansedCTRDates 
	where a.UniqueID = UniqueID and a.SpellID = SpellID and CTRDate > '2013-03-01' and CTRDate >= a.EpisodeStartDate and CTRDate < DATEADD(day,29 ,a.EpisodeStartDate)		group by UniqueID	)  as K 
	outer apply ( 	select max(CTRDate) as MAX_PreCTRDate from [NHSE_Sandbox_LDP_Shared].[dbo].CleansedCTRDates 
	where a.UniqueID = UniqueID  and  CTRDate > '2013-03-01' and CTRDate < a.EpisodeStartDate	and CTRType = 'Community' group by UniqueID	)  as L 
	where OriginatingCCGRegionName = 'LONDON'  and A.NewAdmission = 1 and a.providerName <> 'HERTFORDSHIRE PARTNERSHIP UNIVERSITY NHS FT' 	and  iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) ) <> 'U18'
	and a.EpisodeStartDate > DATEADD(month , -3 , EOMONTH(@reportingdate) ) and a.EpisodeStartDate <= EOMONTH(EOMONTH(@reportingdate))  
	and  iif ( AgeAtPeriodEnd >= 18 , iif( cast( a.EpisodeEndDate - a.EpisodeStartDate as int )  < 28 and MAX_PostCTRDate is null ,0 ,1) , 0  )  = 1 
	group by 	a.OriginatingCCGICSName , A.[CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) ), PatientCategory
	order by 	a.OriginatingCCGICSName , A.[CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) ), PatientCategory
	  
----------------------------------------------------------------------------------------------------------------------------------------------------
-- Community/admissions Adults Excl SRS Denominator End  -------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------------------------------

----------------------------------------------------------------------------------------------------------------------------------------------------
-- Community/admissions Adults Excl SRS Pre Admission Numerator Start  -----------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------------------------------
	   
	insert into #TransformingCare_tblMasterMetric_BackTest
	select   OriginatingCCGICSName , @reportingdate , 82 , count(*) as C 
	, [CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) ) , 1 , PatientCategory,'NA'
	from #TransformingCare_tempLiveData A
	left join [NHSE_Sandbox_LDP_Shared].[dbo].[InpatientAgeBanding_vw] D on a.UniqueID = D.UniqueID and a.EpisodeStartDate = D.EpisodeStartDate and d.TimePeriod = EOMONTH(@reportingdate)
	left join [NHSE_Sandbox_LDP_Shared].[dbo].[InpatientLengthOfStay] H on a.UniqueID = H.UniqueID and a.SpellID = h.SpellID and a.EpisodeID = h.EpisodeID and h.TimePeriod = EOMONTH(@reportingdate)
	outer apply ( 	select max(CTRDate) as MAX_PostCTRDate from [NHSE_Sandbox_LDP_Shared].[dbo].CleansedCTRDates 
	where a.UniqueID = UniqueID and a.SpellID = SpellID and CTRDate > '2013-03-01' and CTRDate >= a.EpisodeStartDate and CTRDate < DATEADD(day,29 ,a.EpisodeStartDate)		group by UniqueID	)  as K 
	outer apply ( 	select max(CTRDate) as MAX_PreCTRDate from [NHSE_Sandbox_LDP_Shared].[dbo].CleansedCTRDates 
	where a.UniqueID = UniqueID  and  CTRDate > '2013-03-01' and CTRDate < a.EpisodeStartDate	and CTRType = 'Community' group by UniqueID	)  as L 	
	where OriginatingCCGRegionName = 'LONDON'  and A.NewAdmission = 1 and a.providerName <> 'HERTFORDSHIRE PARTNERSHIP UNIVERSITY NHS FT' 	and  iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) ) <> 'U18'
	and a.EpisodeStartDate > DATEADD(month , -3 , EOMONTH(@reportingdate) ) and a.EpisodeStartDate <= EOMONTH(@reportingdate)  
	and  iif ( AgeAtPeriodEnd >= 18 , iif( cast( a.EpisodeEndDate - a.EpisodeStartDate as int )  < 28 and MAX_PostCTRDate is null  , 0 , iif ( cast(a.EpisodeStartDate - L.MAX_PreCTRDate as int ) < 28 ,1 , 0 )  ) , 0  )  = 1 
	group by 	a.OriginatingCCGICSName , A.[CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) ), PatientCategory
	order by 	a.OriginatingCCGICSName , A.[CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) ), PatientCategory
	   
----------------------------------------------------------------------------------------------------------------------------------------------------
-- Community/admissions Adults Excl SRS Pre Admission Numerator End  -------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------------------------------

----------------------------------------------------------------------------------------------------------------------------------------------------
-- Community/admissions Adults Excl SRS Post Admission Numerator Start   ---------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------------------------------
 
	insert into #TransformingCare_tblMasterMetric_BackTest
	select   OriginatingCCGICSName , @reportingdate , 83 , count(*) as C 
	, [CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) ) , 1 , PatientCategory,'NA'
	from #TransformingCare_tempLiveData A
	left join [NHSE_Sandbox_LDP_Shared].[dbo].[InpatientAgeBanding_vw] D on a.UniqueID = D.UniqueID and a.EpisodeStartDate = D.EpisodeStartDate and d.TimePeriod = EOMONTH(@reportingdate)
	left join [NHSE_Sandbox_LDP_Shared].[dbo].[InpatientLengthOfStay] H on a.UniqueID = H.UniqueID and a.SpellID = h.SpellID and a.EpisodeID = h.EpisodeID and h.TimePeriod = EOMONTH(@reportingdate)
	outer apply ( 	select max(CTRDate) as MAX_PostCTRDate from [NHSE_Sandbox_LDP_Shared].[dbo].CleansedCTRDates 
	where a.UniqueID = UniqueID and a.SpellID = SpellID and CTRDate > '2013-03-01' and CTRDate >= a.EpisodeStartDate and CTRDate < DATEADD(day,29 ,a.EpisodeStartDate)		group by UniqueID	)  as K 
	outer apply ( 	select max(CTRDate) as MAX_PreCTRDate from [NHSE_Sandbox_LDP_Shared].[dbo].CleansedCTRDates 
	where a.UniqueID = UniqueID  and  CTRDate > '2013-03-01' and CTRDate < a.EpisodeStartDate	and CTRType = 'Community' group by UniqueID	)  as L 	
	where OriginatingCCGRegionName = 'LONDON'  and A.NewAdmission = 1 and a.providerName <> 'HERTFORDSHIRE PARTNERSHIP UNIVERSITY NHS FT'
	and a.EpisodeStartDate > DATEADD(month , -3 , EOMONTH(@reportingdate) ) and a.EpisodeStartDate <= EOMONTH(@reportingdate)  	and  iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) ) <> 'U18'
	and iif ( AgeAtPeriodEnd >= 18 , iif( cast( a.EpisodeEndDate - a.EpisodeStartDate as int )  < 28 and MAX_PostCTRDate is null , 0 , iif ( k.MAX_PostCTRDate is null , 0 , iif( cast (MAX_PostCTRDate - a.EpisodeStartDate as int ) <= 28 , 1 , 0 )) ) , 0  )= 1 
	group by 	a.OriginatingCCGICSName , A.[CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) ), PatientCategory
	order by 	a.OriginatingCCGICSName , A.[CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) ), PatientCategory

----------------------------------------------------------------------------------------------------------------------------------------------------
-- Community/admissions Adults Excl SRS Post Admission Numerator End ------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------------------------------

----------------------------------------------------------------------------------------------------------------------------------------------------
-- Community/admissions Adults Excl SRS Total Numerator Start   ------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------------------------------

 	insert into #TransformingCare_tblMasterMetric_BackTest
	select   OriginatingCCGICSName , @reportingdate , 84 , count(*) as C 
	, [CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) ) , 1 , PatientCategory,'NA'
	from #TransformingCare_tempLiveData A
	left join [NHSE_Sandbox_LDP_Shared].[dbo].[InpatientAgeBanding_vw] D on a.UniqueID = D.UniqueID and a.EpisodeStartDate = D.EpisodeStartDate and d.TimePeriod = EOMONTH(@reportingdate)
	left join [NHSE_Sandbox_LDP_Shared].[dbo].[InpatientLengthOfStay] H on a.UniqueID = H.UniqueID and a.SpellID = h.SpellID and a.EpisodeID = h.EpisodeID and h.TimePeriod = EOMONTH(@reportingdate)
	outer apply ( 	select max(CTRDate) as MAX_PostCTRDate from [NHSE_Sandbox_LDP_Shared].[dbo].CleansedCTRDates 
	where a.UniqueID = UniqueID and a.SpellID = SpellID and CTRDate > '2013-03-01' and CTRDate >= a.EpisodeStartDate and CTRDate < DATEADD(day,29 ,a.EpisodeStartDate)		group by UniqueID	)  as K 
	outer apply ( 	select max(CTRDate) as MAX_PreCTRDate from [NHSE_Sandbox_LDP_Shared].[dbo].CleansedCTRDates 
	where a.UniqueID = UniqueID  and  CTRDate > '2013-03-01' and CTRDate < a.EpisodeStartDate	and CTRType = 'Community' group by UniqueID	)  as L 	
	where OriginatingCCGRegionName = 'LONDON'  and A.NewAdmission = 1 and a.providerName <> 'HERTFORDSHIRE PARTNERSHIP UNIVERSITY NHS FT'
	and a.EpisodeStartDate > DATEADD(month , -3 , EOMONTH(@reportingdate) ) and a.EpisodeStartDate <= EOMONTH(@reportingdate)  	and  iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) ) <> 'U18'
	and ( iif ( AgeAtPeriodEnd >= 18 , iif( cast( a.EpisodeEndDate - a.EpisodeStartDate as int )  < 28 and MAX_PostCTRDate is null , 0 , iif ( k.MAX_PostCTRDate is null , 0 , iif( cast (MAX_PostCTRDate - a.EpisodeStartDate as int ) <= 28 , 1 , 0 )) ) , 0  )= 1 
	or iif ( AgeAtPeriodEnd >= 18 , iif( cast( a.EpisodeEndDate - a.EpisodeStartDate as int )  < 28 and MAX_PostCTRDate is null  , 0 , iif ( cast(a.EpisodeStartDate - L.MAX_PreCTRDate as int ) < 28 ,1 , 0 )  ) , 0  )  = 1  ) 
	group by 	a.OriginatingCCGICSName , A.[CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) ), PatientCategory
	order by 	a.OriginatingCCGICSName , A.[CommissioningArrangement] , iif( ageband is null , iif( AgeAtPeriodEnd >= 18 , 'Adults' , 'U18' )  , iif(ageband <> 'U18' , 'Adults' , 'U18'  ) ), PatientCategory

----------------------------------------------------------------------------------------------------------------------------------------------------
-- Community/admissions Adults Excl SRS Total Numerator End ----------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------------------------------
		delete top(1) @datetable
	END;

----------------------------------------------------------------------------------------------------------------------------------------------------
-- CRT End -----------------------------------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------------------------------	

	update #TransformingCare_tblMasterMetric_BackTest set PatientCategory = 'Learning Disability and Autism' where PatientCategory = 'Learning Disability and Autistic Spectrum'

	update #TransformingCare_tblMasterMetric_BackTest set PatientCategory = 'Autism Only' where PatientCategory = 'Autistic Spectrum Condition Only'

-----------------------------------------------------------------------------------------------------------------------------------------------------
-- Update ICS Names Start ---------------------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------------------------------

	Update #TransformingCare_tblMasterMetric_BackTest 		set ICS = 'London South East'		where ICS = 'South East London'
	
	Update #TransformingCare_tblMasterMetric_BackTest 		set ICS = 'London North West'		where ICS = 'North West London'
	
	Update #TransformingCare_tblMasterMetric_BackTest		set ICS = 'London South West'		where ICS = 'South West London'
	
	Update #TransformingCare_tblMasterMetric_BackTest		set ICS = 'North Central London'    where ICS = 'North, Central London'

-----------------------------------------------------------------------------------------------------------------------------------------------------
-- Update ICS Names End -----------------------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------------------------------

-----------------------------------------------------------------------------------------------------------------------------------------------------
-- Update commissioning Arrangement Names Start -----------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------------------------------

	update  #TransformingCare_tblMasterMetric_BackTest 
	set commissioningArrangement = 'RS'
	where commissioningArrangement = 'SC' 

	update  #TransformingCare_tblMasterMetric_BackTest 
	set commissioningArrangement = 'SC'
	where commissioningArrangement = 'SC&PC' 
-----------------------------------------------------------------------------------------------------------------------------------------------------
-- Update commissioning Arrangement Names End -------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------------------------------

-------------------------------------------------------------------------------------------------------------------------------------------------------
---- Create SC&PC CommissioningArrangements Start -------------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------------------------------------------------------------

	insert into #TransformingCare_tblMasterMetric_BackTest
	select ICS , [ReportingMonth] , [MetricID]  , sum([value]), CommissioningArrangement , PatientGroup , [GroupingLevel]  , 'All' ,[LocationName]
	from #TransformingCare_tblMasterMetric_BackTest
	group by ICS , [ReportingMonth] , [MetricID]   , PatientGroup , CommissioningArrangement , [GroupingLevel] , [LocationName]
	order by ICS , [ReportingMonth] , [MetricID]   , PatientGroup , CommissioningArrangement , [GroupingLevel] , [LocationName]

-------------------------------------------------------------------------------------------------------------------------------------------------------
---- Create SC&PC CommissioningArrangements End ---------------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------------------------------------------------------------

---------------------------------------------------------------------------------------------------------------------------------------------------------
------ Create SC&PC CommissioningArrangements Start -------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------------------------------------------------------

	insert into #TransformingCare_tblMasterMetric_BackTest
	select ICS , [ReportingMonth] , [MetricID]  , sum([value]), 'SC' , PatientGroup  , [GroupingLevel] , PatientCategory , [LocationName]
	from	#TransformingCare_tblMasterMetric_BackTest
	where CommissioningArrangement in ('RS','PC') 
	group by ICS , [ReportingMonth] , [MetricID]   , PatientGroup , PatientCategory , [GroupingLevel] , [LocationName]
	order by ICS , [ReportingMonth] , [MetricID]   , PatientGroup , PatientCategory , [GroupingLevel] , [LocationName]

-----------------------------------------------------------------------------------------------------------------------------------------------------
-- Create SC&PC CommissioningArrangements End ---------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------------------------------

-----------------------------------------------------------------------------------------------------------------------------------------------------
-- Create all CommissioningArrangement Start ----------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------------------------------

	insert into #TransformingCare_tblMasterMetric_BackTest
	select ICS , [ReportingMonth] , [MetricID]  , sum([value]), CommissioningArrangement , 'All' , [GroupingLevel] , PatientCategory , [LocationName]
	from #TransformingCare_tblMasterMetric_BackTest
	where PatientGroup in ('Adults','U18') and CommissioningArrangement in ('RS','PC' , 'CCG' , 'SC'  )
	group by ICS , [ReportingMonth] , [MetricID]   , CommissioningArrangement , [GroupingLevel] , PatientCategory ,[LocationName]
	order by ICS , [ReportingMonth] , [MetricID]   , CommissioningArrangement , [GroupingLevel] , PatientCategory ,[LocationName]
	
-----------------------------------------------------------------------------------------------------------------------------------------------------
-- Create All CommissioningArrangement End ------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------------------------------

-----------------------------------------------------------------------------------------------------------------------------------------------------
-- Create Total PatientGroup Total Start ------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------------------------------

	insert into #TransformingCare_tblMasterMetric_BackTest
	select ICS , [ReportingMonth] , [MetricID]  , sum([value]), 'Total' , PatientGroup  , [GroupingLevel] , PatientCategory  ,[LocationName]
	from #TransformingCare_tblMasterMetric_BackTest
	where CommissioningArrangement in ('RS','PC' , 'CCG' ) and PatientGroup in ( 'Adults' , 'All' ) 
	group by ICS , [ReportingMonth] , [MetricID]   , PatientGroup , [GroupingLevel] , PatientCategory ,[LocationName]
	order by ICS , [ReportingMonth] , [MetricID]   , PatientGroup , [GroupingLevel] , PatientCategory ,[LocationName]
	
-----------------------------------------------------------------------------------------------------------------------------------------------------
-- Create Total PatientGroup Total End --------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------------------------------

-----------------------------------------------------------------------------------------------------------------------------------------------------
-- Create London ICS Start --------------------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------------------------------

	insert into #TransformingCare_tblMasterMetric_BackTest 
	select 'London' as [ICS],  [ReportingMonth] , [MetricID] , sum([Value]) as [Value]  , [CommissioningArrangement] , PatientGroup , [GroupingLevel] , PatientCategory ,[LocationName]
	from #TransformingCare_tblMasterMetric_BackTest 
	group by [ReportingMonth] , [MetricID] , [CommissioningArrangement] , PatientGroup , [GroupingLevel] , PatientCategory ,[LocationName]
	
-----------------------------------------------------------------------------------------------------------------------------------------------------
-- Create London ICS End ----------------------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------------------------------

-----------------------------------------------------------------------------------------------------------------------------------------------------
-- Insert Into Late Reporting Table Start -----------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------------------------------

	insert into [NHSE_Sandbox_Lon_LDAProgramme].[dbo].[TransformingCare_Archive_tblMetric] 
	select a.* , @reportingDateHold 
	from #TransformingCare_tblMasterMetric_BackTest A
	left join [NHSE_Sandbox_Lon_LDAProgramme].[dbo].[TransformingCare_Archive_tblMetric] B
	on a.ICS = b.ICS and a.[ReportingMonth] = b.[ReportingMonth]  and a.[MetricID] = b.[MetricID] and a.[CommissioningArrangement] = b.[CommissioningArrangement] and a.PatientGroup = b.PatientGroup and b.datamonth = @reportingDateHold and  a.[GroupingLevel] = b.[GroupingLevel] and a.PatientCategory = b.PatientCategory and a.[LocationName] = b.[LocationName]
	where b.[ics] is null 

-----------------------------------------------------------------------------------------------------------------------------------------------------
-- Insert Into Late Reporting Table End -------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------------------------------

-------------------------------------------------------------------------------------------------------------------------------------------------------
---- Update Current Live table Start ------------------------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------------------------------------------------------------
	
	update B set b.[Value] = iif ( a.[Value] is null , 0 , a.[Value] ) 
	from #TransformingCare_tblMasterMetric_BackTest  A 
	inner join [NHSE_Sandbox_Lon_LDAProgramme].[dbo].[TransformingCare_tblMetric] B 
	on a.ICS = b.ICS and a.[ReportingMonth] = b.[ReportingMonth] and a.[MetricID] = b.[MetricID] and a.[CommissioningArrangement] = b.[CommissioningArrangement] 
	and a.PatientGroup = b.PatientGroup and  a.[GroupingLevel] = b.[GroupingLevel] 	and a.PatientCategory = b.PatientCategory  and a.[LocationName] = b.[LocationName]
	where b.metricid not in (10)

	
--	select *,  b.[Value] , iif ( a.[Value] is null , 0 , a.[Value] )  , * 
--	from #TransformingCare_tblMasterMetric_BackTest  A 
--	right join [NHSE_Sandbox_Lon_LDAProgramme].[dbo].[TransformingCare_tblMetric] B 
--	on a.ICS = b.ICS and a.[ReportingMonth] = b.[ReportingMonth] and a.[MetricID] = b.[MetricID] and a.[CommissioningArrangement] = b.[CommissioningArrangement] 
--	and a.PatientGroup = b.PatientGroup and  a.[GroupingLevel] = b.[GroupingLevel] 	and a.PatientCategory = b.PatientCategory  and a.[LocationName] = b.[LocationName]
--	where b.metricid not in (10) and b.metricID = 1 and b.GroupingLevel = 1 and b.ReportingMonth = '2022-05-01 00:00:00.000' and b.PatientCategory = 'All'   and b.PatientGroup = 'U18' 

-------------------------------------------------------------------------------------------------------------------------------------------------------
---- Update Current Live table End --------------------------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------------------------------------------------------------

-------------------------------------------------------------------------------------------------------------------------------------------------------
---- Insert New Values into live table Start ----------------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------------------------------------------------------------

    insert into [NHSE_Sandbox_Lon_LDAProgramme].[dbo].[TransformingCare_tblMetric]
    select a.[ICS], a.[ReportingMonth], a.[MetricID] ,iif ( a.[Value] is null , 0 , a.[Value] ) , a.[CommissioningArrangement], a.[PatientGroup], a.[GroupingLevel], a.[PatientCategory],a.[LocationName] 
    from #TransformingCare_tblMasterMetric_BackTest A
    left join [NHSE_Sandbox_Lon_LDAProgramme].[dbo].[TransformingCare_tblMetric] B 
    on a.ICS = b.ICS and a.[ReportingMonth] = b.[ReportingMonth] and a.[MetricID] = b.[MetricID] and a.[CommissioningArrangement] = b.[CommissioningArrangement] and a.PatientGroup = b.PatientGroup and  a.[GroupingLevel] = b.[GroupingLevel] and a.PatientCategory = b.PatientCategory  and a.[LocationName] = b.[LocationName]
    where b.[Value] is null   
	
-------------------------------------------------------------------------------------------------------------------------------------------------------
---- Insert New Values into live table End ------------------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------------------------------------------------------------

	-- delete from [NHSE_Sandbox_Lon_LDAProgramme].[dbo].[TransformingCare_Archive_MasterData] 	where datamonth = @reportingDateHold
	-- 
	-- insert into [NHSE_Sandbox_Lon_LDAProgramme].[dbo].[TransformingCare_Archive_MasterData] 
	-- select * , @reportingDateHold from #TransformingCare_tempLiveData 
	
	drop table #TransformingCare_tempLiveData
	drop table #TransformingCare_tblMasterMetric_BackTest

	CREATE TABLE #TransformingCare_tblMasterIndicator_BackTest (
	[ICS] [varchar](100) NOT NULL,
	[ReportingMonth] [datetime] NOT NULL,
	[IndicatorID] int NOT NULL,
	[Value] decimal(18,5) not NULL,
	[CommissioningArrangement] [varchar](20) NOT NULL,
	[PatientGroup] [varchar](25) NOT NULL,
	[GroupingLevel] int NOT NULL, 
	PatientCategory [varchar](100) NOT NULL	) 	

	insert into #TransformingCare_tblMasterIndicator_BackTest 
	select a.ICS , a.ReportingMonth , 1 , a.[Value] - b.[Value] , a.CommissioningArrangement , a.PatientGroup , a.GroupingLevel , a.PatientCategory
	from [TransformingCare_tblMetric] A 
	inner join [TransformingCare_tblTarget] B on b.[ICS] = a.[ICS] and a.ReportingMonth = b.ReportingMonth and a.CommissioningArrangement = b.CommissioningArrangement and a.PatientGroup = b.PatientGroup and a.PatientCategory = b.PatientCategory
	where a.GroupingLevel = 1 and A.MetricID = 1 and b.TargetID = 1

	insert into #TransformingCare_tblMasterIndicator_BackTest 
	select a.ICS , a.ReportingMonth , 2 , a.[Value] - b.[Value] , a.CommissioningArrangement , a.PatientGroup , a.GroupingLevel , a.PatientCategory
	from [TransformingCare_tblMetric] A 
	inner join [TransformingCare_tblTarget] B on b.[ICS] = a.[ICS] and a.ReportingMonth = b.ReportingMonth and a.CommissioningArrangement = b.CommissioningArrangement and a.PatientGroup = b.PatientGroup and a.PatientCategory = b.PatientCategory
	where a.GroupingLevel = 1 and A.MetricID = 1 and b.TargetID = 2

	insert into #TransformingCare_tblMasterIndicator_BackTest 
	select a.ICS , a.ReportingMonth , 3 ,  (cast(b.[Value] as decimal(18,3)) /  cast(A.[Value] as decimal(18,3))  ) , a.CommissioningArrangement , a.PatientGroup , a.GroupingLevel , a.PatientCategory
	from [TransformingCare_tblMetric] A 
	inner join [TransformingCare_tblMetric] B on b.[ICS] = a.[ICS] and a.ReportingMonth = b.ReportingMonth and a.CommissioningArrangement = b.CommissioningArrangement and a.PatientGroup = b.PatientGroup and a.PatientCategory = b.PatientCategory
	where a.GroupingLevel = 1 and b.GroupingLevel = 1  and A.MetricID = 1 and b.MetricID = 6 and A.[Value] <> 0 

	insert into #TransformingCare_tblMasterIndicator_BackTest  
	select a.ICS , a.ReportingMonth , 4 ,  (cast(b.[Value] as decimal(18,3)) /  cast(A.[Value] as decimal(18,3))  ) , a.CommissioningArrangement , a.PatientGroup , a.GroupingLevel , a.PatientCategory
	from [TransformingCare_tblMetric] A 
	inner join [TransformingCare_tblMetric] B on b.[ICS] = a.[ICS] and a.ReportingMonth = b.ReportingMonth and a.CommissioningArrangement = b.CommissioningArrangement and a.PatientGroup = b.PatientGroup and a.PatientCategory = b.PatientCategory
	where a.GroupingLevel = 1 and b.GroupingLevel = 1  and A.MetricID = 1 and b.MetricID = 8 and A.[Value] <> 0 

	insert into #TransformingCare_tblMasterIndicator_BackTest   
	select a.ICS , a.ReportingMonth , 5 ,  (cast(b.[Value] as decimal(18,3)) /  cast(A.[Value] as decimal(18,3))  ) , a.CommissioningArrangement , a.PatientGroup , a.GroupingLevel , a.PatientCategory
	from [TransformingCare_tblMetric] A 
	inner join [TransformingCare_tblMetric] B on b.[ICS] = a.[ICS] and a.ReportingMonth = b.ReportingMonth and a.CommissioningArrangement = b.CommissioningArrangement and a.PatientGroup = b.PatientGroup and a.PatientCategory = b.PatientCategory
	where a.GroupingLevel = 1 and b.GroupingLevel = 1  and A.MetricID = 1 and b.MetricID = 9 and A.[Value] <> 0 

	insert into #TransformingCare_tblMasterIndicator_BackTest  
	select a.ICS , a.ReportingMonth , 6 , 1 - (cast(a.[Value] as decimal(18,3)) /  cast(b.[Value] as decimal(18,3))  ) , a.CommissioningArrangement , a.PatientGroup , a.GroupingLevel , a.PatientCategory
	from [TransformingCare_tblMetric] A 
	inner join [TransformingCare_tblMetric] B on b.[ICS] = a.[ICS] and a.CommissioningArrangement = b.CommissioningArrangement and a.PatientGroup = b.PatientGroup and a.PatientCategory = b.PatientCategory
	where a.GroupingLevel = 1 and b.GroupingLevel = 1  and A.MetricID = 1 and b.MetricID = 1 and b.ReportingMonth = '2016-03-01' and B.[Value] <> 0 
		
	insert into #TransformingCare_tblMasterIndicator_BackTest  
	select a.ICS , a.ReportingMonth , 7 , 1 - (cast(a.[Value] as decimal(18,3)) /  cast(b.[Value] as decimal(18,3))  ) , a.CommissioningArrangement , a.PatientGroup , a.GroupingLevel , a.PatientCategory
	from [TransformingCare_tblMetric] A 
	inner join [TransformingCare_tblMetric] B on b.[ICS] = a.[ICS] and a.CommissioningArrangement = b.CommissioningArrangement and a.PatientGroup = b.PatientGroup and a.PatientCategory = b.PatientCategory
	where a.GroupingLevel = 1 and b.GroupingLevel = 1  and A.MetricID = 1 and b.MetricID = 1 and b.ReportingMonth = '2018-03-01' and B.[Value] <> 0 
	
	insert into #TransformingCare_tblMasterIndicator_BackTest 
	select a.ICS , a.ReportingMonth , 8 , a.[Value] - b.[Value] , a.CommissioningArrangement , a.PatientGroup , a.GroupingLevel , a.PatientCategory
	from [TransformingCare_tblMetric] A 
	inner join [TransformingCare_tblTarget] B on b.[ICS] = a.[ICS]  and a.CommissioningArrangement = b.CommissioningArrangement and a.PatientGroup = b.PatientGroup and a.PatientCategory = b.PatientCategory
	where a.GroupingLevel = 1 and A.MetricID = 1 and b.TargetID = 1 and b.ReportingMonth = '2022-03-01'

	insert into #TransformingCare_tblMasterIndicator_BackTest 
	select a.ICS , a.ReportingMonth , 9  , ((cast(b.[Value] as decimal(18,3)) * 1000000) /  cast(A.[Value] as decimal(18,3))  ) 
	, b.CommissioningArrangement , b.PatientGroup , b.GroupingLevel , b.PatientCategory
	from [TransformingCare_tblMetric] A 
	inner join [TransformingCare_tblMetric] B on b.[ICS] = a.[ICS] and a.ReportingMonth = b.ReportingMonth and a.PatientGroup = b.PatientGroup 
	where a.GroupingLevel = 1 and b.GroupingLevel = 1  and A.MetricID = 10 and b.MetricID = 1 and A.[Value] <> 0 
	order by a.ReportingMonth desc

	insert into #TransformingCare_tblMasterIndicator_BackTest   --Problem
	select a.ICS , a.ReportingMonth , 10 , (cast(a.[Value] as decimal(18,3)) /  cast(b.[Value] as decimal(18,3))  ) 
	, a.CommissioningArrangement , a.PatientGroup , a.GroupingLevel , a.PatientCategory
	from [TransformingCare_tblMetric] A 
	inner join [TransformingCare_tblMetric] B on b.[ICS] = a.[ICS] and a.ReportingMonth = b.ReportingMonth and a.CommissioningArrangement = b.CommissioningArrangement and a.PatientGroup = b.PatientGroup and a.PatientCategory = b.PatientCategory
	where a.GroupingLevel = 1 and b.GroupingLevel = 1  and A.MetricID = 21 and b.MetricID = 12 and B.[Value] <> 0 

	insert into #TransformingCare_tblMasterIndicator_BackTest 
	select a.ICS , a.ReportingMonth , 11 , ( cast(b.[Value] as decimal(18,3)) - cast(a.[Value] as decimal(18,3)) ) 
	, a.CommissioningArrangement , a.PatientGroup , a.GroupingLevel , a.PatientCategory
	from [TransformingCare_tblMetric] A 
	inner join [TransformingCare_tblMetric] B on b.[ICS] = a.[ICS] and a.ReportingMonth = b.ReportingMonth and a.CommissioningArrangement = b.CommissioningArrangement and a.PatientGroup = b.PatientGroup and a.PatientCategory = b.PatientCategory
	where a.GroupingLevel = 1 and b.GroupingLevel = 1  and A.MetricID = 21 and b.MetricID = 12

	insert into #TransformingCare_tblMasterIndicator_BackTest 
	select a.ICS , a.ReportingMonth , 12 , a.[Value] - b.[Value] , a.CommissioningArrangement , a.PatientGroup , a.GroupingLevel , a.PatientCategory
	from [TransformingCare_tblMetric] A 
	inner join [TransformingCare_tblTarget] B on b.[ICS] = a.[ICS] and a.ReportingMonth = b.ReportingMonth
	and a.CommissioningArrangement = b.CommissioningArrangement and a.PatientGroup = b.PatientGroup and a.PatientCategory = b.PatientCategory
	where a.GroupingLevel = 1 and A.MetricID = 1 and b.TargetID = 2 
		
	insert into #TransformingCare_tblMasterIndicator_BackTest 
	select a.ICS , a.ReportingMonth , 13 , (cast(a.[Value] as decimal(18,3)) /  cast(b.[Value] as decimal(18,3))  )  /365
	, a.CommissioningArrangement , a.PatientGroup , a.GroupingLevel , a.PatientCategory
	from [TransformingCare_tblMetric] A 
	inner join [TransformingCare_tblMetric] B on b.[ICS] = a.[ICS] and a.ReportingMonth = b.ReportingMonth and a.CommissioningArrangement = b.CommissioningArrangement and a.PatientGroup = b.PatientGroup and a.PatientCategory = b.PatientCategory
	where a.GroupingLevel = 1 and b.GroupingLevel = 1  and A.MetricID = 23 and b.MetricID = 24 and B.[Value] <> 0 

	insert into #TransformingCare_tblMasterIndicator_BackTest 
	select a.ICS , a.ReportingMonth , 14 ,  (cast(a.[Value] as decimal(18,3)) /  cast(b.[Value] as decimal(18,3))  ) 
	, a.CommissioningArrangement , a.PatientGroup , a.GroupingLevel , a.PatientCategory
	from [TransformingCare_tblMetric] A 
	inner join [TransformingCare_tblMetric] B on b.[ICS] = a.[ICS] and a.ReportingMonth = b.ReportingMonth and a.CommissioningArrangement = b.CommissioningArrangement and a.PatientGroup = b.PatientGroup and a.PatientCategory = b.PatientCategory
	and a.GroupingLevel = b.GroupingLevel 
	where a.GroupingLevel = 1 and b.GroupingLevel = 1 and A.MetricID = 34 and b.MetricID = 1 and B.[Value] <> 0 

	insert into #TransformingCare_tblMasterIndicator_BackTest 
	select a.ICS , a.ReportingMonth , 15 ,  (cast(a.[Value] as decimal(18,3)) /  cast(b.[Value] as decimal(18,3))  ) 
	, a.CommissioningArrangement , a.PatientGroup , a.GroupingLevel , a.PatientCategory
	from [TransformingCare_tblMetric] A 
	inner join [TransformingCare_tblMetric] B on b.[ICS] = a.[ICS] and a.ReportingMonth = b.ReportingMonth and a.CommissioningArrangement = b.CommissioningArrangement and a.PatientGroup = b.PatientGroup and a.PatientCategory = b.PatientCategory
	and a.GroupingLevel = b.GroupingLevel 
	where a.GroupingLevel = 1 and b.GroupingLevel = 1 and A.MetricID = 45 and b.MetricID = 1 and B.[Value] <> 0 

	insert into #TransformingCare_tblMasterIndicator_BackTest 
	select a.ICS , a.ReportingMonth , 16 ,  (cast(a.[Value] as decimal(18,3)) /  cast(b.[Value] as decimal(18,3))  ) 
	, a.CommissioningArrangement , a.PatientGroup , a.GroupingLevel , a.PatientCategory
	from [TransformingCare_tblMetric] A 
	inner join [TransformingCare_tblMetric] B on b.[ICS] = a.[ICS] and a.ReportingMonth = b.ReportingMonth and a.CommissioningArrangement = b.CommissioningArrangement and a.PatientGroup = b.PatientGroup and a.PatientCategory = b.PatientCategory
	and a.GroupingLevel = b.GroupingLevel 
	where a.GroupingLevel = 1 and b.GroupingLevel = 1 and A.MetricID = 46 and b.MetricID = 1 and B.[Value] <> 0 
	
	insert into #TransformingCare_tblMasterIndicator_BackTest 
	select a.ICS , a.ReportingMonth , 17 ,  (cast(a.[Value] as decimal(18,3)) /  cast(b.[Value] as decimal(18,3))  ) 
	, a.CommissioningArrangement , a.PatientGroup , a.GroupingLevel , a.PatientCategory
	from [TransformingCare_tblMetric] A 
	inner join [TransformingCare_tblMetric] B on b.[ICS] = a.[ICS] and a.ReportingMonth = b.ReportingMonth and a.CommissioningArrangement = b.CommissioningArrangement and a.PatientGroup = b.PatientGroup and a.PatientCategory = b.PatientCategory
	and a.GroupingLevel = b.GroupingLevel 
	where a.GroupingLevel = 1 and b.GroupingLevel = 1 and A.MetricID = 47 and b.MetricID = 1 and B.[Value] <> 0 

	insert into #TransformingCare_tblMasterIndicator_BackTest 
	select a.ICS , a.ReportingMonth , 18 ,  (cast(a.[Value] as decimal(18,3)) /  cast(b.[Value] as decimal(18,3))  ) /365   
	, a.CommissioningArrangement , a.PatientGroup , a.GroupingLevel , a.PatientCategory
	from [TransformingCare_tblMetric] A 
	inner join [TransformingCare_tblMetric] B on b.[ICS] = a.[ICS] and a.ReportingMonth = b.ReportingMonth and a.CommissioningArrangement = b.CommissioningArrangement and a.PatientGroup = b.PatientGroup and a.PatientCategory = b.PatientCategory
	and a.GroupingLevel = b.GroupingLevel 
	where a.GroupingLevel = 1 and b.GroupingLevel = 1 and A.MetricID = 48 and b.MetricID = 1 and B.[Value] <> 0 

	insert into #TransformingCare_tblMasterIndicator_BackTest 
	select a.ICS , a.ReportingMonth , 19 ,  (cast(a.[Value] as decimal(18,3)) /  cast(b.[Value] as decimal(18,3))  )  /365 
	, a.CommissioningArrangement , a.PatientGroup , a.GroupingLevel , a.PatientCategory
	from [TransformingCare_tblMetric] A 
	inner join [TransformingCare_tblMetric] B on b.[ICS] = a.[ICS] and a.ReportingMonth = b.ReportingMonth and a.CommissioningArrangement = b.CommissioningArrangement and a.PatientGroup = b.PatientGroup and a.PatientCategory = b.PatientCategory
	and a.GroupingLevel = b.GroupingLevel 
	where a.GroupingLevel = 1 and b.GroupingLevel = 1 and A.MetricID = 49 and b.MetricID = 2 and B.[Value] <> 0 

	insert into #TransformingCare_tblMasterIndicator_BackTest 
	select a.ICS , a.ReportingMonth , 20 ,  ( cast(a.[Value] as decimal(18,3))   /  cast(b.[Value] as decimal(18,3)) - iif( c.[Value] is null , 0 ,  cast(c.[Value] as decimal(18,3)))  )  /365 
	, a.CommissioningArrangement , a.PatientGroup , a.GroupingLevel , a.PatientCategory
	from [TransformingCare_tblMetric] A 
	inner join [TransformingCare_tblMetric] B on b.[ICS] = a.[ICS] and a.ReportingMonth = b.ReportingMonth and a.CommissioningArrangement = b.CommissioningArrangement and a.PatientGroup = b.PatientGroup and a.PatientCategory = b.PatientCategory	and a.GroupingLevel = b.GroupingLevel 
	left join [TransformingCare_tblMetric] C on C.[ICS] = a.[ICS] and a.ReportingMonth = C.ReportingMonth and a.CommissioningArrangement = C.CommissioningArrangement and a.PatientGroup = C.PatientGroup and a.PatientCategory = C.PatientCategory	and a.GroupingLevel = C.GroupingLevel and c.MetricID = 2 and c.GroupingLevel = 1
	where a.GroupingLevel = 1 and b.GroupingLevel = 1   and A.MetricID = 50 and b.MetricID = 1  and B.[Value] <> 0 

	insert into #TransformingCare_tblMasterIndicator_BackTest 
	select a.ICS , a.ReportingMonth , 21 ,  (cast(a.[Value] as decimal(18,3)) /  cast(b.[Value] as decimal(18,3))  ) 
	, a.CommissioningArrangement , a.PatientGroup , a.GroupingLevel , a.PatientCategory
	from [TransformingCare_tblMetric] A 
	inner join [TransformingCare_tblMetric] B on b.[ICS] = a.[ICS] and a.ReportingMonth = b.ReportingMonth and a.CommissioningArrangement = b.CommissioningArrangement and a.PatientGroup = b.PatientGroup and a.PatientCategory = b.PatientCategory
	and a.GroupingLevel = b.GroupingLevel 
	where a.GroupingLevel = 1 and b.GroupingLevel = 1 and A.MetricID = 62 and b.MetricID = 61 and B.[Value] <> 0 

	insert into #TransformingCare_tblMasterIndicator_BackTest 
	select a.ICS , a.ReportingMonth , 22 ,  ( ( cast(a.[Value] as decimal(18,3)) + cast(C.[Value] as decimal(18,3)) + cast(E.[Value] as decimal(18,3)))  /  ( cast(b.[Value] as decimal(18,3)) + cast(D.[Value] as decimal(18,3)) + cast(F.[Value] as decimal(18,3) ) )  )   
	, a.CommissioningArrangement , a.PatientGroup , a.GroupingLevel , a.PatientCategory
	from [TransformingCare_tblMetric] A 
	inner join [TransformingCare_tblMetric] B on b.[ICS] = a.[ICS] and a.ReportingMonth = b.ReportingMonth and a.CommissioningArrangement = b.CommissioningArrangement and a.PatientGroup = b.PatientGroup and a.PatientCategory = b.PatientCategory and a.GroupingLevel = b.GroupingLevel 
	inner join [TransformingCare_tblMetric] C on C.[ICS] = a.[ICS] and  dateadd( month , -1 ,a.ReportingMonth) = C.ReportingMonth and a.CommissioningArrangement = C.CommissioningArrangement and a.PatientGroup = C.PatientGroup and a.PatientCategory = C.PatientCategory and a.GroupingLevel = C.GroupingLevel 
	inner join [TransformingCare_tblMetric] D on D.[ICS] = a.[ICS] and  dateadd( month , -1 ,a.ReportingMonth) = D.ReportingMonth and a.CommissioningArrangement = D.CommissioningArrangement and a.PatientGroup = D.PatientGroup and a.PatientCategory = D.PatientCategory and a.GroupingLevel = D.GroupingLevel 
	inner join [TransformingCare_tblMetric] E on E.[ICS] = a.[ICS] and  dateadd( month , -2 ,a.ReportingMonth) = E.ReportingMonth and a.CommissioningArrangement = E.CommissioningArrangement and a.PatientGroup = E.PatientGroup and a.PatientCategory = E.PatientCategory and a.GroupingLevel = E.GroupingLevel 
	inner join [TransformingCare_tblMetric] F on F.[ICS] = a.[ICS] and  dateadd( month , -2 ,a.ReportingMonth) = F.ReportingMonth and a.CommissioningArrangement = F.CommissioningArrangement and a.PatientGroup = F.PatientGroup and a.PatientCategory = F.PatientCategory and a.GroupingLevel = F.GroupingLevel 
	where a.GroupingLevel = 1 and b.GroupingLevel = 1 and A.MetricID = 62 and b.MetricID = 61 and C.MetricID = 62 and D.MetricID = 61 and E.MetricID = 62 and F.MetricID = 61 and cast(b.[Value] as decimal(18,3)) <> 0  and  cast(D.[Value] as decimal(18,3))  <> 0  and  cast(F.[Value] as decimal(18,3) ) <> 0 

	insert into #TransformingCare_tblMasterIndicator_BackTest 
	select a.ICS , a.ReportingMonth , 23 ,  (cast(a.[Value] as decimal(18,3)) /  cast(b.[Value] as decimal(18,3))  ) 
	, a.CommissioningArrangement , a.PatientGroup , a.GroupingLevel , a.PatientCategory
	from [TransformingCare_tblMetric] A 
	inner join [TransformingCare_tblMetric] B on b.[ICS] = a.[ICS] and a.ReportingMonth = b.ReportingMonth and a.CommissioningArrangement = b.CommissioningArrangement and a.PatientGroup = b.PatientGroup and a.PatientCategory = b.PatientCategory
	and a.GroupingLevel = b.GroupingLevel 
	where a.GroupingLevel = 1 and b.GroupingLevel = 1 and A.MetricID = 64 and b.MetricID = 63 and cast(b.[Value] as decimal(18,3)) <> 0

	insert into #TransformingCare_tblMasterIndicator_BackTest 
	select a.ICS , a.ReportingMonth , 24 ,  ( ( cast(a.[Value] as decimal(18,3)) + cast(C.[Value] as decimal(18,3)) + cast(E.[Value] as decimal(18,3)))  /  ( cast(b.[Value] as decimal(18,3)) + cast(D.[Value] as decimal(18,3)) + cast(F.[Value] as decimal(18,3) ) )  )  
	, a.CommissioningArrangement , a.PatientGroup , a.GroupingLevel , a.PatientCategory
	from [TransformingCare_tblMetric] A 
	inner join [TransformingCare_tblMetric] B on b.[ICS] = a.[ICS] and a.ReportingMonth = b.ReportingMonth and a.CommissioningArrangement = b.CommissioningArrangement and a.PatientGroup = b.PatientGroup and a.PatientCategory = b.PatientCategory and a.GroupingLevel = b.GroupingLevel 
	inner join [TransformingCare_tblMetric] C on C.[ICS] = a.[ICS] and  dateadd( month , -1 ,a.ReportingMonth) = C.ReportingMonth and a.CommissioningArrangement = C.CommissioningArrangement and a.PatientGroup = C.PatientGroup and a.PatientCategory = C.PatientCategory and a.GroupingLevel = C.GroupingLevel 
	inner join [TransformingCare_tblMetric] D on D.[ICS] = a.[ICS] and  dateadd( month , -1 ,a.ReportingMonth) = D.ReportingMonth and a.CommissioningArrangement = D.CommissioningArrangement and a.PatientGroup = D.PatientGroup and a.PatientCategory = D.PatientCategory and a.GroupingLevel = D.GroupingLevel 
	inner join [TransformingCare_tblMetric] E on E.[ICS] = a.[ICS] and  dateadd( month , -2 ,a.ReportingMonth) = E.ReportingMonth and a.CommissioningArrangement = E.CommissioningArrangement and a.PatientGroup = E.PatientGroup and a.PatientCategory = E.PatientCategory and a.GroupingLevel = E.GroupingLevel 
	inner join [TransformingCare_tblMetric] F on F.[ICS] = a.[ICS] and  dateadd( month , -2 ,a.ReportingMonth) = F.ReportingMonth and a.CommissioningArrangement = F.CommissioningArrangement and a.PatientGroup = F.PatientGroup and a.PatientCategory = F.PatientCategory and a.GroupingLevel = F.GroupingLevel 
	where a.GroupingLevel = 1 and b.GroupingLevel = 1 and A.MetricID = 64 and b.MetricID = 63 and C.MetricID = 64 and D.MetricID = 63 and E.MetricID = 64 and F.MetricID = 63 and cast(b.[Value] as decimal(18,3)) <> 0  and  cast(D.[Value] as decimal(18,3))  <> 0  and  cast(F.[Value] as decimal(18,3) ) <> 0 

	insert into #TransformingCare_tblMasterIndicator_BackTest 
	select a.ICS , a.ReportingMonth , 25 ,  (cast(a.[Value] as decimal(18,3)) /  cast(b.[Value] as decimal(18,3))  ) 
	, a.CommissioningArrangement , a.PatientGroup , a.GroupingLevel , a.PatientCategory
	from [TransformingCare_tblMetric] A 
	inner join [TransformingCare_tblMetric] B on b.[ICS] = a.[ICS] and a.ReportingMonth = b.ReportingMonth and a.CommissioningArrangement = b.CommissioningArrangement and a.PatientGroup = b.PatientGroup and a.PatientCategory = b.PatientCategory
	and a.GroupingLevel = b.GroupingLevel 
	where a.GroupingLevel = 1 and b.GroupingLevel = 1 and A.MetricID = 66 and b.MetricID = 65 and cast(b.[Value] as decimal(18,3)) <> 0

	insert into #TransformingCare_tblMasterIndicator_BackTest 
	select a.ICS , a.ReportingMonth , 26 ,  ( ( cast(a.[Value] as decimal(18,3)) + cast(C.[Value] as decimal(18,3)) + cast(E.[Value] as decimal(18,3)))  /  ( cast(b.[Value] as decimal(18,3)) + cast(D.[Value] as decimal(18,3)) + cast(F.[Value] as decimal(18,3) ) )  )  
	, a.CommissioningArrangement , a.PatientGroup , a.GroupingLevel , a.PatientCategory
	from [TransformingCare_tblMetric] A 
	inner join [TransformingCare_tblMetric] B on b.[ICS] = a.[ICS] and a.ReportingMonth = b.ReportingMonth and a.CommissioningArrangement = b.CommissioningArrangement and a.PatientGroup = b.PatientGroup and a.PatientCategory = b.PatientCategory and a.GroupingLevel = b.GroupingLevel 
	inner join [TransformingCare_tblMetric] C on C.[ICS] = a.[ICS] and  dateadd( month , -1 ,a.ReportingMonth) = C.ReportingMonth and a.CommissioningArrangement = C.CommissioningArrangement and a.PatientGroup = C.PatientGroup and a.PatientCategory = C.PatientCategory and a.GroupingLevel = C.GroupingLevel 
	inner join [TransformingCare_tblMetric] D on D.[ICS] = a.[ICS] and  dateadd( month , -1 ,a.ReportingMonth) = D.ReportingMonth and a.CommissioningArrangement = D.CommissioningArrangement and a.PatientGroup = D.PatientGroup and a.PatientCategory = D.PatientCategory and a.GroupingLevel = D.GroupingLevel 
	inner join [TransformingCare_tblMetric] E on E.[ICS] = a.[ICS] and  dateadd( month , -2 ,a.ReportingMonth) = E.ReportingMonth and a.CommissioningArrangement = E.CommissioningArrangement and a.PatientGroup = E.PatientGroup and a.PatientCategory = E.PatientCategory and a.GroupingLevel = E.GroupingLevel 
	inner join [TransformingCare_tblMetric] F on F.[ICS] = a.[ICS] and  dateadd( month , -2 ,a.ReportingMonth) = F.ReportingMonth and a.CommissioningArrangement = F.CommissioningArrangement and a.PatientGroup = F.PatientGroup and a.PatientCategory = F.PatientCategory and a.GroupingLevel = F.GroupingLevel 
	where a.GroupingLevel = 1 and b.GroupingLevel = 1 and A.MetricID = 66 and b.MetricID = 65 and C.MetricID = 66 and D.MetricID = 65 and E.MetricID = 66 and F.MetricID = 65 and cast(b.[Value] as decimal(18,3)) <> 0  and  cast(D.[Value] as decimal(18,3))  <> 0  and  cast(F.[Value] as decimal(18,3) ) <> 0 

	insert into #TransformingCare_tblMasterIndicator_BackTest 
	select a.ICS , a.ReportingMonth , 27 ,  (cast(a.[Value] as decimal(18,3)) /  cast(b.[Value] as decimal(18,3))  ) 
	, a.CommissioningArrangement , a.PatientGroup , a.GroupingLevel , a.PatientCategory
	from [TransformingCare_tblMetric] A 
	inner join [TransformingCare_tblMetric] B on b.[ICS] = a.[ICS] and a.ReportingMonth = b.ReportingMonth and a.CommissioningArrangement = b.CommissioningArrangement and a.PatientGroup = b.PatientGroup and a.PatientCategory = b.PatientCategory
	and a.GroupingLevel = b.GroupingLevel 
	where a.GroupingLevel = 1 and b.GroupingLevel = 1 and A.MetricID = 68 and b.MetricID = 67 and cast(b.[Value] as decimal(18,3)) <> 0

	insert into #TransformingCare_tblMasterIndicator_BackTest 
	select a.ICS , a.ReportingMonth , 28 ,  ( ( cast(a.[Value] as decimal(18,3)) + cast(C.[Value] as decimal(18,3)) + cast(E.[Value] as decimal(18,3)))  /  ( cast(b.[Value] as decimal(18,3)) + cast(D.[Value] as decimal(18,3)) + cast(F.[Value] as decimal(18,3) ) )  ) 
	, a.CommissioningArrangement , a.PatientGroup , a.GroupingLevel , a.PatientCategory
	from [TransformingCare_tblMetric] A 
	inner join [TransformingCare_tblMetric] B on b.[ICS] = a.[ICS] and a.ReportingMonth = b.ReportingMonth and a.CommissioningArrangement = b.CommissioningArrangement and a.PatientGroup = b.PatientGroup and a.PatientCategory = b.PatientCategory and a.GroupingLevel = b.GroupingLevel 
	inner join [TransformingCare_tblMetric] C on C.[ICS] = a.[ICS] and dateadd( month , -1 , a.ReportingMonth)  = C.ReportingMonth and a.CommissioningArrangement = C.CommissioningArrangement and a.PatientGroup = C.PatientGroup and a.PatientCategory = C.PatientCategory and a.GroupingLevel = C.GroupingLevel 
	inner join [TransformingCare_tblMetric] D on D.[ICS] = a.[ICS] and  dateadd( month , -1 ,a.ReportingMonth)  = D.ReportingMonth and a.CommissioningArrangement = D.CommissioningArrangement and a.PatientGroup = D.PatientGroup and a.PatientCategory = D.PatientCategory and a.GroupingLevel = D.GroupingLevel 
	inner join [TransformingCare_tblMetric] E on E.[ICS] = a.[ICS] and  dateadd( month , -2 ,a.ReportingMonth)  = E.ReportingMonth and a.CommissioningArrangement = E.CommissioningArrangement and a.PatientGroup = E.PatientGroup and a.PatientCategory = E.PatientCategory and a.GroupingLevel = E.GroupingLevel 
	inner join [TransformingCare_tblMetric] F on F.[ICS] = a.[ICS] and  dateadd( month , -2 ,a.ReportingMonth)  = F.ReportingMonth and a.CommissioningArrangement = F.CommissioningArrangement and a.PatientGroup = F.PatientGroup and a.PatientCategory = F.PatientCategory and a.GroupingLevel = F.GroupingLevel 
	where a.GroupingLevel = 1 and b.GroupingLevel = 1 and A.MetricID = 68 and b.MetricID = 67 and C.MetricID = 68 and D.MetricID = 67 and E.MetricID = 68 and F.MetricID = 67 and cast(b.[Value] as decimal(18,3)) <> 0  and  cast(D.[Value] as decimal(18,3))  <> 0  and  cast(F.[Value] as decimal(18,3) ) <> 0 

	insert into #TransformingCare_tblMasterIndicator_BackTest 
	select a.ICS , a.ReportingMonth , 29 ,  (cast(a.[Value] as decimal(18,3)) /  cast(b.[Value] as decimal(18,3))  ) 
	, a.CommissioningArrangement , a.PatientGroup , a.GroupingLevel , a.PatientCategory
	from [TransformingCare_tblMetric] A 
	inner join [TransformingCare_tblMetric] B on b.[ICS] = a.[ICS] and a.ReportingMonth = b.ReportingMonth and a.CommissioningArrangement = b.CommissioningArrangement and a.PatientGroup = b.PatientGroup and a.PatientCategory = b.PatientCategory
	and a.GroupingLevel = b.GroupingLevel 
	where a.GroupingLevel = 1 and b.GroupingLevel = 1 and A.MetricID = 70 and b.MetricID = 69 and cast(b.[Value] as decimal(18,3)) <> 0

	insert into #TransformingCare_tblMasterIndicator_BackTest 
	select a.ICS , a.ReportingMonth , 30 ,  ( ( cast(a.[Value] as decimal(18,3)) + cast(C.[Value] as decimal(18,3)) + cast(E.[Value] as decimal(18,3)))  /  ( cast(b.[Value] as decimal(18,3)) + cast(D.[Value] as decimal(18,3)) + cast(F.[Value] as decimal(18,3) ) )  ) 
	, a.CommissioningArrangement , a.PatientGroup , a.GroupingLevel , a.PatientCategory
	from [TransformingCare_tblMetric] A 
	inner join [TransformingCare_tblMetric] B on b.[ICS] = a.[ICS] and a.ReportingMonth = b.ReportingMonth and a.CommissioningArrangement = b.CommissioningArrangement and a.PatientGroup = b.PatientGroup and a.PatientCategory = b.PatientCategory and a.GroupingLevel = b.GroupingLevel 
	inner join [TransformingCare_tblMetric] C on C.[ICS] = a.[ICS] and  dateadd( month , -1 , a.ReportingMonth ) = C.ReportingMonth and a.CommissioningArrangement = C.CommissioningArrangement and a.PatientGroup = C.PatientGroup and a.PatientCategory = C.PatientCategory and a.GroupingLevel = C.GroupingLevel 
	inner join [TransformingCare_tblMetric] D on D.[ICS] = a.[ICS] and  dateadd( month , -1 , a.ReportingMonth ) = D.ReportingMonth and a.CommissioningArrangement = D.CommissioningArrangement and a.PatientGroup = D.PatientGroup and a.PatientCategory = D.PatientCategory and a.GroupingLevel = D.GroupingLevel 
	inner join [TransformingCare_tblMetric] E on E.[ICS] = a.[ICS] and  dateadd( month , -2 , a.ReportingMonth ) = E.ReportingMonth and a.CommissioningArrangement = E.CommissioningArrangement and a.PatientGroup = E.PatientGroup and a.PatientCategory = E.PatientCategory and a.GroupingLevel = E.GroupingLevel 
	inner join [TransformingCare_tblMetric] F on F.[ICS] = a.[ICS] and  dateadd( month , -2 , a.ReportingMonth ) = F.ReportingMonth and a.CommissioningArrangement = F.CommissioningArrangement and a.PatientGroup = F.PatientGroup and a.PatientCategory = F.PatientCategory and a.GroupingLevel = F.GroupingLevel 
	where a.GroupingLevel = 1 and b.GroupingLevel = 1 and A.MetricID = 70 and b.MetricID = 69 and C.MetricID = 70 and D.MetricID = 69 and E.MetricID = 70 and F.MetricID = 69 and cast(b.[Value] as decimal(18,3)) <> 0  and  cast(D.[Value] as decimal(18,3))  <> 0  and  cast(F.[Value] as decimal(18,3) ) <> 0 

	insert into #TransformingCare_tblMasterIndicator_BackTest 
	select a.ICS , a.ReportingMonth , 31 ,  (  cast(a.[Value] as decimal(18,3)  )  /   cast(b.[Value] as decimal(18,3)	)  )
	, a.CommissioningArrangement , a.PatientGroup , a.GroupingLevel , a.PatientCategory
	from [TransformingCare_tblMetric] A 
	inner join [TransformingCare_tblMetric] B on b.[ICS] = a.[ICS] and a.ReportingMonth = b.ReportingMonth and a.CommissioningArrangement = b.CommissioningArrangement and a.PatientGroup = b.PatientGroup and a.PatientCategory = b.PatientCategory and a.GroupingLevel = b.GroupingLevel 
	where a.GroupingLevel = 1 and b.GroupingLevel = 1  and A.MetricID = 75 and b.MetricID = 71  and cast(b.[Value] as decimal(18,3)) <> 0
	
	insert into #TransformingCare_tblMasterIndicator_BackTest 
	select a.ICS , a.ReportingMonth , 32 ,  ( ( cast(a.[Value] as decimal(18,3)) + cast(C.[Value] as decimal(18,3)) + cast(E.[Value] as decimal(18,3)))  /  ( cast(b.[Value] as decimal(18,3)) + cast(D.[Value] as decimal(18,3)) + cast(F.[Value] as decimal(18,3) ) )  )  
	, a.CommissioningArrangement , a.PatientGroup , a.GroupingLevel , a.PatientCategory
	from [TransformingCare_tblMetric] A 
	inner join [TransformingCare_tblMetric] B on b.[ICS] = a.[ICS] and a.ReportingMonth = b.ReportingMonth and a.CommissioningArrangement = b.CommissioningArrangement and a.PatientGroup = b.PatientGroup and a.PatientCategory = b.PatientCategory and a.GroupingLevel = b.GroupingLevel 
	inner join [TransformingCare_tblMetric] C on C.[ICS] = a.[ICS] and dateadd( month , -1 , a.ReportingMonth ) = C.ReportingMonth and a.CommissioningArrangement = C.CommissioningArrangement and a.PatientGroup = C.PatientGroup and a.PatientCategory = C.PatientCategory and a.GroupingLevel = C.GroupingLevel 
	inner join [TransformingCare_tblMetric] D on D.[ICS] = a.[ICS] and dateadd( month , -1 , a.ReportingMonth ) = D.ReportingMonth and a.CommissioningArrangement = D.CommissioningArrangement and a.PatientGroup = D.PatientGroup and a.PatientCategory = D.PatientCategory and a.GroupingLevel = D.GroupingLevel 
	inner join [TransformingCare_tblMetric] E on E.[ICS] = a.[ICS] and dateadd( month , -2 , a.ReportingMonth ) = E.ReportingMonth and a.CommissioningArrangement = E.CommissioningArrangement and a.PatientGroup = E.PatientGroup and a.PatientCategory = E.PatientCategory and a.GroupingLevel = E.GroupingLevel 
	inner join [TransformingCare_tblMetric] F on F.[ICS] = a.[ICS] and dateadd( month , -2 , a.ReportingMonth ) = F.ReportingMonth and a.CommissioningArrangement = F.CommissioningArrangement and a.PatientGroup = F.PatientGroup and a.PatientCategory = F.PatientCategory and a.GroupingLevel = F.GroupingLevel 
	where a.GroupingLevel = 1 and b.GroupingLevel = 1  and A.MetricID = 75 and b.MetricID = 71 and C.MetricID = 75 and D.MetricID = 71 and E.MetricID = 75 and F.MetricID = 71  and cast(b.[Value] as decimal(18,3)) <> 0  and  cast(D.[Value] as decimal(18,3))  <> 0  and  cast(F.[Value] as decimal(18,3) ) <> 0

	insert into #TransformingCare_tblMasterIndicator_BackTest 
	select a.ICS , a.ReportingMonth , 33 ,  (  cast(a.[Value] as decimal(18,3)  )  /   cast(b.[Value] as decimal(18,3)	)   )
	, a.CommissioningArrangement , a.PatientGroup , a.GroupingLevel , a.PatientCategory
	from [TransformingCare_tblMetric] A 
	inner join [TransformingCare_tblMetric] B on b.[ICS] = a.[ICS] and a.ReportingMonth = b.ReportingMonth and a.CommissioningArrangement = b.CommissioningArrangement and a.PatientGroup = b.PatientGroup and a.PatientCategory = b.PatientCategory and a.GroupingLevel = b.GroupingLevel 
	where a.GroupingLevel = 1 and b.GroupingLevel = 1  and A.MetricID = 76 and b.MetricID = 71  and cast(b.[Value] as decimal(18,3)) <> 0
	
	insert into #TransformingCare_tblMasterIndicator_BackTest 
	select a.ICS , a.ReportingMonth , 34 ,  ( ( cast(a.[Value] as decimal(18,3)) + cast(C.[Value] as decimal(18,3)) + cast(E.[Value] as decimal(18,3)))  /  ( cast(b.[Value] as decimal(18,3)) + cast(D.[Value] as decimal(18,3)) + cast(F.[Value] as decimal(18,3) ) )  )  
	, a.CommissioningArrangement , a.PatientGroup , a.GroupingLevel , a.PatientCategory
	from [TransformingCare_tblMetric] A 
	inner join [TransformingCare_tblMetric] B on b.[ICS] = a.[ICS] and a.ReportingMonth = b.ReportingMonth and a.CommissioningArrangement = b.CommissioningArrangement and a.PatientGroup = b.PatientGroup and a.PatientCategory = b.PatientCategory and a.GroupingLevel = b.GroupingLevel 
	inner join [TransformingCare_tblMetric] C on C.[ICS] = a.[ICS] and dateadd( month , -1 , a.ReportingMonth ) = C.ReportingMonth and a.CommissioningArrangement = C.CommissioningArrangement and a.PatientGroup = C.PatientGroup and a.PatientCategory = C.PatientCategory and a.GroupingLevel = C.GroupingLevel 
	inner join [TransformingCare_tblMetric] D on D.[ICS] = a.[ICS] and dateadd( month , -1 , a.ReportingMonth ) = D.ReportingMonth and a.CommissioningArrangement = D.CommissioningArrangement and a.PatientGroup = D.PatientGroup and a.PatientCategory = D.PatientCategory and a.GroupingLevel = D.GroupingLevel 
	inner join [TransformingCare_tblMetric] E on E.[ICS] = a.[ICS] and dateadd( month , -2 , a.ReportingMonth ) = E.ReportingMonth and a.CommissioningArrangement = E.CommissioningArrangement and a.PatientGroup = E.PatientGroup and a.PatientCategory = E.PatientCategory and a.GroupingLevel = E.GroupingLevel 
	inner join [TransformingCare_tblMetric] F on F.[ICS] = a.[ICS] and dateadd( month , -2 , a.ReportingMonth ) = F.ReportingMonth and a.CommissioningArrangement = F.CommissioningArrangement and a.PatientGroup = F.PatientGroup and a.PatientCategory = F.PatientCategory and a.GroupingLevel = F.GroupingLevel 
	where a.GroupingLevel = 1 and b.GroupingLevel = 1  and A.MetricID = 76 and b.MetricID = 71 and C.MetricID = 76 and D.MetricID = 71 and E.MetricID = 76 and F.MetricID = 71  and cast(b.[Value] as decimal(18,3)) <> 0  and  cast(D.[Value] as decimal(18,3))  <> 0  and  cast(F.[Value] as decimal(18,3) ) <> 0

	insert into #TransformingCare_tblMasterIndicator_BackTest 
	select a.ICS , a.ReportingMonth , 35 ,  (  cast(a.[Value] as decimal(18,3)  )  /   cast(b.[Value] as decimal(18,3)	)   )
	, a.CommissioningArrangement , a.PatientGroup , a.GroupingLevel , a.PatientCategory
	from [TransformingCare_tblMetric] A 
	inner join [TransformingCare_tblMetric] B on b.[ICS] = a.[ICS] and a.ReportingMonth = b.ReportingMonth and a.CommissioningArrangement = b.CommissioningArrangement and a.PatientGroup = b.PatientGroup and a.PatientCategory = b.PatientCategory and a.GroupingLevel = b.GroupingLevel 
	where a.GroupingLevel = 1 and b.GroupingLevel = 1  and A.MetricID = 80 and b.MetricID = 77 and cast(b.[Value] as decimal(18,3)) <> 0
	
	insert into #TransformingCare_tblMasterIndicator_BackTest 
	select a.ICS , a.ReportingMonth , 36 ,  ( ( cast(a.[Value] as decimal(18,3)) + cast(C.[Value] as decimal(18,3)) + cast(E.[Value] as decimal(18,3)))  /  ( cast(b.[Value] as decimal(18,3)) + cast(D.[Value] as decimal(18,3)) + cast(F.[Value] as decimal(18,3) ) )  )  
	, a.CommissioningArrangement , a.PatientGroup , a.GroupingLevel , a.PatientCategory
	from [TransformingCare_tblMetric] A 
	inner join [TransformingCare_tblMetric] B on b.[ICS] = a.[ICS] and a.ReportingMonth = b.ReportingMonth and a.CommissioningArrangement = b.CommissioningArrangement and a.PatientGroup = b.PatientGroup and a.PatientCategory = b.PatientCategory and a.GroupingLevel = b.GroupingLevel 
	inner join [TransformingCare_tblMetric] C on C.[ICS] = a.[ICS] and dateadd( month , -1 , a.ReportingMonth ) = C.ReportingMonth and a.CommissioningArrangement = C.CommissioningArrangement and a.PatientGroup = C.PatientGroup and a.PatientCategory = C.PatientCategory and a.GroupingLevel = C.GroupingLevel 
	inner join [TransformingCare_tblMetric] D on D.[ICS] = a.[ICS] and dateadd( month , -1 , a.ReportingMonth ) = D.ReportingMonth and a.CommissioningArrangement = D.CommissioningArrangement and a.PatientGroup = D.PatientGroup and a.PatientCategory = D.PatientCategory and a.GroupingLevel = D.GroupingLevel 
	inner join [TransformingCare_tblMetric] E on E.[ICS] = a.[ICS] and dateadd( month , -2 , a.ReportingMonth ) = E.ReportingMonth and a.CommissioningArrangement = E.CommissioningArrangement and a.PatientGroup = E.PatientGroup and a.PatientCategory = E.PatientCategory and a.GroupingLevel = E.GroupingLevel 
	inner join [TransformingCare_tblMetric] F on F.[ICS] = a.[ICS] and dateadd( month , -2 , a.ReportingMonth ) = F.ReportingMonth and a.CommissioningArrangement = F.CommissioningArrangement and a.PatientGroup = F.PatientGroup and a.PatientCategory = F.PatientCategory and a.GroupingLevel = F.GroupingLevel 
	where a.GroupingLevel = 1 and b.GroupingLevel = 1  and A.MetricID = 80 and b.MetricID = 77 and C.MetricID = 80 and D.MetricID = 77 and E.MetricID = 80 and F.MetricID = 77 and cast(b.[Value] as decimal(18,3)) <> 0  and  cast(D.[Value] as decimal(18,3))  <> 0  and  cast(F.[Value] as decimal(18,3) ) <> 0

	insert into #TransformingCare_tblMasterIndicator_BackTest 
	select a.ICS , a.ReportingMonth , 37 ,  (  cast(a.[Value] as decimal(18,3)  )  /   cast(b.[Value] as decimal(18,3)	) )
	, a.CommissioningArrangement , a.PatientGroup , a.GroupingLevel , a.PatientCategory
	from [TransformingCare_tblMetric] A 
	inner join [TransformingCare_tblMetric] B on b.[ICS] = a.[ICS] and a.ReportingMonth = b.ReportingMonth and a.CommissioningArrangement = b.CommissioningArrangement and a.PatientGroup = b.PatientGroup and a.PatientCategory = b.PatientCategory and a.GroupingLevel = b.GroupingLevel 
	where a.GroupingLevel = 1 and b.GroupingLevel = 1  and A.MetricID = 80 and b.MetricID = 81  and cast(b.[Value] as decimal(18,3)) <> 0
	
	insert into #TransformingCare_tblMasterIndicator_BackTest 
	select a.ICS , a.ReportingMonth , 38 ,  ( ( cast(a.[Value] as decimal(18,3)) + cast(C.[Value] as decimal(18,3)) + cast(E.[Value] as decimal(18,3)))  /  ( cast(b.[Value] as decimal(18,3)) + cast(D.[Value] as decimal(18,3)) + cast(F.[Value] as decimal(18,3) ) )  )  
	, a.CommissioningArrangement , a.PatientGroup , a.GroupingLevel , a.PatientCategory
	from [TransformingCare_tblMetric] A 
	inner join [TransformingCare_tblMetric] B on b.[ICS] = a.[ICS] and a.ReportingMonth = b.ReportingMonth and a.CommissioningArrangement = b.CommissioningArrangement and a.PatientGroup = b.PatientGroup and a.PatientCategory = b.PatientCategory and a.GroupingLevel = b.GroupingLevel 
	inner join [TransformingCare_tblMetric] C on C.[ICS] = a.[ICS] and dateadd( month , -1 , a.ReportingMonth ) = C.ReportingMonth and a.CommissioningArrangement = C.CommissioningArrangement and a.PatientGroup = C.PatientGroup and a.PatientCategory = C.PatientCategory and a.GroupingLevel = C.GroupingLevel 
	inner join [TransformingCare_tblMetric] D on D.[ICS] = a.[ICS] and dateadd( month , -1 , a.ReportingMonth ) = D.ReportingMonth and a.CommissioningArrangement = D.CommissioningArrangement and a.PatientGroup = D.PatientGroup and a.PatientCategory = D.PatientCategory and a.GroupingLevel = D.GroupingLevel 
	inner join [TransformingCare_tblMetric] E on E.[ICS] = a.[ICS] and dateadd( month , -2 , a.ReportingMonth ) = E.ReportingMonth and a.CommissioningArrangement = E.CommissioningArrangement and a.PatientGroup = E.PatientGroup and a.PatientCategory = E.PatientCategory and a.GroupingLevel = E.GroupingLevel 
	inner join [TransformingCare_tblMetric] F on F.[ICS] = a.[ICS] and dateadd( month , -2 , a.ReportingMonth ) = F.ReportingMonth and a.CommissioningArrangement = F.CommissioningArrangement and a.PatientGroup = F.PatientGroup and a.PatientCategory = F.PatientCategory and a.GroupingLevel = F.GroupingLevel 
	where a.GroupingLevel = 1 and b.GroupingLevel = 1  and A.MetricID = 84 and b.MetricID = 81 and C.MetricID = 84 and D.MetricID = 81 and E.MetricID = 84 and F.MetricID = 81 and cast(b.[Value] as decimal(18,3)) <> 0  and  cast(D.[Value] as decimal(18,3))  <> 0  and  cast(F.[Value] as decimal(18,3) ) <> 0


-------------------------------------------------------------------------------------------------------------------------------------------------------
---- Update Current Live table Start ------------------------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------------------------------------------------------------
	
	update B set b.[Value] = iif ( a.[Value] is null , 0 , a.[Value] ) 
	from #TransformingCare_tblMasterIndicator_BackTest  A 
	inner join [dbo].[TransformingCare_tblIndicator] B 
	on a.ICS = b.ICS and a.[ReportingMonth] = b.[ReportingMonth] and a.[IndicatorID] = b.[IndicatorID] and a.[CommissioningArrangement] = b.[CommissioningArrangement] and a.PatientGroup = b.PatientGroup and  a.[GroupingLevel] = b.[GroupingLevel] and a.PatientCategory = b.PatientCategory  
	where a.[Value] <> b.[Value] 

-------------------------------------------------------------------------------------------------------------------------------------------------------
---- Update Current Live table End --------------------------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------------------------------------------------------------

-------------------------------------------------------------------------------------------------------------------------------------------------------
---- Insert New Values into live table Start ----------------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------------------------------------------------------------

   insert into [NHSE_Sandbox_Lon_LDAProgramme].[dbo].[TransformingCare_tblIndicator]
   select a.* from #TransformingCare_tblMasterIndicator_BackTest A
   left join [NHSE_Sandbox_Lon_LDAProgramme].[dbo].[TransformingCare_tblIndicator] B 
   on a.ICS = b.ICS and a.[ReportingMonth] = b.[ReportingMonth] and a.[IndicatorID] = b.[IndicatorID] and a.[CommissioningArrangement] = b.[CommissioningArrangement] and a.PatientGroup = b.PatientGroup and  a.[GroupingLevel] = b.[GroupingLevel] and a.PatientCategory = b.PatientCategory  
   where b.ICS is null 
	
-------------------------------------------------------------------------------------------------------------------------------------------------------
---- Insert New Values into live table End ------------------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------------------------------------------------------------

	drop table #TransformingCare_tblMasterIndicator_BackTest 

END
GO


