USE [NHSE_Sandbox_Lon_LDAProgramme]
GO

/****** Object:  StoredProcedure [dbo].[SSRS_TransformingCare_WeeklyAT]    Script Date: 17/08/2022 21:18:43 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO





CREATE PROCEDURE [dbo].[SSRS_TransformingCare_WeeklyAT] @ReportingDate datetime 
as	
BEGIN

  SET NOCOUNT ON;	


  declare @SQL varchar(8000) 

--  print(cast(@ReportingDate as varchar(13)))
--  print(cast(year(@ReportingDate) as varchar(4)) + '-' + right(  '00' + cast( month(@ReportingDate) as varchar(2))  ,2)  + '-' + right(  '00' + cast( day(@ReportingDate) as varchar(2))  ,2) ) 

  delete from [NHSE_Sandbox_Lon_LDAProgramme].[dbo].[TransformingCare_RAW_WeeklyAT] 
  delete from [NHSE_Sandbox_Lon_LDAProgramme].[dbo].[TransformingCare_Live_WeeklyAT] 

  set @SQL = 'insert into [NHSE_Sandbox_Lon_LDAProgramme].[dbo].[TransformingCare_RAW_WeeklyAT] select * from  [NHSE_Sandbox_Lon_LDAProgramme].[dbo].[' + ( cast(year(@ReportingDate) as varchar(4)) + '-' + right(  '00' + cast( month(@ReportingDate) as varchar(2))  ,2)  + '-' + right(  '00' + cast( day(@ReportingDate) as varchar(2))  ,2) ) + '_Anon_AT_Weekly_Extract_RESTRICTED]'
  exec (@SQL)

 insert into [NHSE_Sandbox_Lon_LDAProgramme].[dbo].[TransformingCare_Live_WeeklyAT]
 SELECT 
 c.[UniqueID] 
 , iif(  c.UniqueID is null , 'No ID ' + a.patientID , c.UniqueID ) as [Patient Unique ID] 
 , f.[Patient reference number] as [NHSE Old Patient ID] 
 , REPLACE(B.[TCP Groups],'2','') as [London TCP Patient] 
 , a.SubmittingCCGnew as [Submitting Commissioner Code] 
 , e.[CommissioningArrangement] as [CCG/SC/PC]
 , e.[CCG name] as [Submitting Commissioner Name]
 , a.OriginatingCCGnew as [Originating CCG Code] 
 , b.[CCG name]  as [Originating CCG Name]
 , a.HospitalAdmissionDate as [Hospital Admission Date]
 , a.PreviousAdmissionDate as [Previous Admission Date]
 , case 
	when a.PostcodeType = 19	then 'Usual place of residence'	
	when a.PostcodeType = 29	then 'Temporary place of residence'
	when a.PostcodeType = 39	then 'Penal establishment, court, or police station'	
	when a.PostcodeType = 49	then 'NHS other hospital provider - high security'
	when a.PostcodeType = 51	then 'NHS other hospital provider - general ward for physically disabled or A & E department'	
	when a.PostcodeType = 53	then 'NHS other hospital provider - MH or LD ward'
	when a.PostcodeType = 54	then 'NHS run care home'	
	when a.PostcodeType = 65	then 'Local Authority residential accommodation'
	when a.PostcodeType = 66	then 'Local Authority foster care'	
	when a.PostcodeType = 85	then 'Non-NHS run care home'
	when a.PostcodeType = 87	then 'Non-NHS run hospital'	
	when a.PostcodeType = 88	then 'Non-NHS run Hospice'
  end as [Source of admission] 
 , a.IC_ReadmissionYr as [Admission Type] 
 , case 
	when a.AdmissionPlan = 1	then 'Planned'
	when a.AdmissionPlan = 2	then 'Unplanned'
	when a.AdmissionPlan = 9	then 'Not Known'	
  end as [Planned Admission] 
 , a.PreAdmissionCareAndTreatmentReview as [Patient had a pre-admission CTR?]
 , a.PreAdmissionCareAndTreatmentReviewDate as [Pre-admission CTR date]
 , cast( a.IC_LOS  as int )/ 362.25 as [LOS in current location (yrs)]
 , cast( a.IC_Total_LOS  as int )/ 362.25 as [Total LOS (yrs)]	 
 , case 
    when cast( a.IC_Total_LOS  as int )/ 362.25 < 0.5 then '0-6 months'
    when cast( a.IC_Total_LOS  as int )/ 362.25 < 1 and cast( a.IC_Total_LOS  as int )/ 362.25 >= 0.5  then '7-12 months'
    when cast( a.IC_Total_LOS  as int )/ 362.25 < 2 and cast( a.IC_Total_LOS  as int )/ 362.25 >= 1 then '1-2 years'
    when cast( a.IC_Total_LOS  as int )/ 362.25 < 3 and cast( a.IC_Total_LOS  as int )/ 362.25 >= 2 then '2-3 years'
    when cast( a.IC_Total_LOS  as int )/ 362.25 < 4 and cast( a.IC_Total_LOS  as int )/ 362.25 >= 3 then '3-4 years'
    when cast( a.IC_Total_LOS  as int )/ 362.25 < 5 and cast( a.IC_Total_LOS  as int )/ 362.25 >= 4 then '4-5 years'
    when cast( a.IC_Total_LOS  as int )/ 362.25 < 10 and cast( a.IC_Total_LOS  as int )/ 362.25 >= 5 then '5-10 years'
    when cast( a.IC_Total_LOS  as int )/ 362.25 < 15 and cast( a.IC_Total_LOS  as int )/ 362.25 >= 10 then '10-15 years'
    when cast( a.IC_Total_LOS  as int )/ 362.25 >= 0.5 then '> 15 yrs'
 end as [LOS Group]
 , a.CurrentProviderOrganisation as [Provider Code]
 , a.CurrentProviderOrganisationName as [Provider name] 
 , a.TreatingHospitalName as [Provider site] 
 , a.TreatingHospitalPostcode as [Provider Postcode] 
 , case 
	when a.Gender = '1' then 'Male' 
	when a.Gender = '2' then 'Female' 
	when a.Gender = '9' then 'Indeterminate' 
	when a.Gender = 'x' then 'Not known'
   end as Gender
 , case 
	when a.EthnicCategory = 'A'  	then 'British						'
	when a.EthnicCategory = 'B'	then 'Irish							'
	when a.EthnicCategory = 'C'	then 'Any other White background	'
	when a.EthnicCategory = 'D'	then 'White and Black Caribbean		'
	when a.EthnicCategory = 'E'	then 'White and Black African		'
	when a.EthnicCategory = 'F'	then 'White and Asian				'
	when a.EthnicCategory = 'G'	then 'Any other mixed background	'
	when a.EthnicCategory = 'H'	then 'Indian						'
	when a.EthnicCategory = 'J'	then 'Pakistani						'
	when a.EthnicCategory = 'K'	then 'Bangladeshi					'
	when a.EthnicCategory = 'L'	then 'Any other Asian background	'
	when a.EthnicCategory = 'M'	then 'Caribbean						'
	when a.EthnicCategory = 'N'	then 'African						'
	when a.EthnicCategory = 'P'	then 'Any other Black background	'
	when a.EthnicCategory = 'R'	then 'Chinese						'
	when a.EthnicCategory = 'S'	then 'Any other ethnic group		'
	when a.EthnicCategory = 'Z'	then 'Not stated					'
	when a.EthnicCategory = '99'	then 'Not known						'
  end 
  as Ethnicity
  , a.IC_age_PeriodEnd as [Patient age] 
  , a.Registered as [Patient on at risk register?] 
  , case 
  when a.PatientCategory = 1 then 'LD only' 
  when a.PatientCategory = 2 then 'LD and ASD' 
  when a.PatientCategory = 3 then 'ASD only' 
  when a.PatientCategory = 4 then 'None' 
 end as [Patient Category]
 , case
	when try_cast( a.DiagnosticCategory as int )  = 1	then 'Autism											'
	when try_cast( a.DiagnosticCategory as int )  = 2	then 'Learning Disability								'
	when try_cast( a.DiagnosticCategory as int )  = 3	then 'Eating Disorder									'
	when try_cast( a.DiagnosticCategory as int )  = 4	then 'Personality Disorder								'
	when try_cast( a.DiagnosticCategory as int )  = 5	then 'Physical illness									'
	when try_cast( a.DiagnosticCategory as int )  = 6	then 'Mental illness									'
	when try_cast( a.DiagnosticCategory as int )  = 7	then 'Attention deficit hyperactivity disorder (ADHD)	'
	when try_cast( a.DiagnosticCategory as int )  = 8	then 'Dementia											'
	when try_cast( a.DiagnosticCategory as int )  = 9	then 'Other												'
	when try_cast( a.DiagnosticCategory as int )  = 10	then 'Not Known											'
	when try_cast( a.DiagnosticCategory as int )  = 11	then 'None												'
	end as [Diagnosis on Admission] 
 , case 
 when a.DetainedUnderAct = 1	then 'Informal									'
 when a.DetainedUnderAct = 2	then 'Section 2									'
 when a.DetainedUnderAct = 3	then 'Section 3									'
 when a.DetainedUnderAct = 4	then 'Section 4									'
 when a.DetainedUnderAct = 5	then 'Section 5(2)								'
 when a.DetainedUnderAct = 6	then 'Section 5(4)								'
 when a.DetainedUnderAct = 7	then 'Section 35								'
 when a.DetainedUnderAct = 8	then 'Section 36								'
 when a.DetainedUnderAct = 9	then 'Section 37 with section 41 restrictions	'
 when a.DetainedUnderAct = 10	then 'Section 37								'
 when a.DetainedUnderAct = 12	then 'Section 38								'
 when a.DetainedUnderAct = 13	then 'Section 44								'
 when a.DetainedUnderAct = 14	then 'Section 46								'
 when a.DetainedUnderAct = 15	then 'Section 47 with Section 49 restrictions	'
 when a.DetainedUnderAct = 16	then 'Section 47								'
 when a.DetainedUnderAct = 17	then 'Section 48/49								'
 when a.DetainedUnderAct = 18	then 'Section 48								'
 when a.DetainedUnderAct = 19	then 'Section 135								'
 when a.DetainedUnderAct = 20	then 'Section 136								'
 when a.DetainedUnderAct = 31	then 'Criminal Procedure(Insanity) Act 1964		'
 when a.DetainedUnderAct = 32	then 'Other act									'
 when a.DetainedUnderAct = 34	then 'Section 45A								'
 when a.DetainedUnderAct = 35	then 'Section 7 (Guardianship)					'
 when a.DetainedUnderAct = 36	then 'Section 37 (Guardianship)					'
 when a.DetainedUnderAct = 37	then 'Section 45A (Limited direction in force)	'
 when a.DetainedUnderAct = 38	then 'Section 45A (Limitation direction ended)	'
 when a.DetainedUnderAct = 98	then 'Not applicable							'
 when a.DetainedUnderAct = 99	then 'Not known									'
 end as [MH Section] 
, case
when a.WardSecurityLevel = 0	then 'General (non-secure)					'
when a.WardSecurityLevel = 1	then 'Low Secure							'
when a.WardSecurityLevel = 2	then 'Medium Secure							'
when a.WardSecurityLevel = 3	then 'High Secure							'
when a.WardSecurityLevel = 4	then 'Psychiatric Intensive Care Unit (PICU)'
end as [Ward Security Level] 
, 
case 
when try_cast(a.WardType as int ) = 1	then 'Low, Medium and High secure forensic beds								'
when try_cast(a.WardType as int ) = 2	then 'Acute admission beds within specialised LD units						'
when try_cast(a.WardType as int ) = 3	then 'Acute admission beds within generic MH settings						'
when try_cast(a.WardType as int ) = 4	then 'Forensic rehab beds													'
when try_cast(a.WardType as int ) = 5	then 'Complex continuing care and rehab beds								'
when try_cast(a.WardType as int ) = 6	then 'Other beds including those for specialist neuropsychiatric conditions	'
when try_cast(a.WardType as int ) = 9	then 'Other																	'
end as [Ward Type] 
, a.NamedCareCoOrdinator as [Named care coordinator/care manager?] 
, cast ( right(a.ReviewDate , 4 ) + '-' + left(right(a.ReviewDate , 7 ),2) + '-' + left(a.ReviewDate ,2) as datetime )    as [Last Care Plan Review Date] 
,  cast( @ReportingDate - cast ( right(a.ReviewDate , 4 ) + '-' + left(right(a.ReviewDate , 7 ),2) + '-' + left(a.ReviewDate ,2) as datetime )  as int )  as [Days since last care plan review] 
 , a.Advocacy as [Patient has an advocate?] 
 , 
 case
 when try_cast(a.NoAdvocacyReason  as int ) = 1	then 'Yes								'
 when try_cast(a.NoAdvocacyReason  as int ) = 2	then 'No - at the request of the patient'
 when try_cast(a.NoAdvocacyReason  as int ) = 3	then 'No - access restrictions on family'
 when try_cast(a.NoAdvocacyReason  as int ) = 4	then 'No family involved				'
 when try_cast(a.NoAdvocacyReason  as int ) = 5	then 'No family living					'
 when try_cast(a.NoAdvocacyReason  as int ) = 9	then 'Don t know						'
 end as [Reason for no advocacy] 
 , a.IndependentAdvocateFamilyMember as [Family member advocate] 
 , a.IndependentAdvocateIndependentPerson as [Independent advocate] 
 , a.IndependentAdvocateImca as [IMCA] 
 , a.IndependentAdvocateImha as [IMHA] 
 , a.IndependentAdvocateNonInstructed as [Non instructed advocate] 
 , case
 when a.Family  = 1	then 'Yes								'
 when a.Family  = 2	then 'No – at the request of the patient'
 when a.Family  = 3	then 'No – access restrictions on family'
 when a.Family  = 4	then 'No family involved				'
 when a.Family  = 5	then 'No family living					'
 when a.Family  = 9	then 'Don’t know						'
 end as [Family involved in care plan?]

 ,  a.PostAdmissionCtr as [Patient had a post-admission CTR?] 
 , a.PostAdmissionCtrDate as [Post-admission CTR date] 
 , iif ( a.MostRecentCtrDate is null or a.MostRecentCtrDate = '02/01/1900' , null , cast ( right(a.MostRecentCtrDate , 4 ) + '-' + left(right(a.MostRecentCtrDate , 7 ),2) + '-' + left(a.MostRecentCtrDate ,2) as datetime ) )   as [Most recent CTR date] 
 , iif ( a.MostRecentCtrDate is null or a.MostRecentCtrDate = '02/01/1900' , null ,  (cast( @reportingdate -  cast ( right(a.MostRecentCtrDate , 4 ) + '-' + left(right(a.MostRecentCtrDate , 7 ),2) + '-' + left(a.MostRecentCtrDate ,2) as datetime )  as decimal(18,3)  ) /365)*12 ) as [Months since last CTR] 

  , case 
    when iif ( a.MostRecentCtrDate is null or a.MostRecentCtrDate = '02/01/1900' , null ,  (cast( @reportingdate -  cast ( right(a.MostRecentCtrDate , 4 ) + '-' + left(right(a.MostRecentCtrDate , 7 ),2) + '-' + left(a.MostRecentCtrDate ,2) as datetime )  as decimal(18,3)  ) /365)*12 ) < 0.5 then '0-6 months'
    when iif ( a.MostRecentCtrDate is null or a.MostRecentCtrDate = '02/01/1900' , null ,  (cast( @reportingdate -  cast ( right(a.MostRecentCtrDate , 4 ) + '-' + left(right(a.MostRecentCtrDate , 7 ),2) + '-' + left(a.MostRecentCtrDate ,2) as datetime )  as decimal(18,3)  ) /365)*12 ) < 1 and cast( a.IC_Total_LOS  as int )/ 362.25 >= 0.5  then '7-12 months'
    when iif ( a.MostRecentCtrDate is null or a.MostRecentCtrDate = '02/01/1900' , null ,  (cast( @reportingdate -  cast ( right(a.MostRecentCtrDate , 4 ) + '-' + left(right(a.MostRecentCtrDate , 7 ),2) + '-' + left(a.MostRecentCtrDate ,2) as datetime )  as decimal(18,3)  ) /365)*12 ) < 2 and cast( a.IC_Total_LOS  as int )/ 362.25 >= 1 then '1-2 years'
    when iif ( a.MostRecentCtrDate is null or a.MostRecentCtrDate = '02/01/1900' , null ,  (cast( @reportingdate -  cast ( right(a.MostRecentCtrDate , 4 ) + '-' + left(right(a.MostRecentCtrDate , 7 ),2) + '-' + left(a.MostRecentCtrDate ,2) as datetime )  as decimal(18,3)  ) /365)*12 ) < 3 and cast( a.IC_Total_LOS  as int )/ 362.25 >= 2 then '2-3 years'
    when iif ( a.MostRecentCtrDate is null or a.MostRecentCtrDate = '02/01/1900' , null ,  (cast( @reportingdate -  cast ( right(a.MostRecentCtrDate , 4 ) + '-' + left(right(a.MostRecentCtrDate , 7 ),2) + '-' + left(a.MostRecentCtrDate ,2) as datetime )  as decimal(18,3)  ) /365)*12 ) < 4 and cast( a.IC_Total_LOS  as int )/ 362.25 >= 3 then '3-4 years'
    when iif ( a.MostRecentCtrDate is null or a.MostRecentCtrDate = '02/01/1900' , null ,  (cast( @reportingdate -  cast ( right(a.MostRecentCtrDate , 4 ) + '-' + left(right(a.MostRecentCtrDate , 7 ),2) + '-' + left(a.MostRecentCtrDate ,2) as datetime )  as decimal(18,3)  ) /365)*12 ) < 5 and cast( a.IC_Total_LOS  as int )/ 362.25 >= 4 then '4-5 years'
    when iif ( a.MostRecentCtrDate is null or a.MostRecentCtrDate = '02/01/1900' , null ,  (cast( @reportingdate -  cast ( right(a.MostRecentCtrDate , 4 ) + '-' + left(right(a.MostRecentCtrDate , 7 ),2) + '-' + left(a.MostRecentCtrDate ,2) as datetime )  as decimal(18,3)  ) /365)*12 ) < 10 and cast( a.IC_Total_LOS  as int )/ 362.25 >= 5 then '5-10 years'
    when iif ( a.MostRecentCtrDate is null or a.MostRecentCtrDate = '02/01/1900' , null ,  (cast( @reportingdate -  cast ( right(a.MostRecentCtrDate , 4 ) + '-' + left(right(a.MostRecentCtrDate , 7 ),2) + '-' + left(a.MostRecentCtrDate ,2) as datetime )  as decimal(18,3)  ) /365)*12 ) < 15 and cast( a.IC_Total_LOS  as int )/ 362.25 >= 10 then '10-15 years'
    when iif ( a.MostRecentCtrDate is null or a.MostRecentCtrDate = '02/01/1900' , null ,  (cast( @reportingdate -  cast ( right(a.MostRecentCtrDate , 4 ) + '-' + left(right(a.MostRecentCtrDate , 7 ),2) + '-' + left(a.MostRecentCtrDate ,2) as datetime )  as decimal(18,3)  ) /365)*12 ) >= 0.5 then '> 15 yrs'
 end as [Months since last CTR (By Band)]
 
,  case 
	when cast( left(OutcomeOfCtr,2) as int ) = 2	then 'Ready for discharge, discharge plan in place & discharge date in next 4-6 months'
	when cast( left(OutcomeOfCtr,2) as int ) = 3	then 'Ready for discharge - no discharge plan'
	when cast( left(OutcomeOfCtr,2) as int ) = 4	then 'Not ready for discharge - needs to be in a hospital bed for care & treatment'
	when cast( left(OutcomeOfCtr,2) as int ) = 5	then 'Ready for discharge, discharge plan in place but no specified date'
	when cast( left(OutcomeOfCtr,2) as int ) = 6	then 'Ready for discharge, discharge plan in place & discharge date in next 1 month'
	when cast( left(OutcomeOfCtr,2) as int ) = 7	then 'Ready for discharge, discharge plan in place & discharge date in next 2-3 months'
	when cast( left(OutcomeOfCtr,2) as int ) = 8	then 'Patient refused C(E)TR'
	when cast( left(OutcomeOfCtr,2) as int ) = 9	then 'Not applicable'
	when cast( left(OutcomeOfCtr,2) as int ) = 1	then 'Currently not dischargeable because of level of behaviour that presents a risk to the person or others, or mental illness'
 end as [Outcome of CTR] 
 
 , a.ScheduledCtrDate as [Scheduled CTR Date] 

 , iif( a.DateOfPlannedTransfer = '' or a.DateOfPlannedTransfer = 'Null' , null ,cast ( right(a.DateOfPlannedTransfer , 4 ) + '-' + left(right(a.DateOfPlannedTransfer , 7 ),2) + '-' + left(a.DateOfPlannedTransfer ,2) as datetime ) )   as [Date of Planned Transfer/ Discharge]   
 , iif( a.DateOfPlannedTransfer = ''  or a.DateOfPlannedTransfer = 'Null' , null , cast ( cast ( right(a.DateOfPlannedTransfer , 4 ) + '-' + left(right(a.DateOfPlannedTransfer , 7 ),2) + '-' + left(a.DateOfPlannedTransfer ,2) as datetime )  - @ReportingDate as decimal(18,3) ) / 12 ) as [Months to the planned transfer / discharge]  -- Months to planned transfer (Need SP for that) 

, case 
    when iif( a.DateOfPlannedTransfer = '' or a.DateOfPlannedTransfer = 'Null' , null , cast ( cast ( right(a.DateOfPlannedTransfer , 4 ) + '-' + left(right(a.DateOfPlannedTransfer , 7 ),2) + '-' + left(a.DateOfPlannedTransfer ,2) as datetime )  - @ReportingDate as decimal(18,3) ) / 12 ) < 0   then 'Date in the past'
    when iif( a.DateOfPlannedTransfer = '' or a.DateOfPlannedTransfer = 'Null' , null , cast ( cast ( right(a.DateOfPlannedTransfer , 4 ) + '-' + left(right(a.DateOfPlannedTransfer , 7 ),2) + '-' + left(a.DateOfPlannedTransfer ,2) as datetime )  - @ReportingDate as decimal(18,3) ) / 12 ) BETWEEN 0 AND 3  then 'Less than 3 months'
    when iif( a.DateOfPlannedTransfer = '' or a.DateOfPlannedTransfer = 'Null' , null , cast ( cast ( right(a.DateOfPlannedTransfer , 4 ) + '-' + left(right(a.DateOfPlannedTransfer , 7 ),2) + '-' + left(a.DateOfPlannedTransfer ,2) as datetime )  - @ReportingDate as decimal(18,3) ) / 12 ) BETWEEN 3 AND 6    then '3-6 months'
    when iif( a.DateOfPlannedTransfer = '' or a.DateOfPlannedTransfer = 'Null' , null , cast ( cast ( right(a.DateOfPlannedTransfer , 4 ) + '-' + left(right(a.DateOfPlannedTransfer , 7 ),2) + '-' + left(a.DateOfPlannedTransfer ,2) as datetime )  - @ReportingDate as decimal(18,3) ) / 12 ) BETWEEN 6 AND 12   then '6-12 months'
    when iif( a.DateOfPlannedTransfer = '' or a.DateOfPlannedTransfer = 'Null' , null , cast ( cast ( right(a.DateOfPlannedTransfer , 4 ) + '-' + left(right(a.DateOfPlannedTransfer , 7 ),2) + '-' + left(a.DateOfPlannedTransfer ,2) as datetime )  - @ReportingDate as decimal(18,3) ) / 12 ) > 12    then '1 year or more'
	else 'No date given' 
 end as [Months to the planned transfer/discharge date (By Band or Date in the Past)]
 , g.[discharge or transfer]
 , g.[Q35] 
 , a.Discharge as [Discharge under CTO considered] 
 , a.DischargePlanAgreementPatient as [Patient agreement] 
 , a.DischargePlanAgreementFamilyCarer as [Family carer agreement] 
 , a.DischargePlanAgreementAdvocate as [Advocate agreement] 
 , a.DischargePlanAgreementProviderClinicalTeam as [Clinical team agreement] 
 , a.DischargePlanAgreementLocalCommunitySupportTeam as [Community Support Team agreement] 
 , a.DischargePlanAgreementCommissioners as [Commissioners agreement] 
 , case 
	when a.PatientCarePlan = 2 then 'Currently receiving active treatment plan, discharge plan not in place'
	when a.PatientCarePlan = 3 then 'Currently receiving active treatment and working towards discharge or with discharge plan in place '
	when a.PatientCarePlan = 6 then 'Delayed transfer of care: no community option or onward placement available or deliverable'
	when a.PatientCarePlan = 7 then 'Unable to discharge due to legal restrictions (e.g. MM judgement)'
   end as [Detail of patient's discharge plan] 
 , a.TransferReasonLackOfAgreedHealthCareFunding  
 , a.TransferReasonLackOfAgreedSocialCareFunding
 , a.TransferReasonAwaitingNonAcute	
 , a.TransferReasonAwaitingResidentialHome
 , a.TransferReasonAwaitingNursingHome
 , a.TransferReasonAwaitingCarePackageOwnHome
 , a.TransferReasonAwaitingCommunityEquipment
 , a.TransferReasonPatientOrFamilyChoise	
 , a.TransferReasonLackOfLocalHealthServiceProvision
 , a.TransferReasonLackOfLocalSocialCareSupport	
 , a.TransferReasonLackOfSuitableHousingProvision
 , a.TransferReasonOther  
 , a.PatientCreatedDate as [Date patient record created]
 , a.PatientUpdatedDate as [Date patient record last updated]
 , a.EpisodeCreatedDate as [Date patient episode created] 
 , a.EpisodeUpdatedDate as [Date patient episode last updated] 
 , H.[Region of Site Postcode] 
 , iif( H.[Region of Site Postcode] = 'LONDON' , 'London Site' , 'OOA' ) as [Out of area patient or site within London] 
 , iif ( a.IC_age_PeriodEnd < 18 , 'CYP' , 'Adult' )  as [Adult or CYP (U18)?]
 --,  a.PatientID + ' ' + cast( a.HospitalAdmissionDate as varchar(13))  
 , @ReportingDate as [ReportingDate]

 FROM [NHSE_Sandbox_Lon_LDAProgramme].[dbo].[TransformingCare_RAW_WeeklyAT] A 
 left join [NHSE_Sandbox_Lon_LDAProgramme].[dbo].[TransformingCare_LookupCCG] B on a.OriginatingCCGnew = b.[CCG Code]
 left join [NHSE_Sandbox_LDP_Shared].[dbo].[Inpatients] C on c.PatientID = a.PatientID
 left join [NHSE_Sandbox_Lon_LDAProgramme].[dbo].[TransformingCare_LookupDischargeCode] D on D.[code old] =  try_cast( a.[Transfer] as int ) 	 
 left join [NHSE_Sandbox_Lon_LDAProgramme].[dbo].[TransformingCare_LookupCCG] E on a.SubmittingCCGnew = E.[CCG Code]
 
 left join [NHSE_Sandbox_Lon_LDAProgramme].[dbo].[TransformingCare_LookupDischargeLocation] G on G.[Code] = try_cast( a.[Transfer] as int ) 
 left join [NHSE_Sandbox_Lon_LDAProgramme].[dbo].[TransformingCare_LookupPostcode] H on H.[Cleansed CCG of Site Postcode] = a.TreatingHospitalPostcode

 outer apply ( select min([Patient reference number]) as [Patient reference number]  from [NHSE_Sandbox_Lon_LDAProgramme].[dbo].[TransformingCare_LookupOldPatientReferenceNumber] 
 where [UniqueID] = c.[UniqueID] 
 group by [UniqueID]
 ) as F  

 where  b.[region] = 'London' and ( 	DateOfActualTransfer = '' or  	DateOfActualTransfer = 'NULL' ) 

insert into  [NHSE_Sandbox_Lon_LDAProgramme].[dbo].[TransformingCare_Archive_RawWeeklyAT]
select cast('2022-03-13' as datetime) as [ReportingDate] , a.* 
FROM [NHSE_Sandbox_Lon_LDAProgramme].[dbo].[TransformingCare_RAW_WeeklyAT] A 
left join [NHSE_Sandbox_Lon_LDAProgramme].[dbo].[TransformingCare_Archive_RawWeeklyAT] B on a.PatientID = b.PatientID and a.EpisodeID = b.EpisodeID and b.[ReportingDate] = @ReportingDate
where b.PatientID is null

insert into [NHSE_Sandbox_Lon_LDAProgramme].[dbo].[TransformingCare_Archive_WeeklyAT] 
select a.* from  [NHSE_Sandbox_Lon_LDAProgramme].[dbo].[TransformingCare_Live_WeeklyAT] A 
left join [NHSE_Sandbox_Lon_LDAProgramme].[dbo].[TransformingCare_Archive_WeeklyAT] B on a.[Patient Unique ID] = b.[Patient Unique ID] and a.[ReportingDate] = b.[ReportingDate]
where b.[ReportingDate] is null 



END
GO


