### Documentation

This DAG extracts Control-M Schedule data from Control-M Automation API and loads them into SQL Server as normalized tables.

#### Motivation

A lot of companies have legacy workloads that are scheduled on an Enterprise scheduler like BMC Control-M. In order to access schedule of jobs and obtain their dependency information, you have to either rely on Control-M Automation CLI, Control-M Automation API, or the Control-M UI. The entirety of schedule itself is only available as an XML file. This makes it difficult for a person to efficiently query this data and obtain useful information from it. This DAGs extracts the data within the XML files and makes the data available in normalized table that any person can query.

#### Table Details

* `DATAWAREHOUSE.METADATADB.dbo.CONTROLM_SCHEDULE`
* `DATAWAREHOUSE.METADATADB.dbo.CONTROLM_CONDITIONS`
* `DATAWAREHOUSE.METADATADB.dbo.CONTROLM_SHOUTS`
* `DATAWAREHOUSE.METADATADB.dbo.CONTROLM_CMDLINE`


#### DAG Dependencies

* Airflow HTTP Connection: `CONTROL_M_APICREDS`

* Airflow SQL Server Connection: `DATAWAREHOUSE`

* SQL Server Permission: `DATAWAREHOUSE.METADATADB`

#### Examples Queries

* Dependencies of a job

```sql
SELECT *
  FROM METADATADB.dbo.CONTROLM_CONDITIONS
WHERE jobname = 'job_name'
ORDER BY type;
```

* Schedule of a job

```sql
SELECT jobname,
       description,
       nodeid,
       weekdays,
       months,
       timefrom,
       timeto
  FROM METADATADB.dbo.CONTROLM_SCHEDULE
WHERE active_till IS NULL

```

#### Table Schemas

* CONTROLM_SCHEDULE

```sql
CREATE TABLE dbo.CONTROLM_SCHEDULE(
	jobname nvarchar(max) NULL,
	description nvarchar(max) NULL,
	cmdline nvarchar(max) NULL,
	run_as nvarchar(max) NULL,
	interval nvarchar(max) NULL,
	timezone nvarchar(max) NULL,
	nodeid nvarchar(max) NULL,
	tasktype nvarchar(max) NULL,
	weekdays nvarchar(max) NULL,
	months nvarchar(max) NULL,
	timefrom nvarchar(max) NULL,
	timeto nvarchar(max) NULL,
	creation_date nvarchar(max) NULL,
	creation_time nvarchar(max) NULL,
	change_date nvarchar(max) NULL,
	change_time nvarchar(max) NULL,
	active_from nvarchar(max) NULL,
	active_till nvarchar(max) NULL,
	adjust_cond nvarchar(max) NULL,
	application nvarchar(max) NULL,
	appl_type nvarchar(max) NULL,
	autoarch nvarchar(max) NULL,
	change_userid nvarchar(max) NULL,
	confirm nvarchar(max) NULL,
	created_by nvarchar(max) NULL,
	creation_user nvarchar(max) NULL,
	critical nvarchar(max) NULL,
	cyclic nvarchar(max) NULL,
	cyclic_tolerance nvarchar(max) NULL,
	cyclic_type nvarchar(max) NULL,
	days nvarchar(max) NULL,
	dayscal nvarchar(max) NULL,
	days_and_or nvarchar(max) NULL,
	doclib nvarchar(max) NULL,
	docmem nvarchar(max) NULL,
	ind_cyclic nvarchar(max) NULL,
	jobisn nvarchar(max) NULL,
	maxdays nvarchar(max) NULL,
	maxrerun nvarchar(max) NULL,
	maxruns nvarchar(max) NULL,
	maxwait nvarchar(max) NULL,
	memname nvarchar(max) NULL,
	multy_agent nvarchar(max) NULL,
	parent_folder nvarchar(max) NULL,
	priority nvarchar(max) NULL,
	retro nvarchar(max) NULL,
	rule_based_calendar_relationship nvarchar(max) NULL,
	shift nvarchar(max) NULL,
	shiftnum nvarchar(max) NULL,
	sub_application nvarchar(max) NULL,
	sysdb nvarchar(max) NULL,
	use_instream_jcl nvarchar(max) NULL
)
```

* CONTROLM_CONDITIONS

```sql
CREATE TABLE dbo.CONTROLM_CONDITIONS(
	jobname nvarchar(max) NULL,
	type nvarchar(max) NULL,
	condition nvarchar(max) NULL,
	and_or nvarchar(max) NULL
)
```

* CONTROLM_SHOUTS

```sql
CREATE TABLE dbo.CONTROLM_SHOUTS(
	jobname nvarchar(max) NULL,
	type nvarchar(max) NULL,
	destination nvarchar(max) NULL,
	message nvarchar(max) NULL,
	time nvarchar(max) NULL,
	urgency nvarchar(max) NULL,
	when nvarchar(max) NULL
)
```

* CONTROLM_CMDLINE

```sql
CREATE TABLE dbo.CONTROLM_CMDLINE(
	cmd nvarchar(100) NOT NULL,
	package_name nvarchar(150) NULL
)
```