insert [admin].[copyDataConfig] ([configId],[model],[sourceSystemName],[sourceSystemType],[sourceLocationName],[sourceObjectName],[sourceSelectColumns],[sourceKeyColumns],[destinationSystemName],[destinationSystemType],[destinationObjectPattern],[destinationDirectoryPattern],[destinationObjectType],[extractType],[deltaStartDate],[deltaEndDate],[deltaDateColumn],[deltaFilterCondition],[flagBlock],[blockSize],[blockColumn],[flagActive],[createDate],[lastModifiedDate])
select 4,'framework','sql-weu-dev-adventureworks2022-01','sql','adventureworks2022','Person.Person',NULL,NULL,'lh_modernBI_fabricFramework_bronze_01','lakehouse','Person','adventureworks2022/Person/Person/','parquet','delta','2025-02-03 16:48:09.42',NULL,'ModifiedDate',NULL,0,NULL,NULL,1,'2024-09-13 20:38:18.87',NULL UNION ALL
select 2,'framework','sql-weu-dev-adventureworks2022-01','sql','adventureworks2022','Sales.SalesPerson',NULL,NULL,'lh_modernBI_fabricFramework_bronze_01','lakehouse','SalesPerson','adventureworks2022/Sales/SalesPerson/','parquet','full','2025-02-03 16:48:09.39',NULL,NULL,NULL,0,NULL,NULL,1,'2024-09-13 20:38:18.87',NULL UNION ALL
select 1,'framework','sql-weu-dev-adventureworks2022-01','sql','adventureworks2022','Sales.SalesOrderDetail',NULL,NULL,'lh_modernBI_fabricFramework_bronze_01','lakehouse','SalesOrderDetail','adventureworks2022/Sales/SalesOrderDetail/','parquet','full','2025-02-03 16:48:09.51',NULL,NULL,NULL,0,NULL,NULL,1,'2024-09-13 20:38:18.87',NULL UNION ALL
select 3,'framework','sql-weu-dev-adventureworks2022-01','sql','adventureworks2022','Sales.Store',NULL,NULL,'lh_modernBI_fabricFramework_bronze_01','lakehouse','Store','adventureworks2022/Sales/Store/','parquet','full','2025-02-03 16:48:09.36',NULL,NULL,NULL,0,NULL,NULL,1,'2024-09-13 20:38:18.87',NULL UNION ALL
select 5,'framework','sql-weu-dev-adventureworks2022-01','sql','adventureworks2022','Sales.SalesTerritory',NULL,NULL,'lh_modernBI_fabricFramework_bronze_01','lakehouse','SalesTerritory','adventureworks2022/Sales/SalesTerritory/','parquet','full','2025-02-03 16:48:09.51',NULL,NULL,NULL,0,NULL,NULL,1,'2024-09-13 20:38:18.87',NULL;


insert [admin].[inferredMembersConfig] ([columnType],[columnValue],[skValue])
select 'DateType','30/12/2999','-2' UNION ALL
select 'DateType','31/12/2999','-1' UNION ALL
select 'DoubleType','-1','-1' UNION ALL
select 'DoubleType','-2','-2' UNION ALL
select 'TimestampType','00:00','-1' UNION ALL
select 'TimestampType','00:00','-2' UNION ALL
select 'BooleanType','FALSE','-1' UNION ALL
select 'BooleanType','FALSE','-2' UNION ALL
select 'IntegerType','-1','-1' UNION ALL
select 'IntegerType','-2','-2' UNION ALL
select 'LongType','-1','-1' UNION ALL
select 'LongType','-2','-2' UNION ALL
select 'StringType','N/A','-2' UNION ALL
select 'StringType','Unk','-1';


insert [admin].[processDatesConfig] ([model],[tableName],[scope],[fullProcess],[dateType],[filterColumn],[dateColumnFormat],[dateUnit],[date])
select 'framework','framework.dim_person','DimType2',0,'Day',NULL,NULL,NULL,'2025-02-03 00:00:00' UNION ALL
select 'framework','framework.fact_sales_delta','fact',0,'Day','ModifiedDate','MM/dd/yyyy',1,'2024-12-30 00:00:00' UNION ALL
select 'framework','framework.fact_delta','fact',0,'Day','ModifiedDate','MM/dd/yyyy',1,'1900-01-01 00:00:00';


insert [admin].[silverGoldConfig] ([model],[sourceSystemName],[sourceLocationName],[sourceDirectoryPattern],[objectName],[keyColumns],[partitionColumns],[extractType],[loadType],[destinationObjectPattern],[destinationDatabase],[notebookName],[layer],[flagActive])
select 'framework',NULL,NULL,NULL,'',NULL,NULL,NULL,NULL,'framework/fact_sales_full','lh_modernBI_fabricFramework_gold_01','NB_FACT_Sales_Full','Gold',1 UNION ALL
select 'framework',NULL,NULL,NULL,'',NULL,NULL,NULL,NULL,'framework/dim_store','lh_modernBI_fabricFramework_gold_01','NB_DIM_Store','Gold',1 UNION ALL
select 'framework',NULL,NULL,NULL,'',NULL,NULL,NULL,NULL,'framework/dim_person','lh_modernBI_fabricFramework_gold_01','NB_DIM_Person','Gold',1 UNION ALL
select 'framework','lh_modernBI_fabricFramework_bronze_01','adventureworks2022','adventureworks2022/Person/Person/','Person.Person','BusinessEntityID',NULL,'delta','merge','Person/Person_Silver','lh_modernBI_fabricFramework_silver_01','NB_Load_Silver','Silver',1 UNION ALL
select 'framework','lh_modernBI_fabricFramework_bronze_01','adventureworks2022','adventureworks2022/Sales/SalesOrderDetail/','Sales.SalesOrderDetail',NULL,NULL,'full','overwrite','Sales/SalesOrderDetail_Silver','lh_modernBI_fabricFramework_silver_01','NB_Load_Silver','Silver',1 UNION ALL
select 'framework','lh_modernBI_fabricFramework_bronze_01','adventureworks2022','adventureworks2022/Sales/Store/','Sales.Store',NULL,NULL,'full','overwrite','Sales/Store_Silver','lh_modernBI_fabricFramework_silver_01','NB_Load_Silver','Silver',1;


insert [admin].[silverGoldDependency] ([model],[layer],[objectName],[dependencyObjectName],[SystemDateUpdate])
select 'framework','Gold','framework/fact_sales_full','framework/dim_store','2024-12-31 08:38:30' UNION ALL
select 'framework','Gold','framework/fact_sales_full','framework/dim_person','2024-12-30 18:18:12';