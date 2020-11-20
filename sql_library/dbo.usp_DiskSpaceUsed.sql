CREATE PROCEDURE [dbo].[usp_DiskSpaceUsed]
	@table_schema_and_name sysname = ''
AS
BEGIN
	DROP TABLE IF EXISTS #SpaceUsed

	CREATE TABLE #SpaceUsed (
		[TableName] nvarchar(128),
		[NumRows] char(20),
		[ReservedSpace] varchar(18),
		[DataSpace] varchar(18),
		[IndexSize] varchar(18),
		[UnusedSpace] varchar(18),
	)

	DECLARE @str VARCHAR(500)
	SET @str =  'exec sp_spaceused ''?'''
	INSERT INTO #SpaceUsed
	EXEC sp_msforeachtable @command1=@str

	SELECT
		  [TableName]
		, [NumRows]
		, [TotalSpaceUsed_MB] = CONVERT(numeric(18,2),
								(CONVERT(numeric(18,0),REPLACE([DataSpace],' KB','')) / 1024) +
								(CONVERT(numeric(18,0),REPLACE([IndexSize],' KB','')) / 1024)
							 )
		, ReservedSpace_MB = CONVERT(numeric(18,0),REPLACE([ReservedSpace],' KB','')) / 1024
		, DataSpace_MB = CONVERT(numeric(18,0),REPLACE([DataSpace],' KB','')) / 1024
		, IndexSpace_MB = CONVERT(numeric(18,0),REPLACE([IndexSize],' KB','')) / 1024
		, UnusedSpace_MB = CONVERT(numeric(18,0),REPLACE([UnusedSpace],' KB','')) / 1024
	FROM #SpaceUsed
	WHERE TableName like '%' + @table_schema_and_name + '%'
	ORDER BY tablename
END
GO