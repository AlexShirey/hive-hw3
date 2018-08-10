USE ${db};

DROP FUNCTION IF EXISTS maxUsage;

CREATE FUNCTION maxUsage AS 'bigdata.course.hive.hw3.MaxUsage'
	USING JAR "${udf_lib}";