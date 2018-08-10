USE ${db};

SELECT /*+ MAPJOIN(city) */
	cityid, name, maxusage(useragent)['device'] as device, maxusage(useragent)['os'] as os, maxusage(useragent)['browser'] as browser
	FROM bid
	LEFT JOIN city 
	ON (bid.cityId = city.id)
	group by cityid, name;