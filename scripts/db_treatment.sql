create SCHEMA totvs;
	
ALTER TABLE totvs.sales ADD COLUMN sale_date DATE;
ALTER TABLE totvs.sales ADD COLUMN dow INT;
ALTER TABLE totvs.sales ADD COLUMN week INT;

UPDATE totvs.sales SET sale_date = TO_DATE(date_str, 'YYYY-MM-DD');
UPDATE totvs.sales SET dow = EXTRACT(DOW FROM sale_date);
UPDATE totvs.sales SET week = EXTRACT(week FROM sale_date);

alter table totvs.sales drop column date_str;

select product, week, sum(units * sales.unit_price) as revenue  from totvs.sales
GROUP BY product, week
order by week;