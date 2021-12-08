-- 22 seconds
SELECT region, COUNT(titleId) AS nbr_titles 
FROM title_akas 
GROUP BY region 
ORDER BY nbr_titles DESC;

-- 34 seconds
SELECT region, COUNT(titleId) AS nbr_titles
FROM title_akas_ext
GROUP BY region 
ORDER BY nbr_titles DESC;

-- 17 seconds
SELECT startYear, COUNT(tconst) AS nbr_title 
FROM title_basics 
GROUP BY startYear 
ORDER BY startYear;

-- 21 seconds
SELECT startYear, COUNT(tconst) AS nbr_title 
FROM title_basics_ext 
GROUP BY startYear 
ORDER BY startYear;

-- 25 seconds
SELECT category, COUNT(tconst) AS nbr_titles 
FROM title_principals 
GROUP BY category 
ORDER BY nbr_titles DESC;

-- 42 seconds
SELECT category, COUNT(tconst) AS nbr_titles 
FROM title_principals_ext 
GROUP BY category 
ORDER BY nbr_titles DESC;

