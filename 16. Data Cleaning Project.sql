-- Data cleaning project

SELECT *
FROM layoffs;

-- 1. remove dupes if any
-- 2. standardize the data
-- 3. NULL values or blank values
-- 4. Remove any columns


CREATE TABLE layoffs_staging -- creating this so we dont have to work with raw data
LIKE layoffs;

SELECT *
FROM layoffs_staging;

INSERT layoffs_staging
select *
FROM layoffs;


-- 1. Remove dupes

select *,
ROW_NUMBER() OVER (
Partition BY company, industry, total_laid_off, percentage_laid_off, `date`) AS rwo_num
FROM layoffs_staging;

-- CTE making that block of code smaller and more mangeable 
WITH duplicate_cte AS
(
select *,
ROW_NUMBER() OVER (
Partition BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1;

Select *
FROM layoffs_staging
WHERE company = 'Casper';

CREATE TABLE `layoffs_staging2` ( -- Creating another table to remove rows
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SELECT *
FROM layoffs_staging2
WHERE row_num > 1;

INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER() OVER (
Partition BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging;

DELETE
FROM layoffs_staging2
WHERE row_num > 1;

-- Standardizing data
SELECT company, TRIM(company)
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET company = TRIM(company);

SELECT DISTINCT industry
FROM layoffs_staging2
ORDER BY 1;

SELECT *
FROM layoffs_staging2
WHERE industry LIKE 'Crypto%';

UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

UPDATE layoffs_staging2
SET country = 'United States'
WHERE industry LIKE 'United States%';

SELECT `date`,
STR_TO_DATE(`date`,'%m/%d/%Y')
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`,'%m/%d/%Y');

ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

SELECT *
FROM layoffs_staging2;

-- NULL/BLANK VALUES
SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND PERCENTAGE_LAID_OFF is NULL;

SELECT *
FROM layoffs_staging2
WHERE industry IS NULL
OR industry = '';

SELECT *
FROM layoffs_staging2
WHERE company = 'Airbnb';

SELECT *
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
    AND t1.location = t2.location
WHERE (t1.industry IS NULL or T1.industry = '')    
AND t2.industry IS NOT NULL;

UPDATE layoffs_staging2
SET industry = null
WHERE industry = '';


UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
	On t1.company = t2.company
SET t1.industry = t2.industry
WHERE (t1.industry IS NULL or T1.industry = '')    
AND t2.industry IS NOT NULL;

SELECT *
FROM layoffs_staging2;


SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND PERCENTAGE_LAID_OFF is NULL;


delete
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND PERCENTAGE_LAID_OFF is NULL;

SELECT *
FROM layoffs_staging2;

alter table layoffs_staging2
DROP column row_num;

SELECT *
FROM layoffs_staging2;




