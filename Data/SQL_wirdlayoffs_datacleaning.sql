-- Data Cleaning 


SELECT *
FROM layoffs;

-- 1 étapes supprimer les doublons
-- 2 Standaisées les données 
-- 3 Valeur Null ou vide 
-- 4 Supprimer les conlonnes non necessaire 

CREATE TABLE layoffs_staging
LIKE layoffs;

SELECT *
FROM layoffs_staging;

INSERT INTO layoffs_staging
SELECT *
FROM layoffs; 

SELECT *, 
row_number() OVER ( PARTITION BY company, industry, total_laid_off,
percentage_laid_off, `date`) as row_num
FROM layoffs_staging;

WITH duplicate_cte as (
SELECT *, 
row_number() OVER ( PARTITION BY company,location,
industry, total_laid_off,
percentage_laid_off, `date`, stage
, country, funds_raised_millions) as row_num
FROM layoffs_staging
)
SELECT *
FROM duplicate_cte 
WHERE row_num >1;

CREATE TABLE `layoffs_staging2` (
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
FROM layoffs_staging2;

INSERT INTO layoffs_staging2
SELECT *, 
row_number() OVER ( PARTITION BY company,location,
industry, total_laid_off,
percentage_laid_off, `date`, stage
, country, funds_raised_millions) as row_num
FROM layoffs_staging;

SELECT *
FROM layoffs_staging2
WHERE row_num > 1;

DELETE
FROM layoffs_staging2
WHERE row_num > 1;

SELECT *
FROM layoffs_staging2
order by company;


-- Standardizing data 

SELECT company, trim((company))
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET company =trim((company));

SELECT *
FROM layoffs_staging2
WHERE industry like 'Crypto%';

UPDATE layoffs_staging2
SET industry ='Crypto'
WHERE industry like 'Crypto%';

SELECT distinct industry
FROM layoffs_staging2
ORDER BY 1;

SELECT  distinct country, trim( trailing '.' from country)
FROM layoffs_staging2
order by 1;

UPDATE layoffs_staging2
SET country = trim( trailing '.' from country)
WHERE country like 'United States%' ;

SELECT *
FROM layoffs_staging2
WHERE country like 'United States'
ORDER BY 1;

-- formating the date as a date column

SELECT `date`,
str_to_date(`date`, '%m/%d/%Y')
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET `date`=str_to_date(`date`, '%m/%d/%Y');

SELECT `date`
FROM layoffs_staging2;

ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

-- check les colonnes null de la table layoffs_staging 2

SELECT *
from layoffs_staging2
WHERE total_laid_off is null
and percentage_laid_off is null;

SELECT * 
FROM layoffs_staging2
WHERE industry is null
or industry = ''; 

SELECT * 
FROM layoffs_staging2
WHERE company = 'Airbnb'; 

 -- chercher les colonnes vite de la colonne industry 
SELECT t1.industry, t2.industry
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
ON t1.company = t2.company
   AND t1.location = t2.location
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL
;
 
 -- mise à jour de la colonne industry avec les valeurs null
 
 UPDATE layoffs_staging2 t1
 JOIN layoffs_staging2 t2
ON t1.company = t2.company
SET t1.industry = t2.industry 
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL ;

-- changer les valeur vide en null
UPDATE layoffs_staging2
SET industry = null
where industry ='';

-- regarder les changements de la colonne industry

 SELECT * 
FROM layoffs_staging2;

-- suppression des données untiles

DELETE
from layoffs_staging2
WHERE total_laid_off is null
and percentage_laid_off is null;

-- SUPPRIMER LA Colonne ajouter au début 

ALTER TABLE layoffs_staging2
DROP column row_num;