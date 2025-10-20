-- Data cleaning project 

-- data cleaningis where you get a more usable format so you can fix a lot of issues in the raw data

select *
from layoffs

-- 1. remove dublicates
-- 2. standardize the data
-- 3. null values or blank values
-- 4. remove any columns



CREATE TABLE layoffs_staging  -- dhmiourgia table pou tha tropopoihsw ta raw data
LIKE layoffs;


SELECT *
FROM layoffs_staging;

INSERT layoffs_staging  -- eisagwgh twn dedomenwn ston neo pinaka
SELECT *
FROM layoffs;


SELECT *,                -- arithmisi twn grammwn
ROW_NUMBER() OVER(
PARTITION BY company, location,
industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging;


WITH dublicate_cte AS           -- dhmiourgia cte gia thn diadikasia arithmisis
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location,
industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging
)
SELECT *                -- ebresi epanalambanomenwn dedomenwn
FROM dublicate_cte 
WHERE row_num > 1;


SELECT *
FROM layoffs_staging
WHERE company = 'Casper';


CREATE TABLE `layoffs_staging2` (                    -- dhmiourgia neou table apo tables gia thn diagrafi twn diplwn dedomenwn 
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` double DEFAULT NULL,
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
ROW_NUMBER() OVER(
PARTITION BY company, location,
industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging;


SELECT *
FROM layoffs_staging2
WHERE row_num >1;


DELETE                      				-- diagrafi twn diplwn dedomenwn
FROM layoffs_staging2
WHERE row_num >1;


SELECT *
FROM layoffs_staging2;


-- Standardizing Data

SELECT company, trim(company)
FROM layoffs_staging2;


update layoffs_staging2         -- kobw ta kena
set company = trim(company);

SELECT distinct industry
FROM layoffs_staging2
where industry like 'Crypto%';

update layoffs_staging2                        -- ftiaxnw ta crypto na einai ena industry
set industry ='Crypto'
where industry like 'Crypto%';

SELECT distinct country, trim(trailing '.' from country)        -- epilegw na dw monadikes xwres kai xwres me trimarisma sto telos to '.'
FROM layoffs_staging2
order by 1;

update layoffs_staging2                                 -- kanw to parapanw gia united states
set country = trim(trailing '.' from country)
where country like 'United States%';

select `date`,                   -- format to date
str_to_date(`date`, '%m/%d/%Y')
from layoffs_staging2;

update layoffs_staging2				-- to kanw update
set `date` = str_to_date(`date`, '%m/%d/%Y');

select `date`
from layoffs_staging2;

Alter table layoffs_staging2		-- allazw tn pinaka kai sugkekrimena ton tupo toy date apo text se actual date
modify column `date` date;

select *
from layoffs_staging2;

select *						-- axrista dedomena
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

select *
from layoffs_staging2
where industry is null
or industry = '';

select *
from layoffs_staging2
where company = 'Airbnb';

select t1.industry, t2.industry
from layoffs_staging2 t1
join layoffs_staging2 t2
	on t1.company = t2.company
where (t1.industry is null or t1.industry = '')
and t2.industry is not null;

update layoffs_staging2 t1				-- enhmerwsh twn kenwn me to industry to diko toys
join layoffs_staging2 t2
	on t1.company = t2.company
set t1.industry = t2.industry
where t1.industry is null 
and t2.industry is not null;


update layoffs_staging2 			-- update opou exei keno se null logw error tou parapanw update querie bash tou kwdika sto select 
set industry = null
where industry ='';

select *
from layoffs_staging2
where company like 'Bally%';

select *
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

delete 						-- diagrafi twn axrhstwn dedomenwn
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

select * from layoffs_staging2;      -- parathrw oti to row_num den xreiazete na uparxei pleon 

alter table layoffs_staging2		-- kanw drop thn stili row_num
drop column row_num;