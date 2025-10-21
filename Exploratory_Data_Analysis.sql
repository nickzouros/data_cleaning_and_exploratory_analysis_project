-- Exploratory Data Analysis

select * 
from layoffs_staging2;


select max(total_laid_off), max(percentage_laid_off)             -- maximum laid off and the maximum percentage off it
from layoffs_staging2;



select *                                 -- brokecompanies with the most funds
from layoffs_staging2
where percentage_laid_off = 1
order by funds_raised_millions desc;

select company, sum(total_laid_off)     -- most laid offs regarding companies
from layoffs_staging2
group by company
order by 2 desc;

select min(`date`), max(`date`)         -- date range
from layoffs_staging2;


select industry, sum(total_laid_off)     -- most laid offs regarding industries
from layoffs_staging2
group by industry
order by 2 desc;


select * 
from layoffs_staging2;

select country, sum(total_laid_off)     -- most laid offs regarding countries
from layoffs_staging2
group by country
order by 2 desc;


select year(`date`), sum(total_laid_off)     -- most laid offs regarding the year
from layoffs_staging2
group by year(`date`)
order by 2 desc;


select stage, sum(total_laid_off)     -- most laid offs regarding the stage of the company
from layoffs_staging2
group by stage
order by 2 desc;


select company, sum(percentage_laid_off)     -- most  percentage laid offs regarding companies
from layoffs_staging2
group by company
order by 2 desc;

select substring(`date`,1,7) as `month`, sum(total_laid_off)
from layoffs_staging2
where substring(`date`,1,7) is not null
group by substring(`date`,1,7)
order by 1 asc;


with Rolling_total as               -- creating cte to be able to use it to make the rolling total
(
select substring(`date`,1,7) as `month`, sum(total_laid_off) as total_off       
from layoffs_staging2
where substring(`date`,1,7) is not null               -- cleaning the null values
group by substring(`date`,1,7)
order by 1 asc                                        -- asceading order on the first column 
)
select `month`, total_off,
sum(total_off) over(order by `month`) as rolling_total        -- creation of the rolling total
from Rolling_total;


select company, sum(total_laid_off)     
from layoffs_staging2
group by company
order by 2 desc;


select company, year(`date`), sum(total_laid_off)     
from layoffs_staging2
group by company, year(`date`)
order by 3 desc;




with Company_Year (company, years, total_laid_off)as   -- cte creation of companies their year and their sum of theri total laid offs
(
select company, year(`date`), sum(total_laid_off)     
from layoffs_staging2
group by company, year(`date`)
), Company_Year_Rank as                            -- cte creation of their rank
(
select *,
dense_rank() over(partition by years order by total_laid_off desc) as ranking				-- who laid of the most ppl per year
from Company_Year
where years is not null			-- get rid off nulls
)
select *					-- top 5 companies per year regarding dropping of ppl
from Company_Year_Rank
where ranking <=5
;