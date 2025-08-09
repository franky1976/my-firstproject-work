CREATE  DATABASE sales_recorddb;

-- USE DATABASE
USE sales_recorddb;

-- STEP 1: DUPLICATE REMOVAL
CREATE TABLE sales_record_STAGING 
LIKE salesrecords;
INSERT sales_record_STAGING  SELECT * FROM salesrecords;
-- Create sales_record_staging2 table with ID
CREATE TABLE sales_record_staging2 (
  `Region` TEXT,
  `Country` TEXT,
  `Item Type` TEXT,
  `Sales Channel` TEXT,
  `Order Priority` TEXT,
  `Order Date` DATE,
  `Order ID` INT,
  `Ship Date` DATE,
  `Units Sold` INT,
  `Unit Price` DECIMAL(10,2),
  `Unit Cost` DECIMAL(10,2), 
  `Total Revenue` DECIMAL(15,2),
  `Total Cost` DECIMAL(15,2),
  `Total Profit` DECIMAL(15,2),
  `ID` INT 
);

INSERT INTO sales_record_staging2 (
  `Region`, `Country`, `Item Type`, `Sales Channel`, `Order Priority`,
  `Order Date`, `Order ID`, `Ship Date`, `Units Sold`, `Unit Price`,
  `Unit Cost`, `Total Revenue`, `Total Cost`, `Total Profit`, `ID`
)
SELECT
  TRIM(`Region`)        AS `Region`,
  TRIM(`Country`)       AS `Country`,
  TRIM(`Item Type`)     AS `Item Type`,
  TRIM(`Sales Channel`) AS `Sales Channel`,
  TRIM(`Order Priority`)AS `Order Priority`,
  STR_TO_DATE(`Order Date`, '%m/%d/%Y') AS `Order Date`,
  `Order ID`            AS `Order ID`,
  STR_TO_DATE(`Ship Date`, '%m/%d/%Y')  AS `Ship Date`,
  `Units Sold`          AS `Units Sold`,
  `Unit Price`          AS `Unit Price`,
  `Unit Cost`           AS `Unit Cost`,
  `Total Revenue`       AS `Total Revenue`,
  `Total Cost`          AS `Total Cost`,
  `Total Profit`        AS `Total Profit`,
  ROW_NUMBER() OVER (
    PARTITION BY `Order ID`, `Region`, `Country`, `Item Type`, `Sales Channel`,
                 `Order Priority`, `Units Sold`, `Unit Price`, `Unit Cost`
    ORDER BY STR_TO_DATE(`Order Date`, '%m/%d/%Y')
  ) AS `ID`
FROM sales_record_staging;

select *from  sales_record_staging;

WITH cte AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY `Order ID`, `Region`, `Country`, `Item Type`, `Sales Channel`,
                         `Order Priority`, `Units Sold`, `Unit Price`, `Unit Cost`
            ORDER BY STR_TO_DATE(`Order Date`, '%m/%d/%Y')
        ) AS rn
    FROM sales_record_staging
)
SELECT *
FROM cte
WHERE rn > 1;


WITH cte AS (
    SELECT
        TRIM(`Region`)        AS `Region`,
        TRIM(`Country`)       AS `Country`,
        TRIM(`Item Type`)     AS `Item Type`,
        TRIM(`Sales Channel`) AS `Sales Channel`,
        TRIM(`Order Priority`)AS `Order Priority`,
        STR_TO_DATE(`Order Date`, '%m/%d/%Y') AS `Order Date`,
        `Order ID`            AS `Order ID`,
        STR_TO_DATE(`Ship Date`, '%m/%d/%Y')  AS `Ship Date`,
        `Units Sold`          AS `Units Sold`,
        `Unit Price`          AS `Unit Price`,
        `Unit Cost`           AS `Unit Cost`,
        `Total Revenue`       AS `Total Revenue`,
        `Total Cost`          AS `Total Cost`,
        `Total Profit`        AS `Total Profit`,
        ROW_NUMBER() OVER (
            PARTITION BY `Order ID`, `Region`, `Country`, `Item Type`, `Sales Channel`,
                         `Order Priority`, `Units Sold`, `Unit Price`, `Unit Cost`
            ORDER BY STR_TO_DATE(`Order Date`, '%m/%d/%Y')
        ) AS rn
    FROM sales_record_staging
)
SELECT *
FROM cte
WHERE rn = 1;

ALTER TABLE sales_record_staging
DROP COLUMN rn;
