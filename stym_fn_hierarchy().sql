-- Data is fetched from schema.stym_Data consisting of 2 columns parent & child 
-- Create 2 identical table schema.stym_T1 & schema.stym_T2 consisting 7 columns from level1,level2.....level7
-- Create & Run the below mentioned function
-- Output is stored in schema.stym_T2 

CREATE OR REPLACE FUNCTION public.stym_fn_hierarchy(
	)
    RETURNS void
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
BEGIN 
--LEVEL1 & LEVEL2
TRUNCATE TABLE public."stym_T1";
INSERT INTO public."stym_T1"(level1,level2)    
        SELECT t1.parent::INT,
               t2.child::INT                             
            FROM
                   (
                    SELECT DISTINCT parent
                    FROM public."stym_Data" as t0                        
                    WHERE parent NOT IN 
                                (
                                SELECT child 
                                FROM public."stym_Data"
                                WHERE child NOT IN (parent)
                                ORDER BY child
                                )
                    ORDER BY parent       
                    ) as t1       
            LEFT JOIN public."stym_Data" as t2
                     on t1.parent  = t2.parent 
            WHERE (t1.parent  <> t2.child )
            ORDER BY t2.child,t1.parent ;

TRUNCATE TABLE public."stym_T2";
INSERT INTO public."stym_T2"(level1,level2)
SELECT 
      level1,level2
            FROM (
                SELECT                         
                    level1,level2,
                    ROW_NUMBER() OVER (
                        PARTITION BY level2 
                        ORDER BY level1 ) AS rn
                FROM 
                    public."stym_T1"
                  ) as s
            WHERE rn = 1;

--LEVEL3
TRUNCATE public."stym_T1";
INSERT INTO public."stym_T1"(level3,level2,level1)
            SELECT  
            DISTINCT CASE 
                WHEN m1.child ::INT in (select level2 ::INT from public."stym_T2" ) then null
                WHEN m1.child ::INT in (select level1 ::INT from public."stym_T2" ) then null
                ELSE m1.child ::INT
            END AS level3,
            level2::INT,
            level1::INT 
            FROM public."stym_T2" AS b1
            LEFT JOIN public."stym_Data" AS m1
                    ON b1.level2::INT  = m1.parent ::INT           
            ORDER BY level3,level2,level1 ASC;
            
TRUNCATE TABLE public."stym_T2";
INSERT INTO public."stym_T2"(level1,level2,level3)
    SELECT 
        level1,level2,level3 
    FROM (
        SELECT                         
            level1,level2,level3,
        case when level3 is null then null
        else ROW_NUMBER() OVER (
                PARTITION BY level3 
                ORDER BY level2 ) 
        end AS rn
        FROM 
            public."stym_T1"
          ) as s
    WHERE rn is null or rn = 1;     

--LEVEL4
TRUNCATE TABLE public."stym_T1";
INSERT INTO public."stym_T1"(level4,
                           level3,
                           level2, 
                           level1)
            SELECT  
                    DISTINCT CASE 
                        WHEN m1.child::INT  in (select level1::INT from public."stym_T2") then null
                        WHEN m1.child::INT  in (select level2::INT from public."stym_T2") then null
                        WHEN m1.child::INT  in (select level3::INT from public."stym_T2") then null
                        ELSE m1.child ::INT 
                    END AS level4,                   
                    level3::INT,                   
                    level2::INT,
                    level1::INT 
            FROM public."stym_T2" AS b1
            LEFT JOIN public."stym_Data" AS m1
                    ON b1.level3::INT  = m1.parent ::INT           
            ORDER BY level4,level3,level2,level1  ASC;

TRUNCATE TABLE public."stym_T2";
INSERT INTO public."stym_T2"(level1,
                           level2,
                           level3,
                           level4
                           )
    SELECT 
        level1,level2,level3,level4 
    FROM (
        SELECT                         
            level1 ,level2 ,level3 ,level4 ,
        case when level4 is null then null
        else ROW_NUMBER() OVER (
                PARTITION BY level4 
                ORDER BY level3 ) 
        end AS rn
        FROM 
            public."stym_T1"
          ) as s
    WHERE rn is null or rn = 1;  

--LEVEL5
TRUNCATE TABLE public."stym_T1";
INSERT INTO public."stym_T1"(level5,
                            level4,
                            level3,
                            level2,
                            level1)
            SELECT  
                    DISTINCT CASE 
                        WHEN m1.child::INT  in (select level1::INT from public."stym_T2") then null
                        WHEN m1.child::INT  in (select level2::INT from public."stym_T2") then null
                        WHEN m1.child::INT  in (select level3::INT from public."stym_T2") then null
                        WHEN m1.child::INT  in (select level4::INT from public."stym_T2") then null
                        ELSE m1.child::INT 
                    END AS level5,                   
                    level4::INT,                    
                    level3::INT,
                    level2::INT,
                    level1::INT
            FROM public."stym_T2" AS b1
            LEFT JOIN public."stym_Data" AS m1
                    ON b1.level4::INT  = m1.parent::INT             
            ORDER BY level5 ,level4 ,level3 ,level2 ,level1  ASC;

TRUNCATE TABLE public."stym_T2";
INSERT INTO public."stym_T2"(level1,
                            level2,
                            level3,
                            level4,
                            level5
                           )
    SELECT 
        level1 ,level2 ,level3 , level4 ,level5 
    FROM (
        SELECT                         
            level1 ,level2 ,level3 ,level4 ,level5 ,
        case when level5 is null then null
        else ROW_NUMBER() OVER (
                PARTITION BY level5 
                ORDER BY level4 ) 
        end AS rn
        FROM 
            public."stym_T1"
          ) as s
    WHERE rn is null or rn = 1; 

--LEVEL6
TRUNCATE TABLE public."stym_T1";
INSERT INTO public."stym_T1"(level6,
                            level5,
                            level4,
                            level3,
                            level2,
                            level1)
            SELECT  
                    DISTINCT CASE 
                        WHEN m1.child::INT  in (select level1::INT from public."stym_T2") then null
                        WHEN m1.child::INT  in (select level2::INT from public."stym_T2") then null
                        WHEN m1.child::INT  in (select level3::INT from public."stym_T2") then null
                        WHEN m1.child::INT  in (select level4::INT from public."stym_T2") then null
                        WHEN m1.child::INT  in (select level5::INT from public."stym_T2") then null
                        ELSE m1.child::INT 
                    END AS level6,                    
                    level5::INT ,                    
                    level4::INT ,
                    level3::INT ,
                    level2::INT ,
                    level1::INT 
            FROM public."stym_T2" AS b1
            LEFT JOIN public."stym_Data" AS m1
                    ON b1.level5 ::INT = m1.parent::INT            
            ORDER BY level6 ,level5 ,level4 ,level3 ,level2 ,level1  ASC;

TRUNCATE TABLE public."stym_T2";
INSERT INTO public."stym_T2"(level1,
                            level2,
                            level3,
                            level4,
                            level5,
                            level6
                           )
    SELECT 
        level1 ,level2 ,level3 ,level4 ,level5 ,level6 
    FROM (
        SELECT                         
            level1 ,level2 ,level3 ,level4 ,level5 ,level6 ,
        case when level6 is null then null
        else ROW_NUMBER() OVER (
                PARTITION BY level6 
                ORDER BY level5 ) 
        end AS rn
        FROM 
            public."stym_T1"
          ) as s
    WHERE rn is null or rn = 1; 

--LEVEL7
TRUNCATE TABLE public."stym_T1";
INSERT INTO public."stym_T1"(level7,
                            level6,
                            level5,
                            level4,
                            level3,
                            level2,
                            level1)
            SELECT  
                    DISTINCT CASE 
                        WHEN m1.child::INT  in (select level1::INT from public."stym_T2") then null
                        WHEN m1.child::INT  in (select level2::INT from public."stym_T2") then null
                        WHEN m1.child::INT  in (select level3::INT from public."stym_T2") then null
                        WHEN m1.child::INT  in (select level4::INT from public."stym_T2") then null
                        WHEN m1.child::INT  in (select level5::INT from public."stym_T2") then null
                        WHEN m1.child::INT  in (select level6::INT from public."stym_T2") then null
                        ELSE m1.child::INT  
                    END AS level7,                   
                    level6::INT ,                   
                    level5::INT ,
                    level4::INT ,
                    level3::INT ,
                    level2::INT ,
                    level1::INT 
            FROM public."stym_T2" AS b1
            LEFT JOIN public."stym_Data" AS m1
                    ON b1.level6::INT  = m1.parent::INT             
            ORDER BY level7 ,level6 ,level5 ,level4 ,level3 ,level2 ,level1  ASC;

TRUNCATE TABLE public."stym_T2";
INSERT INTO public."stym_T2"(level1,
                            level2,
                            level3,
                            level4,
                            level5,
                            level6,
                            level7
                           )
    SELECT 
        level1 ,level2 ,level3 ,level4 ,level5 ,level6 ,level7 
    FROM (
        SELECT                         
            level1 ,level2 ,level3 ,level4 ,level5 ,level6 ,level7 ,            
            case when level7 is null then null
            else ROW_NUMBER() OVER (
                    PARTITION BY level7 
                    ORDER BY level6 ) 
            end AS rn
        FROM 
            public."stym_T1"
          ) as s
    WHERE rn is null or rn = 1
    ORDER BY level7 ,level6 ,level5 ,level4 ,level3 ,level2 ,level1 ; 


-- Final Hierarchy till level7 stored in public."stym_T2" table 
END;
  

$BODY$;
