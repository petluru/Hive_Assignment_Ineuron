# Hive_Assignment_Ineuron
1. Downloaded Vehicle Sales Data and stored it in Hadoop file system
    ![image](https://user-images.githubusercontent.com/36133568/194927084-2e57b1d8-3dda-4f7c-86b4-675c32a740c4.png)
2. Create Table sales_order_csv  using SerDe with attributes present in spreadsheet
   ![image](https://user-images.githubusercontent.com/36133568/194927117-8a4722f4-b879-4a3f-a7bd-dfb98bafda63.png)
3. Describe the structure of sales_order_csv
   ![image](https://user-images.githubusercontent.com/36133568/194927147-861cdeaf-baa6-4c1f-ac3b-53f6d190ee23.png)
4. Load data from Hive Warehouse into table.
   ![image](https://user-images.githubusercontent.com/36133568/194927195-01994178-3b4b-49fb-9c3b-8cecc0c4a47d.png)
5. Query the data present in the table
   ![image](https://user-images.githubusercontent.com/36133568/194927243-26c0feb0-c7d7-40f9-9dc0-5c2c1f7816a2.png)
6. Create sales_order_orc table
   ![image](https://user-images.githubusercontent.com/36133568/194927319-c6943a8f-c5b8-42d5-b623-6788ab6459cd.png)

7. Describe the format of the orc table.
    ![image](https://user-images.githubusercontent.com/36133568/194927393-c0adedf9-0d82-41a8-a23c-650b1a19870e.png)
8. Copy data from sales_order_csv to sales_order_orc.
   ![image](https://user-images.githubusercontent.com/36133568/194927427-35a1ae32-59a1-4753-820a-fb92b6f03a6a.png)

## Solutions
   a.	Calculate total sales per year
      Query:   select year_id,sum(sales) `Total Sales` from sales_order_orc  group by year_id 
                order by `Total Sales` desc;

      
      Output:
        Year     Total Sales
        2004    4724162.599999997
        2003    3516979.540000001
        2005    1791486.71
  b.	Find a product for which maximum orders were placed
      Query: select productcode,count(ordernumber) as `TotalOrders` 
             from sales_order_orc 
             group by productcode
             order by `TotalOrders` desc limit 1;
      Output:
       S18_3232        52
       
     

  c.	Calculate the total sales for each quarter
      Query: select qtr_id,sum(sales) from sales_order_orc
              group by qtr_id;
      Output: Quarter 1: 2350817.73
                Quarter 2: 2048120.29
                Quarter 3: 1758910.80
                Quarter 4: 3874780.01

     
  d.	In which quarter sales was minimum
      Query: 
       select qtr_id,sum(sales) `TotalSales` from sales_order_orc
       group by qtr_id
       order by `TotalSales` asc limit 1;
       
       Output:     3
       
       
       
       
  e.	In which country sales was maximum and in which country sales was minimum.
  
      Query(Max): 
        select country,SUM(sales) `TotalSales`
        from sales_order_orc
        group by country
        order by `TotalSales` desc limit 1;
        
        Output: USA
        
        
      
     Query(Min):
       select country,SUM(sales) `TotalSales`
        from sales_order_orc
        group by country
        order by `TotalSales` asc limit 1;
        
       Output:Ireland
       
   f.	Calculate quartelry sales for each city
   
       Query:
        select city,
        round(sum(case when qtr_id = '1' then sales else 0 end)) as `Q1 Sales`,
        round(sum(case when qtr_id = '2' then sales else 0 end)) as `Q2 Sales`,
        round(sum(case when qtr_id = '3' then sales else 0 end)) as `Q3 Sales`,
        round(sum(case when qtr_id = '4' then sales else 0 end)) as `Q4 Sales`
        from sales_order_data_orc
        group by city;
        
        Output:
            Aaarhus 0.0     0.0     0.0     100596.0
            Allentown       0.0     6167.0  71931.0 44041.0
            Barcelona       0.0     4219.0  0.0     74193.0
            Bergamo 56181.0 0.0     0.0     81774.0
            Bergen  0.0     0.0     16363.0 95277.0
            Boras   31607.0 0.0     53942.0 48711.0
            Boston  0.0     74994.0 15345.0 63731.0
            Brickhaven      31475.0 7277.0  114975.0        11529.0
            Bridgewater     0.0     75779.0 0.0     26116.0
            Brisbane        16118.0 0.0     34100.0 0.0
            Bruxelles       18800.0 8412.0  47760.0 0.0
            Burbank 37850.0 0.0     0.0     8235.0
            Burlingame      13530.0 0.0     42032.0 65222.0
            Cambridge       21783.0 14381.0 48829.0 54252.0
            Charleroi       16628.0 1711.0  1637.0  13463.0
            Chatswood       0.0     43971.0 69694.0 37905.0
            Cowes   26907.0 0.0     0.0     51334.0
            Dublin  38784.0 0.0     18972.0 0.0
            Espoo   51373.0 31018.0 31569.0 0.0
            Frankfurt       48699.0 0.0     0.0     36473.0
            Gensve  50433.0 0.0     67281.0 0.0
            Glen Waverly    0.0     14378.0 12335.0 37879.0
            Glendale        3987.0  20351.0 7600.0  34485.0
            Graz    8775.0  0.0     0.0     43489.0
            Helsinki        26423.0 0.0     42744.0 42083.0
            Kobenhavn       58871.0 62092.0 0.0     24079.0
            Koln    0.0     0.0     0.0     100307.0
            Las Vegas       0.0     33848.0 34454.0 14450.0
            Lille   20178.0 0.0     0.0     48874.0
            Liverpool       0.0     91211.0 0.0     26797.0
            London  8477.0  32376.0 0.0     83970.0
            Los Angeles     23889.0 0.0     0.0     24159.0
            Lule    9749.0  0.0     0.0     66006.0
            Lyon    101339.0        0.0     0.0     41535.0
            Madrid  357668.0        339588.0        69714.0 315581.0
            Makati City     55245.0 0.0     0.0     38771.0
            Manchester      51018.0 0.0     0.0     106790.0
            Marseille       2317.0  52482.0 0.0     20137.0
            Melbourne       49638.0 60136.0 0.0     91222.0
            Minato-ku       38191.0 26483.0 0.0     55889.0
            Montreal        0.0     58258.0 0.0     15947.0
            Munich  0.0     0.0     34994.0 0.0
            NYC     32648.0 165100.0        63028.0 300012.0
            Nantes  59617.0 60345.0 61311.0 23032.0
            Nashua  12133.0 0.0     0.0     119552.0
            New Bedford     48579.0 0.0     45738.0 113558.0
            New Haven       0.0     36973.0 0.0     42499.0
            Newark  8722.0  74506.0 0.0     0.0
            North Sydney    65012.0 0.0     47192.0 41792.0
            Osaka   50491.0 17114.0 0.0     0.0
            Oslo    0.0     0.0     34145.0 45079.0
            Oulu    49055.0 17813.0 37502.0 0.0
            Paris   71494.0 80215.0 27798.0 89437.0
            Pasadena        44273.0 0.0     55776.0 4512.0
            Philadelphia    27399.0 7287.0  0.0     116503.0
            Reggio Emilia   0.0     41510.0 56422.0 44670.0
            Reims   52029.0 18972.0 15146.0 48896.0
            Salzburg        0.0     98104.0 6693.0  45001.0
            San Diego       87489.0 0.0     0.0     0.0
            San Francisco   72899.0 0.0     0.0     151459.0
            San Jose        0.0     160010.0        0.0     0.0
            San Rafael      267315.0        7262.0  216297.0        163984.0
            Sevilla 0.0     0.0     0.0     54724.0
            Singapore       28395.0 92034.0 90250.0 77809.0
            South Brisbane  21730.0 0.0     10640.0 27099.0
            Stavern 54702.0 0.0     0.0     61897.0
            Strasbourg      0.0     80438.0 0.0     0.0
            Torino  0.0     0.0     94117.0 0.0
            Toulouse        15139.0 0.0     17251.0 38098.0
            Tsawassen       0.0     31303.0 43332.0 0.0
            Vancouver       0.0     0.0     0.0     75239.0
            Versailles      5759.0  0.0     0.0     59075.0
            White Plains    0.0     0.0     0.0     85556.0



   g.	Find a month for each year in which maximum number of quantities were sold
    
        Query: with CTE_1 as (
               select year_id,month_id,sum(QUANTITYORDERED) as Q from sales_order_data_csv
                group by year_id,month_id order by Q desc
               )
               select year_id,max(Q) from CTE_1
               group by year_id;
        

       Result:
       2003    11      10179
       2004    11      10678
       2005    5       4357
       




      
