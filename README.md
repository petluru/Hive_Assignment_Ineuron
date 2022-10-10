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

      ScreenShot:
      ![image](https://user-images.githubusercontent.com/36133568/194927559-bcd715c8-c6e8-4a98-b9e5-181de328f778.png)
      ![image](https://user-images.githubusercontent.com/36133568/194927585-03d819f2-cea7-4b11-ba3b-416a4a504f76.png)
      
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
       
      ![image](https://user-images.githubusercontent.com/36133568/194927848-6090466a-6dc7-49ad-b854-364134c47c61.png)

  c.	Calculate the total sales for each quarter
      Query: select qtr_id,sum(sales) from sales_order_orc
              group by qtr_id;
      Output: Quarter 1: 2350817.73
                Quarter 2: 2048120.29
                Quarter 3: 1758910.80
                Quarter 4: 3874780.01

     ![image](https://user-images.githubusercontent.com/36133568/194927947-2e22ad96-65a3-4bc0-b4cd-01e62dc65de8.png)
  d.	In which quarter sales was minimum
      Query: 
       select qtr_id,sum(sales) `TotalSales` from sales_order_orc
       group by qtr_id
       order by `TotalSales` asc limit 1;
       
       Output:     3
       
       ![image](https://user-images.githubusercontent.com/36133568/194928067-56f4ba1e-90af-4fb3-abe1-0739b1a82eb9.png)
       
       
  e.	In which country sales was maximum and in which country sales was minimum.
  
      Query(Max): 
        select country,SUM(sales) `TotalSales`
        from sales_order_orc
        group by country
        order by `TotalSales` desc limit 1;
        
        Output: USA
        
        ![image](https://user-images.githubusercontent.com/36133568/194928231-f992e208-e79f-436a-9719-8ae7d4f69e37.png)
      
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
        
      ![image](https://user-images.githubusercontent.com/36133568/194928454-15f7e12f-e642-4054-8671-e2aa418fcd47.png)


   g.	Find a month for each year in which maximum number of quantities were sold
    

        


       
       




      
