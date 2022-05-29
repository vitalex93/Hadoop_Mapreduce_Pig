data = load '/user/root/input/clean_data.csv' using PigStorage(',') as (ID:int, Year_Birth:int, Education:chararray, Marital_Status:chararray, Income:float, Kidhome:int,
Teenhome:int, Dt_Customer:chararray, Recency:int, MntWines:int, MntFruits:int, MntMeatProducts:int,
MntFishProducts:int, MntSweetProducts:int, MntGoldProds:int, NumDealsPurchases:int,
NumWebPurchases:int, NumCatalogPurchases:int, NumStorePurchases:int, NumWebVisitsMonth:int, AcceptedCmp3:int,
AcceptedCmp4:int, AcceptedCmp5:int, AcceptedCmp1:int, AcceptedCmp2:int, Complain:int, Response:int);



age = foreach data generate 
ID,2022-Year_Birth as Age, 
Education, Marital_Status, Income,MntWines;

--ok

customers = group age All;

avg_wines = foreach customers generate 
AVG(age.MntWines);

filter_avg = filter age by MntWines > 1.5*avg_wines.$00;

--ok



sorted = order filter_avg by MntWines desc, Income desc; 

final = rank sorted;


--ok

store final into '/user/root/PigResults/task3/' USING PigStorage (',');






