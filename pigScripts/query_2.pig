data = load '/user/root/input/clean_data.csv' using PigStorage(',') as (ID:int, Year_Birth:int, Education:chararray, Marital_Status:chararray, Income:float, Kidhome:int,
Teenhome:int, Dt_Customer:chararray, Recency:int, MntWines:int, MntFruits:int, MntMeatProducts:int,
MntFishProducts:int, MntSweetProducts:int, MntGoldProds:int, NumDealsPurchases:int,
NumWebPurchases:int, NumCatalogPurchases:int, NumStorePurchases:int, NumWebVisitsMonth:int, AcceptedCmp3:int,
AcceptedCmp4:int, AcceptedCmp5:int, AcceptedCmp1:int, AcceptedCmp2:int, Complain:int, Response:int);

group_education = group data by Education;

count = foreach group_education generate
group as education,COUNT(data.ID) as count;

sorted = order count by education;



store sorted into '/user/root/PigResults/task2/' USING PigStorage (',');
