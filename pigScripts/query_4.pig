data = load '/user/root/input/clean_data.csv' using PigStorage(',') as (ID:int, Year_Birth:int, Education:chararray, Marital_Status:chararray, Income:float, Kidhome:int,
Teenhome:int, Dt_Customer:chararray, Recency:int, MntWines:int, MntFruits:int, MntMeatProducts:int,
MntFishProducts:int, MntSweetProducts:int, MntGoldProds:int, NumDealsPurchases:int,
NumWebPurchases:int, NumCatalogPurchases:int, NumStorePurchases:int, NumWebVisitsMonth:int, AcceptedCmp3:int,
AcceptedCmp4:int, AcceptedCmp5:int, AcceptedCmp1:int, AcceptedCmp2:int, Complain:int, Response:int);

date_data = foreach data generate
ID, Year_Birth, Education, Marital_Status,Income, Kidhome,
Teenhome, ToDate(Dt_Customer,'yyyy-MM-dd') as Dt_Customer, Recency, MntWines,MntFruits, MntMeatProducts, MntFishProducts, MntSweetProducts, MntGoldProds, NumDealsPurchases,
NumWebPurchases, NumCatalogPurchases, NumStorePurchases, NumWebVisitsMonth, AcceptedCmp3,
AcceptedCmp4, AcceptedCmp5, AcceptedCmp1, AcceptedCmp2, Complain, Response;

filter_income = filter date_data by Income > 69500;

year_data = foreach filter_income generate
ID, Year_Birth, Education, Marital_Status,Income, Kidhome,
Teenhome, GetYear(Dt_Customer) as Year, Recency, MntWines, MntFruits, MntMeatProducts, MntFishProducts, MntSweetProducts, MntGoldProds, NumDealsPurchases,
NumWebPurchases, NumCatalogPurchases, NumStorePurchases, NumWebVisitsMonth, AcceptedCmp3,
AcceptedCmp4, AcceptedCmp5, AcceptedCmp1, AcceptedCmp2, Complain, Response;

filter_year = filter year_data by Year > 2020;



customers = group data All;


avg = foreach customers generate 
AVG(data.MntWines), AVG(data.MntFruits), AVG(data.MntMeatProducts), 
AVG(data.MntFishProducts), AVG(data.MntSweetProducts),
AVG(data.MntGoldProds);

gold = filter filter_year by MntWines > 1.5*avg.$00 and
MntFruits > 1.5*avg.$01 and
MntMeatProducts > 1.5*avg.$02 and
MntFishProducts > 1.5*avg.$03 and
MntSweetProducts > 1.5*avg.$04 and
MntGoldProds > 1.5*avg.$05;

filter_year_s = filter year_data by Year < 2021;

silver = filter filter_year_s by MntWines > 1.5*avg.$00 and
MntFruits > 1.5*avg.$01 and
MntMeatProducts > 1.5*avg.$02 and
MntFishProducts > 1.5*avg.$03 and
MntSweetProducts > 1.5*avg.$04 and
MntGoldProds > 1.5*avg.$05;

gold_sorted = order gold by ID desc;
silver_sorted = order silver by ID desc;


gold_fin = foreach gold_sorted generate 'Gold'as type,ID;
silver_fin = foreach silver_sorted generate 'Silver'as type,ID;



g = union gold_fin, silver_fin;
f = group g by type;

final = foreach f generate
group as type, g.ID;


store final into '/user/root/PigResults/task4/' USING PigStorage (',');
