customer = load 'file:///home/ds503/shared_folder/Customers.txt' using PigStorage(',') as (id:int, name:chararray, age:int, gender:chararray, countryCode:int, salary:float);
simpleCus = foreach customer generate id, age, gender;
transaction = load 'file:///home/ds503/shared_folder/Transactions.txt' using PigStorage(',') as (TransID:int, CustID:int, TransTotal:float, TransItem:int, TransDesc:chararray);
simpleTrans = foreach transaction generate CustID, TransTotal;
joinedSet = join simpleTrans by CustID, simpleCus by id using 'replicated';
namedSet = foreach joinedSet generate $1 as transTotal, $3 as age, $4 as gender;
namedSetA = foreach namedSet generate transTotal, (
CASE
WHEN age < 20 THEN '[10, 20)'
WHEN age >= 20 and age < 30 THEN '[20,30)'
WHEN age >= 30 and age < 40 THEN '[30, 40)'
WHEN age >= 40 and age < 50 THEN '[40, 50)'
WHEN age >= 50 and age < 60 THEN '[50, 60)'
Else '[60,70)'
END) as ageRange, gender;
groupedSet = group namedSetA by (ageRange, gender);
finalRes = foreach groupedSet generate group, MIN(namedSetA.transTotal), MAX(namedSetA.transTotal), AVG(namedSetA.transTotal);
store finalRes into 'file:///home/ds503/shared_folder/Problem4_4' using PigStorage(',');


