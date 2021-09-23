customer = load 'file:///home/ds503/shared_folder/Customers.txt' using PigStorage(',') as (id:int, name:chararray, age:int, gender:chararray, countryCode:int, salary:float);
simpleCus = foreach customer generate id, name, salary;
transaction = load 'file:///home/ds503/shared_folder/Transactions.txt' using PigStorage(',') as (TransID:int, CustID:int, TransTotal:float, TransItem:int, TransDesc:chararray);
simpleTrans = foreach transaction generate CustID, TransTotal, TransItem;
joinedSet = join simpleTrans by CustID, simpleCus by id using 'replicated';
namedSet = foreach joinedSet generate $0 as id, $1 as transTotal, $2 as transItem, $4 as name, $5 as salary;
groupedSet = group namedSet by (id, name, salary);
finalRes = foreach groupedSet generate group, COUNT(namedSet), SUM(namedSet.transTotal), MIN(namedSet.transItem);
store finalRes into 'file:///home/ds503/shared_folder/Problem4_2' using PigStorage(',');