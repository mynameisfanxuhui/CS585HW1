customer = LOAD 'Customers.txt' USING PigStorage(',') AS (cusID, name, age, gender, countrycode, salary);
trans = LOAD 'Transactions.txt' USING PigStorage(',') AS (transID, ID, total, numitem, descr);

cus1 = FOREACH customer GENERATE cusID, name;

transgrp = GROUP trans BY ID;
transcnt = FOREACH transgrp GENERATE group AS ID, COUNT(trans.transID) AS cnt;

result = JOIN cus1 BY cusID, transcnt BY ID;
result1 = FOREACH result GENERATE name, cnt;
result2 = ORDER result1 BY cnt ASC;

final = LIMIT result2 1;
STORE final into 'Problem4_1_result' USING PigStorage(',');