raw = LOAD 'Customers.txt' USING PigStorage(',') AS (ID, name, age, gender, countrycode, salary);

grp = GROUP raw BY countrycode;
grpres = FOREACH grp GENERATE group as countrycode, COUNT(raw.ID) AS cnt;

clean2 = FILTER grpres by cnt>5000 OR cnt < 2000;
final = FOREACH clean2 GENERATE countrycode;

STORE final INTO 'Problem4_3_result' USING PigStorage(',');