--select * from DBT.CONNECTFASTER.PERSONS;
--INSERT INTO CONNECTFASTER.Persons (PersonID, LastName,FirstName, Address, City) VALUES ('3', 'Tom B. Erichsen','single','Skagen 21', 'Stavanger');

INSERT INTO DBT.CONNECTFASTER.Persons_bkp select * from DBT.CONNECTFASTER.Persons limit 1; 

-- Created backup table
/*CREATE TABLE DBT.CONNECTFASTER.Persons_bkp (
    PersonID int,
    LastName varchar(255),
    FirstName varchar(255),
    Address varchar(255),
    City varchar(255)
);*/
