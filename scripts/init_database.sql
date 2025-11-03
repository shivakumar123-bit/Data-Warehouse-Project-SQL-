/* 
Create Database & Schema
*/
/*
Check and remove the database is already exists.
*/
use master;

create database DataWarehouse;

use DataWarehouse;
go

create schema bronze;
go

create schema silver;
go

create schema gold;
go
