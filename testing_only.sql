
#------------------- test table only ----------------------------------
# used within MYSQL Workbench

USE melbdatathon2017;
#
DROP TABLE IF EXISTS gd_stores;
CREATE TABLE gd_stores
(
	Store_ID	smallint
,	StoreName	varchar(255)
,	StateCode	varchar(255)
,	PostCode	varchar(255)
) ENGINE=MyISAM DEFAULT CHARSET utf8 COLLATE utf8_general_ci;

# %user-home-dir% - supply your own

LOAD DATA LOCAL INFILE '/home/%user-home-dir%/Documents/data-science/2017-datathon/files/toy_shops.txt' INTO TABLE gd_stores FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'  LINES TERMINATED BY '\n' IGNORE 1 LINES;

SELECT * FROM melbdatathon2017.gd_stores;

#ctrl+shift+enter
SET SQL_SAFE_UPDATES = 0;
DELETE FROM melbdatathon2017.gd_stores;
