# SIT-2015_BIA658_Social_Network_Analysis
> A course project done with classmates. Focus on the US president election. Collecting followers data from twitter. Very simple, wish it will help. 


* For the full report [PDF](https://github.com/WolfricWang/SIT-2015_BIA658_Social_Network_Analysis/blob/master/FINAL.REPORT.GROUP.2.pdf).
* Data collect till: Dec 2015
* Instructor: Prof. Rong Duan
* Co-Authors: Dun Wang, Ran Huan, Liye Pan, Tailun Song, Xianqiao Li

  ![Candidates](https://github.com/WolfricWang/SIT-2015_BIA658_Social_Network_Analysis/blob/master/pic_log/candidates.png)

## Project Popose

* Detect the most influential person in a particular community

* Detect relationships between users in a specific geographical location (i.e. States, County, Town etc.)

* Unlock any interesting phenomena

### Process
1. Use Twitter API and Python code to access politician’s IDs, followers’ IDs of each politician and their respective location information
2. Set up database on Amazon and store collected data in the database
3. Link database to MySQL and create table for each politician in MySQL Randomly sample 1% from the data collected and store in MySQL table

  ![Process](https://github.com/WolfricWang/SIT-2015_BIA658_Social_Network_Analysis/blob/master/pic_log/process.PNG)

### Tools Used
* Python, MySQL, Twitter API, Amazon Database

* R to draw relationship map

* Google fusion table to map the data

  ![Data_Store](https://github.com/WolfricWang/SIT-2015_BIA658_Social_Network_Analysis/blob/master/pic_log/data_store.png)
  
### Chanllenges
* Over 15 millions records extracted

* More than 50 hours spent

* Continuous improvement on coding and its efficiency

* Differences in languages used (i.e. Software incompatible with certain languages)
* Gather followers’ other attributes and determine whether a particular account is inactive
* For example we can choose two attributes:
	* Friend density = friends_count/registered days
	* Active level = tweets_account/registered days
* Since these attributes are independent, Naïve Bayes classifier can be used to judge whether the account is fake

### From the Data

[Big Picture].(https://www.google.com/fusiontables/DataSource?docid=1qdaSGMHckgmVaZMu1P2_nGxyhcONFYapab8cDkDw#map:id=3)The darker the more like be a Republican.
![map1](https://github.com/WolfricWang/SIT-2015_BIA658_Social_Network_Analysis/blob/master/pic_log/map1.PNG)

People in Southeast have a higher preference for Republican Party.

Only see [New York]. (https://www.google.com/fusiontables/DataSource?docid=10Hk9tv9zSI-V5nCFxC5A55Wg5cz-BlGwvVLfnWuy#map:id=3)Blue is Republican, Red is Democratic.
![map2](https://github.com/WolfricWang/SIT-2015_BIA658_Social_Network_Analysis/blob/master/pic_log/map2.PNG)



### Conclutions
* People in Southeast have a higher preference for Republican Party. People’s attitudes are diverse inner New York State.
* Though people’s political preference differs, most users’ political preferences are clear.
* Commonly we think that Democratic Party is supposed to win the 2016 Election because of Hilary, but the data from twitter shows that Republican Party still have a chance to beat the Democratic and people on twitter seem to like Republican better.

