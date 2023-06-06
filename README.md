# DE-2023
## 23년 1학기 데이터 공학 수업 정리

### hw1(ver.Hadoop) & hw3(ver.spark)
[IMDBStudent20201051.java](https://github.com/ryu-hyesu/DE-2023/blob/main/HW1/IMDBStudent20201051.java)

Q1. Genre count
+ We need know how many movies falls into each genre.
+ Input file : movie.dat
+ Output : <Genre> <# of movies>
+ Example
 
|genre|count|
|--|--|
|Thriller|182|
|Romance|108|
|Children’s|58|
 
[UBERStudent20201051.java](https://github.com/ryu-hyesu/DE-2023/blob/main/HW1/UBERStudent20201051.java)
 
Q2. Trips & Vehicles
+ We need know the number of trips and active vehicles per day and region.
+ Input file : uber.dat
+ Output : <region,day> <trips,vehicles>

|region|day|trips|vehicles|
|--|--|--|--|
|B02512|MON|1922|122|
|B02512|TUE|2200|451|
|B02512|WED|1200|251|
|B02512|THR|1510|348|
|B02512|FRI|1749|388|
|B02512|SAT|2324|611|
|B02512|SUN|2467|671|
 
### hw2
 
[IMDBStudent20201051.java](https://github.com/ryu-hyesu/DE-2023/blob/main/HW2/IMDBStudent20201051.java)
Q1. Top -k movies
+ We need find k fantasy movies whose avg. rating is high
+ Input file : movies.dat & ratings.dat
+ Output : <movie name> <avg. rating>
+ Example)

|movie name|avg|
|--|--|
|Jumanji (1995)|4.8|


[YouTubeStudent20201051.java](https://github.com/ryu-hyesu/DE-2023/blob/main/HW2/YouTubeStudent20201051.java)
Q2. Top -K category
+ We need find k categories whose average rating is high.
+ Input file : youtube.dat
+ Output : category avg. rating
+ Example)

|category|rating|
|--|--|
|Fantasy|4.8999|
