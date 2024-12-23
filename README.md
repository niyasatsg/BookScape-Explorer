# BookScape-Explorer
The BookScape Explorer project aims to facilitate users in discovering and analyzing book data through a web application. The application will extract data from online book APIs, store this information in a database and perform analysis

**Table of Contents
********************
	•	Installation
	•	Configuration
	•	Usage
	•	Features
	•	Data Analysis Options
	•	Troubleshooting
	

Installation
*************
	Prerequisites
	•	Python 3.7 or higher
	•	MySQL Server
	•	MySQL Workbench (optional, for database management)

Clone the Repository
*********************
git clone https://github.com/yourusername/bookscape-explorer.git
cd bookscape-explorer

Install Dependencies
********************
pip install -r requirements.txt

MySQL Database Setup:
**********************

1.	Create a MySQL Database:
   	CREATE DATABASE bookscape;
2.	Create the books Table:
  	 USE bookscape;
   	 CREATE TABLE books (
       		book_id VARCHAR(36) PRIMARY KEY,
       		search_key VARCHAR(255),
       		book_title VARCHAR(255),
       		book_subtitle VARCHAR(255),
       		book_authors TEXT,
       		book_description TEXT,
       		industryIdentifiers TEXT,
       		text_readingModes INT,
       		image_readingModes INT,
       		pageCount INT,
       		languages VARCHAR(50),
       		imageLinks TEXT,
       		ratingsCount INT,
       		averageRating FLOAT,
       		country VARCHAR(50),
       		saleability VARCHAR(50),
       		isEbook BOOLEAN,
       		amount_listPrice FLOAT,
       		currencyCode_listPrice VARCHAR(10),
       		amount_retailPrice FLOAT,
       		currencyCode_retailPrice VARCHAR(10),
       		buyLink TEXT,
       		Publishedyear VARCHAR(10),
       		categories TEXT,
       		publisher VARCHAR(255)
   );

Configuration
**************
	•	Update Database Connection
	•	Update the database connection string in your code:
		engine = create_engine('mysql+pymysql://root:yourpassword@localhost:3306/bookscape')

Google Books API Key
********************
	Replace "Your api key" with your actual Google Books API key in the code:
	api_key = "Your api key"

Run the Application
*******************
	streamlit run app.py

Access the Application
************************
	Open your web browser and go to http://localhost:8501.

Features
********
Home Page
•	Introduction to Bookscape Explorer.
Search Page
•	Search for books based on genre, authors, etc.
•	Fetch book details from the Google Books API.
•	Store book details in the MySQL database.
Data Analysis Page
•	Perform various data analyses on the stored book data.
Data Analysis Options
1.	Check Availability of eBooks vs Physical Books
2.	Find the Publisher with the Most Books Published
3.	Identify the Publisher with the Highest Average Rating
4.	Get the Top 5 Most Expensive Books by Retail Price
5.	Find Books Published After 2010 with at Least 500 Pages
6.	List Books with Discounts Greater than 20%
7.	Find the Average Page Count for eBooks vs Physical Books
8.	Find the Top 3 Authors with the Most Books
9.	List Publishers with More than 10 Books
10.	Find the Average Page Count for Each Category
11.	Retrieve Books with More than 3 Authors
12.	Books with Ratings Count Greater Than the Average
13.	Books with the Same Author Published in the Same Year
14.	Books with a Specific Keyword in the Title
15.	Year with the Highest Average Book Price
16.	Count Authors Who Published 3 Consecutive Years
17.	Find Authors Who Published Books in the Same Year but Under Different Publishers
18.	Find the Average Retail Price of eBooks and Physical Books
19.	Identify Books with Ratings Significantly Different from the Average
20.	Determine the Publisher with the Highest Average Rating

Troubleshooting:
****************
	Common Issues
	•	Database Connection Error: Ensure your MySQL server is running and the connection string is correct.
	•	API Rate Limit Exceeded: If you encounter a rate limit error, wait for a while before making new requests.

