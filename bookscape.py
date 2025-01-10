import pandas as pd
import requests
import streamlit as st
import plotly.express as px
import time
from sqlalchemy import create_engine
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError, IntegrityError, DataError
from streamlit_option_menu import option_menu
from requests.exceptions import HTTPError
from streamlit.runtime.caching import cache_data

# Title of the project
st.image('C:/Users/niyas.abdul/Documents/Bookscape-HeaderLogo.png')
engine = create_engine('mysql+pymysql://root:K00th%40n%40llur@localhost:3306/bookscape')
# Custom CSS to center the dataframe
st.markdown(
    """
    <style>
    .dataframe-container {
        display: flex;
        width: 100%;
        justify-content: center;
               
    }
    .dataframe {
       justify-content: center;
        margin: auto;
        border-collapse: collapse;
        width: 100%;
    }
    .dataframe th, .dataframe td {
        border: 1px solid #ddd;
        padding: 8px;
    }
    .dataframe th {
        padding-top: 12px;
        padding-bottom: 12px;
        text-align: left;
        background-color: #f2f2f2;
        color: black;
    }
    </style>
    """,
    unsafe_allow_html=True
)
# Google Books API scrapping  
def fetch_books_data(query, api_key, max_results=5):
    query = f"{query}"
    url = f"https://www.googleapis.com/books/v1/volumes?q={query}&maxResults={max_results}&key={api_key}"
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.json()
    except HTTPError as http_err:
        if response.status_code == 429:  # Too Many Requests
            st.warning("Rate limit exceeded. Retrying after a delay...")
            time.sleep(60)  # Wait for 60 seconds before retrying
            return fetch_books_data(search_query, api_key)
        else:
            st.error(f"HTTP error occurred: {http_err}")
    except Exception as err:
        st.error(f"An error occurred: {err}")
# Extracted books details stored into database 
def insert_books_into_db(books_list):
    try:
        # Create SQLAlchemy engine
        engine = create_engine('mysql+pymysql://root:K00th%40n%40llur@localhost:3306/bookscape')
        # Insert books into the database
        with engine.connect() as connection:
            for book in books_list:
                book_data = {
                    "search_key": search_query,
                    "book_title": book.get('Title', ''),
                    "book_subtitle": book.get('Sub-Title', ''),
                    "book_authors": book.get('Authors', ''),
                    "book_description": book.get('description', ''), 
                    "industryIdentifiers": str(book.get('industryIdentifiers', [])),
                    "text_readingModes": int(book.get('Text Reading Modes', 0) or 0),
                    "image_readingModes": int(book.get('Image Reading Modes', 0) or 0),
                    "pageCount": int(book.get('Page Count', 0) or 0),
                    "languages": book.get('language', ''),
                    "imageLinks": str(book.get('imageLinks', {})),
                    "ratingsCount": int(book.get('Rating Count', 0) or 0),
                    "averageRating": float(book.get('Average Rating', 0) or 0),
                    "country": book.get('Country', ''),
                    "saleability": book.get('Saleability', ''),
                    "isEbook": book.get('Is eBook', False),
                    "amount_listPrice": float(book.get('List Price Amount', 0) or 0),
                    "currencyCode_listPrice": book.get('List Price Currency', ''),
                    "amount_retailPrice": float(book.get('Retail Price Amount', 0) or 0),
                    "currencyCode_retailPrice": book.get('Retail Price Currency', ''),
                    "buyLink": book.get('Buy Link', ''),
                    "Publishedyear": book.get('Published Date', ''),
                    "categories": book.get('Categories', ''),
                    "publisher": book.get('Publisher', '')
                    
                }
                sql = text("""
                    INSERT INTO books (book_id, search_key, book_title, book_subtitle, book_authors, book_description, industryIdentifiers, text_readingModes, image_readingModes, pageCount, languages, imageLinks, ratingsCount, averageRating, country, saleability, isEbook, amount_listPrice, currencyCode_listPrice, amount_retailPrice, currencyCode_retailPrice, buyLink, Publishedyear, categories, publisher)
                    VALUES (UUID(), :search_key, :book_title, :book_subtitle, :book_authors, :book_description, :industryIdentifiers, :text_readingModes, :image_readingModes, :pageCount, :languages, :imageLinks, :ratingsCount, :averageRating, :country, :saleability, :isEbook, :amount_listPrice, :currencyCode_listPrice, :amount_retailPrice, :currencyCode_retailPrice, :buyLink, :Publishedyear, :categories, :publisher)
                """)

                connection.execute(sql, book_data)
            connection.commit()  # Explicitly commit the transaction
            st.success("Books data extracted and Data inserted successfully.")
    except IntegrityError as e:
        st.error(f"Integrity error: {e.orig}")
    except DataError as e:
        st.error(f"Data error: {e.orig}")
    except SQLAlchemyError as e:
        st.error(f"SQLAlchemy error: {e}")
    except Exception as e:
        st.error(f"Unexpected error: {e}")
# sidebar menu
sidebar_Menu = 1
def streamlit_menu(sidebar_menu=1):
    if sidebar_menu == 1:
        # 1. as sidebar_menu
        with st.sidebar:
            selected = option_menu(
                menu_title="",
                options=["Home", "Search", "Data Analysis"], 
                icons=["house", "search", "bar-chart"], 
                menu_icon="cast", 
                default_index=0, 
            )
        return selected
selected = streamlit_menu(sidebar_menu=sidebar_Menu)
# Set a time-to-live (TTL) to refresh the cache periodically
@st.cache_data
@st.cache_data(ttl=60)  
# Fetch data from the database
def fetch_data(query):
    with engine.connect() as connection:
        result = pd.read_sql(query, connection)
    return result
# Home Page
if selected == "Home":
    st.write("""
**Bookscape explorer** offers you over a million titles across genres like **Fiction, Non-fiction, Crime, Thriller, Comics** and more, and across categories like **Children’s Books, Young Adults, Academic Books and Textbooks, Graphic Novels, Manga, Indian Writing, Classics** and more.

If reading is your passion or pastime, head over to **Bookscape** to browse through a massive selection of titles in multiple languages as well. From new releases, bestsellers to curated editor’s picks, there is something for everyone.

**Students** can take their pick from exam prep books, university textbooks and references, school textbooks and guides. **Young teenagers** will find fiction and fantasy books from the Young Adults section interesting. Lovers of true crime, suspense and horror stories would find the Crime, Mystery & Thrillers section of the site an absolute delight.

Browse by genre when you buy books online at **Bookscape** that makes it very convenient to choose a book to curl up with. We understand that every reader is different and believe in suggesting the right book to the right reader.
""")
# Search book,extract books details and call insert function for storage
if selected == "Search":
        search_query = st.text_input("Search books", placeholder="Search books based on genre, authors...")
        search_button = st.button("Search")
        if search_button:
            if not search_query.strip():
                st.error("Search query cannot be empty.")
            else:
                api_key = "AIzaSyDSVnISPmCbFmhS4TsFc65DQZN3lYCVRxU" 
                books_data = fetch_books_data(search_query, api_key)
                if books_data:
                    books_list = []
                    for book in books_data.get('items', []):
                        volume_info = book.get('volumeInfo', {})
                        sale_info = book.get('saleInfo', {})
                        title = volume_info.get('title')
                        subtitle = volume_info.get('subtitle')
                        authors = volume_info.get('authors', [])
                        description = volume_info.get('description')
                        industry_identifiers = volume_info.get('industryIdentifiers', [])
                        text_reading_modes = volume_info.get('readingModes', {}).get('text')
                        image_reading_modes = volume_info.get('readingModes', {}).get('image')
                        page_count = volume_info.get('pageCount')
                        language = volume_info.get('language')
                        image_links = volume_info.get('imageLinks', {})
                        ratings_count = volume_info.get('ratingsCount')
                        average_rating = volume_info.get('averageRating')
                        country = sale_info.get('country')
                        saleability = sale_info.get('saleability')
                        is_ebook = sale_info.get('isEbook')
                        list_price = sale_info.get('listPrice', {})
                        retail_price = sale_info.get('retailPrice', {})
                        buy_link = sale_info.get('buyLink')
                        published_date = volume_info.get('publishedDate')
                        categories = volume_info.get('categories'),
                        publisher = volume_info.get('publisher')

                        books_list.append({
                        "Title": title,
                        "Authors": ', '.join(authors),
                        "Page Count": page_count,
                        "Published Date": published_date,
                        "Categories":  categories,
                        "Publisher":publisher

                    })

                    df_books = pd.DataFrame(books_list)
                    # Convert dataframe to HTML
                    html_table = df_books.to_html(classes='dataframe', index=False)
                    # Display the HTML table
                    st.markdown(f'<div class="dataframe-container">{html_table}</div>', unsafe_allow_html=True)
                    # Insert books into database
                    insert_books_into_db(books_list)
# Perform data analysis with the given 20 different questions
if selected == "Data Analysis":
    options = ["1.Check Availability of eBooks vs Physical Books", "2.Find the Publisher with the Most Books Published", "3.Identify the Publisher with the Highest Average Rating", "4.Get the Top 5 Most Expensive Books by Retail Price", "5.Find Books Published After 2010 with at Least 500 Pages", "6.List Books with Discounts Greater than 20%", "7.Find the Average Page Count for eBooks vs Physical Books", "8.Find the Top 3 Authors with the Most Books", "9.List Publishers with More than 10 Books", "10.Find the Average Page Count for Each Category", "11.Retrieve Books with More than 3 Authors", "12.Books with Ratings Count Greater Than the Average", "13.Books with the Same Author Published in the Same Year", "14.Books with a Specific Keyword in the Title", "15.Year with the Highest Average Book Price", "16.Count Authors Who Published 3 Consecutive Years", "17.Write a SQL query to find authors who have published books in the same year but under different publishers. Return the authors, year, and the COUNT of books they published in that year.", "18.Create a query to find the average amount_retailPrice of eBooks and physical books. Return a single result set with columns for avg_ebook_price and avg_physical_price. Ensure to handle cases where either category may have no entries", "19.To identify books that have an averageRating that is more than two standard deviations away from the average rating of all books. Return the title, averageRating, and ratingsCount for these outliers.", "20.Determines which publisher has the highest average rating among its books, but only for publishers that have published more than 10 books. Return the publisher, average_rating, and the number of books published."]
    selected_option = st.selectbox("Select an option to view the data analysis results:", options)
    if selected_option =="1.Check Availability of eBooks vs Physical Books":
            # Query to fetch eBook vs Physical Book availability
            query = """
            SELECT 
                isEbook,
                COUNT(*) as count
            FROM 
                books
            GROUP BY 
                isEbook
            """
            # Fetch the data
            data = fetch_data(query)

            # Map isEbook to meaningful labels
            data['isEbook'] = data['isEbook'].map({1: 'eBooks', 0: 'Physical Books'})

            # Display the data
            st.subheader("Availability of eBooks vs Physical Books")
            
            # Plot the data
            st.bar_chart(data.set_index('isEbook'))

            # Additional analysis
            ebook_count = data[data['isEbook'] == 'eBooks']['count'].values[0] if not data[data['isEbook'] == 'eBooks'].empty else 0
            physical_count = data[data['isEbook'] == 'Physical Books']['count'].values[0] if not data[data['isEbook'] == 'Physical Books'].empty else 0

            st.write(f"Total eBooks: {ebook_count}")
            st.write(f"Total Physical Books: {physical_count}")

            if ebook_count > physical_count:
                st.success("There are more eBooks available than physical books.")
            else:
                st.info("There are more physical books available than eBooks.")
     
    if selected_option =="2.Find the Publisher with the Most Books Published":
            # Query to find the publisher with the most books published
            query = """
            SELECT 
                publisher,
                COUNT(*) as count
            FROM 
                books
            GROUP BY 
                publisher
            ORDER BY 
                count DESC
            LIMIT 1
            """
            # Fetch the data
            data = fetch_data(query)
            # Display the data
            st.subheader("Publisher with the Most Books Published")
            # Additional analysis
            if not data.empty:
                publisher = data['publisher'].values[0]
                count = data['count'].values[0]
                st.write(f"The publisher with the most books published is **{publisher}** with **{count}** books.")
            else:
                st.write("No data available.")

            # Plot the data (if needed)
            st.bar_chart(data.set_index('publisher'))

    if selected_option =="3.Identify the Publisher with the Highest Average Rating":
        # Query to find the publisher with the highest average rating
        query = """
        SELECT 
            publisher,
            AVG(averageRating) as avg_rating
        FROM 
            books
        GROUP BY 
            publisher
        ORDER BY 
            avg_rating DESC
        LIMIT 1
        """

        # Fetch the data
        data = fetch_data(query)

        # Display the data
        st.subheader("Publisher with the Highest Average Rating")
        
        # Additional analysis
        if not data.empty:
            publisher = data['publisher'].values[0]
            avg_rating = data['avg_rating'].values[0]
            st.write(f"The publisher with the highest average rating is **{publisher}** with an average rating of **{avg_rating:.2f}**.")
        else:
            st.write("No data available.")

        # Plot the data (if needed)
        st.bar_chart(data.set_index('publisher'))

    if selected_option =="4.Get the Top 5 Most Expensive Books by Retail Price":
        # Query to get the top 5 most expensive books by retail price
        query = """
        SELECT 
            book_title,
            book_authors,
            amount_retailPrice,
            currencyCode_retailPrice
        FROM 
            books
        ORDER BY 
            amount_retailPrice DESC
        LIMIT 5
        """
        # Fetch the data
        data = fetch_data(query)

        # Display the data
        st.subheader("Top 5 Most Expensive Books by Retail Price")
     
        # Additional analysis
        if not data.empty:
            st.write("Here are the top 5 most expensive books by retail price:")
            for index, row in data.iterrows():
                st.write(f"**{row['book_title']}** by {row['book_authors']} - {row['amount_retailPrice']} {row['currencyCode_retailPrice']}")
        else:
            st.write("No data available.")

        # Plot the data (if needed)
        st.bar_chart(data.set_index('book_title')['amount_retailPrice'])
    
    if selected_option =="5.Find Books Published After 2010 with at Least 500 Pages":
        # Query to find books published after 2010 with at least 500 pages
        query = """
        SELECT 
            book_title,
            book_authors,
            pageCount,
            Publishedyear
        FROM 
            books
        WHERE 
            Publishedyear > 2010 AND pageCount >= 500
        ORDER BY 
            Publishedyear DESC
        """

        # Fetch the data
        data = fetch_data(query)

        # Display the data
        st.subheader("Books Published After 2010 with at Least 500 Pages")
        # Additional analysis
        if not data.empty:
            st.write("Here are the books published after 2010 with at least 500 pages:")
            for index, row in data.iterrows():
                st.write(f"**{row['book_title']}** by {row['book_authors']} - {row['pageCount']} pages, published in {row['Publishedyear']}")
        else:
            st.write("No data available.")

        # Plot the data (if needed)
        st.bar_chart(data.set_index('book_title')['pageCount'])

    if selected_option =="6.List Books with Discounts Greater than 20%":
        # Query to list books with discounts greater than 20%
        query = """
        SELECT 
            book_title,
            book_authors,
            amount_listPrice,
            amount_retailPrice,
            ((amount_listPrice - amount_retailPrice) / amount_listPrice) * 100 AS discount_percentage
        FROM 
            books
        WHERE 
            amount_listPrice > 0 AND amount_retailPrice > 0 AND ((amount_listPrice - amount_retailPrice) / amount_listPrice) * 100 > 20
        ORDER BY 
            discount_percentage DESC
        """
        # Fetch the data
        data = fetch_data(query)

        # Display the data
        st.subheader("Books with Discounts Greater than 20%")
        st.write(data)

        # Additional analysis
        if not data.empty:
            st.write("Here are the books with discounts greater than 20%:")
            for index, row in data.iterrows():
                st.write(f"**{row['book_title']}** by {row['book_authors']} - List Price: {row['amount_listPrice']}, Retail Price: {row['amount_retailPrice']}, Discount: {row['discount_percentage']:.2f}%")
    
    if selected_option =="7.Find the Average Page Count for eBooks vs Physical Books":
        # Query to find the average page count for eBooks vs Physical Books
        query = """
        SELECT 
            isEbook,
            AVG(pageCount) as avg_page_count
        FROM 
            books
        GROUP BY 
            isEbook
        """

        # Fetch the data
        data = fetch_data(query)

        # Map isEbook to meaningful labels
        data['isEbook'] = data['isEbook'].map({1: 'eBooks', 0: 'Physical Books'})

        # Display the data
        st.subheader("Average Page Count for eBooks vs Physical Books")

        # Additional analysis
        if not data.empty:
            for index, row in data.iterrows():
                st.write(f"**{row['isEbook']}**: Average Page Count = {row['avg_page_count']:.2f}")
        else:
            st.write("No data available.")

        # Plot the data (if needed)
        st.bar_chart(data.set_index('isEbook')['avg_page_count'])

    if selected_option =="8.Find the Top 3 Authors with the Most Books":
        # Query to find the top 3 authors with the most books
        query = """
        SELECT 
            book_authors,
            COUNT(*) as count
        FROM 
            books
        GROUP BY 
            book_authors
        ORDER BY 
            count DESC
        LIMIT 3
        """

        # Fetch the data
        data = fetch_data(query)

        # Display the data
        st.subheader("Top 3 Authors with the Most Books")
        # Additional analysis
        if not data.empty:
            st.write("Here are the top 3 authors with the most books:")
            for index, row in data.iterrows():
                st.write(f"**{row['book_authors']}**: {row['count']} books")
        else:
            st.write("No data available.")

        # Plot the data (if needed)
        st.bar_chart(data.set_index('book_authors')['count'])

    if selected_option =="9.List Publishers with More than 10 Books":
        # Query to list publishers with more than 10 books
        query = """
        SELECT 
            publisher,
            COUNT(*) as count
        FROM 
            books
        GROUP BY 
            publisher
        HAVING 
            COUNT(*) > 10
        ORDER BY 
            count DESC
        """

        # Fetch the data
        data = fetch_data(query)

        # Display the data
        st.subheader("Publishers with More than 10 Books")
        st.write(data)

        # Additional analysis
        if not data.empty:
            st.write("Here are the publishers with more than 10 books:")
            for index, row in data.iterrows():
                st.write(f"**{row['publisher']}**: {row['count']} books")
        else:
            st.write("No data available.")
        
    if selected_option =="10.Find the Average Page Count for Each Category":
        # Function to load data from the database
        def load_data():
            query = "SELECT * FROM books"
            df = pd.read_sql(query, engine)
            return df

        # Load data
        df = load_data()

        # Ensure categories column is not empty and split categories into separate rows
        df = df.explode('categories')

        # Calculate the average page count for each category
        average_page_count = df.groupby('categories')['pageCount'].mean().reset_index()
        average_page_count.columns = ['Category', 'Average Page Count']

        # Display the results using Streamlit
        st.subheader("Average Page Count for Each Category")
        st.dataframe(average_page_count)
    
    if selected_option =="11.Retrieve Books with More than 3 Authors":
        # Query to retrieve books with more than 3 authors
        query = """
        SELECT 
            book_title,
            book_authors,
            pageCount,
            Publishedyear
        FROM 
            books
        WHERE 
            LENGTH(book_authors) - LENGTH(REPLACE(book_authors, ',', '')) + 1 > 3
        ORDER BY 
            book_title
        """
        # Fetch the data
        data = fetch_data(query)

        # Display the data
        st.subheader("Books with More than 3 Authors")
        st.write(data)

        # Additional analysis
        if not data.empty:
            st.write("Here are the books with more than 3 authors:")
            for index, row in data.iterrows():
                st.write(f"**{row['book_title']}** by {row['book_authors']} - {row['pageCount']} pages, published in {row['Publishedyear']}")
        else:
            st.write("No data available.")

        # Plot the data (if needed)
        #st.bar_chart(data.set_index('book_title')['pageCount'])
    
    if selected_option =="12.Books with Ratings Count Greater Than the Average":
        # Query to find books with ratings count greater than the average
        query = """
        SELECT 
            book_title,
            book_authors,
            ratingsCount,
            averageRating
        FROM 
            books
        WHERE 
            ratingsCount > (SELECT AVG(ratingsCount) FROM books)
        ORDER BY 
            ratingsCount DESC
        """

        # Fetch the data
        data = fetch_data(query)

        # Display the data
        st.subheader("Books with Ratings Count Greater Than the Average")
        st.write(data)
       
    if selected_option =="13.Books with the Same Author Published in the Same Year":
        # Query to find books with the same author published in the same year
        query = """
        SELECT 
            book_authors,
            Publishedyear,
            GROUP_CONCAT(book_title SEPARATOR ', ') as books,
            COUNT(*) as book_count
        FROM 
            books
        GROUP BY 
            book_authors, Publishedyear
        HAVING 
            COUNT(*) > 1
        ORDER BY 
            book_authors, Publishedyear
        """

        # Fetch the data
        data = fetch_data(query)

        # Display the data
        st.subheader("Books with the Same Author Published in the Same Year")
    
        # Additional analysis
        if not data.empty:
            st.write("Here are the books with the same author published in the same year:")
            for index, row in data.iterrows():
                st.write(f"**{row['book_authors']}** ({row['Publishedyear']}): {row['books']}")
        else:
            st.write("No data available.")

        # Plot the data using Plotly
        fig = px.bar(data, x='book_authors', y='book_count', color='Publishedyear', 
                    labels={'book_authors': 'Authors', 'book_count': 'Number of Books', 'Publishedyear': 'Year'},
                    title='Number of Books by the Same Author Published in the Same Year')
        st.plotly_chart(fig)
    
    if selected_option =="14.Books with a Specific Keyword in the Title":
        # Function to fetch data from the database
        @st.cache_data(ttl=60)  # Set a time-to-live (TTL) to refresh the cache periodically
        def fetch_data(query, keyword):
            with engine.connect() as connection:
                result = pd.read_sql(text(query), connection, params={'keyword': f'%{keyword}%'})
            return result

        # Streamlit app
        st.subheader("Books with a Specific Keyword in the Title")

        # Input for keyword
        keyword = st.text_input("Enter a keyword to search in book titles:")

        if keyword:
            # Query to find books with the specific keyword in the title
            query = """
            SELECT 
                book_title,
                book_authors,
                pageCount,
                Publishedyear
            FROM 
                books
            WHERE 
                book_title LIKE :keyword
            ORDER BY 
                book_title
            """

            # Fetch the data
            data = fetch_data(query, keyword)

            # Display the data
            st.write(data)

            # Additional analysis
            if not data.empty:
                st.write(f"Here are the books with the keyword '{keyword}' in the title:")
                for index, row in data.iterrows():
                    st.write(f"**{row['book_title']}** by {row['book_authors']} - {row['pageCount']} pages, published in {row['Publishedyear']}")
            else:
                st.write("No data available.")
        else:
            st.write("Please enter a keyword to search.")

    if selected_option =="15.Year with the Highest Average Book Price":
        # SQL query to calculate the average book price per year
        query = """
        SELECT 
            Publishedyear, 
            AVG(amount_listPrice) AS average_price 
        FROM 
            books 
        GROUP BY 
            Publishedyear 
        ORDER BY 
            average_price DESC
        """

        # Fetch the data
        data = fetch_data(query)

        # Display the data
        if not data.empty:
            st.write(data)
            highest_avg_price_year = data.iloc[0]
            st.write(f"The year with the highest average book price is {highest_avg_price_year['Publishedyear']} with an average price of {highest_avg_price_year['average_price']:.2f}.")
        else:
            st.write("No data available.")

    if selected_option =="16.Count Authors Who Published 3 Consecutive Years":
        # SQL query to count authors who published in 3 consecutive years
        query = """
                SELECT 
            a.book_authors, 
            a.Publishedyear, 
            b.Publishedyear AS next_year, 
            c.Publishedyear AS year_after_next,
            (SELECT COUNT(DISTINCT book_authors) 
            FROM books 
            WHERE book_authors IN (
                SELECT book_authors 
                FROM books 
                WHERE Publishedyear = a.Publishedyear
            )
            ) AS author_count
        FROM 
            (SELECT book_authors, Publishedyear FROM books) a
        JOIN 
            (SELECT book_authors, Publishedyear FROM books) b
        ON 
            a.book_authors = b.book_authors AND a.Publishedyear = b.Publishedyear + 1
        JOIN 
            (SELECT book_authors, Publishedyear FROM books) c
        ON 
            a.book_authors = c.book_authors AND a.Publishedyear = c.Publishedyear + 2;
        """
        # Streamlit app
        st.subheader("Count of Authors Who Published in 3 Consecutive Years")
        # Fetch the data
        data = fetch_data(query)
        st.write(data)
        # Display the result
        if not data.empty:
            author_count = data.iloc[0]['author_count']
            st.write(f"The number of authors who published books in three consecutive years is: {author_count}")
        else:
            st.write("No data available.")

    if selected_option =="17.Write a SQL query to find authors who have published books in the same year but under different publishers. Return the authors, year, and the COUNT of books they published in that year.":
        # Function to load data from the database
        def load_data():
            engine = create_engine('mysql+pymysql://root:K00th%40n%40llur@localhost:3306/bookscape')
            query = """
                SELECT 
                    book_authors, 
                    Publishedyear, 
                    GROUP_CONCAT(DISTINCT publisher ORDER BY publisher SEPARATOR ', ') AS publishers,
                    COUNT(*) AS book_count
                FROM 
                    books
                GROUP BY 
                    book_authors, 
                    Publishedyear
                HAVING 
                    COUNT(DISTINCT publisher) > 1;
            """
            df = pd.read_sql(query, engine)
            return df

        # Load data
        df = load_data()

        # Display the results using Streamlit
        st.subheader("Authors Who Published Books in the Same Year with Different Publishers")
        st.dataframe(df)

    if selected_option =="18.Create a query to find the average amount_retailPrice of eBooks and physical books. Return a single result set with columns for avg_ebook_price and avg_physical_price. Ensure to handle cases where either category may have no entries":
        # SQL query to find the average amount_retailPrice of eBooks and physical books
        query = """
        SELECT 
            COALESCE(AVG(CASE WHEN IsEbook = 1 THEN amount_retailPrice END), 0) AS avg_ebook_price,
            COALESCE(AVG(CASE WHEN IsEbook = 0 THEN amount_retailPrice END), 0) AS avg_physical_price
        FROM 
            books;
        """

        # Fetch the data
        data = fetch_data(query)

        # Streamlit app
        st.subheader("Average Retail Price of eBooks and Physical Books")

        # Display the result
        if not data.empty:
            avg_ebook_price = data.iloc[0]['avg_ebook_price']
            avg_physical_price = data.iloc[0]['avg_physical_price']
            st.write(f"Average eBook Price: {avg_ebook_price:.2f}")
            st.write(f"Average Physical Book Price: {avg_physical_price:.2f}")
        else:
            st.write("No data available.")
    
    if selected_option =="19.To identify books that have an averageRating that is more than two standard deviations away from the average rating of all books. Return the title, averageRating, and ratingsCount for these outliers.":
        # SQL query to find books with averageRating more than two standard deviations away from the average
        query = """
        WITH stats AS (
            SELECT 
                AVG(averageRating) AS avg_rating, 
                STDDEV(averageRating) AS stddev_rating
            FROM 
                books
        ),
        outliers AS (
            SELECT 
                book_title, 
                averageRating, 
                ratingsCount,
                (averageRating - (SELECT avg_rating FROM stats)) / (SELECT stddev_rating FROM stats) AS z_score
            FROM 
                books
        )
        SELECT 
            book_title, 
            averageRating, 
            ratingsCount
        FROM 
            outliers
        WHERE 
            ABS(z_score) > 2;
        """

        # Fetch the data
        data = fetch_data(query)

        # Streamlit app
        st.subheader("Books with Ratings More Than Two Standard Deviations Away from the Average")

        # Display the result
        if not data.empty:
            st.write(data)
        else:
            st.write("No data available.") 
    
    if selected_option =="20.Determines which publisher has the highest average rating among its books, but only for publishers that have published more than 10 books. Return the publisher, average_rating, and the number of books published.":
        # SQL query to find the publisher with the highest average rating among its books
        def load_data():
            query = """
            SELECT 
                publisher, 
                AVG(averageRating) AS average_rating, 
                COUNT(*) AS book_count
            FROM 
                books
            GROUP BY 
                publisher
            HAVING 
                COUNT(*) > 10
            ORDER BY 
                average_rating DESC
            LIMIT 1;
            """
            df = pd.read_sql(query, engine)
            return df

        # Load data
        df = load_data()

        # Display the results using Streamlit
        st.subheader("Publisher with the Highest Average Rating")
        st.dataframe(df)