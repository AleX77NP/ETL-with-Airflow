import sqlite3

import requests
from bs4 import BeautifulSoup
import pandas as pd
import sqlalchemy
from sqlalchemy.orm import sessionmaker

def validate_data(df: pd.DataFrame) -> bool:
    # check if data frame is empty
    if df.empty:
        print("No data provided")
        return False

    # check if title is unique
    if pd.Series(df['title']).is_unique:
        pass
    else:
        raise Exception("Primary Key Check is violated. Title should be unique.")

    # check if any nulls are present
    if df.isnull().values.any():
        raise Exception("Null values found")

    return True


def run_yugioh_etl():
    # this can go in env
    URL = "https://ygorganization.com"
    DATABASE_URL = "sqlite:///yugioh_news.sqlite"

    # get content from the page
    page = requests.get(URL)
    posts = []

    soup = BeautifulSoup(page.content, "html.parser")

    # get div that contains all articles
    articles_container = soup.find(class_="article-container")

    # get all posts from the page
    post_elements = articles_container.find_all("article", class_="post")

    # extract image, title, date and description from each post
    for post_element in post_elements:
        image = post_element.find("div", class_="featured-image").find("a")
        content = post_element.find("div", class_="article-content clearfix")
        title = content.find("header").find("h2").find("a")
        date = content.find("div", class_="below-entry-meta").find("span", class_="posted-on").find("a").find("time")
        description = content.find("div", class_="entry-content clearfix").find("p")

        # create post dictionary for each post element
        post = {
            "image": image["href"],
            "title": title.text.strip(),
            "date": date.text.strip(),
            "description": description.text.strip()
        }

        # add post to the list
        posts.append(post)

    # show posts in a data frame
    post_df = pd.DataFrame(posts, columns=["image", "title", "date", "description"])

    if validate_data(post_df):
        print("Data is valid, load it...")

    engine = sqlalchemy.create_engine(DATABASE_URL)
    conn = sqlite3.connect('yugioh_news.sqlite')
    cursor = conn.cursor()

    sql_query = """
        CREATE TABLE IF NOT EXISTS yugioh_posts(
            image VARCHAR(200),
            title VARCHAR(200),
            date VARCHAR(200),
            description VARCHAR(200),
            CONSTRAINT primary_key_constraint PRIMARY KEY (title)
        )
        """
    cursor.execute(sql_query)
    print("Opened DB...")

    try:
        post_df.to_sql("yugioh_posts", engine, index=False, if_exists='append')
    except:
        print("Data already exists in the database")

    conn.close()
    print("Connection to DB closed.")
