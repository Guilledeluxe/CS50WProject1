import os
import csv

from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker

engine = create_engine(os.getenv("DATABASE_URL_LOCAL"))
db = scoped_session(sessionmaker(bind=engine))

def main():
  #Create  Authors & Books tables if not exists
  authors = db.execute("CREATE TABLE IF NOT EXISTS authors (id SERIAL PRIMARY KEY, name VARCHAR UNIQUE NOT NULL);")
  books   = db.execute("CREATE TABLE IF NOT EXISTS books (isbn VARCHAR PRIMARY KEY, title VARCHAR NOT NULL,\
                        id_author INTEGER NOT NULL REFERENCES authors(id), year INTEGER NOT NULL);")
  #Load Authors
  a= open("books.csv")
  reader_a = csv.reader(a)
  for isbn, title, author, year in reader_a:
    if db.execute("SELECT * FROM authors WHERE name = :name", {"name": author}).rowcount == 0:
        db.execute("INSERT INTO authors (name) VALUES (:name) RETURNING id",
          {"name":author} )
        print(f"+++ADDED+++ author:{author}) to Authors table")
        db.commit()
    else:
        print(f"<<<ERROR>>> author:{author}) already exists")
  #Load Books
  b= open("books.csv")
  reader_b = csv.reader(b)
  for isbn, title, author_name, year in reader_b:
    if db.execute("SELECT * FROM books WHERE isbn = :isbn", {"isbn": isbn}).rowcount == 0: # Check if book already exists
        author_lcl= db.execute("SELECT * FROM authors WHERE name = :name", {"name": author_name}).fetchone()
        if not author_lcl is None:
            db.execute("INSERT INTO books (isbn,title,id_author,year) VALUES (:isbn,:title,:author_id,:year)",
            {"isbn": isbn, "title": title, "author_id": author_lcl.id , "year": year})
        print(f"+++ADDED+++ Title:{title}) to Books table")
        db.commit()
    else:
        print(f"ERROR Title:{title}) already exists")
if __name__ == "__main__":
    main()
