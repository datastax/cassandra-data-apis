// GraphQL schema:
// type BooksByAuthor {
//   authorId: String
//   firstname: String
//   lastname: String
//   title: String
// }
// 
// type BooksByTitle {
//   authorId: String
//   pages: Int
//   title: String
//   year: Int
// }
//
// type Query {
//   booksByAuthor(
//     firstname: String!
//     lastname: String!
//     title: String
//   ): [BooksByAuthor]
//   booksByTitle(authorId: String, title: String!): [BooksByTitle]
// }

// Example GraphQL query:
// query {
//   booksByAuthor(firstname: "Ariel", lastname: "Stein") {
//     title
//     firstname
//     lastname
//     authorId
//   }
//   booksByTitle(title: "Book 4") {
//     title
//     pages
//     year
//   }
// }

CREATE KEYSPACE IF NOT EXISTS store WITH replication = {
  'class': 'NetworkTopologyStrategy', 'dc1': '1'
};

DROP TABLE IF EXISTS store.books_by_title;
DROP TABLE IF EXISTS store.books_by_author;

CREATE TABLE store.books_by_title (
  title text,
  author_id uuid,
  pages int,
  year int,
  PRIMARY KEY (title, author_id)
);

INSERT INTO store.books_by_title (title, author_id, pages, year) VALUES ('Book 1', 1c3eb87a-9ce7-491e-9dd9-fb3c819875cf, 123, 1901); 
INSERT INTO store.books_by_title (title, author_id, pages, year) VALUES ('Book 2', 8fec26ff-09a0-4c23-9c8b-8bbf8e198f12, 456, 1902); 
INSERT INTO store.books_by_title (title, author_id, pages, year) VALUES ('Book 3', f8f5a9de-4bc6-4177-be64-87d0db7bf9be, 789, 2001); 
INSERT INTO store.books_by_title (title, author_id, pages, year) VALUES ('Book 4', 9e390783-f4c2-4a6e-ac82-818d35cada68, 101, 2002); 
INSERT INTO store.books_by_title (title, author_id, pages, year) VALUES ('Book 5', 9e390783-f4c2-4a6e-ac82-818d35cada68, 201, 2020); 

CREATE TABLE store.books_by_author (
  firstname text, 
  lastname text, 
  title text,
  author_id uuid,
  PRIMARY KEY ((firstname, lastname), title)
);

INSERT INTO store.books_by_author (firstname, lastname, title, author_id) VALUES ('Mike', 'Hoff', 'Book 1', 1c3eb87a-9ce7-491e-9dd9-fb3c819875cf); 
INSERT INTO store.books_by_author (firstname, lastname, title, author_id) VALUES ('Joe', 'Smith', 'Book 2', 8fec26ff-09a0-4c23-9c8b-8bbf8e198f12); 
INSERT INTO store.books_by_author (firstname, lastname, title, author_id) VALUES ('Adam', 'Samsung', 'Book 3', f8f5a9de-4bc6-4177-be64-87d0db7bf9be); 
INSERT INTO store.books_by_author (firstname, lastname, title, author_id) VALUES ('Ariel', 'Stein', 'Book 4', 9e390783-f4c2-4a6e-ac82-818d35cada68); 
INSERT INTO store.books_by_author (firstname, lastname, title, author_id) VALUES ('Ariel', 'Stein', 'Book 5', 9e390783-f4c2-4a6e-ac82-818d35cada68); 
