// Connect to database
const db = connect('mongodb://localhost:27017/plp_bookstore');
db = db.getSiblingDB('plp_bookstore');

// ========== TASK 2: BASIC CRUD OPERATIONS ==========

// 1. Find all books in a specific genre
function findBooksByGenre(genre) {
  return db.books.find({ genre: genre });
}

// 2. Find books published after a certain year
function findBooksAfterYear(year) {
  return db.books.find({ published_year: { $gt: year } });
}

// 3. Find books by a specific author
function findBooksByAuthor(author) {
  return db.books.find({ author: author });
}

// 4. Update the price of a specific book
function updateBookPrice(title, newPrice) {
  return db.books.updateOne(
    { title: title },
    { $set: { price: newPrice } }
  );
}

// 5. Delete a book by its title
function deleteBookByTitle(title) {
  return db.books.deleteOne({ title: title });
}

// ========== TASK 3: ADVANCED QUERIES ==========

// 1. Find books in stock and published after 2010
function findInStockAfter2010() {
  return db.books.find({
    in_stock: true,
    published_year: { $gt: 2010 }
  });
}

// 2. Use projection to return only title, author, and price
function findBooksWithProjection() {
  return db.books.find(
    {},
    { title: 1, author: 1, price: 1, _id: 0 }
  );
}

// 3. Sorting by price (ascending and descending)
function sortBooksByPriceAsc() {
  return db.books.find().sort({ price: 1 });
}

function sortBooksByPriceDesc() {
  return db.books.find().sort({ price: -1 });
}

// 4. Pagination - 5 books per page
function getBooksPage(pageNumber) {
  const booksPerPage = 5;
  const skip = (pageNumber - 1) * booksPerPage;
  
  return db.books.find()
    .skip(skip)
    .limit(booksPerPage)
    .sort({ title: 1 });
}

// ========== TASK 4: AGGREGATION PIPELINE ==========

// 1. Average price by genre
function averagePriceByGenre() {
  return db.books.aggregate([
    {
      $group: {
        _id: "$genre",
        averagePrice: { $avg: "$price" },
        bookCount: { $sum: 1 }
      }
    },
    {
      $sort: { averagePrice: -1 }
    }
  ]);
}

// 2. Author with the most books
function authorWithMostBooks() {
  return db.books.aggregate([
    {
      $group: {
        _id: "$author",
        bookCount: { $sum: 1 }
      }
    },
    {
      $sort: { bookCount: -1 }
    },
    {
      $limit: 1
    }
  ]);
}

// 3. Group books by publication decade
function booksByPublicationDecade() {
  return db.books.aggregate([
    {
      $project: {
        title: 1,
        published_year: 1,
        decade: {
          $subtract: [
            "$published_year",
            { $mod: ["$published_year", 10] }
          ]
        }
      }
    },
    {
      $group: {
        _id: "$decade",
        bookCount: { $sum: 1 },
        books: { $push: "$title" }
      }
    },
    {
      $sort: { _id: 1 }
    }
  ]);
}

// ========== TASK 5: INDEXING ==========

// 1. Create index on title field
function createTitleIndex() {
  return db.books.createIndex({ title: 1 });
}

// 2. Create compound index on author and published_year
function createAuthorYearIndex() {
  return db.books.createIndex({ author: 1, published_year: -1 });
}

// 3. Demonstrate performance improvement with explain()
function demonstrateIndexPerformance() {
  // Without index (if index wasn't created)
  const withoutIndex = db.books.find({ title: "The Great Gatsby" }).explain("executionStats");
  
  // With index
  createTitleIndex();
  const withIndex = db.books.find({ title: "The Great Gatsby" }).explain("executionStats");
  
  return {
    withoutIndex: withoutIndex.executionStats,
    withIndex: withIndex.executionStats
  };
}

// ========== EXECUTE AND DISPLAY RESULTS ==========

print("=== BOOKS BY GENRE (Fantasy) ===");
printjson(findBooksByGenre("Fantasy").toArray());

print("\n=== BOOKS PUBLISHED AFTER 2000 ===");
printjson(findBooksAfterYear(2000).toArray());

print("\n=== BOOKS BY J.K. ROWLING ===");
printjson(findBooksByAuthor("J.K. Rowling").toArray());

print("\n=== IN STOCK AND PUBLISHED AFTER 2010 ===");
printjson(findInStockAfter2010().toArray());

print("\n=== BOOKS WITH PROJECTION (Title, Author, Price) ===");
printjson(findBooksWithProjection().toArray());

print("\n=== AVERAGE PRICE BY GENRE ===");
printjson(averagePriceByGenre().toArray());

print("\n=== AUTHOR WITH MOST BOOKS ===");
printjson(authorWithMostBooks().toArray());

print("\n=== BOOKS BY PUBLICATION DECADE ===");
printjson(booksByPublicationDecade().toArray());

print("\n=== PAGINATION (Page 1) ===");
printjson(getBooksPage(1).toArray());

print("\n=== INDEX PERFORMANCE COMPARISON ===");
printjson(demonstrateIndexPerformance());

// Update and delete examples
print("\n=== UPDATING BOOK PRICE ===");
printjson(updateBookPrice("The Great Gatsby", 15.99));

print("\n=== DELETING A BOOK ===");
// Uncomment to test delete function
// printjson(deleteBookByTitle("1984"));