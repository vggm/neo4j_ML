import pandas as pd
from neo4j import Session
from rich import print as rprint

def insert_user(name: str, session: Session) -> bool:
  # Insertar usuario
  result = session.run(f"MATCH (u:User) WHERE u.name = $name RETURN u.name as name", {"name": name})
  if len(result.data()) == 0:
    try:
      session.run("CREATE (u:User {name: $name})", {"name": name})
      return True
    except:
      pass
  return False

def show_users(session: Session):
  # Mostrar usuarios
  result = session.run("MATCH (u:User) RETURN u.name AS name")
  for i, record in enumerate(result):
    print(f"{i}> {record["name"]}")

def show_connections(session: Session):
  # Mostrar vinculos
  print("\nVinculos:")
  result = session.run("MATCH (u:User)-[r:FRIEND]->(u2:User) RETURN u.name AS name, u2.name AS name2")
  for record in result:
    print(f" - {record['name']} es amigo de {record['name2']}")

def insert_author(author: str, session: Session) -> bool:
  # Insertar autor
  result = session.run("MATCH (a:Author) WHERE a.name = $name RETURN a.name as name", {"name": author})
  if len(result.data()) == 0:
    try:
      session.run("CREATE (a:Author {name: $name})", {"name": author})
      return True
    except:
      pass
  return False

def show_authors(session: Session):
  # Mostrar autores
  print("Autores:")
  result = session.run("MATCH (a:Author) RETURN a.name AS name")
  for record in result:
    print(f" - {record["name"]}")

def insert_genre(genre: str, session: Session) -> bool:
  # Insertar Genero
  result = session.run("MATCH (g:Genre) WHERE g.name = $name RETURN g.name as name", {"name": genre})
  if len(result.data()) == 0:
    try:
      session.run("CREATE (g:Genre {name: $name})", {"name": genre})
      return True
    except:
      pass
  return False

def show_genres(session: Session):
  # Mostrar generos
  print("Generos:")
  result = session.run("MATCH (g:Genre) RETURN g.name AS name")
  for record in result:
    print(f" - {record["name"]}")

def insert_books(books: pd.DataFrame, session: Session) -> bool:
  # Insertar libros
  query = """
    UNWIND $books AS book
    MERGE (b:Book {title: book.title})
    ON CREATE SET b.author = book.author, 
                  b.pages = book.pages, b.rating = book.rating, b.likedPercent = book.likedPercent, 
                  b.numRating = book.numRating, b.price = book.price, b.publishDate = book.publishDate, 
                  b.publishYear = book.publishYear
    """
  
  try:
    books_dict = books.to_dict('records')
    session.run(query, {"books": books_dict})
    return True
  except:
    pass
  return False

def get_random_books(n: int, session: Session):
  # Funcion para obtener n random books
  query = f"""
    MATCH (b:Book)
    RETURN b.title AS title, b.author AS author, b.pages AS pages,
           b.rating AS rating, b.likedPercent AS likedPercent, b.numRating AS numRating,
           b.price AS price, b.publishDate AS publishDate, b.publishYear AS publishYear
    ORDER BY rand()
    LIMIT {n}
    """
  result = session.run(query)
  books = result.data()
  return books

def friendship_relation(tx, user_a: str, user_b: str, session: Session):
  # Funcion para enlazar usuarios como amigos
  query = (
      "MATCH (u1:User {name: $name_a}), (u2:User {name: $name_b}) "
      "CREATE (u1)-[:FRIEND]->(u2)"
  )
  tx.run(query, name_a=user_a, name_b=user_b)

def book_genre_relation(tx, book_title: str, genres: list[str], session: Session):
  # Funcion para enlazar libro con su genero
  for genre in genres:
    query = (
              "MATCH (b:Book {title: $book_title}), (g:Genre {name: $genre}) "
              "CREATE (b)-[:BELONGS_TO]->(g)"
          )
    tx.run(query, book_title=book_title, genre=genre)

def author_book_relation(tx, author_name: str, book_titles: list[str], session: Session):
  # Funcion para enlazar autor con su obra
  for book_title in book_titles:
    query = (
              "MATCH (b:Book {title: $book_title}), (a:Author {name: $name}) "
              "CREATE (a)-[:WROTE]->(b)"
          )
    tx.run(query, book_title=book_title, name=author_name)

def user_book_likes_relation(tx, user_name: str, book_title: str, session: Session):
  # Funcion para enlazar libros con usuarios mediante un like
  query = (
    "MATCH (b:Book {title: $title}), (u:User {name: $name}) "
    "CREATE (u)-[:LIKES]->(b)"
  )
  tx.run(query, title=book_title, name=user_name)

def make_relation(relation, org: str, dst: str | list[str], session: Session):
  # Crear relacion dependiendo de la funcion que se le pase
  session.execute_write(relation, org, dst)

def show_friend_recomendations(user: str, session: Session, limit=5):
  # Recomendar libros en base a los gustos de los amigos
  query = (
    "MATCH (u:User {name: $user_name})-[:FRIEND]->()-[:LIKES]->(b:Book) "
    "WHERE NOT (u)-[:LIKES]->(b) "
    "RETURN b.title AS title, COUNT(*) AS friend_likes "
    "ORDER BY friend_likes DESC "
    "LIMIT $books_limit"
  )

  try:
    result = session.run(query, user_name=user, books_limit=limit)
    for i, record in enumerate(result, start=1):
      rprint(f"{i}> Title: {record["title"]} - Friend's likes: {record["friend_likes"]}")
  except:
    return None
  
def show_friends(user: str, session: Session):
  query = """
    MATCH (u1:User)-[:FRIEND]->(u2:User)
    WHERE u1.name = $username
    RETURN u2.name AS friend
    ORDER BY friend DESC
  """
  result = session.run(query, username=user)
  for i, record in enumerate(result, start=1):
    rprint(f"{i}> {record["friend"]}")

def show_books_from_author(author: str, session: Session):
  query = """
    MATCH (a:Author)-[:WROTE]->(b:Book)
    WHERE toLower(a.name) CONTAINS toLower($author_name)
    RETURN b.title as title, b.rating as rating
  """
  result = session.run(query, author_name=author)
  for i, record in enumerate(result, start=1):
    rprint(f"{i}> {record["title"]} ({record["rating"]})")

def show_book_info(book: str, session: Session):
  query = """
    MATCH (b:Book)
    WHERE toLower(b.title) CONTAINS toLower($book_title)
    RETURN b.title as title, b.pages as pages, b.rating as rating,
           b.author as author, b.publishDate as date, b.price as price,
           b.likedPercent as liked
  """
  result = session.run(query, book_title=book)
  for i, record in enumerate(result, start=1):
    rprint(f"- [ {i} ] -")
    rprint(f"""
      title:  {record["title"]}
      author: {record["author"]}
      rating: {record["rating"]}
      liked:  {record["liked"]}
      pages:  {record["pages"]}
      price:  {record["price"]}
      date:   {record["date"]}\n
    """)

def like_book(user: str, book: str, session: Session):
  query = """
    MATCH (b:Book {title: $book_title})
    RETURN b
  """

  result = session.run(query, book_title=book)
  if result.single() is None:
    rprint(f"El libro con el nombre: {book}, no existe.")
    return

  query = """
    MATCH (u:User {name: $user_name}), (b:Book {title: $book_title})
    CREATE (u)-[:LIKES]->(b)
  """
  result = session.run(query, book_title=book, user_name=user)
  rprint("Like establecido!")

def show_likes_from_user(user: str, session: Session):
  query = """
    MATCH (u:User {name: $user_name})-[:LIKES]->(b:Book)
    RETURN b.title as title, b.author as author
  """
  result = session.run(query, user_name=user)
  for i, record in enumerate(result, start=1):
    rprint(f"{i}> {record["title"]} - {record["author"]}")

def create_friend_relation(user: str, friend: str, session: Session):
  query = """
    MATCH (u:User {name: $username})
    RETURN u
  """
  result = session.run(query, username=friend)
  if result.single() is None:
    rprint(f"El usuario: {friend}, no existe!")
    return
  
  query = """
    MATCH (u:User {name: $username}), (friend:User {name: $friendname})
    CREATE (u)-[:FRIEND]->(friend)
  """

  session.execute_write(lambda tx: tx.run(query, username=user, friendname=friend))
  print("Amistad establecida!")

def delete_friend_relation(user: str, friend: str, session: Session):
  query = """
    MATCH (u:User {name: $username})
    RETURN u
  """
  result = session.run(query, username=friend)
  if result.single() is None:
    rprint(f"El usuario: {friend}, no existe!")
    return
  
  query = """
    MATCH (u:User {name: $username})-[r:FRIEND]->(friend:User {name: $friendname})
    DELETE r
  """

  session.execute_write(lambda tx: tx.run(query, username=user, friendname=friend))
  print("Amistad eliminada!")