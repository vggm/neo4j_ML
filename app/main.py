import os
import pandas as pd
from neo4j import GraphDatabase
from rich import print as rprint
from AccessMethodsForNeo4jBD import *

URI = "neo4j://localhost:7999"
AUTH = ("neo4j", "password")


def clear_console():
  # Detectar el sistema operativo
  if os.name == 'nt':  # Si el sistema operativo es Windows
    os.system('cls')
  else:  # Si el sistema operativo es macOS o Linux
    os.system('clear')


def login() -> str:
  confirm = 'n'
  while confirm not in ['y', '']:
    user = input("Introduce tu usuario: ")
    rprint(f" - Tu usuario es: {user}? (y/n)")
    confirm = input()
  


  query = """
    MERGE (u:User {name: $username})
    RETURN u IS NOT NULL AS user_exists
  """

  result = session.run(query, username=user)
  user_exists = result.single()["user_exists"]

  if not user_exists:
    rprint(f" - Creando usuario...")
  
  rprint(f"--> Sesion iniciada con el usuario {user}!")
  return user


def choose_opt() -> int:
  clear_console()
  total_options = 10
  actions_msg = """
   - Que opcion desea tomar?
      1. Ver amigos
      2. Ver likes a libros
      3. Vincular a un amigo
      4. Borrar amigo
      5. Consultar titulo de un libro
      6. Consultar libros de un autor
      7. Dar Like a un libro
      8. Libros recomendados por amigos
      9. Todos los usuarios
      0. Salir
  """
  opt = -1
  while opt not in list(range(total_options)):
    rprint(actions_msg)
    opt = input()
    try:
      opt = int(opt)
    except:
      rprint("Debe introducir un numero!\n")
      continue
  return opt


def make_action(user: str, opt: int):
  match opt:
    case 1:
      rprint(f"Amigos del usuario: {user}")
      show_friends(user, session)
    case 2:
      rprint(f"Libros favoritos del usuario: {user}")
      show_likes_from_user(user, session)
    case 3:
      friend = input("Introduce el nombre de tu amigo:\n")
      create_friend_relation(user, friend, session)
    case 4:
      friend = input("Introduce el nombre de tu amigo:\n")
      delete_friend_relation(user, friend, session)
    case 5:
      book_title = input("Introduce el titulo del libro:\n")
      if not book_title:
        return
      show_book_info(book_title, session)
    case 6:
      author = input("Introduce el nombre del autor:\n")
      if not author:
        return
      show_books_from_author(author, session)
    case 7:
      book_title = input("Introduce el titulo del libro:\n")
      like_book(user, book_title, session)
    case 8:
      limit = int(input("Limite de libros: "))
      rprint(f"Libros recomendados por amigos:")
      show_friend_recomendations(user, session, limit)
    case 9:
      rprint(f"Todos los usuarios:")
      show_users(session)
  input("\nPresiona enter para continuar...")


def main():
  user = login()
  opt_chosen = choose_opt()
  while opt_chosen != 0:
    make_action(user, opt_chosen)
    opt_chosen = choose_opt()


if __name__ == "__main__":
  print("# Conectando con Neo4j DB...")
  with GraphDatabase.driver(URI, auth=AUTH) as driver:
    try:
      driver.verify_connectivity()
    except:
      print("- No pudo conectarse a la Base de Datos")
      exit(1)
    with driver.session() as session:
      main()
      print("# Desconectando...")