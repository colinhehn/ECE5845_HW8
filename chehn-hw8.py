from neo4j import GraphDatabase as gd

driver = gd.driver("neo4j://localhost:7687", auth=("neo4j", "123456"))
driver.verify_connectivity()

with driver.session(database='movies') as sesh:
    sesh.run("MATCH (m:Movie) RETURN m LIMIT 10;")
    '''
    Step 1. Ask the user for their id X (value between 1-600).
    Search for the user node with id = X. If there is a name property, display: “Welcome,
    <username>!”. If there is no name property, ask the user for their name and add it to the
    database.
    '''
    user_id = input("Please enter your user id (1-600): ")
    sesh.run("MATCH (u:User {id: $user_id}) RETURN u.name;", user_id=user_id)
    if sesh.evaluate("MATCH (u:User {id: $user_id}) RETURN u.name;", user_id=user_id) is None:
        user_name = input("Please enter your name: ")
        sesh.run("MATCH (u:User {id: $user_id}) SET u.name = $user_name;", user_id=user_id, user_name=user_name)
    else:
        print("Welcome, " + sesh.evaluate("MATCH (u:User {id: $user_id}) RETURN u.name;", user_id=user_id) + "!")

    '''
    Step 2. Allow the user to search for a movie title (search for titles that contain the keyword(s)
    entered by the user). List the movie title, genre, average rating, and a flag whether the user has
    already seen it (rated it) and their rating for all matching movies.
    '''
    keyword = input("Please enter a keyword: ")
    sesh.run("MATCH (m:Movie) WHERE m.title CONTAINS $keyword RETURN m.title, m.genre, m.avgRating, m.numRatings;", keyword=keyword)

    '''
    Step 3. Provide the user with their top 5 recommendations.
    Execute query Q5.3 from Homework 7. Display the list of recommendations including movie id,
    title, average user rating (from all users), and number of ratings (from all users).
    '''
    sesh.run("WITH 1 AS X MATCH (u:User {userId: X})-[gp:genre_pref]->(g:Genre) WITH u, g, gp.preference AS pref ORDER BY pref DESC LIMIT 1 MATCH (m:Movie)-[:IN_GENRE]->(g) WHERE NOT EXISTS((u)-[:RATED]->(m)) RETURN m.title AS recommendedMovies ORDER BY m.rating DESC LIMIT 5;")

    '''
    Step 4. Allow the user to provide a rating for any of the previous recommendations (by
    providing the movie id and the rating). Your program should add the RATED relationship
    between the movie and the user to the database.
    '''
    movie_id = input("Please enter a movie id: ")
    rating = input("Please enter a rating: ")
    sesh.run("MATCH (u:User {id: $user_id}), (m:Movie {id: $movie_id}) CREATE (u)-[:RATED {rating: $rating}]->(m);", user_id=user_id, movie_id=movie_id, rating=rating)


'''
Step 5. Make sure to close the connection(s) before your program ends.
'''
driver.close()