from neo4j import GraphDatabase as gd
import neo4j.exceptions

# Initialize Database Connection.
print('Initializing connection to Neo4j database...')
driver = gd.driver("neo4j://localhost:7687", auth=("neo4j", "hello"))
try:
    driver.verify_connectivity()
except neo4j.exceptions.ServiceUnavailable:
    print('Connection to Neo4j database failed. Please try again.')
    exit()
print('Connection to Neo4j database established.')
print('Initializing session...')

# Enter session for making some queries!
with driver.session() as sesh:
    print('Hello! Welcome to the Movie Recommendation System.')

    # User ID input validation.
    user_id = input("Please enter your user id (1-600): ")
    while not user_id.isdigit():
        print("Invalid user id. Please try again.")
        user_id = input("Please enter your user id (1-600): ")
    while (int(user_id) < 1 or int(user_id) > 600):
        print("Invalid user id. Please try again.")
        user_id = input("Please enter your user id (1-600): ")
    user_id = (int)(user_id)

    # Check if username exists under userId in database. If not, add it.
    username_query = sesh.run("MATCH (u:User) WHERE u.userId = $input_user RETURN u.name AS name;", input_user=user_id)
    username_record = username_query.single()

    # If username exists for ID, awesome. If not, add it.
    if username_record['name'] is None:
        username_input = input("No name was found for that ID. Please enter your name: ")
        sesh.run("MATCH (u:User {userId: $input_user}) SET u.name = $user_name;", input_user=user_id, user_name=username_input)
        print("Username added to database under ID: " + (str)(user_id) + ". Welcome, " + username_input + "!")
    else:
        print("Welcome, " + (str)(username_record['name']) + ".")

    # Initialize functions for switch statement below.
    '''
    Allow the user to search for a movie title (search for titles that contain the keyword(s)
    entered by the user). List the movie title, genre, average rating, and a flag whether the user has
    already seen it (rated it) and their rating for all matching movies.
    '''
    def keyword():
        keyword = input("Please enter a keyword (or a few, separated by spaces): ")
        query = (
        "MATCH (user:User {userId: $input_user})-[:RATED]->(ratedMovie:Movie)-[:IN_GENRE]->(genre:Genre), "
        "(movie:Movie)-[:IN_GENRE]->(genre)"
        "WHERE ANY(keyword IN $keywordsList WHERE toLower(movie.title) CONTAINS toLower(keyword))"
        "WITH movie, genre, AVG(ratedMovie.rating) AS avgRating, COUNT(ratedMovie) AS ratedCount"
        "RETURN movie.title AS title, genre.name AS genre, avgRating AS averageRating, "
        "CASE WHEN ratedCount > 0 THEN true ELSE false END AS userRated,"
        "COLLECT(ratedMovie.rating) AS userRatings")

        result = sesh.run(query, input_user=user_id, keywordsList=keyword.split())
        print('Listing movie titles containing keyword(s): ' + keyword + '.')
        print(result.data())
        return

    '''
    Provide the user with their top 5 recommendations.
    Execute query Q5.3 from Homework 7. Display the list of recommendations including movie id,
    title, average user rating (from all users), and number of ratings (from all users).
    '''
    def recommendations():
        query = ("WITH $input_user AS X "
                 "MATCH (u:User {userId: X})-[gp:genre_pref]->(g:Genre)"
                 "WITH u, g, gp.preference AS pref ORDER BY pref DESC LIMIT 1 "
                 "MATCH (u)-[:RATED]->(m:Movie)-[:IN_GENRE]->(g) "
                 "WHERE NOT EXISTS((u)-[:RATED]->(m)) "
                 "RETURN m.title AS recommendedMovies ORDER BY m.rating DESC LIMIT 5;")
                
        result = sesh.run(query, input_user=user_id)
        print('Listing top 5 recommendations...')
        print(result.data())
        return

    '''
    Allow the user to provide a rating for any of the previous recommendations (by
    providing the movie id and the rating). Your program should add the RATED relationship
    between the movie and the user to the database.
    '''
    def rating():
        movie_id = input("Please enter a movie id: ")
        rating = input("Please enter a rating: ")
        sesh.run("MATCH (u:User {userId: $user_id}), (m:Movie {id: $movie_id}) CREATE (u)-[:RATED {rating: $rating}]->(m);", user_id=user_id, movie_id=movie_id, rating=rating)
        print("Rating added to database.")
        return
    
    # Infinite loop for user input. Use switch to delegate to appropriate functionality.
    while True:
        print("1. Search for a movie title.")
        print("2. Provide a rating for a movie.")
        print("3. Get recommendations.")
        print("4. Exit.")
        user_input = input("Please enter a number: ")
        while not user_input.isdigit():
            print("Invalid input. Please try again.")
            user_input = input("Please enter a number: ")

        match user_input:
            case "1":
                keyword()
            case "2":
                rating()
            case "3":
                recommendations()
            case "4":
                print("Closing database connection...")
                break
            case _:
                print("Invalid input. Please try again.")

sesh.close()
driver.close()
print('Connection to Neo4j database closed. Goodbye!')