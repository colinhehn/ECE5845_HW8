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
    def keyword():
        keyword = input("Please enter a keyword (or a few, separated by spaces): ")
        query = ("MATCH (u:User)-[r:RATED]->(m:Movie)-[:IN_GENRE]->(g:Genre) "
                 "WHERE ALL (keyword IN $keywordsList WHERE m.title CONTAINS keyword) "
                 "WITH m, g, AVG(r.rating) AS avgRating, CASE WHEN EXISTS((u:User {userID: $input_user})-[:RATED]->(m)) THEN true ELSE false END AS seenMovie, r.rating AS userRating "
                 "RETURN m.title AS movieTitle, g.name AS genre, avgRating AS averageRating, seenMovie AS seenMovie, userRating AS userRating;"
                )

        result = sesh.run(query, input_user=user_id, keywordsList=keyword.split()).data()
        print('Listing movie titles containing keyword(s): ' + keyword + '.')
        for i in range(len(result)):
            print((str)(i+1) + ". " + result[i]['movieTitle'] + " (" + result[i]['genre'] + ", avg rating: " + (str)(result[i]['averageRating']) + ", seen: " + (str)(result[i]['seenMovie']) + ", your rating: " + (str)(result[i]['userRating']) + ")")
        return

    def recommendations():
        query = ("MATCH (u:User {userId: $input_user})-[gp:genre_pref]->(g:Genre) "
                 "WITH u, g, gp.preference AS pref "
                 "ORDER BY pref DESC "
                 "LIMIT 1 "
                 "MATCH (m:Movie)-[:IN_GENRE]->(g) "
                 "WHERE NOT EXISTS((u)-[:RATED]->(m)) "
                 "RETURN m.title AS recommendedMovies, m.movieId AS movieId, m.rating AS averageUserRating, m.numRatings AS numberOfRatings "
                 "ORDER BY m.rating DESC "
                 "LIMIT 5;"
                )
        result = sesh.run(query, input_user=user_id)
        print('Listing top 5 recommendations...')
        recommendations = result.data()
        for i in range(len(recommendations)):
            print((str)(i+1) + ". " + recommendations[i]['recommendedMovies'] + " (id: " + (str)(recommendations[i]['movieId']) + ")")
        return

    def rating():
        # User input.
        movie_id = input("Please enter a movie id: ")
        rating = input("Please enter a rating: ")
        rating = (float)(rating)
        if rating > 10 or rating < 0:
            print("Invalid rating. Please try again.")
            return
        
        # Add rating to database.
        sesh.run("MATCH (u:User {userId: $user_id}), (m:Movie {movieId: $movie_id}) CREATE (u)-[:RATED {rating: $rating}]->(m);", user_id=user_id, movie_id=movie_id, rating=rating)
        print("Rating added to database.")
        return
    
    # Infinite loop for user input. Use switch to delegate to appropriate functionality.
    while True:
        print()
        print("What would you like to do?")
        print("1. Search for a movie title.")
        print("2. Provide a rating for a movie.")
        print("3. Get recommendations.")
        print("4. Exit.")
        user_input = input("Please enter a number: ")
        while not user_input.isdigit():
            print("Invalid input. Please try again.")
            user_input = input("Please enter a number: ")
        print()

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