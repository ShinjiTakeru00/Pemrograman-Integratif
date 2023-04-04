import mysql.connector
from concurrent import futures
import grpc
import movie_pb2
import movie_pb2_grpc

# MySQL database configuration
db_config = {
    'user': 'root',
    'password': '',
    'host': 'localhost',
    'database': 'ratingsdb'
}

class MovieRatingService(movie_pb2_grpc.MovieRatingServiceServicer):
    def __init__(self):
        self.db = mysql.connector.connect(
            host="localhost",
            user="root",
            password="",
            database="ratingsdb"
        )
        
    def GetRating(self, request, context):
        movie_id = request.movie_id
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()
        query = "SELECT rating FROM ratings WHERE movie_id=%s"
        cursor.execute(query, (movie_id,))
        result = cursor.fetchone()
        if result is not None:
            rating = result[0]
            return movie_pb2.MovieRating(movie_id=movie_id, rating=rating)
        else:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details(f"Rating not found for movie with id {movie_id}")
            return movie_pb2.MovieRating()

    def AddRating(self, request, context):
        try:
            cursor = self.db.cursor()

            # Check if rating already exists for movie ID
            cursor.execute("SELECT * FROM ratings WHERE movie_id=%s", (request.movie_id,))
            result = cursor.fetchone()

            if result:
                # Update existing rating
                cursor.execute("UPDATE ratings SET rating=%s WHERE movie_id=%s", (request.rating, request.movie_id))
                self.db.commit()
            else:
                # Insert new rating
                cursor.execute("INSERT INTO ratings (movie_id, rating) VALUES (%s, %s)", (request.movie_id, request.rating))
                self.db.commit()

            cursor.close()

            return movie_pb2.MovieRating(movie_id=request.movie_id, rating=request.rating)
        except Exception as e:
            print(e)
            context.set_details(str(e))
            context.set_code(grpc.StatusCode.UNKNOWN)
            return movie_pb2.MovieRating()


    def UpdateRating(self, request, context):
        movie_id = request.movie_id
        rating = request.rating
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()
        query = "UPDATE ratings SET rating=%s WHERE movie_id=%s"
        cursor.execute(query, (rating, movie_id))
        conn.commit()
        if cursor.rowcount == 0:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details(f"Rating not found for movie with id {movie_id}")
            return movie_pb2.MovieRating()
        else:
            return movie_pb2.MovieRating(movie_id=movie_id, rating=rating)

    def DeleteRating(self, request, context):
        movie_id = request.movie_id
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()
        query = "DELETE FROM ratings WHERE movie_id=%s"
        cursor.execute(query, (movie_id,))
        conn.commit()
        if cursor.rowcount == 0:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details(f"Rating not found for movie with id {movie_id}")
            return movie_pb2.MovieRating()
        else:
            return movie_pb2.MovieRating(movie_id=movie_id)

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    movie_pb2_grpc.add_MovieRatingServiceServicer_to_server(MovieRatingService(), server)
    server.add_insecure_port('[::]:5000')
    server.start()
    server.wait_for_termination()

if __name__ == '__main__':
    serve()
