from concurrent import futures
import grpc
import movie_pb2
import movie_pb2_grpc
from pymongo import MongoClient

class MovieRatingService(movie_pb2_grpc.MovieRatingServiceServicer):
    def __init__(self):
        # Connect to MongoDB
        self.client = MongoClient('mongodb://localhost:27017/')
        self.db = self.client['movie_ratings']
        self.collection = self.db['ratings']

    def GetRating(self, request, context):
        movie_id = request.movie_id
        rating = self.collection.find_one({"_id": movie_id}, {"rating": 1})
        if rating is not None:
            return movie_pb2.MovieRating(movie_id=movie_id, rating=rating["rating"])
        else:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details(f"Rating not found for movie with id {movie_id}")
            return movie_pb2.MovieRating()

    def AddRating(self, request, context):
        movie_id = request.movie_id
        rating = request.rating
        result = self.collection.insert_one({"_id": movie_id, "rating": rating})
        return movie_pb2.MovieRating(movie_id=movie_id, rating=rating)

    def UpdateRating(self, request, context):
        movie_id = request.movie_id
        rating = request.rating
        result = self.collection.update_one({"_id": movie_id}, {"$set": {"rating": rating}})
        if result.modified_count > 0:
            return movie_pb2.MovieRating(movie_id=movie_id, rating=rating)
        else:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details(f"Rating not found for movie with id {movie_id}")
            return movie_pb2.MovieRating()

    def DeleteRating(self, request, context):
        movie_id = request.movie_id
        result = self.collection.delete_one({"_id": movie_id})
        if result.deleted_count > 0:
            return movie_pb2.MovieRating(movie_id=movie_id)
        else:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details(f"Rating not found for movie with id {movie_id}")
            return movie_pb2.MovieRating()

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    movie_pb2_grpc.add_MovieRatingServiceServicer_to_server(MovieRatingService(), server)
    server.add_insecure_port('[::]:8000')
    server.start()
    server.wait_for_termination()

if __name__ == '__main__':
    serve()