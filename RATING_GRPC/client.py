import grpc
import movie_pb2
import movie_pb2_grpc
from pymongo import MongoClient

def run():
    # Connect to gRPC server
    with grpc.insecure_channel('localhost:8000') as channel:
        stub = movie_pb2_grpc.MovieRatingServiceStub(channel)

        # Connect to MongoDB
        client = MongoClient('mongodb://localhost:3000/')
        db = client['movie_ratings']
        collection = db['ratings']

        # Create
        response = stub.AddRating(movie_pb2.MovieRating(movie_id=5, rating=4.2))
        print(f"Added movie rating with id {response.movie_id} and rating {response.rating}")
        collection.insert_one({"_id": response.movie_id, "rating": response.rating})

        # Read
        response = stub.GetRating(movie_pb2.MovieId(movie_id=1))
        if response.rating:
            print(f"Movie rating for id {response.movie_id} is {response.rating}")
        else:
            print(f"Rating not found for movie with id {response.movie_id}")

        # Update
        response = stub.UpdateRating(movie_pb2.MovieRating(movie_id=5, rating=4.8))
        if response.rating:
            print(f"Updated movie rating for id {response.movie_id}")
            collection.update_one({"_id": response.movie_id}, {"$set": {"rating": response.rating}})
        else:
            print(f"Rating not found for movie with id {response.movie_id}")

        # Delete
        response = stub.DeleteRating(movie_pb2.MovieId(movie_id=5))
        if response.rating:
            print(f"Deleted movie rating for id {response.movie_id} with rating {response.rating}")
            collection.delete_one({"_id": response.movie_id})
        else:
            print(f"Rating not found for movie with id {response.movie_id}")

if __name__ == '__main__':
    run()