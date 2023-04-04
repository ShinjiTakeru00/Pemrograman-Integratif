import grpc
import movie_pb2
import movie_pb2_grpc
import mysql.connector

def run():
    # Connect to the gRPC server
    with grpc.insecure_channel('localhost:5000') as channel:
        stub = movie_pb2_grpc.MovieRatingServiceStub(channel)

        # Add a new rating
        response = stub.AddRating(movie_pb2.MovieRating(movie_id=10, rating=4))
        print(f"Added movie rating for id {response.movie_id} with rating {response.rating}")

        # Get a rating
        response = stub.GetRating(movie_pb2.MovieId(movie_id=8))
        if response.rating:
            print(f"Rating for movie with id {response.movie_id} is {response.rating}")
        else:
            print(f"Rating not found for movie with id {response.movie_id}")

        # Update a rating
        response = stub.UpdateRating(movie_pb2.MovieRating(movie_id=9, rating=5))
        if response.rating:
            print(f"Updated movie rating for id {response.movie_id} to {response.rating}")
        else:
            print(f"Rating not found for movie with id {response.movie_id}")

        # Delete a rating
        response = stub.DeleteRating(movie_pb2.MovieId(movie_id=4))
        if response.rating:
            print(f"Deleted movie rating for id {response.movie_id} with rating {response.rating}")
        else:
            print(f"Rating not found for movie with id {response.movie_id}")

if __name__ == '__main__':
    run()
