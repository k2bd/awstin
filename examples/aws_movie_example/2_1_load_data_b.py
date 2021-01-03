import json

from models import Movie

from awstin.dynamodb import DynamoDB


def load_movies(movies):
    dynamodb = DynamoDB()
    table = dynamodb[Movie]

    for movie in movies:
        table.put_item(movie)


if __name__ == "__main__":
    with open("moviedata.json", "r") as json_file:
        movie_list = json.load(json_file)
    load_movies([Movie.deserialize(movie) for movie in movie_list])
