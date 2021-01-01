from awstin.dynamodb import DynamoDB

from models import Movie


def query_movies(year):
    dynamodb = DynamoDB()
    table = dynamodb[Movie]

    return table.query(Movie.year == year)


if __name__ == '__main__':
    query_year = 1985
    print(f"Movies from {query_year}")
    movies = query_movies(query_year)
    for movie in movies:
        print(movie.year, ": ", movie.title)
