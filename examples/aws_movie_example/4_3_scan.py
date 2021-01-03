from models import Movie

from awstin.dynamodb import DynamoDB


def scan_movies(year_range, display_movies):
    dynamodb = DynamoDB()
    table = dynamodb[Movie]

    display_movies(table.scan(Movie.year.between(*year_range)))


if __name__ == "__main__":

    def print_movies(movies):
        for movie in movies:
            print(f"{movie.year} : {movie.title}")
            print(movie.info)
            print()

    query_range = (1950, 1959)
    print(f"Scanning for movies released from {query_range[0]} to {query_range[1]}...")
    scan_movies(query_range, print_movies)
