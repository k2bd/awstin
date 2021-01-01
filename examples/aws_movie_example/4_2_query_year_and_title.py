from awstin.dynamodb import DynamoDB

from models import Movie


def query_and_project_movies(year, title_range):
    dynamodb = DynamoDB()
    table = dynamodb[Movie]

    query = (
        (Movie.year == year)
        & (Movie.title.between(*title_range))
    )

    return table.query(query)


if __name__ == '__main__':
    query_year = 1992
    query_range = ('A', 'L')
    print(
        f"Get movies from {query_year} with titles from "
        f"{query_range[0]} to {query_range[1]}"
    )
    movies = query_and_project_movies(query_year, query_range)
    for movie in movies:
        print(f"{movie.year} : {movie.title}")
        print(movie.info)
        print()
