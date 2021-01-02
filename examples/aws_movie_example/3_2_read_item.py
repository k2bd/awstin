from awstin.dynamodb import DynamoDB

from models import Movie


def get_movie(title, year):
    dynamodb = DynamoDB()
    table = dynamodb[Movie]

    item: Movie = table[year, title]

    return item


if __name__ == '__main__':
    movie = get_movie("The Big New Movie", 2015)
    if movie:
        print("Get movie succeeded:")
        print(type(movie))
        print(movie.serialize())
