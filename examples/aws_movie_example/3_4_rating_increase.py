from awstin.dynamodb import DynamoDB

from models import Movie


def increase_rating(title, year, rating_increase):
    dynamodb = DynamoDB()
    table = dynamodb[Movie]

    response = table.update_item(
        (year, title),
        update_expression=Movie.info.rating.set(Movie.info.rating + rating_increase),
    )
    return response


if __name__ == "__main__":
    update_response = increase_rating("The Big New Movie", 2015, 1)
    print("Update movie succeeded:")
    print(update_response.serialize())
