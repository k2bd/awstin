from awstin.dynamodb import DynamoDB

from models import Movie


def delete_underrated_movie(title, year, rating):
    dynamodb = DynamoDB()
    table = dynamodb[Movie]

    return table.delete_item(
        (year, title),
        condition_expression=Movie.info.rating <= rating,
    )


if __name__ == '__main__':
    print("Attempting a conditional delete...")
    deleted = delete_underrated_movie("The Big New Movie", 2015, 5)
    if deleted:
        print("Deleted film")
    else:
        print("Did not delete film")
