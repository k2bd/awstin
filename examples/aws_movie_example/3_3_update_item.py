from awstin.dynamodb import DynamoDB

from models import Movie


def update_movie(title, year, rating, plot, actors):
    dynamodb = DynamoDB()
    table = dynamodb[Movie]

    update_expression = (
        Movie.info.rating.set(rating)
        & Movie.info.plot.set(plot)
        & Movie.info.actors.set(actors)
    )

    return table.update_item(
        key=(year, title),
        update_expression=update_expression,
    )


if __name__ == '__main__':
    update_response = update_movie(
        "The Big New Movie", 2015, 5.5, "Everything happens all at once.",
        ["Larry", "Moe", "Curly"])
    print("Update movie succeeded:")
    print(update_response.serialize())
