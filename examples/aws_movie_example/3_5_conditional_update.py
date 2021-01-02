from awstin.dynamodb import DynamoDB

from models import Movie


def remove_actors(title, year, actor_count):
    dynamodb = DynamoDB()
    table = dynamodb[Movie]

    response = table.update_item(
        (year, title),
        update_expression=Movie.info.actors[0].remove(),
        condition_expression=Movie.info.actors.size() >= actor_count,
    )
    return response


if __name__ == '__main__':
    update_response = remove_actors("The Big New Movie", 2015, 3)
    if update_response:
        print("Updated")
        print(update_response.serialize())
    else:
        print("Not Updated")
