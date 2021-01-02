from awstin.dynamodb import DynamoDB

from models import Movie


def put_movie(title, year, plot, rating):
    dynamodb = DynamoDB()
    table = dynamodb[Movie]

    movie = Movie(
        title=title,
        year=year,
        info={
            'plot': plot,
            'rating': rating,
        },
    )
    response = table.put_item(movie)
    return response


if __name__ == '__main__':
    movie_resp = put_movie(
        "The Big New Movie",
        2015,
        "Nothing happens at all.",
        0,
    )
    print("Put movie succeeded:")
    print(movie_resp)
