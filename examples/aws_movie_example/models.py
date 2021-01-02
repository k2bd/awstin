from awstin.dynamodb import Attr, DynamoModel, Key


class Movie(DynamoModel):
    _table_name_ = "Movies"

    #: Year the film  was made (hash key)
    year = Key()

    #: Title of the film (sort key)
    title = Key()

    #: Additional information about the film
    info = Attr()
