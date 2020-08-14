import psycopg2


class Database:
    def __init__(self, host, database, user, password):
        self.connection = None
        self.connect(host, database, user, password)

    def connect(self, host, database, user, password):
        self.connection = None
        try:
            self.connection = psycopg2.connect(
                host=host,
                database=database,
                user=user,
                password=password
            )
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
            raise error

    def execute(self, command):
        if self.connection is not None:
            cur = self.connection.cursor()
            try:
                cur.execute(command)
                self.connection.commit()
            except (Exception, psycopg2.DatabaseError) as error:
                print(error)
                raise error
        else:
            raise Exception('You must be connected to a database in order to run a command !')

    def close(self):
        if self.connection is not None:
            self.connection.close()
            print('Database connection closed.')
