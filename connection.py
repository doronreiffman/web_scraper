import pymysql.cursors


def connect_to_db():
    """
  function that connects to top_albums database
  """

    connection = pymysql.connect(host='localhost',
                                 user='root',
                                 password='Reiffman8',
                                 database='top_albums',
                                 cursorclass=pymysql.cursors.DictCursor)
    return connection.cursor()
