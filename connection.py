import pymysql.cursors


def connect_to_db(login_info):
    """
  function that connects to top_albums database
  """

    connection = pymysql.connect(host='localhost',
                                 user=login_info['username'],
                                 password=login_info['password'],
                                 database='top_albums',
                                 cursorclass=pymysql.cursors.DictCursor)
    return connection.cursor()
