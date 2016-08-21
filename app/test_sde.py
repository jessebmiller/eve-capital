from app import sde
import sqlite3
import unittest


class StaticDataExportTests(unittest.TestCase):
    """
    Assuming the sde database is around
    the sde module provides an interface to it.

    with sde.cursor() as cur:
        cur.execute(sql, args)
        cur.fetchone()

    """

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_sde_provides_sql_context(self):
        """ sde should provide a cursor context """
        with sde.cursor(sqlite3, "") as cur:
            cur.execute("select min(2, 3)")
            self.assertEqual(cur.fetchone(), (2,))

    def test_can_pass(self):
        self.assertTrue(True)


if __name__ == '__main__':
        unittest.main()
