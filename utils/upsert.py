from utils.utils import (
    SQLiteExecutor
)

def upsert(env, sqlite_temp_table, sqlite_table, df, columns):
    _sqlite_executor = SQLiteExecutor(env)
    engine = _sqlite_executor.engine
    with engine.begin() as con:
        # DELETE temp table
        query = 'DROP TABLE IF EXISTS `{temp}`;'.format(
            temp=sqlite_temp_table
        )
        con.execute(query)

        # Create temp table like target table to stage data for upsert
        query = "CREATE TABLE `{temp}` AS SELECT * FROM `{prod}` WHERE false;".format(
            temp=sqlite_temp_table, prod=sqlite_table
        )
        con.execute(query)

        # Insert dataframe into temp table
        df.to_sql(
            sqlite_temp_table,
            con,
            if_exists='append',
            index=False,
            method='multi'
        )

        # INSERT where the key doesn't match (new rows)
        query = "INSERT INTO `{prod}` SELECT * FROM `{temp}` WHERE `id` NOT IN (SELECT `id` FROM `{prod}`);".format(
            temp=sqlite_temp_table, prod=sqlite_table
        )
        con.execute(query)

        # Do an UPDATE ... JOIN to set all non-key columns of target to equal source
        set_expressions = []
        for column in columns:
            set_expression = "{column} = (SELECT {column} FROM {temp} WHERE id = {prod}.id)".format(
                column=column,
                temp=sqlite_temp_table,
                prod=sqlite_table
            )
            set_expressions.append(set_expression)
        set_expressions = ", ".join(set_expressions)
        query = """UPDATE
                        {prod}
                    SET {set_expressions}
                    where EXISTS (SELECT {columns}
                                FROM {temp}
                                WHERE id = {prod}.id)
                    ;""".format(
                        temp=sqlite_temp_table, prod=sqlite_table,
                        set_expressions=set_expressions,
                        columns=", ".join(columns)
                    )

        con.execute(query)

        # DELETE temp table
        query = 'DROP TABLE `{temp}`;'.format(
            temp=sqlite_temp_table
        )
        con.execute(query)
