import pyodbc
import json
import openai
import pandas as pd


class NlToSql:

    def __init__(self, database_name):
        self.database_name = database_name
        self.conn, self.db_type = self._connect_to_db()
        self.cursor = self.conn.cursor()



    # With a NL question as input, return a SQL query as output
    def get_sql(self, question):
        prompt = self._make_db_schema_prompt()
        prompt += question
        response = self._call_gpt(prompt)
        return response
    


    # With a SQL statement, return a pandas dataframe from the database
    def get_df(self, query):
        return self._do_query(query)
    


    # With a NL question as input, return a pandas dataframe from the database
    def get_df_from_question(self, question):
        query = self.get_sql(question)
        return self.get_df(query)



    def _make_db_schema_prompt(self):

        if self.db_type == 'mssql':
            query_string = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE';"
        elif self.db_type == 'mysql':
            query_string = "SHOW TABLES;"
        self.cursor.execute(query_string)
        tables_and_columns = {}
        for row in self.cursor:
            tables_and_columns[row[0]] = []

        for table in tables_and_columns:
            if self.db_type == 'mssql':
                query_string = "SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '%s';" % table
            elif self.db_type == 'mysql':
                query_string = "SHOW COLUMNS FROM %s;" % table

            self.cursor.execute(query_string)
            for row in self.cursor:
                tables_and_columns[table].append(row[0] + " " + row[1])

        prompt = "For the database described next, provide only a sql query. do not include any text that is not valid SQL.\n"
        prompt += "#\n#Database: " + self.database_name + "\n#\n"
        prompt += "#{} tables, with their properties:\n#\n".format(self.db_type)
        for table in tables_and_columns:
            prompt += ("#" + table + "(")
            for column in tables_and_columns[table]:
                prompt += (column + ',')
            prompt = prompt[ : len(prompt) - 1]
            prompt += ')\n'
        prompt += "#\n### a sql query to answer the question: "

        return prompt



    def _do_query(self, query):
        query_string = query
        self.cursor.execute(query_string)
        
        columns = [column[0] for column in self.cursor.description]

        #make sure column names are unique
        for i in range(0, len(columns)):
            if i > 0:
                if columns[i] in columns[:i]:
                    columns[i] = columns[i] + str(i)

        result_dict = {}
        for column in columns:
            result_dict[column] = []

        for row in self.cursor:
            for i in range(len(columns)):
                result_dict[columns[i]].append(row[i])

        try:
            df = pd.DataFrame(result_dict)
        except ValueError as e:
            print("Encountered a problem attempting to translate results to a DataFrame")
            print(e)
            for key in result_dict:
                print(key)
                print(len(result_dict[key]))
            df = pd.DataFrame()

        return df



    def _connect_to_db(self):
        database = self.database_name
        db_list = json.load(open('.local/dbinfo.json'))
        for entry in db_list:
            if entry['database'] == database:
                db_info = entry
                break
        if db_info['db-type'] == 'sql server':
            connection_string = "DSN=%s; database=%s" % (
                db_info['DSN'],
                db_info['database']
                )
        elif db_info['db-type'] == 'mysql':
            # https://dev.mysql.com/doc/connector-odbc/en/connector-odbc-configuration-connection-parameters.html
            connection_string = "DRIVER={%s};user=%s;password=%s;server=%s;database=%s;port=%s" % (
                db_info['driver'],
                db_info['user'],
                db_info['password'],
                db_info['server'],
                db_info['database'],
                db_info['port']
                )
        else:
            print("Database type not recognized or database info not in ./local/dbinfo.json")
            return None
        print("Connecting with connection string: " + connection_string)
        conn = pyodbc.connect(connection_string)
        return conn, db_info['db-type']



    def _call_gpt(self, prompt):
        print("\nGPT prompt:")
        print(prompt)
        f = open('.local/openai.json')
        openai_key = json.load(f)
        f.close()
        openai.api_key = openai_key['api_key']
        # openai.Model.list()
        response = openai.ChatCompletion.create(
            model="gpt-3.5-turbo",
            messages=[
                {
                    "role": "user", 
                    "content": prompt}
            ],
            temperature=0,
            max_tokens=800,
            top_p=1.0,
            frequency_penalty=0.0,
            presence_penalty=0.0,
            stop=["#", ";"]
            )
        print("\nGPT response:")
        for choice in response["choices"]:
            print(choice["message"]["content"])
        print("\n")
        return response["choices"][0]["message"]["content"]
