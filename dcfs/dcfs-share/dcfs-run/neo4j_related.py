import json
from neo4j import GraphDatabase
import logging
# logging.basicConfig(level=logging.DEBUG)
from math import log

class Neo4jRelated:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        
    def close(self):
        self.driver.close()

    def create_nodes_and_relationships(self, columns, column_stats):
        with self.driver.session() as session:
            # Create nodes for all tables
            tables = list(set([(c[4], c[2], c[3]) for c in columns]))  # Extract unique (tablename, ip, port) tuples from columns

            for table_name, ip, port in tables:
                session.write_transaction(self._create_table_node, table_name, ip, port)

            # Create nodes for all columns
            for column in columns:

                stats = next((s for s in column_stats if s['Column_name'] == column[0] and s['Db_name'] == column[5] and s['Table_name'] == column[4]), None)

                logging.debug("Processing column: {}".format(str(column).replace('\n', ' ')))
                session.write_transaction(self._create_node, column, stats)

            # Create relationships for columns 系統說會出現笛卡爾積
            for i, c1 in enumerate(columns):
                for c2 in columns[i+1:]:
                    query = f"""
                        MATCH (a:Column {{name: '{c1[0]}', dbtype: '{c1[1]}', ip: '{c1[2]}', port: '{c1[3]}', tablename: '{c1[4]}', dbname: '{c1[5]}'}}),
                        (b:Column {{name: '{c2[0]}', dbtype: '{c2[1]}', ip: '{c2[2]}', port: '{c2[3]}', tablename: '{c2[4]}', dbname: '{c2[5]}'}})
                        MERGE (a)-[r:RELATED]->(b)
                        MERGE (b)-[r2:RELATED]->(a)
                    """
                    session.run(query)
            # Create relationships for columns 尚未測試
            # for i, c1 in enumerate(columns):
            #     for c2 in columns[i+1:]:
            #         query = f"""
            #             MATCH (a:Column {{name: '{c1[0]}', dbtype: '{c1[1]}', ip: '{c1[2]}', port: '{c1[3]}', tablename: '{c1[4]}', dbname: '{c1[5]}'}})
            #             OPTIONAL MATCH (a)-[:RELATED]-(b:Column {{name: '{c2[0]}', dbtype: '{c2[1]}', ip: '{c2[2]}', port: '{c2[3]}', tablename: '{c2[4]}', dbname: '{c2[5]}'}})
            #             MERGE (a)-[r:RELATED]->(b)
            #             MERGE (b)-[r2:RELATED]->(a)
            #         """
            #         session.run(query)

            # Create relationships for tables
            for i, t1 in enumerate(tables):
                for t2 in tables[i+1:]:
                    query = f"""
                        MATCH (a:Table {{tablename: '{t1[1]}', ip: '{columns[0][2]}', port: '{columns[0][3]}'}}),
                        (b:Table {{tablename: '{t2[1]}', ip: '{columns[0][2]}', port: '{columns[0][3]}'}})
                        MERGE (a)-[r:TABLE_RELATED]->(b)
                        MERGE (b)-[r2:TABLE_RELATED]->(a)
                    """
                    session.run(query)


            # Create relationships from tables to columns
            for column in columns:
                query = f"""
                    MATCH (t:Table {{tablename: '{column[4]}', ip: '{column[2]}', port: '{column[3]}'}}),
                    (c:Column {{name: '{column[0]}', dbtype: '{column[1]}', ip: '{column[2]}', port: '{column[3]}', tablename: '{column[4]}', dbname: '{column[5]}'}})
                    MERGE (t)-[r:HAS_COLUMN]->(c)
                """
                session.run(query)

            
            #Create relationships for dbnames
            dbnames = list(set([(c[5], c[2], c[3]) for c in columns]))  # Extract unique (dbname, ip, port) tuples from columns

            for db_name, ip, port in dbnames:
                session.write_transaction(self._create_dbname_node, db_name, ip, port)
           # Create relationships from dbname to tables
            for column in columns:
                query = f"""
                    MATCH (d:Database {{dbname: '{column[5]}', ip: '{column[2]}', port: '{column[3]}'}}),
                    (t:Table {{tablename: '{column[4]}', ip: '{column[2]}', port: '{column[3]}'}})
                    MERGE (d)-[r:HAS_TABLE]->(t)
                """
                session.run(query)

            
            #Create relationships for dbtypes
            dbtype_ip_port_combinations = list(set([(c[1], c[2], c[3]) for c in columns]))  # Extract unique (dbtype, ip, port) tuples from columns

            for dbtype, ip, port in dbtype_ip_port_combinations:
                session.write_transaction(self._create_dbtype_node, dbtype, ip, port)

            # Create relationships from DBType to Database
            for column in columns:
                query = f"""
                    MATCH (t:DBType {{dbtype: '{column[1]}', ip: '{column[2]}', port: '{column[3]}'}}),
                    (d:Database {{dbname: '{column[5]}', ip: '{column[2]}', port: '{column[3]}'}})
                    MERGE (t)-[r:HAS_DATABASE]->(d)
                """
                session.run(query)



    #update unique node
    def create_unique_constraint(self):
        with self.driver.session() as session:
            query = """
                CREATE CONSTRAINT unique_column IF NOT EXISTS
                FOR (c:Column)
                REQUIRE (c.ip, c.port, c.dbname, c.dbtype, c.tablename, c.name) IS UNIQUE
            """
            session.run(query)
    
    def get_related_columns(self):
        with self.driver.session() as session:
            query = """
                MATCH (c1:Column)-[:RELATED]->(c2:Column)
                RETURN c1.phoenix_ID, collect(c2.phoenix_ID) AS related_columns
        """

            result = session.run(query)
            related_columns_data = {}

            for record in result:
                doc_id = f"{record['c1.phoenix_ID']}"
                related_columns = record['related_columns']
                related_columns_data[doc_id] = related_columns

        return related_columns_data

    @staticmethod
    def _create_node(tx, column, stats):
        query = """
            MERGE (c:Column {
                name: $name,
                dbtype: $dbtype,
                ip: $ip,
                port: $port,
                tablename: $tablename,
                dbname: $dbname
            })
        """
        parameters = {
            'name': column[0],
            'dbtype': column[1],
            'ip': column[2],
            'port': column[3],
            'tablename': column[4],
            'dbname': column[5]
        }
        #phoenix stats set column to neo4j
        if stats:
            query += """
                SET c.column_count = $column_count,
                c.column_frequency = $column_frequency,
                c.phoenix_ID = $phoenix_ID,
                c.last_used_column = $last_used_column,
                c.merge_column_count = $merge_column_count,
                c.SubmittedUser = $SubmittedUser
            """
            parameters.update({
                'column_count': stats['Column_count'],
                'column_frequency': stats['Column_Frequency'],
                'phoenix_ID': stats['ID'],
                'last_used_column': stats['Last_Used_Column'],
                'merge_column_count': stats['Merge_Column_Count'],
                'SubmittedUser': stats['SubmittedUser'].split(",")
            })

            logging.debug(f"Found stats for column: {column[0]} with DbName: {column[5]}")
        else:
            logging.warning(f"No stats found for column: {column[0]} with DbName: {column[5]}")

        result = tx.run(query, parameters)
        for record in result:
            logging.debug(f"Created/Updated node: {record['c']}")

    @staticmethod
    def _create_table_node(tx,table_name, ip, port):
        query = """
            MERGE (t:Table {
                tablename: $tablename,
                ip: $ip,
                port: $port
            })
        """
        parameters = {
            'tablename': table_name,
            'ip': ip,
            'port': port
        }

        result = tx.run(query, parameters)
        for record in result:
            logging.debug(f"Created/Updated table node: {record['t']}")

    

    @staticmethod
    def _create_dbname_node(tx, db_name, ip, port):
        query = """
            MERGE (d:Database {
                dbname: $dbname,
                ip: $ip,
                port: $port
            })
        """
        parameters = {
            'dbname': db_name,
            'ip': ip,
            'port': port
        }

        result = tx.run(query, parameters)
        for record in result:
            logging.debug(f"Created/Updated database node: {record['d']}")


    @staticmethod
    def _create_dbtype_node(tx, dbtype, ip, port):
        query = """
            MERGE (d:DBType {
                dbtype: $dbtype,
                ip: $ip,
                port: $port
            })
        """
        parameters = {
            'dbtype': dbtype,
            'ip': ip,
            'port': port
        }

        result = tx.run(query, parameters)
        for record in result:
            logging.debug(f"Created/Updated DBType node: {record['d']}")
