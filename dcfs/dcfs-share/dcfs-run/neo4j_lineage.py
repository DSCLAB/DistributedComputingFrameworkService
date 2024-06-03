from neo4j import GraphDatabase
import logging
import uuid


class Neo4Lineage:
    def __init__(self, uri, user, password, runid, runinfo, actioninfo):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        self.runid = runid
        self.runinfo = runinfo
        self.actioninfo = actioninfo
        self.input = set() #input  nodes
        self.output = set() #output  nodes
        logging.info(f'Successfully connect to Neo4j !')

    def close(self):
        self.driver.close()

    def sent_request(self, query, query_msg):
        try: 
            with self.driver.session() as session:
                logging.info(f'{query_msg} = {query}')
                result = session.run(query)
                record = result.single()                
                return record
        except Exception as e:
            logging.error( "an error occurred while sendind query" + traceback.format_exc())   

    def find_DN_request(self, nodeinfo):
        dbtype = nodeinfo['DbType']
        ip = nodeinfo['Ip']
        port = nodeinfo['Port']
        dbname = nodeinfo['DbName']
        tablename = nodeinfo['TableName']
        query = f"""
                MATCH (a:DataNode {{DbType: '{dbtype}', DbName: '{dbname}', Ip: '{ip}', Port: '{port}', TableName: '{tablename}'}})
                RETURN ID(a)
            """
        query_msg = "finding DN with query"
        return self.sent_request(query, query_msg)

    def find_existed_IDN(self, inputdata):
        w_IDNS = []
        tmp_input = set()
        for IDN_info in inputdata:
            record = self.find_DN_request(IDN_info)
            if record is not None:
                node_id = record[0]
                logging.info(f'find the existed IDN: {node_id}')
                tmp_input.add(node_id) #already created
            else:
                logging.info(f'did not find the existed IDN')
                w_IDNS.append(IDN_info) #need to create a new one
        if bool(tmp_input):
            for id in tmp_input:
                logging.info(f'update IDN {id}')
                self.update_DN_runid(id) #need to update runid
        self.input.update(tmp_input)
        return self.input,  w_IDNS

    def find_existed_IPN(self, inputps):
        tmp_input = set()
        for IPN_id in inputps:
            query1 = f"""
                        MATCH (a1:ProcessNode)-[:HAS_RUNINFO]->(b1:RunInfo)
                        MATCH (a2:ProcessNode)-[:HAS_RUNINFO]->(b2:RunInfo)
                        WHERE b1.RunID= "{IPN_id}" AND b2.RunID= "{IPN_id}"
                        WITH a1, a2
                        MATCH (a1)-[:lineage]->(a2)
                        RETURN ID(a2)
                        """
            query2 = f"""
                        MATCH (a:ProcessNode)-[:HAS_RUNINFO]->(b:RunInfo)
                        WHERE b.RunID= "{IPN_id}" 
                        RETURN ID(a)
                        """
            for query in [query1, query2]:
                query_msg = "try to find existed IPN with query"
                record =  self.sent_request(query, query_msg)
                if record is not None:
                    logging.info(f'find the existed IPN: {record[0]}')
                    tmp_input.add(record[0]) #already created
                    break
                elif query==query2 and record is None:
                    logging.error(f'error:can not find the IPN')
        if bool(tmp_input):
            for id in tmp_input:
                logging.info(f'update IPN {id}')
                self.create_RN(id) #need to update runid
        self.input.update(tmp_input)
        return self.input

    def find_existed_ODN(self,outputdata):
        w_ODNS = []
        for ODN_info in outputdata:
            record = self.find_DN_request(ODN_info)
            if record is not None:
                logging.info(f'find the existed ODN: {record[0]}')
                self.output.add(record[0]) #already created
            else:
                logging.info(f'did not find the existed ODN')
                w_ODNS.append(ODN_info) #need to create a new one
        if bool(self.output):
            for id in self.output:
                logging.info(f'update ODN:{id}')
                self.update_DN_runid(id) #need to update runid
        return self.output, w_ODNS
    
    def find_existed_PN(self):
        # find if there has existed PN
        query = f"""
                        MATCH (a:ProcessNode {{ActionID: '{self.actioninfo['ActionID']}', ActionType: '{self.actioninfo['ActionType']}'}})
                        WITH COLLECT (ID(a)) AS id
                        RETURN id
                    """
        query_msg = "try to find existed PN with query"
        record =  self.sent_request(query, query_msg)
        if record is not None:
            for pnID in record[0]:
                logging.info(f'check pnID={pnID}')
                check = self.check_input(pnID, list(self.input))
                if check:
                    logging.info(f'find the existed PN:{pnID}')
                    #if yes create a new runinfo node
                    self.create_RN(pnID)
                    return pnID
        logging.info(f'did not find the existed process node')
        return False
            
    def check_input(self, pnID, input):
        query = f"""
                    MATCH(n:ProcessNode)
                    WHERE ID(n) = {pnID}
                    WITH n
                    MATCH (input)-[:lineage]->(n)
                    RETURN COLLECT(ID(input)) AS inputNodes
                """
        query_msg = "check PN input with query"
        record =  self.sent_request(query, query_msg)
        logging.info(f'input= {sorted(input)}')
        logging.info(f'record[0]= {sorted(record[0])}')
        if sorted(record[0]) == sorted(input):
            return True
        else:
            return False 

    def update_DN_runid(self, DNid):
        query = f"""
                    MATCH (n:DataNode)
                    WHERE ID(n)= {DNid}
                    SET n.RunID = n.RunID + ['{self.runid}']
                """
        query_msg = "update_DN_runid with query"
        record =  self.sent_request(query, query_msg)
        #logging.info(f'update data node with runid {self.runid} success')

    def create_RN(self, pnID):
        query = f"""
                    MATCH (n:ProcessNode)
                    WHERE ID(n) = {pnID}
                    CREATE (a:RunInfo {{
                        RunID:'{self.runid}', 
                        RunContent: {repr(self.runinfo.get('Run_content', 'null'))},
                        RunResult: '{self.runinfo['Run_result']}',
                        RunMsg: {repr(self.runinfo.get('Run_msg', 'null'))},
                        RunStartTime: '{self.runinfo['Run_starttime']}',
                        RunEndTime: '{self.runinfo['Run_endtime']}',
                        RunTime: '{self.runinfo['Run_time']}'
                        }})
                    CREATE (n)-[:HAS_RUNINFO]->(a)
                    WITH a
                    RETURN ID(a)
                """
        query_msg = "create new RN with query"
        record =  self.sent_request(query, query_msg)
        if record is not None:
            logging.info(f'create RN:{record[0]} success')
        else:
            logging.error(f"create_RN error !")
                
    def create_DN_request(self, nodeinfo):
        dbtype = nodeinfo['DbType']
        ip = nodeinfo['Ip']
        port = nodeinfo['Port']
        dbname = nodeinfo['DbName']
        tablename = nodeinfo['TableName']
        query = f"""
                MERGE(a:DataNode {{RunID:['{self.runid}'], DbType: '{dbtype}', DbName: '{dbname}', Ip: '{ip}', Port: '{port}', TableName: '{tablename}'}})
                RETURN ID(a)
        """
        query_msg = "create DN with query"
        return self.sent_request(query, query_msg)
        
    def create_IDN(self, w_IDNS):
        for IDN_info in w_IDNS:
            record = self.create_DN_request(IDN_info)
            self.input.add(record[0])
            logging.info(f'create IDN:{record[0]} with runid {self.runid} success')
        return self.input

    def create_ODN(self,w_ODNS):
        for ODN_info in w_ODNS:
            record = self.create_DN_request(ODN_info)
            self.output.add(record[0])
            logging.info(f'create ODN:{record[0]} with runid {self.runid} success')
        return self.output

    def create_PN(self):
        query = f"""
                    CREATE (p:ProcessNode {{ActionID: '{self.actioninfo['ActionID']}', ActionType: '{self.actioninfo['ActionType']}', ActionOwner: '{self.actioninfo['ActionOwner']}', ActionName: '{self.actioninfo['ActionName']}', ActionDesc: '{self.actioninfo['ActionDesc']}'}})
                    RETURN ID(p)
                    """
        query_msg = "creat a new PN with query"
        record = self.sent_request(query, query_msg)
        logging.info(f'create PN:{record[0]} with runid {self.runid} success')
        self.create_RN(record[0])
        return record[0]
        
    def connect_lineage(self, pnID, input, output):
        logging.info(f'connect_lineage with input:{input}, output:{output}, PN:{pnID}')
        self.input = list(input) if input is not None else self.input
        self.output = list(output) if output is not None else self.output
        if output:
            logging.info(f'create lineage [{pnID}]->[{self.output}]')
            query = f"""
                    MATCH(p:ProcessNode)
                    WHERE ID(p)={pnID}

                    WITH p, {self.output} AS outputIds
                    MATCH (output:DataNode)
                    WHERE ID(output) IN outputIds
                    WITH p, COLLECT(output) AS outputNodes
                    FOREACH (outputNode IN outputNodes | MERGE (p)-[:lineage]->(outputNode))

                    RETURN ID(p)
                    """
        if input:
            logging.info(f'create lineage [{self.input}]->[{pnID}]')
            query = f"""
                    MATCH(p:ProcessNode)
                    WHERE ID(p)={pnID}

                    WITH p, {self.input} AS inputIds
                    MATCH (input)
                    WHERE ID(input) IN inputIds
                    WITH p, COLLECT(input) AS inputNodes
                    FOREACH (inputNode IN inputNodes | MERGE (inputNode)-[:lineage]->(p))

                    RETURN ID(p)
                    """
        query_msg = "connect with query"
        record = self.sent_request(query, query_msg)
        if record is None:
            logging.error(f"connect_lineage error !")
