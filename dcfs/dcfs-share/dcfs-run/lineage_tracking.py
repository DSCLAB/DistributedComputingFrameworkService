import uuid
from datetime import datetime
import logging
from neo4j_lineage import Neo4Lineage
import asyncio

lineage_info = {
    "Run":{
        "Run_content":"",
        "Run_result":"",
        "Run_msg":"",
        "Run_starttime":"",
        "Run_endtime":"",
        "Run_time":""

    },
    "Action":{
        "ActionID":"",
        "ActionType":"",
        "ActionOwner":"",
        "ActionName":"",
        "ActionDesc":""
    },
    "Dataset":{
        "Input":{
            "Datainfo":[],
            "Processinfo":[]
        },
        "Output":{
            "Datainfo":[]
        }

    }
}

def lineage_tracing(lineage_info):
    neo4j_uri = "bolt://neo4j:7687"
    neo4j_user = "neo4j"
    neo4j_password = "password"
    runid = uuid.uuid1()
    lineage_info["Run"]["Run_time"]= str(lineage_info["Run"]["Run_endtime"] - lineage_info["Run"]["Run_starttime"])
    lineage_info["Run"]["Run_starttime"] = str(lineage_info["Run"]["Run_starttime"])
    lineage_info["Run"]["Run_endtime"] = str(lineage_info["Run"]["Run_endtime"])
    # Initialize Neo4Lineage instance
    neo4j_conn = Neo4Lineage(neo4j_uri, neo4j_user, neo4j_password, runid, lineage_info["Run"], lineage_info["Action"])
    logging.info(f'start lineage tracking !')
    logging.info(f'runid={runid}')
    logging.info(f'lineage_info = {lineage_info}')
    inputIds = set()
    outputIds = set()
    w_IDNS =[]
    w_ODNS =[]
    if lineage_info["Dataset"]["Input"]["Processinfo"]:
        inputIds = neo4j_conn.find_existed_IPN(lineage_info["Dataset"]["Input"]["Processinfo"])
    if lineage_info["Dataset"]["Input"]["Datainfo"]:
        inputIds, w_IDNS = neo4j_conn.find_existed_IDN(lineage_info["Dataset"]["Input"]["Datainfo"])
    if w_IDNS:
        inputIds = neo4j_conn.create_IDN(w_IDNS)
        pn_ID = neo4j_conn.create_PN()
    else:
        pn_ID = neo4j_conn.find_existed_PN()
        if pn_ID==False:
            pn_ID = neo4j_conn.create_PN()
        
    logging.info(f'connect input:[{inputIds}]-> pn:{pn_ID}')
    neo4j_conn.connect_lineage(pn_ID, inputIds, None)
    if lineage_info["Dataset"]["Output"]["Datainfo"]:
        outputIds, w_ODNS = neo4j_conn.find_existed_ODN(lineage_info["Dataset"]["Output"]["Datainfo"])
        if w_ODNS:
            outputIds = neo4j_conn.create_ODN(w_ODNS)
        logging.info(f'connect input: pn:{pn_ID} ->[{outputIds}]')
        neo4j_conn.connect_lineage(pn_ID, None, outputIds)
    logging.info(f'lineage tracking finish')
    return runid