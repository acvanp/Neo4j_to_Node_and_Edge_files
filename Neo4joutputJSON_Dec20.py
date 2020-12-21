# -*- coding: utf-8 -*-
"""
Created on Thu Dec  3 10:34:36 2020

@author: Alexander.Plantinga
"""
# either run this block by block using fn+F9
# or run the whole thing using ctrl+enter

import pandas as pd
from py2neo import Graph
import json
import numpy as np

# specify your output directory here
user_name = "Alexander.Plantinga"
# change this to an existing file destination on your system
folderloc = "C:/Users/Lenovo/Downloads"


# run your Neo4j database while performing this operation
# use your database address and credentials in this line here.
#graph = Graph("bolt://localhost:11003", auth=("neo4j", "graph"))
graph = Graph("bolt://localhost:7687", auth=("neo4j", "graph"))

# myquery examples given in comment block here. Uncomment one line
# or write your own entry for 'myquery' using cypher
# Tailor the cypher query for maximum specificity,
# try to leave as few unspecified variables
# or vague traversals (e.g., [*0..5]) as possible.
# In the return statement try to specify the relationship variable
# as long as it doesn't slow the database down too much.
# It is best to return organizations on the left and value tree nodes on the right of the cypher query, 
# reverse the Source and Target columns if you want to reverse the Sankey flow.


#Simple NESDIS Org chart
#myquery ='match (org1:Organization)-[rel1:IS_PARENT_OF]->(org2:Organization) where org1.short_name in ["CSSD", "OGSSD", "NODC", "NCDC", "NGDC", "NCEI", "IIAD", "CoastWatch", "STAR", "TPIO", "OPPA", "SAB", "SPB", "NIC", "OSPO", "NESDIS"] return org1, org2, rel1'

# NOAA org chart
#myquery = 'match (grandparent:Organization)-[rel1:IS_PARENT_OF]->(parent:Organization)-[rel2:IS_PARENT_OF]->(noaa:Organization{short_name:"NOAA"})-[rel3:IS_PARENT_OF]->(noaajr:Organization) return grandparent, parent, noaa, noaajr, rel1, rel2, rel3'

#myquery = 'match (grandparent:Organization)-[rel1:IS_PARENT_OF]->(parent:Organization)-[rel2:IS_PARENT_OF]->(noaa:Organization{short_name:"NOAA"})-[rel3:IS_PARENT_OF]->(noaajr:Organization)-[rel4:IS_PARENT_OF]->(noaababies:Organization) return grandparent, parent, noaa, noaajr, noaababies, rel1, rel2, rel3, rel4'

# parent org, org, creates product
#myquery = 'match (parent:Organization)-[]->(org:Organization)-[rel1:CREATES_PRODUCT]->(prd:Product) where prd.palma_id contains "ice" return parent, org, prd, rel1'

# Ocean color
#myquery = "match (vt:Value_Tree)<-[]-(p:Product)<-[]-(s:Source)<-[]-(gr:Group) where p.name contains 'Ocean Color' return p,gr,vt,s"

# The MBRFC data sources and data products
#myquery = "match (s:Source)-[]->(p:Product) where toLower(p.palma_id) contains 'qpf-rfc mbrfc' return p,s"

# Fire Weather Planning
#myquery = 'match (nesdis:Organization{short_name:"NESDIS"})-[rel1:AFFILIATED_WITH]->(sys:System)-[rel2]->(pfm:Platform)-[rel3]->(ele:Element)-[rel4]->(sc:Source)-[rel5]->(prd:Product) where rel1.type in ["FUNDINDG ORGANIZATION", "OBSERVING SYSTEM OWNER", "OBSERVING SYSTEM OPERATOR", "OBSERVING SYSTEM OWNER (NOAA)","OBSERVING SYSTEM OPERATOR (NON-NOAA)"] and toLower(prd.name) contains "fire weather planning" return nesdis, prd, sys, pfm, ele, sc, rel1, rel2, rel3, rel4, rel5'

#GFS
#myquery = 'match (p:Product)<-[rel1]-(sc:Source)<-[rel2:HAS_PALMA_CONNECTION]-(x) where toLower(p.palma_id) contains "gfs" return p,sc, x'

# Fire Weather requirements
#myquery = 'match (nesdis:Organization{short_name:"NESDIS"})-[rel1:IS_PARENT_OF*0..2]->(nesdisjr:Organization)-[rel2:OWNS]->(req:Requirement)-[rel3:REQ_MEASURES]->(par:Parameter)<-[rel4:ELE_MEASURES]-(ele:Element)-[rel5]->(sc:Source)-[rel6]->(prd:Product) where nesdisjr.short_name in [ "NESDIS", "NCEI", "CSSD", "OGSSD", "NODC", "NCDC", "NGDC", "STAR", "CoastWatch", "OPPA", "TPIO", "OSPO", "SAB", "SPB", "NIC", "IIAD"] and toLower(prd.palma_id) contains "fireweather" return nesdis, nesdisjr, req, par, ele,sc, prd,  rel1, rel2, rel3, rel4, rel5, rel6'


#NESDIS satellite sensor products for external stakeholders
#myquery = 'match (nesdis:Organization)-[rel1:AFFILIATED_WITH]->(nsys:System)-[:HOSTS_PLATFORM]->(pfm:Platform)-[:HOSTS_ELEMENT]->(ele:Element)-[:HAS_PALMA_CONNECTION]->(sc:Source)-[:IS_DIRECT_CONTRIBUTOR_TO | IS_CONTRIBUTING_MEMBER]->(prd:Product), (orgsjr:Organization)-[rel2:CREATES_PRODUCT]->(prd) where not  orgsjr.short_name in [ "NESDIS", "NCEI", "CSSD", "OGSSD", "NODC", "NCDC", "NGDC", "STAR", "CoastWatch", "OPPA", "TPIO", "OSPO", "SAB", "SPB", "NIC", "IIAD"]  and nesdis.short_name in [ "NESDIS", "NCEI", "CSSD", "OGSSD", "NODC", "NCDC", "NGDC", "STAR", "CoastWatch", "OPPA", "TPIO", "OSPO", "SAB", "SPB", "NIC", "IIAD"] and rel1.type in ["FUNDINDG ORGANIZATION", "OBSERVING SYSTEM OWNER", "OBSERVING SYSTEM OPERATOR", "OBSERVING SYSTEM OWNER (NOAA)","OBSERVING SYSTEM OPERATOR (NON-NOAA)"] return  nesdis, nsys, pfm, ele, sc, prd, orgsjr, rel1, rel2'

# Fire in the value tree
#myquery = 'match (vt1:Value_Tree)-[rel1]->(vt2:Value_Tree)-[rel2]->(vt3:Value_Tree), (prd:Product)-[rel3]->(vt3) where toLower(vt3.name) contains "fire"  return vt1, vt2, vt3, prd, rel1, rel2, rel3'

# Fire weather - value tree - product - sources
#myquery = 'match (vt1:Value_Tree)-[rel1:IS_PARENT_OF]->(vt2:Value_Tree)-[rel2:IS_PARENT_OF]->(vt3:Value_Tree)<-[rel3]-(prd:Product)<-[rel4]-(src:Source) where toLower(vt3.name) contains "fire" return vt1, vt2, vt3, prd, src, rel1, rel2, rel3, rel4' 

# fire weather value tree product sources connected to NESDIS satellites
#myquery = 'match (nesdis:Organization)-[rel1]->(nesdisjr:Organization)-[rel2:AFFILIATED_WITH]->(sys:System)-[rel3]->(pfm:Platform)-[rel4]-(ele:Element)-[rel5]->(src:Source)-[rel6]->(prd:Product)-[rel7]->(vt3:Value_Tree)<-[rel8]-(vt2:Value_Tree)<-[rel9]-(vt1:Value_Tree) where toLower(vt3.name) contains "fire"  and rel2.type in ["FUNDINDG ORGANIZATION", "OBSERVING SYSTEM OWNER", "OBSERVING SYSTEM OPERATOR", "OBSERVING SYSTEM OWNER (NOAA)","OBSERVING SYSTEM OPERATOR (NON-NOAA)"] and nesdisjr.short_name in [ "NESDIS", "NCEI", "CSSD", "OGSSD", "NODC", "NCDC", "NGDC", "STAR", "CoastWatch", "OPPA", "TPIO", "OSPO", "SAB", "SPB", "NIC", "IIAD"] and nesdis.short_name in ["NESDIS", "NOAA"] return nesdis, nesdisjr, vt1, vt2, vt3, prd, src, ele, pfm, sys, rel1, rel2, rel3, rel4, rel5, rel6, rel7, rel8, rel9'

# hurricane / cyclone value tree to products
#myquery = 'match (src:Source)-[rel1]->(prd:Product)-[rel2]->(vt3:Value_Tree)<-[rel3]-(vt2:Value_Tree)<-[rel4]-(vt1:Value_Tree)  where vt1.subtree_name contains "WRN_HUR" return prd, vt1, vt2, vt3, rel1, rel2, rel3, rel4'

# fire weather watch
#myquery = 'match (vt1:Value_Tree)-[rel1]->(vt2:Value_Tree{name:"Fire Weather Watch_N2-1"})-[rel2]->(vt3:Value_Tree)<-[rel3]-(prd:Product)<-[rel4]-(src:Source) return prd, src, vt1, vt2, vt3, rel1, rel2, rel3, rel4'

# Organization creates produdct - need to specify the relationship variable "rel1" in return statement
#myquery = 'match (org:Organization{short_name:"NGS"})-[rel1:CREATES_PRODUCT]->(prd:Product) return org, prd, rel1'

# Orgs create and support a product
#myquery = 'match (orgsupports:Organization)-[rel1:AFFILIATED_WITH]->(sys:System)-[rel2]->(src:Source)-[rel3]->(prd:Product)<-[rel4:CREATES_PRODUCT]-(orgcreates:Organization) where toLower(prd.palma_id) contains "mpe mbrfc" and toLower(rel1.type) contains "operat" return orgsupports, orgcreates, sys, src, prd, rel1, rel2, rel3, rel4'

#graph.run(myquery)

print("Running Neo4j query...")

mydata = graph.run(myquery)

print("Building node and edge files...")

mydata = list(mydata)
            
# edgelist for the entire neo4j query 
# made of [source_index_number, target_index_number] tuples
edgelist = list()  

for subtree in mydata:
    
    for subtree_variable in subtree.keys():
        
        if 'rel' in subtree_variable:
            
            s_t_pair = [str(subtree[subtree_variable].start_node.labels)[1:], str(subtree[subtree_variable].end_node.labels)[1:]]
            
            if s_t_pair != ['Value_Tree', 'Value_Tree']:
                
                edgelist.append([subtree[subtree_variable].start_node.identity, subtree[subtree_variable].end_node.identity])
                
            else: edgelist.append([subtree[subtree_variable].end_node.identity, subtree[subtree_variable].start_node.identity])
                
# make a node reference table 
# iterate through the query results and assign a new row to a node 
# that has not been encountered
# include all node indices, labels, metadata / properties / attributes
import pandas as pd


myschema = ['Source', 'Group', 'Network', 'Element', 'Platform',
            'System', 'Product', 'Person', 'Value_Tree', 'Organization',
            'Requirement', 'Mandate', 'Parameter']

colnames = ['Identity', 'Label', 'name', 'palma_id', 'type', 'subtree_name', "short_name", ]

node_df = pd.DataFrame([], columns = colnames)

cc = 0 # indexing variable in node_df.loc[cc]

for i in mydata:

    for j in i.values():
        
        if hasattr(j, 'labels'):

            if(str(j.labels)[1:] in myschema):
                
                tt = {} # temporary dictionary with values to be appended to node_df

                if j.identity not in node_df['Identity']:

                    tt['Identity'] = j.identity

                    tt['Label'] = str(j.labels)[1:]
                    
                    if 'name' in j.keys():
                        tt['name'] = j['name']
                    else: tt['name'] = ''
                    
                    if 'palma_id' in j.keys():
                        tt['palma_id'] = j['palma_id']
                    else: tt['palma_id'] = ''
                    
                    if 'type' in j.keys():
                        tt['type'] = j['type']
                    else: tt['type'] = ''
                    
                    if 'subtree_name' in j.keys():
                        tt['subtree_name'] = j['subtree_name']
                    else: tt['subtree_name'] = ''
                    
                    if 'short_name' in j.keys():
                        tt['short_name'] = j['short_name']
                    else: tt['short_name'] = ''
                            
                    node_df.loc[cc] = list(tt.values())
                    cc = cc + 1
                    
                else: next
                    
                    
                    


# function to get unique values 
def unique(list1, emptylist): 
  
    # intilize a null list 
    unique_list = [] 
      
    # traverse for all elements 
    for x in list1: 
        # check if exists in unique_list or not 
        if x not in unique_list: 
            unique_list.append(x) 
    # print list 
    for x in unique_list: 
        #print(x)
        emptylist.append(x)
     
edgelist_unique = list()
unique(edgelist, edgelist_unique)

pd.DataFrame(edgelist_unique, columns = ['Source', 'Target']).to_csv(folderloc+ '/py2neoEDGES.csv',index=False)

pd.DataFrame(node_df, columns = colnames).to_csv(folderloc+ '/py2neoNODES.csv',index=False)

print('Output written to ' + folderloc + '/py2neoEDGES.csv and ' + folderloc + '/py2neoNODES.csv' )
