from flask import Flask, json, g, request
from dcmx import ProvenanceExplorer
from flask_cors import CORS
import json
import networkx as nx
import pandas as pd

import os

app = Flask(__name__)
CORS(app)

orpe = ProvenanceExplorer()

orpe_dict = {}

@app.route("/list_db", methods=["GET"])
@app.route("/explorer", methods=["GET"])
def list_db():
    db_files = []
    for x in os.listdir():
        if x.endswith(".db"):
            db_files.append(x)
    return json.dumps(db_files)

@app.route("/set_db/<db_name>",methods=["GET"])
def set_db(db_name):
    orpe.dbfile=db_name
    orpe.open_connection()
    return json.dumps(True)

def load_db(db_name):
    try:
        orpe = orpe_dict[db_name]
    except:
        orpe_dict[db_name] = ProvenanceExplorer(db_name)    
        orpe = orpe_dict[db_name]
    #orpe = ProvenanceExplorer(db_name)    
    return orpe

@app.route("/explorer/<db_name>/list_operations",methods=["GET"])
def list_operations(db_name):
    orpe = load_db(db_name)
    return json.dumps(orpe.get_all_state_command().to_dict(orient="records"))


@app.route("/explorer/<db_name>/parallel_workflow",methods=["GET"])
def parallel_workflow(db_name):
    orpe = load_db(db_name)
    temp_parallel = orpe.gv_template(orpe.parallel_workflow())[2]
    #collapsed,freq_pattern = orpe.collapsed_iterative(orpe.parallel_workflow(),temp_parallel[1])
    return json.dumps({"workflow": temp_parallel})

@app.route("/explorer/<db_name>/columns",methods=["GET"])
def columns(db_name):
    orpe = load_db(db_name)
    linear_recipe = orpe.get_linear_recipe()
    #print(linear_recipe.state_id)
    columns = orpe.get_column_at_state(linear_recipe.state_id.values[-1])
    #collapsed,freq_pattern = orpe.collapsed_iterative(orpe.parallel_workflow(),temp_parallel[1])
    return json.dumps(columns.to_dict(orient="records"))

@app.route("/explorer/<db_name>/column_recipes/<column_id>",methods=["GET"])
def column_recipes(db_name,column_id):
    orpe = load_db(db_name)
    #recipes = list(filter(lambda x:x!=None,orpe.reuse_recipe(column_id)[0]))
    recipes = orpe.reuse_recipe(column_id)
    #print(recipes)
    #print(json.dumps(recipes,indent=4))
    return json.dumps(recipes,indent=4)

@app.route("/explorer/<db_name>/recipes",methods=["GET"])
def recipes(db_name):
    orpe = load_db(db_name)
    linear_recipes = orpe.get_linear_recipe()
    recipes = []
    for x in linear_recipes.sort_values("state_id",ascending=False).to_records():
        xx = json.loads(x.detail)
        if xx["operation"]!=None:
            recipes.append(xx["operation"])

    #print(recipes)
    #print(json.dumps(recipes,indent=4))
    return json.dumps(recipes,indent=4)    

@app.route("/explorer/<db_name>/state_op",methods=["GET"])
def state_op(db_name):
    orpe = load_db(db_name)
    linear_recipes = orpe.get_linear_recipe()
    recipes = []
    for x in linear_recipes.sort_values("state_id",ascending=False).to_records():
        xx = json.loads(x.detail)
        if xx["operation"]!=None:
            recipes.append({"state_id": int(x.state_id), "op":xx["operation"]})

    return json.dumps(recipes,indent=4)        


@app.route("/explorer/<db_name>/parallel_state/<column_id>",methods=["GET"])
def parallel_state(db_name,column_id):
    orpe = load_db(db_name)
    parallel_workflow = orpe.parallel_workflow()
    process_nodes,parallel_graph,gv_string = orpe.gv_template(parallel_workflow)
    print(parallel_graph.nodes)
    recipes,paths = orpe.parallel_state(parallel_graph,"col"+column_id+"_0")
    selected_workflow = {}
    for p in paths:
        for pi in p[0]:
            selected_workflow[pi] = parallel_workflow[pi]
    process_nodes,parallel_graph,gv_string = orpe.gv_template(selected_workflow)
    #print(json.dumps(recipes,indent=4))
    return json.dumps({"workflow": gv_string})


@app.route("/explorer/<db_name>/state_graph",methods=["GET"])
def state_graph(db_name):
    orpe = load_db(db_name)
    parallel_workflow = orpe.parallel_workflow()
    state_cmd = orpe.get_all_state_command()
    process_nodes,parallel_graph,gv_string = orpe.gv_template(parallel_workflow)
    #print(recipes)    
    sink_nodes = [node for node, outdegree in dict(parallel_graph.out_degree(parallel_graph.nodes())).items() if outdegree == 0]
    source_nodes = [node for node, indegree in dict(parallel_graph.in_degree(parallel_graph.nodes())).items() if indegree == 0]
    paths = []
    for x in [(source, sink) for sink in sink_nodes for source in source_nodes]:
        a_simple_path = list(nx.all_simple_paths(parallel_graph, source=x[0], target=x[1]))
        a_state_path = [list(filter(lambda x:x.startswith("state"), x)) for x in a_simple_path]
        #print(a_simple_path)
        a_data = [list(filter(lambda x:x.startswith("col"), x)) for x in a_simple_path]
        for i,path in enumerate(a_state_path):
            #print(x)
            temp_path_recipe = list(filter(lambda x:x!=None,[json.loads(state_cmd[state_cmd.state_id==orpe.get_state_to_step(int(yy.split("_")[-1])-1)].detail.values[0])["operation"] for yy in path]))
            #recipes.append(temp_path_recipe)
            paths.append((path,a_data[i]))    
    #print(paths)
    return json.dumps(parallel_workflow)

@app.route("/explorer/<db_name>/dataset_state/<state>",methods=["GET"])
def dataset_state(db_name,state):
    orpe = load_db(db_name)
    snapshot_state = orpe.get_snapshot_at_state(orpe.get_step_to_state(int(state))+1)
    #print({"columns":list(snapshot_state.columns),"values":snapshot_state.values.tolist()})
    return json.dumps({"columns":list(snapshot_state.columns),"values":snapshot_state.values.tolist(), "state": int(orpe.get_step_to_state(int(state)))})    


@app.route("/explorer/<db_name>/query",methods=["POST"])
def query(db_name):
    orpe = load_db(db_name)
    query = request.form.get('query')
    #print(db_name,query)
    query_result = pd.read_sql_query(query,orpe.conn)
    return json.dumps({"columns":list(query_result.columns),"values":query_result.values.tolist()})    
    #return ""

@app.route("/explorer/<db_name>/cell_lineage/<col_id>/<row_id>/<step>",methods=["GET"])
def get_cell_lineage(db_name,col_id,row_id,step):
    orpe = load_db(db_name)
    _,_,gv = orpe.get_cell_lineage(int(row_id),int(col_id),int(step))
    print(gv)
    return json.dumps({"dot": gv})

app.run(debug=True)