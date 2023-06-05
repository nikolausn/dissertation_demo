import tarfile
import os
import zipfile
import os
from tqdm import tqdm
import json
import numpy as np
import csv
import sqlite3
import networkx as nx

class RowId():
    def __init__(self,row_id):
        self.row_id = row_id
    
    def __str__(self):
        return self.row_id

class ColId():
    def __init__(self,col_id):
        self.col_id = col_id
    
    def __str__(self):
        return self.col_id

def init_column(n_col):
    list_col = []
    for x in range(n_col):
        list_col.append(ColId(x))

def init_row(n_row):
    list_row = []
    for x in range(n_row):
        list_row.append(RowId(x))


def extract_project(file_name,temp_folder="temp"):
    fname = ".".join(file_name.split(".")[:-2])
    tar = tarfile.open(file_name,"r:gz")
    locex = "{}/{}".format(temp_folder,fname)
    try:
        os.mkdir(locex)
    except BaseException as ex:
        print(ex)
    tar.extractall(path=locex)
    tar.close()
    return locex,fname

def read_dataset(project_file):
    locex = "{}/{}/".format(project_file,"data")
    zipdoc = zipfile.ZipFile(project_file+"/data.zip")
    try:
        os.mkdir(locex)
    except BaseException as ex:
        print(ex)
    zipdoc.extractall(path=locex)
    zipdoc.close()

    datafile = open(locex+"/data.txt","r",encoding="ascii", errors="ignore")
    
    # read column model
    column_model_dict = {}
    line = next(datafile).replace("\n","")
    while line:
        if line=="/e/":
            break
        else:
            head = line.split("=")[0]
            val = line.split("=")[-1]
            column_model_dict[head] = val
            cols = []
            #print(head)
            if head == "columnCount":
                for x in range(int(val)):
                    line = next(datafile).replace("\n","")
                    col = json.loads(line)
                    cols.append(col)
                column_model_dict["cols"] = cols
        try:
            line=next(datafile).replace("\n","")
        except:
            break
    #print(column_model_dict)    

    # read history
    history_dict = {}
    line = next(datafile).replace("\n","")
    while line:
        if line=="/e/":
            break
        else:
            head = line.split("=")[0]
            val = line.split("=")[-1]
            history_dict[head] = val
            hists = []
            #print(head)
            if head == "pastEntryCount":
                for x in range(int(val)):
                    line = next(datafile).replace("\n","")
                    hist = json.loads(line)
                    hists.append(hist)
                history_dict["hists"] = hists
        try:
            line=next(datafile).replace("\n","")
        except:
            break
    #print(history_dict)

    # read data row
    data_row_dict = {}
    line = next(datafile).replace("\n","")
    while line:
        if line=="/e/":
            break
        else:
            head = line.split("=")[0]
            val = line.split("=")[-1]
            data_row_dict[head] = val
            rows = []
            #print(head)
            if head == "rowCount":
                for x in range(int(val)):
                    line = next(datafile).replace("\n","")
                    row = json.loads(line)
                    rows.append(row)
                data_row_dict["rows"] = rows
        try:
            line=next(datafile).replace("\n","")
        except:
            break
    #print(data_row_dict)

    return column_model_dict,history_dict,data_row_dict


def open_change(hist_dir,file_name,target_folder):
    fname = ".".join(file_name.split(".")[:-1])
    zipdoc = zipfile.ZipFile(hist_dir+file_name)
    locex = target_folder+fname
    try:
        os.mkdir(locex)
    except BaseException as ex:
        print(ex)    
    zipdoc.extractall(locex)
    zipdoc.close()
    return locex,fname
        


def read_change(changefile):
    changefile = open(changefile,"r",encoding="ascii", errors="ignore")
    # read version
    version = next(changefile).replace("\n","")
    # read command_name
    command_name = next(changefile).replace("\n","")
    print(version,command_name)
    header_dict = {}
    data_row = []    
    if command_name == "com.google.refine.model.changes.MassCellChange":
        header = ["commonColumnName","updateRowContextDependencies","cellChangeCount"]
        for head in header:
            header_dict[head] = next(changefile).replace("\n","").split("=")[-1]
        print(header_dict)
        data_header = ["row","cell","old","new"]
        data_row = []
        data_dict = {}
        line = next(changefile).replace("\n","")
        while line:
            if line=="/ec/":
                data_row.append(data_dict)
                data_dict = {}
                #break
            else:
                head = line.split("=")[0]
                val = None
                try:
                    val = "=".join(line.split("=")[1:])
                except:
                    pass

                data_dict[head] = val
            try:
                line=next(changefile).replace("\n","")
            except:
                break
        #print(data_row)
    elif command_name == "com.google.refine.model.changes.ColumnAdditionChange":
        line = next(changefile).replace("\n","")
        #print(line)
        while line:
            if line=="/ec/":
                break
            else:
                head = line.split("=")[0]
                val = None
                try:
                    val = "=".join(line.split("=")[1:])
                except:
                    pass
                header_dict[head] = val
                rows = {}
                #print(head)
                if head == "newCellCount":
                    for x in range(int(val)):
                        line = next(changefile).replace("\n","").split(";")
                        val = None
                        try:
                            val = json.loads(line[1])
                        except:
                            pass
                        row = int(line[0])
                        rows[row] = val
                    header_dict["val"] = rows
            try:
                line=next(changefile).replace("\n","")
            except:
                break
    elif command_name == "com.google.refine.model.changes.ColumnRemovalChange" :
        line = next(changefile).replace("\n","")
        #print(line)
        while line:
            if line=="/ec/":
                break
            else:
                head = line.split("=")[0]
                val = None
                try:
                    val = "=".join(line.split("=")[1:])
                except:
                    pass
                header_dict[head] = val
                rows = {}
                #print(head)
                if head == "oldCellCount":
                    for x in range(int(val)):
                        line = next(changefile).replace("\n","").split(";")
                        val = None
                        try:
                            val = json.loads(line[1])
                        except:
                            pass
                        row = int(line[0])
                        rows[row] = val
                    header_dict["val"] = rows
            try:
                line=next(changefile).replace("\n","")
            except:
                break   
    elif command_name == "com.google.refine.model.changes.ColumnSplitChange" :
        line = next(changefile).replace("\n","")
        #print(line)

        new_columns = []
        new_cells = {}
        new_rows = {}       
        
        while line:
            if line=="/ec/":
                break
            else:
                head = line.split("=")[0]
                val = None
                try:
                    val = "=".join(line.split("=")[1:])
                except:
                    pass
                header_dict[head] = val
                #print(head)
                # read new column name
                if head == "columnNameCount":                    
                    for x in range(int(val)):
                        line = next(changefile).replace("\n","")
                        new_columns.append(line)
                    header_dict["new_columns"] = new_columns

                # read new cells
                if head == "rowIndexCount":
                    for x in range(int(val)):
                        line = next(changefile).replace("\n","")
                        index = int(line)
                        new_cells[x] = [None for i in range(int(header_dict["columnNameCount"]))]
                        new_rows[x] = None

                if head == "tupleCount":
                    r_idx = list(new_cells.keys())
                    for i,x in enumerate(range(int(val))):
                        line = next(changefile).replace("\n","")
                        for y in range(int(line)):
                            line = next(changefile).replace("\n","")
                            new_cells[r_idx[i]][y] = line
                    header_dict["new_cells"] = new_cells

                # read new rows values
                if head == "newRowCount":
                    for x in new_rows.keys():
                        line = next(changefile).replace("\n","")
                        new_rows[x] = json.loads(line)
                    header_dict["new_rows"] = new_rows

            try:
                line=next(changefile).replace("\n","")
            except:
                break 
    elif command_name == "com.google.refine.model.changes.ColumnRenameChange":
        line = next(changefile).replace("\n","")
        #print(line)
        while line:
            if line=="/ec/":
                break
            else:
                head = line.split("=")[0]
                val = None
                try:
                    val = "=".join(line.split("=")[1:])
                except:
                    pass
                header_dict[head] = val        

            try:
                line=next(changefile).replace("\n","")
            except:
                break
    elif command_name == "com.google.refine.model.changes.CellChange":
        line = next(changefile).replace("\n","")
        #print(line)
        while line:
            if line=="/ec/":
                break
            else:
                head = line.split("=")[0]
                val = None
                try:
                    val = "=".join(line.split("=")[1:])
                except:
                    pass
                header_dict[head] = val        

            try:
                line=next(changefile).replace("\n","")
            except:
                break
    elif command_name == "com.google.refine.model.changes.ColumnMoveChange":
        line = next(changefile).replace("\n","")
        #print(line)
        while line:
            if line=="/ec/":
                break
            else:
                head = line.split("=")[0]
                val = None
                try:
                    val = "=".join(line.split("=")[1:])
                except:
                    pass
                header_dict[head] = val        

            try:
                line=next(changefile).replace("\n","")
            except:
                break
    elif command_name == "com.google.refine.model.changes.RowReorderChange":
        line = next(changefile).replace("\n","")

        #print(line)
        while line:
            if line=="/ec/":
                break
            else:
                head = line.split("=")[0]
                val = None
                try:
                    val = "=".join(line.split("=")[1:])
                except:
                    pass
                header_dict[head] = val
                if head == "rowIndexCount":
                    row_order = []                    
                    for i,x in enumerate(range(int(val))):
                        line = next(changefile).replace("\n","")
                        row_order.append(int(line))
                    header_dict["row_order"] = row_order

            try:
                line=next(changefile).replace("\n","")
            except:
                break
    elif command_name == "com.google.refine.model.changes.RowRemovalChange":
        line = next(changefile).replace("\n","")
        #print(line)
        while line:
            if line=="/ec/":
                break
            else:
                head = line.split("=")[0]
                val = None
                try:
                    val = "=".join(line.split("=")[1:])
                except:
                    pass
                header_dict[head] = val                
                if head == "rowIndexCount":
                    row_idx_remove = []                    
                    for i,x in enumerate(range(int(val))):
                        line = next(changefile).replace("\n","")
                        row_idx_remove.append(int(line))
                    header_dict["row_idx_remove"] = row_idx_remove
                if head == "rowCount":
                    old_values = []                    
                    for i,x in enumerate(range(int(val))):
                        line = next(changefile).replace("\n","")
                        old_values.append(json.loads(line))
                    header_dict["old_values"] = old_values                                    
            try:
                line=next(changefile).replace("\n","")
            except:
                break
    elif command_name == "com.google.refine.model.changes.RowStarChange":
        line = next(changefile).replace("\n","")
        #print(line)
        while line:
            if line=="/ec/":
                break
            else:
                head = line.split("=")[0]
                val = None
                try:
                    val = "=".join(line.split("=")[1:])
                except:
                    pass
                header_dict[head] = val                             
            try:
                line=next(changefile).replace("\n","")
            except:
                break


    return version,command_name,header_dict,data_row

def search_cell_column(col_mds,cell_index):
    for i,col in enumerate(col_mds):
        if col["cellIndex"] == cell_index:
            return i,col

    return -1, None

def search_cell_column_byname(col_mds,name):
    for i,col in enumerate(col_mds):
        if col["originalName"] == name:
            return i,col
        if col["name"] == name:
            return i,col            

    return -1, None

at = 0 
if __name__ == "__main__":
    import sys
    args = sys.argv
    if len(args)!=2:
        print("usage: {} <openrefine_projectfile>".format(args[0]))
        exit()
    
    file_name = args[1]
    print("Process file: {}".format(file_name))

    # extract project
    #file_name = "airbnb_dirty-csv.openrefine.tar.gz"
    #file_name = "03_poster_demo.openrefine.tar.gz"

    #prepare database

    extract_folder = ".".join(file_name.split(".")[:-2])+".extract"
    try:
        os.mkdir(extract_folder)
    except:
        pass

    print("create extraction folder:",extract_folder)

    db_name = '{}.db'.format(".".join(file_name.split(".")[:-2]))
    if os.path.exists(db_name):
        os.remove(db_name)
    print("create database",db_name)
    conn = sqlite3.connect(db_name)

    cursor = conn.cursor()

    # Create table source
    cursor.execute('''CREATE TABLE IF NOT EXISTS source
                (source_id integer, source_url text, source_format text)''')
    cursor.execute('''CREATE UNIQUE INDEX source_id
    ON source(source_id)''');

    source_id = 0

    # Create table dataset
    cursor.execute('''CREATE TABLE IF NOT EXISTS dataset
                (dataset_id integer, source_id integer)''')
    cursor.execute('''CREATE UNIQUE INDEX dataset_id
    ON dataset(dataset_id)''');
    dataset_id = 0

    # Create table array
    cursor.execute('''CREATE TABLE IF NOT EXISTS array
                (array_id integer, dataset_id integer)''')
    cursor.execute('''CREATE UNIQUE INDEX array_id
    ON array(array_id)''');
    array_id = 0

    # Create table column
    cursor.execute('''CREATE TABLE IF NOT EXISTS column
                (col_id integer, array_id integer)''')
    cursor.execute('''CREATE UNIQUE INDEX col_id 
    ON column(col_id)''');
    col_id = 0

    # row
    cursor.execute('''CREATE TABLE IF NOT EXISTS row
                (row_id integer, array_id integer)''')
    cursor.execute('''CREATE UNIQUE INDEX row_id
    ON row(row_id)''')

    row_id = 0

    # cell
    cursor.execute('''CREATE TABLE IF NOT EXISTS cell
                (cell_id integer, col_id integer, row_id integer)''')
    cursor.execute('''CREATE UNIQUE INDEX cell_id
    ON cell(cell_id)''')            
    cursor.execute('''CREATE UNIQUE INDEX cell_col_row
    ON cell(col_id,row_id)''')         


    cell_id = 0

    # state
    """
    cursor.execute('''CREATE TABLE IF NOT EXISTS state
                (state_id integer, array_id integer, prev_state_id integer, state_label text, command text)''')
    """
    cursor.execute('''CREATE TABLE IF NOT EXISTS state
                (state_id integer, array_id integer, prev_state_id integer)''')
    cursor.execute('''CREATE TABLE IF NOT EXISTS state_detail
                (state_id integer, array_id integer, detail text)''') 
    cursor.execute('''CREATE TABLE IF NOT EXISTS state_command
                (state_id integer, state_label text, command text)''')
    state_id = 0

    
    cursor.execute('''CREATE TABLE IF NOT EXISTS col_dependency
                (state_id integer, output_column integer, input_column integer)''')
    
    # create network for dependency
    col_dependency_graph = nx.DiGraph()

    # content
    cursor.execute('''CREATE TABLE IF NOT EXISTS content
                (content_id integer, cell_id integer, state_id integer, value_id integer, prev_content_id integer)''')
    content_id = 0
    cursor.execute('''CREATE UNIQUE INDEX content_id
    ON content(content_id)''')            
    cursor.execute('''CREATE INDEX content_cell
    ON content(cell_id)''')            
    cursor.execute('''CREATE INDEX content_value_id_idx
    ON content(value_id)''')            
    

    # value
    cursor.execute('''CREATE TABLE IF NOT EXISTS value
                (value_id integer, value_text text)''')
    cursor.execute('''CREATE INDEX value_value_id_idx
    ON value(value_id)''')


    value_id = 0

    # column_schema
    cursor.execute('''CREATE TABLE IF NOT EXISTS column_schema
                (col_schema_id integer, col_id integer, state_id integer, col_type string, col_name string, prev_col_id integer, prev_col_schema_id integer)''')
    col_schema_id = 0

    # row_position
    cursor.execute('''CREATE TABLE IF NOT EXISTS row_position
                (row_pos_id integer, row_id integer, state_id integer, prev_row_id integer, prev_row_pos_id integer)''')

    cursor.execute('''CREATE INDEX row_pos_id_row_pos
    ON row_position(row_pos_id)''')

    cursor.execute('''CREATE INDEX row_id_row_pos_idx
    ON row_position(row_id)''')

    cursor.execute('''CREATE INDEX state_id_row_pos_idx
    ON row_position(state_id)''')


    cursor.execute('''CREATE INDEX prev_row_id_row_pos_idx
    ON row_position(prev_row_id)''')

    cursor.execute('''CREATE INDEX prev_row_posexit_id_row_pos_idx
    ON row_position(prev_row_pos_id)''')



    row_pos_id = 0


   # create additional index

    # create content and state relation
    cursor.execute('''CREATE INDEX content_state_id_idx
    ON content(state_id)''')

    # create content and prev_content_id relation
    cursor.execute('''CREATE INDEX content_prev_content_id_idx
    ON content(prev_content_id)''');



    # create view

    # column schema and position at each state
    cursor.execute('''
    create  view col_each_state as
    select (b.state_id-(select max(state_id) from state s))*-1 as state
    ,b.state_id,a.col_schema_id,a.col_id,a.col_name,a.prev_col_id,a.prev_col_schema_id 
    from column_schema a, (
    WITH RECURSIVE
    state_cnt(state_id) AS ( 
    SELECT -1 UNION ALL 
    SELECT state_id+1 FROM state_cnt
    LIMIT (select max(state_id)+2 from state)
    )
    SELECT state_id FROM state_cnt
    ) b
    where a.state_id<=b.state_id 
    and a.col_schema_id not in
    (
    select a.prev_col_schema_id from column_schema a
    where a.state_id<=b.state_id
    )
    and prev_col_id>=-1    
    ''');

    # row position
    cursor.execute(
    '''create  view row_at_state as
    select (b.state_id-(select max(state_id) from state s))*-1 as state
    ,b.state_id,a.row_pos_id,a.row_id,a.prev_row_id,a.prev_row_pos_id 
    from row_position a, (
    WITH RECURSIVE
    state_cnt(state_id) AS ( 
    SELECT -1 UNION ALL 
    SELECT state_id+1 FROM state_cnt
    LIMIT (select max(state_id)+2 from state)
    ) SELECT state_id FROM state_cnt
    ) b
    where a.state_id<=b.state_id 
    and a.row_pos_id not in
    (
    select a.prev_row_pos_id from row_position a
    where a.state_id<=b.state_id
    )
    ''')

    # column dependency at state
    cursor.execute('''create view col_dep_state as
    WITH RECURSIVE
    col_dep_order(state_id,prev_state_id,prev_input_column,input_column,output_column,level) AS (
    select state_id,state_id,input_column,input_column,output_column,0 from col_dependency cd 
    UNION ALL
    SELECT a.state_id,b.state_id,b.prev_input_column,a.input_column,b.input_column,b.level+1
    FROM col_dependency a, col_dep_order b
    WHERE a.output_column=b.input_column 
    and a.state_id>b.state_id)
    SELECT distinct * from col_dep_order
    ''')
    
    # value at state
    cursor.execute('''
    create  view value_at_state as
    select (b.state_id-(select max(state_id) from state s))*-1 as state
    ,b.state_id,a.content_id,a.prev_content_id,c.value_text,d.row_id,d.col_id 
    from content a, (
    WITH RECURSIVE
    state_cnt(state_id) AS ( 
    SELECT -1 UNION ALL 
    SELECT state_id+1 FROM state_cnt
    LIMIT (select max(state_id)+2 from state)
    ) SELECT state_id FROM state_cnt
    ) b
    NATURAL JOIN value c
    NATURAL JOIN cell d
    where a.state_id<=b.state_id 
    and a.content_id not in
    (
    select a.prev_content_id from content a
    where a.state_id<=b.state_id
    )
    ''')


    cursor.execute("INSERT INTO source VALUES (?,?,?)",(source_id,file_name,"OpenRefine Project File"))

    locex,_ = extract_project(file_name)
    # extract data
    dataset = read_dataset(locex)
    cursor.execute("INSERT INTO dataset VALUES (?,?)",(dataset_id,source_id))
    cursor.execute("INSERT INTO array VALUES (?,?)",(array_id,dataset_id))

    #columns = dataset[0]["cols"].copy()
    #print(columns)
    #exit()
    recipes = {}
    for x in dataset[1]["hists"]:
        recipes[x["id"]] = x
    #exit()
    #print(recipes)
    #exit()
    
    #print([str(x["id"])+".change.zip" for x in dataset[1]["hists"][::-1]])
    #exit()

    # prepare cell changes log
    """
    cell_changes = open("cell_changes.log","w",newline="",encoding="ascii", errors="ignore")
    cell_writer = csv.writer(cell_changes,delimiter=",",quotechar='"',quoting=csv.QUOTE_ALL,escapechar="\\",doublequote=False)
    meta_changes = open("meta_changes.log","w",encoding="ascii", errors="ignore")
    recipe_changes = open("recipe_changes.log","w",encoding="ascii", errors="ignore")
    recipe_writer = csv.writer(recipe_changes,delimiter=",",quotechar='"',quoting=csv.QUOTE_ALL,escapechar="\\",doublequote=False)
    col_changes = open("col_changes.log","w",encoding="ascii", errors="ignore")
    col_writer = csv.writer(col_changes,delimiter=",",quotechar='"',quoting=csv.QUOTE_ALL,escapechar="\\",doublequote=False)
    row_changes = open("row_changes.log","w",encoding="ascii", errors="ignore")
    row_writer = csv.writer(row_changes,delimiter=",",quotechar='"',quoting=csv.QUOTE_ALL,escapechar="\\",doublequote=False)
    col_dependency = open("col_dependency.log","w",encoding="ascii", errors="ignore")
    col_dep_writer = csv.writer(col_dependency,delimiter=",",quotechar='"',quoting=csv.QUOTE_ALL,escapechar="\\",doublequote=False)
    """

    # read history file
    hist_dir = locex+"/history/"
    list_dir = os.listdir(hist_dir)
    #for change in sorted(list_dir)[::-1]:    
    #order = 0    

    """    
    cursor.execute('''CREATE TABLE IF NOT EXISTS cell
                (cell_id number, col_id number, row_id number)''')
    cursor.execute('''CREATE TABLE IF NOT EXISTS value
                (value_id number, value_text text)''')
    cursor.execute('''CREATE TABLE IF NOT EXISTS content
                (content_id number, cell_id number, state_id number, value_id number, prev_content_id)''')
    """

    #print(dataset[0]["cols"],len(dataset[0]["cols"]))
    #print(sorted([x["cellIndex"] for x in dataset[0]["cols"]]))
    #exit()
    
    # insert column maximum index
    for xx in range(int(dataset[0]["maxCellIndex"])+1):
        cursor.execute("INSERT INTO column VALUES (?,?)",(xx,array_id))
        col_dependency_graph.add_node(xx)

    for ix, xx in enumerate(dataset[0]["cols"]):
        #cursor.execute("INSERT INTO column VALUES (?,?)",(col_id,array_id))
        tcid = xx["cellIndex"]
        if ix==0:
            prev_col_id = -1

        cursor.execute('''INSERT INTO column_schema VALUES
            (?,?,?,?,?,?,?)''',(col_schema_id,tcid,state_id-1,"",xx["name"],prev_col_id,-1))

        prev_col_id=tcid
        col_id+=1
        col_schema_id+=1

    cc_ids = list(cursor.execute("SELECT distinct state_id from column_schema order by state_id desc limit 1"))[0][0]            
    ccexs = list(cursor.execute("SELECT col_id,col_schema_id from column_schema  where state_id=? order by col_schema_id asc",(str(cc_ids),)))
    ccexs = [(x[0],x[1]) for x in ccexs]    

    #print(ccexs,len(ccexs))
    for temp_row_id,x in enumerate(dataset[2]["rows"]):
        #print(x["cells"])
        for temp_col_id,y in enumerate(x["cells"]):
            #print(temp_col_id)
            try:
                cursor.execute("INSERT INTO cell VALUES (?,?,?)",(cell_id,temp_col_id,temp_row_id))
            except BaseException as ex:
                print(x["cells"])
                raise ex
            try:
                val = y["v"]
            except:
                val = None
            if type(val)==str:
                val = val.replace("\\","\\\\")
            cursor.execute("INSERT INTO value VALUES (?,?)",(value_id,val))
            cursor.execute("INSERT INTO content VALUES (?,?,?,?,?)",(content_id,cell_id,-1,value_id,-1))
            cell_id+=1
            value_id+=1
            content_id+=1            

            """
            if temp_row_id==0:
                cursor.execute("INSERT INTO column VALUES (?,?)",(col_id,array_id))
                if temp_col_id==0:
                    prev_col_id = None

                cursor.execute('''INSERT INTO column_schema VALUES
                    (?,?,?,?,?,?,?)''',(col_schema_id,col_id,state_id,"","",prev_col_id,None))

                prev_col_id=col_id
                col_id+=1
                col_schema_id+=1
            """

        cursor.execute("INSERT INTO row VALUES (?,?)",(row_id,array_id))
        if temp_row_id==0:
            prev_row_id = -1

        #cursor.execute('''INSERT INTO row_position VALUES
        #    (?,?,?,?)''',(row_pos_id,row_id,state_id,prev_row_id))
        cursor.execute('''INSERT INTO row_position VALUES
            (?,?,?,?,?)''',(row_pos_id,row_id,-1,prev_row_id,-1))
        prev_row_id=row_id
        row_id+=1
        row_pos_id+=1        
            
    #print(row_pos_id)
    #exit()
    #print(temp_row_id,temp_col_id,dataset[0],dataset[1])
    conn.commit()
    #exit()

    #backward
    ccexs_all = list(cursor.execute("SELECT * from column_schema  where state_id=? order by col_schema_id asc",(str(cc_ids),)))

    rcexs_all = list(cursor.execute("SELECT * from row_position  where state_id=? order by row_pos_id asc",(str(cc_ids),)))
    rcexs = list(cursor.execute("SELECT row_id,row_pos_id from row_position  where state_id=? order by row_pos_id asc",(str(cc_ids),)))
    rcexs = [(x[0],x[1]) for x in rcexs]

    print([(x["id"],str(x["id"])+".change.zip") for x in dataset[1]["hists"][::-1]])
    
    """
    for order,(change_id, change) in enumerate([(x["id"],str(x["id"])+".change.zip") for x in dataset[1]["hists"][::-1]]):
        #print(change)
        if change.endswith(".zip"):
            print(change)
            locexzip,_ = open_change(hist_dir,change,target_folder=hist_dir)
            changes = read_change(locexzip+"/change.txt")
            print(changes[1])
    """
    
    #exit()

    for order,(change_id, change) in enumerate([(x["id"],str(x["id"])+".change.zip") for x in dataset[1]["hists"][::-1]]):
        print(change)
        if change.endswith(".zip"):
            print(change)

            locexzip,_ = open_change(hist_dir,change,target_folder=hist_dir)
            # read change
            changes = read_change(locexzip+"/change.txt")

            #recipe_writer.writerow([order,change_id,changes[1],dataset[0]["cols"],recipes[change_id]["description"]])
            
            # insert state
            #prev_state_id = state_id
            #state_id+=1            
            #(state_id number, array_id number, prev_state_id number, state_label text, command text)
            #print(state_id,array_id,prev_state_id,change_id,changes[1])
            #cursor.execute("INSERT INTO state VALUES (?,?,?,?,?)",(state_id,array_id,prev_state_id,change_id,changes[1]))
            if order == 0:
                prev_state_id = -1

            cursor.execute("INSERT INTO state VALUES (?,?,?)",(state_id,array_id,prev_state_id))

            # extract operation history data
            cursor.execute("INSERT INTO state_detail VALUES (?,?,?)",(state_id,array_id,json.dumps(recipes[change_id])))

            #cursor.execute("INSERT INTO state VALUES (?,?,?)",(state_id,array_id,state_id))
            cursor.execute("INSERT INTO state_command VALUES (?,?,?)",(state_id,change_id,changes[1]))

            conn.commit()

            # get rows and cols indexes for the state
            # latest state_id of change
            rc_ids = list(cursor.execute("SELECT distinct state_id from row_position order by state_id desc limit 1"))[0][0]            
            #rcexs = list(cursor.execute("SELECT row_id from row_position  where state_id=? order by row_pos_id asc",(str(rc_ids),)))
            #rcexs = [x[0] for x in rcexs]
            #print(rcexs)
            cc_ids = list(cursor.execute("SELECT distinct state_id from column_schema order by state_id desc limit 1"))[0][0]            
            ccexs = list(cursor.execute("SELECT col_id,col_schema_id from column_schema  where state_id=? order by col_schema_id asc",(str(cc_ids),)))
            ccexs = [(x[0],x[1]) for x in ccexs]

            #ccexs_all = [(x[0],x[1]) for x in ccexs]
            #print(ccexs_all)
            #exit()


            #exit()

            #op-1
            if changes[1] == "com.google.refine.model.changes.MassCellChange":
                #print(changes[3])
                #print(changes)
                is_change = False

                #print(dataset[0]["cols"])
                columns = dataset[0]["cols"].copy()

                cc = search_cell_column_byname(columns,changes[2]["commonColumnName"])
                #print(cc)
                cursor.execute("INSERT INTO col_dependency VALUES (?,?,?)",(state_id,int(cc[1]["cellIndex"]),int(cc[1]["cellIndex"])))
                
                for ch in changes[3]:
                    try:
                        r = int(ch["row"])
                        is_change = True
                    except BaseException as ex:
                        print(ex)
                        continue
                    c = int(ch["cell"])
                    nv = json.loads(ch["new"])
                    if ch["old"] == "":
                        ov = {"v": None}
                    else:
                        try:
                            ov = json.loads(ch["old"])
                        except Exception as ex:
                            print(ch["old"])
                            raise ex

                    
                    #print(ch)
                    #print(ch)
                    #print(dataset[2]["rows"][r])            
                    #print(dataset[2]["rows"][r]["cells"][c],ch)
                    #print(dataset[2]["rows"][r]["cells"][c],nv)                    
                    if dataset[2]["rows"][r]["cells"][c] == nv:
                        # log file recorded here
                        # 0, start, cell_no, row_no, null, 1
                        # <change_id>,<operation_name,<cell_no>,<row_no>,<old_val>,<new_val>,<row_depend>,<cell_depend>
                        dataset[2]["rows"][r]["cells"][c] = ov
                        #print(rcexs[:100])
                        #print(r,c,rcexs[r],rcexs.index(r))
                        val = ov["v"]

                        #cell_changes.write("{},{},{},{},{},{},{},{},{}\n".format(order,change_id,changes[1],r,c,ov,nv,r,c))
                        #cell_writer.writerow([order,change_id,changes[1],r,c,ov,nv,r,c])

                        # write cell_changes

                        # get previous value_id

                        #print(rcexs.index(r))
                        try:
                            cex = cursor.execute("SELECT content_id,cell_id FROM (SELECT a.content_id,a.cell_id,a.state_id FROM content a,cell b where a.cell_id=b.cell_id and b.col_id=? and b.row_id=?) order by state_id desc limit 1",(c,rcexs[r][0]))
                            #cex = cursor.execute("SELECT content_id,cell_id FROM (SELECT a.content_id,a.cell_id,a.state_id FROM content a,cell b where a.cell_id=b.cell_id and b.col_id=? and b.row_id=?) order by state_id desc limit 1",(c,rcexs[r]))
                            #cex = cursor.execute("SELECT content_id,cell_id FROM (SELECT a.content_id,a.cell_id,a.state_id FROM content a,cell b where a.cell_id=b.cell_id and b.col_id=? and b.row_id=?) order by state_id desc limit 1",(c,r))
                        except BaseException as ex:
                            print(dataset[2]["rows"][r]["cells"])
                            print(ccexs,c,len(ccexs),len(dataset[2]["rows"][r]["cells"]))
                            raise ex
                        #print(dataset[2]["rows"][r]["cells"])
                        #print(ccexs,c,len(ccexs),len(dataset[2]["rows"][r]["cells"]))
                        #exit()
                        #print(len(dataset[2]["rows"][r]["cells"]))
                        try:
                            cex = list(cex)[0]
                        except BaseException as ex:
                            print((r,c),list(cex))
                            raise ex
                        
                        if type(val)==str:
                            val = val.replace("\\","\\\\")
                            
                        cursor.execute("INSERT INTO value VALUES (?,?)",(value_id,val))
                        cursor.execute("INSERT INTO content VALUES (?,?,?,?,?)",(content_id,cex[1],state_id,value_id,cex[0]))
                        #print(value_id,content_id)
                        #if state_id==42:
                        #    conn.commit()
                        #    exit()                    
                        value_id+=1
                        content_id+=1


                        #conn.commit()
                        #exit()

                        #print(dataset[2]["rows"][r]["cells"][c],ch)
                #print(dataset[2]["rows"][0]["cells"])
                conn.commit()

                columns = dataset[0]["cols"].copy()
                col_names = [x["name"] for x in columns]
                
                # add dependency column
                ##col_dep_writer.writerow([order,change_id,c_idx,new])                
                #print("recipe:",recipes[change_id])
                #print(len(changes[3]))
                if is_change:
                    description = recipes[change_id]["description"]
                    # removed common function name
                    description = description.replace(".replace","")
                      
                    # find columns from description
                    #print(description)
                    col_names = sorted(col_names,key=lambda x:len(x))[::-1]
                    #print(col_names)
                    all_col = set()
                    for x in col_names:
                        while description.find(x)>=0:
                            all_col.add(x)
                            description = description.replace(x,"")
                                    
                    respective_index = set()
                    #print(all_col)
                    for x in all_col:
                        cc = search_cell_column_byname(columns,x)
                        icol, col = search_cell_column(columns,cc[1]["cellIndex"])  
                        #respective_index.add(icol)
                        respective_index.add(cc[1]["cellIndex"])
                        #print(cc)
                    #print(c,respective_index,cc)
                    #exit()
                    
                    #print(respective_index,c_idx)
                    dependency_index = respective_index - set([c])
                    #for x in dependency_index:
                    #    col_dep_writer.writerow([order,change_id,changes[1],c,x])
                    for x in dependency_index:
                        #cursor.execute("INSERT INTO col_dependency VALUES (?,?,?)",(state_id,[x[1] for x in ccexs_all].index(c),[x[1] for x in ccexs_all].index(x)))
                        cursor.execute("INSERT INTO col_dependency VALUES (?,?,?)",(state_id,c,x))
                        col_dependency_graph.add_edge(x,c)


                #if state_id==42:
                #    exit()


            # op-2
            elif changes[1] == "com.google.refine.model.changes.ColumnAdditionChange":
                #print(changes[2])
                new_cell_index = int(changes[2]["newCellIndex"])
                #print(dataset[0])
                # remove cell_index from coll definition
                c_idx, col = search_cell_column(dataset[0]["cols"],new_cell_index)
                #dataset[0]["cols"].pop(new_cell_index)
                
                columns = dataset[0]["cols"].copy()
                #print(columns)
                col_names = [x["name"] for x in columns]

                # add dependency column
                ##col_dep_writer.writerow([order,change_id,c_idx,new])                
                #print("recipe:",recipes[change_id])
                description = recipes[change_id]["description"]
                # find columns from description
                col_names = sorted(col_names,key=lambda x:len(x))[::-1]
                #print(col_names)
                all_col = set()
                for x in col_names:
                    while description.find(x)>=0:
                        all_col.add(x)
                        description = description.replace(x,"")
                
                respective_index = set()
                for x in all_col:
                    cc = search_cell_column_byname(columns,x)
                    # continue if column not found
                    #if cc[0] == -1:
                    #    continue
                    icol, col = search_cell_column(columns,cc[1]["cellIndex"])  
                    #respective_index.add(icol)
                    respective_index.add(cc[1]["cellIndex"])
                    #print(cc)
                
                #print(respective_index,c_idx,new_cell_index)
                dependency_index = respective_index - set([new_cell_index])
                #for x in dependency_index:
                #    ##col_dep_writer.writerow([order,change_id,changes[1],c_idx,x])
                #    #col_dep_writer.writerow([order,change_id,changes[1],new_cell_index,x])
                for x in dependency_index:
                    #cursor.execute("INSERT INTO col_dependency VALUES (?,?,?)",(state_id,[x[1] for x in ccexs_all].index(new_cell_index),[x[1] for x in ccexs_all].index(x)))
                    cursor.execute("INSERT INTO col_dependency VALUES (?,?,?)",(state_id,new_cell_index,x))
                    col_dependency_graph.add_edge(x,new_cell_index)
                
                #if order == 36:
                #    exit()
                #print(all_col,col)
                #break

                # remove column
                if col["name"] == changes[2]["columnName"]:
                    dataset[0]["cols"].pop(c_idx)
                #print(c_idx,col)
                #print(dataset[0]["cols"])

                # remove column on database state
                """
                temp_ccexs = ccexs.copy()
                #print(temp_ccexs)
                prev_col_schema_id = col_schema_id-1
                #print(temp_ccexs,new_cell_index,c_idx,dataset[0]["cols"])
                temp_ccexs.pop([x[0] for x in ccexs].index(new_cell_index))
                #print(temp_ccexs,new_cell_index,c_idx)
                for v,(vv,vy) in enumerate(temp_ccexs):
                    if v==0:
                        prev_vv = None                        
                    cursor.execute('''INSERT INTO column_schema VALUES
                        (?,?,?,?,?,?,?)''',(col_schema_id,vv,state_id,"","",prev_vv,prev_col_schema_id))
                    prev_vv = vv
                    col_schema_id+=1                
                conn.commit()       
                """

                pop_index = [x[1] for x in ccexs_all].index(new_cell_index)
                try:
                    next_sch = ccexs_all[pop_index+1]                         
                except:
                    next_sch = None

                try:
                    prev_sch_idx = ccexs_all[pop_index-1][1]
                except:
                    prev_sch_idx = None                 

                #print(ccexs_all)

                if next_sch!=None:
                    new_next = (col_schema_id,next_sch[1],state_id,next_sch[3],next_sch[4],prev_sch_idx,next_sch[0])
                    cursor.execute('''INSERT INTO column_schema VALUES
                    (?,?,?,?,?,?,?)''',new_next)
                    
                    col_schema_id+=1
                    ccexs_all[pop_index+1] = new_next

                    #removed column
                    #old_index
                    old_column = ccexs_all[[x[1] for x in ccexs_all].index(new_cell_index)]
                    new_next = (col_schema_id,old_column[1],state_id,old_column[3],old_column[4],-2,old_column[0])
                    cursor.execute('''INSERT INTO column_schema VALUES
                    (?,?,?,?,?,?,?)''',new_next)
                    
                    col_schema_id+=1
                
                ccexs_all.pop([x[1] for x in ccexs_all].index(new_cell_index))
                conn.commit()

                #print(ccexs_all)
                #exit()

                #if at>3:
                #    exit()
                #at+=1
                """
                try:
                    prev_sch = ccexs[c_idx-1]
                except:
                    prev_sch = [None,None]
                try:
                    next_sch = ccexs[c_idx+1]
                except:
                    next_sch = [None,None]
                try:
                    pos_sch = ccexs[c_idx]
                except:
                    pos_sch = [None,None]
                
                cursor.execute('''INSERT INTO column_schema VALUES
                    (?,?,?,?,?,?,?)''',(col_schema_id,next_sch[0],state_id,"","",prev_sch[0],next_sch[1]))
                col_schema_id+=1
                #cursor.execute('''INSERT INTO column_schema VALUES
                #    (?,?,?,?,?,?,?)''',(col_schema_id,None,state_id,"","",pos_sch[0],pos_sch[1]))
                #col_schema_id+=1
                """

                # remove data
                #print(changes[2])
                for c_key,c_val in changes[2]["val"].items():
                    #print(dataset[2]["rows"][c_key]["cells"][new_cell_index])
                    if dataset[2]["rows"][c_key]["cells"][new_cell_index] == c_val:
                        #print(c_key,c_val)
                        dataset[2]["rows"][c_key]["cells"].pop(new_cell_index)
                        ##cell_changes.write("{},{},{},{},{},{},{},{},{}\n".format(order,change_id,changes[1],c_key,new_cell_index,None,c_val,c_key,None))
                        #cell_writer.writerow([order,change_id,changes[1],c_key,new_cell_index,None,c_val,c_key,None])

                """
                for r in dataset[2]["rows"]:
                    # record change of new column here
                    r["cells"].pop(c_idx)
                    #r["cells"][c_idx] = None
                """ 
                #print(dataset[2]["rows"])

                #for r in dataset[2]["rows"]
                #break

            # op-3
            elif changes[1] == "com.google.refine.model.changes.ColumnRemovalChange" :
                oldColumnIndex = int(changes[2]["oldColumnIndex"])
                oldColumn = json.loads(changes[2]["oldColumn"])
                cellIndex = oldColumn["cellIndex"]
                name = oldColumn["name"]                
                #print(dataset[0]["cols"])
                dataset[0]["cols"].insert(oldColumnIndex,oldColumn)
                #print(oldColumn)
                #print(dataset[0]["cols"])
                #print(changes[2])

                for c_key,c_val in changes[2]["val"].items():
                    #print(dataset[2]["rows"][c_key]["cells"][new_cell_index])                
                    dataset[2]["rows"][c_key]["cells"][cellIndex] = c_val
                    ##cell_changes.write("{},{},{},{},{},{},{},{},{}\n".format(order,change_id,changes[1],c_key,cellIndex,c_val,None,c_key,cellIndex))
                    #cell_writer.writerow([order,change_id,changes[1],c_key,cellIndex,c_val,None,c_key,cellIndex])
                #col_writer.writerow([order,change_id,changes[1],cellIndex,None])


                # add column on database state      
                """          
                temp_ccexs = ccexs.copy()
                prev_col_schema_id = col_schema_id-1
                #print(temp_ccexs)
                #print(oldColumnIndex,oldColumn)
                temp_ccexs.insert(oldColumnIndex,(cellIndex,None))
                for v,(vv,vy) in enumerate(temp_ccexs):
                    if v==0:
                        prev_vv = None                        
                    cursor.execute('''INSERT INTO column_schema VALUES
                        (?,?,?,?,?,?,?)''',(col_schema_id,vv,state_id,"","",prev_vv,prev_col_schema_id))
                    prev_vv = vv
                    col_schema_id+=1
                conn.commit()
                """
                
                try:
                    next_sch = ccexs_all[oldColumnIndex]                         
                except:
                    next_sch = None

                try:
                    prev_sch_idx = ccexs_all[oldColumnIndex-1][1]
                except:
                    prev_sch_idx = -1

                #print(ccexs_all)
                new_pos = (col_schema_id,cellIndex,state_id,"",name,prev_sch_idx,-1)
                cursor.execute('''INSERT INTO column_schema VALUES
                (?,?,?,?,?,?,?)''',new_pos)
                col_schema_id+=1

                if next_sch!=None:
                    new_next = (col_schema_id,next_sch[1],state_id,next_sch[3],next_sch[4],cellIndex,next_sch[0])
                    cursor.execute('''INSERT INTO column_schema VALUES
                    (?,?,?,?,?,?,?)''',new_next)
                    col_schema_id+=1
                    ccexs_all[oldColumnIndex] = new_next

                #print(oldColumnIndex)
                ccexs_all.insert(oldColumnIndex,new_pos)                

                cursor.execute("INSERT INTO col_dependency VALUES (?,?,?)",(state_id,-2,cellIndex))

                #print(ccexs_all)
                conn.commit()
                #exit()
                """
                try:
                    prev_sch = ccexs[c_idx-1]
                except:
                    prev_sch = [None,None]
                try:
                    next_sch = ccexs[c_idx+1]
                except:
                    next_sch = [None,None]
                try:
                    pos_sch = ccexs[c_idx]
                except:
                    pos_sch = [None,None]
                
                cursor.execute('''INSERT INTO column_schema VALUES
                    (?,?,?,?,?,?,?)''',(col_schema_id,next_sch[0],state_id,"","",prev_sch[0],next_sch[1]))
                col_schema_id+=1
                #cursor.execute('''INSERT INTO column_schema VALUES
                #    (?,?,?,?,?,?,?)''',(col_schema_id,None,state_id,"","",pos_sch[0],pos_sch[1]))
                #col_schema_id+=1
                """

                """        
                for i,r in enumerate(dataset[2]["rows"]):
                    # record change of new column here
                    try:
                        r["cells"].insert(oldColumnIndex,changes[2][val][i])
                    except:
                        r["cells"].insert(oldColumnIndex,None)
                """
                #break
            
            # op-4
            elif changes[1] == "com.google.refine.model.changes.ColumnSplitChange" :
                #print(changes[2])
                #print(dataset[2]["rows"][0]["cells"])
                # get the cell index
                index_col = []
                for col_name in changes[2]["new_columns"]:
                    cidx = search_cell_column_byname(dataset[0]["cols"],col_name)[1]["cellIndex"]
                    index_col.append(cidx)


                #print(index_col)
                #break
                # remove column metadata
                ori_column = search_cell_column_byname(dataset[0]["cols"],changes[2]["columnName"])
                
                for ind in sorted(index_col)[::-1]:
                    icol, col = search_cell_column(dataset[0]["cols"],ind)
                    dataset[0]["cols"].pop(icol)
                    #print(icol,ori_column)
                    cursor.execute("INSERT INTO col_dependency VALUES (?,?,?)",(state_id,col["cellIndex"],ori_column[1]["cellIndex"]))
                    col_dependency_graph.add_edge(ori_column[1]["cellIndex"],col["cellIndex"])

                # remove cells on row data 
                # print(changes[2]["new_cells"])
                #print(ori_column,index_col)
                for c_key in changes[2]["new_cells"].keys():
                    #print(dataset[2]["rows"][c_key]["cells"])
                    for ind in sorted(index_col)[::-1]:
                        ##cell_changes.write("{},{},{},{},{},{},{},{},{}\n".format(order,change_id,changes[1],c_key,ind,None,dataset[2]["rows"][c_key]["cells"][ind],c_key,ori_column[1]["cellIndex"]))
                        #cell_writer.writerow([order,change_id,changes[1],c_key,ind,None,dataset[2]["rows"][c_key]["cells"][ind],c_key,ori_column[1]["cellIndex"]])
                        try:
                            dataset[2]["rows"][c_key]["cells"].pop(ind)
                        except:
                            continue
                
                #for ind in sorted(index_col)[::-1]:                
                #    col_dep_writer.writerow([order,change_id,changes[1],ind,ori_column[1]["cellIndex"]])

                #print(dataset[2]["rows"][0]["cells"])
                # remove column on database state     
                """           
                temp_ccexs = ccexs.copy()
                print(index_col)
                for ind in sorted(index_col)[::-1]:
                    icol, col = search_cell_column(dataset[0]["cols"],ind)
                    #temp_ccexs.pop(icol)
                    temp_ccexs.pop([x[0] for x in temp_ccexs].index(ind))

                #temp_ccexs.pop([x[0] for x in ccexs].index(new_cell_index))
                prev_col_schema_id = col_schema_id-1
                for v,(vv,vy) in enumerate(temp_ccexs):
                    if v==0:
                        prev_vv = None                        
                    cursor.execute('''INSERT INTO column_schema VALUES
                        (?,?,?,?,?,?,?)''',(col_schema_id,vv,state_id,"","",prev_vv,prev_col_schema_id))
                    prev_vv = vv
                    col_schema_id+=1                
                conn.commit()
                """

                for ind in sorted(index_col)[::-1]:
                    #print(ccexs_all)
                    pop_index = [x[1] for x in ccexs_all].index(ind)
                    try:
                        next_sch = ccexs_all[pop_index+1]                         
                    except:
                        next_sch = None

                    try:
                        prev_sch_idx = ccexs_all[pop_index-1][1]
                    except:
                        prev_sch_idx = -1                 

                    #print(ccexs_all)

                    if next_sch!=None:
                        new_next = (col_schema_id,next_sch[1],state_id,next_sch[3],next_sch[4],prev_sch_idx,next_sch[0])
                        cursor.execute('''INSERT INTO column_schema VALUES
                        (?,?,?,?,?,?,?)''',new_next)
                        col_schema_id+=1
                        ccexs_all[pop_index+1] = new_next
                    

                    # put and end for the new column
                    cur_idx = ccexs_all[pop_index]
                    new_next = (col_schema_id,cur_idx[1],state_id,cur_idx[3],cur_idx[4],-2,cur_idx[0])
                    cursor.execute('''INSERT INTO column_schema VALUES
                    (?,?,?,?,?,?,?)''',new_next)
                    col_schema_id+=1

                    ccexs_all.pop([x[1] for x in ccexs_all].index(ind))      

                    #print(ccexs_all)        

                #print(ccexs_all)
                conn.commit()

                #exit()
                #break
            
            # op-5
            elif changes[1] == "com.google.refine.model.changes.ColumnRenameChange":
                #print(changes[2])                
                index_col = search_cell_column_byname(dataset[0]["cols"],changes[2]["oldColumnName"])[1]
                if index_col == None:
                    index_col = search_cell_column_byname(dataset[0]["cols"],changes[2]["newColumnName"])[1]
                #print(index_col,changes[2],dataset[0]["cols"])
                # continue if column is not found
                #if index_col == None:
                #    continue
                index_col["name"] = changes[2]["oldColumnName"]
                #print(changes[2])
                #exit()
                            
                cursor.execute("INSERT INTO col_dependency VALUES (?,?,?)",(state_id,index_col["cellIndex"],index_col["cellIndex"]))

                """
                prev_col_schema_id = col_schema_id-1
                for v,(vv,vy) in enumerate(temp_ccexs):
                    if v==0:
                        prev_vv = None                        
                    cursor.execute('''INSERT INTO column_schema VALUES
                        (?,?,?,?,?,?,?)''',(col_schema_id,vv,state_id,"","",prev_vv,prev_col_schema_id))
                    prev_vv = vv
                    col_schema_id+=1
                """
                #exit()
                #print(ccexs_all)                
                pop_index = [x[1] for x in ccexs_all].index(index_col["cellIndex"])
                old_pop = ccexs_all[pop_index]

                new_pop = (col_schema_id,old_pop[1],state_id,old_pop[3],changes[2]["oldColumnName"],old_pop[5],old_pop[0])
                cursor.execute('''INSERT INTO column_schema VALUES
                (?,?,?,?,?,?,?)''',new_pop)
                col_schema_id+=1
                ccexs_all[pop_index] = new_pop
                
                #print(ccexs_all)       
                conn.commit()                    
                # should be metadata change
                #exit()

                #break            

            # op-6
            elif changes[1] == "com.google.refine.model.changes.CellChange":
                ch = changes[2]
                try:
                    r = int(ch["row"])
                except BaseException as ex:
                    print(ex)
                    continue
                #print(ch)
                c = int(ch["cell"])
                print(ch)
                try:
                    nv = json.loads(ch["new"])
                except:
                    nv = {"v":""}
                try:
                    ov = json.loads(ch["old"])
                except:
                    ov = {"v":""}

                cursor.execute("INSERT INTO col_dependency VALUES (?,?,?)",(state_id,c,c))

                #print(dataset[2]["rows"][r])            
                #print(dataset[2]["rows"][r]["cells"][c],ch)
                #print(dataset[2]["rows"][r]["cells"][c],nv)
                if dataset[2]["rows"][r]["cells"][c] == nv:
                    # log file recorded here
                    # 0, start, cell_no, row_no, null, 1
                    # <change_id>,<operation_name,<cell_no>,<row_no>,<old_val>,<new_val>,<row_depend>,<cell_depend>
                    #print("change exists")
                    dataset[2]["rows"][r]["cells"][c] = ov
                    ##cell_changes.write("{},{},{},{},{},{},{},{},{}\n".format(order,change_id,changes[1],c,r,nv,ov,c,r))
                    #cell_writer.writerow([order,change_id,changes[1],c,r,nv,ov,c,r])

                try:
                    cex = cursor.execute("SELECT content_id,cell_id FROM (SELECT a.content_id,a.cell_id,a.state_id FROM content a,cell b where a.cell_id=b.cell_id and b.col_id=? and b.row_id=?) order by state_id desc limit 1",(c,rcexs[r][0]))
                except BaseException as ex:
                    print(dataset[2]["rows"][r]["cells"])
                    print(ccexs,c,len(ccexs),len(dataset[2]["rows"][r]["cells"]))
                    raise ex
                #print(dataset[2]["rows"][r]["cells"])
                #print(ccexs,c,len(ccexs),len(dataset[2]["rows"][r]["cells"]))
                #exit()
                #print(len(dataset[2]["rows"][r]["cells"]))
                try:
                    cex = list(cex)[0]
                except BaseException as ex:
                    print((r,c),list(cex))
                    raise ex

                if type(ov["v"])==str:
                    ov["v"] = ov["v"].replace("\\","\\\\")

                cursor.execute("INSERT INTO value VALUES (?,?)",(value_id,ov["v"]))
                cursor.execute("INSERT INTO content VALUES (?,?,?,?,?)",(content_id,cex[1],state_id,value_id,cex[0]))
                value_id+=1
                content_id+=1
                #exit()
                #break                    

            # op-7
            elif changes[1] == "com.google.refine.model.changes.ColumnMoveChange":
                columns = dataset[0]["cols"]
                #print(ccexs_all)
                cursor.execute("INSERT INTO col_dependency VALUES (?,?,?)",(state_id,ccexs_all[int(changes[2]["newColumnIndex"])][1],ccexs_all[int(changes[2]["newColumnIndex"])][1]))
                cursor.execute("INSERT INTO col_dependency VALUES (?,?,?)",(state_id,ccexs_all[int(changes[2]["oldColumnIndex"])][1],ccexs_all[int(changes[2]["oldColumnIndex"])][1]))

                temp = columns[int(changes[2]["newColumnIndex"])]
                columns[int(changes[2]["newColumnIndex"])] = columns[int(changes[2]["oldColumnIndex"])]
                columns[int(changes[2]["oldColumnIndex"])] = temp


                #changes[2]["oldColumnIndex"] = 7
                #print(changes[2])

                # should be metadata change
                #col_writer.writerow([order,change_id,changes[1],int(changes[2]["newColumnIndex"]),int(changes[2]["oldColumnIndex"])])

                """
                temp_ccexs = ccexs.copy()
                print(temp_ccexs)
                temp = temp_ccexs[int(changes[2]["newColumnIndex"])]
                temp_ccexs[int(changes[2]["newColumnIndex"])] = temp_ccexs[int(changes[2]["oldColumnIndex"])]
                temp_ccexs[int(changes[2]["oldColumnIndex"])] = temp
                print(temp_ccexs)
                prev_col_schema_id = col_schema_id-1
                for v,(vv,vy) in enumerate(temp_ccexs):
                    if v==0:
                        prev_vv = None                        
                    cursor.execute('''INSERT INTO column_schema VALUES
                        (?,?,?,?,?,?,?)''',(col_schema_id,vv,state_id,"","",prev_vv,prev_col_schema_id))
                    prev_vv = vv
                    col_schema_id+=1
                conn.commit()
                """

                # switch columnIndex if newColumn > oldColumn
                if changes[2]["newColumnIndex"] > changes[2]["oldColumnIndex"]:
                    temp = changes[2]["newColumnIndex"]
                    changes[2]["newColumnIndex"] = changes[2]["oldColumnIndex"]
                    changes[2]["oldColumnIndex"] = temp
                
                ccexs_all_c = ccexs_all.copy()
                pop_index = int(changes[2]["newColumnIndex"])
                new_pop = ccexs_all_c[pop_index]
                
                #print(new_pop)

                try:
                    next_sch = ccexs_all_c[pop_index+1]                         
                except:
                    next_sch = None

                try:
                    prev_sch_idx = ccexs_all_c[pop_index-1][1]
                except:
                    prev_sch_idx = -1
                
                pop_index2 = int(changes[2]["oldColumnIndex"])
                new_pop2 = ccexs_all_c[pop_index2]
                try:
                    next_sch2 = ccexs_all_c[pop_index2+1]                         
                except:
                    next_sch2 = None

                try:
                    prev_sch_idx2 = ccexs_all_c[pop_index2-1][1]
                except:
                    prev_sch_idx2 = -1
                
                print(pop_index,pop_index2,prev_sch_idx,prev_sch_idx2,new_pop,new_pop2,next_sch,next_sch2)
                if next_sch2!=None:
                    new_next2 = (col_schema_id,next_sch2[1],state_id,next_sch2[3],next_sch2[4],new_pop[1],next_sch2[0]) 
                    cursor.execute('''INSERT INTO column_schema VALUES
                    (?,?,?,?,?,?,?)''',new_next2)
                    ccexs_all[pop_index2+1] = new_next2
                    col_schema_id+=1

                if prev_sch_idx2==new_pop[1]:
                    new_popp = (col_schema_id,new_pop[1],state_id,new_pop[3],new_pop[4],new_pop2[1],new_pop[0])
                    cursor.execute('''INSERT INTO column_schema VALUES
                    (?,?,?,?,?,?,?)''',new_popp)
                    col_schema_id+=1
                    ccexs_all[pop_index2] = new_popp
                else:
                    if next_sch!=None:
                        new_next = (col_schema_id,next_sch[1],state_id,next_sch[3],next_sch[4],new_pop2[1],next_sch[0]) 
                        cursor.execute('''INSERT INTO column_schema VALUES
                        (?,?,?,?,?,?,?)''',new_next)
                        ccexs_all[pop_index2+1] = new_next
                        col_schema_id+=1

                    new_popp = (col_schema_id,new_pop[1],state_id,new_pop[3],new_pop[4],prev_sch_idx2,new_pop[0])
                    cursor.execute('''INSERT INTO column_schema VALUES
                    (?,?,?,?,?,?,?)''',new_popp)
                    col_schema_id+=1
                    ccexs_all[pop_index2] = new_popp                        

                new_popp2 = (col_schema_id,new_pop2[1],state_id,new_pop2[3],new_pop2[4],prev_sch_idx,new_pop2[0])
                cursor.execute('''INSERT INTO column_schema VALUES
                (?,?,?,?,?,?,?)''',new_popp2)
                col_schema_id+=1
                ccexs_all[pop_index] = new_popp2
                
                print(ccexs_all)

                conn.commit()
                #exit()            
                """
                #print(ccexs_all)

                if next_sch!=None:
                    new_next = (col_schema_id,next_sch[1],state_id,next_sch[3],next_sch[4],prev_sch_idx,next_sch[0])
                    cursor.execute('''INSERT INTO column_schema VALUES
                    (?,?,?,?,?,?,?)''',new_next)
                    col_schema_id+=1
                    ccexs_all[pop_index+1] = new_next
                
                ccexs_all.pop([x[1] for x in ccexs_all].index(new_cell_index))                

                old_pop = ccexs_all[pop_index]

                new_pop = (col_schema_id,old_pop[1],state_id,old_pop[3],changes[2]["oldColumnName"],old_pop[5],old_pop[0])
                cursor.execute('''INSERT INTO column_schema VALUES
                (?,?,?,?,?,?,?)''',new_pop)
                col_schema_id+=1
                ccexs_all[pop_index] = new_pop
                
                #print(ccexs_all)       
                conn.commit()         
                """
                #exit()

                #break
            elif changes[1] == "com.google.refine.model.changes.RowReorderChange":
                columns = dataset[0]["cols"].copy()
                col_names = [x["name"] for x in columns]
                description = json.dumps(recipes[change_id]["operation"]["sorting"]["criteria"])
                #print(description)
                # find columns from description
                col_names = sorted(col_names,key=lambda x:len(x))[::-1]
                all_col = set()
                for x in col_names:
                    while description.find(x)>=0:
                        all_col.add(x)
                        description = description.replace(x,"")
                set_idx = []
                for x in all_col:
                    cc = search_cell_column_byname(columns,x)
                    set_idx.append(cc[1]["cellIndex"])
                
                #print(state_id,set_idx)
                #exit()
                for x in set_idx:
                    cursor.execute("INSERT INTO col_dependency VALUES (?,?,?)",(state_id,x,x))
                #for x in range(len(columns)):
                #    cursor.execute("INSERT INTO col_dependency VALUES (?,?,?)",(state_id,x,x))
                

                #print(set_idx)
                #print(all_col)
                #exit()
                # add column dependency

                # create a new row set
                new_rows = dataset[2]["rows"]
                old_rows = np.array(new_rows)
                #print(len(rcexs))

                """
                for i,li in enumerate(changes[2]["row_order"]):
                    old_rows[li] = new_rows[i]
                    #row_writer.writerow([order,change_id,changes[1],li,i])
                    if i == 0:
                        prev_vv = -1
                    #print(li)
                    temp_rid = rcexs[li]
                    #temp_rid = rcexs_all[li]
                    cursor.execute("INSERT INTO row_position VALUES (?,?,?,?)",(row_pos_id,temp_rid,state_id,int(prev_vv)))
                    prev_vv = temp_rid

                    #rcexs[li] = rcexs[i]
                    #rcexs[i] = temp_rid
                    row_pos_id+=1
                """

                #print(row_pos_id)

                rcexs_cp = rcexs.copy()
                for i,li in enumerate(changes[2]["row_order"]):
                    old_rows[li] = new_rows[i]
                    #row_writer.writerow([order,change_id,changes[1],li,i])
                    if i == 0:
                        prev_vv = -1
                    #print(li)
                    temp_rid = rcexs[i]
                    if rcexs_cp[li] != rcexs[i]:
                        #print(temp_rid)
                        rcexs_cp[li] = (temp_rid[0],row_pos_id)
                        #temp_rid = rcexs_all[li]
                        #cursor.execute("INSERT INTO row_position VALUES (?,?,?,?,?)",(row_pos_id,temp_rid[0],state_id,int(prev_vv),temp_rid[1]))

                        #rcexs[li] = rcexs[i]
                        #rcexs[i] = temp_rid
                        #row_pos_id+=1
                    prev_vv = temp_rid[0]
                #print(row_pos_id)
                #exit()
                row_dict_id = {x[0]:x[1] for x in rcexs}
                #print(max([x[1] for x in rcexs]))
                #print(row_dict_id[3])

                #exit()

                # old row_order
                old_row_dict = {}
                for i,x in enumerate(rcexs):
                    if i == 0:
                        prev = -1
                    else:
                        prev = rcexs[i-1][0]
                    old_row_dict[x[0]] = (prev,i,x)

                #if state_id==6:
                #    print(old_row_dict[2])
                #    exit()

                new_row_dict = {}
                for i,x in enumerate(rcexs_cp):
                    if i == 0:
                        prev = -1
                    else:
                        prev = rcexs_cp[i-1][0]
                    new_row_dict[x[0]] = (prev,i,x)

                #print(row_pos_id)
                for i,x in enumerate(new_row_dict.items()):                    
                    if x[1][0] != old_row_dict[x[0]][0]:
                        #cursor.execute("INSERT INTO row_position VALUES (?,?,?,?,?)",(x[1][2][1],int(x[0]),state_id,int(x[1][0]),old_row_dict[x[0]][2][1]))
                        #rcexs_cp[x[1][1]] = x[1][2]
                        cursor.execute("INSERT INTO row_position VALUES (?,?,?,?,?)",(row_pos_id,int(x[0]),state_id,int(x[1][0]),old_row_dict[x[0]][2][1]))
                        rcexs_cp[x[1][1]] = (x[1][2][0],row_pos_id)
                        row_pos_id+=1
                    else:                        
                        rcexs_cp[x[1][1]] = old_row_dict[x[0]][2]
                        #row_pos_id+=1                                
                
                #print(row_pos_id)
                #exit()
                """
                for i,li in enumerate(changes[2]["row_order"]):
                    if i == 0:
                        prev_vv = -1
                    temp_rid = rcexs_cp[i]
                    if rcexs_cp[i] != rcexs[i]:
                        #cursor.execute("INSERT INTO row_position VALUES (?,?,?,?,?)",(temp_rid[1],temp_rid[0],state_id,int(prev_vv),rcexs[i][1]))
                        cursor.execute("INSERT INTO row_position VALUES (?,?,?,?,?)",(temp_rid[1],temp_rid[0],state_id,int(prev_vv),row_dict_id[temp_rid[0]]))
                        #print(temp_rid[1])
                    prev_vv = temp_rid[0]
                """
                
                #print(rcexs[:100])

                #print(len(rcexs),len(rcexs_cp))
                #print(rcexs[:20])
                #print(rcexs_cp[:20])
                #exit()

                rcexs = rcexs_cp

                #print(rcexs[:100])
                #exit()

                #print(rcexs_cp)
                #exit()
                """
                rcexs_cp = rcexs.copy()
                for i,li in enumerate(changes[2]["row_order"]):
                    #temp_tt = rcexs[li]
                    rcexs_cp[li] = rcexs[i]
                    #rcexs[i] = temp_tt
                rcexs = rcexs_cp
                print(rcexs[:100])
                """

                # new patch
                #rcexs_all = old_rows

                #exit()
                
                conn.commit()
                #exit()

                # Write log file
                for i,li in enumerate(changes[2]["row_order"]):
                    for j,jj in enumerate(new_rows[i]["cells"]):
                        #print(j,jj,old_rows[i]["cells"][j])
                        
                        try:
                            ov = old_rows[i]["cells"][j]
                        except:
                            ov = None
                        
                        ##cell_changes.write("{},{},{},{},{},{},{},{},{}\n".format(order,change_id,changes[1],j,i,jj,ov,j,i))       
                        #cell_writer.writerow([order,change_id,changes[1],i,j,jj,ov,i,j])
                


                dataset[2]["rows"] = old_rows.tolist()
                #break                
            elif changes[1] == "com.google.refine.model.changes.RowRemovalChange":  
                #temp_rows = list(range(row_id))
                temp_rows = [x[0] for x in rcexs]
                tt_rows = temp_rows.copy()

                for i,idx in enumerate(changes[2]["row_idx_remove"]):
                    #print(idx)
                    #j = len(changes[2]["row_idx_remove"])-i-1
                    #print(j)
                    dataset[2]["rows"].insert(idx,changes[2]["old_values"][i])

                    """
                    for temp_col_id,y in enumerate(x["cells"]):
                        cursor.execute("INSERT INTO cell VALUES (?,?,?)",(cell_id,temp_col_id,temp_row_id))
                        try:
                            val = y["v"]
                        except:
                            val = None
                        cursor.execute("INSERT INTO value VALUES (?,?)",(value_id,val))
                        cursor.execute("INSERT INTO content VALUES (?,?,?,?,?)",(content_id,cell_id,state_id,value_id,-1))
                        cell_id+=1
                        value_id+=1
                        content_id+=1
                    #(row_pos_id number, row_id number, state_id number, prev_row_id number)
                    """
                    # add row
                    cursor.execute("INSERT INTO row VALUES (?,?)",(row_id,array_id))
                    temp_rows.insert(idx,row_id)

                    # exchanges row                    
                    if i<len(changes[2]["row_idx_remove"])-1:
                        for ii in range(changes[2]["row_idx_remove"][i],changes[2]["row_idx_remove"][i+1]):
                            #row_writer.writerow([order,change_id,changes[1],ii+1+i,ii])           
                            pass
                            #cursor.execute("INSERT INTO row_position VALUES (?,?,?,?)",(row_pos_id,ii+1+i,state_id,ii))
                            #row_pos_id+=1
                            
                    else:
                        for ii in range(changes[2]["row_idx_remove"][i],len(dataset[2]["rows"])-1):
                            #row_writer.writerow([order,change_id,changes[1],ii+1+i,ii])
                            pass
                            #cursor.execute("INSERT INTO row_position VALUES (?,?,?,?)",(row_pos_id,ii+1+i,state_id,ii))
                            #row_pos_id+=1
                    #print(idx,dataset[2]["rows"][idx])
                    
                    # add values
                    for v,vv in enumerate(changes[2]["old_values"][i]["cells"]):                        
                        # add one row
                        cursor.execute("INSERT INTO cell VALUES (?,?,?)",(cell_id,v,row_id))

                        # add to col dependency only for the first time
                        #if i == 0:
                        #    cursor.execute("INSERT INTO col_dependency VALUES (?,?,?)",(state_id,int(v),int(v)))

                        try:
                            val = vv["v"]
                        except:
                            val = None                            
                        if type(val)==str:
                            val = val.replace("\\","\\\\")
                        cursor.execute("INSERT INTO value VALUES (?,?)",(value_id,val))
                        cursor.execute("INSERT INTO content VALUES (?,?,?,?,?)",(content_id,cell_id,state_id,value_id,-1))
                        cell_id+=1
                        value_id+=1
                        content_id+=1
                    #print(changes[2]["old_values"][i],row_id,idx)

                    row_id+=1
                    #exit()
                    
                conn.commit()                            
                
                rcexs_cp = rcexs.copy()


                # old row_order
                old_row_dd = {}
                for i,x in enumerate(tt_rows):
                    if i == 0:
                        prev = -1
                    else:
                        prev = tt_rows[i-1]
                    old_row_dd[x] = prev

                new_row_dd = {}
                for i,x in enumerate(temp_rows):
                    if i == 0:
                        prev = -1
                    else:
                        prev = temp_rows[i-1]
                    new_row_dd[x] = (prev,i)
                
                #print(len(old_row_dd),len(new_row_dd))
                #exit()

                old_row_dict = {}
                for i,x in enumerate(rcexs_cp):
                    if i == 0:
                        prev = -1
                    else:
                        prev = rcexs_cp[i-1][0]
                    old_row_dict[x[0]] = x[1]

                for i,x in enumerate(new_row_dd.items()):
                    if x[0] not in old_row_dd.keys():
                        #print(x)
                        cursor.execute("INSERT INTO row_position VALUES (?,?,?,?,?)",(row_pos_id,int(x[0]),state_id,int(x[1][0]),-1))
                        rcexs.insert(x[1][1],(int(x[0]),row_pos_id))
                        row_pos_id+=1
                    else:
                        if x[1][0] != old_row_dd[x[0]]:
                            cursor.execute("INSERT INTO row_position VALUES (?,?,?,?,?)",(row_pos_id,int(x[0]),state_id,int(x[1][0]),old_row_dict[x[0]]))
                            rcexs[x[1][1]] = (int(x[0]),row_pos_id)
                            row_pos_id+=1
                
                rcexs = rcexs[:len(temp_rows)]

                #print(row_pos_id)
                #print(rcexs_cp[:20])
                #print(rcexs[:20])
                #exit()

                '''
                for v,vv in enumerate(temp_rows):
                    if v==0:
                        prev_vv = -1

                    #cursor.execute("INSERT INTO row_position VALUES (?,?,?,?,?)",(row_pos_id,vv,state_id,int(prev_vv),-1))
                    prev_vv = vv
                    #patch
                    try:
                        if rcexs_cp[v] != rcexs[vv]:
                            pass
                            #rcexs.insert(v,(vv,row_pos_id))
                            #row_pos_id+=1
                            #print("executed1")
                    except:
                        print(row_pos_id)
                        rcexs.insert(v,(vv,row_pos_id))
                        row_pos_id+=1
                        #print("executed2")
                
                #print(len(temp_rows),len(tt_rows))

                #print(row_pos_id)
                #exit()

                rcexs = rcexs[:len(temp_rows)]
                #print(rcexs)
                #exit()
                #rewrite = False

                # old row_order
                old_row_dict = {}
                for i,x in enumerate(rcexs_cp):
                    if i == 0:
                        prev = -1
                    else:
                        prev = rcexs_cp[i-1][0]
                    old_row_dict[x[0]] = prev

                new_row_dict = {}
                for i,x in enumerate(rcexs):
                    if i == 0:
                        prev = -1
                    else:
                        prev = rcexs[i-1][0]
                    new_row_dict[x[0]] = prev                    
                
                #print(old_row_dict,new_row_dict)
                #exit()

                #row_dict_id = {x[0]:x[1] for x in rcexs}
                #print(row_dict_id[3])
                row_dict_id = {x[0]:x[1] for x in rcexs_cp}
                #print(row_dict_id[3])
                #exit()

                for key,val in new_row_dict.items():
                    if key not in old_row_dict.keys():
                        print(key,val)
                        #cursor.execute("INSERT INTO row_position VALUES (?,?,?,?,?)",(rcexs[key][1],rcexs[key][0],state_id,val,-1))
                        cursor.execute("INSERT INTO row_position VALUES (?,?,?,?,?)",(rcexs[key][1],key,state_id,val,-1))
                    elif val!=old_row_dict[key]:
                        #print(rcexs[key][0])
                        print(key,val)
                        print((rcexs[key][1],key,state_id,val,row_dict_id[key]))
                        #cursor.execute("INSERT INTO row_position VALUES (?,?,?,?,?)",(rcexs[key][1],rcexs[key][0],state_id,val,row_dict_id[key]))
                        cursor.execute("INSERT INTO row_position VALUES (?,?,?,?,?)",(rcexs[key][1],key,state_id,val,row_dict_id[key]))
                
                #print(new_row_dict[2],old_row_dict[2])
                print(rcexs_cp[:20])
                print(rcexs[:20])
                exit()

                #{[for i,x in enumerate(rcexs_cp)]}
                
                """
                for v,vv in enumerate(temp_rows):
                    #print(v,vv)
                    if v==0:
                        prev_vv = -1

                    temp_rid = rcexs[v]
                    #print(temp_rid)
                    #print(prev_vv)
                    if rewrite:
                        cursor.execute("INSERT INTO row_position VALUES (?,?,?,?,?)",(temp_rid[1],temp_rid[0],state_id,int(prev_vv),rcexs_cp[vv][1]))
                        prev_vv = temp_rid[0]
                        #row_pos_id+=1
                    try:
                        #print(v,vv)
                        if rcexs_cp[v] != rcexs[v]:
                            #rcexs_cp[vv] = (temp_rid[v],row_pos_id)
                        
                            cursor.execute("INSERT INTO row_position VALUES (?,?,?,?,?)",(temp_rid[1],temp_rid[0],state_id,int(prev_vv),-1))
                            prev_vv = temp_rid[0]
                            rewrite = True
                            #rcexs[li] = rcexs[i]
                            #rcexs[i] = temp_rid
                            #row_pos_id+=1
                    except:
                        pass
                    prev_vv = temp_rid[0]
                """
                #exit()
                '''
                    
                conn.commit()
                #print(rcexs[:100])
                #exit()
                #for i,idx in 
                #row_riter.writerow([order,change_id,ori_column[1]["cellIndex"],ind])
                #break        
            elif changes[1] == "com.google.refine.model.changes.RowStarChange":
                #print(changes[2])
                old_val = True if changes[2]["oldStarred"] == "true" else False
                new_val = True if changes[2]["newStarred"] == "true" else False
                #print(dataset[2]["rows"][int(changes[2]["row"])]["starred"])
                #print(dataset[2]["rows"][idx])
                #print(int(changes[2]["row"]),dataset[2]["rows"][int(changes[2]["row"])])
                if dataset[2]["rows"][int(changes[2]["row"])]["starred"] == new_val:
                    print("change starred")
                    dataset[2]["rows"][int(changes[2]["row"])]["starred"] = old_val
                #break
            else:
                print(changes[2])
                #continue            
            
            prev_state_id=state_id
            #if state_id==32:
            #    exit()
            state_id+=1
        #break
        #print(dataset[0])
        #print(dataset[2]["rows"][0]["cells"])

    #pass
    #print(dataset[0])
    #print(dataset[2]["rows"][0]["cells"])
    
    prev_source_id = source_id
    source_id+=1
    prev_dataset_id=dataset_id        
    dataset_id+=1
    prev_array_id = array_id
    array_id+=1    
    conn.commit()
    #exit()

    # extract table to csv
    #print(col_dependency_graph.edges())
    #print([col_dependency_graph.subgraph(c).copy() for c in nx.connected_components(col_dependency_graph.to_undirected())])
    sub_graphs = [col_dependency_graph.subgraph(c).copy() for c in nx.connected_components(col_dependency_graph.to_undirected())]
    for xx in sub_graphs:
        indeg = dict(xx.in_degree())
        #print(dict(indeg))
        to_add = [n for n in indeg if indeg[n] == 0]
        for yy in to_add:
            cursor.execute("INSERT INTO col_dependency VALUES (?,?,?)",(-1,yy,-1))
        #print(to_add)
    conn.commit()


    import os
    extract_folder = ".".join(file_name.split(".")[:-2])+".extract"
    try:
        os.mkdir(extract_folder)
    except:
        pass
    print("Extracting CSV:")
    import pandas as pd
    import csv
    tables = ["array","cell","column","column_schema","content","dataset","row","row_position","source","state","value","state_command","col_dependency"]
    table_files = []
    for table in tables:
        print("Extract {}".format(table))
        df = pd.read_sql_query("SELECT * from {}".format(table), conn)
        df.to_csv("{}/{}.csv".format(extract_folder,table),sep=",",index=False,header=None,quotechar='"',escapechar="\\",doublequote=False,quoting=csv.QUOTE_NONNUMERIC)
        table_files.append((table,"{}/{}.csv".format(extract_folder,table)))
    print("csv extracted at: {}".format(extract_folder))
    # prepare datalog files
    #print("prepare datalog files: {}/facts.pl".format(extract_folder))
    with open("{}/facts.pl".format(extract_folder),"w") as writer:
        for x,y in table_files:
            with open(y,"r") as file:
                for l in file:
                    # remove whitespace
                    l = l[:-1]
                    writer.write("{}({}).\n".format(x,l))

    tables = ["array","column","column_schema","dataset","row","row_position","source","state","state_command","content","col_dependency"]
    table_files = []
    for table in tables:
        print("Extract {}".format(table))
        df = pd.read_sql_query("SELECT * from {}".format(table), conn)
        df.to_csv("{}/{}.csv".format(extract_folder,table),sep=",",index=False,header=None,quotechar='"',escapechar="\\",doublequote=False,quoting=csv.QUOTE_NONNUMERIC)
        table_files.append((table,"{}/{}.csv".format(extract_folder,table)))
    print("csv extracted at: {}".format(extract_folder))
    # prepare datalog files
    #print("prepare datalog files: {}/facts.pl".format(extract_folder))
    with open("{}/facts_content_excluded.pl".format(extract_folder),"w") as writer:
        for x,y in table_files:
            with open(y,"r") as file:
                for l in file:
                    # remove whitespace
                    l = l[:-1]
                    writer.write("{}({}).\n".format(x,l))                    
    print("prepared datalog files: {}/facts_content_excluded.pl".format(extract_folder))
                    
    conn.close()