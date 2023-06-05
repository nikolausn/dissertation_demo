import networkx as nx
import sqlite3
import pandas as pd
import numpy as np
from graphviz import Source
import json
from nltk import ngrams
from regex import P


class WorkflowViz:
    def __init__(self,state_viz=None):
        self.state_idx = {}
        if state_viz != None:
            self.state_viz = state_viz
            self.refresh_state(state)
        else:
            self.state_viz = []
        self._relation = set()
        self.relation = []

    def add_state(self,step,step_id):
        self.state_viz.append(StateViz(step,[],step_id=step_id))
        self.refresh_state()

    def refresh_state(self):
        for i,x in enumerate(self.state_viz):
            self.state_idx[x.step] = i

    def get_state(self,state):
        return self.state_viz[self.state_idx[state]]

    def add_relation(self,input_step,input_col,output_step,output_col,relation):
        if (input_step,input_col,output_step,output_col,relation) not in self._relation:
            self._relation.add((input_step,input_col,output_step,output_col,relation))
            self.relation.append(RelationViz(input_step,input_col,output_step,output_col,relation))
        #self.get_state(input_step).get_data(input_col).next = self.get_state(output_step).get_data(output_col)
        #self.get_state(output_step).get_data(output_col).prev = self.get_state(input_step).get_data(input_col)

class RelationViz:
    def __init__(self,input_step,input_col,output_step,output_col,relation):
        #self.input_data = None
        #self.output_data = None
        #self.relation = None        
        self.add_relation(input_step,input_col,output_step,output_col,relation)

    def add_relation(self,input_step,input_col,output_step,output_col,relation):
        #self.input_data = self.get_state(input_step).get_data(input_col)
        #self.output_data = self.get_state(output_step).get_data(output_col)
        self.relation = relation
        self.input_step = input_step
        self.input_col = input_col
        self.output_step = output_step
        self.output_col = output_col
        

class StateViz:
    def __init__(self,step,data_list,step_id=None):
        self.step = step
        self.step_id = step_id
        self.col_idx = {}
        self.data_list = data_list
        self.refresh_index()
        #for i,x in enumerate(self.data_list):
        #    self.col_idx[x.col_id] = i
    
    def get_data(self,col_id):
        return self.data_list[self.col_idx[col_id]]

    def add_data(self,col_id,col_name,row=None,value=None,is_used=False):
        self.data_list.append(DataViz(col_id,col_name,row,value,is_used))

    def refresh_index(self):
        for i,x in enumerate(self.data_list):
            self.col_idx[x.col_id] = i

class DataViz:
    def __init__(self,col_id,col_name,row=None,value=None,is_used=False):
        self.col_id = col_id
        self.col_name = col_name
        self.row = row
        self.value = value
        self.next = set()
        self.prev = set()
        self.is_used = is_used


class ProvenanceExplorer:    
    def __init__(self,dbfile=None):
        if dbfile!=None:
            self.dbfile = dbfile
            #self.conn = sqlite3.connect(self.dbfile)
            #self.cursor = self.conn.cursor()
            self.open_connection()

    def open_connection(self):
        self.conn = sqlite3.connect(self.dbfile,check_same_thread=False)
        self.cursor = self.conn.cursor()
    
    def close_connection(self):
        self.conn.close()

    def get_removed_rows(self):
        return pd.read_sql_query("SELECT * FROM row_position where prev_row_pos_id=-1 and state_id>-1",self.conn)

    def get_number_of_state(self):
        return pd.read_sql_query("""
        SELECT max(state_id) as num_state FROM state 
        """, self.conn)

    def get_state_order(self):
        return pd.read_sql_query("""
        WITH RECURSIVE
        state_prev(ord,array_id,prev_state_id,state_id) AS
        (SELECT 0,array_id,prev_state_id,state_id from state where prev_state_id=-1
        UNION ALL
        select b.ord+1,a.array_id,a.prev_state_id,a.state_id  from state a,state_prev b
        where b.state_id=a.prev_state_id and b.array_id=a.array_id)
        select * from state_prev order by ord
        """,self.conn)

    def get_linear_recipe(self):
        #return pd.read_sql_query("""
        ##select * from state_command sc NATURAL JOIN state_detail sd NATURAL JOIN state
        #""",self.conn)

        #return pd.read_sql_query("""select * from state_command a,state_detail b, state c
        #where a.state_id = b.state_id
        #and a.state_id=c.state_id;
        #""",self.conn)

        return pd.read_sql_query("""
        select * from state_detail sd NATURAL JOIN state
        """,self.conn)

    

    def get_changes_each_state(self,state):
        if type(state) in (range,list):
            state=list(state)
            state = ",".join([str(x) for x in state ])
        return pd.read_sql_query("""
        SELECT (state_id-(select max(state_id)+1 from state s))*-1 as state,substr(command,33) as operation,col_id,count(1) as cell_changes FROM state 
        NATURAL JOIN content NATURAL JOIN value  NATURAL JOIN cell NATURAL JOIN state_command
        where state in ({state})
        group by state,col_id
        order by state asc        
        """.format(state=state), self.conn)

    def get_column_at_state(self,state):
        """
        col_at_state = list(self.cursor.execute("select * from col_each_state where state={}".format(state)))
        names = list(map(lambda x: x[0], self.cursor.description))   
        table_dict = {}
        for x in names:
            table_dict[x] = []
        for x in col_at_state:
            for i,y in enumerate(x):
                table_dict[names[i]].append(y)
        
        return pd.DataFrame(table_dict)
        """
        if type(state) in (range,list):
            state=list(state)
            state = ",".join([str(x) for x in state ])        
        
        #return pd.read_sql_query("select * from col_each_state where state={}".format(state), self.conn)
        return pd.read_sql_query("select * from col_each_state where state in ({state}) order by state".format(state=state), self.conn)

    def get_col_at_state_order(self,state):
        if type(state) in (range,list):
            state=list(state)
            state = ",".join([str(x) for x in state ])        

        return pd.read_sql_query("""
        SELECT * FROM (WITH RECURSIVE
        col_state_order(state,col_id,col_name,prev_col_id,level) AS (
        select state,col_id,col_name,prev_col_id,0 from col_each_state 
        where prev_col_id=-1
        UNION ALL
        SELECT a.state,a.col_id, a.col_name, a.prev_col_id,b.level+1 
        FROM col_each_state a, col_state_order b
        WHERE a.prev_col_id=b.col_id and a.state=b.state)
        SELECT state,col_id,col_name,prev_col_id,level from col_state_order
        ) where state in ({state})
        order by state,level
        """.format(state=state)
        , self.conn)

    def get_step_to_state(self,step):
        max_state = self.get_number_of_state().num_state.values[0]
        return max_state - step

    def get_state_to_step(self,state):
        max_state = self.get_number_of_state().num_state.values[0]
        return (state - max_state) * -1
        #{state}-(select max(state_id) from state s))*-1  

    def get_row_at_state(self,state):
        return pd.read_sql_query("select * from row_at_state where state={}".format(state), self.conn)

    def get_row_at_state(self,state):
        return pd.read_sql_query("select * from row_at_state where state={}".format(state), self.conn)

    def get_row_at_state_order(self,state):
        '''
        return pd.read_sql_query("""
        SELECT * FROM (WITH RECURSIVE
        row_state_order(state,row_id,prev_row_id,level) AS (
        select state,row_id,prev_row_id,0 from row_at_state 
        where prev_row_id=-1
        and state={}
        UNION ALL
        SELECT a.state,a.row_id,a.prev_row_id,b.level+1 
        FROM row_at_state a, row_state_order b
        WHERE a.prev_row_id=b.row_id and a.state=b.state)
        SELECT state,row_id,prev_row_id,level from row_state_order
        order by state asc)
        """.format(state)
        , self.conn)
        '''
        return pd.read_sql_query("""
        select distinct * from (
        WITH RECURSIVE
        row_state_order(state,state_id,row_id,prev_row_id,level) AS (
        select ({state}-(select max(state_id) from state s))*-1 as state
        ,{state} as state_id,rp.row_id,rp.prev_row_id,0 from row_position rp 
        where rp.state_id<={state} and
        rp.row_pos_id not IN 
        (
        select prev_row_pos_id from row_position rp2 
        where rp2.state_id<={state}
        and prev_row_pos_id>-1
        )
        and prev_row_id=-1
        UNION ALL
        SELECT a.state,a.state_id,a.row_id,a.prev_row_id,b.level+1 
        FROM (select ({state}-(select max(state_id) from state s))*-1 as state
        ,{state} as state_id,rp.row_pos_id,rp.row_id,rp.prev_row_id,rp.prev_row_pos_id from row_position rp 
        where rp.state_id<={state} and
        rp.row_pos_id not IN 
        (
        select prev_row_pos_id from row_position rp2 
        where rp2.state_id<={state}
        and prev_row_pos_id>-1
        )) a, row_state_order b
        WHERE a.prev_row_id=b.row_id and a.state_id=b.state_id)
        SELECT state,state_id,row_id,prev_row_id,level from row_state_order
        )        
        """.format(state=state),self.conn)

    def get_row_logic_to_idx(self,state,row):
        row_logic = self.get_row_at_state_order(state)
        return row_logic.iloc[row].row_id

    def get_row_idx_to_logic(self,state,row):
        row_idx = self.get_row_at_state_order(state)
        return row_idx.row_id.tolist().index(row)        

    def get_col_logic_to_idx(self,state,col):
        col_logic = self.get_col_at_state_order(state)
        #print(col_logic)
        return col_logic.iloc[col].col_id        
        #return col_logic.col_id.tolist().index(col)
    
    def get_col_idx_to_logic(self,state,col):
        col_idx = self.get_col_at_state_order(state)
        return col_idx.col_id.tolist().index(col) 

    def get_values_at_state(self,state):
        print("get_values_state",state)
        return pd.read_sql_query("""
        select ({state}-(select max(state_id) from state s))*-1 as state
        ,{state} as state_id,a.content_id,a.prev_content_id,c.value_text,d.row_id,d.col_id 
        from content a
        NATURAL JOIN value c
        NATURAL JOIN cell d
        where a.state_id<={state}
        and a.content_id not in
        (
        select a.prev_content_id from content a
        where a.state_id<={state}
        )
        """.format(state=state), self.conn)

    def get_snapshot_at_state(self,state):
        row_order = self.get_row_at_state_order(self.get_step_to_state(state))
        col_order = self.get_col_at_state_order(state)
        #print(col_order)
        values = self.get_values_at_state(self.get_step_to_state(state))

        max_row_id = row_order.row_id.max()
        max_col_id = col_order.col_id.max()
        new_arr = np.empty((max_row_id+1,max_col_id+1),dtype=object)
        #print(new_arr.shape)
        for x in values.to_records():
            try:
                new_arr[x.row_id,x.col_id] = (x.value_text,int(x.row_id),int(x.col_id))
            except:
                continue
        
        col_sort = sorted(col_order[["level","col_id","col_name"]].values.tolist(),key=lambda x:x[0])
        row_sort = sorted(row_order[["level","row_id"]].values.tolist(),key=lambda x:x[0])
        
        #print(col_names,row_names)
        snapshot_pd = pd.DataFrame(new_arr)
        #snapshot_pd.columns = col_names
        snapshot_pd = snapshot_pd.iloc[[x[1] for x in row_sort],[x[1] for x in col_sort]]
        snapshot_pd.columns = [x[2] for x in col_sort]
        return snapshot_pd

    def get_cell_history(self,row,col,is_id=True):
        # if it's a logical id, get the id
        if not is_id:
            col_id = self.get_col_logic_to_idx(state,col)
            row_id = self.get_row_logic_to_idx(state,row)
        else:
            col_id = col
            row_id = row
        
        return pd.read_sql_query("""
        select * from value_at_state a  where row_id={} and col_id={} order by state
        """.format(row_id,col_id), self.conn)

    def get_state_dependency(self,state):
        if type(state) in (range,list):
            state=list(state)
            state = ",".join([str(x) for x in state ])        

        return pd.read_sql_query("""
        select distinct (a.state_id-(select max(state_id)+1 from state s))*-1 as state,substr(c.command,33) command,(b.state_id-(select max(state_id)+1 from state s))*-1 as dep_state,substr(d.command,33) as dep_command,a.input_column,a.output_column
        from col_dependency a,col_dep_state b,state_command c,state_command d
        where a.input_column = b.prev_input_column
        and a.state_id>-1
        and a.state_id<=b.state_id
        and a.state_id=c.state_id 
        and b.state_id=d.state_id
        and state in ({state})
        order by state,dep_state asc;       
         """.format(state=state), self.conn)


    def get_column_dependency(self,col_id):        
        return pd.read_sql_query("""
        select distinct (a.state_id-(select max(state_id)+1 from state s))*-1 as state,substr(c.command,33) command,substr(d.command,33) as dep_command,a.input_column,a.output_column
        from col_dependency a,state_command c,state_command d
        where 
        a.state_id>-1
        and a.state_id=c.state_id 
        and d.state_id=a.state_id
        and output_column={col_id}
        order by state asc;       
         """.format(col_id=col_id), self.conn)

    def get_input_column_dependency(self,col_id):
        return pd.read_sql_query("""
        select distinct (a.state_id-(select max(state_id)+1 from state s))*-1 as state,substr(c.command,33) command,substr(d.command,33) as dep_command,a.input_column,a.output_column
        from col_dependency a,state_command c,state_command d
        where 
        a.state_id>-1
        and a.state_id=c.state_id 
        and d.state_id=a.state_id
        and input_column={col_id}
        order by state asc;       
         """.format(col_id=col_id), self.conn)

    def get_all_column_dependency(self):        
        return pd.read_sql_query("""
        select distinct (a.state_id-(select max(state_id)+1 from state s))*-1 as state,substr(c.command,33) command,substr(d.command,33) as dep_command,a.input_column,a.output_column,a.state_id
        from col_dependency a,state_command c,state_command d
        where 
        a.state_id>-1
        and a.state_id=c.state_id 
        and d.state_id=a.state_id
        order by state asc;       
         """, self.conn)        

    def get_all_state_command(self):        
        return pd.read_sql_query("""
        select * from state_command NATURAL JOIN state_detail
         """, self.conn)         

    def lineage_viz(self,row,col,step=-1):
        if step>-1:
            #col = self.get_col_logic_to_idx(self.get_step_to_state(step),col)
            #row = self.get_row_logic_to_idx(self.get_step_to_state(step),row)
            col = self.get_col_logic_to_idx(step,col)
            row = self.get_row_logic_to_idx(step,row)
        col_state_dep = self.get_column_dependency(col).state.tolist()
        state_dep = self.get_state_dependency(col_state_dep)
        dep_state = state_dep.dep_state.unique()
        all_dep = self.get_all_column_dependency()
        state_dep = all_dep[all_dep.state.isin(dep_state)]
        #print(dep_state)        
        #xx = state_dep[state_dep.command!=]state_dep[["input_column","output_column"]].values.flatten()
        #xx = state_dep[state_dep.command!="RowReorderChange"][["input_column","output_column"]].values.flatten()
        xx = state_dep[state_dep.command!="ColumnMoveChange"][["input_column","output_column"]].values.flatten()
        #print(xx)
        xx_col = {x:x for x in xx}.keys()
        #print(xx_col)
        xx_hist = {}
        for x in xx_col:
            xx_hist[x] = self.get_cell_history(row,x)
        
        dep_state = set(dep_state)
        dep_state.add(0)
        dep_state = list(set(dep_state))
        dep_state = sorted(dep_state)
        #first_state = max(dep_state)
        #if first_state<self.get_number_of_state().num_state.values[0]:
        #    dep_state.append(self.get_number_of_state().num_state.values[0])
        print(dep_state)

        output_trace = []
        for i,x in enumerate(dep_state[1:]):
            dep_x = all_dep[all_dep.state==x]
            print(x,dep_x)
            #print(dep_x)
            for y in dep_x[["input_column","output_column"]].to_records():
                if y.input_column in xx_hist.keys():
                    xx_input = xx_hist[y.input_column]
                if y.output_column in xx_hist.keys():
                    xx_output = xx_hist[y.output_column]
                #print(dep_state[i-1],x)
                #print("input",xx_input[xx_input.state==dep_state[(i+1)-1]])
                #print("output",xx_output[xx_output.state==x])
                output_trace.append((dep_state[(i+1)-1],x,y.input_column,y.output_column,xx_input[xx_input.state==dep_state[(i+1)-1]],xx_output[xx_output.state==x]))
                #print(dep_x)
        
        is_processed = []

        s_ah = {}
        for s in output_trace:
            try:
                s_ah[s[0]]
            except:
                s_ah[s[0]] = []
            s_ah[s[0]].append(s)
            if s[0]>0:
                try:
                    s_ah[s[0]-1]
                except:
                    s_ah[s[0]-1] = []
                s_ah[s[0]-1].append(s)
            
        s0_s1_rel = []

        wf = WorkflowViz()

        for s in output_trace:
            s0 = int(s[0])
            s1 = int(s[1])
            row_logic_s0 = self.get_row_at_state_order(s0)
            row_logic_s1 = self.get_row_at_state_order(s1)
            
            if s0 not in is_processed:
                wf.add_state(s0,step_id=self.get_state_to_step(s0))            

                for x in self.get_col_at_state_order(s0).to_records():
                    if x.col_id not in xx_col:
                        wf.get_state(s0).add_data(x.col_id,x.col_name,row=row_logic_s0[row_logic_s0.row_id==row].level.values[0],value=None)
                    else:
                        xx_tt = xx_hist[x.col_id]
                        xx_tt = xx_tt[xx_tt.state==s0]                     
                        #print(xx_tt)   
                        wf.get_state(s0).add_data(x.col_id,x.col_name,row=row_logic_s0[row_logic_s0.row_id==row].level.values[0],value=xx_tt.value_text.values[0],is_used=True)
                is_processed.append(s0)

            if s1 not in is_processed:
                wf.add_state(s1,step_id=self.get_state_to_step(s1))            

                for x in self.get_col_at_state_order(s1).to_records():
                    if x.col_id not in xx_col:
                        wf.get_state(s1).add_data(x.col_id,x.col_name,row=row_logic_s1[row_logic_s1.row_id==row].level.values[0],value=None)
                    else:
                        xx_tt = xx_hist[x.col_id]
                        xx_tt = xx_tt[xx_tt.state==s1]  
                        #print(xx_tt)   
                        wf.get_state(s1).add_data(x.col_id,x.col_name,row=row_logic_s1[row_logic_s1.row_id==row].level.values[0],value=xx_tt.value_text.values[0],is_used=True)
                is_processed.append(s1)
            
            wf.add_relation(s0,s[2],s1,s[3],all_dep[all_dep.state==s1].command.values[0]) 
            wf.add_relation(s0,col,s1,col,all_dep[all_dep.state==s1].command.values[0])
            #wf.add_relation(self,input_step,input_col,output_step,output_col,relation)

            '''
            s_port+="""
            struct{}:port{}o -> struct{}:port{}i [ label="{}" ];
            """.format(s0,s[2],s1,s[3],all_dep[all_dep.state==s1].command.values[0])
            s0_s1_rel.append((s0,s[2],s1,s[3]))

            #print(col,s0_ex_col,s1_ex_col)

            if s0_ex_col and s1_ex_col and ((s0,col,s1,col) not in s0_s1_rel):
                s_port+="""
                struct{}:port{}o -> struct{}:port{}i [ label="{}" ];
                """.format(s0,col,s1,col,all_dep[all_dep.state==s1].command.values[0])
                s0_s1_rel.append((s0,col,s1,col))            
            '''
        return wf


    def get_cell_lineage(self,row,col,step=-1):
        if step>-1:
            #col = self.get_col_logic_to_idx(self.get_step_to_state(step),col)
            #row = self.get_row_logic_to_idx(self.get_step_to_state(step),row)
            col = self.get_col_logic_to_idx(step,col)
            row = self.get_row_logic_to_idx(step,row)
        col_state_dep = self.get_column_dependency(col).state.tolist()
        state_dep = self.get_state_dependency(col_state_dep)
        dep_state = state_dep.dep_state.unique()
        all_dep = self.get_all_column_dependency()
        state_dep = all_dep[all_dep.state.isin(dep_state)]
        #print(dep_state)        
        #xx = state_dep[state_dep.command!=]state_dep[["input_column","output_column"]].values.flatten()
        #xx = state_dep[state_dep.command!="RowReorderChange"][["input_column","output_column"]].values.flatten()
        xx = state_dep[state_dep.command!="ColumnMoveChange"][["input_column","output_column"]].values.flatten()
        #print(xx)
        xx_col = {x:x for x in xx}.keys()
        #print(xx_col)
        xx_hist = {}
        for x in xx_col:
            xx_hist[x] = self.get_cell_history(row,x)
        
        dep_state = set(dep_state)
        dep_state.add(0)
        dep_state = list(set(dep_state))
        dep_state = sorted(dep_state)
        #print(dep_state)

        is_colored = False

        output_trace = []
        for i,x in enumerate(dep_state[1:]):
            dep_x = all_dep[all_dep.state==x]
            #print(dep_x)
            for y in dep_x[["input_column","output_column"]].to_records():
                xx_input = xx_hist[y.input_column]
                xx_output = xx_hist[y.output_column]
                #print(dep_state[i-1],x)
                #print("input",xx_input[xx_input.state==dep_state[(i+1)-1]])
                #print("output",xx_output[xx_output.state==x])
                output_trace.append((dep_state[(i+1)-1],x,y.input_column,y.output_column,xx_input[xx_input.state==dep_state[(i+1)-1]],xx_output[xx_output.state==x]))
            #print(dep_x)
        
        from graphviz import Source
        temp = """
        digraph {
        rankdir=TB;
        node [ shape=record style="rounded,filled" fillcolor="#FFFFCC" peripheries=1 fontname=Helvetica];
        edge [ style="filled" fontname=Helvetica];
        splines="line";

        """
        s_coll=""
        s_port=""
        is_processed = []

        all_state_command = self.get_all_state_command()

        s_ah = {}
        for s in output_trace:
            try:
                s_ah[s[0]]
            except:
                s_ah[s[0]] = []
            s_ah[s[0]].append(s)
            if s[0]>0:
                try:
                    s_ah[s[0]-1]
                except:
                    s_ah[s[0]-1] = []
                s_ah[s[0]-1].append(s)

                
        #print(s_ah)

        s0_s1_rel = []

        for s in output_trace:
            s0 = int(s[0])
            s1 = int(s[1])
            row_logic_s0 = self.get_row_at_state_order(s0)
            row_logic_s1 = self.get_row_at_state_order(s1)
            
            s0_ex_col = False
            s1_ex_col =False

            if col in self.get_col_at_state_order(s0).col_id.values:
                s0_ex_col = True
            
            if col in self.get_col_at_state_order(s1).col_id.values:
                s1_ex_col = True

            if s0 not in is_processed:
                col_port = []
                for x in self.get_col_at_state_order(s0).to_records():
                    is_ss = False
                    for ss in s_ah[s0]:
                        if (x.col_id==col):
                            xx_col = xx_hist[col]
                            #print("xx_col",xx_col,s0)
                            #print(xx_col)
                            #xx_col = xx_col[xx_col.state_id==self.get_state_to_step(ss[0])]
                            xx_col = xx_col[xx_col.state==s0]
                            #print(xx_col)
                            xx = "{{<port{col_id}i>{col_name}|row:{row}|<port{col_id}o>{value}}}".format(col_id=x.col_id,col_name=x.col_name,value=xx_col.value_text.values[0],row=row_logic_s1[row_logic_s1.row_id==row].level.values[0])
                            is_ss = True
                        elif (x.col_id==ss[4].col_id.values[0]):
                            #print(x)
                            xx = "{{<port{col_id}i>{col_name}|row:{row}|<port{col_id}o>{value}}}".format(col_id=x.col_id,col_name=x.col_name,value=ss[4].value_text.values[0],row=row_logic_s0[row_logic_s0.row_id==row].level.values[0])
                            #xx = "{{<port{col_id}>{col_name}|row:{row}|<port{col_id}>{value}}}".format(col_id=x.col_id,col_name=x.col_name,value=ss[4].value_text.values[0],row=row_logic_s0.iloc[row].row_id)
                            is_ss = True

                    #print(x.col_id)

                    if not is_ss:
                        #xx = "{{<port{col_id}>{col_name}}}".format(col_id=x.col_id,col_name=x.col_name)
                        xx = "{{<port{}i>{}|<port{}o>}}".format(x.col_id,x.col_name,x.col_id)
                        #xx = "<port{}>{}\n{}".format(x.col_id,x.col_name,s[4].value_text.values[0])
                    col_port.append(xx)
                    
                #print("state_step:",s[0],step)
                if int(s[0])>=int(step)-1 and not is_colored:
                    color_step = "color=blue"
                    is_colored = True
                else:
                    color_step = ""
                
                #color_step = ""

                s_coll+="""
                subgraph cluster_{step} {{
                    label="state {step}";
                    {color}
                    fontname=Helvetica                    

                    struct{s_num}[
                        label = "{s_label}";
                    ];
                }}
                """.format(step=s[0],s_num=s0,s_label="|".join(col_port),color=color_step)
                is_processed.append(s0)

            if s1 not in is_processed:
                col_port = []
                for x in self.get_col_at_state_order(s1).to_records():
                    is_ss = False
                    for ss in s_ah[s0]:
                        if (x.col_id==col):
                            xx_col = xx_hist[col]
                            #xx_col = xx_col[xx_col.state_id==self.get_state_to_step(ss[0])]
                            xx_col = xx_col[xx_col.state==s1]
                            #print(xx_col)
                            xx = "{{<port{col_id}i>{col_name}|row:{row}|<port{col_id}o>{value}}}".format(col_id=x.col_id,col_name=x.col_name,value=xx_col.value_text.values[0],row=row_logic_s1[row_logic_s1.row_id==row].level.values[0])
                            is_ss = True
                        elif (x.col_id==ss[5].col_id.values[0]):
                            #xx = "{{<port{col_id}>{col_name}|row:{row}|<port{col_id}>{value}}}".format(col_id=x.col_id,col_name=x.col_name,value=ss[5].value_text.values[0],row=row_logic_s1.iloc[row].row_id)                                         
                            xx = "{{<port{col_id}i>{col_name}|row:{row}|<port{col_id}o>{value}}}".format(col_id=x.col_id,col_name=x.col_name,value=ss[5].value_text.values[0],row=row_logic_s1[row_logic_s1.row_id==row].level.values[0])
                            is_ss = True
                            
                            #xx = "{{<port{}i>{}|<port{}o>{}}}".format(x.col_id,x.col_name,x.col_id,s[5].value_text.values[0])
                            #xx = "<port{}>{}\n{}".format(x.col_id,x.col_name,s[5].value_text.values[0])

                    #print(x.col_id)
                    if x.col_id == col:
                        s1_ex_col = True

                    if not is_ss:
                        #xx = "{{<port{col_id}>{col_name}}}".format(col_id=x.col_id,col_name=x.col_name)
                        xx = "{{<port{}i>{}|<port{}o>}}".format(x.col_id,x.col_name,x.col_id)                    

                    col_port.append(xx)
                '''
                for x in orpe.get_col_at_state_order(s1).to_records():
                    xx = "<port{}>".format(x.col_id)+x.col_name
                    col_port.append(xx)
                '''

                #print("state_step:",s[0],step)
                if int(s[0])>=int(step)-1 and not is_colored:
                    color_step = "color=blue"
                    is_colored = True
                else:
                    color_step = ""
                
                s_coll+="""
                subgraph cluster_{step} {{
                    label="state {step}";
                    {color}
                    fontname=Helvetica                    

                    struct{s_num}[
                        label = "{s_label}";
                    ];
                }}
                """.format(step=s1,s_num=s1,s_label="|".join(col_port),color=color_step)
                is_processed.append(s1)
            
            
            #op_name = all_dep[all_dep.state==s1].command.values[0]
            try:                
                print(s1,self.get_step_to_state(s1)+1)
                #print(all_state_command[all_state_command.state_id==s1].detail.values)
                print(all_state_command[all_state_command.state_id==self.get_step_to_state(s1-1)].values)

                ops = json.loads(all_state_command[all_state_command.state_id==self.get_step_to_state(s1-1)].detail.values[0])
                try:
                    op_name = "{}: {}".format(ops["operation"]["op"].replace("core/",""),ops["operation"]["expression"])
                except:
                    op_name = "{}".format(ops["operation"]["op"].replace("core/",""))
            except:
                op_name = all_dep[all_dep.state==s1].command.values[0]
            #print(all_state_command)

            op_name = op_name.replace('"','\\"')
            #print(op_name)

            #s_port+="""
            #struct{}:port{}o -> struct{}:port{}i [ label="{}" ];
            #""".format(s0,s[2],s1,s[3],all_dep[all_dep.state==s1].command.values[0])
            s_port+="""
            struct{}:port{}o -> struct{}:port{}i [ label="{}" ];
            """.format(s0,s[2],s1,s[3],op_name)
            s0_s1_rel.append((s0,s[2],s1,s[3]))

            #print(col,s0_ex_col,s1_ex_col)

            
            if s0_ex_col and s1_ex_col and ((s0,col,s1,col) not in s0_s1_rel):
                #s_port+="""
                #struct{}:port{}o -> struct{}:port{}i [ label="{}" ];
                #""".format(s0,col,s1,col,all_dep[all_dep.state==s1].command.values[0])
                s_port+="""
                struct{}:port{}o -> struct{}:port{}i [ label="{}" ];
                """.format(s0,col,s1,col,"")
                s0_s1_rel.append((s0,col,s1,col))

            #print(s0_s1_rel)

            '''
            s_port+="""
            struct{source} -> struct{target} [
                tailport = port{porto}
                headport = port{porti}
                label = {label}
            ];
            """.format(source=s0,target=s1,porto=s[2],porti=s[3],label=all_dep[all_dep.state==s1].command.values[0])
            '''

            #if s>0:
            #    s_port+="""
            #    struct{}:port1 -> struct{}:port2 [ label="xyz" ];
            #    """.format(s-1,s)

        temp+=s_coll
        temp+="""
        {}
        }}
        """.format(s_port)
        s = Source(temp)
        #s.view()
        #s
        return output_trace,s,temp

    def reuse_recipe(self,col_id):
        state_dep = self.get_state_dependency(self.get_column_dependency(col_id).state.tolist())        
        #print(",".join([str(x) for x in state_dep.dep_state.unique()]))
        parallel_workflow = self.parallel_workflow()
        _,parallel_graph,_ = self.gv_template(parallel_workflow)
        recipes, _ = self.parallel_state(parallel_graph,"col{}_0".format(col_id))
        output_recipe = []
        for rr in recipes:
            for r in rr:
                if r not in output_recipe:
                    output_recipe.append(r)
        print("recipes:",output_recipe)
        return output_recipe


        """
        print("state_dep",state_dep)
        
        state_input_dep = self.get_state_dependency(self.get_input_column_dependency(col_id).state.tolist())        
        print("state_input_dep",state_input_dep)
        state_input_dep = state_input_dep["output_column"]!=col_id
        state_dep = state_dep.append(state_input_dep)                
        
        #print("all_dep",self.get_all_column_dependency(col_id))
        print([str(self.get_step_to_state(x)+1) for x in state_dep.dep_state.unique()])
        """
        state_det = pd.read_sql("select * from state_detail where state_id in ({})".format(",".join([str(self.get_step_to_state(x)+1) for x in state_dep.dep_state.unique()])),self.conn)
        state_det = {x.state_id:json.loads(x.detail)["operation"] for x in state_det[["detail","state_id"]].to_records()}
        #print(state_det)
        state_list = []
        state_ids = []
        for x in self.get_state_order().sort_values("ord",ascending=False).to_records():
            try:
                #print(state_det[x.state_id])
                state_list.append(state_det[x.state_id])
                state_ids.append(x.state_id)
            except:
                pass
        
        #state_list = list(filter(lambda x: x!=None,[json.loads(x)["operation"] for x in state_det.detail]))
        return state_list,state_ids


    def gv_template(self,stack_process,process=None):

        nodes_def = """
        digraph "[stackcollapse]" {
        node [style=filled fillcolor="#f8f8f8"]
        """

        edge_def = ""
                
        # workflow template gv format

        header = """
        /* Start of top-level graph */
        digraph Workflow {
        rankdir=TB

        /* Start of double cluster for drawing box around nodes in workflow */
        subgraph cluster_workflow_box_outer { label=""; penwidth=0
        subgraph cluster_workflow_box_inner { label=""; penwidth=0
        """

        single_process = """
        /* Style for nodes representing atomic programs in workflow */
        node[shape=box style=filled fillcolor="#CCFFCC" peripheries=1 fontname=Helvetica]

        /* Nodes representing atomic programs in workflow */
        """
        #state_4 [shape=record rankdir=LR label="{<f0> step 4 (to_date) | grel\:value.replace(/\\/i,'') | 16492 cells changed}"];


        group_process_nodes = """
        node[shape=box style=filled fillcolor="#CCCCFF" peripheries=1 fontname=Helvetica]
        """
        #state_5 [shape=record rankdir=LR label="{<f0> state_5 |<f1> core/mass-edit\nclustering\ngroup clustering 4 processes}"];

        freq_pattern_nodes = """
        node[shape=box style=filled fillcolor="#CCFFFF" peripheries=1 fontname=Helvetica]
        """
        #state_5 [shape=record rankdir=LR label="{<f0> state_5 |<f1> core/mass-edit\nclustering\ngroup clustering 4 processes}"];

        column_nodes = """
        /* Style for nodes representing non-parameter data channels in workflow */
        node[shape=box style="rounded,filled" fillcolor="#FFFFCC" peripheries=1 fontname=Helvetica]

        /* Nodes for non-parameter data channels in workflow */
        """
        #"col12_0-date" [shape=record rankdir=LR label="{<f0> date_0 }"]

        parameters_nodes = """
        /* Style for nodes representing parameter channels in workflow */
        node[shape=box style="rounded,filled" fillcolor="#FCFCFC" peripheries=1 fontname=Helvetica]

        /* Nodes representing parameter channels in workflow */
        """

        edges = """
        /* Edges representing connections between programs and channels */
        """
        #"col12_0-date" -> state_4


        footer = """
        /* End of double cluster for drawing box around nodes in workflow */
        }}

        /* End of top-level graph */
        }
        """

        isinput = set()

        process_nodes = []
        data_nodes_in = set()
        data_nodes_out = set()

        parallel_graph = nx.DiGraph()

        if process==None:
            process = stack_process

        #for key,val in process.items():
        #for key,val in collapsed_process.items():
        #for key,val in freq_pattern_process.items():
        #for key,val in simple_process.items():
        for key,val in stack_process.items():
            try:
                # remove single process
                val["input"]
            except:
                continue 

            try:
                group_process = val["group_process"]
                step_name = "Step {}".format(key.split("_")[-1])
            except:
                group_process = []
                step_name = "Step {}".format(key.split("_")[-1])

            try:
                freq_pattern = val["freq_pattern"]
                step_name = "Step {}".format(key.split("_")[-1])
            except:
                freq_pattern = []
                step_name = "Step {}".format(key.split("_")[-1])

            try:
                json_recipe = val["operation"]
            except:
                json_recipe = {}
                json_recipe["op"] = "single-operation"
                val["op"] = "single-operation"

            detail = ""

            #if (len(group_process)==0) & (len(freq_pattern)==0) & () 

            #if len(freq_pattern_process)>0:
                
            if len(group_process)==0:
                if len(freq_pattern)>0:
                    for y in freq_pattern[1]:
                        pname = y[0][2]
                        internal_group = [y[0]] + y[2]
                        detail += "| {}{} ".format("group-" if len(internal_group)>1 else "",pname)
                        # if freq_pattern is cluster
                        if len(internal_group)>0:                    
                            detail+=" ({}, {})".format(internal_group[0][0],len(internal_group))

                            for x in internal_group:
                                temp_val = process[x[0]]
                                try:
                                    json_recipe = temp_val["operation"]
                                except:
                                    json_recipe = {}
                                    json_recipe["op"] = "single-operation"
                                    temp_val["op"] = "single-operation"
                                                        
                                """
                                if temp_val["op"]=="core/mass-edit":
                                    cells_variance = sum([len(x["from"]) for x in json_recipe["edits"]])
                                    cells_affected = int(temp_val["desc"].split("edit")[-1].split("cells")[0])
                                    detail+="| {} =\> {}, {} cells".format(cells_variance,len(json_recipe["edits"]),cells_affected)
                                else:
                                    try:
                                        cells_affected = int(temp_val["desc"].split("Text transform on")[-1].split("cells")[0])        
                                        detail += "| {}, {} changed".format(json_recipe["expression"].replace('"','\\"'),cells_affected)
                                    except:
                                        pass
                                    #break
                                """
                        else:                    
                            temp_val = process[y[0][0]]
                            try:
                                json_recipe = temp_val["operation"]
                            except:
                                json_recipe = {}
                                json_recipe["op"] = "single-operation"   
                                temp_val["op"] = "single-operation"
                                    
                            if temp_val["op"]=="core/mass-edit":
                                cells_variance = sum([len(x["from"]) for x in json_recipe["edits"]])
                                cells_affected = int(temp_val["desc"].split("edit")[-1].split("cells")[0])
                                detail+="| {} =\> {}, {} cells".format(cells_variance,len(json_recipe["edits"]),cells_affected)
                            else:
                                try:
                                    cells_affected = int(temp_val["desc"].split("Text transform on")[-1].split("cells")[0])        
                                    detail += "| {}, {} changed".format(json_recipe["expression"].replace('"','\\"'),cells_affected)
                                except:
                                    pass
                                    
                else:
                    if val["op"]!="core/mass-edit":
                        try:
                            detail += "| {} ".format(json_recipe["expression"].replace('"','\\"'))
                        except:
                            pass
                    else:
                        """
                        for x in json_recipe["edits"]:
                            detail+=",".join(x["from"])[:25]+" ({})".format(len(x["from"]))+" => "+x["to"][:25]+"\n"
                        """
                        cells_variance = sum([len(x["from"]) for x in json_recipe["edits"]])
                        
                        detail+="| {} =\> {} clusters".format(cells_variance,len(json_recipe["edits"]))
            else:
                for x in group_process:
                    temp_val = process[x[0]]
                    try:
                        json_recipe = temp_val["operation"]
                    except:
                        json_recipe = {}
                        json_recipe["op"] = "single-operation"
                        val["op"] = "single-operation"
                    if temp_val["op"]=="core/mass-edit":
                        cells_variance = sum([len(x["from"]) for x in json_recipe["edits"]])
                        cells_affected = int(temp_val["desc"].split("edit")[-1].split("cells")[0])
                        detail+="| {} =\> {}, {} cells".format(cells_variance,len(json_recipe["edits"]),cells_affected)
                    else:
                        try:
                            cells_affected = int(temp_val["desc"].split("Text transform on")[-1].split("cells")[0])        
                            detail += "| {}, {} changed".format(json_recipe["expression"].replace('"','\\"'),cells_affected)
                        except:
                            pass
            
            description = ""
            if (len(group_process)==0) & (len(freq_pattern)==0):
                if val["op"]=="core/mass-edit":
                    cells_affected = int(val["desc"].split("edit")[-1].split("cells")[0])
                    description+="| {} cells affected".format(cells_affected)
                elif val["op"]=="core/text-transform":
                    cells_affected = int(val["desc"].split("Text transform on")[-1].split("cells")[0])        
                    description+="| {} cells changed".format(cells_affected)

            if len(freq_pattern)>0:
                freq_pattern_nodes+='''
                {state_id} [shape=record rankdir=LR label="{{<f0> {step_name} ({function_name}) {detail} {description}}}"];
                '''.format(state_id=key,step_name=step_name,function_name=freq_pattern[0],detail=detail,description=description)    
            elif len(group_process)>0:
                group_process_nodes+='''
                {state_id} [shape=record rankdir=LR label="{{<f0> {step_name} ({is_group}{function_name}) {detail} {description}}}"];
                '''.format(state_id=key,step_name=step_name,is_group="group-" if len(group_process)>0 else "",function_name=val["annotation"],detail=detail,description=description)
            else:  
                single_process+='''
                {state_id} [shape=record rankdir=LR label="{{<f0> {step_name} ({is_group}{function_name}) {detail} {description}}}"];
                '''.format(state_id=key,step_name=step_name,is_group="group-" if len(group_process)>0 else "",function_name=val["annotation"] if len(val["annotation"])>0 else "single-edit",detail=detail,description=description)
                #print(single_process)

            

            temp_name = "{state}\\n".format(state=key)
            temp_desc = ""
            try:
                temp_desc += val["op"] +"\\n" + val["annotation"] +"\\n"
            except:
                pass
            temp_desc += val["desc"]
            nodes_def+="""
                {x} [label="{label}" id="{id}" fontsize=18 shape=box tooltip="{tt}" color="#b20400" fillcolor="#edd6d5"]
            """.format(x=key,label=temp_name+temp_desc.replace('"',"'"),id=key,tt="")
                
            colkey = key.split("_")[-1]
            parallel_graph.add_node(key,label=temp_desc,type="state",t_id=colkey)
            try:
                val["input"]
            except:
                continue

            temp_process_yw = """
            #@begin {key} #@desc {desc} 
            """.format(key=key,desc=temp_desc.replace('"',"'"))
            #format(key=key,desc=key)
            
            
            col_state = self.get_column_at_state(int(key.split("_")[-1]))
            
            prev_col_state = self.get_column_at_state(int(key.split("_")[-1])-1)
            
            #if int(key.split("_")[-1])==25:
            #    print("cstate",key,col_state)
            #    print("pstate",key,prev_col_state)

            
            for valinput in val["input"]:
                try:
                    colkey = valinput.split("_")[0].split("col")[-1]
                    #print(colkey)
                    colname = prev_col_state[prev_col_state.col_id==int(colkey)].col_name.values[0]
                    #print("colname-x",colname)
                except:
                    print(colkey)
                    colname = colkey 
                if valinput not in isinput:            
                    #prev_col_state = orpe.get_column_at_state(int(key.split("_")[-1])+1)            
                    nodes_def+="""
                        {x} [label="{label}" id="{id}" fontsize=18 shape=oval tooltip="" color="#b20400" fillcolor="#edd6d5"]
                    """.format(x=valinput,label=valinput.replace('"',"'")+"\\n"+colname,id=key,tt="")
                    isinput.add(valinput)
                    
                    parallel_graph.add_node(valinput,label=valinput,type="col",t_id=colkey)
                
                #print(col_state)

                edge_def+="""
                    {input} -> {output} [label="{data}" weight=14 color="#b26e37" tooltip="" labeltooltip=""]    
                """.format(input=valinput,output=key,data="")

                parallel_graph.add_edge(valinput,key)
                data_nodes_in.add("{key}-{colname}".format(key=valinput,colname=colname).replace(" ","_"))
                temp_process_yw += """
                #@in """+ """{key}-{colname}""".format(key=valinput,colname=colname).replace(" ","_")
                
                col_label = "{}_{}".format(colname,valinput.split("_")[-1]) 
                column_nodes+="""
                "{node_name}" [shape=record rankdir=LR label="{{<f0> {label} }}"]
                """.format(node_name=valinput,label=col_label)

                edges+="""
                "{}" -> {}
                """.format(valinput,key)

            for valoutput in val["output"]:
                try:
                    colkey = valoutput.split("_")[0].split("col")[-1]
                    #print(colkey)
                    colname = col_state[col_state.col_id==int(colkey)].col_name.values[0]
                    #print(colname)
                except:
                    #print(colkey)
                    colname = "_".join(valoutput.split("_")[:-1])
                    
                if valoutput not in isinput:
                    nodes_def+="""
                        {x} [label="{label}" id="{id}" fontsize=18 shape=oval tooltip="" color="#b20400" fillcolor="#edd6d5"]
                    """.format(x=valoutput,label=valoutput.replace('"',"'")+"\\n"+colname,id=key,tt="")
                    isinput.add(valoutput)

                    parallel_graph.add_node(valoutput,label=valoutput,type="col",t_id=colkey)

                #print(col_state)
                
                edge_def+="""
                    {input} -> {output} [label="{data}" weight=14 color="#b26e37" tooltip="" labeltooltip=""]    
                """.format(input=key,output=valoutput,data="")

                parallel_graph.add_edge(key,valoutput)
                data_nodes_out.add("{key}-{colname}".format(key=valoutput,colname=colname).replace(" ","_"))
                temp_process_yw += """
                #@out """+"""{key}-{colname}""".format(key=valoutput,colname=colname).replace(" ","_")

                col_label = "{}_{}".format(colname,valoutput.split("_")[-1]) 
                column_nodes+="""
                "{node_name}" [shape=record rankdir=LR label="{{<f0> {label} }}"]
                """.format(node_name=valoutput,label=col_label)

                edges+="""
                "{}" -> {}
                """.format(key,valoutput)

            temp_process_yw += """
            #@end {key}
            """.format(key=key)
            process_nodes.append(temp_process_yw)
        

        gv_string = header+single_process+column_nodes+parameters_nodes+group_process_nodes+freq_pattern_nodes+edges+footer

        return process_nodes,parallel_graph,gv_string


    def parallel_workflow(self):
        linear_recipe = self.get_linear_recipe()

        col_dependency = self.get_all_column_dependency()
        state_order = self.get_state_order()
        state_col_dep = col_dependency.merge(state_order)

        process = {}
        col_counter = {}
        max_state = self.get_number_of_state()

        for x in linear_recipe.sort_values("state_id",ascending=False).to_records():
            sid = x.state_id    
            #print(sid)


            xx = json.loads(x.detail)

            real_state = max_state.values[0][0]-x.state_id    
            prev_state = max_state.values[0][0]-x.prev_state_id
            #print(x.prev_state_id)

            if real_state == 0:
                input_name = "tableoriginal"
            else:
                input_name = "table{}".format(real_state) 

            process["state_{}".format(prev_state)] = {}
            try:
                state_pd = state_col_dep.groupby("state_id").get_group(sid)
                process["state_{}".format(prev_state)]["input"] = []
                process["state_{}".format(prev_state)]["output"] = []

                icol = set()
                ocol = set()
                for y in state_pd.to_records():
                    colname = "col{}".format(y.input_column)
                    if colname not in icol:
                        try:
                            col_counter[colname] #+= 1
                        except:
                            col_counter[colname] = 0                                                
                        process["state_{}".format(prev_state)]["input"].append("{colname}_{counter}".format(colname=colname,counter=col_counter[colname]))
                        icol.add(colname)
                    
                    if y.output_column==-2:
                        colname = "col_removed"
                    else:
                        colname = "col{}".format(y.output_column)
                    if colname not in ocol:
                        try:
                            col_counter[colname] += 1
                        except:
                            col_counter[colname] = 0     
                        process["state_{}".format(prev_state)]["output"].append("{colname}_{counter}".format(colname=colname,counter=col_counter[colname]))
                        ocol.add(colname)
                #print(state_pd)
            except BaseException as ex:
                print(ex)
                pass
                #print(x.command)

            process["state_{}".format(prev_state)]["desc"] = xx["description"]
            try:
                process["state_{}".format(prev_state)]["annotation"] = x.process_annotation
            except:
                process["state_{}".format(prev_state)]["annotation"] =  ""
            try:
                process["state_{}".format(prev_state)]["op"] = xx["operation"]["op"].replace("core/","")
                if process["state_{}".format(prev_state)]["annotation"] == "":
                    process["state_{}".format(prev_state)]["annotation"] = xx["operation"]["op"].replace("core/","")                                                    
                process["state_{}".format(prev_state)]["operation"] = xx["operation"]
            except:
                pass      
        
        return process

    def parallel_state(self,parallel_graph,col_name):

        state_cmd = self.get_all_state_command()
        #temp_path_recipe = list(filter(lambda x:x!=None,[json.loads(state_cmd[state_cmd.state_id==orpe.get_state_to_step(int(yy.split("_")[-1])-1)].detail.values[0])["operation"] for yy in combined_recipe]))
        #temp_path_recipe

        sink_nodes = [node for node, outdegree in dict(parallel_graph.out_degree(parallel_graph.nodes())).items() if outdegree == 0]
        source_nodes = [col_name]
        recipes = []
        data = []
        paths = []
        for x in [(source, sink) for sink in sink_nodes for source in source_nodes]:
            a_simple_path = list(nx.all_simple_paths(parallel_graph, source=x[0], target=x[1]))
            a_state_path = [list(filter(lambda x:x.startswith("state"), x)) for x in a_simple_path]
            #print(a_simple_path)
            a_data = [list(filter(lambda x:x.startswith("col"), x)) for x in a_simple_path]
            for i,path in enumerate(a_state_path):
                print(x)
                temp_path_recipe = list(filter(lambda x:x!=None,[json.loads(state_cmd[state_cmd.state_id==self.get_state_to_step(int(yy.split("_")[-1])-1)].detail.values[0])["operation"] for yy in path]))
                recipes.append(temp_path_recipe)
                paths.append((path,a_data[i]))
        return recipes,paths


    def collapsed_iterative(self,process,parallel_graph):
        sink_nodes = [node for node, outdegree in dict(parallel_graph.out_degree(parallel_graph.nodes())).items() if outdegree == 0]
        source_nodes = [node for node, indegree in dict(parallel_graph.in_degree(parallel_graph.nodes())).items() if indegree == 0]

        all_parallel_test = []
        for x in source_nodes: 
            all_parallel_test.extend(self.parallel_state(parallel_graph,x)[1])

        import networkx as nx
        state_graph = nx.DiGraph()
        for x in all_parallel_test:
            #print(x[0])
            if len(x[0])>1:
                for i,y in enumerate(x[0][:-1]):
                    state_graph.add_edge(y,x[0][i+1])
            else:
                #print(y)
                state_graph.add_node(x[0][0])

        end_sink_nodes = [node for node, outdegree in dict(state_graph.out_degree(state_graph.nodes())).items() if outdegree == 0]
        start_source_nodes = [node for node, indegree in dict(state_graph.in_degree(state_graph.nodes())).items() if indegree == 0]
        end_split =  [node for node, outdegree in dict(state_graph.out_degree(state_graph.nodes())).items() if outdegree > 1]
        start_split =   list(set([x[1] for x in state_graph.out_edges(end_split)]))
        start_merge = [node for node, indegree in dict(state_graph.in_degree(state_graph.nodes())).items() if indegree > 1]
        end_merge =   list(set([x[0] for x in state_graph.in_edges(start_merge)]))


        start_points = start_source_nodes+start_split+start_merge
        end_points = end_sink_nodes+end_split+end_merge

        cluster = {}
        for x in start_points:
            for y in end_points:
                try:
                    #print(x)
                    ps = nx.shortest_path(state_graph,x,y)
                    try:
                        if len(ps) < len(cluster[ps[-1]]):
                            cluster[ps[-1]] = ps
                    except:
                        cluster[ps[-1]] = ps
                    #print(ps)
                except:
                    pass
                
        ind_combined = list(cluster.values())

        detail_parallel = []
        for x in ind_combined:
            #it_process = [(y,process[y]["op"],process[y]["annotation"],process[y]["input"],process[y]["output"]) for y in x]
            it_process = []
            for y in x:
                try:
                    it_process.append((y,process[y]["op"],process[y]["annotation"],process[y]["input"],process[y]["output"]))
                except:
                    it_process.append((y,"single-op","single-op",process[y]["input"],process[y]["output"]))
            #print(it_process)
            temp = it_process[0][2]
            temp_y = it_process[0]
            #print("tt",it_process)
            let_temp = []
            details = []
            #print(len(it_process))
            if len(it_process)==1:
                let_temp = [(temp_y,None,details)]
            else:
                #print(temp_y[0])
                test_break = False
                is_collapsed = False        
                if temp_y[0]=="state_14":
                    print(it_process)
                    test_break = True
                for y in it_process[1:]:
                    #if test_break:
                    #    print(y,temp,let_temp)
                    if temp!=y[2]:            
                        let_temp.append((temp_y,details[-1][4] if len(details)>0 else None,details))
                        temp = y[2]
                        temp_y = y
                        details = []
                        is_collapsed = False        
                    else:
                        details.append(y)
                        is_collapsed = True
                if is_collapsed:
                    let_temp.append((temp_y,details[-1][4] if len(details)>0 else None,details))
                if len(details)==0 and not is_collapsed:
                    let_temp.append((temp_y,details[-1][4] if len(details)>0 else None,details))
                #print(let_temp)        
            detail_parallel.append(let_temp)

        collapsed_process = {}
        skip_orphan_nodes = False
        for x in detail_parallel:
            if (len(x)==1) and (len(x[0][2])!=2) and skip_orphan_nodes :
                continue    
            for y in x:           
                print(y[0][0])
                group_process = y[2]
                group_detail = []
                try:
                    temp_process = collapsed_process[y[0][0]]
                except:
                    temp_process = process[y[0][0]].copy()
                if len(group_process)>0:
                    temp_process["output"] = group_process[-1][4]
                    #temp_process["desc"] = "group {} {} processes".format(temp_process["annotation"],len(group_process)+1)
                    temp_process["group_process"] = [y[0]] + group_process
                    #print(group_process[-1][4])            
                collapsed_process[y[0][0]] = temp_process


        n = 1
        is_recur = True
        all_n_count = {}
        while is_recur:
            n_count = {}
            for x in detail_parallel:
                yy = [y[0][2] for y in x]
                for y in ngrams(yy,n):
                    try:
                        n_count[y]
                    except:
                        n_count[y] = 0
                    n_count[y]+=1
            if sum(n_count.values()) == len(n_count.values()):
                is_recur=False        
            else:
                all_n_count[n] = n_count
                #print(sum(n_count.values()),len(n_count.values()))
                n+=1
                #is_recur=False


        max_keys = max(all_n_count.keys())
        filtered_freq = []
        temp_all_n_count = all_n_count.copy()
        for x in range(max_keys,0,-1):
            temp = filter(lambda x:x[1]>1,list(temp_all_n_count[x].items()))
            for y in temp:
                for ii in range(x-1,0,-1):
                    for j in list(ngrams(y[0],x-ii)):
                        temp_all_n_count[x-ii][j]-=y[1]
            print(list(temp))


        flattened_items = []
        for y in [list(x.items()) for x in temp_all_n_count.values()]:
            for x in y:
                flattened_items.append(x)
        list(filter(lambda x:x[1]>1,flattened_items))

        combined_ops = []
        for i,x in enumerate(temp_all_n_count.values()):
            if i == 0:
                combined_ops.extend(list(filter(lambda x: x[1]> 0, list(x.items()))))
            else:
                combined_ops.extend(list(filter(lambda x: x[1]> 1, list(x.items()))))


        sub_workflow = [x[0] for x in list(filter(lambda x: len(x[0])>1,combined_ops))]

        import itertools

        def lookup(iterable, length):
            tees = itertools.tee(iterable, length)
            for i, t in enumerate(tees):
                for _ in range(i):
                    next(t, None)
            return zip(*tees)

        def has_sequence(array, sequence):
            sequence = tuple(sequence)
            group_index = []
            j = 0
            for i,group in enumerate(lookup(array, len(sequence))):
                #if j>0:
                #    j-=1
                #    continue
                if group == sequence:
                    #print(i,array,group)
                    #array=array[i+len(group):]
                    #group_index.append((i,array,group))
                    #j=len(group)
                    return [i,array,group]
                
            #return any(group == sequence for group in lookup(array, len(sequence)))
            return None


        all_new_proc = []
        all_det_proc = []
        for x in detail_parallel:
            proces_det = [y[0][2] for y in x]
            j=0
            new_proc = []
            det_proc = []
            for i,y in enumerate(sub_workflow):
                sequence = has_sequence(proces_det[j:],y)
                if sequence!=None:
                    sequence[0] = sequence[0]+j
                    sequence[1] = proces_det
                    print(sequence)
                    new_proc = new_proc + proces_det[j:sequence[0]] + ["sub_process_{}".format(i)]
                    det_proc = det_proc + x[j:sequence[0]] + [ (("sub_process_{}".format(i),x[j+sequence[0]:(j+sequence[0]+len(sequence[2]))]),) ]
                    j+=sequence[0]+len(sequence[2])
            if j == 0:
                new_proc = proces_det
                det_proc = x
            else:
                new_proc = new_proc + proces_det[j:]
                det_proc = det_proc + x[j:]
            all_new_proc.append(new_proc)
            all_det_proc.append(det_proc)


        freq_pattern_process = {}
        for x in all_det_proc:
            # skip orphan process
            if (len(x)==1) and (len(x[0][0])!=2) and skip_orphan_nodes :
                continue
            for y in x:           
                state = y[0][0]
                #print(state)
                # check freq_pattern_output
                #if len(y[0])
                #print(state)
                #if state=="sub_process_1":
                #    break
                if len(y[0]) == 2:
                    first_process = y[0][1][0]
                    #print(first_process)
                    temp_process = process[first_process[0][0]].copy()
                    last_process = y[0][1][-1]
                    group_process = last_process[2]
                    #group_process = last_process[0]
                    if len(group_process)>0:
                        temp_process["output"] = group_process[-1][4]                
                    else:
                        temp_process["output"] = last_process[0][4]

                    #if len(group_process)>0:
                    #        temp_process["output"] = group_process[-1][4]
                    
                    if state=="sub_process_1":
                        print(group_process)

                    """
                    inside_group = []
                    for z in y[0][1]:
                        inside_state = z[0][0]
                        group_process = z[2]
                        temp_process = process[inside_state].copy()
                        if len(group_process)>0:
                            temp_process["group_process"] = [z[0]] + group_process
                        inside_group.append(temp_process)
                    
                    temp_process["inside_group"] = inside_group
                    """
                    
                    temp_process["freq_pattern"] = y[0]
                    freq_pattern_process[first_process[0][0]] = temp_process
                else:
                    #if state=="sub_process_1":
                    #    print(temp_process)

                    group_process = y[2]
                    temp_process = process[state].copy()
                    if len(group_process)>0:
                        temp_process["output"] = group_process[-1][4]
                        temp_process["group_process"] = [y[0]] + group_process
                    freq_pattern_process[state] = temp_process
                

                """
                group_process = y[2]
                group_detail = []
                try:
                    temp_process = collapsed_process[y[0][0]]
                except:
                    temp_process = process[y[0][0]].copy()
                if len(group_process)>0:
                    temp_process["output"] = group_process[-1][4]
                    #temp_process["desc"] = "group {} {} processes".format(temp_process["annotation"],len(group_process)+1)
                    temp_process["group_process"] = [y[0]] + group_process
                    #print(group_process[-1][4])            
                collapsed_process[y[0][0]] = temp_process
                """
        return collapsed_process,freq_pattern_process
        
