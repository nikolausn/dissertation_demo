import React, { Component } from "react";
import logo from './logo.svg';
import './App.css';
import dynamic from 'next/dynamic';


import DropdownList from "react-widgets/DropdownList";
import Listbox from "react-widgets/Listbox";
import "react-widgets/styles.css";

import Demo from './demo';

import MUIDataTable from "mui-datatables";
import { fontSize } from "@mui/system";

/*
function App() {
  return (
    <div className="App">
      <header className="App-header">
        <img src={logo} className="App-logo" alt="logo" />
        <p>
          Edit <code>src/App.tsx</code> and save to reload.
        </p>
        <a
          className="App-link"
          href="https://reactjs.org"
          target="_blank"
          rel="noopener noreferrer"
        >
          Learn React
        </a>
      </header>
    </div>
  );
}
*/

const Graphviz = dynamic(() => import('graphviz-react'), { ssr: false });

async function getParallelWorkflow() {
  try {
    let response = await fetch('http://127.0.0.1:5000/explorer/nypl_menu_case1.openrefine.db/parallel_workflow');
    let responseJson = await response.json();
    console.log(responseJson)
    return responseJson.workflow;
  } catch (error) {
    console.error(error);
  }
}

//let dot = await getParallelWorkflow()

interface TodoProps {

}

interface TodoState {
  dot?: any
  dbList?: Array<any>
  dbName?: string
  columnList?: Array<any>
  columnRecipe?: string
  recipeExtract?: string
  steps?: any
  table?: any
  columns?: any
  data?: any
  state_id?: number
  queryResult?: any
  queryColumns?: any
  customQuery: any
}

class App extends Component<TodoProps, TodoState> {
  constructor(props: TodoProps) {
    super(props);
    this.state = {
      dot: 'graph{a--b}',
      dbList: ["db1", "db2"],
      dbName: "",
      columnList: [],
      columnRecipe: "<All Columns>",
      recipeExtract: "",
      steps: [],
      columns: [],
      data: [],
      state_id: -1,
      queryResult: [],
      queryColumns: [],
      customQuery: "",
    };
    this.handleChange = this.handleChange.bind(this);    
  }

  changeDb(dbName: string): void {
    this.setState({ dbName: dbName });
    fetch("http://127.0.0.1:5000/explorer/" + dbName + "/parallel_workflow")
      .then(res => res.json())
      .then(json => {
        //console.log(json.workflow);
        this.setState({ dot: json.workflow });
      });
    fetch("http://127.0.0.1:5000/explorer/" + dbName + "/columns")
      .then(res => res.json())
      .then(json => {
        //console.log(json.workflow);
        let columnList = [{ id: -1, name: "<All Columns>" }]
        json.forEach((element: { col_id: any; col_name: any; }): void => {
          columnList.push({ id: element.col_id, name: element.col_name });
        });
        this.setState({ columnList: columnList, columnRecipe: "<All Columns>" });
      });
    fetch("http://127.0.0.1:5000/explorer/" + dbName + "/state_op")
      .then(res => res.json())
      .then(json => {
        //console.log(json.workflow);
        let columnList: { id: any; op: any; }[] = []
        json.forEach((element: { state_id: any; op: { description: any; }; }) => {
          columnList.push({ id: element.state_id, op: element.op.description })
        });
        this.setState({ steps: columnList });
      });
  }

  changeColumn(column_id: string | number, value: string): void {
    this.setState({ recipeExtract: "", columnRecipe: value })
    if (column_id >= 0) {
      fetch("http://127.0.0.1:5000/explorer/" + this.state.dbName + "/column_recipes/" + column_id)
        .then(res => res.json())
        .then(json => {
          //console.log(json.workflow);
          this.setState({ recipeExtract: JSON.stringify(json, null, 4) });
        });
      fetch("http://127.0.0.1:5000/explorer/" + this.state.dbName + "/parallel_state/" + column_id)
        .then(res => res.json())
        .then(json => {
          //console.log(json.workflow);
          this.setState({ dot: json.workflow });
        });
    } else {
      fetch("http://127.0.0.1:5000/explorer/" + this.state.dbName + "/recipes")
        .then(res => res.json())
        .then(json => {
          //console.log(json.workflow);
          this.setState({ recipeExtract: JSON.stringify(json, null, 4) });
        });
      fetch("http://127.0.0.1:5000/explorer/" + this.state.dbName + "/parallel_workflow")
        .then(res => res.json())
        .then(json => {
          //console.log(json.workflow);
          this.setState({ dot: json.workflow });
        });
    }
  }

  changeState(state_id: string | number): void {
    fetch("http://127.0.0.1:5000/explorer/" + this.state.dbName + "/dataset_state/" + (Number(state_id)))
      .then(res => res.json())
      .then(json => {
        //console.log(json.workflow);
        this.setState({ columns: json.columns, data: json.values, state_id: Number(json.state) });
      });
  }

  handleChange(e: { target: { value: any; }; }) {
    this.setState({
      customQuery:e.target.value
    })
  }  

  comboBoxListDb(): JSX.Element {
    return (
      <DropdownList
        data={this.state.dbList}
        onChange={(value) => this.changeDb(value)}
      />
    )
  }

  comboBoxListColumns(): JSX.Element {
    return (
      <DropdownList
        data={this.state.columnList}
        value={this.state.columnRecipe}
        dataKey='id'
        textField='name'
        onChange={(value) => this.changeColumn(value.id, value)}
      />
    )
  }

  async componentDidMount() {
    let dbRes = await fetch("http://127.0.0.1:5000/explorer");
    let jsonRes = await dbRes.json();
    this.setState({ dbList: jsonRes });

  }

  render() {
    const options = {
      filterType: 'checkbox',
    };

    const queryTemplate = [
      {id: "button1", value: "Summary of Cell Changes"},
      {id: "button2", value: "Process Dependency"}
    ]

    return (
      <div className="App">
        <header className="App-header">
          DCM Database: {this.comboBoxListDb()}
          Column: {this.comboBoxListColumns()}
          Recipe: <textarea style={{ width: 600, height: 200 }}
            value={this.state.recipeExtract}
          />
          Visualization: <Graphviz dot={this.state.dot} options={{
            fit: true,
            height: 768,
            width: 1024,
            zoom: false
          }} />
          Data Cleaning Steps: <Listbox data={this.state.steps}
            dataKey="state_id"
            textField="op"
            onChange={(value: any) => this.changeState(value.id)}
            style={{ fontSize: "small" }}
          />
          Table State:
          <MUIDataTable
            title={"Dataset State " + this.state.state_id}
            data={this.state.data}
            columns={this.state.columns}
            options={{
              selectableRows: 'none',
              onCellClick: this.historyClick()
            }}
            
          />
          {/* <table>
            <tr><button onClick={this.test1} name="button1">Summary of Cell Changes</button><button onClick={this.test1} name="button2">Process Dependency</button></tr>
          </table> */}
          Query Template:
          <DropdownList
            data={queryTemplate}            
            dataKey='id'
            textField='value'
            onChange={(value) => this.queryTemplate(value.id)}
         />
          Query:
          <textarea style={{ width: 600, height: 200 }}
            value={this.state.customQuery}
            onChange={this.handleChange}
          />
          <button onClick={this.submitQuery} name="submitQuery">Submit Query</button>
          Result:
          <MUIDataTable
            title={"Query Result"}
            data={this.state.queryResult}
            columns={this.state.queryColumns}
            options={{
              selectableRows: 'none'
            }}
          />
        </header>
      </div>
    );
  }
  historyClick(): ((colData: any, cellMeta: { colIndex: number; rowIndex: number; dataIndex: number; event: React.MouseEvent<Element, MouseEvent>; }) => void) | undefined {
    console.log()
    return undefined
  }

  submitQuery = (event: React.MouseEvent<HTMLButtonElement>) => {
    event.preventDefault();
    const formData = new FormData();
    formData.append("query",this.state.customQuery);
    const button: HTMLButtonElement = event.currentTarget;
    fetch("http://127.0.0.1:5000/explorer/" + this.state.dbName + "/query", {
      method: 'POST',
      mode: 'cors',
      headers: {
        //'Content-Type': 'application/json',
        'Access-Control-Allow-Origin': 'http://127.0.0.1:5000'
      },
      body: formData,
    })
      .then(res => res.json())
      .then(json => {
        //console.log(json.workflow);
        this.setState({ queryColumns: json.columns, queryResult: json.values});
      });
  };

  test1 = (event: React.MouseEvent<HTMLButtonElement>) => {
    event.preventDefault();

    const button: HTMLButtonElement = event.currentTarget;
    this.queryTemplate(button.name);
  };

  queryTemplate(arg0: string): React.MouseEventHandler<HTMLButtonElement> | undefined {
    if (arg0 === "button1") {
      this.setState({
        customQuery: 'SELECT (state_id-(select max(state_id)+1 from state s))*-1 as state, \
        substr(command,33) as operation, \
        col_name as column_name, \
        count(1) as cell_changes \
        FROM state \
        NATURAL JOIN content \
        NATURAL  JOIN value \
        NATURAL JOIN cell \
        NATURAL JOIN state_command \
        NATURAL JOIN col_each_state \
        group by state,col_name order by state asc' })
    } else if (arg0 === "button2") {
      this.setState({
        customQuery: 'drop view col_dep_state; \
      create view col_dep_state as \
      WITH RECURSIVE \
      col_dep_order(state_id,prev_state_id,prev_input_column,input_column,output_column,level) AS ( \
       select state_id,state_id,input_column,input_column,output_column,0 from col_dependency cd \
       UNION ALL \
        SELECT a.state_id,b.state_id,b.prev_input_column,a.input_column,b.input_column,b.level+1 \
         FROM col_dependency a, col_dep_order b \
        WHERE a.output_column=b.input_column  \
        and a.state_id>b.state_id) \
      SELECT distinct * from col_dep_order \
      order by prev_input_column; \
       \
      select distinct (a.state_id-(select max(state_id)+1 from state s))*-1 as state,substr(c.command,33) command,(b.state_id-(select max(state_id)+1 from state s))*-1 as dep_state,substr(d.command,33) as dep_command \
      from col_dependency a,col_dep_state b,state_command c,state_command d \
      where a.input_column = b.prev_input_column \
      and a.state_id>-1 \
      and a.state_id<b.state_id \
      and a.state_id=c.state_id \
      and b.state_id=d.state_id \
      and state in (4,8) \
      order by state,dep_state asc;'})
    }
    return undefined;
  }
}


export default App;
