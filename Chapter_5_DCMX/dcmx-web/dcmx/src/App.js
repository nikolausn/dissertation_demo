import React, { Component } from "react";
import './App.css';
//import AnimatedCircles from './Transitions';
import dynamic from 'next/dynamic';

import DropdownList from "react-widgets/DropdownList";
import Listbox from "react-widgets/Listbox";
import "react-widgets/styles.css";

import { styled } from '@mui/material/styles';
import Table from '@mui/material/Table';
import TableBody from '@mui/material/TableBody';
import TableCell, { tableCellClasses } from '@mui/material/TableCell';
import TableContainer from '@mui/material/TableContainer';
import TableHead from '@mui/material/TableHead';
import TableRow from '@mui/material/TableRow';
import Paper from '@mui/material/Paper';

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

class App extends Component {
  constructor() {
    super();
    this.state = {
      dot: 'graph{a--b}',
      dbList: ["db1", "db2"],
      dbName: "",
      columnList: [],
      columnRecipe: "<All Column>",
      recipeExtract: "",
      steps: []
    };
  }

  changeDb(dbName) {
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
        let columnList = [{ id: -1, name: "<All Column>" }]
        json.forEach(element => {
          columnList.push({ id: element.col_id, name: element.col_name })
        });
        this.setState({ columnList: columnList, columnRecipe: "<All Column>" });
      });
    fetch("http://127.0.0.1:5000/explorer/" + dbName + "/state_op")
      .then(res => res.json())
      .then(json => {
        //console.log(json.workflow);
        let columnList = []
        json.forEach(element => {
          columnList.push({ id: element.state_id, op: element.op.description })
        });
        this.setState({ steps: columnList });
      });
  }

  changeColumn(column_id, value) {
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

  comboBoxListDb() {
    return (
      <DropdownList
        data={this.state.dbList}
        onChange={(value) => this.changeDb(value)}
      />
    )
  }

  comboBoxListColumns() {
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

    //let res = await fetch("http://127.0.0.1:5000/explorer/airbnb_demo.db/parallel_workflow");
    //let json = await res.json();
    //this.setState({ dot: json.workflow });
  }

  /*
  componentDidMount() {
    fetch("http://127.0.0.1:5000/explorer/airbnb_demo.db/parallel_workflow")
      .then(res => res.json())
      .then(json => {
        console.log(json.workflow);
        this.setState({ dot: json.workflow });
      });
  }
  */

  render() {
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
            height: 1024,
            width: 768,
            zoom: false
          }} />
          Table State: <Listbox data={this.state.steps}
            dataKey="state_id"
            textField="op"
            onChange={(value) => this.changeState(value.id)}
          />
          Table: {customizedTables()}
        </header>
      </div>
    );
  }
}

//Table: {customizedTables()}

//ReactDOM.render(<App />, document.getElementById("app"));

const StyledTableCell = styled(TableCell)(({ theme }) => ({
  [`&.${tableCellClasses.head}`]: {
    backgroundColor: theme.palette.common.black,
    color: theme.palette.common.white,
  },
  [`&.${tableCellClasses.body}`]: {
    fontSize: 14,
  },
}));

const StyledTableRow = styled(TableRow)(({ theme }) => ({
  '&:nth-of-type(odd)': {
    backgroundColor: theme.palette.action.hover,
  },
  // hide last border
  '&:last-child td, &:last-child th': {
    border: 0,
  },
}));

function createData(
  name,
  calories,
  fat,
  carbs,
  protein,
) {
  return { name, calories, fat, carbs, protein };
}

const rows = [
  createData('Frozen yoghurt', 159, 6.0, 24, 4.0),
  createData('Ice cream sandwich', 237, 9.0, 37, 4.3),
  createData('Eclair', 262, 16.0, 24, 6.0),
  createData('Cupcake', 305, 3.7, 67, 4.3),
  createData('Gingerbread', 356, 16.0, 49, 3.9),
];

function customizedTables() {
  return (
    <Table aria-label="customized table">
      <TableHead>
        <TableRow>
          <StyledTableCell>Dessert (100g serving)</StyledTableCell>
          <StyledTableCell align="right">Calories</StyledTableCell>
          <StyledTableCell align="right">Fat&nbsp;(g)</StyledTableCell>
          <StyledTableCell align="right">Carbs&nbsp;(g)</StyledTableCell>
          <StyledTableCell align="right">Protein&nbsp;(g)</StyledTableCell>
        </TableRow>
      </TableHead>
      <TableBody>
        {rows.map((row) => (
          <StyledTableRow key={row.name}>
            <StyledTableCell component="th" scope="row">
              {row.name}
            </StyledTableCell>
            <StyledTableCell align="right">{row.calories}</StyledTableCell>
            <StyledTableCell align="right">{row.fat}</StyledTableCell>
            <StyledTableCell align="right">{row.carbs}</StyledTableCell>
            <StyledTableCell align="right">{row.protein}</StyledTableCell>
          </StyledTableRow>
        ))}
      </TableBody>
    </Table>);
}

export default App;
