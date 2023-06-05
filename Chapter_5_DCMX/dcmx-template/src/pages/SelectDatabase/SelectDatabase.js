import { Typography } from '@mui/material'
import Page from 'material-ui-shell/lib/containers/Page'
import React, { Component } from 'react'
import { useIntl } from 'react-intl'
import DropdownList from "react-widgets/DropdownList";
import "react-widgets/styles.css";
import { store, view } from '@risingstack/react-easy-state';
import Container from '@mui/material/Container';
import Grid from '@mui/material/Grid';
import Workflow from 'pages/Workflow/Workflow';
import Graphviz from 'graphviz-react';


// state import
import databaseState from './DatabaseState';
import workflowStore from 'pages/Workflow/WorkflowState';
import datasetStore from 'pages/DatasetView/DatasetStore';


/*
const Home = () => {
  const intl = useIntl()

  return (
    <Page pageTitle={intl.formatMessage({ id: 'Select DCM Database' })}>
      <Typography>{intl.formatMessage({ id: 'home' })}</Typography>
    </Page>
  )
}
*/


class Home extends Component {
  constructor(props) {
    super(props);
    this.state = {
      dbList: ["db1", "db2"],
      dbName: databaseState.dbName,
      dot: "",
    };
  }

  comboBoxListDb() {
    return (
      <DropdownList
        data={this.state.dbList}
        value={databaseState.dbName}
        onChange={(value) => this.changeDb(value)}
      />
    )
  }

  async changeDb(dbName) {
    this.setState({ dbName: dbName });
    databaseState.dbName = dbName;

    /*
    fetch("http://127.0.0.1:5000/explorer/" + databaseState.dbName + "/parallel_workflow")
      .then(res => res.json())
      .then(json => {
        //console.log(json.workflow);
        this.setState({ dot: json.workflow });
      });
    */
    await workflowStore.updateColumns();
    await workflowStore.visualizeWorkflow();
    await datasetStore.changeState(-1);
  }

  async componentDidMount() {
    let dbRes = await fetch("http://127.0.0.1:5000/explorer");
    let jsonRes = await dbRes.json();
    this.setState({ dbList: jsonRes });
  }

  render() {
    return (
      <Page pageTitle={'Select DCM Database'}>
        <Container maxWidth="lg" sx={{ mt: 4, mb: 4 }}>
          <Grid item xs={12}>
            DCM Database:
            {this.comboBoxListDb()}
            Workflow:
            <Graphviz dot={databaseState.workflowDot}
              options={{
                fit: true,
                height: 768,
                width: 1024,
                zoom: true,
              }} />            
            </Grid>
        </Container>
      </Page>
    )
  }
}


export default view(Home)


/*
<Container maxWidth="lg" sx={{ mt: 4, mb: 4 }}>
            <Grid container spacing={3}>
              <Grid item xs={12} md={8} lg={9}>
                <Paper
                  sx={{
                    p: 2,
                    display: 'flex',
                    flexDirection: 'column',
                    height: 240,
                  }}
                >
                  <Chart />
                </Paper>
              </Grid>
              <Grid item xs={12} md={4} lg={3}>
                <Paper
                  sx={{
                    p: 2,
                    display: 'flex',
                    flexDirection: 'column',
                    height: 240,
                  }}
                >
                  <Deposits />
                </Paper>
              </Grid>
              <Grid item xs={12}>
                <Paper sx={{ p: 2, display: 'flex', flexDirection: 'column' }}>
                  <Orders />
                </Paper>
              </Grid>
            </Grid>
            <Copyright sx={{ pt: 4 }} />
          </Container>
*/