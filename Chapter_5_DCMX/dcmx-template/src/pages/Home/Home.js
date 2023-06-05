import { Typography } from '@mui/material'
import Page from 'material-ui-shell/lib/containers/Page'
import React, {Component} from 'react'
import { useIntl } from 'react-intl'
import DropdownList from "react-widgets/DropdownList";
import "react-widgets/styles.css";


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


class Home extends Component{
  constructor(props) {
    super(props);
    this.state = {
      dbList: ["db1", "db2"],
      dbName: "",
      dot: "",
    };
  }

  comboBoxListDb() {
    return (
      <DropdownList
        data={this.state.dbList}
        onChange={(value) => this.changeDb(value)}
      />
    )
  }

  changeDb(dbName) {
    this.setState({ dbName: dbName });
    fetch("http://127.0.0.1:5000/explorer/" + dbName + "/parallel_workflow")
      .then(res => res.json())
      .then(json => {
        //console.log(json.workflow);
        this.setState({ dot: json.workflow });
      });
  } 

  async componentDidMount() {
    let dbRes = await fetch("http://127.0.0.1:5000/explorer");
    let jsonRes = await dbRes.json();
    this.setState({ dbList: jsonRes });
  }

  render(){
    return (
      <Page pageTitle={ 'Select DCM Database'}>
        DCM Database: {this.comboBoxListDb()}
      </Page>
    )
  }
}


export default Home
