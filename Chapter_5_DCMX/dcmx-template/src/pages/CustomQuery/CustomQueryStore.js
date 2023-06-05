import { store, view } from '@risingstack/react-easy-state';
import databaseState from '../SelectDatabase/DatabaseState';


const datasetStore = store({
    data: [],
    columns: [],
    steps: [],
    customQuery: "",
    queryList: [
        { id: "button1", value: "Summary of Cell Changes" },
        { id: "button2", value: "Process Dependency" }
    ],
    handleChange(e) {
        console.log(e);
        datasetStore.customQuery = e.target.value;
    },
    queryTemplate(arg0) {
        if (arg0 === "button1") {
            datasetStore.customQuery = 'SELECT (state_id-(select max(state_id)+1 from state s))*-1 as state, \
                substr(command,33) as operation, \
                col_name as column_name, \
                count(1) as cell_changes \
                FROM state \
                NATURAL JOIN content \
                NATURAL  JOIN value \
                NATURAL JOIN cell \
                NATURAL JOIN state_command \
                NATURAL JOIN col_each_state \
                group by state,col_name order by state asc'
        } else if (arg0 === "button2") {
            datasetStore.customQuery = 'select distinct (a.state_id-(select max(state_id)+1 from state s))*-1 as state,substr(c.command,33) command,(b.state_id-(select max(state_id)+1 from state s))*-1 as dep_state,substr(d.command,33) as dep_command \
              from col_dependency a,col_dep_state b,state_command c,state_command d \
              where a.input_column = b.prev_input_column \
              and a.state_id>-1 \
              and a.state_id<b.state_id \
              and a.state_id=c.state_id \
              and b.state_id=d.state_id \
              and state in (4,8) \
              order by state,dep_state asc;'
        }
        return undefined;

    },
    async submitQuery(event){
        event.preventDefault();
        const formData = new FormData();
        formData.append("query",datasetStore.customQuery);
        const button = event.currentTarget;
        let resJson = await fetch("http://127.0.0.1:5000/explorer/" + databaseState.dbName + "/query", {
          method: 'POST',
          mode: 'cors',
          headers: {
            //'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': 'http://127.0.0.1:5000'
          },
          body: formData,
        })
        let json = await resJson.json()
        datasetStore.columns =  json.columns
        datasetStore.data = json.values;
    },
    async changeState(stateId) {
        //datasetStore.stateId = stateId        
        fetch("http://127.0.0.1:5000/explorer/" + databaseState.dbName + "/dataset_state/" + (Number(stateId)))
            .then(res => res.json())
            .then(json => {
                //console.log(json.workflow);
                datasetStore.columns = []
                json.columns.forEach(element => {
                    datasetStore.columns.push({
                        name: element,
                        options: {
                            customBodyRender: datasetStore.customBodyRender
                        }
                    })
                });
                datasetStore.data = json.values
                datasetStore.stateId = Number(json.state)
            });
    },
    async historyClick(colData, cellMeta) {
        console.log(colData, cellMeta)
        console.log(datasetStore.data[cellMeta.rowIndex][cellMeta.colIndex])
        let dd = datasetStore.data[cellMeta.rowIndex][cellMeta.colIndex]
        let letHistory = await fetch("http://127.0.0.1:5000/explorer/" + databaseState.dbName + "/cell_lineage/" + cellMeta.colIndex + "/" + cellMeta.rowIndex + "/" + datasetStore.stateId)
        let history = await letHistory.json()
        datasetStore.dotHistory = history.dot
        return
    },
    customBodyRender(value, tableMeta, updateValue) {
        return value[0]
    }
})

export default datasetStore