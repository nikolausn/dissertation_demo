import { store, view } from '@risingstack/react-easy-state';
import databaseState from '../SelectDatabase/DatabaseState';


const datasetStore = store({
    stateId: 0,
    data: [],
    columns: [],
    dotHistory: "graph{}",
    steps: [],
    handleChange(value){
        console.log(value);
        datasetStore.changeState(value.id);
    },
    async changeState(stateId){
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
    async historyClick(colData, cellMeta){
        console.log(colData,cellMeta)
        console.log(datasetStore.data[cellMeta.rowIndex][cellMeta.colIndex])   
        let dd = datasetStore.data[cellMeta.rowIndex][cellMeta.colIndex]
        let letHistory = await fetch("http://127.0.0.1:5000/explorer/" + databaseState.dbName + "/cell_lineage/"+cellMeta.colIndex+"/"+cellMeta.rowIndex+"/"+datasetStore.stateId)
        let history = await letHistory.json()
        datasetStore.dotHistory = history.dot
        return 
    },
    customBodyRender(value,tableMeta,updateValue){
        return value[0]
    }
})

export default datasetStore