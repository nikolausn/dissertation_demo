import { store, view } from '@risingstack/react-easy-state';
import databaseState from '../SelectDatabase/DatabaseState';


const workflowStore = store({
    dot: 'graph{}',
    columnRecipe: "<All Columns>",
    columnList: [],
    recipeExtract: "",
    async visualizeWorkflow() {
        if (workflowStore.columnRecipe === "<All Columns>") {
            let dbRes = await fetch("http://127.0.0.1:5000/explorer/" + databaseState.dbName + "/parallel_workflow");
            let jsonRes = await dbRes.json();
            workflowStore.dot = jsonRes.workflow;
            databaseState.workflowDot = jsonRes.workflow;            
        } else {
            let dbRes = await fetch("http://127.0.0.1:5000/explorer/" + databaseState.dbName + "/parallel_state/")
            let jsonRes = await dbRes.json();
            workflowStore.dot = jsonRes.workflow;
        }
    },
    async updateColumns() {
        let dbRes = await fetch("http://127.0.0.1:5000/explorer/" + databaseState.dbName + "/columns")
        let jsonRes = await dbRes.json()
        workflowStore.columnList = [{ id: -1, name: "<All Columns>" }]
        jsonRes.forEach((element) => {
            workflowStore.columnList.push({ id: element.col_id, name: element.col_name });
        });
        workflowStore.columnRecipe = "<All Columns>"
    },
    async changeColumn(column_id, value) {
        workflowStore.columnRecipe = value
        if (column_id >= 0) {
            let dbRes = await fetch("http://127.0.0.1:5000/explorer/" + databaseState.dbName + "/parallel_state/" + column_id)
            let jsonRes = await dbRes.json()
            workflowStore.dot = jsonRes.workflow

            let dbRes2 = await fetch("http://127.0.0.1:5000/explorer/" + databaseState.dbName + "/column_recipes/" + column_id)
            let jsonRes2 = await dbRes2.json()
            //console.log(jsonRes2)
            workflowStore.recipeExtract = JSON.stringify(jsonRes2, null, 4)
        } else {
            let dbRes = await fetch("http://127.0.0.1:5000/explorer/" + databaseState.dbName + "/parallel_workflow")
            let jsonRes = await dbRes.json()
            workflowStore.dot = jsonRes.workflow

            let dbRes2 = await fetch("http://127.0.0.1:5000/explorer/" + databaseState.dbName + "/recipes")
            let jsonRes2 = await dbRes2.json()
            //console.log(jsonRes2)
            workflowStore.recipeExtract = JSON.stringify(jsonRes2, null, 4)
        }
    },
    async recipeChange(value){
        workflowStore.recipeExtract = value.text
    }
})

export default workflowStore