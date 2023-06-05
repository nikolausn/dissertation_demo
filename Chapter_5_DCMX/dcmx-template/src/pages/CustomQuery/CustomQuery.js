import Page from 'material-ui-shell/lib/containers/Page'
import React, { Component } from 'react'
import { useIntl } from 'react-intl'
import DropdownList from "react-widgets/DropdownList";
import { store, view } from '@risingstack/react-easy-state';
import { Container } from '@mui/material';
import Listbox from "react-widgets/Listbox";

import databaseState from '../SelectDatabase/DatabaseState';
import datasetStore from './CustomQueryStore';
import MUIDataTable from "mui-datatables";
import dynamic from 'next/dynamic';

import { fontSize } from "@mui/system";


const Graphviz = dynamic(() => import('graphviz-react'), { ssr: false });

class DatasetView extends Component {
    async componentDidMount() {
        let dbRes = await fetch("http://127.0.0.1:5000/explorer/" + databaseState.dbName + "/state_op")
        let jsonRes = await dbRes.json();
        let columnList = []
        jsonRes.forEach((element) => {
            columnList.push({ id: element.state_id, op: element.op.description })
        });
        datasetStore.steps = columnList;
    }

    render() {
        return (
            <Page pageTitle={'Dataset state: ' + databaseState.dbName}>
                <Container maxWidth="lg" sx={{ mt: 4, mb: 4 }}>
                    Query Template:
                    <DropdownList
                        data={datasetStore.queryList}
                        dataKey='id'
                        textField='value'
                        onChange={(value) => datasetStore.queryTemplate(value.id)}
                    />
                    Query: <br />
                    <textarea style={{ width: 600, height: 200 }}
                        value={datasetStore.customQuery}
                        onChange={datasetStore.handleChange}
                    /> <br />
                    <button onClick={datasetStore.submitQuery} name="submitQuery">Submit Query</button> <br />
                    Result: <br />
                    <MUIDataTable
                        title={"Query Result"}
                        data={datasetStore.data}
                        columns={datasetStore.columns}
                        options={{
                            selectableRows: 'none'
                        }}
                    />
                </Container>
            </Page>
        )
    }
}

export default view(DatasetView);

/*
export default view(() => (
    <Page pageTitle={'Dataset state: ' + databaseState.dbName}>
        <Container maxWidth="lg" sx={{ mt: 4, mb: 4 }}>
            Data Cleaning Steps: <Listbox data={datasetStore.steps}
                dataKey="state_id"
                textField="op"
                onChange={datasetStore.handleChange}
                style={{ fontSize: "small" }}
            />
            Table State:
            <MUIDataTable
                title={"Dataset State " + datasetStore.state_id}
                data={datasetStore.data}
                columns={datasetStore.columns}
                options={{
                    selectableRows: 'none',
                    onCellClick: datasetStore.historyClick,
                }}
            />
            History of a cell:
            <Graphviz dot={datasetStore.dotHistory}
                options={{
                    fit: true,
                    height: 768,
                    width: 1024,
                    zoom: false,
                }} />
        </Container>
    </Page>
));
*/
