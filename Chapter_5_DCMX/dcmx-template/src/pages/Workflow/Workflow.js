import Page from 'material-ui-shell/lib/containers/Page'
import React, { Component } from 'react'
import { useIntl } from 'react-intl'
import { store, view } from '@risingstack/react-easy-state';
import dynamic from 'next/dynamic';
import { Container } from '@mui/material';

import databaseState from '../SelectDatabase/DatabaseState';
import workflowStore from './WorkflowState'
import SelectColumn from './SelectColumn'


const Graphviz = dynamic(() => import('graphviz-react'), { ssr: false });

export default view(() => (
    <Page pageTitle={'Workflow Visualization: ' + databaseState.dbName}>
        <Container maxWidth="lg" sx={{ mt: 4, mb: 4 }}>
            <SelectColumn />
            Recipe: <br />
            <textarea style={{ width: 600, height: 200 }}
            value={workflowStore.recipeExtract}
            onChange={workflowStore.recipeChange}
            />
            <Graphviz dot={workflowStore.dot}
                options={{
                    fit: true,
                    height: 768,
                    width: 1024,
                    zoom: true,
                }} />
        </Container>
    </Page>
));

/*
    <Graphviz dot={workflowStore.dot} options={{
        fit: true,
        height: 768,
        width: 1024,
        zoom: false
      }} />
*/